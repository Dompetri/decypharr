package pikpak

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"go.uber.org/ratelimit"
)

const (
	// cacheCheckWindow bounds how long CheckStatus waits to decide whether a
	// freshly submitted torrent is cached on PikPak (ultimate safety cap when the
	// predict_type signal never resolves).
	cacheCheckWindow = 30 * time.Second
	// cacheCheckGrace is how long to let PikPak settle predict_type before
	// trusting a "not cached" (predict_type "1") verdict. Cached content reports
	// predict_type "3" within ~1-2s, well inside this grace, so this avoids
	// falsely rejecting cached content while still rejecting misses quickly.
	cacheCheckGrace = 10 * time.Second
	// cacheCheckInterval is the poll spacing while waiting for the cache verdict.
	cacheCheckInterval = 2 * time.Second
	// maxSyncDepth bounds how deep GetTorrents recurses into container folders.
	maxSyncDepth = 5
)

// Pikpak implements the debrid.Client interface backed by PikPak's offline
// download (cloud torrent) feature.
type Pikpak struct {
	Host                  string
	username              string
	password              string
	client                *apiClient
	accountsManager       *account.Manager
	autoExpiresLinksAfter time.Duration
	logger                zerolog.Logger
	config                config.Debrid

	profile   *types.Profile
	profileMu sync.Mutex
}

// infohashFromMagnet extracts the lowercase BitTorrent infohash from a magnet URL.
func infohashFromMagnet(magnet string) string {
	return strings.ToLower(utils.ExtractInfoHash(magnet))
}

type credentials struct {
	username string
	password string
	token    string
}

// parseCredentials extracts the PikPak credentials from the api_key field.
// Supports:
// 1. Bearer token format: "Bearer <token>", "token:<token>", or a direct JWT ("eyJ...")
// 2. Username/password format: "username:password"
func parseCredentials(apiKey string) (credentials, error) {
	apiKey = strings.TrimSpace(apiKey)
	if apiKey == "" {
		return credentials{}, fmt.Errorf("pikpak: api_key is required")
	}

	if strings.HasPrefix(strings.ToLower(apiKey), "bearer ") {
		token := strings.TrimSpace(apiKey[7:])
		return credentials{token: token}, nil
	}
	if strings.HasPrefix(apiKey, "token:") {
		token := strings.TrimSpace(strings.TrimPrefix(apiKey, "token:"))
		return credentials{token: token}, nil
	}
	if strings.HasPrefix(apiKey, "eyJ") {
		return credentials{token: apiKey}, nil
	}

	parts := strings.SplitN(apiKey, ":", 2)
	if len(parts) == 2 && strings.TrimSpace(parts[0]) != "" && parts[1] != "" {
		return credentials{username: strings.TrimSpace(parts[0]), password: parts[1]}, nil
	}

	if !strings.Contains(apiKey, ":") {
		return credentials{token: apiKey}, nil
	}

	return credentials{}, fmt.Errorf(`pikpak: api_key must be in "username:password" or token format`)
}

func New(dc config.Debrid, ratelimits map[string]ratelimit.Limiter) (*Pikpak, error) {
	cfg := config.Get()

	creds, err := parseCredentials(dc.APIKey)
	if err != nil {
		return nil, err
	}

	username := creds.username
	password := creds.password
	token := creds.token
	if token != "" && username == "" {
		username = extractJWTSub(token)
	}

	_log := logger.New(dc.Name)

	opts := []request.ClientOption{
		request.WithRateLimiter(ratelimits["main"]),
		request.WithMaxRetries(cfg.Retries),
		request.WithRetryableStatus(http.StatusTooManyRequests, http.StatusBadGateway),
	}
	if dc.Proxy != "" {
		opts = append(opts, request.WithProxy(dc.Proxy))
	}

	autoExpiresLinksAfter, err := utils.ParseDuration(dc.AutoExpireLinksAfter)
	if autoExpiresLinksAfter == 0 || err != nil {
		autoExpiresLinksAfter = 24 * time.Hour
	}

	p := &Pikpak{
		Host:                  apiHost,
		username:              username,
		password:              password,
		client:                newAPIClient(request.New(opts...), _log, username, password, token),
		accountsManager:       account.NewManager(dc, ratelimits["download"], _log),
		autoExpiresLinksAfter: autoExpiresLinksAfter,
		logger:                _log,
		config:                dc,
	}
	// Resolve WebDAV links through the same proxy as the API. PikPak sets the
	// link's tier/region cap at resolve time, so the dav->node redirect must
	// egress from the (Asia) proxy to get the un-throttled "original" tier; the
	// bytes are then streamed direct at full speed.
	p.client.setDavProxy(dc.Proxy)
	return p, nil
}

func (p *Pikpak) Config() config.Debrid {
	return p.config
}

func (p *Pikpak) Logger() zerolog.Logger {
	return p.logger
}

func isNotFound(err error) bool {
	if ae, ok := err.(*apiError); ok {
		return ae.status == http.StatusNotFound
	}
	return false
}

func (p *Pikpak) SubmitMagnet(torrent *types.Torrent) (*types.Torrent, error) {
	if torrent.Magnet == nil || torrent.Magnet.Link == "" {
		return nil, fmt.Errorf("pikpak: missing magnet link")
	}

	name := torrent.Name
	if name == "" && torrent.Magnet != nil {
		name = torrent.Magnet.Name
	}

	ctx := context.Background()
	resp, err := p.client.addOfflineDownload(ctx, torrent.Magnet.Link, name)
	if err != nil {
		return nil, err
	}
	var fileID string
	var fileName string
	if resp.Task != nil && resp.Task.ID != "" {
		task := resp.Task
		fileName = task.FileName

		// At creation the task is in the "Collecting" phase with an empty file_id;
		// PikPak assigns the destination drive entry once it has resolved the magnet
		// metadata. Wait briefly for that id since it's the durable torrent handle.
		fileID = task.FileID
		if fileID == "" {
			resolved, name, err := p.waitForFileID(ctx, task.ID)
			if err != nil {
				return nil, err
			}
			fileID = resolved
			if name != "" {
				fileName = name
			}
		}
		if fileID == "" {
			return nil, fmt.Errorf("pikpak: offline download is still collecting metadata (task %s); try again shortly", task.ID)
		}
	} else if resp.File != nil && resp.File.ID != "" {
		fileID = resp.File.ID
		fileName = resp.File.Name
	} else {
		return nil, fmt.Errorf("pikpak: offline download returned no task or file")
	}

	torrent.Id = fileID
	// Prefer PikPak's clean file name (e.g. "Zootopia 2.mkv") for the torrent/folder
	// since it makes for tidy library names. But PikPak strips the year/quality from
	// the cached name, and without a year *arr rejects the import ("Unable to parse
	// file"). In that case fall back to the release name (the magnet's dn, as grabbed
	// by the *arr) which carries the year. The actual file keeps its PikPak name in
	// the per-file list (see collectFiles).
	folderName := folderNameFor(fileName, name)
	if folderName != "" {
		torrent.Name = folderName
		torrent.Filename = folderName
		torrent.OriginalFilename = folderName // drives the mount folder (GetFolder)
	}
	torrent.Debrid = p.config.Name
	torrent.Added = time.Now()

	return torrent, nil
}

// folderNameFor picks the folder/torrent name: the clean PikPak file name when it
// already has a parseable year, otherwise the release name (magnet dn) which does.
// The result is sanitized into a single safe path component.
func folderNameFor(fileName, releaseName string) string {
	if fileName != "" && (utils.HasYear(fileName) || strings.TrimSpace(releaseName) == "") {
		return utils.SafeFolderName(fileName)
	}
	if name := utils.SafeFolderName(releaseName); name != "" {
		return name
	}
	return utils.SafeFolderName(fileName)
}

// waitForFileID polls a freshly created offline task until PikPak assigns the
// destination drive entry id (it starts empty during the "Collecting" phase).
// Returns the file id and the resolved name, or empty strings on timeout.
func (p *Pikpak) waitForFileID(ctx context.Context, taskID string) (string, string, error) {
	const (
		attempts = 20
		interval = 2 * time.Second
	)
	for i := 0; i < attempts; i++ {
		tk, err := p.client.getTask(ctx, taskID)
		if err != nil {
			// A transient lookup error shouldn't abort the whole submit.
			p.logger.Debug().Err(err).Str("task", taskID).Msg("pikpak: task poll failed")
		} else if tk != nil {
			if tk.FileID != "" {
				return tk.FileID, tk.FileName, nil
			}
			if tk.Phase == phaseError {
				msg := tk.Message
				if msg == "" {
					msg = "offline download failed"
				}
				return "", "", fmt.Errorf("pikpak: %s", msg)
			}
		}
		select {
		case <-ctx.Done():
			return "", "", ctx.Err()
		case <-time.After(interval):
		}
	}
	return "", "", nil
}

// statusOf determines the torrent status from the download root file, falling
// back to the offline task list when the file phase is ambiguous.
func (p *Pikpak) statusOf(ctx context.Context, file *pikpakFile) (types.TorrentStatus, float64) {
	switch file.Phase {
	case phaseComplete:
		return types.TorrentStatusDownloaded, 100
	case phaseError:
		return types.TorrentStatusError, 0
	}

	if tasks, err := p.client.listOfflineTasks(ctx); err == nil {
		for _, tk := range tasks {
			if tk.FileID == file.ID {
				if tk.Phase == phaseError {
					return types.TorrentStatusError, float64(tk.Progress)
				}
				return types.TorrentStatusDownloading, float64(tk.Progress)
			}
		}
		// Not in the running/pending/error list and no completion phase yet:
		// the task has finished and dropped off the list.
		return types.TorrentStatusDownloaded, 100
	}

	if file.Phase == phaseRunning || file.Phase == phasePending {
		return types.TorrentStatusDownloading, 0
	}
	return types.TorrentStatusDownloading, 0
}

// collectFiles recursively walks a completed download folder, populating out
// with the allowed leaf files keyed by base name.
func (p *Pikpak) collectFiles(ctx context.Context, parentID, prefix string, torrentID string, cfg *config.Config, out map[string]types.File) error {
	// created tracks the source created_time per emitted (de-suffixed) name so that,
	// when PikPak holds several re-add copies ("name(1)", "name(2)"), we keep the
	// newest one. This collapse is in-memory: the entry resolves to a single clean
	// file regardless of whether PikPak has finished trashing the redundant copies.
	created := make(map[string]string)
	return p.collectFilesInto(ctx, parentID, prefix, torrentID, cfg, out, created)
}

func (p *Pikpak) collectFilesInto(ctx context.Context, parentID, prefix, torrentID string, cfg *config.Config, out map[string]types.File, created map[string]string) error {
	pageToken := ""
	for {
		fl, err := p.client.listFiles(ctx, parentID, pageToken)
		if err != nil {
			return err
		}
		for _, child := range fl.Files {
			if child == nil || child.Trashed {
				continue
			}
			relPath := child.Name
			if prefix != "" {
				relPath = prefix + "/" + child.Name
			}
			if child.Kind == kindFolder {
				if err := p.collectFilesInto(ctx, child.ID, relPath, torrentID, cfg, out, created); err != nil {
					return err
				}
				continue
			}
			if err := cfg.ValidateFileAllowed(relPath, child.Size); err != nil {
				continue
			}
			// Collapse "(N)" re-add copies to one clean entry; keep the newest. Distinct
			// files of a multi-file torrent have different names, so they don't collide.
			cleanRel := utils.NormalizeDupName(relPath)
			name := filepath.Base(cleanRel)
			if prevTime, ok := created[name]; ok && child.CreatedTime <= prevTime {
				continue
			}
			created[name] = child.CreatedTime
			out[name] = types.File{
				TorrentId: torrentID,
				Id:        child.ID,
				Name:      name,
				Size:      child.Size,
				Path:      cleanRel,
				Link:      "pikpak://" + child.ID,
			}
		}
		if fl.NextPageToken == "" {
			break
		}
		pageToken = fl.NextPageToken
	}
	return nil
}

// refresh fetches the latest state of a torrent (by PikPak file id) and rebuilds
// its file list when the download has completed.
func (p *Pikpak) refresh(ctx context.Context, t *types.Torrent) error {
	file, err := p.client.getFile(ctx, t.Id)
	if err != nil {
		if isNotFound(err) {
			// The drive entry isn't queryable yet (e.g. just submitted) or has
			// been removed. Consult the offline task list before giving up: a
			// still-running task means the download is in progress, not errored.
			if tasks, terr := p.client.listOfflineTasks(ctx); terr == nil {
				for _, tk := range tasks {
					if tk.FileID == t.Id {
						p.logger.Debug().
							Str("id", t.Id).
							Str("phase", tk.Phase).
							Int("progress", tk.Progress).
							Str("message", tk.Message).
							Msg("pikpak refresh (task fallback)")
						if tk.Phase == phaseError {
							t.Status = types.TorrentStatusError
						} else {
							t.Status = types.TorrentStatusDownloading
							t.Progress = float64(tk.Progress)
						}
						return nil
					}
				}
				p.logger.Debug().Str("id", t.Id).Int("tasks", len(tasks)).Msg("pikpak refresh: file not found and no matching task")
			} else {
				p.logger.Debug().Err(terr).Str("id", t.Id).Msg("pikpak refresh: file not found, task list failed")
			}
			t.Status = types.TorrentStatusError
			return nil
		}
		return err
	}

	// Prefer PikPak's clean file name; fall back to the release name (magnet dn)
	// when the clean name has no year, otherwise *arr rejects the import. See
	// folderNameFor. The release name is only available when the magnet was carried
	// through (the *arr add path) — on background sync / direct lookup it isn't, and
	// the clean file name is the best we have.
	releaseName := ""
	if t.Magnet != nil {
		releaseName = t.Magnet.Name
	}
	folderName := folderNameFor(file.Name, releaseName)
	t.Name = folderName
	t.Filename = folderName
	t.OriginalFilename = folderName
	t.Debrid = p.config.Name
	if file.Size > 0 {
		t.Bytes = file.Size
	}

	status, progress := p.statusOf(ctx, file)
	t.Status = status
	t.Progress = progress
	t.Files = make(map[string]types.File)

	p.logger.Debug().
		Str("id", t.Id).
		Str("kind", file.Kind).
		Str("file_phase", file.Phase).
		Str("status", string(status)).
		Float64("progress", progress).
		Msg("pikpak refresh")

	if status != types.TorrentStatusDownloaded {
		return nil
	}

	cfg := config.Get()
	if file.Kind == kindFolder {
		// Re-adding a magnet makes PikPak create "name(1)", "name(2)" siblings (it never
		// overwrites). collectFiles collapses them in-memory to a single clean entry, so
		// we do NOT trash anything here: a destructive pass races with concurrent
		// refreshes (sync, re-insertion) that each picked a different "newest" copy, so it
		// kept deleting the very copy another refresh or the live entry was using —
		// causing file_disappeared / "no files after re-insertion" / bad-marking.
		if err := p.collectFiles(ctx, file.ID, "", t.Id, cfg, t.Files); err != nil {
			return err
		}
	} else {
		if cfg.ValidateFileAllowed(file.Name, file.Size) == nil {
			// Strip any "(N)" re-add suffix so the entry name is stable across re-adds
			// (PikPak hands a different copy each time); the Link still points at this
			// specific physical file.
			clean := utils.NormalizeDupName(file.Name)
			name := filepath.Base(clean)
			t.Files[name] = types.File{
				TorrentId: t.Id,
				Id:        file.ID,
				Name:      name,
				Size:      file.Size,
				Path:      clean,
				Link:      "pikpak://" + file.ID,
			}
		}
	}
	return nil
}

func (p *Pikpak) UpdateTorrent(t *types.Torrent) error {
	return p.refresh(context.Background(), t)
}

func (p *Pikpak) GetTorrent(torrentId string) (*types.Torrent, error) {
	t := &types.Torrent{
		Id:     torrentId,
		Debrid: p.config.Name,
		Files:  make(map[string]types.File),
	}
	if err := p.refresh(context.Background(), t); err != nil {
		return nil, err
	}
	return t, nil
}

// cacheState reports whether PikPak considers the torrent's content cached.
// PikPak has no pre-submit hash check, so this inspects the freshly created
// offline task's predict_type ("3" => cached/instant, otherwise a real download).
func (p *Pikpak) cacheState(ctx context.Context, fileID string) (cached bool, known bool) {
	tasks, err := p.client.listOfflineTasks(ctx)
	if err != nil {
		return false, false
	}
	for _, tk := range tasks {
		if tk.FileID == fileID {
			c, known := tk.cached()
			pt := ""
			if tk.Params != nil {
				pt = tk.Params.PredictType
			}
			p.logger.Debug().Str("id", fileID).Str("predict_type", pt).Str("message", tk.Message).Str("phase", tk.Phase).Bool("cached", c).Bool("known", known).Msg("pikpak cacheState (from task list)")
			return c, known
		}
	}
	p.logger.Debug().Str("id", fileID).Int("tasks", len(tasks)).Msg("pikpak cacheState: no matching task")
	return false, false
}

func (p *Pikpak) CheckStatus(torrent *types.Torrent) (*types.Torrent, error) {
	ctx := context.Background()

	// PikPak offline downloads start in a "Collecting"/"Saving" phase for both
	// cached and uncached content, so we can't decide on the very first check.
	// Wait briefly: cached content either completes quickly or is flagged via
	// predict_type, while uncached content keeps "Collecting".
	start := time.Now()
	deadline := start.Add(cacheCheckWindow)
	graceDeadline := start.Add(cacheCheckGrace)
	for {
		if err := p.refresh(ctx, torrent); err != nil {
			return torrent, err
		}

		switch torrent.Status {
		case types.TorrentStatusDownloaded:
			p.logger.Info().Msgf("Torrent: %s downloaded", torrent.Name)
			return torrent, nil
		case types.TorrentStatusError:
			return torrent, fmt.Errorf("torrent: %s has error", torrent.Name)
		}

		// Still downloading. If the caller allows uncached downloads, accept it
		// now and let it finish in the background.
		if torrent.DownloadUncached {
			return torrent, nil
		}

		// Cache gating (download_uncached disabled). PikPak flags cached content
		// with predict_type "3" shortly after submission, but not instantly — so
		// we only accept on a *confirmed* cached signal (or actual completion,
		// handled above) and never reject early. Uncached content never produces
		// that signal and is rejected once the window elapses, matching the
		// RealDebrid/TorBox "only if cached" behaviour.
		cached, known := p.cacheState(ctx, torrent.Id)
		if cached {
			p.logger.Info().Msgf("Torrent: %s is cached on PikPak", torrent.Name)
			return torrent, nil
		}

		now := time.Now()
		// After the grace period, a confirmed "not cached" verdict is trusted.
		if known && now.After(graceDeadline) {
			return torrent, fmt.Errorf("torrent: %s not cached", torrent.Name)
		}
		// Safety cap if predict_type never resolves.
		if now.After(deadline) {
			return torrent, fmt.Errorf("torrent: %s not cached", torrent.Name)
		}
		time.Sleep(cacheCheckInterval)
	}
}

func (p *Pikpak) DeleteTorrent(torrentId string) error {
	ctx := context.Background()

	// Best effort: cancel a still-running offline task for this file.
	if tasks, err := p.client.listOfflineTasks(ctx); err == nil {
		for _, tk := range tasks {
			if tk.FileID == torrentId {
				_ = p.client.deleteTask(ctx, tk.ID, true)
			}
		}
	}

	if err := p.client.batchDelete(ctx, []string{torrentId}); err != nil {
		if isNotFound(err) {
			return nil
		}
		return err
	}
	p.logger.Info().Msgf("Torrent %s deleted from PikPak", torrentId)
	return nil
}

// linkExpiry derives the expiry of a signed PikPak URL from its "expire" query
// parameter, falling back to the configured auto-expiry window.
func linkExpiry(rawurl string, fallback time.Duration) time.Time {
	now := time.Now()
	if u, err := url.Parse(rawurl); err == nil {
		if e := u.Query().Get("expire"); e != "" {
			if secs, perr := strconv.ParseInt(e, 10, 64); perr == nil && secs > 0 {
				return time.Unix(secs, 0)
			}
		}
	}
	return now.Add(fallback)
}

// linkSpeedCap returns the per-connection speed ceiling (bytes/sec) PikPak
// stamps into a signed dl-* URL via the `ms`/`th` query params. The same file
// is handed different caps at different times (~6 MB/s throttled tier vs ~30-50
// MB/s "original" tier); reading the cap lets us tell which link we got without
// downloading. Returns 0 when neither param is present (e.g. the raw dav host
// URL, which carries no cap until it redirects to a node).
func linkSpeedCap(rawurl string) int64 {
	u, err := url.Parse(rawurl)
	if err != nil {
		return 0
	}
	q := u.Query()
	for _, k := range []string{"ms", "th"} {
		if v := q.Get(k); v != "" {
			if n, perr := strconv.ParseInt(v, 10, 64); perr == nil && n > 0 {
				return n
			}
		}
	}
	return 0
}

func (p *Pikpak) GetDownloadLink(ctx context.Context, torrentID string, file *types.File) (types.DownloadLink, error) {
	return p.accountsManager.GetDownloadLink(ctx, torrentID, file, p.fetchDownloadLink)
}

func (p *Pikpak) fetchDownloadLink(ctx context.Context, acc *account.Account, id string, file *types.File) (types.DownloadLink, error) {
	if ctx == nil {
		ctx = context.Background()
	}

	// A freshly-completed offline download — especially a cached one — reports
	// "downloaded" before its drive file is fully resolvable: getFile can briefly
	// 404 and web_content_link can be empty for a few seconds. The post-download
	// precache ffprobe reads the file in exactly that window; if the link fetch
	// failed there, it escalated to a destructive re-insertion that marked the
	// entry bad. Retry briefly to ride out that materialization window. A hard
	// (non-not-found) error is returned immediately rather than spun on.
	const attempts = 5
	var lastErr error
	for attempt := 0; attempt < attempts; attempt++ {
		f, err := p.client.getFile(ctx, file.Id)
		if err != nil {
			if !isNotFound(err) {
				return types.DownloadLink{}, err
			}
			lastErr = err
		} else if dl, ok := p.buildDownloadLink(ctx, acc, file, f); ok {
			return dl, nil
		} else {
			lastErr = fmt.Errorf("pikpak: no download url available for %s", file.Name)
		}

		if attempt < attempts-1 {
			p.logger.Debug().
				Str("file", file.Name).
				Int("attempt", attempt+1).
				Msg("pikpak: download link not ready, retrying")
			time.Sleep(2 * time.Second)
		}
	}
	return types.DownloadLink{}, lastErr
}

// directDownloadURL returns PikPak's direct /download/ link (octet-stream, else
// web_content_link) — a per-node URL. Empty when neither is a valid URL yet
// (freshly materialized files can lack it for a few seconds).
func directDownloadURL(f *pikpakFile) string {
	if f.Links != nil && f.Links.ApplicationOctetStream != nil && utils.IsValidURL(f.Links.ApplicationOctetStream.URL) {
		return f.Links.ApplicationOctetStream.URL
	}
	if utils.IsValidURL(f.WebContentLink) {
		return f.WebContentLink
	}
	return ""
}

// buildDownloadLink resolves a streamable URL for f and returns the link. ok is
// false when no URL is available yet (caller may retry).
func (p *Pikpak) buildDownloadLink(ctx context.Context, acc *account.Account, file *types.File, f *pikpakFile) (types.DownloadLink, bool) {
	// PikPak decides a link's region cap at RESOLVE time, and the two link variants
	// then want OPPOSITE fetch egresses (measured Brazil<->Singapore, July 2026):
	//   - the WebDAV-resolved ".net" original tier (resolved via the proxy) is
	//     region-bypassed and fetches FASTEST DIRECT (~10 MB/s); a proxy hop only
	//     slows it (the relay itself becomes the bottleneck).
	//   - the API ".com" web link is region-throttled to ~200 KB/s unless fetched
	//     THROUGH the proxy, which lifts the region cap (up to its ms).
	//
	// So when proxied we ALWAYS resolve WebDAV and prefer it — we must NOT trust the
	// ".com" link's ms, because that cap is a lie when the link is fetched direct
	// (region-throttled regardless of the stamped ms). Each chosen link is tagged
	// with the egress it needs (streamViaProxy): the ".net" link streams direct, the
	// ".com" fallback streams through the proxy. We only fall back to ".com" when
	// WebDAV is unavailable or actually slower (an x_limited ".net" node) — and that
	// fallback goes through the proxy so it isn't region-capped.
	target := ""
	streamViaProxy := false
	if p.config.Proxy != "" {
		direct := directDownloadURL(f)                      // ".com" web tier
		directCap := linkSpeedCap(direct)
		resolved, rerr := p.client.resolveWebDAVURL(ctx, f) // ".net" original, resolved via proxy
		switch {
		case rerr == nil && utils.IsValidURL(resolved) && (linkSpeedCap(resolved) >= directCap || direct == ""):
			target, streamViaProxy = resolved, false // .net original: fetch DIRECT (fast, region-bypassed)
		case direct != "":
			target, streamViaProxy = direct, true // .com fallback: fetch via PROXY to dodge the ~200 KB/s region cap
		}
	}
	if target == "" {
		davURL, err := p.client.webdavURL(ctx, f)
		if err != nil || !utils.IsValidURL(davURL) {
			p.logger.Debug().
				Err(err).
				Str("file", file.Name).
				Bool("has_links", f.Links != nil).
				Msg("pikpak: no resolvable download url")
			return types.DownloadLink{}, false
		}
		target = davURL
		// The raw dav host redirects to a node; fetched direct it can be region-
		// capped, so stream it through the proxy when one exists.
		streamViaProxy = p.config.Proxy != ""
	}

	if u, perr := url.Parse(target); perr == nil {
		// x_limited=1 means PikPak has applied a per-content rate cap: ms (the
		// applied ceiling) drops far below th (the file's nominal tier), and NO
		// resolution path — direct, octet-stream or WebDAV — dodges it. Surface it so
		// a 100 KB/s file is recognizable as a limited-content case, not a bug.
		limited := u.Query().Get("x_limited") == "1"
		p.logger.Debug().
			Str("file", file.Name).
			Str("host", u.Host).
			Bool("proxied", p.config.Proxy != "").
			Bool("stream_via_proxy", streamViaProxy).
			Int64("speed_cap_bps", linkSpeedCap(target)).
			Bool("limited", limited).
			Msg("pikpak resolved download link")
	}

	size := file.Size
	if size == 0 {
		size = f.Size
	}

	now := time.Now()
	return types.DownloadLink{
		Filename:     file.Name,
		Size:         size,
		Token:        acc.Token,
		Link:         file.Link,
		DownloadLink: target,
		Debrid:       p.config.Name,
		Id:           file.Id,
		Generated:    now,
		ExpiresAt:    linkExpiry(target, p.autoExpiresLinksAfter),
		// The per-connection ms/th cap PikPak stamps into the link — lets the mount
		// skip parallel reads when one connection already clears the target.
		SpeedCapBps: linkSpeedCap(target),
		// Fetch this link through the provider proxy (true) or direct (false); see
		// the egress reasoning above.
		StreamViaProxy: streamViaProxy,
	}, true
}

// GetTorrents lists the account's offline-download content so decypharr can sync
// it (import new items, detect removals). PikPak keeps the original magnet in
// each item's params.url, which gives us the real infohash decypharr reconciles
// on. The list endpoint returns params, so this stays cheap (no per-file calls).
func (p *Pikpak) GetTorrents() ([]*types.Torrent, error) {
	ctx := context.Background()
	seen := make(map[string]*types.Torrent)
	// Fail hard on any walk error: returning a partial list would make the
	// reconciler treat the missing entries as removed and delete them.
	if err := p.collectTorrents(ctx, "", 0, seen); err != nil {
		return nil, err
	}
	result := make([]*types.Torrent, 0, len(seen))
	for _, t := range seen {
		result = append(result, t)
	}
	return result, nil
}

// collectTorrents walks the drive, treating any item with a magnet source
// (params.url) as a torrent (keyed by infohash, newest copy wins) and recursing
// into plain container folders ("My Pack", user folders) up to a bounded depth.
func (p *Pikpak) collectTorrents(ctx context.Context, parentID string, depth int, seen map[string]*types.Torrent) error {
	if depth > maxSyncDepth {
		return nil
	}
	pageToken := ""
	for {
		fl, err := p.client.listFiles(ctx, parentID, pageToken)
		if err != nil {
			return err
		}
		for _, c := range fl.Files {
			if c == nil || c.Trashed {
				continue
			}
			if infohash := c.sourceInfohash(); infohash != "" {
				t := p.torrentFromFile(c, infohash)
				if old, ok := seen[infohash]; !ok || t.Added.After(old.Added) {
					seen[infohash] = t
				}
				continue // don't descend into a torrent's own files
			}
			if c.Kind == kindFolder {
				if err := p.collectTorrents(ctx, c.ID, depth+1, seen); err != nil {
					return err
				}
			}
		}
		if fl.NextPageToken == "" {
			break
		}
		pageToken = fl.NextPageToken
	}
	return nil
}

// torrentFromFile builds a lightweight torrent for sync. Files are left empty;
// processSyncTorrent calls UpdateTorrent to populate them only when needed.
func (p *Pikpak) torrentFromFile(c *pikpakFile, infohash string) *types.Torrent {
	status := types.TorrentStatusDownloading
	switch c.Phase {
	case phaseComplete:
		status = types.TorrentStatusDownloaded
	case phaseError:
		status = types.TorrentStatusError
	}

	magnet, err := utils.GetMagnetInfo(c.Params.URL, false)
	if err != nil || magnet == nil {
		magnet = utils.ConstructMagnet(infohash, c.Name)
	}

	return &types.Torrent{
		Id:               c.ID,
		InfoHash:         infohash,
		Name:             c.Name,
		OriginalFilename: c.Name,
		Status:           status,
		Added:            parsePikpakTime(c.CreatedTime),
		Debrid:           p.config.Name,
		Magnet:           magnet,
		Files:            make(map[string]types.File),
	}
}

// parsePikpakTime parses PikPak's RFC3339(+ms) timestamps, defaulting to now.
func parsePikpakTime(s string) time.Time {
	if s != "" {
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t
		}
	}
	return time.Now()
}

func (p *Pikpak) fetchDownloadLinks(acc *account.Account) ([]types.DownloadLink, error) {
	return []types.DownloadLink{}, nil
}

func (p *Pikpak) RefreshDownloadLinks() error {
	return p.accountsManager.RefreshLinks(p.fetchDownloadLinks)
}

// IsAvailable has no equivalent on PikPak (there is no public cached-hash check).
func (p *Pikpak) IsAvailable(infohashes []string) (map[string]bool, error) {
	return map[string]bool{}, nil
}

// SupportsCheck reports false: PikPak exposes no per-file availability probe, so
// the repair sweep relies on download-time link validation instead.
func (p *Pikpak) SupportsCheck() bool {
	return false
}

func (p *Pikpak) CheckFile(ctx context.Context, infohash, fileID string) error {
	return nil
}

func (p *Pikpak) AccountManager() *account.Manager {
	return p.accountsManager
}

// StorageQuota returns the account's used and total stored bytes. Implements the
// common.QuotaReporter capability so the eviction sweep can free space on PikPak.
func (p *Pikpak) StorageQuota(ctx context.Context) (used, limit int64, err error) {
	about, err := p.client.getAbout(ctx)
	if err != nil {
		return 0, 0, err
	}
	return about.Quota.Usage, about.Quota.Limit, nil
}

func (p *Pikpak) GetProfile() (*types.Profile, error) {
	p.profileMu.Lock()
	defer p.profileMu.Unlock()
	if p.profile != nil {
		return p.profile, nil
	}

	ctx := context.Background()
	if _, err := p.client.getAbout(ctx); err != nil {
		return nil, err
	}

	profile := &types.Profile{
		Name:     p.config.Name,
		Username: p.username,
		Type:     "free",
	}

	if profile.Username == "" {
		profile.Username = p.client.getUserID()
	}

	if info, err := p.client.getUserInfo(ctx); err == nil && info != nil {
		profile.Email = info.Email
		if info.Email != "" {
			profile.Username = info.Email
		} else if info.Name != "" && profile.Username == "" {
			profile.Username = info.Name
		}
	}

	if vip, err := p.client.getVIPInfo(ctx); err == nil && vip != nil && vip.Result == "ACCEPTED" {
		if vip.Data.Type != "" {
			profile.Type = vip.Data.Type
		} else {
			profile.Type = "premium"
		}
		if exp, perr := time.Parse(time.RFC3339, vip.Data.Expire); perr == nil {
			profile.Expiration = exp
		}
	}

	p.profile = profile
	return profile, nil
}

func (p *Pikpak) GetAvailableSlots() (int, error) {
	// PikPak does not expose the concurrent offline-download limit; use a
	// conservative default that won't block scheduling.
	return 5, nil
}

func (p *Pikpak) syncAccount(acc *account.Account) error {
	acc.Username = p.username
	if profile, err := p.GetProfile(); err == nil && profile != nil {
		if profile.Username != "" {
			acc.Username = profile.Username
		}
		if !profile.Expiration.IsZero() {
			acc.Expiration = profile.Expiration
		}
	}
	return nil
}

func (p *Pikpak) SyncAccounts() {
	p.accountsManager.Sync(p.syncAccount)
}

func (p *Pikpak) deleteDownloadLink(acc *account.Account, downloadLink types.DownloadLink) error {
	return nil
}

func (p *Pikpak) DeleteLink(downloadLink types.DownloadLink) error {
	return p.accountsManager.DeleteDownloadLink(downloadLink, p.deleteDownloadLink)
}

// SpeedTest measures API latency and download speed using a cached link.
func (p *Pikpak) SpeedTest(ctx context.Context) types.SpeedTestResult {
	result := types.SpeedTestResult{
		Provider: p.config.Name,
		TestedAt: time.Now(),
	}

	start := time.Now()
	_, err := p.client.getAbout(ctx)
	latency := time.Since(start)
	if err != nil {
		result.Error = fmt.Sprintf("latency test failed: %v", err)
		return result
	}
	result.LatencyMs = latency.Milliseconds()

	current := p.accountsManager.Current()
	if current == nil {
		return result
	}

	link, found := current.GetRandomLink()
	if !found || link.DownloadLink == "" {
		return result
	}

	const downloadSize = 1 * 1024 * 1024 // 1MB
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, link.DownloadLink, nil)
	if err != nil {
		return result
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=0-%d", downloadSize-1))

	downloadStart := time.Now()
	dlResp, err := current.Client().Do(req)
	if err != nil {
		return result
	}
	defer dlResp.Body.Close()

	data, err := io.ReadAll(dlResp.Body)
	downloadDuration := time.Since(downloadStart)
	if err != nil || len(data) == 0 {
		return result
	}

	result.BytesRead = int64(len(data))
	if downloadDuration.Seconds() > 0 {
		result.SpeedMBps = float64(result.BytesRead) / downloadDuration.Seconds() / (1024 * 1024)
	}

	return result
}
