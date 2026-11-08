package manager

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// RebuildResult is the per-folder outcome of a rebuild-from-history pass.
type RebuildResult struct {
	Folder   string `json:"folder"`
	Arr      string `json:"arr"`
	InfoHash string `json:"infohash,omitempty"`
	Status   string `json:"status"` // rebuilt | would_rebuild | no_history | not_torrent | exists | failed
	Error    string `json:"error,omitempty"`
}

// RebuildReport summarizes a rebuild-from-history pass. The top-level counts
// reconcile as: ArrFilesFound = Local + Unresolved + Scanned, and
// Scanned = Rebuilt + WouldRebuild + Failed + NoHistory + NotTorrent + AlreadyHave.
type RebuildReport struct {
	DryRun bool `json:"dry_run"`

	// Accounting for where every *arr file went (so a low Scanned is explainable).
	ArrFilesFound int `json:"arr_files_found"` // media items the arr returned WITH a file
	Local         int `json:"local"`           // symlink resolved to an entry already present locally
	Unresolved    int `json:"unresolved"`      // had a file but no symlink resolved on this host (path mismatch / not a decypharr symlink)

	Scanned      int `json:"scanned"`       // lost folders examined (candidates)
	Rebuilt      int `json:"rebuilt"`       //
	WouldRebuild int `json:"would_rebuild"` // dry-run only: recoverable, not rebuilt
	Skipped      int `json:"skipped"`       // = NoHistory + NotTorrent + AlreadyHave
	Failed       int `json:"failed"`        //

	// Skip breakdown.
	NoHistory   int `json:"no_history"`   // no torrent grab in arr history (trimmed / manual import / usenet)
	NotTorrent  int `json:"not_torrent"`  // grab downloadId wasn't a torrent infohash
	AlreadyHave int `json:"already_have"` // infohash already known locally

	Results []RebuildResult `json:"results"`
}

// rebuildCandidate is a lost folder plus what's needed to rebuild it: a media id
// for the history lookup and the files the *arr expects inside it.
type rebuildCandidate struct {
	mediaID int
	files   []arr.ContentFile
}

// RebuildFromArrHistory recovers entries that an *arr still references (via a
// symlink) but that no longer exist in local storage — e.g. after a wiped DB or
// a PikPak account switch that pruned them. For each such symlink it reads the
// release's most-recent grab from the *arr history to recover the torrent
// infohash, then rebuilds a local entry so the mount path (and the symlink)
// resolves again.
//
// The rebuilt entry is created in the *orphaned* state (nothing is added to the
// provider): the file metadata comes from the *arr, and the torrent is re-added
// to the provider lazily on the first stream — same as an evicted file. So a
// rebuild is cheap and never touches the debrid.
//
// arrName scopes the pass to one *arr; empty scans every eligible Radarr/Sonarr.
// When dryRun is set it resolves and validates recovery candidates but writes
// nothing, reporting each as "would_rebuild". It never deletes anything and
// skips media it can't recover, so it's safe to re-run.
func (m *Manager) RebuildFromArrHistory(ctx context.Context, arrName string, dryRun bool) (*RebuildReport, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	arrs := m.rebuildArrs(arrName)
	if len(arrs) == 0 {
		return nil, fmt.Errorf("no eligible radarr/sonarr configured")
	}

	report := &RebuildReport{DryRun: dryRun, Results: make([]RebuildResult, 0)}
	// Dedup within a run (a season pack is one infohash across many files/folders).
	seenHash := make(map[string]bool)

	for _, a := range arrs {
		if ctx.Err() != nil {
			break
		}
		media, err := m.arr.Media(ctx, a.Name, "")
		if err != nil {
			m.logger.Warn().Err(err).Str("arr", a.Name).Msg("rebuild: Media failed; skipping arr")
			continue
		}
		provider := m.rebuildProvider(a)
		report.ArrFilesFound += len(media)

		// Collect folders whose *arr symlink no longer resolves to a local entry.
		candidates := make(map[string]*rebuildCandidate)
		seenFolder := make(map[string]bool)
		for _, content := range media {
			grouped := collectArrFiles(content)
			if len(grouped) == 0 {
				// The arr says this item has a file, but none of its paths resolve to
				// a decypharr symlink on THIS host — a path mismatch between the arr
				// and decypharr, or the file isn't a decypharr symlink. Can't recover
				// what we can't locate, so surface it rather than dropping it silently.
				report.Unresolved++
				continue
			}
			for entryPath, files := range grouped {
				folder := filepath.Clean(filepath.Base(entryPath))
				if folder == "" || folder == "." {
					continue
				}
				if seenFolder[folder] {
					if c, ok := candidates[folder]; ok {
						c.files = append(c.files, files...)
					}
					continue
				}
				seenFolder[folder] = true
				// A live local entry (item) means nothing to rebuild — Slice A's
				// orphan re-hydration or the repair sweep already covers those.
				if _, err := m.GetEntryItem(folder); err == nil {
					report.Local++
					continue
				}
				mediaID := content.Id // Radarr: movie id
				if a.Type == arr.Sonarr {
					for _, f := range files {
						if f.EpisodeId != 0 {
							mediaID = f.EpisodeId
							break
						}
					}
				}
				candidates[folder] = &rebuildCandidate{mediaID: mediaID, files: files}
			}
		}

		for folder, c := range candidates {
			if ctx.Err() != nil {
				break
			}
			report.Scanned++
			res := m.rebuildOne(ctx, a, provider, folder, c, seenHash, dryRun)
			switch res.Status {
			case "rebuilt":
				report.Rebuilt++
			case "would_rebuild":
				report.WouldRebuild++
			case "failed":
				report.Failed++
			case "no_history":
				report.NoHistory++
				report.Skipped++
			case "not_torrent":
				report.NotTorrent++
				report.Skipped++
			case "exists":
				report.AlreadyHave++
				report.Skipped++
			default:
				report.Skipped++
			}
			report.Results = append(report.Results, res)
		}
	}
	return report, nil
}

// rebuildOne resolves one lost folder to an infohash via *arr grab history and
// rebuilds it as an orphaned entry (unless dryRun). seenHash dedups infohashes
// handled earlier in the same run.
func (m *Manager) rebuildOne(ctx context.Context, a arr.Arr, provider, folder string, c *rebuildCandidate, seenHash map[string]bool, dryRun bool) RebuildResult {
	res := RebuildResult{Folder: folder, Arr: a.Name}

	_, downloadID, herr := m.arr.LatestGrabID(ctx, a.Name, c.mediaID)
	if herr != nil || downloadID == "" {
		res.Status = "no_history"
		if herr != nil {
			res.Error = herr.Error()
		}
		return res
	}

	// Radarr/Sonarr store the download client's hash as downloadId; for a torrent
	// grabbed through decypharr's qbit shim that's the (upper-cased) infohash. A
	// usenet or other-client grab won't be a 40-hex hash — skip those cleanly
	// rather than treating a non-torrent id as an infohash.
	infohash := strings.ToLower(strings.TrimSpace(downloadID))
	if !looksLikeInfohash(infohash) {
		res.Status = "not_torrent"
		res.Error = "grab downloadId is not a torrent infohash"
		return res
	}
	res.InfoHash = infohash

	if seenHash[infohash] {
		res.Status = "exists"
		return res
	}
	if ok, _ := m.EntryExists(infohash); ok {
		seenHash[infohash] = true
		res.Status = "exists"
		return res
	}
	if provider == "" {
		res.Status = "failed"
		res.Error = "no debrid provider available for this arr"
		return res
	}

	// Dry run: candidate is recoverable (lost locally, torrent grab in history,
	// a provider to re-hydrate onto) but we write nothing.
	if dryRun {
		seenHash[infohash] = true
		res.Status = "would_rebuild"
		return res
	}

	entry := buildOrphanEntry(provider, infohash, folder, c.files)
	if len(entry.Files) == 0 {
		res.Status = "failed"
		res.Error = "no files resolved from arr symlinks"
		return res
	}
	if err := m.AddOrUpdate(entry, func(t *storage.Entry) { m.RefreshEntries(true) }); err != nil {
		res.Status = "failed"
		res.Error = err.Error()
		m.logger.Warn().Err(err).Str("arr", a.Name).Str("folder", folder).Str("infohash", infohash).Msg("rebuild: failed to persist rebuilt entry")
		return res
	}
	seenHash[infohash] = true
	res.Status = "rebuilt"
	m.logger.Info().Str("arr", a.Name).Str("folder", folder).Str("infohash", infohash).Msg("rebuild: rebuilt orphan entry from arr history (re-adds to provider on first stream)")
	return res
}

// buildOrphanEntry constructs an orphaned (not-yet-on-provider) entry from an
// infohash and the files the *arr expects. folder becomes both Name and
// OriginalFilename so GetFolder() reproduces the original mount folder under
// every naming scheme; the placement is marked RemovedAt so the first read
// re-adds the magnet to the provider (the same lazy path an evicted file takes).
func buildOrphanEntry(provider, infohash, folder string, files []arr.ContentFile) *storage.Entry {
	now := time.Now()
	entry := &storage.Entry{
		Protocol:         config.ProtocolTorrent,
		InfoHash:         infohash,
		Name:             folder,
		OriginalFilename: folder,
		ActiveProvider:   provider,
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Status:           debridTypes.TorrentStatusDownloaded,
		IsComplete:       true,
		AddedOn:          now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if magnet := utils.ConstructMagnet(infohash, folder); magnet != nil {
		entry.Magnet = magnet.Link
	}

	// Placement is a stub marked evicted: no real provider ID/links yet. The
	// eviction re-hydrate path fills those in (fresh ID + real files) on the first
	// read and clears RemovedAt.
	pe := &storage.ProviderEntry{
		Provider:  provider,
		Status:    debridTypes.TorrentStatusDownloaded,
		RemovedAt: &now,
		Files:     make(map[string]*storage.ProviderFile),
		AddedAt:   now,
	}

	var size int64
	for _, f := range files {
		name := f.TargetPath // symlink-target base — the on-disk file name
		if name == "" {
			continue
		}
		if _, ok := entry.Files[name]; ok {
			continue
		}
		entry.Files[name] = &storage.File{
			Name:     name,
			Size:     f.Size,
			Deleted:  false,
			InfoHash: infohash,
			AddedOn:  now,
		}
		pe.Files[name] = &storage.ProviderFile{}
		size += f.Size
	}
	entry.Size = size
	entry.Bytes = size
	entry.Providers[provider] = pe
	return entry
}

// rebuildArrs returns the eligible Radarr/Sonarr instances for a rebuild pass.
// arrName scopes to one; empty returns all configured Radarr/Sonarr.
func (m *Manager) rebuildArrs(arrName string) []arr.Arr {
	if arrName != "" {
		if a, ok := m.arr.Get(arrName); ok && (a.Type == arr.Radarr || a.Type == arr.Sonarr) {
			return []arr.Arr{a}
		}
		return nil
	}
	var out []arr.Arr
	for _, a := range m.arr.All() {
		// Only Radarr/Sonarr expose the movie/episode + grab-history model used here.
		if a.Type != arr.Radarr && a.Type != arr.Sonarr {
			continue
		}
		out = append(out, a)
	}
	return out
}

// rebuildProvider picks the debrid the rebuilt entry will re-hydrate onto: the
// arr's selected debrid when it has a live client, else the first configured
// debrid with one. Returns "" when no provider client is available.
func (m *Manager) rebuildProvider(a arr.Arr) string {
	if a.SelectedDebrid != "" && m.ProviderClient(a.SelectedDebrid) != nil {
		return a.SelectedDebrid
	}
	for _, d := range m.config.Debrids {
		if m.ProviderClient(d.Name) != nil {
			return d.Name
		}
	}
	return ""
}

// looksLikeInfohash reports whether s is a 40-character hex string (a BitTorrent
// v1 infohash), so non-torrent download ids (usenet, other clients) are skipped.
func looksLikeInfohash(s string) bool {
	if len(s) != 40 {
		return false
	}
	for _, c := range s {
		switch {
		case c >= '0' && c <= '9', c >= 'a' && c <= 'f':
		default:
			return false
		}
	}
	return true
}
