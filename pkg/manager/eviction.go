package manager

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// entryFileSize sums the sizes of an entry's live files. Folder torrents report
// entry.Size == 0, so eviction accounting must use the files' total instead.
func entryFileSize(e *storage.Entry) int64 {
	var s int64
	for _, f := range e.Files {
		if f != nil && !f.Deleted {
			s += f.Size
		}
	}
	return s
}

// rehydrateCall coalesces concurrent re-hydrations of the same evicted entry so
// that simultaneous reads trigger a single re-add.
type rehydrateCall struct {
	done chan struct{}
	err  error
}

const (
	// rehydrateBurstWindow / rehydrateBurstLimit rate-limit distinct re-adds: at
	// most rehydrateBurstLimit within any rehydrateBurstWindow. A user pressing
	// play on one title is a single re-add (its reads coalesce); a library scan
	// touching many evicted/orphaned files at once trips the limit, and the excess
	// re-adds are suppressed so the provider isn't stampeded.
	rehydrateBurstWindow = 10 * time.Second
	rehydrateBurstLimit  = 4
)

// errRehydrateThrottled is returned when a re-add is suppressed because too many
// distinct re-hydrations fired at once (a scan). It's retryable: once the burst
// subsides the next read re-adds normally.
var errRehydrateThrottled = fmt.Errorf("re-hydration throttled: many files requested at once (scan?), retry shortly")

// rehydrateThrottle caps the number of distinct re-adds started within a rolling
// window. It records only ADMITTED re-adds, so a scan that keeps hammering
// blocked files can't leak past the limit.
type rehydrateThrottle struct {
	mu     sync.Mutex
	starts []int64 // unix-nano of admitted re-adds within the window
	window time.Duration
	limit  int
}

func newRehydrateThrottle(window time.Duration, limit int) *rehydrateThrottle {
	return &rehydrateThrottle{window: window, limit: limit}
}

// admit records a re-add attempt at now and reports whether it may proceed.
// Returns false once the window already holds `limit` admitted re-adds — the
// caller should then skip the re-add (treat the read as temporarily unavailable).
func (t *rehydrateThrottle) admit(now time.Time) bool {
	if t == nil || t.limit <= 0 {
		return true
	}
	t.mu.Lock()
	defer t.mu.Unlock()

	cutoff := now.Add(-t.window).UnixNano()
	kept := t.starts[:0]
	for _, ts := range t.starts {
		if ts >= cutoff {
			kept = append(kept, ts)
		}
	}
	t.starts = kept

	if len(t.starts) >= t.limit {
		return false
	}
	t.starts = append(t.starts, now.UnixNano())
	return true
}

// evictionCandidate is an entry eligible for eviction, with the bytes it frees.
type evictionCandidate struct {
	infohash string
	addedOn  time.Time
	size     int64
}

// runEviction frees remote storage on a quota-limited provider when usage crosses
// the configured high-water mark, evicting oldest-added torrents first down to the
// low-water mark. Evicted torrents keep their local metadata + magnet so the mount
// path still resolves and the file is re-added lazily on the next read.
func (m *Manager) runEviction(ctx context.Context, cfg config.Debrid, client debrid.Client) {
	if !cfg.Eviction.Enabled {
		return
	}
	// Never let two sweeps overlap: a sweep can take longer than check_interval
	// (hundreds of entries × slow remote reads), and concurrent sweeps would evict
	// the same entries and collide on the probe-cache temp files.
	if !m.evictionRunning.CompareAndSwap(false, true) {
		m.logger.Debug().Str("debrid", cfg.Name).Msg("eviction: previous sweep still running, skipping")
		return
	}
	defer m.evictionRunning.Store(false)

	if cfg.Eviction.Archive {
		m.runArchive(ctx, cfg, client)
		return
	}
	reporter, ok := client.(debrid.QuotaReporter)
	if !ok {
		return // provider exposes no finite storage quota
	}

	used, limit, err := reporter.StorageQuota(ctx)
	if err != nil {
		m.logger.Error().Err(err).Str("debrid", cfg.Name).Msg("eviction: failed to read storage quota")
		return
	}
	if limit <= 0 {
		return // unlimited / unknown
	}

	usedPct := float64(used) / float64(limit) * 100
	if usedPct < float64(cfg.Eviction.HighWaterPercent) {
		return // below high-water mark; nothing to do
	}

	// Free down to the low-water mark.
	targetUsed := int64(float64(limit) * float64(cfg.Eviction.LowWaterPercent) / 100)
	toFree := used - targetUsed
	if toFree <= 0 {
		return
	}

	m.logger.Info().
		Str("debrid", cfg.Name).
		Float64("used_pct", usedPct).
		Int64("to_free_bytes", toFree).
		Msg("eviction: usage over high-water mark, selecting victims")

	// Never evict a file that's being watched right now.
	streaming := make(map[string]bool)
	for _, s := range m.GetActiveStreams() {
		streaming[s.EntryName] = true
	}

	// Collect candidates: completed torrents whose active placement is on this
	// provider and not already evicted.
	candidates := make([]evictionCandidate, 0, 256)
	_ = m.storage.ForEachBatch(refreshBatchSize, func(batch []*storage.Entry) error {
		for _, entry := range batch {
			if entry == nil || entry.Bad || !entry.IsTorrent() {
				continue
			}
			if streaming[entry.Name] {
				continue
			}
			pe, ok := entry.Providers[cfg.Name]
			if !ok || pe == nil || pe.ID == "" || pe.RemovedAt != nil {
				continue
			}
			if pe.Status != debridTypes.TorrentStatusDownloaded {
				continue
			}
			// Skip entries with unknown size: we can't account for the space they'd
			// free, and counting them as 0 would make `freed` never reach the target
			// and over-evict the whole library. Use the files' total (folder
			// torrents report entry.Size == 0).
			fsize := entryFileSize(entry)
			if fsize <= 0 {
				continue
			}
			candidates = append(candidates, evictionCandidate{
				infohash: entry.InfoHash,
				addedOn:  entry.AddedOn,
				size:     fsize,
			})
		}
		return nil
	})

	// Oldest added first.
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].addedOn.Before(candidates[j].addedOn)
	})

	var freed int64
	evicted := 0
	for _, c := range candidates {
		if freed >= toFree {
			break
		}
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := m.evictEntry(ctx, cfg, client, c.infohash); err != nil {
			m.logger.Warn().Err(err).Str("infohash", c.infohash).Msg("eviction: failed to evict entry")
			continue
		}
		freed += c.size
		evicted++
	}

	m.logger.Info().
		Str("debrid", cfg.Name).
		Int("evicted", evicted).
		Int64("freed_bytes", freed).
		Msg("eviction: sweep complete")
}

// runArchive is the archive-mode sweep: evict EVERY downloaded torrent down to
// its probe slices (regardless of water marks), then drip-feed queued magnets
// while usage is below the feed target. This keeps a library far larger than the
// quota resident only as probe slices, re-hydrated on play.
func (m *Manager) runArchive(ctx context.Context, cfg config.Debrid, client debrid.Client) {
	// Never evict a file being watched right now.
	streaming := make(map[string]bool)
	for _, s := range m.GetActiveStreams() {
		streaming[s.EntryName] = true
	}

	// Keep titles streamed within the hot window resident (so you can actually
	// watch / resume them without the sweep yanking them back mid-session).
	var hotCutoff int64
	if hw, err := utils.ParseDuration(cfg.Eviction.HotWindow); err == nil && hw > 0 {
		hotCutoff = time.Now().Unix() - int64(hw.Seconds())
	}

	var victims []string
	_ = m.storage.ForEachBatch(refreshBatchSize, func(batch []*storage.Entry) error {
		for _, entry := range batch {
			if entry == nil || entry.Bad || !entry.IsTorrent() || streaming[entry.Name] {
				continue
			}
			if hotCutoff > 0 {
				if ts, ok := m.lastStreamed.Load(entry.InfoHash); ok && ts >= hotCutoff {
					continue // streamed recently — keep resident
				}
			}
			pe, ok := entry.Providers[cfg.Name]
			if !ok || pe == nil || pe.ID == "" || pe.RemovedAt != nil {
				continue
			}
			if pe.Status != debridTypes.TorrentStatusDownloaded || entryFileSize(entry) <= 0 {
				continue
			}
			victims = append(victims, entry.InfoHash)
		}
		return nil
	})

	archived := 0
	for _, ih := range victims {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if err := m.evictEntry(ctx, cfg, client, ih); err != nil {
			m.logger.Warn().Err(err).Str("infohash", ih).Msg("archive: failed to evict entry")
			continue
		}
		archived++
	}
	if archived > 0 {
		m.logger.Info().Str("debrid", cfg.Name).Int("archived", archived).Msg("archive: reduced downloaded torrents to probe slices")
	}

	// Feed the next magnets into the freed space.
	m.feedMagnets(ctx, cfg, client)
}

// ForceEvict evicts a single torrent immediately, ignoring quota thresholds.
// Intended for testing the eviction + probe-cache + re-hydration cycle end to
// end. The probe-cache guard is applied even if eviction isn't formally enabled,
// so a forced test still retains head/tail slices.
func (m *Manager) ForceEvict(ctx context.Context, infohash string) (int64, error) {
	entry, err := m.GetEntry(infohash)
	if err != nil || entry == nil {
		return 0, fmt.Errorf("entry %s not found", infohash)
	}
	provider := entry.ActiveProvider
	client := m.ProviderClient(provider)
	if client == nil {
		return 0, fmt.Errorf("provider %q not available", provider)
	}
	cfg := client.Config()
	cfg.Name = provider
	if cfg.Eviction.HeaderCacheMB == 0 {
		cfg.Eviction.HeaderCacheMB = 16
	}
	if cfg.Eviction.HeaderCacheMB > 0 && cfg.Eviction.TailCacheMB <= 0 {
		cfg.Eviction.TailCacheMB = 1
	}
	freed := entryFileSize(entry)
	if err := m.evictEntry(ctx, cfg, client, infohash); err != nil {
		return 0, err
	}
	return freed, nil
}

// QuotaStatus reports a quota-limited provider's used/limit bytes (for testing
// and observability). Errors if the provider exposes no storage quota.
func (m *Manager) QuotaStatus(ctx context.Context, debridName string) (used, limit int64, err error) {
	client := m.ProviderClient(debridName)
	if client == nil {
		return 0, 0, fmt.Errorf("provider %q not available", debridName)
	}
	reporter, ok := client.(debrid.QuotaReporter)
	if !ok {
		return 0, 0, fmt.Errorf("provider %q has no storage quota", debridName)
	}
	return reporter.StorageQuota(ctx)
}

// archiveDownloadURL resolves a direct download URL for a file from its active
// provider placement, WITHOUT the linkService refresh/repair path (which can
// re-submit magnets, mark entries bad, or panic on a malformed entry). Returns an
// error to skip — never panics, never repairs.
// archiveDownloadURL resolves a placement file's download URL. The returned
// viaProxy reports whether the link must be fetched through the provider proxy
// (see types.DownloadLink.StreamViaProxy).
func (m *Manager) archiveDownloadURL(ctx context.Context, entry *storage.Entry, filename string) (link string, viaProxy bool, err error) {
	if entry == nil {
		return "", false, fmt.Errorf("nil entry")
	}
	pe := entry.GetActiveProvider()
	if pe == nil {
		return "", false, fmt.Errorf("no active placement")
	}
	pf := pe.Files[filename]
	if pf == nil || (pf.Link == "" && pf.Id == "") {
		return "", false, fmt.Errorf("no placement file/link for %s", filename)
	}
	client := m.ProviderClient(entry.ActiveProvider)
	if client == nil {
		return "", false, fmt.Errorf("provider %q not available", entry.ActiveProvider)
	}
	dl, err := client.GetDownloadLink(ctx, pe.ID, &debridTypes.File{
		TorrentId: pe.ID,
		Id:        pf.Id,
		Link:      pf.Link,
		Path:      pf.Path,
		Name:      filename,
	})
	if err != nil {
		return "", false, err
	}
	if dl.DownloadLink == "" {
		return "", false, fmt.Errorf("empty download link for %s", filename)
	}
	return dl.DownloadLink, dl.StreamViaProxy, nil
}

// evictEntry deletes the remote files for one torrent and marks its placement
// evicted, keeping all local metadata so it can be re-hydrated on the next read.
func (m *Manager) evictEntry(ctx context.Context, cfg config.Debrid, client debrid.Client, infohash string) error {
	provider := cfg.Name
	entry, err := m.GetEntry(infohash)
	if err != nil || entry == nil {
		return fmt.Errorf("entry %s not found", infohash)
	}
	pe, ok := entry.Providers[provider]
	if !ok || pe == nil || pe.ID == "" {
		return fmt.Errorf("no placement for %s on %s", infohash, provider)
	}
	if pe.RemovedAt != nil {
		return nil // already evicted
	}

	// Retain head/tail probe slices locally BEFORE deleting, so metadata reads
	// don't trigger a re-add. If the guard is on and caching fails, skip eviction
	// entirely — never delete a file we couldn't make probe-serveable.
	headLen := int64(cfg.Eviction.HeaderCacheMB) * 1024 * 1024
	tailLen := int64(cfg.Eviction.TailCacheMB) * 1024 * 1024
	if headLen > 0 {
		if err := m.cacheEntryProbeSlices(ctx, entry, headLen, tailLen); err != nil {
			return fmt.Errorf("cache probe slices: %w", err)
		}
	}

	// Delete the actual files on the provider. Only mark evicted if this
	// succeeds, so we never lose track of a file that still consumes quota.
	if err := client.DeleteTorrent(pe.ID); err != nil {
		m.clearProbeCache(infohash) // don't leave slices for a file we didn't evict
		return fmt.Errorf("delete remote torrent: %w", err)
	}

	now := time.Now()
	pe.RemovedAt = &now
	entry.UpdatedAt = now
	if err := m.AddOrUpdate(entry, nil); err != nil {
		return fmt.Errorf("persist evicted entry: %w", err)
	}

	m.logger.Info().
		Str("debrid", provider).
		Str("name", entry.Name).
		Str("infohash", infohash).
		Msg("eviction: evicted torrent (re-addable on next read)")
	return nil
}

// isEvicted reports whether an entry's active placement was evicted by us.
func isEvicted(entry *storage.Entry) bool {
	if entry == nil {
		return false
	}
	pe := entry.GetActiveProvider()
	return pe != nil && pe.RemovedAt != nil
}

// IsEvicted is the exported form for callers outside this package (e.g. the FUSE
// VFS read path).
func (m *Manager) IsEvicted(entry *storage.Entry) bool { return isEvicted(entry) }

// EnsureHydrated re-adds an evicted entry (if needed) and returns the live entry
// to use for streaming. Used by both the WebDAV and FUSE read paths so an evicted
// file is brought back before any bytes are expected. Returns the entry unchanged
// when it isn't evicted.
func (m *Manager) EnsureHydrated(ctx context.Context, entry *storage.Entry) (*storage.Entry, error) {
	if !isEvicted(entry) {
		return entry, nil
	}
	wait := 20
	if c := m.ProviderClient(entry.ActiveProvider); c != nil {
		if w := c.Config().Eviction.RehydrateWaitSeconds; w > 0 {
			wait = w
		}
	}
	if err := m.rehydrateEvicted(ctx, entry, wait); err != nil {
		return entry, err
	}
	if fresh, ferr := m.GetEntry(entry.InfoHash); ferr == nil && fresh != nil {
		return fresh, nil
	}
	return entry, nil
}

// rehydrateEvicted re-adds an evicted torrent's magnet to its provider and waits
// up to waitSeconds for it to become streamable. Concurrent calls for the same
// entry coalesce onto a single re-add. Returns nil once the entry is usable; a
// non-nil error means the caller should treat the read as temporarily
// unavailable (retryable) while the background re-add finishes.
func (m *Manager) rehydrateEvicted(ctx context.Context, entry *storage.Entry, waitSeconds int) error {
	if entry == nil {
		return fmt.Errorf("entry is nil")
	}
	infohash := entry.InfoHash

	call := &rehydrateCall{done: make(chan struct{})}
	if existing, loaded := m.rehydrating.LoadOrStore(infohash, call); loaded {
		// A re-hydration is already running; wait on it.
		select {
		case <-existing.done:
			return existing.err
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// We own this re-hydration. If distinct re-adds are firing in bulk (a library
	// scan touching many evicted/orphaned files), suppress this one instead of
	// stampeding the provider. Any readers that coalesced onto this call get the
	// same retryable error; the entry stays evicted and re-adds on a later read.
	if !m.rehydrateThrottle.admit(time.Now()) {
		call.err = errRehydrateThrottled
		m.rehydrating.Delete(infohash)
		close(call.done)
		m.logger.Debug().Str("infohash", infohash).Str("name", entry.Name).Msg("eviction: re-hydration throttled (scan burst)")
		return errRehydrateThrottled
	}

	// Run it to completion in the background even if the
	// caller's wait times out, so a slow (e.g. still-"Saving") re-add finishes and
	// the next read succeeds.
	go func() {
		defer func() {
			m.rehydrating.Delete(infohash)
			close(call.done)
		}()

		// Re-add the magnet and poll until files materialize. Bounded generously so
		// a slow "Saving" phase completes in the background even after the caller's
		// short wait has elapsed (and returned a retryable 503).
		hctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if err := m.rehydrateReAdd(hctx, entry); err != nil {
			call.err = err
			return
		}
		entry.Bad = false
		entry.UpdatedAt = time.Now()
		_ = m.AddOrUpdate(entry, func(t *storage.Entry) { m.InvalidateEntryCache() })
		m.clearProbeCache(infohash) // file is back on the provider; slices no longer needed
		m.logger.Info().Str("name", entry.Name).Str("infohash", infohash).Msg("eviction: re-hydrated evicted torrent")
	}()

	wait := time.Duration(waitSeconds) * time.Second
	if wait <= 0 {
		wait = 20 * time.Second
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()

	select {
	case <-call.done:
		return call.err
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("re-hydration of %s in progress, retry shortly", entry.Name)
	}
}

// rehydrateReAdd re-submits an evicted torrent's magnet to its provider and polls
// until the files materialize (PikPak reports "cached" via predict_type before
// the "Saving" phase finishes, so a single status check isn't enough). Unlike the
// repair fixer, it never deletes a slow re-add — the download is left to finish so
// a later read succeeds. On success it revives the placement and global files.
func (m *Manager) rehydrateReAdd(ctx context.Context, entry *storage.Entry) error {
	client := m.ProviderClient(entry.ActiveProvider)
	if client == nil {
		return fmt.Errorf("provider %q not available", entry.ActiveProvider)
	}

	magnet, err := utils.GetMagnetInfo(entry.Magnet, m.config.AlwaysRmTrackerUrls)
	if err != nil || magnet == nil || magnet.Link == "" {
		magnet = utils.ConstructMagnet(entry.InfoHash, entry.Name)
	}
	if magnet == nil || magnet.Link == "" {
		return fmt.Errorf("no magnet to re-add %s", entry.Name)
	}

	dbt := &debridTypes.Torrent{
		Name:             entry.Name,
		InfoHash:         entry.InfoHash,
		Magnet:           magnet,
		Size:             entry.Size,
		Files:            make(map[string]debridTypes.File),
		DownloadUncached: false,
	}
	dbt, err = client.SubmitMagnet(dbt)
	if err != nil || dbt == nil || dbt.Id == "" {
		return fmt.Errorf("re-add submit failed for %s: %w", entry.Name, err)
	}

	// Poll until files are available (or the torrent errors / we run out of time).
	for {
		if uerr := client.UpdateTorrent(dbt); uerr != nil {
			return fmt.Errorf("re-add status check for %s: %w", entry.Name, uerr)
		}
		if dbt.Status == debridTypes.TorrentStatusError {
			return fmt.Errorf("re-add of %s errored on provider", entry.Name)
		}
		if len(dbt.GetFiles()) > 0 {
			break
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("re-add of %s not ready before timeout (still saving)", entry.Name)
		case <-time.After(2 * time.Second):
		}
	}

	// Revive the placement (fresh ID/Files, RemovedAt cleared) and the global file
	// metadata so the mount serves the file again.
	entry.AddTorrentProvider(dbt)
	if entry.Files == nil {
		entry.Files = make(map[string]*storage.File)
	}
	now := time.Now()
	for _, f := range dbt.GetFiles() {
		if ex, ok := entry.Files[f.Name]; ok && ex != nil {
			ex.Size = f.Size
			ex.ByteRange = f.ByteRange
			ex.Deleted = false
			ex.InfoHash = entry.InfoHash
		} else {
			entry.Files[f.Name] = &storage.File{
				Name:      f.Name,
				Size:      f.Size,
				ByteRange: f.ByteRange,
				Deleted:   false,
				InfoHash:  entry.InfoHash,
				AddedOn:   now,
			}
		}
	}
	return nil
}
