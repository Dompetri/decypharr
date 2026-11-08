package manager

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func (m *Manager) syncTorrents(ctx context.Context) {
	// First time syncTorrents debrid -> storage
	m.logger.Info().
		Int("debrids", m.clients.Size()).
		Msg("Performing initial sync of torrents from debrid clients...")
	var wg sync.WaitGroup
	m.clients.Range(func(name string, client debrid.Client) bool {
		wg.Go(func() {
			if err := m.refreshTorrents(ctx, name, client); err != nil {
				m.logger.Error().Err(err).Str("debrid", name).Msg("Initial torrent sync failed")
			}
			m.InvalidateEntryCache()
		})
		return true
	})
	wg.Wait()
	m.logger.Info().
		Msg("Initial sync of torrents from debrid clients completed")
}

// Refresh configuration constants
const (
	refreshBatchSize       = 500
	refreshWriteBatchSize  = 50
	refreshFlushInterval   = 3 * time.Second
	refreshMaxWorkers      = 50 // Capped to avoid overwhelming debrid APIs
	refreshMinWorkers      = 5
	refreshDeleteWorkers   = 10
	refreshWorkChanBuffer  = 100
	refreshBatchChanBuffer = 50
)

// refreshTorrents refreshes torrents from a specific debrid service.
// Returns an error if the refresh fails.
func (m *Manager) refreshTorrents(ctx context.Context, provider string, debridClient debrid.Client) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	// Use singleflight to prevent concurrent refreshes for the same debrid
	_, err, _ := m.refreshSG.Do(provider, func() (any, error) {
		return nil, m.doRefreshTorrents(ctx, provider, debridClient)
	})

	return err
}

// doRefreshTorrents performs the actual refresh logic
func (m *Manager) doRefreshTorrents(_ context.Context, provider string, debridClient debrid.Client) error {
	remote, err := debridClient.GetTorrents()
	if err != nil {
		m.logger.Error().Err(err).Str("debrid", provider).Msg("Failed to get remote")
		return err
	}

	preserveOrphaned := debridClient.Config().RehydrateOrphaned

	// An empty remote normally means "nothing to reconcile". But with orphan
	// re-hydration on it may instead mean the account was swapped for an empty one,
	// in which case every local placement has genuinely vanished and must be
	// preserved for lazy re-add (handled in detectTorrentChanges) rather than
	// skipped. PikPak's GetTorrents fails hard on a walk error (returns above), so
	// an empty list here is a real empty account, not a transient read failure.
	if len(remote) == 0 && !preserveOrphaned {
		m.logger.Debug().Str("debrid", provider).Msg("No remote found")
		return nil
	}

	// Build map of current remote by infohash
	remoteTorrentsByHash := make(map[string]*types.Torrent, len(remote))
	for _, t := range remote {
		old, exists := remoteTorrentsByHash[t.InfoHash]
		if !exists {
			remoteTorrentsByHash[t.InfoHash] = t
		}
		if exists && t.Added.After(old.Added) {
			remoteTorrentsByHash[t.InfoHash] = t
		}
	}

	// Detect changes by streaming through cached entries
	newTorrents, torrentsToUpdate, torrentsToDelete, err := m.detectTorrentChanges(provider, remoteTorrentsByHash, preserveOrphaned)
	if err != nil {
		return err
	}

	// Handle deletions
	m.handleTorrentDeletions(torrentsToDelete)

	// Batch update torrents with changed placements (run concurrently)
	var updateWg sync.WaitGroup
	if len(torrentsToUpdate) > 0 {
		updateWg.Add(1)
		go func(torrents []*storage.Entry) {
			defer updateWg.Done()
			if err := m.storage.BatchAddOrUpdate(torrents); err != nil {
				m.logger.Error().Err(err).Msg("Failed to batch update remote")
			}
		}(torrentsToUpdate)
	}

	// Process new torrents
	if len(newTorrents) > 0 {
		if err := m.processNewTorrents(provider, newTorrents); err != nil {
			m.logger.Error().Err(err).Str("debrid", provider).Msg("Failed to process new torrents")
		}
	}

	// Wait for concurrent update to finish
	updateWg.Wait()

	return nil
}

// canRehydrateOrphan reports whether an entry whose placement vanished from the
// remote can be kept for lazy re-hydration instead of pruned. Limited to
// single-provider, completed torrents with an infohash and files: multi-debrid
// entries may still be served by another placement, and incomplete/errored
// entries have nothing worth re-adding.
func canRehydrateOrphan(entry *storage.Entry, provider string, pe *storage.ProviderEntry) bool {
	if entry == nil || pe == nil {
		return false
	}
	if !entry.IsTorrent() || entry.InfoHash == "" {
		return false
	}
	// Only when the vanished placement is the sole, active one. With another
	// provider present, fall through to RemoveProvider so that placement serves.
	if entry.ActiveProvider != provider || len(entry.Providers) != 1 {
		return false
	}
	if len(entry.Files) == 0 {
		return false
	}
	return pe.Status == types.TorrentStatusDownloaded
}

// detectTorrentChanges streams through cached entries and detects what changed
func (m *Manager) detectTorrentChanges(provider string, remoteTorrentsByHash map[string]*types.Torrent, preserveOrphaned bool) (
	newTorrents []*types.Torrent,
	torrentsToUpdate []*storage.Entry,
	torrentsToDelete []string,
	err error,
) {
	newTorrents = make([]*types.Torrent, 0, 100)
	torrentsToUpdate = make([]*storage.Entry, 0, 100)
	torrentsToDelete = make([]string, 0, 10)
	cachedInfoHashes := make(map[string]bool, len(remoteTorrentsByHash))

	err = m.storage.ForEachBatch(refreshBatchSize, func(batch []*storage.Entry) error {
		for _, entry := range batch {
			cachedInfoHashes[entry.InfoHash] = true

			currentTorrent, onRemote := remoteTorrentsByHash[entry.InfoHash]
			oldPlacement, placementOnDebrid := entry.Providers[provider]

			if placementOnDebrid {
				if !onRemote {
					// We intentionally evicted this placement to free remote storage
					// (RemovedAt set). Its absence from the remote list is expected, not
					// a removal — keep it so the mount path resolves and the magnet can
					// be re-added on the next read. Only genuinely-vanished placements
					// (RemovedAt == nil) are removed/deleted.
					if oldPlacement != nil && oldPlacement.RemovedAt != nil {
						continue
					}
					// The placement vanished and we didn't evict it — the account was
					// switched, or the file was deleted out of band. When orphan
					// re-hydration is on, keep recoverable entries (mark the placement
					// for lazy re-add) instead of pruning them, so a live *arr /
					// media-server symlink doesn't break. The next read re-adds the
					// magnet onto the current account via the eviction re-hydrate path.
					if preserveOrphaned && canRehydrateOrphan(entry, provider, oldPlacement) {
						now := time.Now()
						oldPlacement.RemovedAt = &now
						entry.UpdatedAt = now
						torrentsToUpdate = append(torrentsToUpdate, entry)
						m.logger.Info().
							Str("debrid", provider).
							Str("name", entry.Name).
							Str("infohash", entry.InfoHash).
							Msg("sync: placement vanished from remote; preserving for lazy re-hydration")
						continue
					}
					entry.RemoveProvider(provider, nil)
					if len(entry.Providers) == 0 {
						torrentsToDelete = append(torrentsToDelete, entry.InfoHash)
					} else {
						torrentsToUpdate = append(torrentsToUpdate, entry)
					}
				} else if oldPlacement.RemovedAt != nil {
					// Evicted placement reappeared on the remote (re-hydrated, or the
					// user re-added it). Treat as a fresh placement so its ID/Files and
					// the cleared eviction state are persisted.
					newTorrents = append(newTorrents, currentTorrent)
				} else if oldPlacement.NeedsUpdate(currentTorrent) {
					// currentTorrent has changes for this provider - update placement info
					// But the issue is that currentTorrent may not have all the metadata we need to update the placement (e.g. downloadedAt, files etc)
					// So we need to fetch the full torrent info from debrid to ensure we have all the metadata to update the placement correctly
					// So let's just add it to the newTorrents list and let processNewTorrents handle the update logic - it will be smart enough to only update the placement info without overwriting other metadata
					newTorrents = append(newTorrents, currentTorrent)
				}
			} else if onRemote {
				newTorrents = append(newTorrents, currentTorrent)
			}
		}
		return nil
	})

	if err != nil {
		m.logger.Error().Err(err).Msg("Failed to stream cached remote")
		return nil, nil, nil, err
	}

	// Check for brand new torrents (not in cache at all)
	for infohash, t := range remoteTorrentsByHash {
		if !cachedInfoHashes[infohash] {
			newTorrents = append(newTorrents, t)
		}
	}

	return newTorrents, torrentsToUpdate, torrentsToDelete, nil
}

// handleTorrentDeletions processes torrent deletions concurrently
func (m *Manager) handleTorrentDeletions(torrentsToDelete []string) {
	if len(torrentsToDelete) == 0 {
		return
	}

	var deleteWg sync.WaitGroup
	deleteChan := make(chan string, len(torrentsToDelete))

	deleteWorkers := min(refreshDeleteWorkers, len(torrentsToDelete))
	for range deleteWorkers {
		deleteWg.Go(func() {
			for infohash := range deleteChan {
				if err := m.storage.Delete(infohash); err != nil {
					m.logger.Error().Err(err).Str("infohash", infohash).Msg("Failed to delete torrent")
				}
				m.clearProbeCache(infohash) // drop any retained probe slices
			}
		})
	}

	for _, infohash := range torrentsToDelete {
		deleteChan <- infohash
	}
	close(deleteChan)
	deleteWg.Wait()
}

// processNewTorrents processes new torrents with worker pool and batch writing
func (m *Manager) processNewTorrents(provider string, newTorrents []*types.Torrent) error {
	workChan := make(chan *types.Torrent, min(refreshWorkChanBuffer, len(newTorrents)))
	batchChan := make(chan *storage.Entry, refreshBatchChanBuffer)
	errChan := make(chan error, 1) // Buffer for first error

	var processWg sync.WaitGroup
	var batchWg sync.WaitGroup
	var processed atomic.Int64
	totalTorrents := len(newTorrents)

	// Batch writer goroutine
	batchWg.Go(func() {
		m.runBatchWriter(batchChan, errChan)
	})

	// Scale workers based on torrent count, but cap to avoid overwhelming APIs
	workers := min(refreshMaxWorkers, max(refreshMinWorkers, len(newTorrents)/10))

	for range workers {
		processWg.Go(func() {
			for t := range workChan {
				if mt, err := m.processSyncTorrent(t); err != nil {
					m.logger.Error().Err(err).Str("debrid", provider).Msgf("Failed to process torrent %s", t.Id)
				} else if mt != nil {
					batchChan <- mt
				}
				count := processed.Add(1)
				if count%50 == 0 {
					m.logger.Debug().Str("debrid", provider).Msgf("Processed %d / %d new torrents", count, totalTorrents)
				}
			}
		})
	}

	// Send torrents to workers
	for _, t := range newTorrents {
		workChan <- t
	}

	close(workChan)
	processWg.Wait()
	close(batchChan)
	batchWg.Wait()

	// Check if batch writer encountered an error
	select {
	case err := <-errChan:
		return err
	default:
		return nil
	}
}

// runBatchWriter collects entries and writes them in batches
func (m *Manager) runBatchWriter(batchChan <-chan *storage.Entry, errChan chan<- error) {
	batch := make([]*storage.Entry, 0, refreshWriteBatchSize)
	ticker := time.NewTicker(refreshFlushInterval)
	defer ticker.Stop()

	var writeErr error
	flushBatch := func() {
		if len(batch) == 0 || writeErr != nil {
			return
		}
		if err := m.storage.BatchAddOrUpdate(batch); err != nil {
			m.logger.Error().Err(err).Msg("Failed to batch write remote")
			writeErr = err
			// Send first error to channel (non-blocking)
			select {
			case errChan <- err:
			default:
			}
		}
		// Clear slice
		for i := range batch {
			batch[i] = nil
		}
		batch = batch[:0]
	}

	for {
		select {
		case t, ok := <-batchChan:
			if !ok {
				flushBatch()
				return
			}
			batch = append(batch, t)
			if len(batch) >= refreshWriteBatchSize {
				flushBatch()
			}
		case <-ticker.C:
			flushBatch()
		}
	}
}

// processSyncTorrent processes a single torrent and returns it for batched writing
func (m *Manager) processSyncTorrent(t *types.Torrent) (*storage.Entry, error) {
	// GetReader the debrid client
	client := m.ProviderClient(t.Debrid)
	if client == nil {
		return nil, nil
	}

	// Check if files are complete - only make API call if needed
	needsUpdate := len(t.Files) == 0 || !isComplete(t.Files)
	if needsUpdate {
		// This is the main bottleneck - API call per torrent
		// Consider: Could we batch UpdateTorrent calls? Depends on debrid API
		if err := client.UpdateTorrent(t); err != nil {
			return nil, err
		}

		// Re-check completion after update
		if !isComplete(t.Files) {
			return nil, nil
		}
	}

	addedOn := t.Added
	if addedOn.IsZero() {
		addedOn = time.Now()
	}

	// Check if we have an existing managed torrent
	// Note: This is a database read per torrent - could be optimized with batch reads
	// or an in-memory cache, but storage.GetReader is likely fast (indexed by InfoHash)
	mt, err := m.storage.Get(t.InfoHash)
	if err != nil {
		// Create new managed torrent
		var magnet *utils.Magnet
		if t.Magnet == nil || t.Magnet.Link == "" {
			magnet = utils.ConstructMagnet(t.InfoHash, t.Name)
		} else {
			magnet = t.Magnet
		}
		size := t.Size
		if size == 0 {
			size = t.Bytes
		}
		mt = &storage.Entry{
			Protocol:         config.ProtocolTorrent,
			InfoHash:         t.InfoHash,
			Name:             t.Name,
			OriginalFilename: t.OriginalFilename,
			Size:             size,
			Bytes:            size,
			Magnet:           magnet.Link,
			ActiveProvider:   t.Debrid,
			Providers:        make(map[string]*storage.ProviderEntry),
			Files:            make(map[string]*storage.File),
			Status:           t.Status,
			Progress:         t.Progress,
			Speed:            t.Speed,
			Seeders:          t.Seeders,
			IsComplete:       len(t.Files) > 0,
			Bad:              false,
			AddedOn:          addedOn,
			CreatedAt:        addedOn,
			UpdatedAt:        time.Now(),
		}
	}

	// Populate global Files metadata (only if empty)
	if len(mt.Files) == 0 {
		for _, f := range t.GetFiles() {
			mt.Files[f.Name] = &storage.File{
				Name:      f.Name,
				Size:      f.Size,
				ByteRange: f.ByteRange,
				Deleted:   f.Deleted,
				InfoHash:  t.InfoHash,
				AddedOn:   addedOn,
			}
		}
	}

	// AddOrUpdate or update placement
	placement := mt.AddTorrentProvider(t)
	placement.Progress = t.Progress
	if t.Status == types.TorrentStatusDownloaded {
		downloadedAt := addedOn
		placement.DownloadedAt = &downloadedAt
	}

	// If this is the first placement or the only one, make it active
	if mt.ActiveProvider == "" || len(mt.Providers) == 1 {
		if t.Status == types.TorrentStatusDownloaded {
			_ = mt.ActivatePlacement(t.Debrid)
		}
	}

	// confirm everything is complete
	if err := mt.Validate(); err != nil {
		m.logger.Warn().Err(err).Str("infohash", t.InfoHash).Str("name", mt.Name).Msg("Validation failed for torrent, marking as bad")
	}

	return mt, nil
}

// refreshTorrent refreshes a single torrent from its active debrid
func (m *Manager) refreshTorrent(infohash string) (*storage.Entry, error) {
	torrent, err := m.storage.Get(infohash)
	if err != nil {
		return nil, err
	}

	if torrent.ActiveProvider == "" {
		return torrent, nil
	}

	client := m.ProviderClient(torrent.ActiveProvider)
	if client == nil {
		return torrent, nil
	}

	placement := torrent.GetActiveProvider()
	if placement == nil {
		return torrent, nil
	}

	// GetReader updated torrent info from debrid
	debridTorrent, err := client.GetTorrent(placement.ID)
	if err != nil {
		return nil, err
	}

	entry, err := m.processSyncTorrent(debridTorrent)
	if err != nil {
		return nil, err
	}
	// Store updated entry in storage
	if entry != nil {
		if err := m.storage.AddOrUpdate(entry); err != nil {
			return nil, err
		}
	}
	return entry, nil
}

// refreshDebridDownloadLinks refreshes download links for a specific debrid service
func (m *Manager) refreshDebridDownloadLinks(ctx context.Context, debridName string, client debrid.Client) {
	select {
	case <-ctx.Done():
		return
	default:
	}

	if client == nil {
		m.logger.Warn().Str("debrid", debridName).Msg("Provider client is nil, skipping download link refresh")
		return
	}

	if err := client.RefreshDownloadLinks(); err != nil {
		m.logger.Error().Err(err).Str("debrid", debridName).Msg("Failed to refresh download links")
	}
}

// isComplete checks if all files in a torrent have download links
func isComplete(files map[string]types.File) bool {
	if len(files) == 0 {
		return false
	}
	for _, file := range files {
		if file.Link == "" {
			return false
		}
	}
	return true
}
