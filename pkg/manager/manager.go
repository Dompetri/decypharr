package manager

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-co-op/gocron/v2"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/logger"
	"github.com/sirrobot01/decypharr/internal/request"
	"github.com/sirrobot01/decypharr/internal/utils"
	"github.com/sirrobot01/decypharr/pkg/arr"
	"github.com/sirrobot01/decypharr/pkg/arr/reacquire"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
	"github.com/sirrobot01/decypharr/pkg/hearsay"
	"github.com/sirrobot01/decypharr/pkg/manager/link"
	"github.com/sirrobot01/decypharr/pkg/manager/virtualfolders"
	"github.com/sirrobot01/decypharr/pkg/notifications"
	"github.com/sirrobot01/decypharr/pkg/repair"
	"github.com/sirrobot01/decypharr/pkg/storage"
	"github.com/sirrobot01/decypharr/pkg/storage/migration"
	"github.com/sirrobot01/decypharr/pkg/strm"
	"github.com/sirrobot01/decypharr/pkg/usenet"
	"github.com/sirrobot01/decypharr/pkg/version"
	"golang.org/x/sync/singleflight"
)

// newStreamTransport builds the streaming transport. proxyURL is optional: when
// set, the transport routes through that proxy (http:// or socks5://) and bounds
// concurrency — a proxied path funnels every connection through one upstream, and
// providers like PikPak only serve a handful of connections per source IP, so the
// unbounded 500-conn pool used for direct streaming would overwhelm it and stall.
func newStreamTransport(proxyURL string) *http.Transport {
	dialer := &net.Dialer{
		Timeout:   5 * time.Second,  // Fast connection timeout
		KeepAlive: 30 * time.Second, // Keep connections alive
	}

	t := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS12,
			ClientSessionCache: tls.NewLRUClientSessionCache(200),
		},
		TLSHandshakeTimeout:    20 * time.Second,
		MaxIdleConns:           1000,
		MaxIdleConnsPerHost:    500,
		MaxConnsPerHost:        500,
		IdleConnTimeout:        120 * time.Second,
		DisableCompression:     false,
		DialContext:            dialer.DialContext,
		Proxy:                  http.ProxyFromEnvironment,
		MaxResponseHeaderBytes: 1 << 20,   // 1MB header buffer for CDN responses
		WriteBufferSize:        32 << 10,  // requests are tiny
		ReadBufferSize:         256 << 10, // caps how much a single body.Read can return
	}
	if proxyURL != "" {
		request.SetProxy(t, proxyURL)
		t.MaxConnsPerHost = 64
		t.MaxIdleConnsPerHost = 64
	}
	return t
}

// Manager handles unified torrent management - replaces wire.Store completely
type Manager struct {
	storage      *storage.Storage
	migrator     *migration.Migrator
	repair       *repair.Service
	clients      *xsync.Map[string, debrid.Client]
	arr          *arr.Service
	arrService   *reacquire.Service
	arrIndexer   *reacquire.Indexer
	logger       zerolog.Logger
	ready        chan struct{}
	readyOnce    sync.Once
	streamClient *http.Client
	// Per-provider stream clients, keyed by proxy URL. A debrid with a configured
	// proxy streams through it instead of the shared client, so only that
	// provider's downloads take the detour.
	providerStreamClients *xsync.Map[string, *http.Client]

	// In-flight lazy re-hydrations of evicted entries, keyed by InfoHash, so
	// simultaneous reads of the same evicted file trigger a single re-add.
	rehydrateMu sync.Mutex
	rehydrating *xsync.Map[string, *rehydrateCall]

	// Rate-limits distinct re-add attempts so a burst of reads touching many
	// evicted/orphaned files at once (e.g. a library scan) doesn't stampede the
	// provider. Excess attempts return a retryable error and are not re-added.
	rehydrateThrottle *rehydrateThrottle

	// Guards against overlapping eviction/archive sweeps (a slow sweep must not
	// run concurrently with the next scheduled run).
	evictionRunning atomic.Bool

	// Archive ingestion: queue lock + recently-submitted infohashes (suppresses
	// re-submitting before the provider creates the entry).
	ingestMu        sync.Mutex
	ingestSubmitted *xsync.Map[string, int64] // infohash -> submit unix timestamp
	lastStreamed    *xsync.Map[string, int64] // infohash -> last streamed unix timestamp

	arrRecoveryMu          sync.RWMutex
	arrRecovery            ArrRecovery
	reacquireNotifications sync.Map

	// Migration jobs tracking
	migrationJobs   *xsync.Map[string, *storage.SwitcherJob]
	refreshInterval time.Duration

	config *config.Config

	// Processing workers
	scheduler    gocron.Scheduler
	cetScheduler gocron.Scheduler
	queue        *Queue

	// downloading
	refreshSG   singleflight.Group
	linkService *link.Service

	// repair
	fixer *Fixer
	ctx   context.Context

	// strm reconciler
	strm *strm.Reconciler

	virtualFoldersMu sync.RWMutex
	virtualFolders   *virtualfolders.Folders
	mountManager     MountManager

	startTime     time.Time
	usenetTimeout time.Duration

	rootInfo   *FileInfo
	entry      *EntryCache
	downloader *Downloader
	usenet     *usenet.Usenet

	// Debrid speed test results storage
	debridSpeedTestResults *xsync.Map[string, debridTypes.SpeedTestResult]

	// Active streams tracking
	activeStreams *xsync.Map[string, *ActiveStream]

	// In-flight queue-processor dispatches, keyed by InfoHash, to prevent
	// duplicate goroutines from processing the same entry when the scheduler
	// re-fires before the previous pass has updated the queue row.
	processingEntries *xsync.Map[string, struct{}]

	// Set when restoreActiveDownloadJobs has resubmitted the persisted queue.
	// Until then a Queued entry may still be waiting for its job.
	queueRestored atomic.Bool

	// Suppresses repeated provider submissions for the same torrent after an
	// Arr import/re-grab loop. The queue itself handles duplicates while an
	// entry is present; this gate covers the short window after Arr deletes it.
	torrentSubmissions *torrentSubmissionGate

	// Unified active-download queue for torrent and NZB imports.
	jobQueue  *JobQueue
	nzbSyncMu sync.Mutex

	downloadMu       sync.Mutex
	downloadTasks    sync.WaitGroup
	downloadsStopped bool
	cancelDownloads  context.CancelFunc

	// Notifications service
	Notifications *notifications.Service

	// Hearsay network participation; nil when disabled.
	hearsay *hearsay.Service
}

var _ repair.Backend = (*Manager)(nil)

// New creates a new Manager instance
func New() *Manager {
	cfg := config.Get()
	_logger := logger.New("manager")

	strg, err := storage.NewStorage(filepath.Join(config.GetMainPath(), "db"))
	if err != nil {
		panic(fmt.Errorf("failed to create manager storage: %w", err))
	}

	// Initialize debrid registry
	ctx := context.Background()

	streamClient := &http.Client{
		Timeout:   0,
		Transport: newStreamTransport(""),
	}

	usenetTimeout, err := utils.ParseDuration(cfg.Usenet.ProcessingTimeout)
	if err != nil {
		usenetTimeout = 10 * time.Minute
	}

	instance := &Manager{
		storage:                strg,
		clients:                xsync.NewMap[string, debrid.Client](),
		logger:                 _logger,
		migrationJobs:          xsync.NewMap[string, *storage.SwitcherJob](),
		config:                 cfg,
		arr:                    arr.New(),
		queue:                  newQueue(strg, cfg.RemoveStalledAfter),
		ctx:                    ctx,
		ready:                  make(chan struct{}),
		streamClient:           streamClient,
		providerStreamClients:  xsync.NewMap[string, *http.Client](),
		rehydrating:            xsync.NewMap[string, *rehydrateCall](),
		rehydrateThrottle:      newRehydrateThrottle(rehydrateBurstWindow, rehydrateBurstLimit),
		ingestSubmitted:        xsync.NewMap[string, int64](),
		lastStreamed:           xsync.NewMap[string, int64](),
		usenetTimeout:          usenetTimeout,
		debridSpeedTestResults: xsync.NewMap[string, debridTypes.SpeedTestResult](),
		activeStreams:          xsync.NewMap[string, *ActiveStream](),
		processingEntries:      xsync.NewMap[string, struct{}](),
		torrentSubmissions:     newTorrentSubmissionGate(torrentSubmissionDedupWindow),
	}

	instance.init()

	// Create migrator
	return instance
}

func (m *Manager) init() {
	m.downloadMu.Lock()
	m.ctx, m.cancelDownloads = context.WithCancel(context.Background())
	m.downloadsStopped = false
	m.downloadMu.Unlock()
	cfg := config.Get()
	scheduler, err := gocron.NewScheduler(gocron.WithLocation(time.Local), gocron.WithGlobalJobOptions(gocron.WithTags("decypharr-manager")))
	if err != nil {
		scheduler, _ = gocron.NewScheduler(gocron.WithGlobalJobOptions(gocron.WithTags("decypharr-manager")))
	}

	// Create CET scheduler for time-specific jobs
	cetLocation, err := time.LoadLocation("CET")
	if err != nil {
		cetLocation = time.UTC
	}
	cetScheduler, err := gocron.NewScheduler(gocron.WithLocation(cetLocation), gocron.WithGlobalJobOptions(gocron.WithTags("decypharr-cet")))
	if err != nil {
		cetScheduler, _ = gocron.NewScheduler(gocron.WithGlobalJobOptions(gocron.WithTags("decypharr-cet")))
	}

	m.config = cfg

	// Recreate queue with new config
	m.queue = newQueue(m.storage, cfg.RemoveStalledAfter)

	// Clear debrid clients so they get recreated with new config
	m.clients = xsync.NewMap[string, debrid.Client]()

	// Reset ready channel and syncTorrents.Once for the next start
	m.ready = make(chan struct{})
	m.readyOnce = sync.Once{}

	m.scheduler = scheduler
	m.cetScheduler = cetScheduler
	m.migrator = migration.New(m.storage)
	m.downloader = NewDownloadManager(m)

	// Initialize HTTP pool for streaming
	// Note: We can't create a single pool for all files because the LinkRefresh callback
	// needs torrent+filename context. Instead, manager.Stream will create a pool per request
	// and cache it. This is actually better because different files may have different
	// download links from different CDNs.

	refreshInterval, err := utils.ParseDuration(cfg.RefreshInterval)
	if err != nil {
		refreshInterval = 15 * time.Minute
	}
	m.refreshInterval = refreshInterval

	// initialize debrid clients
	m.initDebridClients()

	// Initialize usenet client
	m.initUsenet()

	// Initialize link service
	m.initLinkService()

	// Initialize virtual folders.
	m.initVirtualFolders()

	// Initialize fixer
	m.fixer = NewFixer(m)

	// Initialize strm reconciler
	m.strm = strm.NewReconciler(m.ctx, m.storage, func(ctx context.Context, entry *storage.Entry, filename string) (io.ReadCloser, error) {
		return m.OpenStreamUntracked(ctx, entry, filename, 0)
	}, m.logger)

	// Set mount paths
	m.setMountPaths()

	m.initEntryCache()
	m.initArrServices()

	// Initialize notifications service
	m.Notifications = notifications.New(&m.config.Notifications, m.logger)

	// Initialize Hearsay state and its default network participation.
	if hs, err := hearsay.New(m.config, m.logger); err != nil {
		m.logger.Warn().Err(err).Msg("Hearsay disabled: failed to initialize")
	} else {
		m.hearsay = hs
	}

	m.repair = repair.New(repair.Dependencies{
		Scheduler:     m.scheduler,
		Backend:       m,
		Storage:       m.storage,
		Arrs:          m.arr,
		Reacquirer:    m.arrService,
		Usenet:        m.usenet,
		Notifications: m.Notifications,
		Hearsay:       m.hearsay,
	})

	// Initialize the unified active-download queue after all processors exist.
	m.initJobQueue()
}

func (m *Manager) initArrServices() {
	service, err := reacquire.NewService(reacquire.ServiceOptions{
		Arrs:      m.arr,
		Directory: filepath.Join(config.GetMainPath(), "db"),
		Handler:   reacquire.NewHandler(m.arr, m),
	})
	if err != nil {
		m.logger.Error().Err(err).Msg("Failed to initialize Arr reacquisition service")
		m.arrService = nil
		m.arrIndexer = nil
		m.SetArrRecovery(nil)
		return
	}
	m.arrService = service
	m.arrIndexer = reacquire.NewIndexer(m.arr, managedArrCatalog{storage: m.storage, logger: m.logger}, service,
		filepath.Join(m.config.Mount.MountPath, EntryAllFolder))
	m.SetArrRecovery(service)
}

func (m *Manager) initUsenet() {
	usenetClient, err := usenet.New()
	if err != nil {
		m.logger.Warn().Msg("Usenet client not configured")
		m.usenet = nil
		return
	}
	m.usenet = usenetClient
}

// initLinkService initializes the link service
func (m *Manager) initLinkService() {
	m.linkService = link.New(
		m.clients,
		m.refreshTorrent,
		m.ReinsertEntry,
		func(entry *storage.Entry) error { return m.AddOrUpdate(entry, nil) },
		m.streamClient,
		m.config.Retries,
		logger.New("link"),
	)
}

func (m *Manager) initJobQueue() {
	m.jobQueue = NewJobQueue(m.ctx, m.config.MaxActiveDownloads, m.processJob)
	// Restore persisted active/queued downloads in the background. With large
	// queues this re-parses thousands of NZBs over the network, and running it
	// inline blocked manager construction — and therefore the HTTP server —
	// for 60-90 minutes on big libraries, during which every arr reported
	// "download client unavailable". Backgrounding lets the API serve and the
	// worker pool drain immediately while the restore catches up.
	m.startDownloadTask(func() {
		defer func() {
			if r := recover(); r != nil {
				m.logger.Error().Interface("panic", r).Msg("Recovered from panic while restoring active downloads")
			}
		}()
		defer m.queueRestored.Store(true)
		m.restoreActiveDownloadJobs()
	})
}

// startDownloadTask registers work before shutdown can wait for it.
// Once shutdown starts, queued entries stay in storage for the next start.
func (m *Manager) startDownloadTask(task func()) bool {
	m.downloadMu.Lock()
	defer m.downloadMu.Unlock()
	if m.downloadsStopped {
		return false
	}
	m.downloadTasks.Go(task)
	return true
}

func (m *Manager) processJob(ctx context.Context, job *Job) {
	if job == nil {
		return
	}

	var err error
	switch job.Type {
	case JobTypeTorrent:
		err = m.processTorrentJob(ctx, job)
	case JobTypeNZB:
		err = m.processNZBJob(ctx, job)
	default:
		err = fmt.Errorf("unknown job type: %s", job.Type)
	}

	if err != nil {
		if ctx.Err() != nil {
			return
		}
		if isTooManyActiveDownloads(err) {
			if job.Entry != nil {
				job.Entry.Status = debridTypes.TorrentStatusQueued
				_ = m.queue.Update(job.Entry)
			}
			m.jobQueue.Retry(job, 30*time.Second)
			return
		}
		m.logger.Error().Err(err).Str("job_id", job.ID).Str("type", string(job.Type)).Msg("Active download failed")
		if job.Entry != nil {
			job.Entry.MarkAsError(err)
			_ = m.queue.Update(job.Entry)
		}
		return
	}
	// The worker slot is released as soon as the job is handed off. Waiting
	// here until the entry left the downloading state parked a worker for the
	// whole post-download lifecycle - including the 30 minute mount wait of the
	// symlink action - so a handful of slow imports stalled every job the arrs
	// submitted. processQueuedEntries drives the entry from here.
}

func (m *Manager) migrate() {
	// Check if migration has already been done
	status, err := m.migrator.GetStatus()
	if err == nil && !status.Running && status.Completed > 0 {
		m.logger.Info().
			Int("completed", status.Completed).
			Msg("Migration already completed previously")
		return
	}

	// GetReader migration stats to see if there are cache files
	stats, err := m.migrator.GetStats()
	if err != nil {
		m.logger.Warn().Err(err).Msg("Failed to get migration stats")
		return
	}

	cacheFiles, ok := stats["cache_files"].(int)
	if !ok || cacheFiles == 0 {
		return
	}

	cacheTorrents, ok := stats["cache_torrents"].(int)
	if !ok {
		cacheTorrents = 0
	}

	m.logger.Info().
		Int("cache_files", cacheFiles).
		Int("unique_torrents", cacheTorrents).
		Msg("Found cache files, starting automatic migration...")

	// Start migration with backup
	if err := m.migrator.Start(); err != nil {
		m.logger.Error().Err(err).Msg("Failed to start automatic migration")
		return
	}

	m.logger.Info().Msg("Automatic migration started successfully")
}

// Start starts the manager and all its components
func (m *Manager) Start(ctx context.Context) error {
	m.startTime = time.Now()
	m.logger.Info().
		Str("version", version.GetInfo().String()).
		Str("mount_type", string(m.config.Mount.Type)).
		Str("notifications", fmt.Sprintf("%v", m.Notifications.IsEnabled())).
		Str("mount_path", m.config.Mount.MountPath).
		Msg("Starting manager")

	// run the migration process
	m.migrate()

	if m.arrService != nil {
		if err := m.arrService.Start(ctx); err != nil {
			return fmt.Errorf("failed to start Arr reacquisition service: %w", err)
		}
	}
	if m.arrIndexer != nil {
		if err := m.arrIndexer.Start(ctx); err != nil {
			return fmt.Errorf("failed to start Arr indexer: %w", err)
		}
	}

	go func() {
		m.syncTorrents(ctx)
		m.ReconcileCatalogOrphans()
		// Sync NZBs
		if err := m.syncNZBs(ctx); err != nil {
			m.logger.Error().Err(err).Msg("Failed to perform initial NZB syncTorrents")
		}
		if fixNZB := os.Getenv("DECYPHARR_FIX_NZB_SIZES"); fixNZB == "1" {
			m.logger.Info().Msg("Starting NZB file size correction as requested by environment variable")
			m.fixNZBFileSizes(ctx)
		}
		// Converge the .strm export tree with config applied since last run.
		m.strm.SweepAsync("startup")
	}()

	// Start workers
	if err := m.StartWorker(ctx); err != nil {
		return fmt.Errorf("failed to start manager worker: %w", err)
	}

	// Start network participation when the operator opted in.
	if m.hearsay != nil {
		if err := m.hearsay.Start(ctx); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to start hearsay network participation")
		}
	}

	// Close ready channel once, safe for multiple calls
	m.readyOnce.Do(func() {
		close(m.ready)
	})

	// Start the mount manager if set
	// This also start thr mounting process
	if m.mountManager != nil {
		if err := m.mountManager.Start(ctx); err != nil {
			// If mount manager fails to start, we log the error but continue running the manager
			m.logger.Error().Err(err).Msg("Failed to start mount manager, continuing without mounting")
			return nil
		}
	}

	return nil
}

// Stop stops the manager and cleans up all resources
func (m *Manager) Stop() error {
	m.logger.Info().Msg("Stopping manager")
	m.downloadMu.Lock()
	m.downloadsStopped = true
	if m.cancelDownloads != nil {
		m.cancelDownloads()
	}
	m.downloadMu.Unlock()

	// Stop schedulers
	if m.scheduler != nil {
		if err := m.scheduler.Shutdown(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to shutdown scheduler")
		}
	}
	if m.cetScheduler != nil {
		if err := m.cetScheduler.Shutdown(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to shutdown CET scheduler")
		}
	}

	if m.jobQueue != nil {
		m.logger.Info().Msg("Closing active download queue")
		m.jobQueue.Close()
	}

	m.downloadTasks.Wait()

	// Downloads have saved their final or resumable state.
	if m.mountManager != nil {
		m.logger.Info().Msg("Stopping mount manager")
		if err := m.mountManager.Stop(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to stop mount manager")
		}
	}
	if m.repair != nil {
		m.repair.Stop()
	}
	if m.arrIndexer != nil {
		m.arrIndexer.Close()
	}
	if m.arrService != nil {
		if err := m.arrService.Close(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to close Arr reacquisition service")
		}
	}
	m.SetArrRecovery(nil)

	// Close usenet connection manager if active
	if m.usenet != nil {
		m.logger.Info().Msg("Closing usenet connections")
		if err := m.usenet.Close(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to close usenet")
		}
	}

	m.hearsay.Close()

	// Close storage
	if m.storage != nil {
		m.logger.Info().Msg("Closing storage database")
		if err := m.storage.Close(); err != nil {
			m.logger.Warn().Err(err).Msg("Failed to close storage")
		}
	}

	m.logger.Info().Msg("Manager stopped successfully")
	return nil
}

// Reset resets the manager with the new configuration
// This is called after config changes (e.g., setup wizard) to apply new settings
func (m *Manager) Reset() error {
	m.logger.Info().Msg("Resetting manager with new configuration")

	// Stop resources before resetting
	if err := m.Stop(); err != nil {
		m.logger.Warn().Err(err).Msg("Failed to stop manager during reset")
	}

	// Reopen storage database (it was closed by Stop)
	strg, err := storage.NewStorage(filepath.Join(config.GetMainPath(), "db"))
	if err != nil {
		return fmt.Errorf("failed to reopen storage after reset: %w", err)
	}
	m.storage = strg

	// Reload configuration
	m.init()
	m.logger.Info().Msg("Manager reset complete")
	return nil
}

func (m *Manager) GetStats() (map[string]any, error) {
	count, err := m.storage.Count()
	if err != nil {
		return nil, err
	}

	diskSize := m.storage.DiskSize()
	activeJobs := 0
	completedJobs := 0
	failedJobs := 0
	m.migrationJobs.Range(func(_ string, job *storage.SwitcherJob) bool {
		switch job.Status {
		case storage.SwitcherStatusPending, storage.SwitcherStatusInProgress:
			activeJobs++
		case storage.SwitcherStatusCompleted:
			completedJobs++
		case storage.SwitcherStatusFailed, storage.SwitcherStatusCancelled:
			failedJobs++
		}
		return true
	})

	return map[string]any{
		"total_torrents": count,
		"storage_stats":  map[string]any{"total_size": diskSize},
		"active_jobs":    activeJobs,
		"completed_jobs": completedJobs,
		"failed_jobs":    failedJobs,
	}, nil
}

func (m *Manager) IsReady() chan struct{} {
	return m.ready
}

func (m *Manager) Uptime() time.Duration {
	return time.Since(m.startTime)
}

func (m *Manager) StartTime() time.Time {
	return m.startTime
}

// CRUD operations

func (m *Manager) GetEntryItem(torrentName string) (*storage.EntryItem, error) {
	return m.storage.GetEntryItem(torrentName)
}

func (m *Manager) GetEntryByName(torrentName, filename string) (*storage.Entry, error) {
	// First get entry
	entry, err := m.storage.GetEntryItem(torrentName)
	if err != nil {
		return nil, err
	}

	// Find the file in the entry
	file, err := entry.GetFile(filename)
	if err != nil {
		return nil, err
	}
	torr, err := m.GetEntry(file.InfoHash)
	if err != nil || torr == nil {
		if recovered := m.recoverOrphanFromItem(entry, file.InfoHash); recovered != nil {
			return recovered, nil
		}
		return nil, err
	}
	return torr, nil
}

func (m *Manager) defaultOrFirstProvider() string {
	var first string
	m.clients.Range(func(key string, _ debrid.Client) bool {
		if first == "" {
			first = key
		}
		return true
	})
	if first != "" {
		return first
	}
	if m.config != nil && len(m.config.Debrids) > 0 {
		return m.config.Debrids[0].Name
	}
	return "pikpak"
}

// recoverOrphanFromItem reconstructs an orphaned storage.Entry from a catalog EntryItem.
// This allows entries that were pruned or missing from the entries store (e.g. after
// debrid account changes) to be recovered on demand and re-added to the provider upon streaming.
func (m *Manager) recoverOrphanFromItem(item *storage.EntryItem, infohash string) *storage.Entry {
	if item == nil || infohash == "" {
		return nil
	}
	provider := m.defaultOrFirstProvider()
	now := time.Now()
	entry := &storage.Entry{
		Protocol:         config.ProtocolTorrent,
		InfoHash:         infohash,
		Name:             item.Name,
		OriginalFilename: item.Name,
		ActiveProvider:   provider,
		Providers:        make(map[string]*storage.ProviderEntry),
		Files:            make(map[string]*storage.File),
		Status:           debridTypes.TorrentStatusDownloaded,
		IsComplete:       true,
		AddedOn:          now,
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	if magnet := utils.ConstructMagnet(infohash, item.Name); magnet != nil {
		entry.Magnet = magnet.Link
	}

	pe := &storage.ProviderEntry{
		Provider:  provider,
		Status:    debridTypes.TorrentStatusDownloaded,
		RemovedAt: &now,
		Files:     make(map[string]*storage.ProviderFile),
		AddedAt:   now,
	}

	var size int64
	for name, f := range item.Files {
		if f.InfoHash != "" && !strings.EqualFold(f.InfoHash, infohash) {
			continue
		}
		entry.Files[name] = &storage.File{
			ID:        f.ID,
			Name:      name,
			Size:      f.Size,
			Deleted:   f.Deleted,
			InfoHash:  infohash,
			AddedOn:   f.AddedOn,
			ByteRange: f.ByteRange,
		}
		pe.Files[name] = &storage.ProviderFile{}
		size += f.Size
	}
	if len(entry.Files) == 0 {
		return nil
	}
	entry.Size = size
	entry.Bytes = size
	if provider != "" {
		entry.Providers[provider] = pe
	}

	if err := m.AddOrUpdate(entry, func(t *storage.Entry) { m.InvalidateEntryCache() }); err != nil {
		m.logger.Warn().Err(err).Str("folder", item.Name).Str("infohash", infohash).Msg("failed to save recovered orphan entry")
		return nil
	}
	m.logger.Info().Str("folder", item.Name).Str("infohash", infohash).Msg("recovered orphan entry from catalog item (will re-add to provider on stream)")
	return entry
}

func (m *Manager) recoverOrphanByInfoHash(infohash string) *storage.Entry {
	var matchedItem *storage.EntryItem
	_ = m.storage.ForEachEntryItem(func(item *storage.EntryItem) error {
		for _, f := range item.Files {
			if strings.EqualFold(f.InfoHash, infohash) {
				matchedItem = item
				return fmt.Errorf("found")
			}
		}
		return nil
	})
	if matchedItem != nil {
		return m.recoverOrphanFromItem(matchedItem, infohash)
	}
	return nil
}

// ReconcileCatalogOrphans scans the name index (EntryItems) and recovers any items
// whose infohash is missing from active storage entries.
func (m *Manager) ReconcileCatalogOrphans() int {
	type orphanCandidate struct {
		item *storage.EntryItem
		hash string
	}
	var candidates []orphanCandidate

	_ = m.storage.ForEachEntryItem(func(item *storage.EntryItem) error {
		for _, f := range item.Files {
			if f == nil || f.InfoHash == "" || !looksLikeInfohash(f.InfoHash) {
				continue
			}
			if ok, _ := m.EntryExists(f.InfoHash); !ok {
				candidates = append(candidates, orphanCandidate{item: item, hash: f.InfoHash})
				break
			}
		}
		return nil
	})

	recoveredCount := 0
	for _, c := range candidates {
		if m.recoverOrphanFromItem(c.item, c.hash) != nil {
			recoveredCount++
		}
	}
	if recoveredCount > 0 {
		m.logger.Info().Int("recovered", recoveredCount).Msg("reconciled missing catalog entries into orphaned storage")
		m.InvalidateEntryCache()
	}
	return recoveredCount
}

func (m *Manager) AddOrUpdate(entry *storage.Entry, callback func(t *storage.Entry)) error {
	entry.UpdatedAt = time.Now()
	if err := m.storage.AddOrUpdate(entry); err != nil {
		return err
	}
	// Keep .strm files derived state: any post-completion update (repair,
	// refresh, provider switch) re-syncs them. Cheap and idempotent.
	if entry.IsComplete {
		m.strm.SyncEntryAsync(entry)
	}
	if callback != nil {
		go callback(entry)
	}
	return nil
}

// GetEntry gets a torrent by name
func (m *Manager) GetEntry(infohash string) (*storage.Entry, error) {
	torr, err := m.storage.Get(infohash)
	if (err != nil || torr == nil) && looksLikeInfohash(infohash) {
		if recovered := m.recoverOrphanByInfoHash(infohash); recovered != nil {
			return recovered, nil
		}
	}
	return torr, err
}

func (m *Manager) EntryExists(infohash string) (bool, error) {
	return m.storage.Exists(infohash)
}

func (m *Manager) GetTorrents(filter func(*storage.Entry) bool) ([]*storage.Entry, error) {
	// Use streaming to avoid loading all torrents into memory at once
	var torrents []*storage.Entry
	err := m.storage.ForEach(func(t *storage.Entry) error {
		if filter == nil || filter(t) {
			torrents = append(torrents, t)
		}
		return nil
	})
	return torrents, err
}

func (m *Manager) GetTorrentsCount() (int, error) {
	return m.storage.Count()
}

// DeleteEntry deletes a torrent by infohash
func (m *Manager) DeleteEntry(infohash string, removePlacements bool) error {
	torr, err := m.GetEntry(infohash)
	if err != nil {
		return err
	}
	// Delete active placements from debrid clients
	if removePlacements {
		go m.RemoveTorrentPlacements(torr)
	}

	if err := m.storage.Delete(infohash); err != nil {
		return err
	}
	m.strm.RemoveEntryAsync(torr)
	// Refresh entry cache
	m.InvalidateEntryCache()
	go func() {
		if err := m.RefreshMount(); err != nil {
			m.logger.Error().Err(err).Msg("Mount refresh after entry deletion failed")
		}
	}()
	return nil
}

func (m *Manager) DeleteTorrents(infohashes []string, removeFromDebrid bool) error {
	for _, infohash := range infohashes {
		if err := m.DeleteEntry(infohash, removeFromDebrid); err != nil {
			return err
		}
	}
	return nil
}

func (m *Manager) GetMigrationJob(jobID string) (*storage.SwitcherJob, error) {
	job, exists := m.migrationJobs.Load(jobID)
	if !exists {
		return nil, fmt.Errorf("migration job not found: %s", jobID)
	}
	return job, nil
}

// SubmitJob submits an import to the unified active-download queue.
func (m *Manager) SubmitJob(job *Job) error {
	if m.jobQueue == nil {
		return fmt.Errorf("active download queue not initialized")
	}
	return m.jobQueue.Submit(job)
}
