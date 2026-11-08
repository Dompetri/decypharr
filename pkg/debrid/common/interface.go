package common

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/debrid/account"
	"github.com/sirrobot01/decypharr/pkg/debrid/types"
)

type Client interface {
	SubmitMagnet(tr *types.Torrent) (*types.Torrent, error)
	CheckStatus(tr *types.Torrent) (*types.Torrent, error)
	GetDownloadLink(ctx context.Context, torrentID string, file *types.File) (types.DownloadLink, error)
	DeleteTorrent(torrentId string) error
	// IsAvailable returns one result for each checked, nonempty input hash.
	// Result keys retain the input spelling. Missing keys were not checked.
	// On failure, results contain only completed batches. Unsupported providers
	// return types.ErrAvailabilityUnsupported.
	IsAvailable(infohashes []string) (map[string]bool, error)
	UpdateTorrent(torrent *types.Torrent) error
	GetTorrent(torrentId string) (*types.Torrent, error)
	GetTorrents() ([]*types.Torrent, error)
	Config() config.Debrid
	Logger() zerolog.Logger
	RefreshDownloadLinks() error
	CheckFile(ctx context.Context, infohash, fileID string) error // fileID here can link, file id(in the case of torbox), etc.
	AccountManager() *account.Manager                             // Returns the active download account/token
	GetProfile() (*types.Profile, error)
	GetAvailableSlots() (int, error)
	SyncAccounts() // Updates each accounts details(like traffic, username, etc.)
	DeleteLink(dl types.DownloadLink) error
	SpeedTest(ctx context.Context) types.SpeedTestResult
	SupportsCheck() bool
}

// QuotaReporter is an optional capability implemented by providers that expose a
// finite storage quota (e.g. PikPak). The eviction sweep type-asserts a Client
// to this interface; providers without a storage quota simply don't implement it.
type QuotaReporter interface {
	// StorageQuota returns used and total bytes for the account's stored files.
	StorageQuota(ctx context.Context) (used, limit int64, err error)
}
