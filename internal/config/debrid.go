package config

import (
	"errors"
	"fmt"
	"runtime"
)

type Debrid struct {
	Provider                     string   `json:"provider,omitempty"` // realdebrid, alldebrid, debridlink, torbox, premiumize, pikpak
	// For pikpak, APIKey holds the credentials in "username:password" format.
	Name                         string   `json:"name,omitempty"`
	APIKey                       string   `json:"api_key,omitempty"`
	DownloadAPIKeys              []string `json:"download_api_keys,omitempty"`
	DownloadUncached             bool     `json:"download_uncached,omitempty"`
	RateLimit                    string   `json:"rate_limit,omitempty"` // 200/minute or 10/second
	RepairRateLimit              string   `json:"repair_rate_limit,omitempty"`
	DownloadRateLimit            string   `json:"download_rate_limit,omitempty"`
	Proxy                        string   `json:"proxy,omitempty"`
	// StreamDirect uses Proxy for the API and link RESOLUTION only, streaming the
	// bytes DIRECTLY (no proxy). PikPak decides a link's region cap at resolve time,
	// so resolving through an Asia proxy yields the un-throttled "original" tier
	// which then downloads at full speed direct — avoiding a slow/costly proxy on
	// the bulk transfer. No effect when Proxy is empty.
	StreamDirect                 bool     `json:"stream_direct,omitempty"`
	UnpackRar                    bool     `json:"unpack_rar,omitempty"`
	MinimumFreeSlot              int      `json:"minimum_free_slot,omitempty"` // Minimum active pots to use this debrid
	Limit                        int      `json:"limit,omitempty"`             // Maximum number of total torrents
	TorrentsRefreshInterval      string   `json:"torrents_refresh_interval,omitempty"`
	DownloadLinksRefreshInterval string   `json:"download_links_refresh_interval,omitempty"`
	Workers                      int      `json:"workers,omitempty"`
	AutoExpireLinksAfter         string   `json:"auto_expire_links_after,omitempty"`
	UserAgent                    string   `json:"user_agent,omitempty"`

	// Eviction frees remote storage on quota-limited providers (e.g. PikPak) by
	// deleting the actual files when usage crosses a threshold, while keeping the
	// torrent known locally so the mount path still resolves and the magnet can be
	// re-added on the next read (lazy re-hydration). Disabled by default.
	Eviction Eviction `json:"eviction,omitempty"`

	// RehydrateOrphaned keeps a torrent known locally when its file vanishes from
	// the provider without us evicting it — e.g. after switching PikPak accounts,
	// or an out-of-band delete on the provider. Instead of pruning the entry
	// (which would break the *arr / media-server symlink still pointing at it), the
	// vanished placement is marked for lazy re-hydration: the mount path keeps
	// resolving and the next read re-adds the magnet onto the *current* account.
	// Only single-provider, completed torrents are preserved. Disabled by default.
	RehydrateOrphaned bool `json:"rehydrate_orphaned,omitempty"`

	// Folder
	Folder        string `json:"folder,omitempty"`          // Deprecated. Use Mount MountPath instead.
	FolderNaming  string `json:"folder_naming,omitempty"`   // Deprecated. Use global setting instead.
	RcUrl         string `json:"rc_url,omitempty"`          // Deprecated. Use global setting instead.
	RcUser        string `json:"rc_user,omitempty"`         // Deprecated. Use global setting instead.
	RcPass        string `json:"rc_pass,omitempty"`         // Deprecated. Use global setting instead.
	RcRefreshDirs string `json:"rc_refresh_dirs,omitempty"` // Deprecated. Use global setting instead.

	// Directories
	Directories map[string]WebdavDirectories `json:"directories,omitempty"` // Deprecated. Use global setting instead.
}

// Eviction configures quota-based eviction + lazy re-hydration for a debrid.
type Eviction struct {
	Enabled              bool   `json:"enabled,omitempty"`                // Master switch (default false)
	HighWaterPercent     int    `json:"high_water_percent,omitempty"`     // Start evicting when usage% exceeds this (default 90)
	LowWaterPercent      int    `json:"low_water_percent,omitempty"`      // Evict (oldest-first) down to this usage% (default 75)
	CheckInterval        string `json:"check_interval,omitempty"`         // How often to check quota (default 15m)
	RehydrateWaitSeconds int    `json:"rehydrate_wait_seconds,omitempty"` // Max seconds a read blocks while re-adding (default 20)

	// Probe-slice cache: retain this many MB of each evicted file's head (and a
	// small tail) on local disk so metadata reads (ffprobe header/end-of-file
	// seeks) are served locally without re-adding. 0 disables the guard (any read
	// of an evicted file re-hydrates). Defaults: 16 MB head, 1 MB tail.
	HeaderCacheMB int `json:"header_cache_mb,omitempty"`
	TailCacheMB   int `json:"tail_cache_mb,omitempty"`

	// Archive ingestion: evict every downloaded torrent down to its probe slices
	// as soon as it's cached (ignoring the water marks), and drip-feed queued
	// magnets into the provider while usage stays below FeedTargetPercent. Lets a
	// library far larger than the quota be kept as probe slices and re-hydrated on
	// play. Magnets are queued via POST /api/eviction/ingest.
	Archive           bool   `json:"archive,omitempty"`
	FeedTargetPercent int    `json:"feed_target_percent,omitempty"` // feed magnets while usage% is below this (default 70)
	FeedBatch         int    `json:"feed_batch,omitempty"`          // magnets submitted per check interval (default 5)
	HotWindow         string `json:"hot_window,omitempty"`          // don't archive titles streamed within this window (default "2h"; "0" to disable)
}

func (c *Config) updateDebrid(d Debrid) Debrid {
	workers := runtime.NumCPU() * 50
	perDebrid := workers / len(c.Debrids)

	if d.Provider == "" {
		d.Provider = d.Name
	}

	var downloadKeys []string

	if len(d.DownloadAPIKeys) > 0 {
		downloadKeys = d.DownloadAPIKeys
	} else {
		// If no download API keys are specified, use the main API key
		downloadKeys = []string{d.APIKey}
	}
	d.DownloadAPIKeys = downloadKeys

	if d.TorrentsRefreshInterval == "" {
		d.TorrentsRefreshInterval = DefaultTorrentsRefreshInterval
	}
	if d.DownloadLinksRefreshInterval == "" {
		d.DownloadLinksRefreshInterval = DefaultDownloadsRefreshInterval
	}
	if d.Workers == 0 {
		d.Workers = perDebrid
	}
	if d.AutoExpireLinksAfter == "" {
		d.AutoExpireLinksAfter = DefaultAutoExpireLinksAfter
	}

	if d.Eviction.Enabled {
		if d.Eviction.HighWaterPercent <= 0 || d.Eviction.HighWaterPercent > 100 {
			d.Eviction.HighWaterPercent = 90
		}
		if d.Eviction.LowWaterPercent <= 0 || d.Eviction.LowWaterPercent >= d.Eviction.HighWaterPercent {
			d.Eviction.LowWaterPercent = 75
		}
		if d.Eviction.CheckInterval == "" {
			d.Eviction.CheckInterval = "15m"
		}
		if d.Eviction.RehydrateWaitSeconds <= 0 {
			d.Eviction.RehydrateWaitSeconds = 20
		}
		// Guard is ON by default when eviction is enabled: unset (0) -> 16 MB head.
		// Set a negative value to disable the probe cache (any read re-hydrates).
		switch {
		case d.Eviction.HeaderCacheMB == 0:
			d.Eviction.HeaderCacheMB = 16
		case d.Eviction.HeaderCacheMB < 0:
			d.Eviction.HeaderCacheMB = 0
		}
		if d.Eviction.HeaderCacheMB > 0 && d.Eviction.TailCacheMB <= 0 {
			d.Eviction.TailCacheMB = 1
		}
		if d.Eviction.Archive {
			if d.Eviction.FeedTargetPercent <= 0 || d.Eviction.FeedTargetPercent >= 100 {
				d.Eviction.FeedTargetPercent = 70
			}
			if d.Eviction.FeedBatch <= 0 {
				d.Eviction.FeedBatch = 5
			}
			if d.Eviction.HotWindow == "" {
				d.Eviction.HotWindow = "2h"
			}
		}
	}

	return d
}

func validateDebrids(debrids []Debrid) error {
	if len(debrids) == 0 {
		return nil
	}

	for _, debrid := range debrids {
		// Basic field validation
		if debrid.APIKey == "" {
			return errors.New("debrid api key is required")
		}
	}

	return nil
}

func (c *Config) applyDebridEnvVars() {
	// Debrid providers array
	for i := range 10 { // Support up to 10 debrid providers
		prefix := fmt.Sprintf("DEBRIDS__%d__", i)
		if val := getEnv(prefix + "NAME"); val != "" {
			// Ensure array is large enough
			if i >= len(c.Debrids) {
				c.Debrids = append(c.Debrids, make([]Debrid, i-len(c.Debrids)+1)...)
			}
			c.Debrids[i].Name = val

			// Set other debrid fields
			if apiKey := getEnv(prefix + "API_KEY"); apiKey != "" {
				c.Debrids[i].APIKey = apiKey
			}
			if folder := getEnv(prefix + "FOLDER"); folder != "" {
				c.Debrids[i].Folder = folder
			}
			if provider := getEnv(prefix + "PROVIDER"); provider != "" {
				c.Debrids[i].Provider = provider
			}
			if proxy := getEnv(prefix + "PROXY"); proxy != "" {
				c.Debrids[i].Proxy = proxy
			}
			if v := getEnv(prefix + "STREAM_DIRECT"); v != "" {
				c.Debrids[i].StreamDirect = v == "true" || v == "1"
			}
		}
	}
}
