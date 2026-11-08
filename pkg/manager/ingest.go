package manager

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	debridTypes "github.com/sirrobot01/decypharr/pkg/debrid/types"
)

// The archive-ingest queue is a simple newline-delimited file of magnet links.
// The feeder (run from the eviction sweep in archive mode) drips magnets into the
// provider while usage stays below the feed target; the archive sweep then evicts
// each one to its probe slices as soon as it finishes downloading.

const ingestSubmitTTL = 30 * 60 // seconds to suppress re-submitting before sync creates the entry

func (m *Manager) ingestQueuePath() string {
	return filepath.Join(config.GetMainPath(), "db", "ingest_queue.txt")
}

func (m *Manager) readQueue() []string {
	f, err := os.Open(m.ingestQueuePath())
	if err != nil {
		return nil
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1024*1024), 1024*1024) // magnets can be long
	for sc.Scan() {
		if l := strings.TrimSpace(sc.Text()); l != "" {
			lines = append(lines, l)
		}
	}
	return lines
}

func (m *Manager) writeQueue(lines []string) error {
	path := m.ingestQueuePath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strings.Join(lines, "\n")), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// EnqueueMagnets appends valid magnet links to the ingest queue and returns how
// many were added and the new queue length.
func (m *Manager) EnqueueMagnets(magnets []string) (added, total int, err error) {
	m.ingestMu.Lock()
	defer m.ingestMu.Unlock()

	lines := m.readQueue()
	for _, mg := range magnets {
		mg = strings.TrimSpace(mg)
		if !strings.HasPrefix(strings.ToLower(mg), "magnet:") {
			continue
		}
		lines = append(lines, mg)
		added++
	}
	if err := m.writeQueue(lines); err != nil {
		return 0, 0, err
	}
	return added, len(lines), nil
}

func (m *Manager) IngestQueueLen() int {
	m.ingestMu.Lock()
	defer m.ingestMu.Unlock()
	return len(m.readQueue())
}

// dequeueMagnets removes and returns up to n magnets from the front of the queue.
func (m *Manager) dequeueMagnets(n int) []string {
	m.ingestMu.Lock()
	defer m.ingestMu.Unlock()
	lines := m.readQueue()
	if len(lines) == 0 {
		return nil
	}
	if n > len(lines) {
		n = len(lines)
	}
	taken := lines[:n]
	_ = m.writeQueue(lines[n:])
	return taken
}

// requeueMagnets appends magnets back to the queue (e.g. a transient submit error).
func (m *Manager) requeueMagnets(magnets []string) {
	if len(magnets) == 0 {
		return
	}
	m.ingestMu.Lock()
	defer m.ingestMu.Unlock()
	_ = m.writeQueue(append(m.readQueue(), magnets...))
}

// alreadyIngested reports whether a magnet's torrent is already known (a live or
// evicted entry) or was submitted very recently (entry not yet synced).
func (m *Manager) alreadyIngested(infohash string) bool {
	if infohash == "" {
		return false
	}
	if e, _ := m.GetEntry(infohash); e != nil {
		return true
	}
	if ts, ok := m.ingestSubmitted.Load(infohash); ok && time.Now().Unix()-ts < ingestSubmitTTL {
		return true
	}
	return false
}

// feedMagnets submits queued magnets to the provider while usage is below the
// feed target. Called from the archive sweep. Each submitted torrent downloads on
// the provider and is later archived (evicted to probe slices) by the sweep.
func (m *Manager) feedMagnets(ctx context.Context, cfg config.Debrid, client debrid.Client) {
	if m.IngestQueueLen() == 0 {
		return
	}
	reporter, ok := client.(debrid.QuotaReporter)
	if !ok {
		return
	}
	used, limit, err := reporter.StorageQuota(ctx)
	if err != nil || limit <= 0 {
		return
	}
	usedPct := float64(used) / float64(limit) * 100
	if usedPct >= float64(cfg.Eviction.FeedTargetPercent) {
		m.logger.Debug().Str("debrid", cfg.Name).Float64("used_pct", usedPct).Msg("ingest: at feed target, pausing")
		return
	}

	batch := m.dequeueMagnets(cfg.Eviction.FeedBatch)
	if len(batch) == 0 {
		return
	}

	fed := 0
	var requeue []string
	for _, mg := range batch {
		select {
		case <-ctx.Done():
			requeue = append(requeue, mg)
			continue
		default:
		}
		mi, err := utils.GetMagnetInfo(mg, m.config.AlwaysRmTrackerUrls)
		if err != nil || mi == nil || mi.InfoHash == "" {
			m.logger.Warn().Str("magnet", truncate(mg, 60)).Msg("ingest: invalid magnet, dropping")
			continue
		}
		ih := strings.ToLower(mi.InfoHash)
		if m.alreadyIngested(ih) {
			continue // already on the provider / recently submitted
		}
		dbt := &debridTypes.Torrent{
			Name:             mi.Name,
			InfoHash:         ih,
			Magnet:           mi,
			Files:            make(map[string]debridTypes.File),
			DownloadUncached: cfg.DownloadUncached, // ingest must fully download to cache the slice
		}
		if _, serr := client.SubmitMagnet(dbt); serr != nil {
			m.logger.Warn().Err(serr).Str("infohash", ih).Msg("ingest: submit failed, requeueing")
			requeue = append(requeue, mg)
			continue
		}
		m.ingestSubmitted.Store(ih, time.Now().Unix())
		fed++
	}
	m.requeueMagnets(requeue)

	m.logger.Info().
		Str("debrid", cfg.Name).
		Int("fed", fed).
		Int("remaining", m.IngestQueueLen()).
		Float64("used_pct", usedPct).
		Msg("ingest: submitted batch")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
