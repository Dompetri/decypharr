package manager

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

// The probe cache retains a small head (and tail) slice of each evicted file on
// decypharr's local disk. Metadata reads (ffprobe header/end-of-file seeks) are
// served from these slices without ever touching the remote provider, so a
// library scan can't trigger a re-hydration storm — only a read into the file
// body (real playback) does.

func (m *Manager) probeCacheDir() string {
	return filepath.Join(config.GetMainPath(), "db", "probecache")
}

// probeCacheKey is a stable filesystem-safe key for one file's retained slices.
func probeCacheKey(infohash, filename string) string {
	h := sha1.Sum([]byte(filename))
	return infohash + "_" + hex.EncodeToString(h[:8])
}

// cacheEntryProbeSlices fetches and stores the head (and tail) slices for every
// live file in the entry. Called at eviction time, before the remote files are
// deleted. Any failure is returned so the caller can skip evicting this entry
// (we never delete a file we couldn't make probe-serveable).
func (m *Manager) cacheEntryProbeSlices(ctx context.Context, entry *storage.Entry, headLen, tailLen int64) error {
	if headLen <= 0 {
		return nil
	}
	for name, f := range entry.Files {
		if f == nil || f.Deleted {
			continue
		}
		// Resolve the link directly from the provider — NOT via linkService.GetLink,
		// which runs the repair/refresh machinery (re-submits magnets, marks entries
		// bad, and can panic on a malformed entry). Archiving only needs to read a
		// few MB; on any problem we skip and retry next sweep.
		//
		// Retry transient failures (PikPak short-reads a just-downloaded file with
		// "unexpected EOF" until it settles) a few times with a fresh link each time.
		var lastErr error
		for attempt := 0; attempt < 3; attempt++ {
			if attempt > 0 {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(3 * time.Second):
				}
			}
			url, viaProxy, err := m.archiveDownloadURL(entry, name)
			if err != nil {
				lastErr = fmt.Errorf("get link for %s: %w", name, err)
				continue
			}
			if err := m.writeProbeCache(ctx, entry.ActiveProvider, entry.InfoHash, name, url, viaProxy, f.Size, headLen, tailLen); err != nil {
				lastErr = fmt.Errorf("cache %s: %w", name, err)
				continue
			}
			lastErr = nil
			break
		}
		if lastErr != nil {
			return lastErr
		}
	}
	return nil
}

func (m *Manager) writeProbeCache(ctx context.Context, provider, infohash, filename, link string, viaProxy bool, size, headLen, tailLen int64) error {
	if link == "" {
		return fmt.Errorf("empty download link")
	}
	dir := m.probeCacheDir()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	key := probeCacheKey(infohash, filename)

	head := min(headLen, size)
	if head > 0 {
		if err := m.fetchRangeToFile(ctx, provider, link, viaProxy, 0, head-1, filepath.Join(dir, key+".head")); err != nil {
			return err
		}
	}
	// Tail only when the file extends past the head slice (covers MP4 moov-at-end
	// and MKV cues that ffprobe seeks to).
	if tailLen > 0 && size > headLen {
		t := min(tailLen, size-headLen)
		if t > 0 {
			if err := m.fetchRangeToFile(ctx, provider, link, viaProxy, size-t, size-1, filepath.Join(dir, key+".tail")); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *Manager) fetchRangeToFile(ctx context.Context, provider, link string, viaProxy bool, start, end int64, path string) error {
	resp, err := m.doRequest(ctx, provider, link, start, end, viaProxy)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return fmt.Errorf("unexpected status %d fetching probe slice", resp.StatusCode)
	}
	// Unique temp file in the same dir so concurrent writers/cleanups can't collide
	// on a fixed ".tmp" name.
	out, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmp := out.Name()
	want := end - start + 1
	n, copyErr := io.CopyN(out, resp.Body, want)
	closeErr := out.Close()
	if copyErr != nil && copyErr != io.EOF {
		_ = os.Remove(tmp)
		return copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmp)
		return closeErr
	}
	if n < want {
		// Short read (e.g. provider returned fewer bytes); keep what we got — it
		// still covers the very start which is what ffprobe needs most.
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

// serveProbeFromCache serves a read entirely from the locally-retained head/tail
// slices when the requested range falls inside one of them. Returns served=false
// (without error) when the range needs body bytes, so the caller re-hydrates.
func (m *Manager) serveProbeFromCache(infohash, filename string, size, start, end int64, writer io.Writer, onReady StreamReadyFunc) (served bool, err error) {
	if size <= 0 {
		return false, nil
	}
	if end < 0 || end >= size {
		end = size - 1
	}
	if start < 0 {
		start = 0
	}
	if start > end {
		return false, nil
	}

	key := probeCacheKey(infohash, filename)
	headPath := filepath.Join(m.probeCacheDir(), key+".head")
	tailPath := filepath.Join(m.probeCacheDir(), key+".tail")

	var headLen, tailLen int64
	if fi, statErr := os.Stat(headPath); statErr == nil {
		headLen = fi.Size()
	}
	if fi, statErr := os.Stat(tailPath); statErr == nil {
		tailLen = fi.Size()
	}

	switch {
	case headLen > 0 && start < headLen:
		// Range starts inside the head slice. Serve the covered portion only
		// (clipped to the slice). An open-ended/large request (e.g. ffprobe's
		// "bytes=0-") gets the head bytes and a Content-Range showing the true
		// size; a client that needs more issues a fresh range beyond the slice,
		// which then re-hydrates. So header probes never trigger a re-add.
		servedEnd := min(end, headLen-1)
		return true, m.serveCacheSlice(headPath, start, start, servedEnd, size, writer, onReady)
	case tailLen > 0 && start >= size-tailLen:
		// Range starts inside the retained tail slice (end-of-file seeks).
		off := start - (size - tailLen)
		return true, m.serveCacheSlice(tailPath, off, start, end, size, writer, onReady)
	default:
		// Body read (real playback) — let the caller re-hydrate.
		return false, nil
	}
}

func (m *Manager) serveCacheSlice(path string, fileOffset, start, end, size int64, writer io.Writer, onReady StreamReadyFunc) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	length := end - start + 1
	if onReady != nil {
		meta := &StreamMetadata{
			StatusCode:    http.StatusOK,
			ContentLength: length,
			Header:        http.Header{},
		}
		meta.Header.Set("Content-Type", "application/octet-stream")
		meta.Header.Set("Content-Length", strconv.FormatInt(length, 10))
		if start > 0 || end < size-1 {
			meta.StatusCode = http.StatusPartialContent
			meta.Header.Set("Content-Range", buildContentRange(start, end, size))
		}
		if readyErr := onReady(meta); readyErr != nil {
			return readyErr
		}
	}

	if _, err := f.Seek(fileOffset, io.SeekStart); err != nil {
		return err
	}
	if _, err := io.CopyN(writer, f, length); err != nil && err != io.EOF {
		return err
	}
	return nil
}

// ProbeCovers reports whether [start,end] is fully served by the locally retained
// head or tail slice (so the caller can read it without re-hydrating). Used by the
// FUSE read path to keep metadata/header reads from triggering a re-add.
func (m *Manager) ProbeCovers(infohash, filename string, start, end, size int64) bool {
	if size <= 0 {
		return false
	}
	if end < 0 || end >= size {
		end = size - 1
	}
	if start < 0 {
		start = 0
	}
	if start > end {
		return false
	}
	key := probeCacheKey(infohash, filename)
	var headLen, tailLen int64
	if fi, err := os.Stat(filepath.Join(m.probeCacheDir(), key+".head")); err == nil {
		headLen = fi.Size()
	}
	if fi, err := os.Stat(filepath.Join(m.probeCacheDir(), key+".tail")); err == nil {
		tailLen = fi.Size()
	}
	if headLen > 0 && end < headLen {
		return true
	}
	if tailLen > 0 && start >= size-tailLen {
		return true
	}
	return false
}

// clearProbeCache removes all retained slices for a torrent (after re-hydration
// or deletion).
func (m *Manager) clearProbeCache(infohash string) {
	// Only remove finalized slices, never in-flight ".tmp" files.
	for _, suffix := range []string{"_*.head", "_*.tail"} {
		matches, _ := filepath.Glob(filepath.Join(m.probeCacheDir(), infohash+suffix))
		for _, p := range matches {
			_ = os.Remove(p)
		}
	}
}
