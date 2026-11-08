//go:build linux

package hanwen

import (
	"context"
	"os"
	"strconv"
	"strings"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// callerProcess resolves the process that issued a FUSE request from the kernel-
// supplied caller PID, reading its name (/proc/<pid>/comm) and full command line
// (/proc/<pid>/cmdline). It is how the DFS mount tells one reader from another —
// Jellyfin vs Plex, and a direct read vs an ffmpeg trickplay/transcode — since at
// the filesystem layer every access is just a read.
//
// ok is false / fields empty when the caller can't be resolved. The important
// case: when decypharr runs in a container WITHOUT pid=host, the caller PID is in
// the host PID namespace, which decypharr's /proc cannot see — so the reads fail
// and only the (host) pid is returned. Run decypharr with pid=host to resolve the
// process name. Capture this at Open(): the reads themselves are often issued by
// kernel readahead threads (unreliable PIDs), but open() is always done by the
// real calling process.
func callerProcess(ctx context.Context) (callerInfo, bool) {
	c, ok := fuse.FromContext(ctx)
	if !ok || c.Pid == 0 {
		return callerInfo{}, false
	}
	ci := callerInfo{Pid: c.Pid}
	base := "/proc/" + strconv.FormatUint(uint64(c.Pid), 10)
	if b, err := os.ReadFile(base + "/comm"); err == nil {
		ci.Comm = strings.TrimSpace(string(b))
	}
	if b, err := os.ReadFile(base + "/cmdline"); err == nil {
		// argv is NUL-separated; join for a readable single-line command.
		ci.Cmdline = strings.TrimSpace(strings.ReplaceAll(string(b), "\x00", " "))
	}
	return ci, true
}
