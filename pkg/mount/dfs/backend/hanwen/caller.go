//go:build linux || (darwin && amd64)

package hanwen

import (
	"strconv"
	"strings"
)

// callerInfo describes the process that issued a FUSE request.
type callerInfo struct {
	Pid     uint32
	Comm    string // /proc/<pid>/comm, e.g. "jellyfin", "ffmpeg", "Plex Transcoder"
	Cmdline string // full argv, space-joined
}

// label is the compact "process/kind" identifier used for stream tracking/stats,
// e.g. "jellyfin-ffmpeg/trickplay". Falls back to the pid when the process name
// couldn't be resolved.
func (ci callerInfo) label() string {
	name := ci.Comm
	if name == "" {
		name = "pid" + strconv.FormatUint(uint64(ci.Pid), 10)
	}
	return name + "/" + classifyCall(ci)
}

// classifyCall coarsely labels what a caller is doing, from its process name and
// command line. These heuristics are a STARTING POINT — the exact ffmpeg argument
// shapes for Jellyfin trickplay vs transcode aren't confirmed yet, so refine the
// ffmpeg branches from real cmdlines captured in the "DFS open" debug logs.
//
//	"probe"     - ffprobe metadata scan
//	"trickplay" - ffmpeg generating scrub thumbnails (whole-file, bandwidth-heavy)
//	"transcode" - ffmpeg HLS/DASH transcode for playback
//	"ffmpeg"    - ffmpeg, purpose not yet recognized
//	"read"      - plain read (server direct-play / metadata), no ffmpeg
func classifyCall(ci callerInfo) string {
	comm := strings.ToLower(ci.Comm)
	cl := strings.ToLower(ci.Cmdline)
	switch {
	case strings.Contains(comm, "ffprobe"):
		return "probe"
	case strings.Contains(comm, "ffmpeg") || strings.Contains(comm, "transcoder") || strings.Contains(cl, "ffmpeg"):
		switch {
		case strings.Contains(cl, "trickplay") || strings.Contains(cl, "image2") ||
			strings.Contains(cl, "tile=") || strings.Contains(cl, ".jpg") || strings.Contains(cl, ".bif"):
			return "trickplay"
		case strings.Contains(cl, "hls") || strings.Contains(cl, "segment") || strings.Contains(cl, "dash"):
			return "transcode"
		default:
			return "ffmpeg"
		}
	default:
		return "read"
	}
}
