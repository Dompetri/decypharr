package utils

import (
	"path/filepath"
	"regexp"
	"strings"
	"unicode/utf8"
)

// yearRegex matches a 4-digit release year (1900-2099) as a standalone token, so
// it ignores resolutions (1080/2160) and digit runs embedded in other tokens.
var yearRegex = regexp.MustCompile(`(^|[^0-9])(19|20)\d{2}([^0-9]|$)`)

// dupSuffixRe matches a duplicate-copy suffix "(N)" appended directly before the
// dupSuffixRe strips one or more duplicate copy suffixes like "(1)", "(2)", " (2) (1)"
// while preserving 4-digit years like "(2022)" by limiting copy numbers to 1-2 digits (\d{1,2}).
var dupSuffixRe = regexp.MustCompile(`(?:\s*\(\d{1,2}\))+\s*(\.[^./\\]+)?$`)

// NormalizeDupName removes duplicate copy suffixes: "name (2) (1).mkv" -> "name.mkv". Used to
// recognise the same logical file across re-add copies that a debrid (PikPak) makes.
func NormalizeDupName(name string) string {
	return dupSuffixRe.ReplaceAllString(name, "$1")
}

// HasYear reports whether name contains a parseable release year. *arr rejects a
// download it can't parse, and a missing year is the usual cause, so this is used
// to decide whether a clean file name is usable as-is or needs the release name.
func HasYear(name string) bool {
	return yearRegex.MatchString(name)
}

// videoExtensions is a set of known video file extensions (lowercase, without dot)
var videoExtensions = map[string]struct{}{
	"webm": {}, "m4v": {}, "3gp": {}, "nsv": {}, "ty": {},
	"rm": {}, "rmvb": {}, "ifo": {}, "mov": {}, "qt": {},
	"divx": {}, "xvid": {}, "bivx": {}, "nrg": {}, "pva": {}, "wmv": {},
	"asf": {}, "asx": {}, "ogm": {}, "ogv": {}, "m2v": {}, "avi": {},
	"bin": {}, "dat": {}, "dvr-ms": {}, "mpg": {}, "mpeg": {}, "mp4": {},
	"avc": {}, "vp3": {}, "svq3": {}, "nuv": {}, "viv": {}, "dv": {},
	"fli": {}, "flv": {}, "wpl": {}, "vob": {}, "mkv": {}, "mk3d": {},
	"ts": {}, "wtv": {}, "m2ts": {},
}

// mediaExtensions is a set of known media file extensions (lowercase, without dot)
var mediaExtensions = func() map[string]struct{} {
	m := map[string]struct{}{
		"strm": {}, "m3u": {},
		// Audio
		"mp2": {}, "mp3": {}, "m4a": {}, "m4b": {}, "m4p": {}, "ogg": {},
		"oga": {}, "opus": {}, "wma": {}, "wav": {}, "wv": {}, "flac": {},
		"ape": {}, "aif": {}, "aiff": {}, "aifc": {},
	}
	for ext := range videoExtensions {
		m[ext] = struct{}{}
	}
	return m
}()

func RemoveInvalidChars(value string) string {
	return strings.Map(func(r rune) rune {
		if r == filepath.Separator || r == ':' {
			return r
		}
		if filepath.IsAbs(string(r)) {
			return r
		}
		if strings.ContainsRune(filepath.VolumeName("C:"+string(r)), r) {
			return r
		}
		if r < 32 || strings.ContainsRune(`<>:"/\|?*`, r) {
			return -1
		}
		return r
	}, value)
}

// SafeFolderName converts an arbitrary release/torrent name into a single, safe
// filesystem path component. Path separators are replaced with spaces (so a
// dual-title release like "A / B [2025]" stays one folder instead of nesting),
// reserved and control characters are stripped, whitespace is collapsed, and the
// result is capped to stay within the common 255-byte name limit. The year and
// quality tokens that *arr needs for parsing sit near the front, so they survive.
func SafeFolderName(name string) string {
	name = strings.NewReplacer("/", " ", "\\", " ").Replace(name)
	name = strings.Map(func(r rune) rune {
		if r < 32 || strings.ContainsRune(`<>:"|?*`, r) {
			return -1
		}
		return r
	}, name)
	name = strings.Join(strings.Fields(name), " ")
	const maxBytes = 240
	if len(name) > maxBytes {
		b := []byte(name)[:maxBytes]
		// Trim back to a valid UTF-8 boundary so multi-byte runes aren't split.
		for len(b) > 0 && !utf8.ValidString(string(b)) {
			b = b[:len(b)-1]
		}
		name = strings.TrimSpace(string(b))
	}
	return name
}

func RemoveExtension(value string) string {
	ext := filepath.Ext(value)
	if ext == "" {
		return value
	}
	// Remove the leading dot and lowercase for lookup
	extLower := strings.ToLower(ext[1:])
	if _, ok := mediaExtensions[extLower]; ok {
		name := value[:len(value)-len(ext)]
		if name != "" && name != "." {
			return name
		}
	}
	return value
}

func IsMediaFile(path string) bool {
	ext := filepath.Ext(path)
	if ext == "" {
		return false
	}
	extLower := strings.ToLower(ext[1:])
	_, ok := mediaExtensions[extLower]
	return ok
}

func IsVideoFile(path string) bool {
	ext := filepath.Ext(path)
	if ext == "" {
		return false
	}
	_, ok := videoExtensions[strings.ToLower(ext[1:])]
	return ok
}
