package storage

import (
	"cmp"
	"maps"

	"github.com/sirrobot01/decypharr/internal/config"
	"github.com/sirrobot01/decypharr/internal/utils"
)

// HandleExistingEntryMerge merges an incoming entry with an existing one that
// shares the same infohash. This preserves placements, files, and tags from
// the existing entry that the incoming entry may not know about.
func HandleExistingEntryMerge(existing, incoming *Entry) *Entry {
	if incoming.Protocol == config.ProtocolNZB {
		return incoming
	}
	incoming.Files = mergeFiles(existing.Files, incoming.Files)
	incoming.ActiveProvider = cmp.Or(incoming.ActiveProvider, existing.ActiveProvider)
	incoming.Providers = mergeProviders(existing.Providers, incoming.Providers)
	incoming.Tags = mergeTags(existing.Tags, incoming.Tags)

	return incoming
}

func mergeProviders(existing, incoming map[string]*ProviderEntry) map[string]*ProviderEntry {
	if existing == nil {
		return incoming
	}
	if incoming == nil {
		return existing
	}

	merged := maps.Clone(existing)
	for k, v := range incoming {
		if current, exists := merged[k]; !exists || v.AddedAt.After(current.AddedAt) {
			merged[k] = v
		}
	}

	return merged
}

func mergeFiles(existing, incoming map[string]*File) map[string]*File {
	if existing == nil {
		return incoming
	}
	if incoming == nil {
		return existing
	}

	merged := maps.Clone(existing)
	for k, v := range incoming {
		if current, exists := merged[k]; !exists || v.AddedOn.After(current.AddedOn) {
			merged[k] = v
		}
	}

	return dedupeFileCopies(merged)
}

// dedupeFileCopies collapses re-add copies of the same logical file that a debrid
// (notably PikPak) leaves behind: when it re-fetches a magnet it makes "name(1)",
// "name(2)" siblings rather than overwriting, and merging entry snapshots across
// re-adds otherwise accumulates them as phantom files. Files whose names match once
// the "(N)" suffix is stripped are folded to one entry (newest AddedOn wins) under
// the clean name. Distinct files of a multi-file torrent keep different names, so
// they are never collapsed.
func dedupeFileCopies(files map[string]*File) map[string]*File {
	out := make(map[string]*File, len(files))
	for _, f := range files {
		if f == nil {
			continue
		}
		clean := utils.NormalizeDupName(f.Name)
		if existing, ok := out[clean]; ok && !f.AddedOn.After(existing.AddedOn) {
			continue
		}
		nf := *f
		nf.Name = clean // stable name across re-adds; the placement link still resolves
		out[clean] = &nf
	}
	return out
}

func mergeTags(existing, incoming []string) []string {
	if len(existing) == 0 {
		return incoming
	}
	if len(incoming) == 0 {
		return existing
	}

	tagSet := make(map[string]bool)
	for _, tag := range existing {
		tagSet[tag] = true
	}
	for _, tag := range incoming {
		tagSet[tag] = true
	}

	merged := make([]string, 0, len(tagSet))
	for tag := range tagSet {
		merged = append(merged, tag)
	}
	return merged
}
