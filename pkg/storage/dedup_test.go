package storage

import (
	"testing"
	"time"
)

func TestMergeFilesCollapsesReAddCopies(t *testing.T) {
	base := time.Date(2026, 6, 22, 14, 40, 0, 0, time.UTC)
	existing := map[string]*File{
		"Code.Name.Banshee.2022.BDRemux.1080p(2).mkv": {Name: "Code.Name.Banshee.2022.BDRemux.1080p(2).mkv", Size: 21075169042, AddedOn: base},
		"Code.Name.Banshee.2022.BDRemux.1080p(1).mkv": {Name: "Code.Name.Banshee.2022.BDRemux.1080p(1).mkv", Size: 21075169042, AddedOn: base.Add(time.Hour)},
	}
	incoming := map[string]*File{
		"Code.Name.Banshee.2022.BDRemux.1080p.mkv": {Name: "Code.Name.Banshee.2022.BDRemux.1080p.mkv", Size: 21075169042, AddedOn: base.Add(2 * time.Hour)},
	}

	merged := mergeFiles(existing, incoming)
	if len(merged) != 1 {
		t.Fatalf("expected 1 file after collapsing copies, got %d: %v", len(merged), keys(merged))
	}
	f, ok := merged["Code.Name.Banshee.2022.BDRemux.1080p.mkv"]
	if !ok {
		t.Fatalf("expected clean name key, got %v", keys(merged))
	}
	if !f.AddedOn.Equal(base.Add(2 * time.Hour)) {
		t.Errorf("expected newest copy to win, got AddedOn=%v", f.AddedOn)
	}
}

func TestMergeFilesKeepsDistinctMultiFile(t *testing.T) {
	now := time.Now()
	existing := map[string]*File{
		"Movie.CD1.mkv": {Name: "Movie.CD1.mkv", Size: 100, AddedOn: now},
	}
	incoming := map[string]*File{
		"Movie.CD2.mkv": {Name: "Movie.CD2.mkv", Size: 200, AddedOn: now},
	}
	merged := mergeFiles(existing, incoming)
	if len(merged) != 2 {
		t.Fatalf("multi-file torrent collapsed: got %d: %v", len(merged), keys(merged))
	}
}

func keys(m map[string]*File) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
