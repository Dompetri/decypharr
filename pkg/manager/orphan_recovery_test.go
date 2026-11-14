package manager

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/puzpuzpuz/xsync/v4"
	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	debrid "github.com/sirrobot01/decypharr/pkg/debrid/common"
	"github.com/sirrobot01/decypharr/pkg/storage"
)

func TestRecoverOrphanFromItem(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	dbPath := filepath.Join(t.TempDir(), "db")
	store, err := storage.NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	m := &Manager{
		storage: store,
		clients: xsync.NewMap[string, debrid.Client](),
		logger:  zerolog.Nop(),
		config:  config.Get(),
	}
	m.entry = NewEntryCache(m)

	infoHash := "afac356370d82a07ebe7806d7c69a204c0491560"
	folderName := "Haywire (2) (1).mkv"
	fileName := "Haywire (2) (1).mkv"

	// Create an EntryItem in storage without a corresponding entry in s.entries (simulates wiped/pruned DB)
	item := &storage.EntryItem{
		Name: folderName,
		Files: map[string]*storage.File{
			fileName: {
				Name:     fileName,
				Size:     26573229821,
				InfoHash: infoHash,
				AddedOn:  time.Now(),
			},
		},
		Size: 26573229821,
	}
	if err := store.UpdateItem(item); err != nil {
		t.Fatalf("failed to put EntryItem: %v", err)
	}

	// Verify entry does NOT exist in entries store
	exists, err := store.Exists(infoHash)
	if err != nil {
		t.Fatal(err)
	}
	if exists {
		t.Fatal("entry should not exist in s.entries before recovery")
	}

	// 1. Test GetEntryByName recovers the missing entry
	entry, err := m.GetEntryByName(folderName, fileName)
	if err != nil {
		t.Fatalf("GetEntryByName failed: %v", err)
	}
	if entry == nil {
		t.Fatal("expected recovered entry, got nil")
	}
	if entry.InfoHash != infoHash {
		t.Fatalf("recovered infohash = %s, want %s", entry.InfoHash, infoHash)
	}
	if entry.Name != folderName {
		t.Fatalf("recovered name = %s, want %s", entry.Name, folderName)
	}
	if len(entry.Files) != 1 {
		t.Fatalf("recovered files count = %d, want 1", len(entry.Files))
	}

	// Verify entry was persisted into s.entries
	exists, err = store.Exists(infoHash)
	if err != nil || !exists {
		t.Fatalf("recovered entry was not persisted to s.entries: exists=%v err=%v", exists, err)
	}

	// Verify the recovered entry is marked evicted (ready for lazy rehydration)
	if !m.IsEvicted(entry) {
		t.Fatal("recovered orphan entry should be marked as evicted for lazy rehydration")
	}

	// 2. Test GetFile fallback with duplicate suffixes
	f, err := entry.GetFile("Haywire.mkv")
	if err != nil {
		t.Fatalf("GetFile('Haywire.mkv') should match 'Haywire (2) (1).mkv': %v", err)
	}
	if f == nil || f.Size != 26573229821 {
		t.Fatalf("unexpected file returned: %#v", f)
	}
}

func TestReconcileCatalogOrphans(t *testing.T) {
	config.Reset()
	config.SetConfigPath(t.TempDir())
	t.Cleanup(config.Reset)

	dbPath := filepath.Join(t.TempDir(), "db")
	store, err := storage.NewStorage(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })

	m := &Manager{
		storage: store,
		clients: xsync.NewMap[string, debrid.Client](),
		logger:  zerolog.Nop(),
		config:  config.Get(),
	}
	m.entry = NewEntryCache(m)

	// Create 3 items in EntryItem, only 1 in s.entries
	hash1 := "1111111111111111111111111111111111111111"
	hash2 := "2222222222222222222222222222222222222222"
	hash3 := "3333333333333333333333333333333333333333"

	item1 := &storage.EntryItem{
		Name: "Movie.1.2021",
		Files: map[string]*storage.File{
			"movie1.mkv": {Name: "movie1.mkv", Size: 1000, InfoHash: hash1, AddedOn: time.Now()},
		},
		Size: 1000,
	}
	item2 := &storage.EntryItem{
		Name: "Movie.2.2022",
		Files: map[string]*storage.File{
			"movie2.mkv": {Name: "movie2.mkv", Size: 2000, InfoHash: hash2, AddedOn: time.Now()},
		},
		Size: 2000,
	}
	item3 := &storage.EntryItem{
		Name: "Movie.3.2023",
		Files: map[string]*storage.File{
			"movie3.mkv": {Name: "movie3.mkv", Size: 3000, InfoHash: hash3, AddedOn: time.Now()},
		},
		Size: 3000,
	}

	_ = store.UpdateItem(item1)
	_ = store.UpdateItem(item2)
	_ = store.UpdateItem(item3)

	// Persist only entry1 in s.entries
	entry1 := &storage.Entry{
		InfoHash: hash1,
		Name:     "Movie.1.2021",
		Files:    map[string]*storage.File{"movie1.mkv": {Name: "movie1.mkv", Size: 1000, InfoHash: hash1}},
	}
	if err := store.AddOrUpdate(entry1); err != nil {
		t.Fatal(err)
	}

	// Reconcile
	recovered := m.ReconcileCatalogOrphans()
	if recovered != 2 {
		t.Fatalf("ReconcileCatalogOrphans() recovered = %d, want 2", recovered)
	}

	// Verify all 3 now exist
	for _, h := range []string{hash1, hash2, hash3} {
		ok, err := store.Exists(h)
		if err != nil || !ok {
			t.Fatalf("expected hash %s to exist in s.entries, exists=%v err=%v", h, ok, err)
		}
	}
}
