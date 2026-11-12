// Copyright 2018 Adam Tauber
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestInMemorySnapshotStoreRoundTrip(t *testing.T) {
	store := &InMemorySnapshotStore{}
	if err := store.SnapshotInit(); err != nil {
		t.Fatalf("SnapshotInit failed: %v", err)
	}

	if _, err := store.GetSnapshot("missing"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("expected ErrSnapshotNotFound, got %v", err)
	}

	original := &ResourceSnapshot{
		Key:          "https://example.com/page",
		ETag:         `"v1"`,
		LastModified: "Wed, 21 Oct 2015 07:28:00 GMT",
		Fingerprint:  "abc123",
		UpdatedAt:    42,
	}
	if err := store.PutSnapshot(original); err != nil {
		t.Fatalf("PutSnapshot failed: %v", err)
	}

	got, err := store.GetSnapshot(original.Key)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}
	if *got != *original {
		t.Fatalf("snapshot mismatch: got %+v want %+v", got, original)
	}

	// Mutating the returned snapshot must not affect stored state.
	got.Fingerprint = "tampered"
	again, err := store.GetSnapshot(original.Key)
	if err != nil {
		t.Fatalf("GetSnapshot failed: %v", err)
	}
	if again.Fingerprint != original.Fingerprint {
		t.Fatalf("store returned shared/mutable state: %+v", again)
	}

	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

func TestFileSnapshotStorePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshots.jsonl")

	first := &FileSnapshotStore{Path: path}
	if err := first.SnapshotInit(); err != nil {
		t.Fatalf("SnapshotInit failed: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := first.PutSnapshot(&ResourceSnapshot{
			Key:          fmt.Sprintf("https://example.com/%d", i),
			ETag:         fmt.Sprintf(`"etag-%d"`, i),
			LastModified: "Wed, 21 Oct 2015 07:28:00 GMT",
			Fingerprint:  fmt.Sprintf("fp-%d", i),
			UpdatedAt:    int64(i),
		}); err != nil {
			t.Fatalf("PutSnapshot failed: %v", err)
		}
	}
	// Overwrite one key to verify only the latest value survives a replay.
	if err := first.PutSnapshot(&ResourceSnapshot{
		Key:         "https://example.com/1",
		ETag:        `"etag-1-updated"`,
		Fingerprint: "fp-1-updated",
		UpdatedAt:   99,
	}); err != nil {
		t.Fatalf("PutSnapshot overwrite failed: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	second := &FileSnapshotStore{Path: path}
	if err := second.SnapshotInit(); err != nil {
		t.Fatalf("reopen SnapshotInit failed: %v", err)
	}
	defer second.Close()

	got, err := second.GetSnapshot("https://example.com/1")
	if err != nil {
		t.Fatalf("GetSnapshot after reopen failed: %v", err)
	}
	if got.Fingerprint != "fp-1-updated" || got.ETag != `"etag-1-updated"` {
		t.Fatalf("overwritten snapshot not replayed correctly: %+v", got)
	}
	if _, err := second.GetSnapshot("https://example.com/4"); err != nil {
		t.Fatalf("snapshot missing after reopen: %v", err)
	}
}

func TestFileSnapshotStoreCompaction(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshots.jsonl")
	store := &FileSnapshotStore{Path: path, CompactionEntries: 10}
	if err := store.SnapshotInit(); err != nil {
		t.Fatalf("SnapshotInit failed: %v", err)
	}

	// Repeatedly overwrite the same key; compaction must keep every key intact.
	for round := 0; round < 25; round++ {
		for i := 0; i < 3; i++ {
			if err := store.PutSnapshot(&ResourceSnapshot{
				Key:         fmt.Sprintf("k%d", i),
				Fingerprint: fmt.Sprintf("k%d-r%d", i, round),
				UpdatedAt:   int64(round),
			}); err != nil {
				t.Fatalf("PutSnapshot failed: %v", err)
			}
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	reopened := &FileSnapshotStore{Path: path, CompactionEntries: 10}
	if err := reopened.SnapshotInit(); err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer reopened.Close()

	for i := 0; i < 3; i++ {
		got, err := reopened.GetSnapshot(fmt.Sprintf("k%d", i))
		if err != nil {
			t.Fatalf("key k%d lost after compaction: %v", i, err)
		}
		want := fmt.Sprintf("k%d-r24", i)
		if got.Fingerprint != want {
			t.Fatalf("k%d: got fingerprint %q want %q", i, got.Fingerprint, want)
		}
	}

	// The compacted log must hold one record per key, not the full history.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read compacted log failed: %v", err)
	}
	if lines := strings.Count(strings.TrimRight(string(data), "\n"), "\n") + 1; lines != 3 {
		t.Fatalf("compacted log should hold 3 records, got %d", lines)
	}
}

func TestSnapshotStoreConcurrentAccess(t *testing.T) {
	stores := map[string]SnapshotStore{
		"memory": &InMemorySnapshotStore{},
		"file":   &FileSnapshotStore{Path: filepath.Join(t.TempDir(), "concurrent.jsonl")},
	}

	for name, store := range stores {
		t.Run(name, func(t *testing.T) {
			if err := store.SnapshotInit(); err != nil {
				t.Fatalf("SnapshotInit failed: %v", err)
			}
			defer store.Close()

			const writers = 16
			const iterations = 100
			var wg sync.WaitGroup
			for w := 0; w < writers; w++ {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for i := 0; i < iterations; i++ {
						key := fmt.Sprintf("writer-%d", id%4)
						if err := store.PutSnapshot(&ResourceSnapshot{
							Key:         key,
							Fingerprint: fmt.Sprintf("%s-%d", key, i),
							UpdatedAt:   int64(i),
						}); err != nil {
							t.Errorf("PutSnapshot failed: %v", err)
							return
						}
						if _, err := store.GetSnapshot(key); err != nil && !errors.Is(err, ErrSnapshotNotFound) {
							t.Errorf("GetSnapshot failed: %v", err)
							return
						}
					}
				}(w)
			}
			wg.Wait()
		})
	}
}

func TestFileSnapshotStoreRequiresPath(t *testing.T) {
	store := &FileSnapshotStore{}
	if err := store.SnapshotInit(); err == nil {
		t.Fatal("expected error when Path is empty")
	}
}
