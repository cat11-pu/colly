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
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// ErrSnapshotNotFound is returned by SnapshotStore implementations when no
// snapshot exists for the requested key.
var ErrSnapshotNotFound = errors.New("snapshot not found")

// ResourceSnapshot holds the three kinds of validation information used by
// incremental crawling:
//   - ETag: the server provided validator ("not modified marker"), echoed
//     back through the If-None-Match request header.
//   - LastModified: the server provided last modification timestamp, echoed
//     back through the If-Modified-Since request header.
//   - Fingerprint: a client side content fingerprint (hash of the response
//     body) which detects changes even when the server offers no validators.
//
// Key identifies the resource the snapshot belongs to; it is normally the
// requested URL. UpdatedAt records when the snapshot was last refreshed.
type ResourceSnapshot struct {
	Key          string `json:"key"`
	ETag         string `json:"etag,omitempty"`
	LastModified string `json:"last_modified,omitempty"`
	Fingerprint  string `json:"fingerprint,omitempty"`
	UpdatedAt    int64  `json:"updated_at"`
}

// SnapshotStore persists ResourceSnapshot entries between crawler runs.
// All implementations must be safe for concurrent use because a Collector
// performs requests from multiple goroutines in async mode.
type SnapshotStore interface {
	// SnapshotInit prepares the store (opens files, replays logs, etc.).
	// It must be callable exactly once before any other method.
	SnapshotInit() error
	// GetSnapshot returns the snapshot stored under key or
	// ErrSnapshotNotFound when no snapshot exists.
	GetSnapshot(key string) (*ResourceSnapshot, error)
	// PutSnapshot stores (insert or replace) the snapshot.
	PutSnapshot(snapshot *ResourceSnapshot) error
	// Close flushes buffered data and releases underlying resources.
	Close() error
}

// InMemorySnapshotStore keeps snapshots in memory. It is concurrency safe but
// does not survive process restarts; use FileSnapshotStore for persistence.
type InMemorySnapshotStore struct {
	snapshots map[string]*ResourceSnapshot
	lock      *sync.RWMutex
}

// SnapshotInit implements SnapshotStore.
func (s *InMemorySnapshotStore) SnapshotInit() error {
	if s.snapshots == nil {
		s.snapshots = make(map[string]*ResourceSnapshot)
	}
	if s.lock == nil {
		s.lock = &sync.RWMutex{}
	}
	return nil
}

// GetSnapshot implements SnapshotStore.
func (s *InMemorySnapshotStore) GetSnapshot(key string) (*ResourceSnapshot, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	snapshot, ok := s.snapshots[key]
	if !ok {
		return nil, ErrSnapshotNotFound
	}
	copy := *snapshot
	return &copy, nil
}

// PutSnapshot implements SnapshotStore.
func (s *InMemorySnapshotStore) PutSnapshot(snapshot *ResourceSnapshot) error {
	if snapshot == nil {
		return errors.New("snapshot must not be nil")
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	copy := *snapshot
	s.snapshots[snapshot.Key] = &copy
	return nil
}

// Close implements SnapshotStore.
func (s *InMemorySnapshotStore) Close() error {
	return nil
}

// defaultFileSnapshotCompactionEntries is the threshold at which the append
// log is compacted on Close when no explicit value is configured.
const defaultFileSnapshotCompactionEntries = 10000

// FileSnapshotStore is a persistent SnapshotStore.
//
// Snapshots are kept in an in-memory index for fast reads and appended as
// JSON lines to a write-ahead log on disk for durability. Once the log grows
// beyond CompactionEntries records it is atomically rewritten with only the
// latest state of every key, keeping the file bounded while preserving
// crash safety (compaction writes to a temp file and renames it into place).
//
// Every method is guarded by a RWMutex so concurrent collectors/goroutines
// can read and write snapshots safely.
type FileSnapshotStore struct {
	// Path is the location of the snapshot log file. Required.
	Path string
	// CompactionEntries triggers log compaction on Close once the number of
	// appended records reaches this value. Zero uses the default.
	CompactionEntries int

	snapshots       map[string]*ResourceSnapshot
	lock            *sync.RWMutex
	file            *os.File
	writer          *bufio.Writer
	appendedRecords int
}

// SnapshotInit implements SnapshotStore. It opens (or creates) the backing
// file and replays any previously persisted records into the memory index.
func (s *FileSnapshotStore) SnapshotInit() error {
	if s.Path == "" {
		return errors.New("FileSnapshotStore: Path is required")
	}
	if s.CompactionEntries <= 0 {
		s.CompactionEntries = defaultFileSnapshotCompactionEntries
	}
	s.lock = &sync.RWMutex{}
	s.snapshots = make(map[string]*ResourceSnapshot)

	if dir := filepath.Dir(s.Path); dir != "." && dir != "" {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return err
		}
	}
	file, err := os.OpenFile(s.Path, os.O_CREATE|os.O_RDWR, 0o640)
	if err != nil {
		return err
	}
	s.file = file

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		snapshot := &ResourceSnapshot{}
		if err := json.Unmarshal(line, snapshot); err != nil {
			continue
		}
		s.snapshots[snapshot.Key] = snapshot
		s.appendedRecords++
	}
	if err := scanner.Err(); err != nil {
		file.Close()
		return err
	}

	s.writer = bufio.NewWriter(file)
	return nil
}

// GetSnapshot implements SnapshotStore.
func (s *FileSnapshotStore) GetSnapshot(key string) (*ResourceSnapshot, error) {
	s.lock.RLock()
	defer s.lock.RUnlock()
	snapshot, ok := s.snapshots[key]
	if !ok {
		return nil, ErrSnapshotNotFound
	}
	copy := *snapshot
	return &copy, nil
}

// PutSnapshot implements SnapshotStore.
func (s *FileSnapshotStore) PutSnapshot(snapshot *ResourceSnapshot) error {
	if snapshot == nil {
		return errors.New("snapshot must not be nil")
	}
	s.lock.Lock()
	defer s.lock.Unlock()

	copy := *snapshot
	s.snapshots[snapshot.Key] = &copy
	line, err := json.Marshal(&copy)
	if err != nil {
		return err
	}
	line = append(line, '\n')
	if _, err := s.writer.Write(line); err != nil {
		return err
	}
	if err := s.writer.Flush(); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return err
	}

	s.appendedRecords++
	return nil
}

// Close flushes pending writes and closes the backing file.
func (s *FileSnapshotStore) Close() error {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.file == nil {
		return nil
	}
	if err := s.writer.Flush(); err != nil {
		s.file.Close()
		return err
	}
	if err := s.file.Close(); err != nil {
		return err
	}
	s.file = nil
	s.writer = nil
	// Rewrite the log with one record per key when it has accumulated enough
	// history to make the rewrite worthwhile.
	if s.appendedRecords >= s.CompactionEntries {
		return s.rewriteLocked()
	}
	return nil
}

// rewriteLocked atomically rewrites the log containing only the latest
// snapshot per key.
// Callers must hold s.lock.
func (s *FileSnapshotStore) rewriteLocked() error {
	tmpPath := s.Path + ".tmp"
	// A leftover temp file from a crashed previous run must not be appended to.
	_ = os.Remove(tmpPath)
	tmp, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(tmp)
	for _, snapshot := range s.snapshots {
		line, err := json.Marshal(snapshot)
		if err != nil {
			tmp.Close()
			return err
		}
		if _, err := writer.Write(append(line, '\n')); err != nil {
			tmp.Close()
			return err
		}
	}
	if err := writer.Flush(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, s.Path); err != nil {
		return err
	}
	return nil
}
