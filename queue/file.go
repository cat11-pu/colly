package queue

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly/v2"
)

const (
	fileQueuePendingDir  = "pending"
	fileQueueInflightDir = "inflight"
	fileQueueStartedDir  = "started"
	fileQueueDoneDir     = "done"
	fileQueueTempDir     = "tmp"
)

// ErrQueueEmpty is returned by FileQueueStorage.GetRequest when there are no
// pending requests and no requests are currently being processed.
var ErrQueueEmpty = errors.New("queue is empty")

// AckStorage is an optional extension of Storage. When a queue uses an
// implementation of AckStorage, it calls AckRequest after a request attempt so
// durable backends can commit the completed work.
type AckStorage interface {
	Storage
	// AckRequest acknowledges a request previously returned by GetRequest.
	// Implementations should make the acknowledgement idempotent.
	AckRequest(request []byte) error
}

// StartStorage is an optional extension of Storage. The queue calls
// StartRequest immediately before attempting a request. Durable backends can
// use this marker to distinguish a claimed-but-never-started request, which
// should be requeued after a crash, from a started request which must not be
// requested again.
type StartStorage interface {
	Storage
	StartRequest(request []byte) error
}

// FileQueueStorage is a durable queue storage which persists requests on disk.
//
// Multiple Queue instances, including Queue instances in separate processes,
// can share the same Directory. Requests are deduplicated by normalized URL
// and request body, and an inflight request left behind by a stopped process is
// returned to the pending queue on recovery unless its HTTP attempt had already
// started.
//
// The directory must be on a filesystem which provides atomic rename and
// cross-process file locking. A local filesystem is recommended. For a
// multi-machine setup, use a shared POSIX filesystem with working flock
// support, or provide a custom Storage implementation backed by a shared
// service.
//
// Completed URLs are represented by marker files in Directory/done and are not
// requested again by any Queue using the same Directory. Use a new Directory
// for a new crawl.
type FileQueueStorage struct {
	// Directory is the directory where queue state is stored.
	Directory string
	// MaxSize limits the number of pending and inflight requests. Values <= 0
	// mean no limit.
	MaxSize int
	// LeaseDuration controls how long an inflight request owned by another
	// machine is protected before it can be recovered. Processes on the same
	// machine are recovered immediately when their PID is no longer alive.
	// Values <= 0 select the default of five minutes.
	LeaseDuration time.Duration
	// PollInterval controls how often GetRequest checks for work owned by
	// another live queue instance. Values <= 0 select the default of 10ms.
	PollInterval time.Duration

	initMu      sync.Mutex
	opMu        sync.Mutex
	lock        *processLock
	initialized bool
	token       string
	hostHash    string
	pid         int
	claims      map[string]string
}

// NewFileQueueStorage creates a durable queue storage rooted at directory.
// Initialization happens lazily and is also performed by queue.New.
func NewFileQueueStorage(directory string) *FileQueueStorage {
	return &FileQueueStorage{Directory: directory}
}

// NewPersistentStorage is an alias for NewFileQueueStorage.
func NewPersistentStorage(directory string) *FileQueueStorage {
	return NewFileQueueStorage(directory)
}

// PersistentStorage is an alias for FileQueueStorage.
type PersistentStorage = FileQueueStorage

// NewPersistent creates a Queue backed by a FileQueueStorage at directory.
func NewPersistent(threads int, directory string) (*Queue, error) {
	return New(threads, NewFileQueueStorage(directory))
}

// Init creates the on-disk layout and registers this process-local storage
// owner. It is safe to call Init more than once.
func (s *FileQueueStorage) Init() error {
	s.initMu.Lock()
	defer s.initMu.Unlock()
	if s.initialized {
		return nil
	}
	if s.Directory == "" {
		return errors.New("queue: empty file storage directory")
	}
	for _, dir := range []string{
		fileQueuePendingDir,
		fileQueueInflightDir,
		fileQueueStartedDir,
		fileQueueDoneDir,
		fileQueueTempDir,
	} {
		if err := os.MkdirAll(filepath.Join(s.Directory, dir), 0750); err != nil {
			return fmt.Errorf("queue: create storage directory: %w", err)
		}
	}
	s.pid = os.Getpid()
	host, err := os.Hostname()
	if err != nil {
		host = "unknown"
	}
	s.hostHash = shortHash(host)
	s.token = randomToken()
	s.lock = newProcessLock(filepath.Join(s.Directory, "queue.lock"))
	if s.claims == nil {
		s.claims = make(map[string]string)
	}
	activeFileQueueOwners.Store(s.token, struct{}{})
	s.initialized = true
	return nil
}

var activeFileQueueOwners sync.Map

// Close releases this storage owner. It does not delete queued state, so a
// later Init on the same directory can continue the crawl. Inflight requests
// owned by a closed owner are recovered by the next queue operation.
func (s *FileQueueStorage) Close() error {
	s.initMu.Lock()
	defer s.initMu.Unlock()
	if !s.initialized {
		return nil
	}
	activeFileQueueOwners.Delete(s.token)
	if s.lock != nil {
		if err := s.lock.Close(); err != nil {
			return err
		}
		s.lock = nil
	}
	s.claims = make(map[string]string)
	s.initialized = false
	return nil
}

// AddRequest implements Storage. A request with the same normalized URL and
// body as an existing pending, inflight, or completed request is ignored.
func (s *FileQueueStorage) AddRequest(request []byte) error {
	id, err := fileQueueRequestID(request)
	if err != nil {
		return err
	}
	return s.withState(func() error {
		if s.existsLocked(id) {
			return nil
		}
		if s.MaxSize > 0 {
			size, err := s.sizeLocked()
			if err != nil {
				return err
			}
			if size >= s.MaxSize {
				return colly.ErrQueueFull
			}
		}
		return s.writeAtomicLocked(filepath.Join(s.Directory, fileQueuePendingDir, id+".json"), request)
	})
}

// GetRequest implements Storage. It atomically moves a pending request to the
// inflight state before returning it.
func (s *FileQueueStorage) GetRequest() ([]byte, error) {
	if err := s.Init(); err != nil {
		return nil, err
	}
	interval := s.PollInterval
	if interval <= 0 {
		interval = 10 * time.Millisecond
	}
	for {
		var (
			request []byte
			size    int
		)
		err := s.withState(func() error {
			var err error
			size, err = s.sizeLocked()
			if err != nil || size == 0 {
				return err
			}
			id, path, err := s.nextPendingLocked()
			if err != nil || path == "" {
				return err
			}
			request, err = os.ReadFile(path)
			if err != nil {
				return fmt.Errorf("queue: read pending request: %w", err)
			}
			claimPath := s.claimPath(id)
			if err := os.Rename(path, claimPath); err != nil {
				return fmt.Errorf("queue: claim pending request: %w", err)
			}
			s.claims[id] = claimPath
			return nil
		})
		if err != nil {
			return nil, err
		}
		if request != nil {
			return request, nil
		}
		if size == 0 {
			return nil, ErrQueueEmpty
		}
		time.Sleep(interval)
	}
}

// AckRequest implements AckStorage. It atomically records the request as
// completed so it will not be queued or requested again.
func (s *FileQueueStorage) AckRequest(request []byte) error {
	id, err := fileQueueRequestID(request)
	if err != nil {
		return err
	}
	return s.withState(func() error {
		claimPath := s.claims[id]
		if claimPath == "" {
			claimPath = s.findInflightLocked(id)
		}
		if claimPath == "" {
			if exists, err := fileExists(filepath.Join(s.Directory, fileQueueDoneDir, id+".json")); err != nil {
				return err
			} else if exists {
				return nil
			}
			return fmt.Errorf("queue: acknowledged request %s is not inflight", id)
		}
		if err := s.writeAtomicLocked(filepath.Join(s.Directory, fileQueueDoneDir, id+".json"), []byte{}); err != nil {
			return err
		}
		if err := os.Remove(claimPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("queue: remove inflight request: %w", err)
		}
		if err := os.Remove(filepath.Join(s.Directory, fileQueueStartedDir, id+".json")); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("queue: remove started marker: %w", err)
		}
		delete(s.claims, id)
		return nil
	})
}

// StartRequest implements StartStorage. It records that the request attempt
// has started. If the process dies after this marker is written, recovery
// treats the request as attempted rather than requeueing it and creating a
// duplicate HTTP request.
func (s *FileQueueStorage) StartRequest(request []byte) error {
	id, err := fileQueueRequestID(request)
	if err != nil {
		return err
	}
	return s.withState(func() error {
		return s.writeAtomicLocked(filepath.Join(s.Directory, fileQueueStartedDir, id+".json"), []byte{})
	})
}

// QueueSize implements Storage. It counts pending and currently owned inflight
// requests, but not completed requests. Counting live inflight work lets a
// queue instance in one process wait for work being processed by another
// process before it reports that the shared crawl is complete.
func (s *FileQueueStorage) QueueSize() (int, error) {
	var size int
	err := s.withState(func() error {
		var err error
		size, err = s.sizeLocked()
		return err
	})
	return size, err
}

func (s *FileQueueStorage) withState(fn func() error) error {
	if err := s.Init(); err != nil {
		return err
	}
	s.opMu.Lock()
	defer s.opMu.Unlock()
	if err := s.lock.Lock(); err != nil {
		return fmt.Errorf("queue: lock storage: %w", err)
	}
	defer s.lock.Unlock()
	if err := s.recoverLocked(); err != nil {
		return err
	}
	return fn()
}

func (s *FileQueueStorage) recoverLocked() error {
	entries, err := os.ReadDir(filepath.Join(s.Directory, fileQueueInflightDir))
	if err != nil {
		return fmt.Errorf("queue: list inflight requests: %w", err)
	}
	now := time.Now()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		id := fileQueueIDFromName(entry.Name())
		done, err := fileExists(filepath.Join(s.Directory, fileQueueDoneDir, id+".json"))
		if err != nil {
			return err
		}
		if done {
			if err := os.Remove(filepath.Join(s.Directory, fileQueueInflightDir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("queue: clean completed inflight request: %w", err)
			}
			if err := os.Remove(filepath.Join(s.Directory, fileQueueStartedDir, id+".json")); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("queue: clean completed started marker: %w", err)
			}
			delete(s.claims, id)
			continue
		}
		owner, ok := parseFileQueueOwner(entry.Name())
		if !ok || s.ownerActive(owner, now) {
			continue
		}
		inflightPath := filepath.Join(s.Directory, fileQueueInflightDir, entry.Name())
		started, err := fileExists(filepath.Join(s.Directory, fileQueueStartedDir, id+".json"))
		if err != nil {
			return err
		}
		if started {
			if err := s.writeAtomicLocked(filepath.Join(s.Directory, fileQueueDoneDir, id+".json"), []byte{}); err != nil {
				return err
			}
			if err := os.Remove(inflightPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("queue: complete interrupted request: %w", err)
			}
			if err := os.Remove(filepath.Join(s.Directory, fileQueueStartedDir, id+".json")); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("queue: remove interrupted started marker: %w", err)
			}
			delete(s.claims, id)
			continue
		}
		pendingPath := filepath.Join(s.Directory, fileQueuePendingDir, id+".json")
		if exists, err := fileExists(pendingPath); err != nil {
			return err
		} else if exists {
			if err := os.Remove(inflightPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("queue: remove duplicate inflight request: %w", err)
			}
		} else if err := os.Rename(inflightPath, pendingPath); err != nil {
			return fmt.Errorf("queue: recover inflight request: %w", err)
		}
		delete(s.claims, id)
	}
	return nil
}

func (s *FileQueueStorage) sizeLocked() (int, error) {
	pending, err := countQueueFiles(filepath.Join(s.Directory, fileQueuePendingDir))
	if err != nil {
		return 0, err
	}
	inflightEntries, err := os.ReadDir(filepath.Join(s.Directory, fileQueueInflightDir))
	if err != nil {
		return 0, fmt.Errorf("queue: list inflight requests: %w", err)
	}
	inflight := 0
	now := time.Now()
	for _, entry := range inflightEntries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		owner, ok := parseFileQueueOwner(entry.Name())
		if !ok || s.ownerActive(owner, now) {
			inflight++
		}
	}
	return pending + inflight, nil
}

func (s *FileQueueStorage) nextPendingLocked() (string, string, error) {
	entries, err := os.ReadDir(filepath.Join(s.Directory, fileQueuePendingDir))
	if err != nil {
		return "", "", fmt.Errorf("queue: list pending requests: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return "", "", nil
	}
	name := names[0]
	id := fileQueueIDFromName(name)
	return id, filepath.Join(s.Directory, fileQueuePendingDir, name), nil
}

func (s *FileQueueStorage) existsLocked(id string) bool {
	for _, dir := range []string{fileQueuePendingDir, fileQueueDoneDir} {
		exists, err := fileExists(filepath.Join(s.Directory, dir, id+".json"))
		if err == nil && exists {
			return true
		}
	}
	return s.findInflightLocked(id) != ""
}

func (s *FileQueueStorage) findInflightLocked(id string) string {
	entries, err := os.ReadDir(filepath.Join(s.Directory, fileQueueInflightDir))
	if err != nil {
		return ""
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), "-"+id+".json") {
			return filepath.Join(s.Directory, fileQueueInflightDir, entry.Name())
		}
	}
	return ""
}

func (s *FileQueueStorage) claimPath(id string) string {
	lease := time.Now().Add(s.leaseDuration()).UnixNano()
	name := fmt.Sprintf("%d-%s-%d-%s-%s.json", lease, s.hostHash, s.pid, s.token, id)
	return filepath.Join(s.Directory, fileQueueInflightDir, name)
}

func (s *FileQueueStorage) leaseDuration() time.Duration {
	if s.LeaseDuration > 0 {
		return s.LeaseDuration
	}
	return 5 * time.Minute
}

func (s *FileQueueStorage) ownerActive(owner fileQueueOwner, now time.Time) bool {
	if owner.hostHash == s.hostHash {
		if owner.pid == s.pid {
			if owner.token == s.token {
				return true
			}
			_, active := activeFileQueueOwners.Load(owner.token)
			return active
		}
		return processAlive(owner.pid) && now.Before(owner.lease)
	}
	return now.Before(owner.lease)
}

func (s *FileQueueStorage) writeAtomicLocked(path string, data []byte) error {
	tmp, err := os.CreateTemp(filepath.Join(s.Directory, fileQueueTempDir), "write-*.tmp")
	if err != nil {
		return fmt.Errorf("queue: create temporary file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0600); err != nil {
		tmp.Close()
		return fmt.Errorf("queue: chmod temporary file: %w", err)
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("queue: write temporary file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("queue: sync temporary file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("queue: close temporary file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("queue: commit file: %w", err)
	}
	return syncDirectory(filepath.Dir(path))
}

type fileQueueOwner struct {
	hostHash string
	pid      int
	token    string
	lease    time.Time
}

func parseFileQueueOwner(name string) (fileQueueOwner, bool) {
	base := strings.TrimSuffix(name, ".json")
	parts := strings.Split(base, "-")
	if len(parts) != 5 {
		return fileQueueOwner{}, false
	}
	lease, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return fileQueueOwner{}, false
	}
	pid, err := strconv.Atoi(parts[2])
	if err != nil {
		return fileQueueOwner{}, false
	}
	return fileQueueOwner{
		hostHash: parts[1],
		pid:      pid,
		token:    parts[3],
		lease:    time.Unix(0, lease),
	}, true
}

func fileQueueIDFromName(name string) string {
	base := strings.TrimSuffix(name, ".json")
	parts := strings.Split(base, "-")
	if len(parts) == 5 {
		return parts[4]
	}
	return base
}

func fileQueueRequestID(request []byte) (string, error) {
	var serialized struct {
		URL  string
		Body []byte
	}
	if err := json.Unmarshal(request, &serialized); err != nil {
		return "", fmt.Errorf("queue: decode request for deduplication: %w", err)
	}
	parsed, err := urlParser.Parse(serialized.URL)
	if err != nil {
		return "", fmt.Errorf("queue: normalize request URL: %w", err)
	}
	h := sha256.New()
	_, _ = h.Write([]byte(parsed.Href(false)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(serialized.Body)
	return hex.EncodeToString(h.Sum(nil)), nil
}

func countQueueFiles(directory string) (int, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return 0, fmt.Errorf("queue: list queue directory: %w", err)
	}
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".json") {
			count++
		}
	}
	return count, nil
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, os.ErrNotExist):
		return false, nil
	default:
		return false, err
	}
}

func shortHash(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:4])
}

func randomToken() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err == nil {
		return hex.EncodeToString(buf[:])
	}
	return shortHash(fmt.Sprintf("%d-%d", time.Now().UnixNano(), os.Getpid()))
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return nil
	}
	defer dir.Close()
	_ = dir.Sync()
	return nil
}
