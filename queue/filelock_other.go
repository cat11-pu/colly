//go:build !unix

package queue

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

// The fallback lock uses an exclusively created lock file. It is adequate for
// platforms without flock, but a crashed process can leave the lock in place
// briefly until staleLockAge expires.
type processLock struct {
	path string
	mu   sync.Mutex
	file *os.File
}

const staleLockAge = 30 * time.Second

func newProcessLock(path string) *processLock {
	return &processLock{path: path}
}

func (l *processLock) Lock() error {
	l.mu.Lock()
	for {
		file, err := os.OpenFile(l.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err == nil {
			l.file = file
			return nil
		}
		if !errors.Is(err, os.ErrExist) {
			l.mu.Unlock()
			return fmt.Errorf("queue: create lock file: %w", err)
		}
		if info, statErr := os.Stat(l.path); statErr == nil && time.Since(info.ModTime()) > staleLockAge {
			_ = os.Remove(l.path)
			continue
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (l *processLock) Unlock() error {
	defer l.mu.Unlock()
	if l.file != nil {
		if err := l.file.Close(); err != nil {
			return err
		}
		l.file = nil
	}
	if err := os.Remove(l.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("queue: remove lock file: %w", err)
	}
	return nil
}

func (l *processLock) Close() error {
	return nil
}

func processAlive(pid int) bool {
	return pid > 0
}
