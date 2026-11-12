//go:build unix

package queue

import (
	"fmt"
	"os"
	"sync"
	"syscall"
)

type processLock struct {
	path string
	mu   sync.Mutex
	file *os.File
}

func newProcessLock(path string) *processLock {
	return &processLock{path: path}
}

func (l *processLock) Lock() error {
	l.mu.Lock()
	file, err := l.open()
	if err != nil {
		l.mu.Unlock()
		return err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		l.mu.Unlock()
		return fmt.Errorf("queue: flock: %w", err)
	}
	return nil
}

func (l *processLock) Unlock() error {
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("queue: funlock: %w", err)
	}
	return nil
}

func (l *processLock) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	err := l.file.Close()
	l.file = nil
	return err
}

func (l *processLock) open() (*os.File, error) {
	if l.file != nil {
		return l.file, nil
	}
	file, err := os.OpenFile(l.path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("queue: open lock file: %w", err)
	}
	l.file = file
	return file, nil
}

func processAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}
