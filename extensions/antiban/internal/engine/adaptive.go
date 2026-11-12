package engine

import (
	"context"
	"sync"
)

type adaptiveLimiter struct {
	mu sync.Mutex

	max         int
	min         int
	current     int
	active      int
	maxObserved int

	decreaseFactor float64
	rampUpAfter    int
	rampUpStep     int
	successStreak  int

	notify chan struct{}
}

func newAdaptiveLimiter(cfg AdaptiveConfig) *adaptiveLimiter {
	return &adaptiveLimiter{
		max:            cfg.MaxConcurrency,
		min:            cfg.MinConcurrency,
		current:        cfg.InitialConcurrency,
		decreaseFactor: cfg.DecreaseFactor,
		rampUpAfter:    cfg.RampUpAfter,
		rampUpStep:     cfg.RampUpStep,
		notify:         make(chan struct{}),
	}
}

func (l *adaptiveLimiter) acquire(ctx context.Context) error {
	for {
		l.mu.Lock()
		if l.active < l.current {
			l.active++
			if l.active > l.maxObserved {
				l.maxObserved = l.active
			}
			l.mu.Unlock()
			return nil
		}
		notify := l.notify
		l.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

func (l *adaptiveLimiter) release() {
	l.mu.Lock()
	l.active--
	l.signalLocked()
	l.mu.Unlock()
}

func (l *adaptiveLimiter) onThrottle() {
	l.mu.Lock()
	defer l.mu.Unlock()

	next := int(float64(l.current) * l.decreaseFactor)
	if next >= l.current {
		next = l.current - 1
	}
	if next < l.min {
		next = l.min
	}
	l.current = next
	l.successStreak = 0
}

func (l *adaptiveLimiter) onSuccess() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.current >= l.max {
		l.successStreak = 0
		return
	}
	l.successStreak++
	if l.successStreak < l.rampUpAfter {
		return
	}
	l.current += l.rampUpStep
	if l.current > l.max {
		l.current = l.max
	}
	l.successStreak = 0
	l.signalLocked()
}

func (l *adaptiveLimiter) snapshot() (current, active, maxObserved int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.current, l.active, l.maxObserved
}

func (l *adaptiveLimiter) signalLocked() {
	close(l.notify)
	l.notify = make(chan struct{})
}
