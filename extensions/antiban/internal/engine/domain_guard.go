package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ErrDomainPaused is returned when a host circuit breaker is open. Callers
// can retry after the reported pause time.
var ErrDomainPaused = errors.New("domain paused by antiban circuit breaker")

type domainGuard struct {
	mu      sync.Mutex
	domains map[string]*domainState
	cfg     DomainGuardConfig
	opens   atomic.Uint64
}

type domainState struct {
	windowStart time.Time
	requests    int
	failures    int
	openUntil   time.Time
	probing     bool
	notify      chan struct{}
}

type guardTicket struct {
	host  string
	probe bool
}

func newDomainGuard(cfg DomainGuardConfig) *domainGuard {
	return &domainGuard{
		domains: make(map[string]*domainState),
		cfg:     cfg,
	}
}

func (g *domainGuard) before(ctx context.Context, host string) (*guardTicket, error) {
	for {
		g.mu.Lock()
		state := g.stateLocked(host)
		now := time.Now()

		if !state.openUntil.IsZero() && now.Before(state.openUntil) {
			if !g.cfg.WaitOnPause {
				until := state.openUntil
				g.mu.Unlock()
				return nil, fmt.Errorf("%w: %s until %s", ErrDomainPaused, host, until.Format(time.RFC3339))
			}
			wait := time.Until(state.openUntil)
			notify := state.notify
			g.mu.Unlock()
			if err := waitContext(ctx, wait, notify); err != nil {
				return nil, err
			}
			continue
		}

		if !state.openUntil.IsZero() {
			if state.probing {
				if !g.cfg.WaitOnPause {
					until := state.openUntil
					g.mu.Unlock()
					return nil, fmt.Errorf("%w: %s until %s", ErrDomainPaused, host, until.Format(time.RFC3339))
				}
				notify := state.notify
				g.mu.Unlock()
				if err := waitContext(ctx, 0, notify); err != nil {
					return nil, err
				}
				continue
			}
			state.probing = true
			state.windowStart = now
			state.requests = 0
			state.failures = 0
			g.mu.Unlock()
			return &guardTicket{host: host, probe: true}, nil
		}

		g.mu.Unlock()
		return &guardTicket{host: host}, nil
	}
}

func (g *domainGuard) after(ticket *guardTicket, failed bool) {
	if ticket == nil {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	state := g.stateLocked(ticket.host)
	now := time.Now()

	if ticket.probe {
		state.probing = false
		if failed {
			state.openUntil = now.Add(g.cfg.Pause)
			state.windowStart = now
			state.requests = 0
			state.failures = 0
			g.opens.Add(1)
		} else {
			state.openUntil = time.Time{}
			state.windowStart = now
			state.requests = 0
			state.failures = 0
		}
		g.signalLocked(state)
		return
	}

	if !state.openUntil.IsZero() {
		return
	}
	if state.windowStart.IsZero() || now.Sub(state.windowStart) >= g.cfg.Window {
		state.windowStart = now
		state.requests = 0
		state.failures = 0
	}

	state.requests++
	if failed {
		state.failures++
	}

	if state.requests < g.cfg.MinRequests {
		return
	}
	if float64(state.failures)/float64(state.requests) < g.cfg.ErrorRate {
		return
	}

	state.openUntil = now.Add(g.cfg.Pause)
	state.windowStart = now
	state.requests = 0
	state.failures = 0
	g.opens.Add(1)
	g.signalLocked(state)
}

func (g *domainGuard) opened() uint64 {
	return g.opens.Load()
}

func (g *domainGuard) stateLocked(host string) *domainState {
	state, ok := g.domains[host]
	if !ok {
		state = &domainState{
			windowStart: time.Now(),
			notify:      make(chan struct{}),
		}
		g.domains[host] = state
	}
	return state
}

func (g *domainGuard) signalLocked(state *domainState) {
	close(state.notify)
	state.notify = make(chan struct{})
}

func waitContext(ctx context.Context, wait time.Duration, notify <-chan struct{}) error {
	if wait <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
			return nil
		}
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	case <-notify:
		return nil
	}
}
