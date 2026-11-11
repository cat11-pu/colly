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

package colly

import (
	"errors"
	"io"
	"net/http"
	"regexp"
	"sync"

	"github.com/gobwas/glob"
)

// Priority is the scheduling class of a request. Lower values are
// dispatched first.
type Priority int

const (
	// PriorityP0 is the highest priority: matching requests preempt
	// lower priority ones waiting in the scheduler.
	PriorityP0 Priority = iota
	// PriorityP1 is the medium priority.
	PriorityP1
	// PriorityP2 is the lowest priority. It is guaranteed a minimum
	// share of dispatches so it cannot be starved by higher priorities.
	PriorityP2

	priorityLevels = int(PriorityP2) + 1
)

const (
	defaultPriorityParallelism = 8
	defaultPriorityQuota       = 10
)

// ErrNoPriorityPattern is the error type for PriorityRules without patterns.
var ErrNoPriorityPattern = errors.New("No pattern defined in PriorityRule")

// ErrInvalidPriority is returned when a priority value is outside
// [PriorityP0, PriorityP2].
var ErrInvalidPriority = errors.New("Invalid priority, must be between PriorityP0 and PriorityP2")

// ErrNoPriorityRules is returned when SetPriorityConfig is called without
// any rule.
var ErrNoPriorityRules = errors.New("No priority rules defined")

// PriorityRule classifies requests into a Priority by matching the request
// URL or domain. At least one of the four patterns must be set; a request
// matches the rule when any of the set patterns matches. Both glob and
// regular expression patterns are supported.
type PriorityRule struct {
	// URLRegexp is a regular expression matched against the full request URL
	URLRegexp string
	// URLGlob is a glob pattern matched against the full request URL
	URLGlob string
	// DomainRegexp is a regular expression matched against the request domain
	DomainRegexp string
	// DomainGlob is a glob pattern matched against the request domain
	DomainGlob string
	// Priority is the scheduling class assigned to matching requests
	Priority Priority

	compiledURLRegexp    *regexp.Regexp
	compiledURLGlob      *glob.Pattern
	compiledDomainRegexp *regexp.Regexp
	compiledDomainGlob   *glob.Pattern
}

// Init compiles the patterns of the PriorityRule. It is idempotent.
func (r *PriorityRule) Init() error {
	if r.Priority < PriorityP0 || r.Priority > PriorityP2 {
		return ErrInvalidPriority
	}
	hasPattern := false
	if r.URLRegexp != "" {
		c, err := regexp.Compile(r.URLRegexp)
		if err != nil {
			return err
		}
		r.compiledURLRegexp = c
		hasPattern = true
	}
	if r.URLGlob != "" {
		c, err := glob.Compile(r.URLGlob)
		if err != nil {
			return err
		}
		r.compiledURLGlob = c
		hasPattern = true
	}
	if r.DomainRegexp != "" {
		c, err := regexp.Compile(r.DomainRegexp)
		if err != nil {
			return err
		}
		r.compiledDomainRegexp = c
		hasPattern = true
	}
	if r.DomainGlob != "" {
		c, err := glob.Compile(r.DomainGlob)
		if err != nil {
			return err
		}
		r.compiledDomainGlob = c
		hasPattern = true
	}
	if !hasPattern {
		return ErrNoPriorityPattern
	}
	return nil
}

// Match reports whether the rule matches the given URL and domain.
func (r *PriorityRule) Match(u, domain string) bool {
	if r.compiledURLRegexp != nil && r.compiledURLRegexp.MatchString(u) {
		return true
	}
	if r.compiledURLGlob != nil && r.compiledURLGlob.Match(u) {
		return true
	}
	if r.compiledDomainRegexp != nil && r.compiledDomainRegexp.MatchString(domain) {
		return true
	}
	if r.compiledDomainGlob != nil && r.compiledDomainGlob.Match(domain) {
		return true
	}
	return false
}

// PriorityConfig configures priority-aware scheduling of asynchronous
// requests. Install it with Collector.SetPriorityConfig.
type PriorityConfig struct {
	// Rules classify requests into priorities. Rules are evaluated in
	// order and the first matching rule wins. Requests matching no rule
	// are scheduled with PriorityP1.
	Rules []*PriorityRule
	// Parallelism is the maximum number of requests the scheduler runs
	// concurrently. Values <= 0 select the default of 8.
	Parallelism int
	// Quota guarantees that within every Quota dispatched requests at
	// least one comes from the lowest non-empty priority level, so lower
	// priorities cannot be starved. Values <= 0 select the default of 10.
	Quota int
}

// priorityTask is a queued fetch waiting for dispatch.
type priorityTask struct {
	u           string
	method      string
	depth       int
	requestData io.Reader
	ctx         *Context
	hdr         http.Header
	req         *http.Request
}

// priorityScheduler admits asynchronous requests according to their
// priority. Higher priority requests are dispatched first, while a quota
// guarantees a minimum share of dispatches to the lowest non-empty
// priority level.
type priorityScheduler struct {
	collector       *Collector
	rules           []*PriorityRule
	maxInFlight     int
	quota           int
	defaultPriority Priority

	mu          sync.Mutex
	cond        *sync.Cond
	queues      [priorityLevels][]*priorityTask
	inFlight    int
	highStreak  int
	loopRunning bool
}

func newPriorityScheduler(c *Collector, cfg *PriorityConfig) (*priorityScheduler, error) {
	if cfg == nil || len(cfg.Rules) == 0 {
		return nil, ErrNoPriorityRules
	}
	for _, r := range cfg.Rules {
		if r == nil {
			return nil, ErrNoPriorityPattern
		}
		if err := r.Init(); err != nil {
			return nil, err
		}
	}
	parallelism := cfg.Parallelism
	if parallelism <= 0 {
		parallelism = defaultPriorityParallelism
	}
	quota := cfg.Quota
	if quota <= 0 {
		quota = defaultPriorityQuota
	}
	s := &priorityScheduler{
		collector:       c,
		rules:           cfg.Rules,
		maxInFlight:     parallelism,
		quota:           quota,
		defaultPriority: PriorityP1,
	}
	s.cond = sync.NewCond(&s.mu)
	return s, nil
}

// priorityOf returns the priority of a request: the priority of the first
// matching rule, or the default priority when no rule matches.
func (s *priorityScheduler) priorityOf(u, domain string) Priority {
	for _, r := range s.rules {
		if r.Match(u, domain) {
			return r.Priority
		}
	}
	return s.defaultPriority
}

// submit enqueues a task and makes sure the dispatch loop is running.
func (s *priorityScheduler) submit(p Priority, t *priorityTask) {
	s.mu.Lock()
	s.queues[p] = append(s.queues[p], t)
	if !s.loopRunning {
		s.loopRunning = true
		go s.loop()
	}
	s.cond.Signal()
	s.mu.Unlock()
}

// loop dispatches queued tasks while respecting maxInFlight. It exits when
// all queues are empty and no task is in flight; a later submit restarts it.
func (s *priorityScheduler) loop() {
	for {
		s.mu.Lock()
		for {
			if s.emptyLocked() {
				if s.inFlight == 0 {
					s.loopRunning = false
					s.mu.Unlock()
					return
				}
			} else if s.inFlight < s.maxInFlight {
				break
			}
			s.cond.Wait()
		}
		t := s.popLocked()
		s.inFlight++
		s.mu.Unlock()

		go func() {
			defer s.taskDone()
			s.collector.fetch(t.u, t.method, t.depth, t.requestData, t.ctx, t.hdr, t.req)
		}()
	}
}

func (s *priorityScheduler) taskDone() {
	s.mu.Lock()
	s.inFlight--
	s.cond.Signal()
	s.mu.Unlock()
}

func (s *priorityScheduler) emptyLocked() bool {
	for p := 0; p < priorityLevels; p++ {
		if len(s.queues[p]) > 0 {
			return false
		}
	}
	return true
}

// popLocked removes and returns the next task to dispatch. The highest
// non-empty priority level is served first, except that after quota-1
// consecutive high-priority dispatches the lowest non-empty level is
// served once, guaranteeing at least 1 low-priority dispatch per quota.
// Callers must hold s.mu and ensure at least one queue is non-empty.
func (s *priorityScheduler) popLocked() *priorityTask {
	hi, lo := -1, -1
	for p := 0; p < priorityLevels; p++ {
		if len(s.queues[p]) > 0 {
			if hi == -1 {
				hi = p
			}
			lo = p
		}
	}
	pick := hi
	if lo > hi && s.highStreak >= s.quota-1 {
		pick = lo
	}
	if pick == hi && lo > hi {
		s.highStreak++
	} else {
		s.highStreak = 0
	}
	t := s.queues[pick][0]
	s.queues[pick] = s.queues[pick][1:]
	return t
}

// SetPriorityConfig installs priority-aware scheduling for asynchronous
// requests. When active, requests submitted in Async mode are classified
// by the configured rules and dispatched by priority: higher priority
// requests preempt lower priority ones, while the quota guarantees that
// lower priorities keep making progress. It must be called before
// visiting URLs. Synchronous (non-Async) collectors are unaffected.
func (c *Collector) SetPriorityConfig(cfg *PriorityConfig) error {
	s, err := newPriorityScheduler(c, cfg)
	if err != nil {
		return err
	}
	c.scheduler.Store(s)
	return nil
}

// SetPriorityRules is a shorthand for SetPriorityConfig with default
// parallelism and quota.
func (c *Collector) SetPriorityRules(rules ...*PriorityRule) error {
	return c.SetPriorityConfig(&PriorityConfig{Rules: rules})
}
