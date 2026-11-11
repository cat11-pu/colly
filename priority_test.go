package colly

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestPriorityRuleMatch(t *testing.T) {
	rules := []*PriorityRule{
		{URLGlob: "http://example.com/api/*", Priority: PriorityP0},
		{URLRegexp: `\.jpg$`, Priority: PriorityP1},
		{DomainGlob: "*.example.org", Priority: PriorityP2},
		{DomainRegexp: `^cdn\d+\.example\.net$`, Priority: PriorityP0},
	}
	for _, r := range rules {
		if err := r.Init(); err != nil {
			t.Fatalf("Init failed: %v", err)
		}
	}
	tests := []struct {
		rule   *PriorityRule
		url    string
		domain string
		want   bool
	}{
		{rules[0], "http://example.com/api/users", "example.com", true},
		{rules[0], "http://example.com/static/x", "example.com", false},
		{rules[1], "http://foo.com/a/b.jpg", "foo.com", true},
		{rules[1], "http://foo.com/a/b.png", "foo.com", false},
		{rules[2], "http://sub.example.org/", "sub.example.org", true},
		{rules[2], "http://example.org/", "example.org", false},
		{rules[3], "http://cdn3.example.net/x", "cdn3.example.net", true},
		{rules[3], "http://cdn.example.net/x", "cdn.example.net", false},
	}
	for i, tt := range tests {
		if got := tt.rule.Match(tt.url, tt.domain); got != tt.want {
			t.Errorf("case %d: Match(%q, %q) = %v, want %v", i, tt.url, tt.domain, got, tt.want)
		}
	}
}

func TestPriorityRuleInitErrors(t *testing.T) {
	if err := (&PriorityRule{Priority: PriorityP0}).Init(); err != ErrNoPriorityPattern {
		t.Errorf("expected ErrNoPriorityPattern, got %v", err)
	}
	if err := (&PriorityRule{URLGlob: "*", Priority: Priority(7)}).Init(); err != ErrInvalidPriority {
		t.Errorf("expected ErrInvalidPriority, got %v", err)
	}
	if err := (&PriorityRule{URLRegexp: "(["}).Init(); err == nil {
		t.Error("expected regexp compile error, got nil")
	}
	if err := (&PriorityRule{URLGlob: "[a-"}).Init(); err == nil {
		t.Error("expected glob compile error, got nil")
	}
}

func TestSetPriorityConfigErrors(t *testing.T) {
	c := NewCollector()
	if err := c.SetPriorityConfig(nil); err != ErrNoPriorityRules {
		t.Errorf("expected ErrNoPriorityRules, got %v", err)
	}
	if err := c.SetPriorityConfig(&PriorityConfig{}); err != ErrNoPriorityRules {
		t.Errorf("expected ErrNoPriorityRules, got %v", err)
	}
	if err := c.SetPriorityRules(&PriorityRule{}); err != ErrNoPriorityPattern {
		t.Errorf("expected ErrNoPriorityPattern, got %v", err)
	}
	if c.scheduler.Load() != nil {
		t.Error("scheduler must not be installed after failed SetPriorityConfig")
	}
}

// fillQueues is a test helper that enqueues n tasks of the given priority
// without starting the dispatch loop.
func (s *priorityScheduler) fillQueues(p Priority, n int) {
	s.mu.Lock()
	for i := 0; i < n; i++ {
		s.queues[p] = append(s.queues[p], &priorityTask{})
	}
	s.mu.Unlock()
}

func TestPrioritySchedulerPreemption(t *testing.T) {
	s, err := newPriorityScheduler(nil, &PriorityConfig{
		Rules:       []*PriorityRule{{URLGlob: "*", Priority: PriorityP0}},
		Parallelism: 1,
		Quota:       10,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Low priority queued first, high priority queued later: the high
	// priority task must still be dispatched first.
	s.fillQueues(PriorityP2, 1)
	s.fillQueues(PriorityP1, 1)
	s.fillQueues(PriorityP0, 1)
	want := []Priority{PriorityP0, PriorityP1, PriorityP2}
	for i, w := range want {
		got := s.popForTest()
		if got != w {
			t.Errorf("dispatch %d: got priority %d, want %d", i, got, w)
		}
	}
}

// popForTest pops one task and reports which priority queue it came from.
func (s *priorityScheduler) popForTest() Priority {
	s.mu.Lock()
	defer s.mu.Unlock()
	before := [priorityLevels]int{}
	for p := 0; p < priorityLevels; p++ {
		before[p] = len(s.queues[p])
	}
	s.popLocked()
	for p := 0; p < priorityLevels; p++ {
		if len(s.queues[p]) < before[p] {
			return Priority(p)
		}
	}
	return -1
}

func TestPrioritySchedulerQuota(t *testing.T) {
	s, err := newPriorityScheduler(nil, &PriorityConfig{
		Rules:       []*PriorityRule{{URLGlob: "*", Priority: PriorityP0}},
		Parallelism: 1,
		Quota:       10,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.fillQueues(PriorityP0, 100)
	s.fillQueues(PriorityP2, 100)

	var order []Priority
	for i := 0; i < 100; i++ {
		order = append(order, s.popForTest())
	}

	// In every window of 10 consecutive dispatches at least one must be P2
	// while P2 tasks are waiting.
	for i := 0; i+10 <= len(order); i++ {
		hasLow := false
		for _, p := range order[i : i+10] {
			if p == PriorityP2 {
				hasLow = true
				break
			}
		}
		if !hasLow {
			t.Fatalf("window [%d,%d) has no low-priority dispatch: %v", i, i+10, order[i:i+10])
		}
	}
	// High priority must dominate: the first 9 dispatches are P0.
	for i := 0; i < 9; i++ {
		if order[i] != PriorityP0 {
			t.Fatalf("dispatch %d: got priority %d, want P0", i, order[i])
		}
	}
}

type priorityRunStats struct {
	mu        sync.Mutex
	enqueued  map[string]time.Time
	count     [priorityLevels]int
	latency   [priorityLevels]time.Duration
	completed [priorityLevels]time.Duration
	start     time.Time
}

func newPriorityRunStats() *priorityRunStats {
	return &priorityRunStats{enqueued: make(map[string]time.Time)}
}

func urlPriority(u string) int {
	for p := 0; p < priorityLevels; p++ {
		if strings.Contains(u, fmt.Sprintf("/p%d/", p)) {
			return p
		}
	}
	return -1
}

func runPriorityWorkload(t *testing.T, urls []string, parallelism int, withPriority bool) *priorityRunStats {
	t.Helper()
	stats := newPriorityRunStats()
	c := NewCollector(Async(true))
	if err := c.Limit(&LimitRule{DomainGlob: "*", Parallelism: parallelism}); err != nil {
		t.Fatal(err)
	}
	if withPriority {
		err := c.SetPriorityConfig(&PriorityConfig{
			Rules: []*PriorityRule{
				{URLGlob: "*/p0/*", Priority: PriorityP0},
				{URLRegexp: `/p1/\d+$`, Priority: PriorityP1},
				{URLGlob: "*/p2/*", Priority: PriorityP2},
			},
			Parallelism: parallelism,
			Quota:       10,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	c.OnResponse(func(r *Response) {
		now := time.Now()
		u := r.Request.URL.String()
		p := urlPriority(u)
		if p < 0 {
			return
		}
		stats.mu.Lock()
		enqueued, ok := stats.enqueued[u]
		if ok {
			stats.count[p]++
			stats.latency[p] += now.Sub(enqueued)
			if d := now.Sub(stats.start); d > stats.completed[p] {
				stats.completed[p] = d
			}
		}
		stats.mu.Unlock()
	})
	stats.start = time.Now()
	for _, u := range urls {
		stats.mu.Lock()
		stats.enqueued[u] = time.Now()
		stats.mu.Unlock()
		if err := c.Visit(u); err != nil {
			t.Fatalf("Visit(%s) failed: %v", u, err)
		}
	}
	c.Wait()
	return stats
}

func (s *priorityRunStats) report(t *testing.T, label string) {
	t.Helper()
	for p := 0; p < priorityLevels; p++ {
		if s.count[p] == 0 {
			continue
		}
		t.Logf("%s P%d: count=%d avgLatency=%v completed=%v",
			label, p, s.count[p], s.latency[p]/time.Duration(s.count[p]), s.completed[p])
	}
}

func TestPrioritySchedulingFairness(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(5 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	const perLevel = 1000
	const parallelism = 32
	urls := make([]string, 0, 3*perLevel)
	for i := 0; i < perLevel; i++ {
		urls = append(urls,
			fmt.Sprintf("%s/p0/%d", srv.URL, i),
			fmt.Sprintf("%s/p1/%d", srv.URL, i),
			fmt.Sprintf("%s/p2/%d", srv.URL, i),
		)
	}

	baseline := runPriorityWorkload(t, urls, parallelism, false)
	baseline.report(t, "baseline")

	prioritized := runPriorityWorkload(t, urls, parallelism, true)
	prioritized.report(t, "prioritized")

	for p := 0; p < priorityLevels; p++ {
		if baseline.count[p] != perLevel {
			t.Fatalf("baseline: P%d completed %d requests, want %d", p, baseline.count[p], perLevel)
		}
		if prioritized.count[p] != perLevel {
			t.Fatalf("prioritized: P%d completed %d requests, want %d", p, prioritized.count[p], perLevel)
		}
	}

	baselineMakespan := baseline.completed[PriorityP2]
	if baseline.completed[PriorityP0] > baselineMakespan {
		baselineMakespan = baseline.completed[PriorityP0]
	}
	if baseline.completed[PriorityP1] > baselineMakespan {
		baselineMakespan = baseline.completed[PriorityP1]
	}
	p2Completed := prioritized.completed[PriorityP2]
	t.Logf("P2 completion: prioritized=%v baseline makespan=%v ratio=%.2f",
		p2Completed, baselineMakespan, float64(p2Completed)/float64(baselineMakespan))
	if p2Completed > baselineMakespan*3/2 {
		t.Errorf("P2 completion %v exceeds 1.5x baseline makespan %v", p2Completed, baselineMakespan)
	}

	avgP0 := prioritized.latency[PriorityP0] / time.Duration(prioritized.count[PriorityP0])
	avgP2 := prioritized.latency[PriorityP2] / time.Duration(prioritized.count[PriorityP2])
	if avgP0 >= avgP2 {
		t.Errorf("expected P0 avg latency (%v) < P2 avg latency (%v)", avgP0, avgP2)
	}
}
