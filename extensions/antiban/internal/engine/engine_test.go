package engine

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testClient(t *testing.T, cfg Config) (*Manager, *http.Client) {
	t.Helper()
	manager, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(manager.Close)
	return manager, &http.Client{Transport: manager.Transport(nil), Timeout: 10 * time.Second}
}

func TestHeadersRotateFromConfiguredPools(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]map[string]bool{
		"User-Agent":      {},
		"Referer":         {},
		"Accept-Language": {},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		seen["User-Agent"][r.UserAgent()] = true
		seen["Referer"][r.Referer()] = true
		seen["Accept-Language"][r.Header.Get("Accept-Language")] = true
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.Headers.Seed = 1
	cfg.Headers.UserAgents = []string{"agent-a", "agent-b"}
	cfg.Headers.Referers = []string{"https://ref-a.example/", "https://ref-b.example/"}
	cfg.Headers.AcceptLanguages = []string{"en-US", "zh-CN"}
	cfg.Adaptive.MaxConcurrency = 2
	cfg.Adaptive.InitialConcurrency = 2
	cfg.Retry.MaxAttempts = 1
	cfg.DomainGuard.MinRequests = 1000

	_, client := testClient(t, cfg)
	for i := 0; i < 20; i++ {
		resp, err := client.Get(server.URL)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
	}

	for header, values := range seen {
		if len(values) < 2 {
			t.Fatalf("%s did not rotate, saw %v", header, values)
		}
	}
}

func TestRetriesThrottleResponsesAndLowersConcurrency(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 1:
			w.WriteHeader(http.StatusForbidden)
		case 2:
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.Adaptive.MaxConcurrency = 4
	cfg.Adaptive.InitialConcurrency = 4
	cfg.Adaptive.DecreaseFactor = 0.5
	cfg.Retry.MaxAttempts = 4
	cfg.Retry.BaseDelay = time.Millisecond
	cfg.Retry.MaxDelay = 5 * time.Millisecond
	cfg.Retry.Jitter = 0
	cfg.DomainGuard.MinRequests = 1000

	manager, client := testClient(t, cfg)
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	stats := manager.Stats()
	if stats.Attempts != 3 || stats.ThrottledResponses != 2 || stats.Retries != 2 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
	if stats.CurrentConcurrency != 1 {
		t.Fatalf("concurrency = %d, want 1 after two throttles", stats.CurrentConcurrency)
	}
}

func TestConcurrencyNeverExceedsConfiguredMaximum(t *testing.T) {
	const maxConcurrency = 3
	var active atomic.Int64
	var observedMax atomic.Int64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := active.Add(1)
		for {
			previous := observedMax.Load()
			if current <= previous || observedMax.CompareAndSwap(previous, current) {
				break
			}
		}
		time.Sleep(25 * time.Millisecond)
		active.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.Adaptive.MaxConcurrency = maxConcurrency
	cfg.Adaptive.InitialConcurrency = maxConcurrency
	cfg.Retry.MaxAttempts = 1
	cfg.DomainGuard.MinRequests = 1000

	manager, client := testClient(t, cfg)
	var wg sync.WaitGroup
	for i := 0; i < 15; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp, err := client.Get(server.URL)
			if err != nil {
				t.Errorf("Get: %v", err)
				return
			}
			resp.Body.Close()
		}()
	}
	wg.Wait()

	if got := observedMax.Load(); got > maxConcurrency {
		t.Fatalf("server observed %d concurrent requests, limit is %d", got, maxConcurrency)
	}
	if stats := manager.Stats(); stats.MaxObservedConcurrency > maxConcurrency {
		t.Fatalf("manager observed %d concurrent requests, limit is %d", stats.MaxObservedConcurrency, maxConcurrency)
	}
}

func TestConcurrencyRampsUpAfterSuccesses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.Adaptive.MaxConcurrency = 4
	cfg.Adaptive.InitialConcurrency = 1
	cfg.Adaptive.RampUpAfter = 2
	cfg.Adaptive.RampUpStep = 1
	cfg.Retry.MaxAttempts = 1
	cfg.DomainGuard.MinRequests = 1000

	manager, client := testClient(t, cfg)
	for i := 0; i < 6; i++ {
		resp, err := client.Get(server.URL)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
	}
	if got := manager.Stats().CurrentConcurrency; got != 4 {
		t.Fatalf("concurrency = %d, want ramp-up to 4", got)
	}
}

func TestDomainBreakerPausesThenProbes(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) <= 3 {
			w.WriteHeader(http.StatusForbidden)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.Adaptive.MaxConcurrency = 1
	cfg.Adaptive.InitialConcurrency = 1
	cfg.Retry.MaxAttempts = 1
	cfg.DomainGuard.MinRequests = 3
	cfg.DomainGuard.Window = time.Second
	cfg.DomainGuard.ErrorRate = 1
	cfg.DomainGuard.Pause = 100 * time.Millisecond

	manager, client := testClient(t, cfg)
	for i := 0; i < 3; i++ {
		resp, err := client.Get(server.URL)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp.Body.Close()
	}

	_, err := client.Get(server.URL)
	if !errors.Is(err, ErrDomainPaused) {
		t.Fatalf("request during pause error = %v, want ErrDomainPaused", err)
	}

	time.Sleep(120 * time.Millisecond)
	resp, err := client.Get(server.URL)
	if err != nil {
		t.Fatalf("probe request: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("probe status = %d, want 200", resp.StatusCode)
	}
	if stats := manager.Stats(); stats.CircuitOpens != 1 {
		t.Fatalf("circuit opens = %d, want 1", stats.CircuitOpens)
	}
}

func TestProxyEvictionAndAutomaticRecovery(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	proxyHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req, err := http.NewRequestWithContext(r.Context(), r.Method, r.URL.String(), r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		req.Header = r.Header.Clone()
		resp, err := (&http.Transport{DisableKeepAlives: true}).RoundTrip(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		for name, values := range resp.Header {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	})

	liveProxy := httptest.NewServer(proxyHandler)
	defer liveProxy.Close()

	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve proxy address: %v", err)
	}
	deadAddress := reserved.Addr().String()
	reserved.Close()

	cfg := DefaultConfig()
	cfg.Proxies.URLs = []string{"http://" + deadAddress, liveProxy.URL}
	cfg.Proxies.ProbeURL = target.URL
	cfg.Proxies.ProbeInterval = 20 * time.Millisecond
	cfg.Proxies.ProbeTimeout = 500 * time.Millisecond
	cfg.Proxies.DeadCooldown = 10 * time.Millisecond
	cfg.Adaptive.MaxConcurrency = 1
	cfg.Adaptive.InitialConcurrency = 1
	cfg.Retry.MaxAttempts = 4
	cfg.Retry.BaseDelay = time.Millisecond
	cfg.Retry.MaxDelay = 5 * time.Millisecond
	cfg.Retry.Jitter = 0
	cfg.DomainGuard.MinRequests = 1000

	manager, client := testClient(t, cfg)
	resp, err := client.Get(target.URL)
	if err != nil {
		t.Fatalf("first request through proxy pool: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	stats := manager.Stats()
	if stats.ProxyEvictions != 1 {
		t.Fatalf("proxy evictions = %d, want 1", stats.ProxyEvictions)
	}
	if stats.HealthyProxies != 1 {
		t.Fatalf("healthy proxies = %d, want 1", stats.HealthyProxies)
	}

	listener, err := net.Listen("tcp", deadAddress)
	if err != nil {
		t.Fatalf("reopen dead proxy address: %v", err)
	}
	recoveredProxy := httptest.NewUnstartedServer(proxyHandler)
	recoveredProxy.Listener = listener
	recoveredProxy.Start()
	defer recoveredProxy.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		for _, status := range manager.ProxyStatus() {
			if status.URL == "http://"+deadAddress && status.Alive {
				if got := manager.Stats().ProxyRecoveries; got != 1 {
					t.Fatalf("proxy recoveries = %d, want 1", got)
				}
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("dead proxy was not automatically restored")
}

func TestMasterSwitchCanBeToggled(t *testing.T) {
	var seenUserAgent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seenUserAgent = r.UserAgent()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.Enabled = false
	cfg.Headers.UserAgents = []string{"rotated-agent"}
	cfg.Retry.MaxAttempts = 1
	cfg.DomainGuard.MinRequests = 1000

	manager, client := testClient(t, cfg)
	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("User-Agent", "original-agent")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request with layer disabled: %v", err)
	}
	resp.Body.Close()
	if seenUserAgent != "original-agent" {
		t.Fatalf("disabled layer changed UA to %q", seenUserAgent)
	}
	if stats := manager.Stats(); stats.Requests != 0 {
		t.Fatalf("disabled layer recorded %d requests", stats.Requests)
	}

	manager.SetEnabled(true)
	resp, err = client.Get(server.URL)
	if err != nil {
		t.Fatalf("request with layer enabled: %v", err)
	}
	resp.Body.Close()
	if seenUserAgent != "rotated-agent" {
		t.Fatalf("enabled layer UA = %q, want rotated-agent", seenUserAgent)
	}
	if stats := manager.Stats(); stats.Requests != 1 {
		t.Fatalf("enabled layer recorded %d requests, want 1", stats.Requests)
	}
}

func TestSimulationFiveMinutes(t *testing.T) {
	if os.Getenv("ANTIBAN_SIMULATION") != "1" {
		t.Skip("set ANTIBAN_SIMULATION=1 to run the five-minute simulation")
	}

	duration := 5 * time.Minute
	if value := os.Getenv("ANTIBAN_SIM_DURATION"); value != "" {
		parsed, err := time.ParseDuration(value)
		if err != nil {
			t.Fatalf("parse ANTIBAN_SIM_DURATION: %v", err)
		}
		duration = parsed
	}

	var sequence atomic.Uint64
	var active atomic.Int64
	var observedMax atomic.Int64
	var mu sync.Mutex
	identity := map[string]map[string]bool{
		"User-Agent":      {},
		"Referer":         {},
		"Accept-Language": {},
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			previous := observedMax.Load()
			if current <= previous || observedMax.CompareAndSwap(previous, current) {
				break
			}
		}

		mu.Lock()
		identity["User-Agent"][r.UserAgent()] = true
		identity["Referer"][r.Referer()] = true
		identity["Accept-Language"][r.Header.Get("Accept-Language")] = true
		mu.Unlock()

		time.Sleep(time.Duration(15+sequence.Load()%20) * time.Millisecond)
		n := sequence.Add(1)
		switch {
		case n%19 == 0:
			w.WriteHeader(http.StatusForbidden)
		case n%23 == 0:
			w.WriteHeader(http.StatusTooManyRequests)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	const maxConcurrency = 8
	cfg := DefaultConfig()
	cfg.Headers.Seed = 42
	cfg.Adaptive.MaxConcurrency = maxConcurrency
	cfg.Adaptive.InitialConcurrency = maxConcurrency
	cfg.Retry.MaxAttempts = 4
	cfg.Retry.BaseDelay = 20 * time.Millisecond
	cfg.Retry.MaxDelay = 250 * time.Millisecond
	cfg.Retry.Jitter = 0.2
	cfg.DomainGuard.MinRequests = 100
	cfg.DomainGuard.ErrorRate = 0.7

	manager, _ := testClient(t, cfg)
	client := &http.Client{Transport: manager.Transport(nil), Timeout: 15 * time.Second}

	deadline := time.Now().Add(duration)
	var successes atomic.Uint64
	var failures atomic.Uint64
	var wg sync.WaitGroup

	for worker := 0; worker < 32; worker++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(deadline) {
				ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
				if err != nil {
					cancel()
					failures.Add(1)
					continue
				}
				resp, err := client.Do(req)
				cancel()
				if err != nil {
					failures.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode >= 200 && resp.StatusCode < 400 {
					successes.Add(1)
				} else {
					failures.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	stats := manager.Stats()
	completed := successes.Load() + failures.Load()
	successRate := float64(successes.Load()) / float64(completed)
	t.Logf("duration=%s completed=%d success=%d failure=%d success_rate=%.3f%%",
		duration, completed, successes.Load(), failures.Load(), successRate*100)
	t.Logf("manager stats: %+v", stats)
	t.Logf("server observed max concurrency=%d", observedMax.Load())

	if completed < 100 {
		t.Fatalf("only %d completed requests", completed)
	}
	if stats.ThrottledResponses == 0 {
		t.Fatal("mock server did not exercise 403/429 responses")
	}
	if successRate < 0.95 {
		t.Fatalf("success rate %.2f%% is below 95%%", successRate*100)
	}
	if stats.MaxObservedConcurrency > maxConcurrency {
		t.Fatalf("manager observed concurrency %d > configured %d", stats.MaxObservedConcurrency, maxConcurrency)
	}
	if observedMax.Load() > maxConcurrency {
		t.Fatalf("server observed concurrency %d > configured %d", observedMax.Load(), maxConcurrency)
	}

	mu.Lock()
	defer mu.Unlock()
	for header, values := range identity {
		if len(values) < 2 {
			t.Fatalf("%s did not rotate during simulation: %v", header, values)
		}
	}
}

func BenchmarkRoundTrip(b *testing.B) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.Adaptive.MaxConcurrency = 16
	cfg.Adaptive.InitialConcurrency = 16
	cfg.Retry.MaxAttempts = 1
	manager, err := New(cfg)
	if err != nil {
		b.Fatal(err)
	}
	defer manager.Close()

	client := &http.Client{Transport: manager.Transport(nil)}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := client.Get(server.URL)
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}
