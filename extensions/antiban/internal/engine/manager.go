package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Manager owns the anti-blocking state shared by all wrapped transports.
type Manager struct {
	cfg         Config
	enabled     atomic.Bool
	random      *randomizer
	headerPools map[string][]string
	limiter     *adaptiveLimiter
	domainGuard *domainGuard
	proxies     *proxyPool

	requests        atomic.Uint64
	attempts        atomic.Uint64
	successes       atomic.Uint64
	failures        atomic.Uint64
	retries         atomic.Uint64
	throttles       atomic.Uint64
	proxyEvictions  atomic.Uint64
	proxyRecoveries atomic.Uint64

	probeOnce   sync.Once
	probeMu     sync.Mutex
	probeCancel context.CancelFunc
	closeOnce   sync.Once
	closed      atomic.Bool
}

// New validates the configuration and creates a manager.
func New(cfg Config) (*Manager, error) {
	cfg = normalizeConfig(cfg)

	proxies, err := newProxyPool(cfg.Proxies)
	if err != nil {
		return nil, err
	}

	m := &Manager{
		cfg:         cfg,
		random:      newRandomizer(cfg.Headers.Seed),
		headerPools: buildHeaderPools(cfg.Headers),
		limiter:     newAdaptiveLimiter(cfg.Adaptive),
		domainGuard: newDomainGuard(cfg.DomainGuard),
		proxies:     proxies,
	}
	m.enabled.Store(cfg.Enabled)
	if cfg.Enabled {
		m.startProxyProbes()
	}
	return m, nil
}

// Transport wraps an HTTP transport. A nil base uses a clone of the default
// transport. The manager only owns state; multiple transports may share it.
func (m *Manager) Transport(base *http.Transport) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport.(*http.Transport)
	}
	base = base.Clone()
	originalProxy := base.Proxy

	base.Proxy = func(req *http.Request) (*url.URL, error) {
		if !m.Enabled() {
			if originalProxy != nil {
				return originalProxy(req)
			}
			return nil, nil
		}
		if m.proxies.len() == 0 {
			if originalProxy != nil {
				return originalProxy(req)
			}
			return nil, nil
		}
		return m.proxies.proxyFromContext(req)
	}

	return &roundTripper{manager: m, base: base}
}

// SetEnabled toggles the layer at runtime.
func (m *Manager) SetEnabled(enabled bool) {
	if m.closed.Load() {
		return
	}
	m.enabled.Store(enabled)
	if enabled {
		m.startProxyProbes()
	}
}

// Enabled reports the current master-switch state.
func (m *Manager) Enabled() bool {
	return m.enabled.Load()
}

// Close stops proxy recovery checks.
func (m *Manager) Close() {
	m.closeOnce.Do(func() {
		m.closed.Store(true)
		m.enabled.Store(false)
		m.probeMu.Lock()
		cancel := m.probeCancel
		m.probeMu.Unlock()
		if cancel != nil {
			cancel()
		}
	})
}

// Stats returns a point-in-time snapshot.
func (m *Manager) Stats() Stats {
	current, active, maxObserved := m.limiter.snapshot()
	stats := Stats{
		Requests:               m.requests.Load(),
		Attempts:               m.attempts.Load(),
		Successes:              m.successes.Load(),
		Failures:               m.failures.Load(),
		Retries:                m.retries.Load(),
		ThrottledResponses:     m.throttles.Load(),
		ProxyEvictions:         m.proxyEvictions.Load(),
		ProxyRecoveries:        m.proxyRecoveries.Load(),
		CircuitOpens:           m.domainGuard.opened(),
		CurrentConcurrency:     current,
		ActiveRequests:         active,
		MaxObservedConcurrency: maxObserved,
		ProxyCount:             m.proxies.len(),
	}
	for _, status := range m.proxies.status() {
		if status.Alive {
			stats.HealthyProxies++
		}
	}
	return stats
}

// ProxyStatus returns the health state of every configured proxy.
func (m *Manager) ProxyStatus() []ProxyStatus {
	return m.proxies.status()
}

type Stats struct {
	Requests               uint64
	Attempts               uint64
	Successes              uint64
	Failures               uint64
	Retries                uint64
	ThrottledResponses     uint64
	ProxyEvictions         uint64
	ProxyRecoveries        uint64
	CircuitOpens           uint64
	CurrentConcurrency     int
	ActiveRequests         int
	MaxObservedConcurrency int
	ProxyCount             int
	HealthyProxies         int
}

// SuccessRate returns successful logical requests divided by completed
// logical requests.
func (s Stats) SuccessRate() float64 {
	completed := s.Successes + s.Failures
	if completed == 0 {
		return 0
	}
	return float64(s.Successes) / float64(completed)
}

type roundTripper struct {
	manager *Manager
	base    *http.Transport
}

func (t *roundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if !t.manager.Enabled() {
		return t.base.RoundTrip(req)
	}
	return t.manager.roundTrip(t.base, req)
}

func (m *Manager) roundTrip(base *http.Transport, req *http.Request) (*http.Response, error) {
	if req == nil || req.URL == nil {
		return nil, errors.New("antiban: request URL is nil")
	}

	ctx := req.Context()
	host := strings.ToLower(req.URL.Hostname())

	m.requests.Add(1)

	ticket, err := m.domainGuard.before(ctx, host)
	if err != nil {
		m.failures.Add(1)
		return nil, err
	}
	finalFailed := true
	defer func() {
		m.domainGuard.after(ticket, finalFailed)
	}()

	if err := m.limiter.acquire(ctx); err != nil {
		m.failures.Add(1)
		return nil, err
	}
	defer m.limiter.release()

	var lastResponse *http.Response
	var lastErr error
	maxAttempts := m.cfg.Retry.MaxAttempts

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		m.attempts.Add(1)
		if attempt > 1 {
			m.retries.Add(1)
		}

		attemptCtx := ctx
		proxyEntry, proxyErr := m.proxies.nextProxy()
		if proxyErr != nil {
			lastErr = proxyErr
			break
		}
		if proxyEntry != nil {
			attemptCtx = context.WithValue(attemptCtx, proxyContextKey{}, proxyEntry)
		}

		attemptReq := req.Clone(attemptCtx)
		attemptReq.Header = req.Header.Clone()
		m.randomizeHeaders(attemptReq.Header, attemptReq.URL)

		if attempt > 1 && req.Body != nil {
			if req.GetBody == nil {
				if lastResponse != nil {
					finalFailed = finalResponseFailed(lastResponse.StatusCode)
					m.finish(lastResponse)
					return lastResponse, nil
				}
				lastErr = ErrRetryBodyUnseekable
				break
			}
			body, bodyErr := req.GetBody()
			if bodyErr != nil {
				if lastResponse != nil {
					finalFailed = finalResponseFailed(lastResponse.StatusCode)
					m.finish(lastResponse)
					return lastResponse, nil
				}
				lastErr = bodyErr
				break
			}
			attemptReq.Body = body
		}

		resp, roundTripErr := base.RoundTrip(attemptReq)
		if roundTripErr != nil {
			lastErr = roundTripErr
			if proxyEntry != nil && !isContextError(roundTripErr) {
				if m.proxies.markDead(proxyEntry, roundTripErr) {
					m.proxyEvictions.Add(1)
				}
			}
			if ctx.Err() != nil {
				m.failures.Add(1)
				return nil, ctx.Err()
			}
			if attempt == maxAttempts {
				break
			}
			if err := sleepContext(ctx, m.backoff(attempt)); err != nil {
				m.failures.Add(1)
				return nil, err
			}
			continue
		}

		if m.retryStatus(resp.StatusCode) {
			m.throttles.Add(1)
			m.limiter.onThrottle()
			lastResponse = resp

			if attempt == maxAttempts {
				break
			}
			if req.Body != nil && req.GetBody == nil {
				break
			}
			delay := m.retryDelay(attempt, resp)
			drainAndClose(resp.Body)
			if err := sleepContext(ctx, delay); err != nil {
				m.failures.Add(1)
				return nil, err
			}
			continue
		}

		finalFailed = finalResponseFailed(resp.StatusCode)
		m.finish(resp)
		return resp, nil
	}

	if lastResponse != nil {
		finalFailed = finalResponseFailed(lastResponse.StatusCode)
		m.finish(lastResponse)
		return lastResponse, nil
	}

	m.failures.Add(1)
	if lastErr == nil {
		lastErr = errors.New("antiban: request failed")
	}
	return nil, lastErr
}

func (m *Manager) finish(resp *http.Response) {
	if finalResponseFailed(resp.StatusCode) {
		m.failures.Add(1)
		return
	}
	m.successes.Add(1)
	m.limiter.onSuccess()
}

func finalResponseFailed(status int) bool {
	return status >= http.StatusBadRequest
}

func (m *Manager) retryStatus(status int) bool {
	for _, code := range m.cfg.Retry.StatusCodes {
		if status == code {
			return true
		}
	}
	return false
}

func (m *Manager) backoff(attempt int) time.Duration {
	delay := m.cfg.Retry.BaseDelay
	for i := 1; i < attempt; i++ {
		delay *= 2
		if delay >= m.cfg.Retry.MaxDelay {
			delay = m.cfg.Retry.MaxDelay
			break
		}
	}
	if m.cfg.Retry.Jitter > 0 {
		delay += time.Duration(float64(delay) * m.cfg.Retry.Jitter * m.random.float64())
	}
	if delay > m.cfg.Retry.MaxDelay {
		delay = m.cfg.Retry.MaxDelay
	}
	if delay <= 0 {
		return time.Millisecond
	}
	return delay
}

func (m *Manager) retryDelay(attempt int, resp *http.Response) time.Duration {
	if !m.cfg.Retry.IgnoreRetryAfter {
		if delay, ok := parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()); ok {
			if delay > m.cfg.Retry.MaxDelay {
				delay = m.cfg.Retry.MaxDelay
			}
			if delay > 0 {
				return delay
			}
		}
	}
	return m.backoff(attempt)
}

func parseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds < 0 {
			return 0, false
		}
		return time.Duration(seconds) * time.Second, true
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	delay := when.Sub(now)
	if delay < 0 {
		delay = 0
	}
	return delay, true
}

func drainAndClose(body io.ReadCloser) {
	if body == nil {
		return
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(body, 16*1024))
	_ = body.Close()
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func isContextError(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func (m *Manager) startProxyProbes() {
	if m.proxies.len() == 0 {
		return
	}
	m.probeOnce.Do(func() {
		ctx, cancel := context.WithCancel(context.Background())
		m.probeMu.Lock()
		m.probeCancel = cancel
		closed := m.closed.Load()
		m.probeMu.Unlock()
		if closed {
			cancel()
			return
		}
		go m.proxies.probeLoop(ctx, m.Enabled, func() {
			m.proxyRecoveries.Add(1)
		})
	})
}

func (m *Manager) String() string {
	stats := m.Stats()
	return fmt.Sprintf(
		"antiban: requests=%d attempts=%d success_rate=%.2f%% concurrency=%d/%d proxies=%d/%d",
		stats.Requests,
		stats.Attempts,
		stats.SuccessRate()*100,
		stats.CurrentConcurrency,
		stats.MaxObservedConcurrency,
		stats.HealthyProxies,
		stats.ProxyCount,
	)
}
