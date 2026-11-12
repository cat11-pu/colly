// Package antiban provides an opt-in anti-blocking transport for Colly.
//
// The layer rotates configurable request headers and proxies, retries 403 and
// 429 responses with backoff, lowers concurrency under throttling, and pauses
// hosts whose error rate crosses a configured threshold. Existing Colly
// methods are unchanged; attach the layer with Apply or Manager.Transport and
// toggle it at runtime with Manager.SetEnabled.
package antiban

import (
	"net/http"
	"net/url"

	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/extensions/antiban/internal/engine"
)

type Config = engine.Config
type HeaderConfig = engine.HeaderConfig
type AdaptiveConfig = engine.AdaptiveConfig
type RetryConfig = engine.RetryConfig
type ProxyConfig = engine.ProxyConfig
type DomainGuardConfig = engine.DomainGuardConfig
type Stats = engine.Stats
type ProxyStatus = engine.ProxyStatus

var (
	ErrNoProxyAvailable = engine.ErrNoProxyAvailable
	ErrDomainPaused     = engine.ErrDomainPaused
)

type Manager struct {
	inner *engine.Manager
}

// DefaultConfig returns the enabled default configuration.
func DefaultConfig() Config {
	return engine.DefaultConfig()
}

// New creates a standalone manager. Use Transport to wrap an HTTP transport.
func New(cfg Config) (*Manager, error) {
	inner, err := engine.New(cfg)
	if err != nil {
		return nil, err
	}
	return &Manager{inner: inner}, nil
}

// Apply attaches the layer to a collector while preserving its cookie jar,
// redirect policy and timeout. Apply replaces the collector transport; call it
// after custom proxy/transport setup, or use ApplyWithTransport instead.
func Apply(c *colly.Collector, cfg Config) (*Manager, error) {
	m, err := New(cfg)
	if err != nil {
		return nil, err
	}
	c.WithTransport(m.Transport(nil))
	return m, nil
}

// ApplyWithTransport is like Apply but clones and wraps the supplied transport.
func ApplyWithTransport(c *colly.Collector, cfg Config, base *http.Transport) (*Manager, error) {
	m, err := New(cfg)
	if err != nil {
		return nil, err
	}
	c.WithTransport(m.Transport(base))
	return m, nil
}

// Transport wraps an HTTP transport. A nil base uses a clone of
// http.DefaultTransport.
func (m *Manager) Transport(base *http.Transport) http.RoundTripper {
	return m.inner.Transport(base)
}

// SetEnabled toggles the entire layer at runtime.
func (m *Manager) SetEnabled(enabled bool) {
	m.inner.SetEnabled(enabled)
}

// Enabled reports whether the layer is active.
func (m *Manager) Enabled() bool {
	return m.inner.Enabled()
}

// Stats returns current counters and concurrency state.
func (m *Manager) Stats() Stats {
	return m.inner.Stats()
}

// ProxyStatus returns the health of each configured proxy.
func (m *Manager) ProxyStatus() []ProxyStatus {
	return m.inner.ProxyStatus()
}

// RandomizeHeaders applies the configured pools to a header set.
func (m *Manager) RandomizeHeaders(header http.Header, requestURL *url.URL) {
	m.inner.RandomizeHeaders(header, requestURL)
}

// Close stops background proxy recovery checks.
func (m *Manager) Close() {
	m.inner.Close()
}
