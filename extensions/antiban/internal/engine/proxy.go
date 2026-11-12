package engine

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var (
	ErrNoProxyAvailable    = errors.New("no healthy proxy available")
	ErrRetryBodyUnseekable = errors.New("retry body is not seekable")
)

type proxyEntry struct {
	url     *url.URL
	raw     string
	alive   bool
	until   time.Time
	probing bool
	err     string
}

type proxyPool struct {
	mu      sync.Mutex
	entries []*proxyEntry
	next    uint64
	cfg     ProxyConfig
}

type proxyContextKey struct{}

func newProxyPool(cfg ProxyConfig) (*proxyPool, error) {
	pool := &proxyPool{cfg: cfg}
	if cfg.ProbeURL != "" {
		probeURL, err := url.Parse(cfg.ProbeURL)
		if err != nil {
			return nil, fmt.Errorf("parse probe URL %q: %w", cfg.ProbeURL, err)
		}
		if probeURL.Scheme != "http" && probeURL.Scheme != "https" {
			return nil, fmt.Errorf("probe URL %q must use http or https", cfg.ProbeURL)
		}
		if probeURL.Host == "" {
			return nil, fmt.Errorf("probe URL %q has no host", cfg.ProbeURL)
		}
	}
	for _, raw := range cfg.URLs {
		parsed, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("parse proxy %q: %w", raw, err)
		}
		if parsed.Scheme == "" {
			parsed.Scheme = "http"
			parsed, err = url.Parse(parsed.String())
			if err != nil {
				return nil, fmt.Errorf("parse proxy %q: %w", raw, err)
			}
		}
		if parsed.Host == "" {
			return nil, fmt.Errorf("proxy %q has no host", raw)
		}
		switch parsed.Scheme {
		case "http", "https", "socks5", "socks5h":
		default:
			return nil, fmt.Errorf("proxy %q has unsupported scheme %q", raw, parsed.Scheme)
		}
		pool.entries = append(pool.entries, &proxyEntry{
			url:   parsed,
			raw:   parsed.Redacted(),
			alive: true,
		})
	}
	return pool, nil
}

func (p *proxyPool) len() int {
	return len(p.entries)
}

func (p *proxyPool) nextProxy() (*proxyEntry, error) {
	if len(p.entries) == 0 {
		return nil, nil
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	for i := 0; i < len(p.entries); i++ {
		index := int(p.next % uint64(len(p.entries)))
		p.next++
		entry := p.entries[index]
		if entry.alive {
			return entry, nil
		}
	}
	if p.cfg.AllowDirectFallback {
		return nil, nil
	}
	return nil, ErrNoProxyAvailable
}

func (p *proxyPool) markDead(entry *proxyEntry, err error) bool {
	if entry == nil {
		return false
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if !entry.alive {
		return false
	}
	entry.alive = false
	entry.until = time.Now().Add(p.cfg.DeadCooldown)
	if err != nil {
		entry.err = err.Error()
	}
	return true
}

func (p *proxyPool) markAlive(entry *proxyEntry) bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	if entry.alive {
		return false
	}
	entry.alive = true
	entry.until = time.Time{}
	entry.err = ""
	return true
}

func (p *proxyPool) proxyFromContext(req *http.Request) (*url.URL, error) {
	entry, ok := req.Context().Value(proxyContextKey{}).(*proxyEntry)
	if !ok || entry == nil {
		return nil, nil
	}
	return entry.url, nil
}

func (p *proxyPool) probeLoop(ctx context.Context, enabled func() bool, recovered func()) {
	ticker := time.NewTicker(p.cfg.ProbeInterval)
	defer ticker.Stop()

	probeSlots := make(chan struct{}, 4)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}

		if !enabled() {
			continue
		}
		for _, entry := range p.deadReadyForProbe() {
			select {
			case probeSlots <- struct{}{}:
				go func(entry *proxyEntry) {
					defer func() { <-probeSlots }()
					if p.probe(ctx, entry) {
						recovered()
					}
				}(entry)
			default:
			}
		}
	}
}

func (p *proxyPool) deadReadyForProbe() []*proxyEntry {
	p.mu.Lock()
	defer p.mu.Unlock()

	now := time.Now()
	var ready []*proxyEntry
	for _, entry := range p.entries {
		if entry.alive || entry.probing || now.Before(entry.until) {
			continue
		}
		entry.probing = true
		ready = append(ready, entry)
	}
	return ready
}

func (p *proxyPool) probe(ctx context.Context, entry *proxyEntry) bool {
	defer func() {
		p.mu.Lock()
		entry.probing = false
		if !entry.alive {
			entry.until = time.Now().Add(p.cfg.DeadCooldown)
		}
		p.mu.Unlock()
	}()

	probeCtx, cancel := context.WithTimeout(ctx, p.cfg.ProbeTimeout)
	defer cancel()

	ok := false
	if p.cfg.ProbeURL != "" {
		ok = p.probeHTTP(probeCtx, entry)
	} else {
		ok = p.probeTCP(probeCtx, entry)
	}
	if ok {
		return p.markAlive(entry)
	}
	return false
}

func (p *proxyPool) probeHTTP(ctx context.Context, entry *proxyEntry) bool {
	client := &http.Client{
		Timeout: p.cfg.ProbeTimeout,
		Transport: &http.Transport{
			Proxy:             http.ProxyURL(entry.url),
			DisableKeepAlives: true,
		},
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodHead, p.cfg.ProbeURL, nil)
	if err != nil {
		return false
	}
	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return true
}

func (p *proxyPool) probeTCP(ctx context.Context, entry *proxyEntry) bool {
	address := entry.url.Host
	if !strings.Contains(address, ":") {
		address += ":80"
	}
	dialer := net.Dialer{Timeout: p.cfg.ProbeTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func (p *proxyPool) status() []ProxyStatus {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make([]ProxyStatus, 0, len(p.entries))
	for _, entry := range p.entries {
		result = append(result, ProxyStatus{
			URL:       entry.raw,
			Alive:     entry.alive,
			DeadUntil: entry.until,
			LastError: entry.err,
		})
	}
	return result
}

type ProxyStatus struct {
	URL       string
	Alive     bool
	DeadUntil time.Time
	LastError string
}
