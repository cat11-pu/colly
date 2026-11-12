package engine

import (
	"math/rand"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type randomizer struct {
	mu  sync.Mutex
	rng *rand.Rand
}

func newRandomizer(seed int64) *randomizer {
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	return &randomizer{rng: rand.New(rand.NewSource(seed))}
}

func (r *randomizer) pick(pool []string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return pool[r.rng.Intn(len(pool))]
}

func (r *randomizer) float64() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rng.Float64()
}

func buildHeaderPools(cfg HeaderConfig) map[string][]string {
	pools := make(map[string][]string)
	pools["User-Agent"] = nonEmpty(cfg.UserAgents)
	pools["Referer"] = nonEmpty(cfg.Referers)
	pools["Accept-Language"] = nonEmpty(cfg.AcceptLanguages)

	for header, values := range cfg.Custom {
		canonical := http.CanonicalHeaderKey(header)
		if canonical == "" {
			continue
		}
		if pool := nonEmpty(values); len(pool) > 0 {
			pools[canonical] = pool
		}
	}

	for header, pool := range pools {
		if len(pool) == 0 {
			delete(pools, header)
		}
	}
	return pools
}

func nonEmpty(values []string) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			result = append(result, value)
		}
	}
	return result
}

func (m *Manager) randomizeHeaders(header http.Header, requestURL *url.URL) {
	if header == nil || len(m.headerPools) == 0 {
		return
	}

	for name, pool := range m.headerPools {
		value := m.random.pick(pool)
		header.Set(name, expandHeaderValue(value, requestURL))
	}
}

// RandomizeHeaders applies one random value from each configured pool.
func (m *Manager) RandomizeHeaders(header http.Header, requestURL *url.URL) {
	if !m.Enabled() {
		return
	}
	m.randomizeHeaders(header, requestURL)
}

func expandHeaderValue(value string, requestURL *url.URL) string {
	if requestURL == nil {
		return value
	}

	scheme := requestURL.Scheme
	host := requestURL.Host
	origin := scheme + "://" + host
	path := requestURL.EscapedPath()
	if path == "" {
		path = "/"
	}

	return strings.NewReplacer(
		"{scheme}", scheme,
		"{host}", host,
		"{origin}", origin,
		"{path}", path,
		"{url}", requestURL.String(),
	).Replace(value)
}
