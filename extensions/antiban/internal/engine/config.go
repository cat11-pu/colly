package engine

import "time"

// Config controls the anti-blocking layer.
type Config struct {
	// Enabled is the master switch. A disabled manager passes requests through
	// unchanged, without header rotation, retries, proxy selection or limits.
	Enabled bool

	Headers     HeaderConfig
	Adaptive    AdaptiveConfig
	Retry       RetryConfig
	Proxies     ProxyConfig
	DomainGuard DomainGuardConfig
}

// HeaderConfig defines the pools used to rotate request identity.
type HeaderConfig struct {
	UserAgents      []string
	Referers        []string
	AcceptLanguages []string
	// Custom adds or overrides pools for arbitrary headers.
	Custom map[string][]string
	// Seed makes header selection reproducible. Zero uses a time-based seed.
	Seed int64
}

// AdaptiveConfig controls the global in-flight request limit.
type AdaptiveConfig struct {
	MaxConcurrency     int
	InitialConcurrency int
	MinConcurrency     int
	// DecreaseFactor is applied after a 403 or 429. Values must be below 1.
	DecreaseFactor float64
	// RampUpAfter is the number of consecutive successes needed to increase
	// concurrency by RampUpStep.
	RampUpAfter int
	RampUpStep  int
}

// RetryConfig controls retries for throttling responses.
type RetryConfig struct {
	// MaxAttempts includes the first request. Set it to 1 to disable retries.
	MaxAttempts int
	StatusCodes []int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Jitter      float64
	// IgnoreRetryAfter leaves the server-provided Retry-After header unused.
	IgnoreRetryAfter bool
}

// ProxyConfig controls proxy rotation and recovery.
type ProxyConfig struct {
	URLs []string
	// ProbeURL is requested through a dead proxy during recovery checks.
	// When empty, the proxy endpoint is checked with a TCP connection.
	ProbeURL      string
	ProbeInterval time.Duration
	ProbeTimeout  time.Duration
	DeadCooldown  time.Duration
	// AllowDirectFallback permits direct requests when no configured proxy is
	// currently healthy. It is false by default.
	AllowDirectFallback bool
}

// DomainGuardConfig controls the per-host circuit breaker.
type DomainGuardConfig struct {
	// Window is the window used to calculate the error rate.
	Window      time.Duration
	MinRequests int
	ErrorRate   float64
	Pause       time.Duration
	// WaitOnPause blocks requests until the pause expires instead of returning
	// ErrDomainPaused immediately. The default is fail-fast.
	WaitOnPause bool
}

const (
	defaultMaxConcurrency = 8
	defaultMinConcurrency = 1
	defaultRampUpAfter    = 20
	defaultMaxAttempts    = 4
)

// DefaultConfig returns a practical enabled configuration.
func DefaultConfig() Config {
	return Config{
		Enabled: true,
		Headers: HeaderConfig{
			UserAgents: []string{
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
				"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
				"Mozilla/5.0 (X11; Linux x86_64; rv:125.0) Gecko/20100101 Firefox/125.0",
				"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0",
			},
			Referers: []string{
				"https://www.google.com/",
				"https://www.bing.com/",
				"https://{host}/",
				"https://duckduckgo.com/",
			},
			AcceptLanguages: []string{
				"en-US,en;q=0.9",
				"zh-CN,zh;q=0.9,en;q=0.8",
				"en-GB,en;q=0.9",
				"ja-JP,ja;q=0.9,en;q=0.8",
			},
		},
		Adaptive: AdaptiveConfig{
			MaxConcurrency:     defaultMaxConcurrency,
			InitialConcurrency: defaultMaxConcurrency,
			MinConcurrency:     defaultMinConcurrency,
			DecreaseFactor:     0.5,
			RampUpAfter:        defaultRampUpAfter,
			RampUpStep:         1,
		},
		Retry: RetryConfig{
			MaxAttempts: defaultMaxAttempts,
			StatusCodes: []int{403, 429},
			BaseDelay:   100 * time.Millisecond,
			MaxDelay:    2 * time.Second,
			Jitter:      0.3,
		},
		Proxies: ProxyConfig{
			ProbeInterval: 10 * time.Second,
			ProbeTimeout:  3 * time.Second,
			DeadCooldown:  30 * time.Second,
		},
		DomainGuard: DomainGuardConfig{
			Window:      30 * time.Second,
			MinRequests: 10,
			ErrorRate:   0.5,
			Pause:       30 * time.Second,
		},
	}
}

func normalizeConfig(cfg Config) Config {
	cfg = normalizeHeaders(cfg)
	cfg = normalizeAdaptive(cfg)
	cfg = normalizeRetry(cfg)
	cfg = normalizeProxies(cfg)
	cfg = normalizeDomainGuard(cfg)
	return cfg
}

func normalizeHeaders(cfg Config) Config {
	defaults := DefaultConfig().Headers
	if len(cfg.Headers.UserAgents) == 0 {
		cfg.Headers.UserAgents = defaults.UserAgents
	}
	if len(cfg.Headers.Referers) == 0 {
		cfg.Headers.Referers = defaults.Referers
	}
	if len(cfg.Headers.AcceptLanguages) == 0 {
		cfg.Headers.AcceptLanguages = defaults.AcceptLanguages
	}
	return cfg
}

func normalizeAdaptive(cfg Config) Config {
	if cfg.Adaptive.MaxConcurrency <= 0 {
		cfg.Adaptive.MaxConcurrency = defaultMaxConcurrency
	}
	if cfg.Adaptive.MinConcurrency <= 0 {
		cfg.Adaptive.MinConcurrency = defaultMinConcurrency
	}
	if cfg.Adaptive.MinConcurrency > cfg.Adaptive.MaxConcurrency {
		cfg.Adaptive.MinConcurrency = cfg.Adaptive.MaxConcurrency
	}
	if cfg.Adaptive.InitialConcurrency <= 0 {
		cfg.Adaptive.InitialConcurrency = cfg.Adaptive.MaxConcurrency
	}
	if cfg.Adaptive.InitialConcurrency > cfg.Adaptive.MaxConcurrency {
		cfg.Adaptive.InitialConcurrency = cfg.Adaptive.MaxConcurrency
	}
	if cfg.Adaptive.InitialConcurrency < cfg.Adaptive.MinConcurrency {
		cfg.Adaptive.InitialConcurrency = cfg.Adaptive.MinConcurrency
	}
	if cfg.Adaptive.DecreaseFactor <= 0 || cfg.Adaptive.DecreaseFactor >= 1 {
		cfg.Adaptive.DecreaseFactor = 0.5
	}
	if cfg.Adaptive.RampUpAfter <= 0 {
		cfg.Adaptive.RampUpAfter = defaultRampUpAfter
	}
	if cfg.Adaptive.RampUpStep <= 0 {
		cfg.Adaptive.RampUpStep = 1
	}
	return cfg
}

func normalizeRetry(cfg Config) Config {
	if cfg.Retry.MaxAttempts <= 0 {
		cfg.Retry.MaxAttempts = defaultMaxAttempts
	}
	if len(cfg.Retry.StatusCodes) == 0 {
		cfg.Retry.StatusCodes = []int{403, 429}
	}
	if cfg.Retry.BaseDelay <= 0 {
		cfg.Retry.BaseDelay = 100 * time.Millisecond
	}
	if cfg.Retry.MaxDelay <= 0 {
		cfg.Retry.MaxDelay = 2 * time.Second
	}
	if cfg.Retry.MaxDelay < cfg.Retry.BaseDelay {
		cfg.Retry.MaxDelay = cfg.Retry.BaseDelay
	}
	if cfg.Retry.Jitter < 0 {
		cfg.Retry.Jitter = 0
	}
	if cfg.Retry.Jitter > 1 {
		cfg.Retry.Jitter = 1
	}
	return cfg
}

func normalizeProxies(cfg Config) Config {
	if cfg.Proxies.ProbeInterval <= 0 {
		cfg.Proxies.ProbeInterval = 10 * time.Second
	}
	if cfg.Proxies.ProbeTimeout <= 0 {
		cfg.Proxies.ProbeTimeout = 3 * time.Second
	}
	if cfg.Proxies.DeadCooldown <= 0 {
		cfg.Proxies.DeadCooldown = 30 * time.Second
	}
	return cfg
}

func normalizeDomainGuard(cfg Config) Config {
	if cfg.DomainGuard.Window <= 0 {
		cfg.DomainGuard.Window = 30 * time.Second
	}
	if cfg.DomainGuard.MinRequests <= 0 {
		cfg.DomainGuard.MinRequests = 10
	}
	if cfg.DomainGuard.ErrorRate <= 0 || cfg.DomainGuard.ErrorRate > 1 {
		cfg.DomainGuard.ErrorRate = 0.5
	}
	if cfg.DomainGuard.Pause <= 0 {
		cfg.DomainGuard.Pause = 30 * time.Second
	}
	return cfg
}
