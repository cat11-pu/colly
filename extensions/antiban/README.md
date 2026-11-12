# antiban

`antiban` is an opt-in HTTP transport for Colly that reduces blocking and rate
limit failures. It does not change existing collector methods.

## Features

- Rotates `User-Agent`, `Referer`, `Accept-Language` and arbitrary custom
  headers from configurable pools.
- Retries `403` and `429` responses with exponential backoff and optional
  `Retry-After` support.
- Applies multiplicative concurrency decrease on throttling and additive
  recovery after successful requests.
- Tracks a per-host error-rate window and pauses the host when the threshold
  is crossed. A half-open request is allowed after the pause.
- Rotates healthy proxies, evicts proxies on connection errors, and periodically
  probes dead proxies before putting them back into rotation.
- Can be disabled at construction time or toggled at runtime.

## Usage

```go
cfg := antiban.DefaultConfig()
cfg.Headers.UserAgents = []string{
	"Mozilla/5.0 ... Chrome/124.0.0.0",
	"Mozilla/5.0 ... Firefox/125.0",
}
cfg.Headers.Referers = []string{
	"https://www.google.com/",
	"https://{host}/",
}
cfg.Adaptive.MaxConcurrency = 8
cfg.Proxies.URLs = []string{
	"http://127.0.0.1:8080",
	"socks5://127.0.0.1:1080",
}
cfg.Proxies.ProbeURL = "https://example.com/"
cfg.DomainGuard.Pause = 30 * time.Second
// Fail-fast is the default. Set cfg.DomainGuard.WaitOnPause = true to block
// requests until the pause expires instead.

layer, err := antiban.Apply(c, cfg)
if err != nil {
	log.Fatal(err)
}
defer layer.Close()
```

If a custom transport must be preserved, pass it explicitly:

```go
base, ok := http.DefaultTransport.(*http.Transport)
if !ok {
	log.Fatal("default transport is not *http.Transport")
}
layer, err := antiban.ApplyWithTransport(c, cfg, base.Clone())
```

`Apply` should be called after proxy or transport setup because it replaces the
collector transport. The layer is enabled by default in `DefaultConfig()`;
use `cfg.Enabled = false` or `layer.SetEnabled(false)` to bypass it.

When a domain is paused, the default behavior is to fail fast with
`antiban.ErrDomainPaused`. After the pause, one half-open request is allowed;
if it succeeds, the domain returns to normal rotation.

## Proxy recovery

When `ProbeURL` is set, a dead proxy is checked by sending a `HEAD` request
through it. Any HTTP response, including `403` or `429`, proves the proxy is
reachable. Without `ProbeURL`, recovery uses a TCP connection to the proxy
endpoint. Dead proxies remain out of rotation until a probe succeeds.

## Verification

The package includes deterministic tests for header rotation, retries,
concurrency limits, domain pauses, proxy eviction/recovery and the runtime
switch. A five-minute mock-server simulation is opt-in:

```bash
ANTIBAN_SIMULATION=1 ANTIBAN_SIM_DURATION=5m \
go test ./extensions/antiban/internal/engine \
  -run TestSimulationFiveMinutes -v -timeout 10m
```

If the workspace toolchain is older than the version in `go.mod`, this package
only uses the standard library, so it can also be run with
`GO111MODULE=off`.

The simulation serves a mix of `200`, `403` and `429` responses and asserts:

- final success rate is at least 95%;
- observed concurrency never exceeds `Adaptive.MaxConcurrency`;
- at least two values from each identity pool were actually used.
