// Copyright 2026 Adam Tauber
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
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type adaptiveTestTarget struct {
	capacity int
	service  func(*rand.Rand) time.Duration
}

func startAdaptiveTestTarget(target adaptiveTestTarget) *httptest.Server {
	sem := make(chan struct{}, target.capacity)
	var seed int64
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sem <- struct{}{}
		defer func() { <-sem }()
		delay := target.service(rand.New(rand.NewSource(atomic.AddInt64(&seed, 1))))
		time.Sleep(delay)
		w.WriteHeader(http.StatusOK)
	}))
}

func TestAdaptiveConcurrencyConfigDefaults(t *testing.T) {
	cfg := DefaultAdaptiveConcurrencyConfig()
	if cfg.MinConcurrency != 1 || cfg.TargetLatency != 500*time.Millisecond ||
		cfg.LatencyPercentile != 0.99 || cfg.Window != 3*time.Second ||
		cfg.SampleInterval != 200*time.Millisecond || cfg.ErrorRateThreshold != 0.05 {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}

	rule := &LimitRule{DomainGlob: "*", Parallelism: 17}
	settings := rule.adaptiveSettings()
	if settings.max != 17 {
		t.Fatalf("Parallelism must be the adaptive ceiling when MaxConcurrency is unset, got %d", settings.max)
	}

	rule.Parallelism = 0
	saved := defaultAdaptiveMaxConcurrency
	if settings = rule.adaptiveSettings(); settings.max != saved {
		t.Fatalf("expected default ceiling %d, got %d", saved, settings.max)
	}
}

func TestAdaptiveConcurrencyDisabledKeepsFixedLimit(t *testing.T) {
	var maxActive atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		active := maxActive.Add(1)
		if active > 2 {
			t.Errorf("fixed limit exceeded: %d requests in flight", active)
		}
		time.Sleep(20 * time.Millisecond)
		maxActive.Add(-1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	rule := &LimitRule{DomainGlob: "*", Parallelism: 2}
	c := NewCollector(Async(true), AllowURLRevisit())
	if err := c.Limit(rule); err != nil {
		t.Fatal(err)
	}
	if limit := rule.AdaptiveConcurrencyLimit(); limit != 0 {
		t.Fatalf("disabled controller should report zero limit, got %d", limit)
	}

	for i := 0; i < 8; i++ {
		if err := c.Visit(fmt.Sprintf("%s/%d", srv.URL, i)); err != nil {
			t.Fatal(err)
		}
	}
	c.Wait()
}

type adaptiveAcceptanceScenario struct {
	name       string
	config     AdaptiveConcurrencyConfig
	maxConc    int
	optimalRPS float64
	target     *adaptiveTestTarget
}

func runAdaptiveAcceptanceScenario(t *testing.T, scenario adaptiveAcceptanceScenario, duration time.Duration, artifactDir string) {
	t.Helper()
	srv := startAdaptiveTestTarget(*scenario.target)
	defer srv.Close()

	rule := &LimitRule{
		DomainGlob:                "*",
		Parallelism:               scenario.maxConc,
		AdaptiveConcurrency:       true,
		AdaptiveConcurrencyConfig: &scenario.config,
	}
	c := NewCollector(Async(false), AllowURLRevisit())
	if err := c.Limit(rule); err != nil {
		t.Fatal(err)
	}

	var completed atomic.Int64
	c.OnResponse(func(*Response) { completed.Add(1) })
	c.OnError(func(*Response, error) { completed.Add(1) })

	dst := filepath.Join(artifactDir, "adaptive_"+scenario.name+".csv")
	if err := os.MkdirAll(artifactDir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := fmt.Fprintln(f, "elapsed_ms,concurrency,throughput_rps,p99_latency_ms,error_rate,in_flight,samples"); err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var urlID atomic.Int64
	for i := 0; i < scenario.maxConc+16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = c.Visit(fmt.Sprintf("%s/item/%d", srv.URL, urlID.Add(1)))
				}
			}
		}()
	}

	var previousCompleted int64
	previousAt := start
	var recentConcurrency []int
	var recentThroughput []float64
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var stats AdaptiveConcurrencyStats

	for {
		select {
		case <-ticker.C:
			stats = rule.AdaptiveConcurrencyStats()
			elapsed := time.Since(start)
			nowCompleted := completed.Load()
			intervalRPS := float64(nowCompleted-previousCompleted) / time.Since(previousAt).Seconds()
			previousCompleted = nowCompleted
			previousAt = time.Now()

			if _, err := fmt.Fprintf(f, "%d,%d,%.2f,%.2f,%.4f,%d,%d\n",
				elapsed.Milliseconds(), stats.Concurrency, intervalRPS,
				float64(stats.P99Latency.Microseconds())/1000, stats.ErrorRate,
				stats.InFlight, stats.Samples); err != nil {
				t.Fatal(err)
			}
			if elapsed >= duration-10*time.Second {
				recentConcurrency = append(recentConcurrency, stats.Concurrency)
				recentThroughput = append(recentThroughput, intervalRPS)
			}
			if elapsed >= duration {
				close(stop)
				wg.Wait()

				var minConc, maxConc int
				if len(recentConcurrency) > 0 {
					minConc, maxConc = recentConcurrency[0], recentConcurrency[0]
					for _, value := range recentConcurrency[1:] {
						if value < minConc {
							minConc = value
						}
						if value > maxConc {
							maxConc = value
						}
					}
				}
				var sumRPS float64
				for _, value := range recentThroughput {
					sumRPS += value
				}
				avgRPS := 0.0
				if len(recentThroughput) > 0 {
					avgRPS = sumRPS / float64(len(recentThroughput))
				}

				spreadLimit := maxConc/2 + 2
				if maxConc-minConc > spreadLimit {
					t.Errorf("%s did not converge: last-10s concurrency range %d..%d", scenario.name, minConc, maxConc)
				}
				if stats.P99Latency > scenario.config.TargetLatency {
					t.Errorf("%s p99 %v exceeds target %v", scenario.name, stats.P99Latency, scenario.config.TargetLatency)
				}
				if stats.ErrorRate > scenario.config.ErrorRateThreshold {
					t.Errorf("%s error rate %.3f exceeds %.3f", scenario.name, stats.ErrorRate, scenario.config.ErrorRateThreshold)
				}
				if avgRPS < scenario.optimalRPS*0.9 {
					t.Errorf("%s throughput %.1f is below 90%% of optimal %.1f", scenario.name, avgRPS, scenario.optimalRPS)
				}

				t.Logf("%s: limit=%d last-10s=%d..%d rps=%.1f/%.1f (%.1f%%), p99=%v, errors=%.3f, curve=%s",
					scenario.name, stats.Concurrency, minConc, maxConc, avgRPS, scenario.optimalRPS,
					100*avgRPS/scenario.optimalRPS, stats.P99Latency, stats.ErrorRate, dst)
				return
			}
		}
	}
}

func TestAdaptiveConcurrencyAcceptance(t *testing.T) {
	if os.Getenv("COLLY_ADAPTIVE_ACCEPTANCE") == "" || testing.Short() {
		t.Skip("set COLLY_ADAPTIVE_ACCEPTANCE=1 to run the 3x30s acceptance simulation")
	}

	artifactDir := os.Getenv("COLLY_ADAPTIVE_ARTIFACT_DIR")
	if artifactDir == "" {
		artifactDir = t.TempDir()
	}

	const maxConcurrency = 64
	const duration = 30 * time.Second

	fastConfig := DefaultAdaptiveConcurrencyConfig()
	fastConfig.MaxConcurrency = maxConcurrency
	slowConfig := DefaultAdaptiveConcurrencyConfig()
	slowConfig.MaxConcurrency = maxConcurrency
	jitterConfig := DefaultAdaptiveConcurrencyConfig()
	jitterConfig.MaxConcurrency = maxConcurrency

	fast := adaptiveAcceptanceScenario{
		name:       "fast",
		config:     fastConfig,
		maxConc:    maxConcurrency,
		optimalRPS: float64(maxConcurrency) / (20 * time.Millisecond).Seconds(),
		target: &adaptiveTestTarget{
			capacity: 1000,
			service:  func(*rand.Rand) time.Duration { return 20 * time.Millisecond },
		},
	}
	slow := adaptiveAcceptanceScenario{
		name:       "slow",
		config:     slowConfig,
		maxConc:    maxConcurrency,
		optimalRPS: float64(8) / (100 * time.Millisecond).Seconds(),
		target: &adaptiveTestTarget{
			capacity: 8,
			service:  func(*rand.Rand) time.Duration { return 100 * time.Millisecond },
		},
	}
	jitterService := func(r *rand.Rand) time.Duration {
		switch r.Intn(10) {
		case 0, 1, 2:
			return 20 * time.Millisecond
		case 3, 4, 5, 6, 7:
			return 80 * time.Millisecond
		default:
			return 300 * time.Millisecond
		}
	}
	const jitterMean = 106 * time.Millisecond
	jitter := adaptiveAcceptanceScenario{
		name:       "jitter",
		config:     jitterConfig,
		maxConc:    maxConcurrency,
		optimalRPS: float64(8) / jitterMean.Seconds(),
		target: &adaptiveTestTarget{
			capacity: 8,
			service:  jitterService,
		},
	}

	scenarios := []adaptiveAcceptanceScenario{fast, slow, jitter}
	if selected := os.Getenv("COLLY_ADAPTIVE_SCENARIO"); selected != "" {
		filtered := scenarios[:0]
		for _, scenario := range scenarios {
			if scenario.name == selected {
				filtered = append(filtered, scenario)
			}
		}
		scenarios = filtered
	}
	if len(scenarios) == 0 {
		t.Fatalf("unknown COLLY_ADAPTIVE_SCENARIO %q", os.Getenv("COLLY_ADAPTIVE_SCENARIO"))
	}
	for _, scenario := range scenarios {
		runAdaptiveAcceptanceScenario(t, scenario, duration, artifactDir)
	}
}
