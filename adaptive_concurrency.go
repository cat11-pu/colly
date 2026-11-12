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
	"math"
	"sort"
	"sync"
	"time"
)

const (
	defaultAdaptiveMinConcurrency    = 1
	defaultAdaptiveMaxConcurrency    = 64
	defaultAdaptiveTargetLatency     = 500 * time.Millisecond
	defaultAdaptiveLatencyPercentile = 0.99
	defaultAdaptiveWindow            = 3 * time.Second
	defaultAdaptiveSampleInterval    = 200 * time.Millisecond
	defaultAdaptiveErrorRate         = 0.05
	defaultAdaptiveMinSamples        = 8
	maxAdaptiveSamples               = 16384
)

// AdaptiveConcurrencyConfig tunes the latency- and error-aware concurrency
// controller used by LimitRule when AdaptiveConcurrency is enabled.
//
// All zero values select the defaults documented on the individual fields.
type AdaptiveConcurrencyConfig struct {
	// MinConcurrency is the lower bound of the concurrency window.
	// The default is 1.
	MinConcurrency int
	// MaxConcurrency is the upper bound of the concurrency window.
	// When zero, LimitRule.Parallelism is used if it is positive; otherwise
	// the default is 64.
	MaxConcurrency int
	// InitialConcurrency is the concurrency used when the first request is
	// issued. When zero, MinConcurrency is used.
	InitialConcurrency int
	// TargetLatency is the tail-latency budget. The controller increases
	// concurrency while the configured latency percentile is below this
	// value and decreases it once the percentile exceeds the budget.
	// The default is 500ms.
	TargetLatency time.Duration
	// LatencyPercentile is the percentile used by the controller to decide
	// whether the target is overloaded. The default is 0.99 (p99).
	LatencyPercentile float64
	// Window is how far back the controller looks for latency and error-rate
	// samples. The default is 3 seconds.
	Window time.Duration
	// SampleInterval is the minimum time between concurrency adjustments.
	// The default is 200ms.
	SampleInterval time.Duration
	// ErrorRateThreshold is the fraction of failed requests tolerated in the
	// observation window before concurrency is reduced. The default is 0.05.
	ErrorRateThreshold float64
	// MinSamples is the minimum number of samples in the observation window
	// before latency-based adjustments are made. Error-based reductions are
	// always considered. The default is 8.
	MinSamples int
}

// DefaultAdaptiveConcurrencyConfig returns the default controller settings.
func DefaultAdaptiveConcurrencyConfig() AdaptiveConcurrencyConfig {
	return AdaptiveConcurrencyConfig{
		MinConcurrency:     defaultAdaptiveMinConcurrency,
		InitialConcurrency: defaultAdaptiveMinConcurrency,
		TargetLatency:      defaultAdaptiveTargetLatency,
		LatencyPercentile:  defaultAdaptiveLatencyPercentile,
		Window:             defaultAdaptiveWindow,
		SampleInterval:     defaultAdaptiveSampleInterval,
		ErrorRateThreshold: defaultAdaptiveErrorRate,
		MinSamples:         defaultAdaptiveMinSamples,
	}
}

// AdaptiveConcurrencyStats is a point-in-time snapshot of the adaptive
// controller. Throughput and the percentiles describe the observation window.
type AdaptiveConcurrencyStats struct {
	Timestamp   time.Time
	Concurrency int
	InFlight    int
	Throughput  float64
	P99Latency  time.Duration
	ErrorRate   float64
	Samples     int
}

type adaptiveSettings struct {
	min            int
	max            int
	initial        int
	targetLatency  time.Duration
	percentile     float64
	window         time.Duration
	sampleInterval time.Duration
	errorRate      float64
	minSamples     int
}

func (r *LimitRule) adaptiveSettings() adaptiveSettings {
	s := adaptiveSettings{
		min:            defaultAdaptiveMinConcurrency,
		max:            0,
		initial:        defaultAdaptiveMinConcurrency,
		targetLatency:  defaultAdaptiveTargetLatency,
		percentile:     defaultAdaptiveLatencyPercentile,
		window:         defaultAdaptiveWindow,
		sampleInterval: defaultAdaptiveSampleInterval,
		errorRate:      defaultAdaptiveErrorRate,
		minSamples:     defaultAdaptiveMinSamples,
	}

	if c := r.AdaptiveConcurrencyConfig; c != nil {
		if c.MinConcurrency > 0 {
			s.min = c.MinConcurrency
		}
		if c.MaxConcurrency > 0 {
			s.max = c.MaxConcurrency
		}
		if c.InitialConcurrency > 0 {
			s.initial = c.InitialConcurrency
		}
		if c.TargetLatency > 0 {
			s.targetLatency = c.TargetLatency
		}
		if c.LatencyPercentile > 0 && c.LatencyPercentile <= 1 {
			s.percentile = c.LatencyPercentile
		}
		if c.Window > 0 {
			s.window = c.Window
		}
		if c.SampleInterval > 0 {
			s.sampleInterval = c.SampleInterval
		}
		if c.ErrorRateThreshold > 0 && c.ErrorRateThreshold <= 1 {
			s.errorRate = c.ErrorRateThreshold
		}
		if c.MinSamples > 0 {
			s.minSamples = c.MinSamples
		}
	}

	if s.max == 0 {
		if r.Parallelism > 0 {
			s.max = r.Parallelism
		} else {
			s.max = defaultAdaptiveMaxConcurrency
		}
	}
	if s.max < s.min {
		s.max = s.min
	}
	if s.initial < s.min {
		s.initial = s.min
	}
	if s.initial > s.max {
		s.initial = s.max
	}
	if s.window < s.sampleInterval {
		s.window = s.sampleInterval
	}
	return s
}

type adaptiveSample struct {
	at      time.Time
	latency time.Duration
	gen     uint64
	err     bool
}

type adaptiveLimiter struct {
	mu     sync.Mutex
	cond   *sync.Cond
	config adaptiveSettings

	limit      int
	inFlight   int
	generation uint64
	started    time.Time
	lastAdjust time.Time
	changeAt   time.Time
	probeAfter time.Time
	prevLimit  int
	prevRate   float64
	afterRaise bool
	probing    bool
	samples    []adaptiveSample
}

func newAdaptiveLimiter(config adaptiveSettings) *adaptiveLimiter {
	a := &adaptiveLimiter{
		config:  config,
		limit:   config.initial,
		started: time.Now(),
		samples: make([]adaptiveSample, 0, 256),
	}
	a.cond = sync.NewCond(&a.mu)
	return a
}

func (a *adaptiveLimiter) acquire() uint64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	for {
		a.adjustLocked(time.Now())
		if a.inFlight < a.limit {
			a.inFlight++
			return a.generation
		}
		a.cond.Wait()
	}
}

func (a *adaptiveLimiter) release(gen uint64, latency time.Duration, at time.Time, failed bool) {
	a.mu.Lock()
	a.samples = append(a.samples, adaptiveSample{
		at:      at,
		latency: latency,
		gen:     gen,
		err:     failed,
	})
	a.inFlight--
	a.trimLocked(time.Now())
	a.adjustLocked(time.Now())
	a.cond.Broadcast()
	a.mu.Unlock()
}

func (a *adaptiveLimiter) snapshot() AdaptiveConcurrencyStats {
	a.mu.Lock()
	defer a.mu.Unlock()

	now := time.Now()
	a.trimLocked(now)
	n := len(a.samples)
	stats := AdaptiveConcurrencyStats{
		Timestamp:   now,
		Concurrency: a.limit,
		InFlight:    a.inFlight,
		Samples:     n,
	}
	if n == 0 {
		return stats
	}

	stats.P99Latency = samplePercentile(a.samples, 0.99)
	errs := 0
	for _, sample := range a.samples {
		if sample.err {
			errs++
		}
	}
	stats.ErrorRate = float64(errs) / float64(n)

	window := now.Sub(a.started)
	if window > a.config.window {
		window = a.config.window
	}
	if window > 0 {
		stats.Throughput = float64(n) / window.Seconds()
	}
	return stats
}

func (a *adaptiveLimiter) trimLocked(now time.Time) {
	cutoff := now.Add(-a.config.window)
	first := 0
	for first < len(a.samples) && a.samples[first].at.Before(cutoff) {
		first++
	}
	if first > 0 {
		copy(a.samples, a.samples[first:])
		a.samples = a.samples[:len(a.samples)-first]
	}
	if len(a.samples) > maxAdaptiveSamples {
		drop := len(a.samples) - maxAdaptiveSamples
		copy(a.samples, a.samples[drop:])
		a.samples = a.samples[:maxAdaptiveSamples]
	}
}

func (a *adaptiveLimiter) adjustLocked(now time.Time) {
	if now.Sub(a.lastAdjust) < a.config.sampleInterval {
		return
	}
	a.trimLocked(now)
	if len(a.samples) == 0 {
		return
	}
	a.lastAdjust = now

	controlSamples, fresh := a.controlSamples()
	if len(controlSamples) == 0 {
		return
	}
	if a.generation > 0 && fresh < a.config.minSamples {
		return
	}
	if !a.changeAt.IsZero() && now.Sub(a.changeAt) >= a.config.window {
		controlSamples = a.samples
	}

	errs := 0
	for _, sample := range controlSamples {
		if sample.err {
			errs++
		}
	}
	errorRate := float64(errs) / float64(len(controlSamples))

	if errorRate > a.config.errorRate {
		factor := 0.9
		if errorRate >= 6*a.config.errorRate {
			factor = 0.5
		} else if errorRate >= 3*a.config.errorRate {
			factor = 0.75
		}
		a.decreaseLocked(factor, now)
		return
	}

	if len(controlSamples) < a.config.minSamples {
		return
	}

	latency := samplePercentile(controlSamples, a.config.percentile)
	target := a.config.targetLatency

	if a.afterRaise {
		rate := a.windowThroughputLocked(now)
		if rate < a.prevRate*1.03 {
			a.setLimitLocked(a.prevLimit, now)
			a.afterRaise = false
			a.probing = true
			a.probeAfter = now.Add(a.config.window)
			return
		}
		a.afterRaise = false
		a.probing = false
	}

	windowTail := samplePercentile(a.samples, a.config.percentile)
	switch {
	case latency > target*9/10:
		factor := 0.9
		if latency > 2*target {
			factor = 0.6
		} else if latency > 3*target/2 {
			factor = 0.8
		}
		a.decreaseLocked(factor, now)
	case latency < target*8/10:
		if windowTail < target*8/10 && !now.Before(a.probeAfter) {
			a.increaseLocked(target, latency, now)
		}
	}
}

func (a *adaptiveLimiter) controlSamples() ([]adaptiveSample, int) {
	current := 0
	for i := len(a.samples) - 1; i >= 0; i-- {
		if a.samples[i].gen == a.generation {
			current++
		}
	}
	if current < a.config.minSamples {
		return a.samples, current
	}

	result := make([]adaptiveSample, 0, current)
	for _, sample := range a.samples {
		if sample.gen == a.generation {
			result = append(result, sample)
		}
	}
	return result, current
}

func (a *adaptiveLimiter) windowThroughputLocked(now time.Time) float64 {
	if len(a.samples) == 0 {
		return 0
	}
	window := now.Sub(a.started)
	if window > a.config.window {
		window = a.config.window
	}
	if window <= 0 {
		return 0
	}
	return float64(len(a.samples)) / window.Seconds()
}

func (a *adaptiveLimiter) increaseLocked(target, latency time.Duration, now time.Time) {
	if a.limit >= a.config.max {
		return
	}

	if a.probing {
		next := a.limit + 1
		if next > a.config.max {
			next = a.config.max
		}
		a.setLimitLocked(next, now)
		return
	}

	headroom := float64(target-latency) / float64(target)
	step := 1
	switch {
	case headroom > 0.5:
		step = int(math.Ceil(float64(a.limit) * 0.5))
	case headroom > 0.25:
		step = int(math.Ceil(float64(a.limit) * 0.2))
	case headroom > 0.1:
		step = int(math.Ceil(float64(a.limit) * 0.1))
	}
	if step < 1 {
		step = 1
	}

	next := a.limit + step
	if next > a.config.max {
		next = a.config.max
	}
	a.prevLimit = a.limit
	a.prevRate = a.windowThroughputLocked(now)
	a.afterRaise = true
	a.setLimitLocked(next, now)
}

func (a *adaptiveLimiter) decreaseLocked(factor float64, now time.Time) {
	if a.limit <= a.config.min {
		return
	}
	next := int(math.Floor(float64(a.limit) * factor))
	if next >= a.limit {
		next = a.limit - 1
	}
	if next < a.config.min {
		next = a.config.min
	}
	a.afterRaise = false
	a.probing = true
	a.probeAfter = time.Time{}
	a.setLimitLocked(next, now)
}

func (a *adaptiveLimiter) setLimitLocked(next int, now time.Time) {
	if next == a.limit {
		return
	}
	a.limit = next
	a.generation++
	a.lastAdjust = now
	a.changeAt = now
	a.cond.Broadcast()
}

func samplePercentile(samples []adaptiveSample, percentile float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	if percentile <= 0 || percentile > 1 {
		percentile = 0.99
	}

	latencies := make([]time.Duration, len(samples))
	for i, sample := range samples {
		latencies[i] = sample.latency
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	index := int(math.Ceil(percentile*float64(len(latencies))) - 1)
	if index < 0 {
		index = 0
	}
	if index >= len(latencies) {
		index = len(latencies) - 1
	}
	return latencies[index]
}

// AdaptiveConcurrencyStats returns a snapshot of the adaptive controller.
// It returns the zero value when adaptive concurrency is disabled.
func (r *LimitRule) AdaptiveConcurrencyStats() AdaptiveConcurrencyStats {
	r.lock.Lock()
	a := r.adaptive
	r.lock.Unlock()
	if a == nil {
		return AdaptiveConcurrencyStats{}
	}
	return a.snapshot()
}

// AdaptiveConcurrencyLimit returns the controller's current concurrency limit.
// It returns zero when adaptive concurrency is disabled.
func (r *LimitRule) AdaptiveConcurrencyLimit() int {
	r.lock.Lock()
	a := r.adaptive
	r.lock.Unlock()
	if a == nil {
		return 0
	}
	return a.snapshot().Concurrency
}
