// Incremental_benchmark simulates a daily inspection crawl against a local
// HTTP server and compares a full crawl (default Colly behavior) with an
// incremental crawl (conditional requests + content fingerprints).
//
// The simulated server serves three kinds of pages:
//   - validator pages respond to If-None-Match/If-Modified-Since with 304;
//   - fingerprint pages send no validators, so unchanged bodies are detected
//     client side with a SHA-256 fingerprint;
//   - 20% of every page is "changed" between the two crawls and must be
//     re-fetched and re-parsed.
//
// Usage:
//
//	go run ./_examples/incremental_benchmark [-pages N] [-workdir DIR]
package main

import (
	"flag"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/storage"
)

type pageSpec struct {
	path         string
	hasValidator bool
	version      int
}

type serverStats struct {
	httpRequests int64
	fullBodies   int64
	notModified  int64
	bytesSent    int64
}

type crawlResult struct {
	name          string
	httpRequests  int64
	fullBodies    int64
	notModified   int64
	parsed        int64
	skipped304    int64
	skippedFinger int64
	bytesReceived int64
}

func main() {
	pageCount := flag.Int("pages", 100, "number of simulated pages")
	changedRatio := flag.Float64("changed-ratio", 0.2, "fraction of pages changed before the incremental crawl")
	noValidatorRatio := flag.Float64("no-validator-ratio", 0.05, "fraction of pages without ETag/Last-Modified support")
	workdir := flag.String("workdir", ".", "directory for the persistent snapshot file")
	flag.Parse()

	pages := make([]*pageSpec, *pageCount)
	for i := range pages {
		pages[i] = &pageSpec{
			path:         fmt.Sprintf("/page/%04d", i),
			hasValidator: float64(i)/float64(*pageCount) >= *noValidatorRatio,
			version:      1,
		}
	}

	var stats serverStats
	mux := http.NewServeMux()
	mux.HandleFunc("/page/", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&stats.httpRequests, 1)
		spec := findPage(pages, r.URL.Path)
		if spec == nil {
			http.NotFound(w, r)
			return
		}
		etag := fmt.Sprintf(`"v%d"`, spec.version)
		lastModified := fmt.Sprintf("Wed, 21 Oct 2015 07:28:%02d GMT", spec.version)

		if spec.hasValidator {
			if r.Header.Get("If-None-Match") == etag ||
				(spec.version == 1 && r.Header.Get("If-Modified-Since") == lastModified) {
				atomic.AddInt64(&stats.notModified, 1)
				w.WriteHeader(http.StatusNotModified)
				return
			}
		}

		body := []byte(fmt.Sprintf("<html><body>%s content version %d</body></html>", spec.path, spec.version))
		atomic.AddInt64(&stats.fullBodies, 1)
		atomic.AddInt64(&stats.bytesSent, int64(len(body)))
		if spec.hasValidator {
			w.Header().Set("ETag", etag)
			w.Header().Set("Last-Modified", lastModified)
		}
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	urls := make([]string, len(pages))
	for i, spec := range pages {
		urls[i] = server.URL + spec.path
	}

	fmt.Printf("Simulated site: %d pages, %.0f%% without validators, %.0f%% changed between crawls\n\n",
		*pageCount, (*noValidatorRatio)*100, (*changedRatio)*100)

	// Baseline: default Colly behavior (incremental disabled). Two daily
	// full crawls, the second one after the same change set as below.
	baselineFirst := runFullCrawl("full crawl #1 (before change)", urls, &stats)

	changed := applyChanges(pages, *changedRatio)
	baselineSecond := runFullCrawl("full crawl #2 (after change)", urls, &stats)

	// Reset site to v1 so the incremental crawl starts from the same state
	// the first full crawl observed.
	for _, spec := range pages {
		spec.version = 1
	}
	// The full crawls above already populated server counters; keep only the
	// page versions and reset counters so the incremental comparison is not
	// polluted by baseline traffic.
	atomic.StoreInt64(&stats.httpRequests, 0)
	atomic.StoreInt64(&stats.fullBodies, 0)
	atomic.StoreInt64(&stats.notModified, 0)
	atomic.StoreInt64(&stats.bytesSent, 0)

	// Incremental run #1 builds the persistent snapshot store.
	snapshotPath := filepath.Join(*workdir, "incremental-snapshots.jsonl")
	if err := os.MkdirAll(*workdir, 0o750); err != nil {
		fmt.Fprintf(os.Stderr, "workdir create failed: %v\n", err)
		os.Exit(1)
	}
	incFirst := runIncrementalCrawl("incremental crawl #1 (warm up)", urls, &stats, snapshotPath)

	// Apply the identical change set, then run the real comparison crawl in
	// a fresh collector, replaying snapshots from disk.
	for _, idx := range changed {
		pages[idx].version = 2
	}
	incSecond := runIncrementalCrawl("incremental crawl #2 (after change)", urls, &stats, snapshotPath)

	baseline := addResults(baselineFirst, baselineSecond)
	baseline.name = "full crawl (2 daily runs)"
	incremental := addResults(incFirst, incSecond)
	incremental.name = "incremental crawl (2 daily runs)"

	printReport(*pageCount, len(changed), baseline, incremental, incSecond)

	// Acceptance gates.
	fetchReduction := 1 - float64(incSecond.fullBodies)/float64(incSecond.httpRequests)
	parseReduction := 1 - float64(incSecond.parsed)/float64(*pageCount)
	recall := float64(incSecond.parsed) / float64(len(changed))

	failed := false
	if fetchReduction < 0.70 {
		fmt.Printf("FAIL: full-content fetch reduction %.1f%% is below the 70%% target\n", fetchReduction*100)
		failed = true
	}
	if parseReduction < 0.70 {
		fmt.Printf("FAIL: parsed-page reduction %.1f%% is below the 70%% target\n", parseReduction*100)
		failed = true
	}
	if recall < 1.0 {
		fmt.Printf("FAIL: change recall %.1f%% is below 100%%\n", recall*100)
		failed = true
	}
	if failed {
		os.Exit(1)
	}
	fmt.Println("\nAll acceptance criteria passed: fetch/parse reduction >= 70%, change recall = 100%")
}

func runFullCrawl(name string, urls []string, stats *serverStats) crawlResult {
	before := snapshotStats(stats)
	c := colly.NewCollector(colly.AllowURLRevisit())
	var parsed int64
	c.OnResponse(func(r *colly.Response) {
		atomic.AddInt64(&parsed, 1)
	})
	for _, u := range urls {
		if err := c.Visit(u); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
			os.Exit(1)
		}
	}
	after := snapshotStats(stats)
	return crawlResult{
		name:          name,
		httpRequests:  after.httpRequests - before.httpRequests,
		fullBodies:    after.fullBodies - before.fullBodies,
		notModified:   after.notModified - before.notModified,
		parsed:        parsed,
		bytesReceived: after.bytesSent - before.bytesSent,
	}
}

func runIncrementalCrawl(name string, urls []string, stats *serverStats, snapshotPath string) crawlResult {
	before := snapshotStats(stats)
	store := &storage.FileSnapshotStore{Path: snapshotPath}
	c := colly.NewCollector(colly.SnapshotStorage(store))

	var parsed, skipped304, skippedFinger int64
	c.OnResponse(func(r *colly.Response) {
		atomic.AddInt64(&parsed, 1)
	})
	c.OnIncrementalSkip(func(r *colly.Request, reason colly.IncrementalSkipReason) {
		switch reason {
		case colly.IncrementalSkipNotModified:
			atomic.AddInt64(&skipped304, 1)
		case colly.IncrementalSkipFingerprint:
			atomic.AddInt64(&skippedFinger, 1)
		}
	})
	for _, u := range urls {
		if err := c.Visit(u); err != nil {
			fmt.Fprintf(os.Stderr, "%s: %v\n", name, err)
			os.Exit(1)
		}
	}
	if err := store.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "%s close: %v\n", name, err)
		os.Exit(1)
	}
	after := snapshotStats(stats)
	return crawlResult{
		name:          name,
		httpRequests:  after.httpRequests - before.httpRequests,
		fullBodies:    after.fullBodies - before.fullBodies,
		notModified:   after.notModified - before.notModified,
		parsed:        parsed,
		skipped304:    skipped304,
		skippedFinger: skippedFinger,
		bytesReceived: after.bytesSent - before.bytesSent,
	}
}

func applyChanges(pages []*pageSpec, ratio float64) []int {
	changedCount := int(float64(len(pages)) * ratio)
	indices := make([]int, changedCount)
	changed := 0
	// Leave the no-validator pages unchanged so the fingerprint path is
	// exercised; change validator-capable pages instead.
	for i := len(pages) - 1; i >= 0 && changed < changedCount; i-- {
		if !pages[i].hasValidator {
			continue
		}
		pages[i].version = 2
		indices[changed] = i
		changed++
	}
	return indices
}

func findPage(pages []*pageSpec, path string) *pageSpec {
	for _, spec := range pages {
		if spec.path == path {
			return spec
		}
	}
	return nil
}

func snapshotStats(stats *serverStats) serverStats {
	return serverStats{
		httpRequests: atomic.LoadInt64(&stats.httpRequests),
		fullBodies:   atomic.LoadInt64(&stats.fullBodies),
		notModified:  atomic.LoadInt64(&stats.notModified),
		bytesSent:    atomic.LoadInt64(&stats.bytesSent),
	}
}

func addResults(a, b crawlResult) crawlResult {
	return crawlResult{
		httpRequests:  a.httpRequests + b.httpRequests,
		fullBodies:    a.fullBodies + b.fullBodies,
		notModified:   a.notModified + b.notModified,
		parsed:        a.parsed + b.parsed,
		skipped304:    a.skipped304 + b.skipped304,
		skippedFinger: a.skippedFinger + b.skippedFinger,
		bytesReceived: a.bytesReceived + b.bytesReceived,
	}
}

func printReport(pageCount, changedCount int, baseline, incremental, secondIncremental crawlResult) {
	fmt.Println("================ before / after comparison ================")
	header := "%-34s %14s %14s %12s %14s\n"
	fmt.Printf(header, "metric", "full crawl", "incremental", "delta", "reduction")
	row := func(label string, base, inc int64) {
		reduction := 0.0
		if base > 0 {
			reduction = (1 - float64(inc)/float64(base)) * 100
		}
		fmt.Printf("%-34s %14d %14d %12d %13.1f%%\n", label, base, inc, base-inc, reduction)
	}
	row("HTTP requests (total, 2 runs)", baseline.httpRequests, incremental.httpRequests)
	row("full-content fetches (2 runs)", baseline.fullBodies, incremental.fullBodies)
	row("parsed pages (2 runs)", baseline.parsed, incremental.parsed)
	row("response bytes received (2 runs)", baseline.bytesReceived, incremental.bytesReceived)

	fmt.Println("\n---------------- second (daily) crawl only ----------------")
	fmt.Printf("pages total:                 %d\n", pageCount)
	fmt.Printf("pages changed:               %d (%.0f%%)\n", changedCount, float64(changedCount)/float64(pageCount)*100)
	fmt.Printf("HTTP requests sent:          %d (conditional GET; 304 carries no body)\n", secondIncremental.httpRequests)
	fmt.Printf("304 Not Modified responses:  %d\n", secondIncremental.skipped304)
	fmt.Printf("fingerprint skips (200):     %d\n", secondIncremental.skippedFinger)
	fmt.Printf("full-content fetches:        %d\n", secondIncremental.fullBodies)
	fmt.Printf("pages parsed:                %d\n", secondIncremental.parsed)

	fetchReduction := (1 - float64(secondIncremental.fullBodies)/float64(secondIncremental.httpRequests)) * 100
	parseReduction := (1 - float64(secondIncremental.parsed)/float64(pageCount)) * 100
	recall := float64(secondIncremental.parsed) / float64(changedCount) * 100

	fmt.Println("\n------------ second crawl: before vs after change -----------")
	fmt.Printf("%-30s %14s %14s %13s\n", "metric", "full crawl", "incremental", "reduction")
	fmt.Printf("%-30s %14d %14d %12.1f%%\n", "full-content fetches", int64(pageCount), secondIncremental.fullBodies, fetchReduction)
	fmt.Printf("%-30s %14d %14d %12.1f%%\n", "pages parsed", int64(pageCount), secondIncremental.parsed, parseReduction)
	fmt.Printf("\nfull-content fetch reduction: %.1f%% (target >= 70%%)\n", fetchReduction)
	fmt.Printf("parsed-page reduction:        %.1f%% (target >= 70%%)\n", parseReduction)
	fmt.Printf("change recall:                %.1f%% (target 100%%)\n", recall)
}
