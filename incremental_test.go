// Copyright 2018 Adam Tauber
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
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gocolly/colly/v2/storage"
)

// newIncrementalTestServer serves pages that fully support HTTP conditional
// requests (ETag + Last-Modified). It tracks how often a full body was
// transferred (200) versus how often a conditional request got a 304.
func newIncrementalTestServer(t *testing.T, bodies map[string]string) (*httptest.Server, func() (full, notModified int32)) {
	t.Helper()
	var fullCount, notModifiedCount int32
	etags := map[string]string{}
	lastModified := "Wed, 21 Oct 2015 07:28:00 GMT"
	var lock sync.Mutex

	mux := http.NewServeMux()
	for path, body := range bodies {
		mux.HandleFunc(path, func(w http.ResponseWriter, r *http.Request) {
			lock.Lock()
			etag, ok := etags[r.URL.Path]
			if !ok {
				etag = `"etag-` + r.URL.Path + `"`
				etags[r.URL.Path] = etag
			}
			lock.Unlock()

			if match := r.Header.Get("If-None-Match"); match != "" && match == etag {
				atomic.AddInt32(&notModifiedCount, 1)
				w.WriteHeader(http.StatusNotModified)
				return
			}
			if since := r.Header.Get("If-Modified-Since"); since != "" && since == lastModified {
				atomic.AddInt32(&notModifiedCount, 1)
				w.WriteHeader(http.StatusNotModified)
				return
			}

			atomic.AddInt32(&fullCount, 1)
			w.Header().Set("ETag", etag)
			w.Header().Set("Last-Modified", lastModified)
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(body))
		})
	}

	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, func() (int32, int32) {
		return atomic.LoadInt32(&fullCount), atomic.LoadInt32(&notModifiedCount)
	}
}

func TestIncrementalSkipsUnchangedWith304(t *testing.T) {
	const pages = 10
	bodies := make(map[string]string, pages)
	urls := make([]string, pages)
	for i := 0; i < pages; i++ {
		path := "/" + string(rune('a'+i))
		bodies[path] = "content-" + path
		urls[i] = path
	}
	server, stats := newIncrementalTestServer(t, bodies)

	store := &storage.InMemorySnapshotStore{}
	if err := store.SnapshotInit(); err != nil {
		t.Fatalf("SnapshotInit failed: %v", err)
	}

	newCollector := func() *Collector {
		c := NewCollector(SnapshotStorage(store))
		return c
	}

	var parsedFirst, parsedSecond int32
	var skippedFirst, skippedSecond int32

	first := newCollector()
	first.OnResponse(func(r *Response) {
		atomic.AddInt32(&parsedFirst, 1)
	})
	first.OnIncrementalSkip(func(r *Request, reason IncrementalSkipReason) {
		atomic.AddInt32(&skippedFirst, 1)
	})
	for _, path := range urls {
		if err := first.Visit(server.URL + path); err != nil {
			t.Fatalf("first Visit failed: %v", err)
		}
	}

	full, notModified := stats()
	if full != pages || notModified != 0 || parsedFirst != pages || skippedFirst != 0 {
		t.Fatalf("first scan: full=%d 304=%d parsed=%d skipped=%d", full, notModified, parsedFirst, skippedFirst)
	}

	second := newCollector()
	second.OnResponse(func(r *Response) {
		atomic.AddInt32(&parsedSecond, 1)
	})
	second.OnIncrementalSkip(func(r *Request, reason IncrementalSkipReason) {
		atomic.AddInt32(&skippedSecond, 1)
		if reason != IncrementalSkipNotModified {
			t.Errorf("expected not_modified reason, got %s", reason)
		}
	})
	for _, path := range urls {
		if err := second.Visit(server.URL + path); err != nil {
			t.Fatalf("second Visit failed: %v", err)
		}
	}

	full, notModified = stats()
	if full != pages {
		t.Fatalf("no additional full responses expected, got %d", full)
	}
	if notModified != pages {
		t.Fatalf("expected %d 304 responses, got %d", pages, notModified)
	}
	if parsedSecond != 0 {
		t.Fatalf("unchanged pages must not be parsed, got %d parsed", parsedSecond)
	}
	if skippedSecond != pages {
		t.Fatalf("expected %d skip callbacks, got %d", pages, skippedSecond)
	}
}

func TestIncrementalFingerprintSkipsUnchangedWithoutValidators(t *testing.T) {
	var fullCount int32
	body := []byte("<html>stable content that never changes</html>")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&fullCount, 1)
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer server.Close()

	store := &storage.InMemorySnapshotStore{}
	if err := store.SnapshotInit(); err != nil {
		t.Fatalf("SnapshotInit failed: %v", err)
	}

	var parsed int32
	var skips int32
	for scan := 0; scan < 2; scan++ {
		c := NewCollector(SnapshotStorage(store))
		c.OnResponse(func(r *Response) {
			atomic.AddInt32(&parsed, 1)
		})
		c.OnIncrementalSkip(func(r *Request, reason IncrementalSkipReason) {
			if reason != IncrementalSkipFingerprint {
				t.Errorf("expected fingerprint_unchanged, got %s", reason)
			}
			atomic.AddInt32(&skips, 1)
		})
		if err := c.Visit(server.URL + "/no-validators"); err != nil {
			t.Fatalf("scan %d failed: %v", scan, err)
		}
	}

	if got := atomic.LoadInt32(&fullCount); got != 2 {
		t.Fatalf("server without validators always returns full bodies, got %d", got)
	}
	if parsed != 1 || skips != 1 {
		t.Fatalf("expected 1 parse and 1 fingerprint skip, got parsed=%d skips=%d", parsed, skips)
	}
}

func TestIncrementalDetectsChangedContent(t *testing.T) {
	var version atomic.Value
	version.Store("v1")

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No ETag/Last-Modified: change detection must rely on fingerprints.
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("<html>" + version.Load().(string) + "</html>"))
	}))
	defer server.Close()

	store := &storage.InMemorySnapshotStore{}
	if err := store.SnapshotInit(); err != nil {
		t.Fatalf("SnapshotInit failed: %v", err)
	}

	var seen []string
	var mu sync.Mutex

	scan := func() {
		c := NewCollector(SnapshotStorage(store))
		c.OnResponse(func(r *Response) {
			mu.Lock()
			seen = append(seen, string(r.Body))
			mu.Unlock()
		})
		if err := c.Visit(server.URL + "/changing"); err != nil {
			t.Fatalf("Visit failed: %v", err)
		}
	}

	scan()
	version.Store("v2")
	scan()

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 2 {
		t.Fatalf("changed content must be parsed each time, got %d parses", len(seen))
	}
	if seen[0] != "<html>v1</html>" || seen[1] != "<html>v2</html>" {
		t.Fatalf("unexpected parsed bodies: %v", seen)
	}
}

func TestIncrementalDisabledByDefault(t *testing.T) {
	var requests int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.Header().Set("ETag", `"fixed"`)
		w.Header().Set("Last-Modified", "Wed, 21 Oct 2015 07:28:00 GMT")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("same"))
	}))
	defer server.Close()

	var parsed int32
	for i := 0; i < 2; i++ {
		c := NewCollector(AllowURLRevisit())
		c.OnResponse(func(r *Response) {
			atomic.AddInt32(&parsed, 1)
			if r.Request.Headers.Get("If-None-Match") != "" {
				t.Error("conditional headers must not be sent when incremental is disabled")
			}
		})
		if err := c.Visit(server.URL + "/default"); err != nil {
			t.Fatalf("Visit failed: %v", err)
		}
	}

	if got := atomic.LoadInt32(&requests); got != 2 {
		t.Fatalf("default behavior should send normal requests every time, got %d", got)
	}
	if parsed != 2 {
		t.Fatalf("default behavior should parse every response, got %d", parsed)
	}
}

func TestIncrementalWithFileSnapshotPersistence(t *testing.T) {
	const pages = 5
	bodies := make(map[string]string, pages)
	urls := make([]string, pages)
	for i := 0; i < pages; i++ {
		path := "/" + string(rune('p'+i))
		bodies[path] = "persistent-" + path
		urls[i] = path
	}
	server, stats := newIncrementalTestServer(t, bodies)
	snapshotPath := filepath.Join(t.TempDir(), "snapshots.jsonl")

	run := func() {
		store := &storage.FileSnapshotStore{Path: snapshotPath}
		c := NewCollector(SnapshotStorage(store))
		for _, path := range urls {
			if err := c.Visit(server.URL + path); err != nil {
				t.Fatalf("Visit failed: %v", err)
			}
		}
		if err := store.Close(); err != nil {
			t.Fatalf("store close failed: %v", err)
		}
	}

	run()
	run()

	full, notModified := stats()
	if full != pages {
		t.Fatalf("expected %d full fetches across two processes, got %d", pages, full)
	}
	if notModified != pages {
		t.Fatalf("persisted snapshots should yield %d 304s on second run, got %d", pages, notModified)
	}
}

func TestIncremental304WithoutSnapshotIsNotSkipped(t *testing.T) {
	// A misbehaving server may return 304 even though Colly has no stored
	// snapshot. Without a snapshot there is nothing to reconcile against, so
	// the response must follow the normal error callback path instead of
	// being silently skipped.
	var parsed int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotModified)
	}))
	defer server.Close()

	var gotErr bool
	c := NewCollector(Incremental())
	c.OnResponse(func(r *Response) {
		atomic.AddInt32(&parsed, 1)
	})
	c.OnError(func(r *Response, err error) {
		gotErr = true
	})
	if err := c.Visit(server.URL + "/spurious-304"); err == nil {
		t.Fatal("expected an error for 304 without snapshot")
	}
	if parsed != 0 || !gotErr {
		t.Fatalf("response should not be parsed without a snapshot, parsed=%d errorCb=%v", parsed, gotErr)
	}
}

func TestIncrementalErrorResponsesDoNotOverwriteSnapshot(t *testing.T) {
	var ok bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !ok {
			w.Header().Set("ETag", `"good"`)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("good body"))
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("server error"))
	}))
	defer server.Close()

	store := &storage.InMemorySnapshotStore{}
	if err := store.SnapshotInit(); err != nil {
		t.Fatalf("SnapshotInit failed: %v", err)
	}

	first := NewCollector(SnapshotStorage(store))
	if err := first.Visit(server.URL + "/flaky"); err != nil {
		t.Fatalf("first visit failed: %v", err)
	}
	snapshotBefore, err := store.GetSnapshot(server.URL + "/flaky")
	if err != nil {
		t.Fatalf("snapshot missing after successful fetch: %v", err)
	}

	ok = true
	second := NewCollector(SnapshotStorage(store), ParseHTTPErrorResponse())
	if err := second.Visit(server.URL + "/flaky"); err != nil {
		t.Fatalf("ParseHTTPErrorResponse should surface 500 without Visit error: %v", err)
	}

	snapshotAfter, err := store.GetSnapshot(server.URL + "/flaky")
	if err != nil {
		t.Fatalf("snapshot disappeared after error response: %v", err)
	}
	if snapshotBefore.Fingerprint != snapshotAfter.Fingerprint ||
		snapshotBefore.ETag != snapshotAfter.ETag {
		t.Fatalf("error response must not overwrite the snapshot: before=%+v after=%+v", snapshotBefore, snapshotAfter)
	}
}

func TestIncrementalEnvSwitch(t *testing.T) {
	var fullCount, notModifiedCount int32
	etag := `"env-etag"`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") == etag {
			atomic.AddInt32(&notModifiedCount, 1)
			w.WriteHeader(http.StatusNotModified)
			return
		}
		atomic.AddInt32(&fullCount, 1)
		w.Header().Set("ETag", etag)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("env body"))
	}))
	defer server.Close()

	snapshotPath := filepath.Join(t.TempDir(), "env-snapshots.jsonl")
	t.Setenv("COLLY_INCREMENTAL", "1")
	t.Setenv("COLLY_SNAPSHOT_FILE", snapshotPath)

	var skips int32
	scan := func() {
		c := NewCollector()
		c.OnIncrementalSkip(func(r *Request, reason IncrementalSkipReason) {
			atomic.AddInt32(&skips, 1)
		})
		if err := c.Visit(server.URL + "/env"); err != nil {
			t.Fatalf("Visit failed: %v", err)
		}
	}

	scan()
	scan()

	if atomic.LoadInt32(&fullCount) != 1 {
		t.Fatalf("expected 1 full fetch, got %d", fullCount)
	}
	if atomic.LoadInt32(&notModifiedCount) != 1 || atomic.LoadInt32(&skips) != 1 {
		t.Fatalf("expected 1 304 and 1 skip, got 304=%d skips=%d", notModifiedCount, skips)
	}
}

func TestIncrementalConcurrentCollectors(t *testing.T) {
	const pages = 20
	bodies := make(map[string]string, pages)
	urls := make([]string, pages)
	for i := 0; i < pages; i++ {
		path := "/" + string(rune('a'+i)) + string(rune('a'+i))
		bodies[path] = "concurrent-" + path
		urls[i] = path
	}
	server, stats := newIncrementalTestServer(t, bodies)

	store := &storage.InMemorySnapshotStore{}
	if err := store.SnapshotInit(); err != nil {
		t.Fatalf("SnapshotInit failed: %v", err)
	}

	scan := func() {
		c := NewCollector(Async(true), SnapshotStorage(store))
		c.Wait()
		for _, path := range urls {
			if err := c.Visit(server.URL + path); err != nil {
				t.Errorf("Visit failed: %v", err)
			}
		}
		c.Wait()
	}

	scan()
	scan()

	full, notModified := stats()
	if full != pages {
		t.Fatalf("expected %d full fetches, got %d", pages, full)
	}
	if notModified != pages {
		t.Fatalf("expected %d 304s on concurrent second scan, got %d", pages, notModified)
	}
}
