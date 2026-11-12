package queue

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/gocolly/colly/v2"
)

func TestFileQueueStorageDeduplicatesAcrossInstances(t *testing.T) {
	const (
		instances = 3
		pageCount = 1000
	)

	hits := newCountingServer(pageCount)
	server := httptest.NewServer(http.HandlerFunc(hits.handle))
	defer server.Close()
	urls := hits.urls(server.URL)

	queues := make([]*Queue, instances)
	// Use one shared directory while keeping separate Queue instances, like
	// separate processes opening the same crawl state.
	dir := t.TempDir()
	var sharedStorages []*FileQueueStorage
	for i := range queues {
		storage := NewFileQueueStorage(dir)
		q, err := New(4, storage)
		if err != nil {
			t.Fatalf("create shared queue %d: %v", i, err)
		}
		queues[i] = q
		sharedStorages = append(sharedStorages, storage)
	}
	t.Cleanup(func() {
		for _, q := range queues {
			_ = q.Close()
		}
	})

	for i, q := range queues {
		for _, u := range urls {
			if err := q.AddURL(u); err != nil {
				t.Fatalf("queue %d add %s: %v", i, u, err)
			}
		}
	}
	if size, err := sharedStorages[0].QueueSize(); err != nil {
		t.Fatalf("queue size: %v", err)
	} else if size != pageCount {
		t.Fatalf("queue size after duplicate enqueue = %d, want %d", size, pageCount)
	}

	var wg sync.WaitGroup
	errs := make(chan error, instances)
	for i, q := range queues {
		wg.Add(1)
		go func(i int, q *Queue) {
			defer wg.Done()
			c := colly.NewCollector(colly.AllowURLRevisit())
			if err := q.Run(c); err != nil {
				errs <- fmt.Errorf("queue %d: %w", i, err)
			}
		}(i, q)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	hits.assertExactlyOnce(t, pageCount)
}

func TestFileQueueStorageResumesCompletedCrawl(t *testing.T) {
	const pageCount = 100

	hits := newCountingServer(pageCount)
	server := httptest.NewServer(http.HandlerFunc(hits.handle))
	defer server.Close()
	urls := hits.urls(server.URL)
	dir := t.TempDir()

	first, err := NewPersistent(4, dir)
	if err != nil {
		t.Fatalf("create first queue: %v", err)
	}
	for _, u := range urls {
		if err := first.AddURL(u); err != nil {
			t.Fatalf("add %s: %v", u, err)
		}
	}
	if err := first.Run(colly.NewCollector(colly.AllowURLRevisit())); err != nil {
		t.Fatalf("first run: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first queue: %v", err)
	}
	hits.assertExactlyOnce(t, pageCount)

	second, err := NewPersistent(4, dir)
	if err != nil {
		t.Fatalf("create second queue: %v", err)
	}
	for _, u := range urls {
		if err := second.AddURL(u); err != nil {
			t.Fatalf("re-add %s: %v", u, err)
		}
	}
	if err := second.Run(colly.NewCollector(colly.AllowURLRevisit())); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("close second queue: %v", err)
	}
	hits.assertExactlyOnce(t, pageCount)
}

func TestFileQueueStorageRecoversClaimedRequests(t *testing.T) {
	const (
		pageCount = 100
		claimed   = 25
	)

	hits := newCountingServer(pageCount)
	server := httptest.NewServer(http.HandlerFunc(hits.handle))
	defer server.Close()
	urls := hits.urls(server.URL)
	dir := t.TempDir()

	firstStore := NewFileQueueStorage(dir)
	first, err := New(1, firstStore)
	if err != nil {
		t.Fatalf("create first queue: %v", err)
	}
	for _, u := range urls {
		if err := first.AddURL(u); err != nil {
			t.Fatalf("add %s: %v", u, err)
		}
	}
	for i := 0; i < claimed; i++ {
		request, err := firstStore.GetRequest()
		if err != nil {
			t.Fatalf("claim request %d: %v", i, err)
		}
		if request == nil {
			t.Fatalf("claim request %d returned no data", i)
		}
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close interrupted queue: %v", err)
	}

	second, err := NewPersistent(4, dir)
	if err != nil {
		t.Fatalf("create resumed queue: %v", err)
	}
	for _, u := range urls {
		if err := second.AddURL(u); err != nil {
			t.Fatalf("re-add %s: %v", u, err)
		}
	}
	if err := second.Run(colly.NewCollector(colly.AllowURLRevisit())); err != nil {
		t.Fatalf("resumed run: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("close resumed queue: %v", err)
	}
	hits.assertExactlyOnce(t, pageCount)
}

type countingServer struct {
	mu   sync.Mutex
	hits map[int]*atomic.Int64
}

func newCountingServer(pageCount int) *countingServer {
	hits := make(map[int]*atomic.Int64, pageCount)
	for i := 0; i < pageCount; i++ {
		hits[i] = &atomic.Int64{}
	}
	return &countingServer{hits: hits}
}

func (s *countingServer) handle(w http.ResponseWriter, r *http.Request) {
	index, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/page/"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	s.mu.Lock()
	hits := s.hits[index]
	s.mu.Unlock()
	if hits == nil {
		http.NotFound(w, r)
		return
	}
	hits.Add(1)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = fmt.Fprintf(w, "<html><body>page %d</body></html>", index)
}

func (s *countingServer) urls(base string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	urls := make([]string, 0, len(s.hits))
	for i := range s.hits {
		urls = append(urls, fmt.Sprintf("%s/page/%d", base, i))
	}
	return urls
}

func (s *countingServer) assertExactlyOnce(t *testing.T, pageCount int) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.hits) != pageCount {
		t.Fatalf("server has %d pages, want %d", len(s.hits), pageCount)
	}
	for i := 0; i < pageCount; i++ {
		if got := s.hits[i].Load(); got != 1 {
			t.Fatalf("page %d requested %d times, want 1", i, got)
		}
	}
}
