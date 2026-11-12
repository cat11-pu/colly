package main

import (
	"flag"
	"fmt"
	"log"

	"github.com/gocolly/colly/v2"
	"github.com/gocolly/colly/v2/queue"
)

func main() {
	stateDir := flag.String("state", "crawl-state", "durable queue state directory")
	baseURL := flag.String("base", "https://example.com", "base URL of the pages to crawl")
	start := flag.Int("start", 0, "first page number")
	count := flag.Int("count", 100, "number of pages")
	flag.Parse()

	q, err := queue.NewPersistent(4, *stateDir)
	if err != nil {
		log.Fatal(err)
	}
	defer q.Close()

	c := colly.NewCollector(colly.AllowURLRevisit())
	c.OnResponse(func(r *colly.Response) {
		fmt.Println(r.Request.URL)
	})

	for i := *start; i < *start+*count; i++ {
		if err := q.AddURL(fmt.Sprintf("%s/page/%d", *baseURL, i)); err != nil {
			log.Fatal(err)
		}
	}
	if err := q.Run(c); err != nil {
		log.Fatal(err)
	}
}
