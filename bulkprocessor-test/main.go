package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"time"

	"gopkg.in/olivere/elastic.v5"
)

const (
	DefaultIndex    = "bulkprocessor-test"
	DefaultType     = "record"
	DefaultInterval = 10 * time.Millisecond
)

func main() {
	log.SetFlags(0)
	var (
		nodes         = flag.String("nodes", "http://localhost:9200", "Elasticsearch URL (comma-separated list)")
		workers       = flag.Int("workers", 5, "Number of workers for bulk processor")
		index         = flag.String("index", DefaultIndex, "name of ES index to use")
		typ           = flag.String("type", DefaultType, "name of ES index to use")
		interval      = flag.Duration("interval", DefaultInterval, "Interval between sending documents to Bulk Processor")
		errorlogfile  = flag.String("errorlog", "", "error log file")
		infologfile   = flag.String("infolog", "", "info log file")
		tracelogfile  = flag.String("tracelog", "", "trace log file")
		sniff         = flag.Bool("sniff", elastic.DefaultSnifferEnabled, "enable or disable sniffer")
		sniffer       = flag.Duration("sniffer", elastic.DefaultSnifferInterval, "sniffer interval")
		healthcheck   = flag.Bool("healthcheck", elastic.DefaultHealthcheckEnabled, "enable or disable healthchecks")
		healthchecker = flag.Duration("healthchecker", elastic.DefaultHealthcheckInterval, "healthcheck interval")
	)
	flag.Parse()

	urls := strings.Split(*nodes, ",")
	if len(urls) == 0 {
		log.Fatal("no nodes specified")
	}
	if *index == "" {
		log.Fatal("no index specified")
	}
	if *typ == "" {
		log.Fatal("no type specified")
	}

	var opts []elastic.ClientOptionFunc
	opts = append(opts, elastic.SetURL(urls...))
	if *errorlogfile != "" {
		f, err := os.Open(*errorlogfile)
		if err != nil {
			log.Fatalf("cannot open error log: %v", err)
		}
		defer f.Close()
		logger := log.New(f, "", 0)
		opts = append(opts, elastic.SetErrorLog(logger))
	}
	if *infologfile != "" {
		f, err := os.Open(*infologfile)
		if err != nil {
			log.Fatalf("cannot open info log: %v", err)
		}
		defer f.Close()
		logger := log.New(f, "", 0)
		opts = append(opts, elastic.SetInfoLog(logger))
	}
	if *tracelogfile != "" {
		f, err := os.Open(*tracelogfile)
		if err != nil {
			log.Fatalf("cannot open trace log: %v", err)
		}
		defer f.Close()
		logger := log.New(f, "", 0)
		opts = append(opts, elastic.SetTraceLog(logger))
	}
	opts = append(opts, elastic.SetSniff(*sniff))
	opts = append(opts, elastic.SetSnifferInterval(*sniffer))
	opts = append(opts, elastic.SetHealthcheck(*healthcheck))
	opts = append(opts, elastic.SetHealthcheckInterval(*healthchecker))

	// Specify retrier
	backoff := elastic.NewExponentialBackoff(100*time.Millisecond, 8*time.Second)
	retrier := elastic.NewBackoffRetrier(backoff)
	opts = append(opts, elastic.SetRetrier(retrier))

	client, err := elastic.NewClient(opts...)
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.NodesInfo().Do(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	// Set up bulk processor
	bp, err := client.BulkProcessor().
		Name("bulkprocessor-test").
		Workers(*workers).
		BulkSize(100).
		Stats(true).
		Do(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errc := make(chan error, 1)

	// Start workers
	for i := 0; i < *workers-1; i++ {
		w := newWorker(bp, i+1, *workers, *index, *typ, *interval)
		go func() {
			errc <- w.Run(ctx)
		}()
	}

	m := newMonitor(bp, *workers)
	go func() {
		errc <- m.Run(ctx)
	}()

	// Wait for Ctrl+C or other signals
	go func() {
		c := make(chan os.Signal, 1)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		<-c
		errc <- nil
	}()

	if err := <-errc; err != nil {
		log.Fatal(err)
	}
}
