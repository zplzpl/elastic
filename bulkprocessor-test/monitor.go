package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"time"

	elastic "gopkg.in/olivere/elastic.v5"
)

type Monitor struct {
	bp         *elastic.BulkProcessor
	numWorkers int
}

func newMonitor(bp *elastic.BulkProcessor, numWorkers int) *Monitor {
	return &Monitor{
		bp:         bp,
		numWorkers: numWorkers,
	}
}

func (m *Monitor) Run(ctx context.Context) error {
	first := true
	for {
		select {
		case <-time.After(1 * time.Second):
			m.print(first, m.bp.Stats())
			first = false
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (m *Monitor) print(first bool, stats elastic.BulkProcessorStats) {
	var buf bytes.Buffer

	if !first {
		buf.WriteString("\033[4A") // up 4 lines
	}
	buf.WriteString(fmt.Sprintf("\rTime       %s\n", time.Now().Format("15:04:05")))
	buf.WriteString(fmt.Sprintf("\rIndexed    %8d\n", stats.Indexed))
	buf.WriteString(fmt.Sprintf("\rSucceeded  %8d\n", stats.Succeeded))
	buf.WriteString(fmt.Sprintf("\rFailed     %8d\n", stats.Failed))

	fmt.Fprint(os.Stdout, buf.String())
}
