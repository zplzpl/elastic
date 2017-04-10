package main

import (
	"context"
	"fmt"
	"time"

	elastic "gopkg.in/olivere/elastic.v5"
)

type Worker struct {
	bp         *elastic.BulkProcessor
	id         int
	numWorkers int
	index      string
	typ        string
	interval   time.Duration
}

func newWorker(bp *elastic.BulkProcessor, id, numWorkers int, index, typ string, interval time.Duration) *Worker {
	return &Worker{
		bp:         bp,
		id:         id,
		numWorkers: numWorkers,
		index:      index,
		typ:        typ,
		interval:   interval,
	}
}

func (w *Worker) Run(ctx context.Context) error {
	for {
		select {
		case <-time.After(w.interval):
			doc := MakeRecord()
			req := elastic.NewBulkIndexRequest().Index(w.index).Type(w.typ).Id(fmt.Sprint(doc.ID)).Doc(doc)
			w.bp.Add(req)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
