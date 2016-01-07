// Copyright 2012-2016 Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"sync"
	"sync/atomic"
	"time"
)

// BulkProcessorService allows to easily process bulk requests. It allows setting
// policies when to flush new bulk requests, e.g. based on a number of actions,
// on the size of the actions, and/or to flush periodically. It also allows
// to control the number of concurrent bulk requests allowed to be executed
// in parallel.
//
// BulkProcessorService, by default, commits either every 1000 requests or when the
// (estimated) size of the bulk requests exceeds 5 MB. However, it does not
// commit periodically.
//
// The caller is responsible for setting the index and type on every
// bulk request added to BulkProcessorService.
//
// BulkProcessorService takes ideas from the BulkProcessor of the
// Elasticsearch Java API as documented in
// https://www.elastic.co/guide/en/elasticsearch/client/java-api/current/java-docs-bulk-processor.html.
type BulkProcessorService struct {
	c             *Client
	beforeFn      BulkBeforeFunc
	afterFn       BulkAfterFunc
	failureFn     BulkFailureFunc
	name          string        // name of processor
	numWorkers    int           // # of workers (>= 1)
	bulkActions   int           // # of requests after which to commit
	bulkByteSize  int           // # of bytes after which to commit
	flushInterval time.Duration // periodic flush interval
	wantStats     bool          // indicates whether to gather statistics

	workerWg     sync.WaitGroup       // used to stop workers
	workers      []*bulkWorker        // list of workers
	requestsC    chan BulkableRequest // for adding requests
	flusherStopC chan struct{}        // for stopping the flusher
	executionId  int64                // unique id for bulk commits
	running      bool                 // indicates whether the processor is running

	statsMu sync.Mutex          // guard the following variables
	stats   *BulkProcessorStats // current stats
}

// BulkProcessorStats contains various statistics of a bulk processor
// while it is running. Use the Stats func to return it while running.
type BulkProcessorStats struct {
	Flushed   int64 // number of times the flush interval has been invoked
	Committed int64 // # of times workers committed bulk requests
	Indexed   int64 // # of requests indexed
	Created   int64 // # of requests that ES reported as creates (201)
	Updated   int64 // # of requests that ES reported as updates
	Deleted   int64 // # of requests that ES reported as deletes
	Succeeded int64 // # of requests that ES reported as successful
	Failed    int64 // # of requests that ES reported as failed
}

// NewBulkProcessorService creates a new BulkProcessorService.
func NewBulkProcessorService(client *Client) *BulkProcessorService {
	return &BulkProcessorService{
		c:            client,
		numWorkers:   1,
		bulkActions:  1000,
		bulkByteSize: 5 << 20, // 5 MB
	}
}

// BulkBeforeFunc defines the signature of callbacks that are executed
// before a commit to Elasticsearch.
type BulkBeforeFunc func(executionId int64, requests []BulkableRequest)

// BulkBeforeFunc defines the signature of callbacks that are executed
// after a commit to Elasticsearch, regardless of being successful or not.
type BulkAfterFunc func(executionId int64, response *BulkResponse)

// BulkFailureFunc defines the signature of callbacks that are executed
// when Elasticsearch reports an error.
type BulkFailureFunc func(executionId int64, response *BulkResponse, err error)

// Before specifies a function to be executed before bulk requests get comitted
// to Elasticsearch.
func (p *BulkProcessorService) Before(fn BulkBeforeFunc) *BulkProcessorService {
	p.beforeFn = fn
	return p
}

// After specifies a function to be executed when bulk requests have been
// comitted to Elasticsearch. The After callback always executes, even when
// Elasticsearch reported an error (and therefor executes Failure callback).
func (p *BulkProcessorService) After(fn BulkAfterFunc) *BulkProcessorService {
	p.afterFn = fn
	return p
}

// Failure specifies a function to be executed when bulk requests failed
// to be comitted successfully. The Failure callback is executed before
// the After callback.
func (p *BulkProcessorService) Failure(fn BulkFailureFunc) *BulkProcessorService {
	p.failureFn = fn
	return p
}

// Name is an optional name to identify this bulk processor.
func (p *BulkProcessorService) Name(name string) *BulkProcessorService {
	p.name = name
	return p
}

// Workers is the number of concurrent workers allowed to be
// executed. Defaults to 1 and must be greater or equal to 1.
func (p *BulkProcessorService) Workers(num int) *BulkProcessorService {
	p.numWorkers = num
	return p
}

// BulkActions specifies when to flush based on the number of actions
// currently added. Defaults to 1000 and can be set to -1 to be disabled.
func (p *BulkProcessorService) BulkActions(bulkActions int) *BulkProcessorService {
	p.bulkActions = bulkActions
	return p
}

// BulkByteSize specifies when to flush based on the size of the actions
// currently added. Defaults to 5 MB and can be set to -1 to be disabled.
func (p *BulkProcessorService) BulkByteSize(bulkByteSize int) *BulkProcessorService {
	p.bulkByteSize = bulkByteSize
	return p
}

// FlushInterval specifies when to flush at the end of the given interval.
// This is disabled by default. If you want the bulk processor to
// operate completely asynchronously, set both BulkActions and BulkSize to
// -1 and set the FlushInterval to a meaningful interval.
func (p *BulkProcessorService) FlushInterval(interval time.Duration) *BulkProcessorService {
	p.flushInterval = interval
	return p
}

// CollectStats tells bulk processor to gather stats while running.
// Use Stats to return the stats. This is disabled by default.
func (p *BulkProcessorService) CollectStats(enable bool) *BulkProcessorService {
	p.wantStats = enable
	return p
}

// Stats returns the latest bulk processor statistics.
// Collecting stats must be enabled first by calling CollectStats(true).
func (p *BulkProcessorService) Stats() BulkProcessorStats {
	p.statsMu.Lock()
	defer p.statsMu.Unlock()
	return *p.stats
}

// Do starts the bulk processor. Use Close to stop it.
// If the processor is already running, this is a no-op and nil is returned.
func (p *BulkProcessorService) Do() error {
	if p.running {
		return nil
	}

	// We must have at least one worker.
	if p.numWorkers < 1 {
		p.numWorkers = 1
	}

	p.requestsC = make(chan BulkableRequest)
	p.executionId = 0
	p.stats = &BulkProcessorStats{}

	// Create and start up workers.
	p.workers = make([]*bulkWorker, p.numWorkers)
	for i := 0; i < p.numWorkers; i++ {
		p.workerWg.Add(1)
		p.workers[i] = newBulkWorker(p)
		go p.workers[i].work()
	}

	// Start the ticker for flush (if enabled)
	if int64(p.flushInterval) > 0 {
		p.flusherStopC = make(chan struct{})
		go p.flusher(p.flushInterval)
	}

	p.running = true

	return nil
}

// Close stops the bulk processor previously started with Do.
// If it is already stopped, this is a no-op and nil is returned.
func (p *BulkProcessorService) Close() error {
	// Already stopped? Do nothing.
	if !p.running {
		return nil
	}

	// Stop flusher (if enabled)
	if p.flusherStopC != nil {
		p.flusherStopC <- struct{}{}
		<-p.flusherStopC
		close(p.flusherStopC)
		p.flusherStopC = nil
	}

	// Close all channels.
	close(p.requestsC)
	p.workerWg.Wait()

	p.running = false

	return nil
}

// Add adds a single request to commit by the BulkProcessorService.
//
// The caller is responsible for setting the index and type on the request.
func (p *BulkProcessorService) Add(request BulkableRequest) {
	p.requestsC <- request
}

// Flush manually asks all workers to commit their outstanding requests.
// It returns only when all workers acknowledge completion.
func (p *BulkProcessorService) Flush() error {
	atomic.AddInt64(&p.stats.Flushed, 1)
	for _, w := range p.workers {
		w.flushC <- struct{}{}
		<-w.flushAckC // wait for completion
	}
	return nil
}

// flusher is a single goroutine that periodically asks all workers to
// commit their outstanding bulk requests. It is only started if
// FlushInterval is greater than 0.
func (p *BulkProcessorService) flusher(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C: // Periodic flush
			p.Flush() // TODO swallow errors here?

		case <-p.flusherStopC:
			p.flusherStopC <- struct{}{}
			return
		}
	}
}

// bulkWorker is a single worker receiving bulk requests and eventually
// committing them to Elasticsearch. It is strongly bound to a BulkProcessorService.
type bulkWorker struct {
	p            *BulkProcessorService
	bulkActions  int
	bulkByteSize int
	service      *BulkService
	flushC       chan struct{}
	flushAckC    chan struct{}
}

// newBulkWorker creates a new bulkWorker instance.
func newBulkWorker(p *BulkProcessorService) *bulkWorker {
	return &bulkWorker{
		p:            p,
		bulkActions:  p.bulkActions,
		bulkByteSize: p.bulkByteSize,
		service:      NewBulkService(p.c),
		flushC:       make(chan struct{}),
		flushAckC:    make(chan struct{}),
	}
}

// work waits for bulk requests and manual flush calls on the respective
// channels and is invoked as a goroutine when the bulk processor is started.
func (w *bulkWorker) work() {
	defer func() {
		w.p.workerWg.Done()
		close(w.flushAckC)
		close(w.flushC)
	}()

	var stop bool
	for !stop {
		select {
		case req, open := <-w.p.requestsC:
			if open {
				// Received a new request
				w.service.Add(req)
				if w.commitRequired() {
					w.commit() // TODO swallow errors here?
				}
			} else {
				// Channel closed: Stop.
				stop = true
				if w.service.NumberOfActions() > 0 {
					w.commit() // TODO swallow errors here?
				}
			}

		case <-w.flushC:
			// Commit outstanding requests
			if w.service.NumberOfActions() > 0 {
				w.commit() // TODO swallow errors here?
			}
			w.flushAckC <- struct{}{}
		}
	}
}

// commit commits the bulk requests in the given service,
// invoking callbacks as specified.
func (w *bulkWorker) commit() error {
	id := atomic.AddInt64(&w.p.executionId, 1)

	// Invoke before callback
	if w.p.beforeFn != nil {
		w.p.beforeFn(id, w.service.requests)
	}

	// Commit bulk requests
	res, err := w.service.Do()
	if err != nil {
		w.p.c.errorf("elastic: bulk processor %q failed: %v", w.p.name, err)

		// Invoke failure callback
		if w.p.failureFn != nil {
			w.p.failureFn(id, res, err)
		}
		return err
	}

	// Update stats
	w.p.statsMu.Lock()
	if w.p.wantStats {
		w.p.stats.Committed += 1
		w.p.stats.Indexed += int64(len(res.Indexed()))
		w.p.stats.Created += int64(len(res.Created()))
		w.p.stats.Updated += int64(len(res.Updated()))
		w.p.stats.Deleted += int64(len(res.Deleted()))
		w.p.stats.Succeeded += int64(len(res.Succeeded()))
		w.p.stats.Failed += int64(len(res.Failed()))
	}
	w.p.statsMu.Unlock()

	// Invoke after callback
	if w.p.afterFn != nil {
		w.p.afterFn(id, res)
	}

	return nil
}

// commitRequired returns true if the service has to commit its
// bulk requests. This can be either because the number of actions
// or the estimated size in bytes is larger than specified in the
// BulkProcessorService.
func (w *bulkWorker) commitRequired() bool {
	if w.bulkActions >= 0 && w.service.NumberOfActions() >= w.bulkActions {
		return true
	}
	if w.bulkByteSize >= 0 && w.service.EstimatedSizeInBytes() >= int64(w.bulkByteSize) {
		return true
	}
	return false
}
