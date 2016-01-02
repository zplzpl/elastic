// Copyright 2012-2016 Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"sync"
	"sync/atomic"
	"time"
)

// BulkProcessor allows to easily process bulk requests. It allows setting
// policies when to flush new bulk requests, e.g. based on a number of actions,
// on the size of the actions, and/or to flush periodically. It also allows
// to control the number of concurrent bulk requests allowed to be executed
// in parallel.
//
// BulkProcessor, by default, commits either every 1000 requests or when the
// (estimated) size of the bulk requests exceeds 5 MB. However, it does not
// commit periodically.
type BulkProcessor struct {
	c             *Client
	beforeFn      BulkBeforeFunc
	afterFn       BulkAfterFunc
	failureFn     BulkFailureFunc
	name          string
	numWorkers    int
	bulkActions   int
	bulkByteSize  int
	flushInterval time.Duration

	wg sync.WaitGroup

	mu          sync.Mutex // guard the following block
	closed      bool
	closeCh     chan bool
	requestCh   chan BulkableRequest
	ticker      *time.Ticker
	executionId int64
	flushes     int64 // number of times the flush interval has been invoked
}

// NewBulkProcessor creates a new BulkProcessor.
func NewBulkProcessor(client *Client) *BulkProcessor {
	return &BulkProcessor{
		c:            client,
		numWorkers:   1,
		bulkActions:  1000,
		bulkByteSize: 5 << 20, // 5 MB
		closed:       true,
	}
}

type BulkBeforeFunc func(executionId int64, requests []BulkableRequest)
type BulkAfterFunc func(executionId int64, response *BulkResponse)
type BulkFailureFunc func(executionId int64, response *BulkResponse, err error)

// Before specifies a function to be executed before bulk requests get executed.
func (p *BulkProcessor) Before(fn BulkBeforeFunc) *BulkProcessor {
	p.beforeFn = fn
	return p
}

// After specifies a function to be executed when bulk requests have been
// successfully executed.
func (p *BulkProcessor) After(fn BulkAfterFunc) *BulkProcessor {
	p.afterFn = fn
	return p
}

// Failure specifies a function to be executed when a bulk request failed
// to be executed successfully.
func (p *BulkProcessor) Failure(fn BulkFailureFunc) *BulkProcessor {
	p.failureFn = fn
	return p
}

// Name is an optional name to identify this bulk processor.
func (p *BulkProcessor) Name(name string) *BulkProcessor {
	p.name = name
	return p
}

// Workers is the number of concurrent workers allowed to be
// executed. Defaults to 1.
func (p *BulkProcessor) Workers(num int) *BulkProcessor {
	p.numWorkers = num
	return p
}

// BulkActions specifies when to flush based on the number of actions
// currently added. Defaults to 1000 and can be set to -1 to be disabled.
func (p *BulkProcessor) BulkActions(bulkActions int) *BulkProcessor {
	p.bulkActions = bulkActions
	return p
}

// BulkByteSize specifies when to flush based on the size of the actions
// currently added. Defaults to 5 MB and can be set to -1 to be disabled.
func (p *BulkProcessor) BulkByteSize(bulkByteSize int) *BulkProcessor {
	p.bulkByteSize = bulkByteSize
	return p
}

// FlushInterval specifies when to flush at the end of the given interval.
// This is disabled by default. If you want the bulk processor to
// operate completely asynchronously, set both BulkActions and BulkSize to
// -1 and set the FlushInterval to a meaningful interval.
func (p *BulkProcessor) FlushInterval(interval time.Duration) *BulkProcessor {
	p.flushInterval = interval
	return p
}

// Do starts the bulk processor. Use Close to stop it.
func (p *BulkProcessor) Do() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// We must have at least one worker.
	if p.numWorkers < 1 {
		p.numWorkers = 1
	}

	p.closeCh = make(chan bool, p.numWorkers)
	p.requestCh = make(chan BulkableRequest)
	p.executionId = 0

	// Start the ticker for flush
	if int64(p.flushInterval) > 0 {
		p.ticker = time.NewTicker(p.flushInterval)
	} else {
		// TODO Is this the correct way to initialize a ticker that never ticks?
		p.ticker = time.NewTicker(1 * time.Second)
		p.ticker.Stop()
	}

	// Start up workers.
	for i := 0; i < p.numWorkers; i++ {
		p.wg.Add(1)
		go p.worker(i, NewBulkService(p.c))
	}

	p.closed = false

	return nil
}

// Close stops the bulk processor. If it is already stopped, this is a no-op.
func (p *BulkProcessor) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Already closed: Do nothing.
	if p.closed {
		return nil
	}

	// Stop all workers.
	for i := 0; i < p.numWorkers; i++ {
		p.closeCh <- true
	}
	p.wg.Wait()

	// Stop the periodic flush.
	p.ticker.Stop()

	// Close all channels.
	close(p.closeCh)
	close(p.requestCh)

	p.closed = true

	return nil
}

// Add adds a single request to commit by the BulkProcessor.
// This operation is asynchronous.
func (p *BulkProcessor) Add(request BulkableRequest) {
	p.requestCh <- request
}

// executeRequired returns true if the service has to commit its
// bulk requests. This can be either because the number of actions
// or the estimated size in bytes is larger than specified in the
// BulkProcessor.
func (p *BulkProcessor) executeRequired(service *BulkService) bool {
	if p.bulkActions >= 0 && service.NumberOfActions() >= p.bulkActions {
		return true
	}
	if p.bulkByteSize >= 0 && service.EstimatedSizeInBytes() >= int64(p.bulkByteSize) {
		return true
	}
	return false
}

// worker is a single goroutine handling and committing bulk requests.
func (p *BulkProcessor) worker(i int, service *BulkService) {
	defer p.wg.Done()

	for {
		select {
		case req := <-p.requestCh:
			// Received a new request
			service.Add(req)
			if p.executeRequired(service) {
				id := atomic.AddInt64(&p.executionId, 1)
				err := p.execute(id, service)
				if err != nil {
					// TODO swallow errors here?
					p.c.errorf("elastic: BulkProcessor %q failed: %v", p.name, err)
				}
			}

		case <-p.ticker.C:
			// Periodic flush
			if service.NumberOfActions() > 0 {
				atomic.AddInt64(&p.flushes, 1)
				id := atomic.AddInt64(&p.executionId, 1)
				err := p.execute(id, service)
				if err != nil {
					// TODO swallow errors here?
					p.c.errorf("elastic: BulkProcessor %q failed: %v", p.name, err)
				}
			}

		case <-p.closeCh:
			// Commit last batch before workers stops
			if service.NumberOfActions() > 0 {
				id := atomic.AddInt64(&p.executionId, 1)
				err := p.execute(id, service)
				if err != nil {
					// TODO swallow errors here?
					p.c.errorf("elastic: BulkProcessor %q failed: %v", p.name, err)
				}
			}
			return
		}
	}
}

// execute commits the bulk requests in the given service,
// invoking callbacks as specified.
func (p *BulkProcessor) execute(id int64, service *BulkService) error {
	// Invoke before callback
	if p.beforeFn != nil {
		p.beforeFn(id, service.requests)
	}

	// Commit bulk requests
	res, err := service.Do()
	if err != nil {
		// Invoke failure callback
		if p.failureFn != nil {
			p.failureFn(id, res, err)
		}
		return err
	}

	// Invoke after callback
	if p.afterFn != nil {
		p.afterFn(id, res)
	}

	return nil
}
