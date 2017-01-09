// Copyright 2012-present Oliver Eilhard. All rights reserved.
// Use of this source code is governed by a MIT-license.
// See http://olivere.mit-license.org/license.txt for details.

package elastic

import (
	"time"

	"golang.org/x/net/context"

	"gopkg.in/olivere/elastic.v5/backoff"
)

// RetrierFactory is a function that creates Retriers.
type RetrierFactory func() Retrier

// Retrier is a generic interface that decides whether HTTP requests to
// Elasticsearch need to be retried and how long to wait until the next
// request.
type Retrier interface {
	// Retry is called when a HTTP error is returned as the result of a
	// request to Elasticsearch. It returns if and how long to wait for
	// the next HTTP request to Elasticsearch. The error from the HTTP request
	// is passed into Retry.
	Retry(context.Context, error) (time.Duration, bool)
}

// -- Backoff-based Retrier --

// BackoffRetrier is a Retrier that uses a backoff algorithm, such as
// exponential backoff, to check for retries.
type BackoffRetrier struct {
	backoff backoff.Backoff
}

// NewBackoffRetrier creates a new backoff-based retrier.
func NewBackoffRetrier(backoff backoff.Backoff) *BackoffRetrier {
	return &BackoffRetrier{backoff}
}

// Retry returns the duration to wait for the next request and a flag that
// indicates whether to stop retrying.
func (r *BackoffRetrier) Retry(_ context.Context, _ error) (time.Duration, bool) {
	duration := r.backoff.Next()
	if duration == backoff.Stop {
		return backoff.Stop, false
	}
	return duration, true
}

// -- Predefined retriers --

// NewDisabledRetrier will create a Retrier that performs no retries.
func NewDisabledRetrier() *BackoffRetrier {
	backoff := backoff.NewStopBackoff()
	return NewBackoffRetrier(backoff)
}

// NewExponentialBackoffRetrier will create a Retrier that uses the list of
// given fixed intervals (in milliseconds).
func NewSimpleBackoffRetrier(ticks ...int) *BackoffRetrier {
	backoff := backoff.NewSimpleBackoff(ticks...).SendStop(true).Jitter(true)
	return NewBackoffRetrier(backoff)
}

// NewExponentialBackoffRetrier will create a Retrier that uses exponential
// backoff for the intervals.
func NewExponentialBackoffRetrier(initialTimeout, maxTimeout time.Duration) *BackoffRetrier {
	backoff := backoff.NewExponentialBackoff(initialTimeout, maxTimeout).SendStop(true)
	return NewBackoffRetrier(backoff)
}
