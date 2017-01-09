package elastic

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	"gopkg.in/olivere/elastic.v5/backoff"
)

func TestRetrier(t *testing.T) {
	r := NewBackoffRetrier(backoff.NewSimpleBackoff(1000, 1000).SendStop(true))

	// 1st call
	d, ok := r.Retry(context.TODO(), nil)
	if got, want := ok, true; want != got {
		t.Fatalf("expected %v; got %v", want, got)
	}
	if got, want := d, 1*time.Second; got != want {
		t.Errorf("expected %v; got: %v", want, got)
	}

	// 2nd call
	d, ok = r.Retry(context.TODO(), nil)
	if got, want := ok, true; want != got {
		t.Fatalf("expected %v; got %v", want, got)
	}
	if got, want := d, 1*time.Second; got != want {
		t.Errorf("expected %v; got: %v", want, got)
	}

	// 3rd call
	_, ok = r.Retry(context.TODO(), nil)
	if got, want := ok, false; want != got {
		t.Fatalf("expected %v; got %v", want, got)
	}
}
