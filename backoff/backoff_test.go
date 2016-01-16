package backoff

import (
	"math/rand"
	"testing"
	"time"
)

func TestSimpleBackoff(t *testing.T) {
	b := NewSimpleBackoff(false, false, 1, 2, 7)

	if got, want := b.Next(), time.Duration(1)*time.Millisecond; got != want {
		t.Errorf("expected %v; got: %v", want, got)
	}
	if got, want := b.Next(), time.Duration(2)*time.Millisecond; got != want {
		t.Errorf("expected %v; got: %v", want, got)
	}
	if got, want := b.Next(), time.Duration(7)*time.Millisecond; got != want {
		t.Errorf("expected %v; got: %v", want, got)
	}
	if got, want := b.Next(), time.Duration(7)*time.Millisecond; got != want {
		t.Errorf("expected %v; got: %v", want, got)
	}

	b.Reset()

	if got, want := b.Next(), time.Duration(1)*time.Millisecond; got != want {
		t.Errorf("expected %v; got: %v", want, got)
	}
	if got, want := b.Next(), time.Duration(2)*time.Millisecond; got != want {
		t.Errorf("expected %v; got: %v", want, got)
	}
	if got, want := b.Next(), time.Duration(7)*time.Millisecond; got != want {
		t.Errorf("expected %v; got: %v", want, got)
	}
	if got, want := b.Next(), time.Duration(7)*time.Millisecond; got != want {
		t.Errorf("expected %v; got: %v", want, got)
	}
}

func TestSimpleBackoffWithStop(t *testing.T) {
	b := NewSimpleBackoff(false, true, 1, 2, 7)

	// It should eventually return Stop (-1) after some loops.
	var last time.Duration
	for i := 0; i < 10; i++ {
		last = b.Next()
		if last == Stop {
			break
		}
	}
	if got, want := last, Stop; got != want {
		t.Errorf("expected %v; got: %v", want, got)
	}

	b.Reset()

	// It should eventually return Stop (-1) after some loops.
	for i := 0; i < 10; i++ {
		last = b.Next()
		if last == Stop {
			break
		}
	}
	if got, want := last, Stop; got != want {
		t.Errorf("expected %v; got: %v", want, got)
	}
}

func TestThainBackoff(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	min := time.Duration(8) * time.Millisecond
	max := time.Duration(256) * time.Millisecond
	b := NewThainBackoff(min, max, false)

	between := func(value time.Duration, a, b int) bool {
		x := int(value / time.Millisecond)
		return a <= x && x <= b
	}

	if got := b.Next(); !between(got, 8, 256) {
		t.Errorf("expected [%v..%v]; got: %v", 8, 256, got)
	}
	if got := b.Next(); !between(got, 8, 256) {
		t.Errorf("expected [%v..%v]; got: %v", 8, 256, got)
	}
	if got := b.Next(); !between(got, 8, 256) {
		t.Errorf("expected [%v..%v]; got: %v", 8, 256, got)
	}
	if got := b.Next(); !between(got, 8, 256) {
		t.Errorf("expected [%v..%v]; got: %v", 8, 256, got)
	}

	b.Reset()

	if got := b.Next(); !between(got, 8, 256) {
		t.Errorf("expected [%v..%v]; got: %v", 8, 256, got)
	}
	if got := b.Next(); !between(got, 8, 256) {
		t.Errorf("expected [%v..%v]; got: %v", 8, 256, got)
	}
	if got := b.Next(); !between(got, 8, 256) {
		t.Errorf("expected [%v..%v]; got: %v", 8, 256, got)
	}
	if got := b.Next(); !between(got, 8, 256) {
		t.Errorf("expected [%v..%v]; got: %v", 8, 256, got)
	}
}

func TestThainBackoffWithStop(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	min := time.Duration(8) * time.Millisecond
	max := time.Duration(256) * time.Millisecond
	b := NewThainBackoff(min, max, true)

	// It should eventually return Stop (-1) after some loops.
	var last time.Duration
	for i := 0; i < 10; i++ {
		last = b.Next()
		if last == Stop {
			break
		}
	}
	if got, want := last, Stop; got != want {
		t.Errorf("expected %v; got: %v", want, got)
	}

	b.Reset()

	// It should eventually return Stop (-1) after some loops.
	for i := 0; i < 10; i++ {
		last = b.Next()
		if last == Stop {
			break
		}
	}
	if got, want := last, Stop; got != want {
		t.Errorf("expected %v; got: %v", want, got)
	}
}
