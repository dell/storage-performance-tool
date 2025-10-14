package tui

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeFetcher supports both streaming and polling behaviors for tests.
type fakeFetcher struct {
	// streaming mode
	streamLines []string
	streamDelay time.Duration

	// polling mode
	polls [][]string
	mu    sync.Mutex
}

func (f *fakeFetcher) Stream(ctx context.Context, onLine func(string)) error {
	for _, s := range f.streamLines {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if f.streamDelay > 0 {
			time.Sleep(f.streamDelay)
		}
		onLine(s)
	}
	// block until cancel to emulate follow
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeFetcher) Poll(ctx context.Context, _ time.Time) ([]string, time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.polls) == 0 {
		return nil, time.Time{}, nil
	}
	batch := f.polls[0]
	f.polls = f.polls[1:]
	return batch, time.Now(), nil
}

func TestEntryLogRelay_Stream_ForwardsAndStops(t *testing.T) {
	ff := &fakeFetcher{streamLines: []string{"line1", "line2", "line3"}, streamDelay: 5 * time.Millisecond}
	relay := NewEntryLogRelay(ff, true, 0)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got []string
	var mu sync.Mutex
	relay.Start(ctx, func(s string) {
		mu.Lock()
		got = append(got, s)
		mu.Unlock()
	})

	// allow lines to flow
	time.Sleep(50 * time.Millisecond)
	relay.Stop()

	mu.Lock()
	defer mu.Unlock()
	if len(got) < 3 {
		t.Fatalf("expected at least 3 forwarded lines, got %d: %#v", len(got), got)
	}
	if got[0] != "[SPT] line1" || got[1] != "[SPT] line2" || got[2] != "[SPT] line3" {
		t.Fatalf("unexpected prefixing: %#v", got[:3])
	}
}

func TestEntryLogRelay_Poll_PaginatesAndStops(t *testing.T) {
	ff := &fakeFetcher{polls: [][]string{{"a1", "a2"}, {"b1"}, {}}}
	relay := NewEntryLogRelay(ff, false, 10*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var got []string
	var mu sync.Mutex
	relay.Start(ctx, func(s string) {
		mu.Lock()
		got = append(got, s)
		mu.Unlock()
	})

	// Wait for a few polling ticks
	time.Sleep(50 * time.Millisecond)
	relay.Stop()

	mu.Lock()
	defer mu.Unlock()
	// Expect a1,a2,b1 in order with prefix
	want := []string{"[SPT] a1", "[SPT] a2", "[SPT] b1"}
	if len(got) < len(want) {
		t.Fatalf("expected at least %d lines, got %d: %#v", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("at %d: want %q got %q (all: %#v)", i, want[i], got[i], got)
		}
	}
}
