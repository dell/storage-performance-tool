package tui

import (
	"context"
	"sync"
	"testing"
	"time"
)

// fakeFetcher emits scripted lines, then blocks until cancelled to emulate follow.
type fakeFetcher struct {
	streamLines []string
	streamDelay time.Duration
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

func TestEntryLogRelay_Stream_ForwardsAndStops(t *testing.T) {
	ff := &fakeFetcher{streamLines: []string{"line1", "line2", "line3"}, streamDelay: 5 * time.Millisecond}
	relay := NewEntryLogRelay(ff)

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
