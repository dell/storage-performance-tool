package tui

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// LogFetcher abstracts a source of entry-node logs for the relay.
type LogFetcher interface {
	// Stream should block and invoke onLine for each new line until ctx is cancelled or an error occurs.
	// It should return when ctx.Done() is closed or on fatal error.
	Stream(ctx context.Context, onLine func(string)) error
}

// EntryLogRelay tails entry-node console output and forwards lines into the TUI.
type EntryLogRelay struct {
	fetcher LogFetcher

	// lifecycle
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewEntryLogRelay creates a relay that streams lines from fetcher.
func NewEntryLogRelay(fetcher LogFetcher) *EntryLogRelay {
	return &EntryLogRelay{fetcher: fetcher, done: make(chan struct{})}
}

// Start begins forwarding lines to onLine. Safe to call once.
func (r *EntryLogRelay) Start(parent context.Context, onLine func(string)) {
	r.mu.Lock()
	if r.cancel != nil {
		r.mu.Unlock()
		return // already started
	}
	ctx, cancel := context.WithCancel(parent)
	r.cancel = cancel
	done := r.done
	fetcher := r.fetcher
	r.mu.Unlock()

	go func() {
		defer close(done)
		// Single blocking call that emits lines until ctx is cancelled
		if err := fetcher.Stream(ctx, func(s string) {
			// Skip lines with no visible text, such as the bare ANSI reset the
			// engine writes as its last output when the container stops.
			if strings.TrimSpace(stripANSIEscapeSequences(s)) != "" {
				onLine("[SPT] " + s)
			}
		}); err != nil {
			logging.LogWarn("entry-log-relay", "stream ended with error", "error", err.Error())
		}
	}()
}

// Stop cancels the relay and waits for it to exit.
func (r *EntryLogRelay) Stop() {
	r.mu.Lock()
	if r.cancel == nil {
		r.mu.Unlock()
		return
	}
	cancel := r.cancel
	done := r.done
	r.cancel = nil
	r.mu.Unlock()

	cancel()
	// Wait up to a short grace period
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		logging.LogWarn("entry-log-relay", "stop timeout waiting for goroutine to exit")
	}
}
