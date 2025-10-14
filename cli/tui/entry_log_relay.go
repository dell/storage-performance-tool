package tui

import (
	"context"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// LogFetcher abstracts a source of entry-node logs for the relay.
// Exactly one of Stream or Poll will be used by a relay instance.
type LogFetcher interface {
	// Stream should block and invoke onLine for each new line until ctx is cancelled or an error occurs.
	// It should return when ctx.Done() is closed or on fatal error.
	Stream(ctx context.Context, onLine func(string)) error

	// Poll should return lines newer than the provided 'since' watermark and a new watermark.
	// Implementations may ignore 'since' if they track internally. Errors should be transient.
	Poll(ctx context.Context, since time.Time) (lines []string, nextSince time.Time, err error)
}

// EntryLogRelay tails entry-node console output and forwards lines into the TUI.
type EntryLogRelay struct {
	fetcher      LogFetcher
	streaming    bool
	pollInterval time.Duration

	// lifecycle
	mu     sync.Mutex
	cancel context.CancelFunc
	done   chan struct{}
}

// NewEntryLogRelay creates a relay with either streaming (true) or polling (false) behavior.
func NewEntryLogRelay(fetcher LogFetcher, streaming bool, pollInterval time.Duration) *EntryLogRelay {
	if pollInterval <= 0 {
		// Reuse existing API polling cadence as a sensible default
		pollInterval = constants.APIPollingTimeout
	}
	return &EntryLogRelay{fetcher: fetcher, streaming: streaming, pollInterval: pollInterval, done: make(chan struct{})}
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
	streaming := r.streaming
	pollEvery := r.pollInterval
	fetcher := r.fetcher
	r.mu.Unlock()

	go func() {
		defer close(done)
		if streaming {
			// Streaming path: single blocking call that emits lines until ctx is cancelled
			if err := fetcher.Stream(ctx, func(s string) {
				if s != "" {
					onLine("[SPT] " + s)
				}
			}); err != nil {
				logging.LogWarn("entry-log-relay", "stream ended with error", "error", err.Error())
			}
			return
		}

		// Polling path: periodically call Poll and forward new lines
		ticker := time.NewTicker(pollEvery)
		defer ticker.Stop()
		var since time.Time
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				lines, nextSince, err := fetcher.Poll(ctx, since)
				if err != nil {
					logging.LogWarn("entry-log-relay", "poll error", "error", err.Error())
					continue
				}
				for _, s := range lines {
					if s != "" {
						onLine("[SPT] " + s)
					}
				}
				if !nextSince.IsZero() {
					since = nextSince
				} else if len(lines) > 0 {
					// crude fallback watermark
					since = time.Now()
				}
			}
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
