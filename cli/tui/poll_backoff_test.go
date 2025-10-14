package tui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"testing"
	"time"
)

func TestNodePollStateRetryableBackoff(t *testing.T) {
	state := newNodePollState()
	cfg := backoffConfig{
		Base:      time.Second,
		Initial:   time.Second,
		Max:       4 * time.Second,
		Jitter:    0,
		WarnAfter: 10 * time.Second,
	}
	jitter := func(time.Duration) time.Duration { return 0 }

	now := time.Unix(0, 0)
	delay, warn := state.recordFailure(now, errors.New("boom"), pollErrorRetryable, cfg, jitter)
	if warn {
		t.Fatal("expected warn to be false on first failure")
	}
	if delay != cfg.Initial {
		t.Fatalf("expected delay %v, got %v", cfg.Initial, delay)
	}
	if state.shouldPoll(now.Add(500 * time.Millisecond)) {
		t.Fatalf("expected shouldPoll false before backoff expires")
	}
	if !state.shouldPoll(now.Add(delay + time.Millisecond)) {
		t.Fatalf("expected shouldPoll true after backoff duration")
	}

	now2 := now.Add(delay + time.Second)
	delay2, _ := state.recordFailure(now2, errors.New("boom2"), pollErrorRetryable, cfg, jitter)
	if delay2 != 2*time.Second {
		t.Fatalf("expected delay 2s, got %v", delay2)
	}

	now3 := now2.Add(delay2 + time.Second)
	delay3, _ := state.recordFailure(now3, errors.New("boom3"), pollErrorRetryable, cfg, jitter)
	if delay3 != 4*time.Second {
		t.Fatalf("expected delay capped at 4s, got %v", delay3)
	}

	state.recordSuccess(now3.Add(delay3 + time.Second))
	if !state.shouldPoll(now3.Add(delay3 + time.Second)) {
		t.Fatal("expected shouldPoll true after success reset")
	}
}

func TestNodePollStateWarnAfterThreshold(t *testing.T) {
	state := newNodePollState()
	cfg := backoffConfig{
		Base:      time.Second,
		Initial:   time.Second,
		Max:       4 * time.Second,
		Jitter:    0,
		WarnAfter: 2 * time.Second,
	}
	jitter := func(time.Duration) time.Duration { return 0 }

	now := time.Unix(0, 0)
	if _, warn := state.recordFailure(now, errors.New("boom"), pollErrorRetryable, cfg, jitter); warn {
		t.Fatal("did not expect warn on first failure")
	}

	now2 := now.Add(3 * time.Second)
	if _, warn := state.recordFailure(now2, errors.New("boom2"), pollErrorRetryable, cfg, jitter); !warn {
		t.Fatal("expected warn after exceeding threshold")
	}

	now3 := now2.Add(500 * time.Millisecond)
	if _, warn := state.recordFailure(now3, errors.New("boom3"), pollErrorRetryable, cfg, jitter); warn {
		t.Fatal("expected throttle to suppress rapid repeated warnings")
	}

	now4 := now2.Add(1500 * time.Millisecond)
	if _, warn := state.recordFailure(now4, errors.New("boom4"), pollErrorRetryable, cfg, jitter); !warn {
		t.Fatal("expected warning after throttle window elapsed")
	}
}

func TestFatalPollErrorDoesNotBackoff(t *testing.T) {
	state := newNodePollState()
	cfg := backoffConfig{
		Base:      time.Second,
		Initial:   time.Second,
		Max:       4 * time.Second,
		Jitter:    0,
		WarnAfter: 10 * time.Second,
	}

	jitter := func(time.Duration) time.Duration { return 0 }
	state.recordFailure(time.Unix(0, 0), errors.New("fatal"), pollErrorFatal, cfg, jitter)
	if !state.shouldPoll(time.Unix(0, 0).Add(100 * time.Millisecond)) {
		t.Fatal("expected immediate retry after fatal error")
	}
}

func TestClassifyPollError(t *testing.T) {
	parseErr := fmt.Errorf("%w: missing", ErrMetricsIncompatible)
	if kind := classifyPollError(parseErr); kind != pollErrorParse {
		t.Fatalf("expected pollErrorParse, got %v", kind)
	}

	jsonErr := &json.SyntaxError{Offset: 10}
	if kind := classifyPollError(jsonErr); kind != pollErrorParse {
		t.Fatalf("expected pollErrorParse for syntax error, got %v", kind)
	}

	http500 := &HTTPStatusError{StatusCode: 500}
	if kind := classifyPollError(http500); kind != pollErrorRetryable {
		t.Fatalf("expected retryable for 500, got %v", kind)
	}

	http404 := &HTTPStatusError{StatusCode: 404}
	if kind := classifyPollError(http404); kind != pollErrorFatal {
		t.Fatalf("expected fatal for 404, got %v", kind)
	}

	netErr := &url.Error{Err: context.DeadlineExceeded}
	if kind := classifyPollError(netErr); kind != pollErrorRetryable {
		t.Fatalf("expected retryable for url error, got %v", kind)
	}
}
