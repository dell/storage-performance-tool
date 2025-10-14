/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/url"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

type pollErrorKind int

const (
	pollErrorNone pollErrorKind = iota
	pollErrorRetryable
	pollErrorParse
	pollErrorFatal
)

const metricsPayloadPreviewLen = 200

type backoffConfig struct {
	Base      time.Duration
	Initial   time.Duration
	Max       time.Duration
	Jitter    time.Duration
	WarnAfter time.Duration
}

type nodePollState struct {
	mu           sync.Mutex
	backoff      time.Duration
	nextAllowed  time.Time
	lastSuccess  time.Time
	firstFailure time.Time
	lastWarn     time.Time
	lastError    error
	consecutive  int
}

func newNodePollState() *nodePollState {
	return &nodePollState{}
}

func (s *nodePollState) shouldPoll(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !now.Before(s.nextAllowed)
}

func (s *nodePollState) recordSuccess(now time.Time) {
	s.mu.Lock()
	s.backoff = 0
	s.nextAllowed = time.Time{}
	s.lastSuccess = now
	s.firstFailure = time.Time{}
	s.lastWarn = time.Time{}
	s.lastError = nil
	s.consecutive = 0
	s.mu.Unlock()
}

func (s *nodePollState) recordFailure(
	now time.Time,
	err error,
	kind pollErrorKind,
	cfg backoffConfig,
	jitterFn func(time.Duration) time.Duration,
) (time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.lastError = err
	if s.consecutive == 0 {
		s.firstFailure = now
	}
	s.consecutive++

	delay := cfg.Base
	if kind == pollErrorRetryable {
		if s.backoff == 0 {
			s.backoff = cfg.Initial
		} else {
			s.backoff *= 2
			if s.backoff > cfg.Max {
				s.backoff = cfg.Max
			}
		}
		delay = s.backoff
		if jitterFn != nil && cfg.Jitter > 0 {
			j := jitterFn(cfg.Jitter)
			if j < 0 {
				j = 0
			}
			delay += j
		}
		s.nextAllowed = now.Add(delay)
	} else {
		s.backoff = 0
		s.nextAllowed = time.Time{}
	}

	if delay < cfg.Base {
		delay = cfg.Base
	}

	warn := false
	var reference time.Time
	if !s.lastSuccess.IsZero() {
		reference = s.lastSuccess
	} else if !s.firstFailure.IsZero() {
		reference = s.firstFailure
	}

	if cfg.WarnAfter > 0 && !reference.IsZero() {
		if now.Sub(reference) >= cfg.WarnAfter {
			if s.lastWarn.IsZero() || now.Sub(s.lastWarn) >= time.Second {
				warn = true
				s.lastWarn = now
			}
		}
	}

	return delay, warn
}

func (s *nodePollState) lastErrorSnapshot() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastError
}

func classifyPollError(err error) pollErrorKind {
	if err == nil {
		return pollErrorNone
	}

	if errors.Is(err, ErrMetricsIncompatible) {
		return pollErrorParse
	}

	var syntaxErr *json.SyntaxError
	if errors.As(err, &syntaxErr) {
		return pollErrorParse
	}

	var typeErr *json.UnmarshalTypeError
	if errors.As(err, &typeErr) {
		return pollErrorParse
	}

	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) {
		if statusErr.StatusCode >= 500 {
			return pollErrorRetryable
		}
		return pollErrorFatal
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return pollErrorRetryable
	}

	var netErr net.Error
	if errors.As(err, &netErr) {
		return pollErrorRetryable
	}

	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return pollErrorRetryable
	}

	return pollErrorRetryable
}

var (
	singleNodeBackoff = backoffConfig{
		Base:      constants.DefaultMetricsInterval,
		Initial:   constants.MetricsBackoffInitial,
		Max:       constants.MetricsBackoffMax,
		Jitter:    constants.MetricsBackoffJitter,
		WarnAfter: constants.MetricsHealthTimeout,
	}
	multiNodeBackoff = backoffConfig{
		Base:      constants.MetricsPollInterval,
		Initial:   constants.MetricsBackoffInitial,
		Max:       constants.MetricsBackoffMax,
		Jitter:    constants.MetricsBackoffJitter,
		WarnAfter: constants.MetricsHealthTimeout,
	}
)
