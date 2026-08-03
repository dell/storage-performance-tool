/*
Copyright © 2026 Dell Technologies
*/

package runcontrol

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// SubmissionState is the strongest fact known about POST /run.
type SubmissionState uint32

const (
	// SubmissionNotSubmitted means no ambiguous POST was dispatched.
	SubmissionNotSubmitted SubmissionState = iota
	// SubmissionSubmitted means structured evidence confirmed acceptance.
	SubmissionSubmitted
	// SubmissionUnknown means a dispatched POST could not be reconciled.
	SubmissionUnknown
)

func (s SubmissionState) String() string {
	switch s {
	case SubmissionSubmitted:
		return "submitted"
	case SubmissionUnknown:
		return "submission-unknown"
	default:
		return "not-submitted"
	}
}

// ResourceFinalizer performs the bounded diagnostics/removal lifecycle and
// reports whether ownership was released.
type ResourceFinalizer func(context.Context) FinalizationOutcome

// ErrResourceFinalizerNotRegistered reports incomplete session wiring.
var ErrResourceFinalizerNotRegistered = errors.New("run session resource finalizer is not registered")

type finalizationAttempt struct {
	done    chan struct{}
	outcome FinalizationOutcome
}

// Session is the presentation-neutral authority for accepted submission,
// terminal workload notification, and post-submission resource disposal.
type Session struct {
	submission atomic.Uint32
	terminal   chan struct{}
	termOnce   sync.Once

	mu        sync.Mutex
	finalizer ResourceFinalizer
	attempt   *finalizationAttempt
}

// NewSession creates an unsubmitted session with no registered resources.
func NewSession() *Session {
	return &Session{terminal: make(chan struct{})}
}

// MarkWorkloadTerminal broadcasts that authoritative completion tracking has
// resolved the workload, whether successfully or with an error. Presentation
// adapters consume this signal only to stop waiting and release their polling
// resources; the coordinator still owns evidence collection and finalization.
func (s *Session) MarkWorkloadTerminal() {
	if s == nil || s.terminal == nil {
		return
	}
	s.termOnce.Do(func() { close(s.terminal) })
}

// WorkloadTerminal returns the broadcast channel closed by
// MarkWorkloadTerminal. A nil or zero-value session returns a nil channel.
func (s *Session) WorkloadTerminal() <-chan struct{} {
	if s == nil {
		return nil
	}
	return s.terminal
}

// MarkSubmitted advances the session to a confirmed accepted submission. It
// returns true only for the transition which first establishes that fact.
func (s *Session) MarkSubmitted() bool {
	if s == nil {
		return false
	}
	for {
		current := SubmissionState(s.submission.Load())
		if current == SubmissionSubmitted {
			return false
		}
		if s.submission.CompareAndSwap(uint32(current), uint32(SubmissionSubmitted)) {
			return true
		}
	}
}

// MarkSubmissionUnknown records a dispatched but unresolved POST without
// weakening a previously confirmed submission.
func (s *Session) MarkSubmissionUnknown() bool {
	if s == nil {
		return false
	}
	return s.submission.CompareAndSwap(
		uint32(SubmissionNotSubmitted), uint32(SubmissionUnknown))
}

// SubmissionState returns the strongest submission fact observed.
func (s *Session) SubmissionState() SubmissionState {
	if s == nil {
		return SubmissionNotSubmitted
	}
	return SubmissionState(s.submission.Load())
}

// RegisterResourceFinalizer binds the one launcher-owned resource set to this
// session. Registering a second finalizer is a lifecycle wiring error.
func (s *Session) RegisterResourceFinalizer(finalizer ResourceFinalizer) error {
	if s == nil {
		return fmt.Errorf("nil run session")
	}
	if finalizer == nil {
		return fmt.Errorf("nil resource finalizer")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finalizer != nil {
		return fmt.Errorf("resource finalizer already registered")
	}
	s.finalizer = finalizer
	return nil
}

// FinalizeResources starts or joins the one canonical finalization attempt.
// A canceled waiter may return while the bounded worker continues. A failed
// attempt which retains ownership becomes retryable only after it completes;
// concurrent callers never overlap cleanup.
func (s *Session) FinalizeResources(ctx context.Context) FinalizationOutcome {
	if s == nil {
		return FinalizationOutcome{
			Removal:   CompletedPhase(ErrResourceFinalizerNotRegistered),
			Resources: ResourceDispositionUnknown,
		}
	}
	if ctx == nil {
		ctx = context.Background()
	}

	s.mu.Lock()
	if s.finalizer == nil {
		s.mu.Unlock()
		return FinalizationOutcome{
			Removal:   CompletedPhase(ErrResourceFinalizerNotRegistered),
			Resources: ResourceDispositionUnknown,
		}
	}
	if s.attempt != nil {
		select {
		case <-s.attempt.done:
			if s.attempt.outcome.Resources == ResourceDispositionRetained {
				s.attempt = nil
			} else {
				attempt := s.attempt
				s.mu.Unlock()
				return attempt.outcome
			}
		default:
			attempt := s.attempt
			s.mu.Unlock()
			return waitForFinalization(ctx, attempt)
		}
	}
	attempt := &finalizationAttempt{done: make(chan struct{})}
	s.attempt = attempt
	finalizer := s.finalizer
	s.mu.Unlock()

	go func() {
		// The finalizer owns explicit per-phase bounds. Detaching it from a
		// caller which merely stops waiting preserves mandatory cleanup.
		attempt.outcome = finalizer(context.WithoutCancel(ctx))
		close(attempt.done)
	}()
	return waitForFinalization(ctx, attempt)
}

func waitForFinalization(ctx context.Context, attempt *finalizationAttempt) FinalizationOutcome {
	select {
	case <-attempt.done:
		return attempt.outcome
	case <-ctx.Done():
		return FinalizationOutcome{
			Resources: ResourceDispositionRetained,
			WaitErr:   ctx.Err(),
		}
	}
}
