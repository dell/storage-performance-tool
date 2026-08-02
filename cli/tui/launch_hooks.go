/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"sync"
	"sync/atomic"
)

// LaunchState records whether the engine accepted the run submission. It is
// shared by value-copied LaunchHooks so launchers and their callers agree on
// the pre-/post-submission boundary even when the launcher later returns an
// unrelated UI, signal, or cleanup error.
type LaunchState struct {
	submission atomic.Uint32
	once       sync.Once
}

// SubmissionState describes what the client can prove about POST /run. Once
// an HTTP request has been dispatched, a transport error cannot prove that the
// server rejected it, so SubmissionUnknown is intentionally distinct from
// SubmissionNotSubmitted.
type SubmissionState uint32

const (
	SubmissionNotSubmitted SubmissionState = iota
	SubmissionSubmitted
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

// LaunchHooks carries orchestration lifecycle notifications that are not part
// of scenario configuration. Hooks run synchronously at the named boundary.
type LaunchHooks struct {
	OnSubmitted func()
	state       *LaunchState
}

// NewLaunchHooks creates hooks with a race-safe, exactly-once submission
// state. The zero value remains valid for compatibility callers that do not
// need to inspect submission state.
func NewLaunchHooks(onSubmitted func()) LaunchHooks {
	return LaunchHooks{OnSubmitted: onSubmitted, state: &LaunchState{}}
}

func ensureLaunchState(hooks LaunchHooks) LaunchHooks {
	if hooks.state == nil {
		return NewLaunchHooks(hooks.OnSubmitted)
	}
	return hooks
}

// NotifySubmitted reports that the engine accepted the /run submission.
func (h LaunchHooks) NotifySubmitted() {
	if h.state == nil {
		if h.OnSubmitted != nil {
			h.OnSubmitted()
		}
		return
	}
	h.state.once.Do(func() {
		h.state.submission.Store(uint32(SubmissionSubmitted))
		if h.OnSubmitted != nil {
			h.OnSubmitted()
		}
	})
}

// NotifySubmissionUnknown records that POST /run may have reached the engine,
// but bounded reconciliation could not prove whether it was accepted. The
// state may later advance to SubmissionSubmitted if matching /status evidence
// appears; it never moves back to NotSubmitted.
func (h LaunchHooks) NotifySubmissionUnknown() {
	if h.state == nil {
		return
	}
	h.state.submission.CompareAndSwap(
		uint32(SubmissionNotSubmitted), uint32(SubmissionUnknown))
}

// Submitted reports whether NotifySubmitted crossed the accepted-POST
// boundary for this launch.
func (h LaunchHooks) Submitted() bool {
	return h.SubmissionState() == SubmissionSubmitted
}

// SubmissionState returns the strongest submission fact observed by these
// hooks. A zero-value LaunchHooks reports SubmissionNotSubmitted.
func (h LaunchHooks) SubmissionState() SubmissionState {
	if h.state == nil {
		return SubmissionNotSubmitted
	}
	return SubmissionState(h.state.submission.Load())
}
