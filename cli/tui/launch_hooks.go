/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"sync"
	"sync/atomic"

	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
)

// LaunchState records whether the engine accepted the run submission. It is
// shared by value-copied LaunchHooks so launchers and their callers agree on
// the pre-/post-submission boundary even when the launcher later returns an
// unrelated UI, signal, or cleanup error.
type LaunchState struct {
	submission atomic.Uint32
	once       sync.Once
	session    *runcontrol.Session
}

// SubmissionState is retained as a TUI compatibility alias while RunSession
// owns the authoritative state for session-managed launches.
type SubmissionState = runcontrol.SubmissionState

const (
	// SubmissionNotSubmitted means no ambiguous POST was dispatched.
	SubmissionNotSubmitted = runcontrol.SubmissionNotSubmitted
	// SubmissionSubmitted means structured evidence confirmed acceptance.
	SubmissionSubmitted = runcontrol.SubmissionSubmitted
	// SubmissionUnknown means a dispatched POST could not be reconciled.
	SubmissionUnknown = runcontrol.SubmissionUnknown
)

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

// NewSessionLaunchHooks binds launch notifications and resource registration
// to the supplied RunSession.
func NewSessionLaunchHooks(session *runcontrol.Session, onSubmitted func()) LaunchHooks {
	return LaunchHooks{
		OnSubmitted: onSubmitted, state: &LaunchState{session: session},
	}
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
	transitioned := true
	if h.state.session != nil {
		transitioned = h.state.session.MarkSubmitted()
	} else {
		h.state.submission.Store(uint32(SubmissionSubmitted))
	}
	if !transitioned {
		return
	}
	h.state.once.Do(func() {
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
	if h.state.session != nil {
		if !h.state.session.MarkSubmissionUnknown() {
			return
		}
		// Unknown means the POST may be live. Arm evidence collection once so
		// cancellation salvage can retain any status/artifacts that appear,
		// while the launcher performs bounded conservative cleanup.
		h.state.once.Do(func() {
			if h.OnSubmitted != nil {
				h.OnSubmitted()
			}
		})
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

// ProvenNotSubmitted reports that no request with an ambiguous outcome was
// dispatched. Only this state permits canceling an unarmed evidence monitor.
func (h LaunchHooks) ProvenNotSubmitted() bool {
	return h.SubmissionState() == SubmissionNotSubmitted
}

// PotentiallySubmitted includes both confirmed and unresolved submissions.
func (h LaunchHooks) PotentiallySubmitted() bool {
	return h.SubmissionState() != SubmissionNotSubmitted
}

// SubmissionState returns the strongest submission fact observed by these
// hooks. A zero-value LaunchHooks reports SubmissionNotSubmitted.
func (h LaunchHooks) SubmissionState() SubmissionState {
	if h.state == nil {
		return SubmissionNotSubmitted
	}
	if h.state.session != nil {
		return h.state.session.SubmissionState()
	}
	return SubmissionState(h.state.submission.Load())
}

// SessionManaged reports whether post-submission disposal belongs to a
// RunSession rather than this presentation adapter.
func (h LaunchHooks) SessionManaged() bool {
	return h.state != nil && h.state.session != nil
}

// RegisterResourceFinalizer binds launcher-owned resources to the session.
// Compatibility/self-managed hooks intentionally ignore registration.
func (h LaunchHooks) RegisterResourceFinalizer(finalizer runcontrol.ResourceFinalizer) error {
	if h.state == nil || h.state.session == nil {
		return nil
	}
	return h.state.session.RegisterResourceFinalizer(finalizer)
}
