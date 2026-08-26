/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
)

// LaunchState records cleanup ownership and whether ordinary run evidence is
// trusted. It is shared by value-copied LaunchHooks so launchers and callers
// agree on both facts after an unrelated UI, signal, or cleanup error.
type LaunchState struct {
	submission            atomic.Uint32
	normalEvidenceTrusted atomic.Bool
	once                  sync.Once
	session               *runcontrol.Session
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
	OnSubmitted        func()
	preSubmissionCheck func(context.Context) ([]string, error)
	state              *LaunchState
}

// NewLaunchHooks creates hooks with a race-safe, exactly-once submission
// state. The zero value remains valid for compatibility callers that do not
// need to inspect submission state.
func NewLaunchHooks(onSubmitted func()) LaunchHooks {
	return LaunchHooks{OnSubmitted: onSubmitted, state: &LaunchState{}}
}

// WithPreSubmissionCheck returns a copy that runs check synchronously after
// engine readiness and before POST /run. The returned lines are already safe
// for normal launch output and the error prevents submission.
func (h LaunchHooks) WithPreSubmissionCheck(
	check func(context.Context) ([]string, error),
) LaunchHooks {
	h.preSubmissionCheck = check
	return h
}

// RunPreSubmissionCheck executes the optional readiness-to-submission gate.
func (h LaunchHooks) RunPreSubmissionCheck(ctx context.Context) ([]string, error) {
	if h.preSubmissionCheck == nil {
		return nil, nil
	}
	return h.preSubmissionCheck(ctx)
}

func runPreSubmissionCheck(ctx context.Context, hooks LaunchHooks, output func(string)) error {
	lines, err := hooks.RunPreSubmissionCheck(ctx)
	if output != nil {
		for _, line := range lines {
			output(line)
		}
	}
	return err
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
		initialized := NewLaunchHooks(hooks.OnSubmitted)
		initialized.preSubmissionCheck = hooks.preSubmissionCheck
		return initialized
	}
	return hooks
}

// NotifySubmitted reports that the engine accepted the /run submission and
// established the trusted identity required for ordinary evidence.
func (h LaunchHooks) NotifySubmitted() {
	if h.state == nil {
		if h.OnSubmitted != nil {
			h.OnSubmitted()
		}
		return
	}
	// Trust in ordinary run evidence is distinct from cleanup ownership. Set it
	// before the synchronous callback arms result monitoring.
	h.state.normalEvidenceTrusted.Store(true)
	if h.state.session != nil {
		h.state.session.MarkSubmitted()
	} else {
		h.state.submission.Store(uint32(SubmissionSubmitted))
	}
	h.state.once.Do(func() {
		if h.OnSubmitted != nil {
			h.OnSubmitted()
		}
	})
}

// NotifyAcceptedForCleanup reports that the engine accepted POST /run, but
// its response did not establish a trusted run identity. It retains cleanup
// ownership without arming the normal evidence monitor.
func (h LaunchHooks) NotifyAcceptedForCleanup() {
	if h.state == nil {
		return
	}
	if h.state.session != nil {
		h.state.session.MarkSubmitted()
	} else {
		h.state.submission.Store(uint32(SubmissionSubmitted))
	}
}

func notifyAcceptedSubmission(hooks LaunchHooks, submissionErr error) {
	var identityErr *SubmissionIdentityError
	if errors.As(submissionErr, &identityErr) {
		hooks.NotifyAcceptedForCleanup()
		return
	}
	hooks.NotifySubmitted()
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

// Submitted reports confirmed POST acceptance, including an identity-invalid
// response retained only for cleanup.
func (h LaunchHooks) Submitted() bool {
	return h.SubmissionState() == SubmissionSubmitted
}

// ProvenNotSubmitted reports that no request with an ambiguous outcome was
// dispatched. Evidence permission is reported separately.
func (h LaunchHooks) ProvenNotSubmitted() bool {
	return h.SubmissionState() == SubmissionNotSubmitted
}

// PotentiallySubmitted includes both confirmed and unresolved submissions.
func (h LaunchHooks) PotentiallySubmitted() bool {
	return h.SubmissionState() != SubmissionNotSubmitted
}

// NormalEvidencePermitted reports whether the launch established the trusted
// run identity required for ordinary completion tracking and artifact use.
// Accepted-for-cleanup and unresolved submissions deliberately return false.
func (h LaunchHooks) NormalEvidencePermitted() bool {
	return h.state != nil && h.state.normalEvidenceTrusted.Load()
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

// WorkloadTerminal returns the authoritative terminal-workload signal.
// Compatibility hooks return nil and retain presentation-owned completion.
func (h LaunchHooks) WorkloadTerminal() <-chan struct{} {
	if h.state == nil || h.state.session == nil {
		return nil
	}
	return h.state.session.WorkloadTerminal()
}

// RegisterResourceFinalizer binds launcher-owned resources to the session.
// Compatibility/self-managed hooks intentionally ignore registration.
func (h LaunchHooks) RegisterResourceFinalizer(finalizer runcontrol.ResourceFinalizer) error {
	if h.state == nil || h.state.session == nil {
		return nil
	}
	return h.state.session.RegisterResourceFinalizer(finalizer)
}
