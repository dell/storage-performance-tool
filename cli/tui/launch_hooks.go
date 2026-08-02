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
	submitted atomic.Bool
	once      sync.Once
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
		h.state.submitted.Store(true)
		if h.OnSubmitted != nil {
			h.OnSubmitted()
		}
	})
}

// Submitted reports whether NotifySubmitted crossed the accepted-POST
// boundary for this launch.
func (h LaunchHooks) Submitted() bool {
	return h.state != nil && h.state.submitted.Load()
}
