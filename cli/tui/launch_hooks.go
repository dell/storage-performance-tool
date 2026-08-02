/*
Copyright © 2026 Dell Technologies
*/

package tui

// LaunchHooks carries orchestration lifecycle notifications that are not part
// of scenario configuration. Hooks run synchronously at the named boundary.
type LaunchHooks struct {
	OnSubmitted func()
}

// NotifySubmitted reports that the engine accepted the /run submission.
func (h LaunchHooks) NotifySubmitted() {
	if h.OnSubmitted != nil {
		h.OnSubmitted()
	}
}
