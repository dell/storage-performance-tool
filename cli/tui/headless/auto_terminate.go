/*
Copyright © 2026 Dell Technologies
*/

package headless

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/tui"
)

// errAutoTerminateReached reports the configured deadline. It wraps
// context.DeadlineExceeded, which the self-managed shutdown result maps to an
// AutoTerminateError.
var errAutoTerminateReached = fmt.Errorf("auto-terminate deadline reached: %w", context.DeadlineExceeded)

// autoTerminateDeadline is the --auto-terminate-seconds clock of one headless
// run. Until the workload is submitted it cancels the launch context, so the
// deadline also bounds host connection, container start and the API wait.
// After submission it only reports that the deadline was reached: the launch
// context, and with it log reading and status polling, stays open while the
// run is stopped.
type autoTerminateDeadline struct {
	seconds          int
	reached          chan struct{}
	beforeSubmission atomic.Bool
	timer            *time.Timer
}

// startAutoTerminateDeadline returns the launch context and the deadline. The
// returned stop function releases the timer and cancels the launch context; a
// caller defers it. With seconds <= 0 the deadline never fires.
func startAutoTerminateDeadline(
	parent context.Context, seconds int, hooks tui.LaunchHooks,
) (context.Context, *autoTerminateDeadline, func()) {
	if parent == nil {
		parent = context.Background()
	}
	launchCtx, cancel := context.WithCancel(parent)
	deadline := &autoTerminateDeadline{seconds: seconds}
	if seconds <= 0 {
		return launchCtx, deadline, cancel
	}
	deadline.reached = make(chan struct{})
	deadline.timer = time.AfterFunc(time.Duration(seconds)*time.Second, func() {
		if !hooks.PotentiallySubmitted() {
			deadline.beforeSubmission.Store(true)
			cancel()
		}
		close(deadline.reached)
	})
	return launchCtx, deadline, func() {
		deadline.timer.Stop()
		cancel()
	}
}

// Reached returns a channel closed when the deadline fires. It is nil, and
// never ready, when no deadline is configured.
func (d *autoTerminateDeadline) Reached() <-chan struct{} {
	if d == nil {
		return nil
	}
	return d.reached
}

func (d *autoTerminateDeadline) reachedNow() bool {
	if d == nil || d.reached == nil {
		return false
	}
	select {
	case <-d.reached:
		return true
	default:
		return false
	}
}

// startError names auto-terminate when the deadline ended start-up before the
// workload was submitted. The wrapped error is what start-up was doing then;
// any timeout it quotes is that step's own limit, not the time waited.
func (d *autoTerminateDeadline) startError(err error) error {
	if err == nil || d == nil || !d.beforeSubmission.Load() {
		return err
	}
	return fmt.Errorf(
		"auto-terminate deadline (%d s) reached during start-up, before the workload was submitted; start-up was stopped at: %w",
		d.seconds, err)
}

// autoTerminateReachedMessage is the line printed when the deadline fires.
func autoTerminateReachedMessage(seconds int) string {
	if seconds <= 0 {
		return "Auto-terminate deadline reached; stopping the run"
	}
	return fmt.Sprintf("Auto-terminate deadline (%d s) reached; stopping the run", seconds)
}

// channelClosed reports whether ch is closed. A nil channel is never closed.
func channelClosed(ch <-chan struct{}) bool {
	if ch == nil {
		return false
	}
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// stopSessionRunAtAutoTerminate asks the run session to stop the workload at
// the deadline and waits, bounded, until the session reports it terminal, so
// the caller's log reader and trace file stay open until the run has stopped.
// It returns ctx.Err() when ctx ends first (an interrupt during the stop); a
// wait that times out is reported and does not fail the run, since the
// session still stops the run and finalizes on its own.
func stopSessionRunAtAutoTerminate(
	ctx context.Context, hooks tui.LaunchHooks, output func(category, message string),
) error {
	err := hooks.StopWorkloadAndWait(ctx, constants.AutoTerminateStopWaitTimeout)
	if err == nil {
		return nil
	}
	if ctx != nil && ctx.Err() != nil && errors.Is(err, ctx.Err()) {
		return err
	}
	output("WARNING", fmt.Sprintf("Auto-terminate stop is still in progress: %v", err))
	return nil
}
