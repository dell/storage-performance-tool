/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
)

const replayTerminalFailureReason = `step replay-001-create failed (execution): target bucket "b" does not exist and ` +
	`target-bucket provisioning is disabled`

// replayFailedOutcome is how the results monitor ends a replay the engine ended FAILED, as the
// triage measured it: the failed step left a messages log, so the summary counts it, while the
// fetch of its other artifacts failed.
func replayFailedOutcome(stepsWithResults int) autoResultsOutcome {
	return autoResultsOutcome{
		SummaryRead:      true,
		StepsWithResults: stepsWithResults,
		ArtifactErr:      errors.New("missing required artifacts: config.yaml, errors.log, metrics.total.csv"),
		Tracker: &portcheck.RunResult{
			FinalState:      constants.StateFailed,
			FailureStepID:   "replay-001-create",
			FailureCategory: "execution",
			FailureMessage:  `target bucket "b" does not exist and target-bucket provisioning is disabled`,
		},
	}
}

// The engine can end a replay FAILED in a step that still leaves results, here its only step, so
// the step count passes it. Replay exits 1 with the engine's cause, as spt run does, whichever step
// failed.
func TestResolveReplayRunErrorExitsOneOnATerminalFailure(t *testing.T) {
	launchErr := errors.New("launch failed")
	cases := []struct {
		name           string
		runErr         error
		autoTerminated bool
		expectedSteps  int
		outcome        autoResultsOutcome
		wantMsg        string
		wantCause      error
	}{
		{name: "its only step", expectedSteps: 1, outcome: replayFailedOutcome(1), wantMsg: replayTerminalFailureReason},
		{name: "a later step, with results for every step", expectedSteps: 3, outcome: replayFailedOutcome(3),
			wantMsg: replayTerminalFailureReason},
		{name: "the presenter's reflection of it adds nothing",
			runErr:        &runcontrol.OwnedEngineTerminalFailure{Detail: "engine reported FAILED"},
			expectedSteps: 1, outcome: replayFailedOutcome(1), wantMsg: replayTerminalFailureReason},
		{name: "an independent run error is kept", runErr: launchErr, expectedSteps: 1, outcome: replayFailedOutcome(1),
			wantMsg: replayTerminalFailureReason + "; run also failed: launch failed", wantCause: launchErr},
		{name: "under auto-terminate", autoTerminated: true, expectedSteps: 1, outcome: replayFailedOutcome(1),
			wantMsg: replayTerminalFailureReason},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := resolveReplayRunError(tc.runErr, tc.autoTerminated, tc.expectedSteps, tc.outcome)
			var exit *ExitCodeError
			if !errors.As(err, &exit) || exit.Code != constants.ExitCodeWorkloadFailure {
				t.Fatalf("error = %v, want exit code %d", err, constants.ExitCodeWorkloadFailure)
			}
			if exit.Msg != tc.wantMsg {
				t.Fatalf("message = %q, want %q", exit.Msg, tc.wantMsg)
			}
			if tc.wantCause != nil && !errors.Is(err, tc.wantCause) {
				t.Fatalf("error = %v, want cause %v", err, tc.wantCause)
			}
		})
	}
}

// A replay whose steps all finished keeps today's exit 0 when the results fetch is partial.
func TestResolveReplayRunErrorKeepsASuccessfulReplayWithAPartialFetch(t *testing.T) {
	for _, state := range []string{constants.StateCompleted, constants.StateRunning, ""} {
		outcome := replayFailedOutcome(1)
		outcome.Tracker = &portcheck.RunResult{FinalState: state}
		if err := resolveReplayRunError(nil, false, 1, outcome); err != nil {
			t.Fatalf("final state %q: error = %v, want nil", state, err)
		}
	}
	if err := resolveReplayRunError(nil, false, 1, autoResultsOutcome{SummaryRead: true, StepsWithResults: 1}); err != nil {
		t.Fatalf("no tracker result: error = %v, want nil", err)
	}
}

// The measured case end to end: the engine ends the replay's only step FAILED, the presenter
// returns no error, and the summary counts the step. Replay exits 1 on every route.
func TestReplayCommandExitsOneWhenItsOnlyStepFailsTerminally(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()

	routes := []struct {
		name, hosts string
		headless    bool
	}{
		{name: "local headless", hosts: "127.0.0.1", headless: true},
		{name: "local TUI", hosts: "127.0.0.1"},
		{name: "lone remote headless", hosts: "entry.example", headless: true},
		{name: "distributed TUI", hosts: "entry.example,worker.example"},
	}
	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			t.Cleanup(installReplayEngineInfoSeams(t))
			resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			connectReplayOrchestrator = func(context.Context, *tui.MultiHostOrchestrator) error { return nil }
			confirmReplayLaunchCommand = func(io.Writer) error { return nil }
			shouldReplayRunHeadless = func(*cobra.Command) bool { return route.headless }
			launches := 0
			submit := func(hooks tui.LaunchHooks) error {
				launches++
				hooks.NotifySubmitted()
				return nil
			}
			startReplayLocalHeadless = func(
				_ string, _ string, _ scenario.Params, options headless.HeadlessOptions, _ []byte, _ []byte,
			) error {
				return submit(options.LaunchHooks)
			}
			startReplayLocalTUI = func(_ string, _ string, _ scenario.Params, options tui.RunOptions) error {
				return submit(options.LaunchHooks)
			}
			startReplayRemoteHeadless = func(
				_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions,
				_ []byte, _ []byte,
			) error {
				return submit(options.LaunchHooks)
			}
			startReplayRemoteTUI = func(
				_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options tui.RunOptions,
			) error {
				return submit(options.LaunchHooks)
			}
			startReplayAutoResultsMonitor = func(
				context.Context, string, string, string, []string, int64,
				bool, []*hostparse.HostInfo, string, bool, int, string,
				*runMetadata, io.Writer, io.Writer, string, func(context.Context),
				...*integrity.FinalizeOptions,
			) *autoResultsMonitor {
				done := make(chan autoResultsOutcome, 1)
				done <- replayFailedOutcome(1)
				return &autoResultsMonitor{done: done, armed: make(chan struct{})}
			}

			cmd := newReplayCommandForTest(t)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{
				"--from", server.URL,
				"--endpoints", "http://s3.example",
				"--test-hosts", route.hosts,
				"--results-dir", t.TempDir(),
			})
			err := cmd.Execute()
			var exit *ExitCodeError
			if !errors.As(err, &exit) || exit.Code != constants.ExitCodeWorkloadFailure {
				t.Fatalf("Execute() error = %v, want exit code %d", err, constants.ExitCodeWorkloadFailure)
			}
			if !strings.Contains(exit.Msg, replayTerminalFailureReason) {
				t.Fatalf("message = %q, want the engine's cause", exit.Msg)
			}
			if launches != 1 {
				t.Fatalf("launches = %d, want 1", launches)
			}
		})
	}
}
