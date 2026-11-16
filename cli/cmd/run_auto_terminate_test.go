/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results/summary"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
)

// autoTerminateTracker plays the engine's run tracker. In a stopped run the first call follows
// the run until the monitor ends tracking, and the reconciliation after the stop request sees the
// run STOPPED. In a failed run the first call sees the step FAILED.
type autoTerminateTracker struct {
	failed bool
	calls  *atomic.Int32
}

func (f *autoTerminateTracker) WaitForCompletion(ctx context.Context, _ []string) (*portcheck.RunResult, error) {
	if f.failed {
		f.calls.Add(1)
		return &portcheck.RunResult{
			FinalState: constants.StateFailed, RunID: 41,
			FailureStepID: "mock-step", FailureCategory: "IO", FailureMessage: "target refused writes",
		}, nil
	}
	if f.calls.Add(1) == 1 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &portcheck.RunResult{FinalState: constants.StateStopped, RunID: 41}, nil
}

func (f *autoTerminateTracker) SetDebug(bool)                {}
func (f *autoTerminateTracker) SetExpectedRunID(int64)       {}
func (f *autoTerminateTracker) SetRequireTerminalState(bool) {}

// autoTerminateEvents records the order of the engine stop, the artifact fetch and the shutdown.
type autoTerminateEvents struct {
	mu     sync.Mutex
	events []string
}

func (e *autoTerminateEvents) add(event string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.events = append(e.events, event)
}

func (e *autoTerminateEvents) list() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return slices.Clone(e.events)
}

// installAutoTerminateMonitorSeams keeps the shipped auto-results monitor off the network and
// records each engine call it makes.
func installAutoTerminateMonitorSeams(t *testing.T, failed bool) *autoTerminateEvents {
	t.Helper()
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	origShutdown := requestShutdownAllFunc
	origStop := requestRunStopFunc
	t.Cleanup(func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
		requestShutdownAllFunc = origShutdown
		requestRunStopFunc = origStop
	})
	events := &autoTerminateEvents{}
	calls := &atomic.Int32{}
	newRunTrackerFunc = func(string) autoResultsRunTracker {
		return &autoTerminateTracker{failed: failed, calls: calls}
	}
	discoverStepIDsFunc = func(context.Context, string, int64) ([]string, error) { return []string{"mock-step"}, nil }
	discoverFleetStepIDsFunc = func(context.Context, string, int64) ([]string, error) { return nil, nil }
	newResultsFetcherFunc = func(_, output string) autoResultsFetcher {
		return &fakeFetcher{output: output, onFetch: func(string, []string) error {
			events.add("fetch")
			return nil
		}}
	}
	generateRunSummaryFunc = func(context.Context, string, io.Writer) (*summary.RunSummary, error) {
		return nil, nil
	}
	requestShutdownAllFunc = func(context.Context, []*hostparse.HostInfo, string, time.Duration, int64, bool) error {
		events.add("shutdown")
		return nil
	}
	requestRunStopFunc = func(_ context.Context, _ string, runID int64) error {
		if runID <= 0 {
			t.Errorf("stop request for run %d, want the owned run", runID)
		}
		events.add("stop")
		return nil
	}
	return events
}

// autoTerminateLaunch plays the headless runner. A stopped run reaches the deadline after
// submission, has the session stop the run, and returns the runner's auto-terminate result once
// the session reports the run terminal. A failed run returns after submission.
func autoTerminateLaunch(t *testing.T, failed bool) func(context.Context, tui.LaunchHooks) error {
	t.Helper()
	monitorReady := awaitMonitorSetup(t)
	return func(ctx context.Context, hooks tui.LaunchHooks) error {
		monitorReady()
		hooks.NotifySubmitted()
		if failed {
			return nil
		}
		if err := hooks.StopWorkloadAndWait(ctx, 10*time.Second); err != nil {
			return err
		}
		return &headless.AutoTerminateError{CleanupComplete: true}
	}
}

type autoTerminateCommandCase struct {
	name       string
	hosts      []string
	shutdownOn bool
	failed     bool
}

var autoTerminateCommandCases = []autoTerminateCommandCase{
	{name: "local", shutdownOn: true},
	{name: "local without shutdown"},
	{name: "local step failed", shutdownOn: true, failed: true},
	{name: "multi-host", hosts: []string{"entry.example", "worker.example"}, shutdownOn: true},
	{name: "multi-host without shutdown", hosts: []string{"entry.example", "worker.example"}},
	{name: "multi-host step failed", hosts: []string{"entry.example", "worker.example"}, shutdownOn: true, failed: true},
}

// checkAutoTerminateCommand asserts the exit status and the engine calls. A stopped run asks the
// engine to stop the step, fetches its artifacts and only then shuts down, and exits 0. Without
// --shutdown-on-complete the engines stay up. A step that failed before the deadline exits 1 and
// is not stopped.
func checkAutoTerminateCommand(t *testing.T, test autoTerminateCommandCase, err error, events []string, output string) {
	t.Helper()
	if test.failed {
		var exitErr *ExitCodeError
		if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeWorkloadFailure {
			t.Fatalf("error = %v, want exit %d for the failed step", err, constants.ExitCodeWorkloadFailure)
		}
		if slices.Contains(events, "stop") {
			t.Fatalf("engine calls = %v; a run that failed before the deadline was stopped", events)
		}
		return
	}
	if err != nil {
		t.Fatalf("error = %v, want exit 0 for a run auto-terminate stopped", err)
	}
	want := []string{"stop", "fetch"}
	if test.shutdownOn {
		want = append(want, "shutdown")
	}
	if !slices.Equal(events, want) {
		t.Fatalf("engine calls = %v, want %v", events, want)
	}
	for _, line := range []string{
		"Auto-results: auto-terminate deadline reached; stopping the run...",
		"Auto-results: run stopped.",
	} {
		if !strings.Contains(output, line) {
			t.Errorf("output lacks %q:\n%s", line, output)
		}
	}
}

func setAutoTerminateGate(t *testing.T) {
	t.Helper()
	evaluateEngineIdentityGate = func(
		ctx context.Context, _ engineinfo.FleetCollector, descriptors []engineinfo.ParticipantDescriptor, force bool,
	) (engineinfo.GateOutcome, error) {
		return engineinfo.EvaluateGate(ctx, fixedFleetCollector{
			fleet: runGateFleet(engineinfo.ConsistencyConsistent, engineinfo.StatusCollected),
		}, descriptors, force)
	}
}

// spt run --auto-terminate-seconds with auto-results stops the run at the deadline and exits 0,
// on the local and the multi-host route. Before the fix the CLI let the step run to its natural
// end and exited 1.
func TestRunCommandAutoTerminateStopsTheRun(t *testing.T) {
	for _, test := range autoTerminateCommandCases {
		t.Run(test.name, func(t *testing.T) {
			workDir := t.TempDir()
			t.Chdir(workDir)
			hosts, minHosts := "127.0.0.1", "1"
			if len(test.hosts) > 0 {
				hosts, minHosts = strings.Join(test.hosts, ","), "2"
			}
			setRunEngineInfoFlags(t, hosts, minHosts, "", false, true)
			setGlobalRunFlagForTest(t, "shutdown-on-complete", boolString(test.shutdownOn))
			setGlobalRunFlagForTest(t, "results-dir", filepath.Join(workDir, "results"))
			setGlobalRunFlagForTest(t, "label", "auto-terminate")
			setGlobalRunFlagForTest(t, "auto-terminate-seconds", "1")
			restore := installRunEngineInfoCommandSeams(t)
			defer restore()
			shouldRunHeadlessForRun = func(*cobra.Command) bool { return true }
			setAutoTerminateGate(t)
			events := installAutoTerminateMonitorSeams(t, test.failed)
			output := &lockedBuffer{}
			origAuto := startAutoResultsFunc
			t.Cleanup(func() { startAutoResultsFunc = origAuto })
			startAutoResultsFunc = func(
				ctx context.Context, baseURL, label, dir string, steps []string, runID int64, debug bool,
				hosts []*hostparse.HostInfo, apiPort string, shutdownOn bool, linger int, scenarioPath string,
				metadata *runMetadata, _ io.Writer, _ io.Writer, traceFile string, hook func(context.Context),
				options ...*integrity.FinalizeOptions,
			) *autoResultsMonitor {
				return startAutoResultsMonitor(ctx, baseURL, label, dir, steps, runID, debug, hosts, apiPort,
					shutdownOn, linger, scenarioPath, metadata, output, output, traceFile, hook, options...)
			}

			launch := autoTerminateLaunch(t, test.failed)
			var localLaunches, remoteLaunches atomic.Int32
			startLocalHeadlessRunFunc = func(_ string, _ string, _ scenario.Params, options headless.HeadlessOptions) error {
				localLaunches.Add(1)
				if options.AutoTerminateSeconds != 1 {
					t.Errorf("runner auto-terminate = %d s, want 1", options.AutoTerminateSeconds)
				}
				return launch(options.Context, options.LaunchHooks)
			}
			startMultiHostHeadlessRunFunc = func(
				_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions,
			) error {
				remoteLaunches.Add(1)
				if options.AutoTerminateSeconds != 1 {
					t.Errorf("runner auto-terminate = %d s, want 1", options.AutoTerminateSeconds)
				}
				return launch(options.Context, options.LaunchHooks)
			}

			err := runCmd.RunE(runCmd, []string{WorkloadTypeMock})
			wantLocal, wantRemote := int32(1), int32(0)
			if len(test.hosts) > 0 {
				wantLocal, wantRemote = 0, 1
			}
			if localLaunches.Load() != wantLocal || remoteLaunches.Load() != wantRemote {
				t.Fatalf("launches local=%d multi-host=%d, want %d/%d",
					localLaunches.Load(), remoteLaunches.Load(), wantLocal, wantRemote)
			}
			checkAutoTerminateCommand(t, test, err, events.list(), output.String())
		})
	}
}

// spt replay --auto-terminate-seconds stops the replay at the deadline and exits 0, on the local
// and the multi-host route, and closes with the auto-terminate line.
func TestReplayCommandAutoTerminateStopsTheRun(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()
	for _, test := range autoTerminateCommandCases {
		t.Run(test.name, func(t *testing.T) {
			t.Cleanup(installReplayEngineInfoSeams(t))
			origEvaluate := evaluateEngineIdentityGate
			t.Cleanup(func() { evaluateEngineIdentityGate = origEvaluate })
			resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			connectReplayOrchestrator = func(context.Context, *tui.MultiHostOrchestrator) error { return nil }
			confirmReplayLaunchCommand = func(io.Writer) error { return nil }
			shouldReplayRunHeadless = func(*cobra.Command) bool { return true }
			replayLocalEnginePlan = func(string) ([]engineinfo.ParticipantDescriptor, error) { return nil, nil }
			replayDistributedEnginePlan = func(*tui.MultiHostOrchestrator, string, bool) ([]engineinfo.ParticipantDescriptor, error) {
				return nil, nil
			}
			setAutoTerminateGate(t)
			events := installAutoTerminateMonitorSeams(t, test.failed)

			launch := autoTerminateLaunch(t, test.failed)
			var localLaunches, remoteLaunches atomic.Int32
			startReplayLocalHeadless = func(
				_ string, _ string, _ scenario.Params, options headless.HeadlessOptions, _ []byte, _ []byte,
			) error {
				localLaunches.Add(1)
				return launch(options.Context, options.LaunchHooks)
			}
			startReplayRemoteHeadless = func(
				_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions,
				_ []byte, _ []byte,
			) error {
				remoteLaunches.Add(1)
				return launch(options.Context, options.LaunchHooks)
			}

			cmd := newReplayCommandForTest(t)
			cmd.SetContext(context.Background())
			output := &lockedBuffer{}
			cmd.SetOut(output)
			cmd.SetErr(io.Discard)
			args := []string{
				"--from", server.URL, "--endpoints", "http://s3.example", "--headless",
				"--results-dir", t.TempDir(), "--auto-terminate-seconds", "1",
				"--shutdown-on-complete=" + boolString(test.shutdownOn),
			}
			if len(test.hosts) > 0 {
				args = append(args, "--test-hosts", strings.Join(test.hosts, ","))
			}
			cmd.SetArgs(args)
			err := cmd.Execute()
			wantLocal, wantRemote := int32(1), int32(0)
			if len(test.hosts) > 0 {
				wantLocal, wantRemote = 0, 1
			}
			if localLaunches.Load() != wantLocal || remoteLaunches.Load() != wantRemote {
				t.Fatalf("launches local=%d multi-host=%d, want %d/%d",
					localLaunches.Load(), remoteLaunches.Load(), wantLocal, wantRemote)
			}
			checkAutoTerminateCommand(t, test, err, events.list(), output.String())
			if !test.failed && !strings.Contains(output.String(), "Auto-terminate: the run was stopped at the 1 s deadline") {
				t.Errorf("output lacks the closing auto-terminate line:\n%s", output.String())
			}
		})
	}
}
