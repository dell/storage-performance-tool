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

// autoTerminateEngine says how the faked engine answers after the deadline.
type autoTerminateEngine int

const (
	// engineStepFailed: a step FAILED before the deadline. The zero value
	// stops the run once the stop request lands.
	engineStepFailed autoTerminateEngine = iota + 1
	// engineNeverTerminal: the run never reports a terminal state.
	engineNeverTerminal
	// engineAlreadyCompleted: the run COMPLETED on its own at the deadline.
	engineAlreadyCompleted
	// engineStopsOnShutdown: only the /shutdown fallback ends the run.
	engineStopsOnShutdown
)

// autoTerminateFake plays the engine for the shipped auto-results monitor: its run tracker, its
// stop and shutdown endpoints and its artifacts. It records the order of the engine calls.
type autoTerminateFake struct {
	engine      autoTerminateEngine
	stopFound   bool  // DELETE /run found the run active
	stopErr     error // DELETE /run failed
	allFailed   bool  // every request of the run failed
	onStop      func()
	calls       atomic.Int32
	shutdown    chan struct{}
	shutdownOne sync.Once

	mu          sync.Mutex
	events      []string
	fetchBudget time.Duration
}

func (f *autoTerminateFake) add(event string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.events = append(f.events, event)
}

func (f *autoTerminateFake) list() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.events)
}

func (f *autoTerminateFake) WaitForCompletion(ctx context.Context, _ []string) (*portcheck.RunResult, error) {
	first := f.calls.Add(1) == 1
	switch {
	case f.engine == engineStepFailed:
		return &portcheck.RunResult{
			FinalState: constants.StateFailed, RunID: 41,
			FailureStepID: "mock-step", FailureCategory: "IO", FailureMessage: "target refused writes",
		}, nil
	case first, f.engine == engineNeverTerminal:
		<-ctx.Done()
		return nil, ctx.Err()
	case f.engine == engineAlreadyCompleted:
		return &portcheck.RunResult{FinalState: constants.StateCompleted, RunID: 41}, nil
	case f.engine == engineStopsOnShutdown:
		select {
		case <-f.shutdown:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &portcheck.RunResult{FinalState: constants.StateStopped, RunID: 41}, nil
}

func (f *autoTerminateFake) SetDebug(bool)                {}
func (f *autoTerminateFake) SetExpectedRunID(int64)       {}
func (f *autoTerminateFake) SetRequireTerminalState(bool) {}

// installAutoTerminateMonitorSeams keeps the shipped auto-results monitor off the network. The
// stop budget is shortened, so a stop that is never confirmed ends quickly.
func installAutoTerminateMonitorSeams(t *testing.T, fake *autoTerminateFake) {
	t.Helper()
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	origShutdown := requestShutdownAllFunc
	origStop := requestRunStopFunc
	origBudgets := autoResultsPhaseBudgets
	t.Cleanup(func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
		requestShutdownAllFunc = origShutdown
		requestRunStopFunc = origStop
		autoResultsPhaseBudgets = origBudgets
	})
	autoResultsPhaseBudgets.Shutdown = 300 * time.Millisecond
	fake.shutdown = make(chan struct{})
	newRunTrackerFunc = func(string) autoResultsRunTracker { return fake }
	discoverStepIDsFunc = func(context.Context, string, int64) ([]string, error) { return []string{"mock-step"}, nil }
	discoverFleetStepIDsFunc = func(context.Context, string, int64) ([]string, error) { return nil, nil }
	newResultsFetcherFunc = func(_, output string) autoResultsFetcher {
		return &fakeFetcher{
			output: output,
			onContext: func(ctx context.Context) {
				if deadline, ok := ctx.Deadline(); ok {
					fake.mu.Lock()
					fake.fetchBudget = time.Until(deadline)
					fake.mu.Unlock()
				}
			},
			onFetch: func(string, []string) error {
				fake.add("fetch")
				return nil
			},
		}
	}
	generateRunSummaryFunc = func(context.Context, string, io.Writer) (*summary.RunSummary, error) {
		if !fake.allFailed {
			return nil, nil
		}
		return &summary.RunSummary{Steps: []summary.StepSummary{{
			StepID: "mock-step", PhaseLabel: "Write", Operation: "CREATE",
			Metrics: &summary.PhaseMetrics{FailureCount: 7},
		}}}, nil
	}
	requestShutdownAllFunc = func(context.Context, []*hostparse.HostInfo, string, time.Duration, int64, bool) error {
		fake.add("shutdown")
		fake.shutdownOne.Do(func() { close(fake.shutdown) })
		return nil
	}
	requestRunStopFunc = func(_ context.Context, _ string, runID int64) (bool, error) {
		if runID <= 0 {
			t.Errorf("stop request for run %d, want the owned run", runID)
		}
		fake.add("stop")
		if fake.onStop != nil {
			fake.onStop()
		}
		return fake.stopFound && fake.stopErr == nil, fake.stopErr
	}
}

// autoTerminateLaunch plays the headless runner. Unless a step failed first, it reaches the
// deadline after submission, has the session stop the run, and returns the runner's
// auto-terminate result once the session reports the run terminal, or the interrupt.
func autoTerminateLaunch(t *testing.T, stepFailed bool) func(context.Context, tui.LaunchHooks) error {
	t.Helper()
	monitorReady := awaitMonitorSetup(t)
	return func(ctx context.Context, hooks tui.LaunchHooks) error {
		monitorReady()
		hooks.NotifySubmitted()
		if stepFailed {
			return nil
		}
		if err := hooks.StopWorkloadAndWait(ctx, 10*time.Second); err != nil {
			if ctx.Err() == nil {
				t.Errorf("the session did not report the run terminal after the stop request: %v", err)
			}
			return err
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return &headless.AutoTerminateError{CleanupComplete: true}
	}
}

// runCommandWithin fails the test instead of hanging when a command does not return.
func runCommandWithin(t *testing.T, run func() error) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- run() }()
	select {
	case err := <-done:
		return err
	case <-time.After(30 * time.Second):
		t.Fatal("the command did not return within 30 s")
		return nil
	}
}

const (
	autoTerminateWantExit0       = ""
	autoTerminateWantStepFailure = "step mock-step failed"
	autoTerminateWantUnconfirmed = "auto-terminate: the stop at the deadline was not confirmed"
	autoTerminateWantAllFailed   = "every request of phase Write failed"
	autoTerminateWantInterrupt   = "interrupt"

	autoTerminateClosingStopped = "Auto-terminate: the run was stopped at the 1 s deadline"
	autoTerminateClosingEnded   = "Auto-terminate: the run had already ended at the 1 s deadline"
	autoTerminateMonitorStopped = "Auto-results: run stopped."
)

type autoTerminateCommandCase struct {
	name       string
	shutdownOn bool
	engine     autoTerminateEngine
	stopFound  bool
	stopFails  bool
	allFailed  bool
	ctrlC      bool

	want        string // autoTerminateWant*
	wantEvents  []string
	wantLines   []string
	wantClosing string // "" when no closing line may be printed
}

var autoTerminateCommandCases = []autoTerminateCommandCase{
	{
		name: "stopped", shutdownOn: true, stopFound: true,
		want: autoTerminateWantExit0, wantEvents: []string{"stop", "fetch", "shutdown"},
		wantLines:   []string{"Auto-results: auto-terminate deadline reached; stopping the run...", autoTerminateMonitorStopped},
		wantClosing: autoTerminateClosingStopped,
	},
	{
		name: "stopped without shutdown", stopFound: true,
		want: autoTerminateWantExit0, wantEvents: []string{"stop", "fetch"},
		wantLines: []string{autoTerminateMonitorStopped}, wantClosing: autoTerminateClosingStopped,
	},
	{
		name: "step failed before the deadline", shutdownOn: true, engine: engineStepFailed,
		want: autoTerminateWantStepFailure, wantEvents: []string{"fetch", "shutdown"},
	},
	{
		name: "stop request fails without shutdown", stopFails: true, engine: engineNeverTerminal,
		want: autoTerminateWantUnconfirmed, wantEvents: []string{"stop", "fetch"},
		wantLines: []string{"Auto-results: the run stop request failed; see logs for details."},
	},
	{
		name: "stop request fails and shutdown does not end the run", shutdownOn: true, stopFails: true,
		engine: engineNeverTerminal,
		want:   autoTerminateWantUnconfirmed, wantEvents: []string{"stop", "shutdown", "fetch"},
	},
	{
		name: "stop request fails and shutdown ends the run", shutdownOn: true, stopFails: true,
		engine: engineStopsOnShutdown,
		want:   autoTerminateWantExit0, wantEvents: []string{"stop", "shutdown", "fetch"},
		wantLines: []string{"Auto-results: run stopped by the shutdown."}, wantClosing: autoTerminateClosingStopped,
	},
	{
		name: "stop accepted but never terminal without shutdown", stopFound: true, engine: engineNeverTerminal,
		want: autoTerminateWantUnconfirmed, wantEvents: []string{"stop", "fetch"},
		wantLines: []string{"Auto-results: the run reported no terminal state after the stop request."},
	},
	{
		name: "stop accepted but never terminal with shutdown", shutdownOn: true, stopFound: true,
		engine: engineNeverTerminal,
		want:   autoTerminateWantUnconfirmed, wantEvents: []string{"stop", "shutdown", "fetch"},
	},
	{
		name: "run already ended at the deadline", shutdownOn: true, engine: engineAlreadyCompleted,
		want: autoTerminateWantExit0, wantEvents: []string{"stop", "fetch", "shutdown"},
		wantLines:   []string{"Auto-results: the run had already ended at the auto-terminate deadline."},
		wantClosing: autoTerminateClosingEnded,
	},
	{
		name: "every request failed", shutdownOn: true, stopFound: true, allFailed: true,
		want: autoTerminateWantAllFailed, wantEvents: []string{"stop", "fetch", "shutdown"},
		wantClosing: autoTerminateClosingStopped,
	},
	{
		name: "ctrl-c during the stop", shutdownOn: true, stopFound: true, ctrlC: true,
		want: autoTerminateWantInterrupt, wantEvents: []string{"stop", "fetch", "shutdown"},
	},
}

// autoTerminateCaseForReplay adjusts the cases whose outcome on spt replay differs: replay has no all-failed
// check, so a stopped replay whose every request failed still exits 0.
func autoTerminateCaseForReplay(test autoTerminateCommandCase) autoTerminateCommandCase {
	if test.allFailed {
		test.want = autoTerminateWantExit0
	}
	return test
}

// checkAutoTerminateCommand asserts the exit status, the engine calls and the output.
func checkAutoTerminateCommand(
	t *testing.T, test autoTerminateCommandCase, err error, fake *autoTerminateFake, output string,
	closingCaptured bool,
) {
	t.Helper()
	switch test.want {
	case autoTerminateWantExit0:
		if err != nil {
			t.Errorf("error = %v, want exit 0", err)
		}
	case autoTerminateWantInterrupt:
		if !errors.Is(err, context.Canceled) {
			t.Errorf("error = %v, want the interrupt", err)
		}
	default:
		var exitErr *ExitCodeError
		if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeWorkloadFailure ||
			!strings.Contains(err.Error(), test.want) {
			t.Errorf("error = %v, want exit %d naming %q", err, constants.ExitCodeWorkloadFailure, test.want)
		}
	}
	if events := fake.list(); !slices.Equal(events, test.wantEvents) {
		t.Errorf("engine calls = %v, want %v", events, test.wantEvents)
	}
	for _, line := range test.wantLines {
		if !strings.Contains(output, line) {
			t.Errorf("output lacks %q:\n%s", line, output)
		}
	}
	if closingCaptured {
		for _, line := range []string{autoTerminateClosingStopped, autoTerminateClosingEnded} {
			if want := line == test.wantClosing; strings.Contains(output, line) != want {
				t.Errorf("output has %q = %t, want %t:\n%s", line, !want, want, output)
			}
		}
	}
	if test.want == autoTerminateWantUnconfirmed && strings.Contains(output, autoTerminateMonitorStopped+"\n") {
		t.Errorf("output claims the run stopped:\n%s", output)
	}
	if test.ctrlC {
		fake.mu.Lock()
		budget := fake.fetchBudget
		fake.mu.Unlock()
		if budget <= 0 || budget > autoResultsPhaseBudgets.CancelSalvage {
			t.Errorf("artifact budget after Ctrl-C = %s, want at most the interrupt budget %s",
				budget, autoResultsPhaseBudgets.CancelSalvage)
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

func newAutoTerminateFake(test autoTerminateCommandCase, cancel context.CancelFunc) *autoTerminateFake {
	fake := &autoTerminateFake{engine: test.engine, stopFound: test.stopFound, allFailed: test.allFailed}
	if test.stopFails {
		fake.stopErr = errors.New("stop status 500")
	}
	if test.ctrlC {
		fake.onStop = cancel
	}
	return fake
}

var autoTerminateRoutes = []struct {
	name  string
	hosts []string
}{
	{name: "local"},
	{name: "multi-host", hosts: []string{"entry.example", "worker.example"}},
}

// spt run --auto-terminate-seconds with auto-results stops the run at the deadline and exits 0
// only once the stop is confirmed, on the local and the multi-host route. Before the fix the CLI
// let the step run to its natural end and exited 1.
func TestRunCommandAutoTerminateStopsTheRun(t *testing.T) {
	for _, route := range autoTerminateRoutes {
		for _, test := range autoTerminateCommandCases {
			t.Run(route.name+"/"+test.name, func(t *testing.T) {
				workDir := t.TempDir()
				t.Chdir(workDir)
				hosts, minHosts := "127.0.0.1", "1"
				if len(route.hosts) > 0 {
					hosts, minHosts = strings.Join(route.hosts, ","), "2"
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

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				origCtx := runCmd.Context()
				runCmd.SetContext(ctx)
				t.Cleanup(func() { runCmd.SetContext(origCtx) })
				fake := newAutoTerminateFake(test, cancel)
				installAutoTerminateMonitorSeams(t, fake)
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

				launch := autoTerminateLaunch(t, test.engine == engineStepFailed)
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

				err := runCommandWithin(t, func() error { return runCmd.RunE(runCmd, []string{WorkloadTypeMock}) })
				wantLocal, wantRemote := int32(1), int32(0)
				if len(route.hosts) > 0 {
					wantLocal, wantRemote = 0, 1
				}
				if localLaunches.Load() != wantLocal || remoteLaunches.Load() != wantRemote {
					t.Fatalf("launches local=%d multi-host=%d, want %d/%d",
						localLaunches.Load(), remoteLaunches.Load(), wantLocal, wantRemote)
				}
				// spt run prints its closing line on stdout, which the test doesn't capture.
				checkAutoTerminateCommand(t, test, err, fake, output.String(), false)
			})
		}
	}
}

// spt replay --auto-terminate-seconds stops the replay at the deadline and exits 0 only once the
// stop is confirmed, on the local and the multi-host route, and closes with the auto-terminate
// line only then.
func TestReplayCommandAutoTerminateStopsTheRun(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()
	for _, route := range autoTerminateRoutes {
		for _, test := range autoTerminateCommandCases {
			test := autoTerminateCaseForReplay(test)
			t.Run(route.name+"/"+test.name, func(t *testing.T) {
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
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				fake := newAutoTerminateFake(test, cancel)
				installAutoTerminateMonitorSeams(t, fake)

				launch := autoTerminateLaunch(t, test.engine == engineStepFailed)
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
				cmd.SetContext(ctx)
				output := &lockedBuffer{}
				cmd.SetOut(output)
				cmd.SetErr(io.Discard)
				args := []string{
					"--from", server.URL, "--endpoints", "http://s3.example", "--headless",
					"--results-dir", t.TempDir(), "--auto-terminate-seconds", "1",
					"--shutdown-on-complete=" + boolString(test.shutdownOn),
				}
				if len(route.hosts) > 0 {
					args = append(args, "--test-hosts", strings.Join(route.hosts, ","))
				}
				cmd.SetArgs(args)
				err := runCommandWithin(t, cmd.Execute)
				wantLocal, wantRemote := int32(1), int32(0)
				if len(route.hosts) > 0 {
					wantLocal, wantRemote = 0, 1
				}
				if localLaunches.Load() != wantLocal || remoteLaunches.Load() != wantRemote {
					t.Fatalf("launches local=%d multi-host=%d, want %d/%d",
						localLaunches.Load(), remoteLaunches.Load(), wantLocal, wantRemote)
				}
				checkAutoTerminateCommand(t, test, err, fake, output.String(), true)
			})
		}
	}
}
