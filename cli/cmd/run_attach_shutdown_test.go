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

// attachShutdownTracker plays the engine's run tracker. A normal run completes at once. An
// interrupted run's tracking waits for the cancellation and fails with it; the reconciliation
// that the interrupted shutdown starts then sees the run complete.
type attachShutdownTracker struct {
	interrupt bool
	calls     *atomic.Int32
}

func (f *attachShutdownTracker) WaitForCompletion(ctx context.Context, _ []string) (*portcheck.RunResult, error) {
	if f.calls.Add(1) == 1 && f.interrupt {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &portcheck.RunResult{FinalState: constants.StateCompleted, RunID: 41}, nil
}

func (f *attachShutdownTracker) SetDebug(bool)                {}
func (f *attachShutdownTracker) SetExpectedRunID(int64)       {}
func (f *attachShutdownTracker) SetRequireTerminalState(bool) {}

// installAttachShutdownSeams keeps the shipped auto-results monitor off the network and
// records the hosts of each end-of-run shutdown request at requestShutdownAllFunc. The tests
// prove the host list the monitor hands to requestShutdownAll, which posts /shutdown only to
// the hosts it is given; they send no HTTP requests themselves.
func installAttachShutdownSeams(t *testing.T, interrupt bool) (shutdowns func() [][]string, trackerCalls *atomic.Int32) {
	t.Helper()
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	origShutdown := requestShutdownAllFunc
	t.Cleanup(func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
		requestShutdownAllFunc = origShutdown
	})
	trackerCalls = &atomic.Int32{}
	newRunTrackerFunc = func(string) autoResultsRunTracker {
		return &attachShutdownTracker{interrupt: interrupt, calls: trackerCalls}
	}
	discoverStepIDsFunc = func(context.Context, string, int64) ([]string, error) { return nil, nil }
	discoverFleetStepIDsFunc = func(context.Context, string, int64) ([]string, error) { return nil, nil }
	newResultsFetcherFunc = func(_, output string) autoResultsFetcher {
		return &fakeFetcher{output: output}
	}
	generateRunSummaryFunc = func(context.Context, string, io.Writer, summary.RunEnd) (*summary.RunSummary, error) {
		return nil, nil
	}
	var mu sync.Mutex
	var requests [][]string
	requestShutdownAllFunc = func(_ context.Context, hosts []*hostparse.HostInfo, _ string, _ time.Duration, _ int64, _ bool) error {
		names := make([]string, 0, len(hosts))
		for _, host := range hosts {
			names = append(names, host.Host)
		}
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, names)
		return nil
	}
	return func() [][]string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(requests)
	}, trackerCalls
}

// attachShutdownLaunch returns a launch stub that submits the run. An interrupted launch then
// cancels the command's context, as Ctrl-C does, and returns the cancellation.
func attachShutdownLaunch(t *testing.T, interrupt bool, cancel context.CancelFunc) func(context.Context, tui.LaunchHooks) error {
	t.Helper()
	return func(ctx context.Context, hooks tui.LaunchHooks) error {
		hooks.NotifySubmitted()
		if !interrupt {
			return nil
		}
		cancel()
		<-ctx.Done()
		return ctx.Err()
	}
}

type attachShutdownCase struct {
	name      string
	attach    bool
	interrupt bool
}

var attachShutdownCases = []attachShutdownCase{
	{name: "attach normal", attach: true},
	{name: "attach interrupted", attach: true, interrupt: true},
	{name: "managed workers normal"},
	{name: "managed workers interrupted", interrupt: true},
}

// checkAttachShutdown asserts one end-of-run shutdown request: to the entry node only in attach
// mode, where the workers were started by someone else, and to every host otherwise. An
// interrupted run's shutdown is the one that reconciles the stopped run.
func checkAttachShutdown(
	t *testing.T, test attachShutdownCase, hosts []string, requests [][]string, trackerCalls int32, output string,
) {
	t.Helper()
	// The progress line names no host set, since an attach run asks the entry only.
	if !strings.Contains(output, "Auto-results: requesting shutdown...") || strings.Contains(output, "all hosts") {
		t.Errorf("shutdown progress output:\n%s\nwant \"Auto-results: requesting shutdown...\" without \"all hosts\"", output)
	}
	want := hosts
	if test.attach {
		want = hosts[:1]
	}
	if len(requests) != 1 || !slices.Equal(requests[0], want) {
		t.Fatalf("shutdown requests = %v, want one to %v", requests, want)
	}
	if test.interrupt && trackerCalls != 2 {
		t.Fatalf("tracker calls = %d, want the interrupted tracking and the shutdown's reconciliation", trackerCalls)
	}
}

// spt run with --attach-existing asks only the entry node to shut down at the end of a run,
// normal or interrupted, so no /shutdown reaches a prestarted worker. Without attach mode every
// host still gets one.
func TestRunCommandAttachShutdownReachesEntryOnly(t *testing.T) {
	hosts := []string{"entry.example", "worker.example"}
	for _, test := range attachShutdownCases {
		t.Run(test.name, func(t *testing.T) {
			workDir := t.TempDir()
			t.Chdir(workDir)
			setRunEngineInfoFlags(t, strings.Join(hosts, ","), "2", "", false, true)
			setGlobalRunFlagForTest(t, "shutdown-on-complete", "true")
			setGlobalRunFlagForTest(t, "results-dir", filepath.Join(workDir, "results"))
			setGlobalRunFlagForTest(t, "label", "attach")
			setGlobalRunFlagForTest(t, flagAttachExistingWorkers, boolString(test.attach))
			restore := installRunEngineInfoCommandSeams(t)
			defer restore()
			shouldRunHeadlessForRun = func(*cobra.Command) bool { return true }
			evaluateEngineIdentityGate = func(
				ctx context.Context, _ engineinfo.FleetCollector, descriptors []engineinfo.ParticipantDescriptor, force bool,
			) (engineinfo.GateOutcome, error) {
				return engineinfo.EvaluateGate(ctx, fixedFleetCollector{
					fleet: runGateFleet(engineinfo.ConsistencyConsistent, engineinfo.StatusCollected),
				}, descriptors, force)
			}
			shutdowns, trackerCalls := installAttachShutdownSeams(t, test.interrupt)
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

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			origCtx := runCmd.Context()
			runCmd.SetContext(ctx)
			t.Cleanup(func() { runCmd.SetContext(origCtx) })
			launch := attachShutdownLaunch(t, test.interrupt, cancel)
			var launches atomic.Int32
			startMultiHostHeadlessRunFunc = func(
				orchestrator *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions,
			) error {
				launches.Add(1)
				if orchestrator.AttachExistingWorkersEnabled() != test.attach {
					t.Errorf("orchestrator attach mode = %t, want %t", orchestrator.AttachExistingWorkersEnabled(), test.attach)
				}
				return launch(options.Context, options.LaunchHooks)
			}

			err := runCmd.RunE(runCmd, []string{WorkloadTypeMock})
			if test.interrupt {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("RunE() error = %v, want the cancellation", err)
				}
			} else if err != nil {
				t.Fatalf("RunE() error = %v", err)
			}
			if launches.Load() != 1 {
				t.Fatalf("distributed launches = %d, want 1", launches.Load())
			}
			checkAttachShutdown(t, test, hosts, shutdowns(), trackerCalls.Load(), output.String())
		})
	}
}

// spt replay --attach-existing shares the run's shutdown and asks only the entry node to shut
// down at the end of a replay, normal or interrupted.
func TestReplayCommandAttachShutdownReachesEntryOnly(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()
	hosts := []string{"qa-entry.example", "qa-worker.example"}
	for _, test := range attachShutdownCases {
		t.Run(test.name, func(t *testing.T) {
			origPort := resolvePortConflictFunc
			origConnect := connectReplayOrchestrator
			origConfirm := confirmReplayLaunchCommand
			origRemoteHeadless := startReplayRemoteHeadless
			origShouldHeadless := shouldReplayRunHeadless
			origEvaluate := evaluateEngineIdentityGate
			origLocalPlan := replayLocalEnginePlan
			origDistributedPlan := replayDistributedEnginePlan
			t.Cleanup(func() {
				resolvePortConflictFunc = origPort
				connectReplayOrchestrator = origConnect
				confirmReplayLaunchCommand = origConfirm
				startReplayRemoteHeadless = origRemoteHeadless
				shouldReplayRunHeadless = origShouldHeadless
				evaluateEngineIdentityGate = origEvaluate
				replayLocalEnginePlan = origLocalPlan
				replayDistributedEnginePlan = origDistributedPlan
			})
			resolvePortConflictFunc = func(context.Context, string, portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			connectReplayOrchestrator = func(context.Context, *tui.MultiHostOrchestrator) error { return nil }
			confirmReplayLaunchCommand = func(io.Writer) error { return nil }
			shouldReplayRunHeadless = func(*cobra.Command) bool { return true }
			replayLocalEnginePlan = func(string) ([]engineinfo.ParticipantDescriptor, error) { return nil, nil }
			replayDistributedEnginePlan = func(*tui.MultiHostOrchestrator, string, bool) ([]engineinfo.ParticipantDescriptor, error) {
				return nil, nil
			}
			evaluateEngineIdentityGate = func(
				ctx context.Context, _ engineinfo.FleetCollector, descriptors []engineinfo.ParticipantDescriptor, force bool,
			) (engineinfo.GateOutcome, error) {
				return engineinfo.EvaluateGate(ctx, fixedFleetCollector{
					fleet: runGateFleet(engineinfo.ConsistencyConsistent, engineinfo.StatusCollected),
				}, descriptors, force)
			}
			shutdowns, trackerCalls := installAttachShutdownSeams(t, test.interrupt)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			launch := attachShutdownLaunch(t, test.interrupt, cancel)
			var launches atomic.Int32
			startReplayRemoteHeadless = func(
				orchestrator *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions,
				_ []byte, _ []byte,
			) error {
				launches.Add(1)
				if orchestrator.AttachExistingWorkersEnabled() != test.attach {
					t.Errorf("orchestrator attach mode = %t, want %t", orchestrator.AttachExistingWorkersEnabled(), test.attach)
				}
				return launch(options.Context, options.LaunchHooks)
			}

			cmd := newReplayCommandForTest(t)
			cmd.SetContext(ctx)
			output := &lockedBuffer{}
			cmd.SetOut(output)
			cmd.SetErr(io.Discard)
			args := []string{
				"--from", server.URL, "--endpoints", "http://s3.example", "--headless",
				"--test-hosts", strings.Join(hosts, ","), "--results-dir", t.TempDir(),
			}
			if test.attach {
				args = append(args, "--"+flagAttachExistingWorkers)
			}
			cmd.SetArgs(args)
			err := cmd.Execute()
			if test.interrupt {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("Execute() error = %v, want the cancellation", err)
				}
			} else if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if launches.Load() != 1 {
				t.Fatalf("distributed launches = %d, want 1", launches.Load())
			}
			checkAttachShutdown(t, test, hosts, shutdowns(), trackerCalls.Load(), output.String())
		})
	}
}
