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
)

const genericSetupFailure = "target bucket bench does not exist and target-bucket provisioning is disabled"

func failedSetupTracker() *portcheck.RunResult {
	return &portcheck.RunResult{
		FinalState: constants.StateFailed, FailureStepID: "mt-001-create",
		FailureCategory: "execution", FailureMessage: genericSetupFailure,
	}
}

func TestResolveRunCompletionErrorFailsOrdinaryWorkloadOnEngineFailure(t *testing.T) {
	for _, workloadType := range []string{
		WorkloadTypeWrite, WorkloadTypeRead, WorkloadTypeMixed, WorkloadTypeList,
	} {
		t.Run(workloadType, func(t *testing.T) {
			outcome := autoResultsOutcome{
				Tracker:     failedSetupTracker(),
				ArtifactErr: errors.New("no steps reported by metrics/json"),
			}
			err := resolveRunCompletionError(nil, outcome, true, scenario.Params{WorkloadType: workloadType})
			var exitErr *ExitCodeError
			if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeWorkloadFailure {
				t.Fatalf("completion error = %#v, want workload failure exit", err)
			}
			want := "step mt-001-create failed (execution): " + genericSetupFailure
			if exitErr.Msg != want {
				t.Fatalf("message = %q, want %q", exitErr.Msg, want)
			}
		})
	}
}

func TestResolveRunCompletionErrorKeepsIndependentPresenterFailure(t *testing.T) {
	presenter := errors.New("tui crashed")
	err := resolveRunCompletionError(
		presenter, autoResultsOutcome{Tracker: failedSetupTracker()}, true,
		scenario.Params{WorkloadType: WorkloadTypeWrite})
	if !errors.Is(err, presenter) || !strings.Contains(err.Error(), "run also failed: tui crashed") ||
		!strings.HasPrefix(err.Error(), "step mt-001-create failed (execution)") {
		t.Fatalf("completion error = %v, want engine cause first and presenter failure kept", err)
	}

	owned := &runcontrol.OwnedEngineTerminalFailure{Detail: genericSetupFailure}
	err = resolveRunCompletionError(
		owned, autoResultsOutcome{Tracker: failedSetupTracker()}, true,
		scenario.Params{WorkloadType: WorkloadTypeWrite})
	if strings.Contains(err.Error(), "run also failed") {
		t.Fatalf("completion error = %v repeats the presenter's reflection of the same failure", err)
	}
}

func TestResolveRunCompletionErrorLeavesSuccessfulOrdinaryRunsAlone(t *testing.T) {
	params := scenario.Params{WorkloadType: WorkloadTypeWrite}
	completed := autoResultsOutcome{Tracker: &portcheck.RunResult{FinalState: constants.StateCompleted}}
	if err := resolveRunCompletionError(nil, completed, true, params); err != nil {
		t.Fatalf("completed run error = %v", err)
	}
	if err := resolveRunCompletionError(nil, autoResultsOutcome{}, false, params); err != nil {
		t.Fatalf("untracked run error = %v", err)
	}
	if got := trackedFailureReason(&portcheck.RunResult{FinalState: constants.StateFailed}); got != "step failed: engine reported FAILED" {
		t.Fatalf("unattributed reason = %q", got)
	}
}

func TestRunCmdWriteFailedTrackerReturnsWorkloadFailureEveryTopology(t *testing.T) {
	previousValidate := validateRunWorkloadTypeFunc
	previousPort := resolvePortConflictFunc
	previousConnect := connectMultiHostOrchestratorFunc
	previousLocal := startLocalHeadlessRunFunc
	previousMulti := startMultiHostHeadlessRunFunc
	previousAutoResults := startAutoResultsFunc
	t.Cleanup(func() {
		validateRunWorkloadTypeFunc = previousValidate
		resolvePortConflictFunc = previousPort
		connectMultiHostOrchestratorFunc = previousConnect
		startLocalHeadlessRunFunc = previousLocal
		startMultiHostHeadlessRunFunc = previousMulti
		startAutoResultsFunc = previousAutoResults
	})
	validateRunWorkloadTypeFunc = func(string) error { return nil }
	resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
		return &portcheck.ResolutionResult{Success: true}, nil
	}
	connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error {
		return nil
	}
	startLocalHeadlessRunFunc = func(
		_ string, _ string, _ scenario.Params, options headless.HeadlessOptions,
	) error {
		options.LaunchHooks.NotifySubmitted()
		return nil
	}
	startMultiHostHeadlessRunFunc = func(
		_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params,
		options headless.HeadlessOptions,
	) error {
		options.LaunchHooks.NotifySubmitted()
		return nil
	}
	startAutoResultsFunc = func(
		_ context.Context, _ string, _ string, _ string, _ []string, _ int64, _ bool,
		_ []*hostparse.HostInfo, _ string, _ bool, _ int, _ string, metadata *runMetadata,
		_ io.Writer, _ io.Writer, _ string, _ func(context.Context),
		_ ...*integrity.FinalizeOptions,
	) *autoResultsMonitor {
		monitor := &autoResultsMonitor{
			done: make(chan autoResultsOutcome, 1), armed: make(chan struct{}),
		}
		go func() {
			<-monitor.armed
			outcome := autoResultsOutcome{Tracker: failedSetupTracker()}
			outcome.Lifecycle.PreparedInputs = cleanupPreparedForMonitorTest(metadata)
			monitor.done <- outcome
		}()
		return monitor
	}

	for _, topology := range []struct {
		name, hosts, minHosts string
	}{
		{name: "local", hosts: "127.0.0.1", minHosts: "1"},
		{name: "one-remote-host", hosts: "remote.example", minHosts: "1"},
		{name: "distributed", hosts: "entry.example,worker.example", minHosts: "2"},
	} {
		t.Run(topology.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			for name, value := range map[string]string{
				"test-hosts": topology.hosts, "min-hosts": topology.minHosts,
				"endpoints": "http://s3.example", "access-key": "access", "secret-key": "secret",
				"bucket": "bench", "object-size": "1KiB", "object-count": "1", "duration": "",
				"threads": "1", "headless": "true", "auto-results": "true",
				"shutdown-on-complete": "false", "generate-only": "false", "items-file": "",
			} {
				setGlobalRunFlagForTest(t, name, value)
			}

			err := runCmd.RunE(runCmd, []string{WorkloadTypeWrite})
			var exitErr *ExitCodeError
			if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeWorkloadFailure ||
				!strings.Contains(err.Error(), genericSetupFailure) {
				t.Fatalf("RunE() error = %#v, want workload failure preserving %q", err, genericSetupFailure)
			}
		})
	}
}
