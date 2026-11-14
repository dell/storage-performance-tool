package cmd

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

const (
	deleteOutputStep   = "mt-002-20261111.100000.000-delete"
	deleteOutputReason = "Standalone DELETE inventory slice exhausted before the requested duration"
)

func failedDeleteTracker() *portcheck.RunResult {
	return &portcheck.RunResult{
		FinalState: constants.StateFailed, FailureStepID: deleteOutputStep,
		FailureCategory: "execution", FailureMessage: deleteOutputReason,
	}
}

// The presenter's reflection of the engine's FAILED status, as ownedTerminalStatusFailure builds it.
func ownedDeleteReflection() error {
	return &runcontrol.OwnedEngineTerminalFailure{
		Detail: "step " + deleteOutputStep + " (execution): " + deleteOutputReason,
	}
}

// The verdict auto-results reads from the stored DELETE metrics.
func storedDeleteVerdict(t *testing.T, verification bool) error {
	t.Helper()
	metrics := &deletemetrics.Metrics{
		FailurePolicy: deletemetrics.FailurePolicy{Outcome: deletemetrics.OutcomeFailed},
	}
	if verification {
		metrics.Verification = deletemetrics.Verification{AcceptedPresent: 1, CorrectnessFailures: 1}
	}
	verdict := terminalDeleteOutcomeError(map[string]*deletemetrics.Metrics{deleteOutputStep: metrics})
	if verdict == nil {
		t.Fatal("stored DELETE metrics produced no verdict")
	}
	return verdict
}

// The final Error: of a failed DELETE run states the failed step's reason and the DELETE verdict once
// each, on one line, whichever completion signal arrives first and however the presenter's
// completion error is nested.
func TestResolveRunCompletionErrorStatesADeleteFailureOnceWhicheverSignalWins(t *testing.T) {
	policy := &runcontrol.DeleteTerminalPolicyFailure{StepID: deleteOutputStep}
	verification := &runcontrol.DeleteTerminalPolicyFailure{StepID: deleteOutputStep, Verification: true}
	owned := ownedDeleteReflection()
	tickJoin := errors.Join(policy, owned)
	for _, test := range []struct {
		name         string
		runErr       error
		verdict      error
		verdictText  string
		verdictCount int
	}{
		{name: "tracker wins with no presenter error",
			verdict: storedDeleteVerdict(t, false), verdictText: "failure policy rejected", verdictCount: 1},
		{name: "single-host status tick: policy and owned reflections", runErr: tickJoin,
			verdict: storedDeleteVerdict(t, false), verdictText: "failure policy rejected", verdictCount: 1},
		{name: "multi-host metrics poll: policy reflection alone", runErr: policy,
			verdict: storedDeleteVerdict(t, false), verdictText: "failure policy rejected", verdictCount: 1},
		{name: "multi-host entry status: the tick's join under the headless join",
			runErr:  errors.Join(nil, errors.Join(policy, owned)),
			verdict: storedDeleteVerdict(t, false), verdictText: "failure policy rejected", verdictCount: 1},
		{name: "FAILED DELETE with no failed policy: owned reflection joined", runErr: errors.Join(nil, owned),
			verdictText: "failure policy rejected", verdictCount: 0},
		{name: "failed terminal-metrics capture: bare owned reflection", runErr: owned,
			verdictText: "failure policy rejected", verdictCount: 0},
		{name: "verification verdict", runErr: errors.Join(verification, owned),
			verdict: storedDeleteVerdict(t, true), verdictText: "verification failed", verdictCount: 1},
		{name: "full-screen UI nests the tick's join", runErr: errors.Join(nil, nil, tickJoin),
			verdict: storedDeleteVerdict(t, false), verdictText: "failure policy rejected", verdictCount: 1},
		{name: "full-screen UI nests the verification join",
			runErr:  errors.Join(nil, nil, errors.Join(verification, owned)),
			verdict: storedDeleteVerdict(t, true), verdictText: "verification failed", verdictCount: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			outcome := autoResultsOutcome{Tracker: failedDeleteTracker(), DeleteTerminalErr: test.verdict}
			err := resolveRunCompletionError(
				test.runErr, outcome, true, scenario.Params{WorkloadType: scenario.WorkloadTypeDelete})
			var exitErr *ExitCodeError
			if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeWorkloadFailure {
				t.Fatalf("error = %#v, want exit %d", err, constants.ExitCodeWorkloadFailure)
			}
			msg := err.Error()
			t.Logf("Error: %s", msg)
			want := "step " + deleteOutputStep + " failed (execution): " + deleteOutputReason
			if !strings.HasPrefix(msg, want) {
				t.Fatalf("Error: should lead with the failed step:\n%s", msg)
			}
			if got := strings.Count(msg, deleteOutputReason); got != 1 {
				t.Fatalf("step reason appears %d times, want once:\n%s", got, msg)
			}
			if got := strings.Count(msg, test.verdictText); got != test.verdictCount {
				t.Fatalf("%q appears %d times, want %d:\n%s", test.verdictText, got, test.verdictCount, msg)
			}
			if strings.Contains(msg, "\n") || strings.Contains(msg, "run also failed") {
				t.Fatalf("Error: repeats the presenter's failure:\n%s", msg)
			}
			if test.verdict != nil && !errors.Is(err, test.verdict) {
				t.Fatalf("cause lost the stored verdict: %#v", err)
			}
			if test.runErr == nil {
				return
			}
			if !errors.Is(err, test.runErr) {
				t.Fatalf("cause lost the presenter error: %#v", err)
			}
			var ownedCause *runcontrol.OwnedEngineTerminalFailure
			if errors.As(test.runErr, &ownedCause) && !errors.As(err, &ownedCause) {
				t.Fatalf("cause lost the owned reflection: %#v", err)
			}
			var policyCause *runcontrol.DeleteTerminalPolicyFailure
			if errors.As(test.runErr, &policyCause) && !errors.As(err, &policyCause) {
				t.Fatalf("cause lost the presenter verdict: %#v", err)
			}
		})
	}
}

func TestResolveRunCompletionErrorKeepsDeleteFailuresTheReasonsDoNotState(t *testing.T) {
	params := scenario.Params{WorkloadType: scenario.WorkloadTypeDelete}
	policy := &runcontrol.DeleteTerminalPolicyFailure{StepID: deleteOutputStep}
	owned := ownedDeleteReflection()
	stopFailure := errors.New("stop containers on root@worker.example: connection reset")
	shutdownFailure := errors.New("request shutdown: context deadline exceeded")
	presenterPolicy := `run also failed: DELETE failure policy rejected the terminal outcome for step "` +
		deleteOutputStep + `"`

	for _, test := range []struct {
		name     string
		runErr   error
		outcome  autoResultsOutcome
		params   scenario.Params
		wantTail string
	}{
		{
			name:     "an unrelated failure joined to the tick's reflections",
			runErr:   errors.Join(policy, owned, stopFailure),
			outcome:  autoResultsOutcome{Tracker: failedDeleteTracker(), DeleteTerminalErr: storedDeleteVerdict(t, false)},
			params:   params,
			wantTail: "; run also failed: " + stopFailure.Error(),
		},
		{
			name:     "a full-screen UI stop failure beside the nested reflections",
			runErr:   errors.Join(nil, stopFailure, errors.Join(policy, owned)),
			outcome:  autoResultsOutcome{Tracker: failedDeleteTracker(), DeleteTerminalErr: storedDeleteVerdict(t, false)},
			params:   params,
			wantTail: "; run also failed: " + stopFailure.Error(),
		},
		{
			name:     "two unrelated failures share one line",
			runErr:   errors.Join(owned, stopFailure, shutdownFailure),
			outcome:  autoResultsOutcome{Tracker: failedDeleteTracker()},
			params:   params,
			wantTail: "; run also failed: " + stopFailure.Error() + "; " + shutdownFailure.Error(),
		},
		{
			name:    "a wrapper's own text is kept whole",
			runErr:  fmt.Errorf("stop presentation: %w; %w", owned, stopFailure),
			outcome: autoResultsOutcome{Tracker: failedDeleteTracker()},
			params:  params,
			wantTail: "; run also failed: stop presentation: owned engine run failed: step " + deleteOutputStep +
				" (execution): " + deleteOutputReason + "; " + stopFailure.Error(),
		},
		{
			name:    "a single wrapper's own text is kept whole",
			runErr:  errors.Join(policy, fmt.Errorf("stop presentation: %w", owned)),
			outcome: autoResultsOutcome{Tracker: failedDeleteTracker(), DeleteTerminalErr: storedDeleteVerdict(t, false)},
			params:  params,
			wantTail: "; run also failed: stop presentation: owned engine run failed: step " + deleteOutputStep +
				" (execution): " + deleteOutputReason,
		},
		{
			name:   "the presenter verdict without a stored verdict",
			runErr: errors.Join(policy, owned),
			outcome: autoResultsOutcome{
				Tracker:          failedDeleteTracker(),
				DeleteMetricsErr: errors.New("capture terminal DELETE metrics: expected step is missing"),
			},
			params:   params,
			wantTail: "; " + presenterPolicy,
		},
		{
			name:   "the owned reflection of a failed seeded cleanup",
			runErr: errors.Join(policy, &runcontrol.OwnedEngineTerminalFailure{Detail: "cleanup partially failed"}),
			outcome: autoResultsOutcome{
				Tracker: &portcheck.RunResult{
					FinalState: constants.StateFailed, FailureStepID: "mt-003-20261111.100000.000-cleanup",
					FailureCategory: "execution", FailureMessage: "cleanup partially failed",
				},
				DeleteTerminalErr: storedDeleteVerdict(t, false),
			},
			params:   scenario.Params{WorkloadType: scenario.WorkloadTypeDelete, Cleanup: true},
			wantTail: "; run also failed: owned engine run failed: cleanup partially failed",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := resolveRunCompletionError(test.runErr, test.outcome, true, test.params)
			var exitErr *ExitCodeError
			if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeWorkloadFailure {
				t.Fatalf("error = %#v, want exit %d", err, constants.ExitCodeWorkloadFailure)
			}
			msg := err.Error()
			t.Logf("Error: %s", msg)
			if !strings.HasSuffix(msg, test.wantTail) || strings.Contains(msg, "\n") {
				t.Fatalf("Error: = %q, want one line ending %q", msg, test.wantTail)
			}
			if strings.Count(msg, "run also failed") != 1 {
				t.Fatalf("Error: = %q, want one run also failed", msg)
			}
			if !errors.Is(err, test.runErr) {
				t.Fatalf("cause lost the presenter error: %#v", err)
			}
		})
	}
}

func TestResolveRunCompletionErrorReturnsTheTypedPresenterVerdictWithoutAutoResults(t *testing.T) {
	presenterFailure := errors.Join(
		&runcontrol.DeleteTerminalPolicyFailure{StepID: deleteOutputStep}, ownedDeleteReflection())
	got := resolveRunCompletionError(
		presenterFailure, autoResultsOutcome{}, false,
		scenario.Params{WorkloadType: scenario.WorkloadTypeDelete})
	if got != presenterFailure {
		t.Fatalf("DELETE presenter error = %v, want original %v", got, presenterFailure)
	}
}
