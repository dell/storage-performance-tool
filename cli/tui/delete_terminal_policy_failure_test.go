package tui

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
)

const (
	policyTestStep   = "mt-002-20261111.100000.000-delete"
	policyTestReason = "Standalone DELETE inventory slice exhausted before the requested duration"
)

var deleteVerdicts = []struct {
	name         string
	verification bool
	text         string
}{
	{name: "failure policy", text: `DELETE failure policy rejected the terminal outcome for step "` + policyTestStep + `"`},
	{name: "verification", verification: true, text: `DELETE verification failed for step "` + policyTestStep + `"`},
}

func rejectDeleteOutcome(metric *DeleteMetrics, verification bool) {
	metric.FailurePolicy.Outcome = deletemetrics.OutcomeFailed
	if verification {
		failDeleteVerification(metric)
	}
}

// failDeleteVerification records one accepted object that full inventory verification still found
// present, in the shape the DELETE metrics contract accepts.
func failDeleteVerification(metric *DeleteMetrics) {
	metric.Verification = DeleteVerificationMetrics{
		Enabled: true, PreValidationEnabled: true, PostVerificationEnabled: true,
		PreValidationComplete: true, PostVerificationComplete: true, TimeoutSeconds: 30,
		StillPresent: 1, AcceptedPresent: 1, CorrectnessFailures: 1, Residual: 1,
		Notice: "Full inventory validation and verification classifications are reported.",
	}
	metric.Phases.PreValidationSeconds = float64Pointer(0.25)
	metric.Phases.PostVerificationSeconds = float64Pointer(0.5)
}

func requireDeletePolicyLeaf(t *testing.T, err error, verification bool, text string) {
	t.Helper()
	var failure *runcontrol.DeleteTerminalPolicyFailure
	if !errors.As(err, &failure) {
		t.Fatalf("DELETE verdict %T %q is not a runcontrol.DeleteTerminalPolicyFailure", err, err)
	}
	if failure.StepID != policyTestStep || failure.Verification != verification || failure.Error() != text {
		t.Fatalf("DELETE verdict = %+v %q, want step %q verification %t %q",
			failure, failure.Error(), policyTestStep, verification, text)
	}
}

// Both returns of terminalDeletePolicyFailure are the typed reflection, with their text unchanged,
// for the aggregated metric and for the per-op DELETE metric it falls back to.
func TestTerminalDeletePolicyFailureIsTypedForBothVerdicts(t *testing.T) {
	for _, verdict := range deleteVerdicts {
		for _, shape := range []string{"aggregated", "per-op DELETE"} {
			t.Run(verdict.name+"/"+shape, func(t *testing.T) {
				metric := deleteNodeMetric("single", 1, 1, 1)
				metric.StepID = policyTestStep
				metric.TestState = constants.TestStateCompleted
				rejectDeleteOutcome(metric.Delete, verdict.verification)
				update := &MultiNodeMetricsUpdate{Aggregated: metric}
				if shape == "per-op DELETE" {
					update = &MultiNodeMetricsUpdate{
						Aggregated: &PerformanceMetric{OpType: "MIXED"},
						PerOpType:  map[string]*PerformanceMetric{"DELETE": metric},
					}
				}
				requireDeletePolicyLeaf(t, terminalDeletePolicyFailure(update), verdict.verification, verdict.text)
			})
		}
	}
}

func rejectedDeleteSteps(t *testing.T, verification bool) (node, fleet JSONMetricsStep) {
	t.Helper()
	node = newTestStep()
	node.MetricsSchema = deletemetrics.SchemaVersion
	node.RunID = "41"
	node.Scope = "node"
	node.Role = "worker"
	node.StepID = policyTestStep
	node.OpType = "DELETE"
	node.DeleteDetailExpected = true
	node.TestState = constants.TestStateCompleted
	node.Operations.SuccessCount = 1
	node.Delete = testDeleteMetrics("single", 1, 1, 1)
	node.Delete.Buckets = []DeleteBucketMetrics{{Bucket: "bucket-a", Selected: 1, Attempted: 1, Accepted: 1}}
	node.Delete.Timing.Latency = testTimingStat(1, 7)
	node.Delete.Timing.Duration = testTimingStat(1, 11)
	node.Delete.FailurePolicy.Outcome = deletemetrics.OutcomeRunning
	if verification {
		// The node metric carries verification; the fleet metric adds only the policy verdict.
		failDeleteVerification(node.Delete)
	}
	fleet = node
	fleet.Scope = "fleet"
	fleet.Role = aggregateLabel
	fleet.NodesCount = 1
	fleet.NodesPresent = []string{constants.MetricsLocalContributorID}
	fleet.ContributorsPresent = []string{constants.MetricsLocalContributorID}
	fleetDelete := *node.Delete
	fleet.Delete = &fleetDelete
	fleet.Delete.FailurePolicy.Outcome = deletemetrics.OutcomeFailed
	return node, fleet
}

func failedDeleteStatus() []byte {
	return []byte(`{"state":"FAILED","run_id":41,"step_id":"` + policyTestStep + `","category":"execution",` +
		`"message":"` + policyTestReason + `"}`)
}

func requireTypedDeleteCompletion(t *testing.T, err error, verification bool, text string) {
	t.Helper()
	joined, ok := err.(interface{ Unwrap() []error })
	if !ok {
		t.Fatalf("completion error %T %q is not the presenter's join", err, err)
	}
	leaves := joined.Unwrap()
	if len(leaves) != 2 {
		t.Fatalf("completion error leaves = %d %q, want the verdict and the owned reflection", len(leaves), err)
	}
	requireDeletePolicyLeaf(t, leaves[0], verification, text)
	var owned *runcontrol.OwnedEngineTerminalFailure
	if !errors.As(leaves[1], &owned) || !strings.Contains(owned.Error(), policyTestReason) {
		t.Fatalf("second leaf = %T %q, want the owned FAILED reflection", leaves[1], leaves[1])
	}
}

// The single-host presenter's status tick records the typed verdict beside its FAILED reflection.
func TestSingleHostStatusTickRecordsTheTypedDeleteVerdict(t *testing.T) {
	for _, verdict := range deleteVerdicts {
		t.Run(verdict.name, func(t *testing.T) {
			node, fleet := rejectedDeleteSteps(t, verdict.verification)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/status":
					_, _ = w.Write(failedDeleteStatus())
				case constants.SptMetricsEndpoint:
					_, _ = w.Write([]byte(marshalSteps(t, []JSONMetricsStep{node})))
				case constants.SptFleetMetricsEndpoint:
					_, _ = w.Write([]byte(marshalSteps(t, []JSONMetricsStep{fleet})))
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()

			orchestrator := NewTestOrchestrator(nil, constants.SptAPIPort, "")
			orchestrator.apiClient = NewSptAPIClient(server.URL)
			orchestrator.apiClient.SetRunID("41")
			orchestrator.statusInterval = 5 * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			go orchestrator.monitorStatus(ctx)
			select {
			case <-orchestrator.CompletionCh():
			case <-ctx.Done():
				t.Fatal("the FAILED status tick did not complete presentation")
			}
			requireTypedDeleteCompletion(t, orchestrator.CompletionError(), verdict.verification, verdict.text)
		})
	}
}

// The distributed presenter's entry-status path records the typed verdict beside its FAILED
// reflection when the entry engine fails before the metrics show completion.
func TestDistributedEntryFailureRecordsTheTypedDeleteVerdict(t *testing.T) {
	for _, verdict := range deleteVerdicts {
		t.Run(verdict.name, func(t *testing.T) {
			node, _ := rejectedDeleteSteps(t, verdict.verification)
			node.TestState = constants.TestStateRunning
			rejectDeleteOutcome(node.Delete, verdict.verification)
			payload := marshalSteps(t, []JSONMetricsStep{node})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Path == "/status" {
					_, _ = w.Write(failedDeleteStatus())
					return
				}
				_, _ = w.Write([]byte(payload))
			}))
			defer server.Close()

			orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{{Original: "entry", Host: "entry"}}, 1)
			orchestrator.hosts[0].SetStatus(HostStatusRunning)
			orchestrator.hosts[0].APIClient = NewSptAPIClient(server.URL)
			orchestrator.hosts[0].APIClient.SetRunID("41")
			wrapper := NewMultiHostTestOrchestrator(orchestrator)
			wrapper.pollInterval = 5 * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			wrapper.StartMetricsPolling(ctx)
			defer wrapper.StopMetricsPolling()
			select {
			case <-wrapper.CompletionCh():
			case <-ctx.Done():
				t.Fatal("the entry engine's FAILED status did not complete presentation")
			}
			requireTypedDeleteCompletion(t, wrapper.CompletionError(), verdict.verification, verdict.text)
		})
	}
}
