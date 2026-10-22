package tui

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
)

// A run that fails during setup, before any slice starts, publishes no metrics.
// The engine's FAILED cause must reach the completion error unchanged.
func TestFailedRunWithoutMetricsKeepsEngineCause(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/status":
			_, _ = w.Write([]byte(`{"state":"FAILED","step_id":"create-step","category":"execution",` +
				`"message":"target bucket bench does not exist and target-bucket provisioning is disabled"}`))
		case constants.SptMetricsEndpoint, constants.SptFleetMetricsEndpoint:
			_, _ = w.Write([]byte(`[]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	orchestrator := NewTestOrchestrator(nil, constants.SptAPIPort, "")
	orchestrator.apiClient = NewSptAPIClient(server.URL)
	orchestrator.statusInterval = 5 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go orchestrator.monitorStatus(ctx)

	select {
	case <-orchestrator.CompletionCh():
	case <-ctx.Done():
		t.Fatal("failed run without metrics did not finish orchestration")
	}
	err := orchestrator.CompletionError()
	var owned *runcontrol.OwnedEngineTerminalFailure
	if !errors.As(err, &owned) {
		t.Fatalf("completion error = %v, want the owned engine failure", err)
	}
	want := "owned engine run failed: step create-step (execution): target bucket bench does not exist"
	if !strings.HasPrefix(err.Error(), want) {
		t.Fatalf("completion error = %q, want prefix %q", err.Error(), want)
	}
	if strings.Contains(err.Error(), "metrics") {
		t.Fatalf("completion error %q reports missing metrics instead of the engine cause", err.Error())
	}
}

func TestStatusParsesFailureStepAndCategory(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"state":"FAILED","run_id":7,"step_id":" create-step ","category":"configuration","message":"m"}`))
	}))
	defer server.Close()

	status, err := NewSptAPIClient(server.URL).GetStatus()
	if err != nil {
		t.Fatalf("GetStatus: %v", err)
	}
	if status.StepID != "create-step" || status.Category != "configuration" || status.RunID != "7" {
		t.Fatalf("status = %+v, want step create-step, category configuration, run 7", status)
	}
}

func TestOwnedTerminalStatusFailureOmitsMissingAttribution(t *testing.T) {
	err := ownedTerminalStatusFailure(&TestStatus{State: constants.StateFailed, Message: "boom"})
	if err == nil || err.Error() != "owned engine run failed: boom" {
		t.Fatalf("failure = %v, want unattributed message", err)
	}
	if ownedTerminalStatusFailure(&TestStatus{State: constants.StateCompleted}) != nil {
		t.Fatal("a COMPLETED status must not produce a failure")
	}
}

func TestDistributedSetupFailureCompletesWithoutMetrics(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/status" {
			_, _ = w.Write([]byte(`{"state":"FAILED","run_id":41,"step_id":"create-step","category":"execution",` +
				`"message":"target bucket bench is not accessible"}`))
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{
		{Original: "entry", Host: "entry"}, {Original: "worker", Host: "worker"},
	}, 2)
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusRunning)
		host.APIClient = NewSptAPIClient(server.URL)
	}
	orchestrator.hosts[0].APIClient.SetRunID("41")
	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	wrapper.pollInterval = 5 * time.Millisecond
	var observed []string
	wrapper.SetCallbacks(func(status *TestStatus) { observed = append(observed, status.State) }, nil, nil, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	wrapper.StartMetricsPolling(ctx)
	defer wrapper.StopMetricsPolling()

	select {
	case <-wrapper.CompletionCh():
	case <-ctx.Done():
		t.Fatal("distributed setup failure did not complete orchestration")
	}
	err := wrapper.CompletionError()
	if err == nil || !strings.Contains(err.Error(), "step create-step (execution): target bucket bench is not accessible") {
		t.Fatalf("completion error = %v, want the entry engine's cause", err)
	}
	if len(observed) == 0 || observed[0] != constants.StateFailed {
		t.Fatalf("status callbacks = %v, want the FAILED status forwarded", observed)
	}
}

func TestDistributedFailureOfAnotherRunIsIgnored(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/status" {
			_, _ = w.Write([]byte(`{"state":"FAILED","run_id":40,"category":"execution","message":"old run"}`))
			return
		}
		_, _ = w.Write([]byte(`[]`))
	}))
	defer server.Close()

	orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{{Original: "entry", Host: "entry"}}, 1)
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	orchestrator.hosts[0].APIClient = NewSptAPIClient(server.URL)
	orchestrator.hosts[0].APIClient.SetRunID("41")
	wrapper := NewMultiHostTestOrchestrator(orchestrator)

	if failure := wrapper.observeEntryTerminalFailure(context.Background()); failure != nil {
		t.Fatalf("a FAILED status attributed to another run must not complete this one: %v", failure)
	}
}

// Provisioning runs before every phase: a later phase can fail its bucket check after earlier
// phases published metrics, and must still end the run with its own cause.
func TestDistributedLaterPhaseSetupFailureCompletesAfterEarlierMetrics(t *testing.T) {
	step := newTestStep()
	step.StepID = "seed-step"
	step.RunID = "41"
	step.OpType = "CREATE"
	step.TestState = constants.TestStateCompleted
	step.CompletionPercent = 100
	step.ElapsedTimeSeconds = 300
	payload := marshalSteps(t, []JSONMetricsStep{step})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/status" {
			_, _ = w.Write([]byte(`{"state":"FAILED","run_id":41,"step_id":"read-step","category":"execution",` +
				`"message":"target bucket bench is not accessible (HTTP 403)"}`))
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
	wrapper.SetExpectedStepIDs([]string{"seed-step", "read-step"})
	wrapper.pollInterval = 5 * time.Millisecond
	var metricsSeen atomic.Bool
	wrapper.SetCallbacks(nil, func(*MultiNodeMetricsUpdate) { metricsSeen.Store(true) }, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	wrapper.StartMetricsPolling(ctx)
	defer wrapper.StopMetricsPolling()

	select {
	case <-wrapper.CompletionCh():
	case <-ctx.Done():
		t.Fatal("a later phase's setup failure did not complete orchestration after earlier metrics")
	}
	if !metricsSeen.Load() {
		t.Fatal("the earlier phase's metrics were not published")
	}
	err := wrapper.CompletionError()
	if err == nil || !strings.Contains(err.Error(), "step read-step (execution): target bucket bench is not accessible") {
		t.Fatalf("completion error = %v, want the later phase's setup failure", err)
	}
}
