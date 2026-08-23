package tui

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

func TestEmitBaselineNodeStatus_IncludesAllHosts(t *testing.T) {
	// Prepare three hosts
	hostInfos := []*hostparse.HostInfo{
		{Original: "hostA", Host: "hostA"},
		{Original: "hostB", Host: "hostB"},
		{Original: "hostC", Host: "hostC"},
	}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)

	// Configure states
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	orchestrator.hosts[0].APIClient = NewSptAPIClient("http://example")

	orchestrator.hosts[1].SetStatus(HostStatusRunning)
	// no APIClient for hostB

	orchestrator.hosts[2].SetStatus(HostStatusFailed)
	orchestrator.hosts[2].SetError(fmt.Errorf("boom"))

	wrapper := NewMultiHostTestOrchestrator(orchestrator)

	var got *MultiNodeMetricsUpdate
	wrapper.SetCallbacks(nil, func(update *MultiNodeMetricsUpdate) {
		got = update
	}, nil, nil)

	// Act: build a baseline update and simulate delivery via callback
	got = wrapper.BuildBaselineUpdate()

	if got == nil {
		t.Fatal("expected baseline metrics update to be emitted")
	}

	// Assert NodeStatus contains all hosts
	if len(got.NodeStatus) != 3 {
		t.Fatalf("expected 3 node statuses, got %d", len(got.NodeStatus))
	}

	a := got.NodeStatus["hostA"]
	b := got.NodeStatus["hostB"]
	c := got.NodeStatus["hostC"]

	if !a.IsConnected || a.IsActive {
		t.Errorf("hostA expected IsConnected=true, IsActive=false, got %+v", a)
	}
	if a.Phase != NodePhaseAPIReady {
		t.Errorf("hostA expected Phase api_ready, got %s", a.Phase)
	}
	if b.IsConnected || b.IsActive {
		t.Errorf("hostB expected IsConnected=false, IsActive=false, got %+v", b)
	}
	if b.Phase != NodePhaseContainerStarting {
		t.Errorf("hostB expected Phase container_starting, got %s", b.Phase)
	}
	if c.IsConnected || c.Error == nil {
		t.Errorf("hostC expected IsConnected=false and non-nil error, got %+v", c)
	}
	if c.Phase != NodePhasePending {
		t.Errorf("hostC expected Phase pending, got %s", c.Phase)
	}

	if got.Aggregated == nil {
		t.Error("expected placeholder aggregated metric to be set")
	}
	if len(got.PerNode) != 0 {
		t.Error("expected PerNode to be empty in baseline update")
	}
}

func TestPollerSetsIsConnectedFlag(t *testing.T) {
	// Good server returns a minimal valid JSON metrics payload
	goodSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics/json":
			w.Header().Set("Content-Type", "application/json")
			step := newTestStep()
			step.StepID = "s1"
			step.OpType = "CREATE"
			step.Timestamp = 1_700_000_000_000
			step.ElapsedTimeSeconds = 1.2
			step.TestState = 1
			step.CompletionPercent = 10
			step.OverallCompletion = 10
			step.Unbounded = false
			step.OverallUnbounded = false
			step.Operations.SuccessCount = 10
			step.Operations.FailedCount = 0
			step.Operations.SuccessRateLast = 1000
			step.Operations.FailedRateLast = 0
			step.Bandwidth.BytesTotal = 1024
			step.Bandwidth.BytesRateLast = 1_048_576
			step.Timing.LatencyMeanUs = 1000
			step.Timing.DurationMeanUs = 2000
			step.Concurrency.Current = 1
			step.Concurrency.Mean = 1.0
			_, _ = w.Write([]byte(marshalSteps(t, []JSONMetricsStep{step})))
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer goodSrv.Close()

	// Bad server returns 500
	badSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics/json" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer badSrv.Close()

	hostInfos := []*hostparse.HostInfo{
		{Original: "goodHost", Host: "goodHost"},
		{Original: "badHost", Host: "badHost"},
	}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)

	// Make both hosts Running with API clients
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	orchestrator.hosts[0].APIClient = NewSptAPIClient(goodSrv.URL)

	orchestrator.hosts[1].SetStatus(HostStatusRunning)
	orchestrator.hosts[1].APIClient = NewSptAPIClient(badSrv.URL)

	wrapper := NewMultiHostTestOrchestrator(orchestrator)

	// Act
	results := wrapper.pollNodesConcurrently(
		// Background context; the method wraps it with its own timeout.
		//nolint:staticcheck // SA1019 is not applicable here
		context.Background(),
		[]*HostConnection{orchestrator.hosts[0], orchestrator.hosts[1]},
	)

	// Assert connectivity flags
	if s, ok := results.Status["goodHost"]; !ok || !s.IsConnected {
		t.Errorf("expected goodHost IsConnected=true, got %+v", s)
	} else if !s.IsActive {
		t.Errorf("expected goodHost IsActive=true, got %+v", s)
	} else if s.Phase != NodePhaseMetricsFlowing {
		t.Errorf("expected goodHost Phase metrics_flowing, got %s", s.Phase)
	}
	if s, ok := results.Status["badHost"]; !ok || s.IsConnected {
		t.Errorf("expected badHost IsConnected=false, got %+v", s)
	} else if s.IsActive {
		t.Errorf("expected badHost IsActive=false, got %+v", s)
	} else if s.Phase != NodePhaseContainerStarting {
		t.Errorf("expected badHost Phase container_starting, got %s", s.Phase)
	}

	// Ensure we actually parsed metrics for the good host
	if m := results.Metrics["goodHost"]; m == nil || m.OpsPerSec == 0 {
		t.Errorf("expected parsed metrics for goodHost, got %+v", m)
	} else if m.NodeID != fleetLocalContributorID {
		t.Fatalf("entry metric contributor = %q, want %q", m.NodeID, fleetLocalContributorID)
	}

	update := wrapper.collectAllMetrics(context.Background())
	if update == nil || update.Aggregated == nil {
		t.Fatal("expected a fail-closed aggregate from the successful contributor")
	}
	if update.Aggregated.NodesCount != 2 || len(update.Aggregated.NodesPresent) != 1 {
		t.Fatalf("expected one of two contributors, got count=%d present=%v",
			update.Aggregated.NodesCount, update.Aggregated.NodesPresent)
	}
	if update.Aggregated.NodesPresent[0] != fleetLocalContributorID {
		t.Fatalf("successful contributor = %q, want %q",
			update.Aggregated.NodesPresent[0], fleetLocalContributorID)
	}
	if !update.Aggregated.Partial {
		t.Fatal("a failed contributor poll must make the aggregate partial")
	}
}

func TestPerOperationCompletenessUsesExpectedFleetNotObservedContributors(t *testing.T) {
	metric := &PerformanceMetric{
		MetricsSchema: 4,
		Scope:         "node",
		ClusterID:     "cluster-a",
		RunID:         "run-a",
		StepID:        "delete-step",
		NodeID:        fleetLocalContributorID,
		OpType:        "DELETE",
		Delete:        &DeleteMetrics{},
	}
	present, unique := contributorsForOp([]*PerformanceMetric{metric}, "DELETE")
	got := withPollCompleteness(metric, 2, present, !unique || len(present) != 2)
	if got.NodesCount != 2 || len(got.NodesPresent) != 1 || !got.Partial {
		t.Fatalf("N-1 operation must be partial against expected fleet: %+v", got)
	}
}

func TestFleetContributorIDsMatchEngineContract(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Original: "entry.example", Host: "entry.example"},
		{Original: "worker.example", Host: "worker.example"},
	}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	orchestrator.hosts[1].AdvertisedIP = "192.0.2.25"
	wrapper := NewMultiHostTestOrchestrator(orchestrator)

	if got := wrapper.fleetContributorID(orchestrator.hosts[0]); got != "local" {
		t.Fatalf("entry contributor = %q, want local", got)
	}
	if got := wrapper.fleetContributorID(orchestrator.hosts[1]); got != "192.0.2.25:1099" {
		t.Fatalf("worker contributor = %q, want 192.0.2.25:1099", got)
	}
	orchestrator.executionContributorIDs = []string{"local", "192.0.2.25:1099"}
	got, err := orchestrator.MetricContributorIDs()
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != "local" || got[1] != "192.0.2.25:1099" {
		t.Fatalf("execution contributor IDs = %v", got)
	}
	got[0] = "mutated"
	again, err := orchestrator.MetricContributorIDs()
	if err != nil || again[0] != "local" {
		t.Fatalf("execution contributor IDs were not copied: ids=%v err=%v", again, err)
	}
}

func TestMetricContributorIDsRejectUnavailableExecutionTopology(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Original: "entry.example", Host: "entry.example"}}, 1)
	if _, err := orchestrator.MetricContributorIDs(); err == nil {
		t.Fatal("unavailable execution contributor topology was accepted")
	}
}

func TestPollerDemotesPhaseOnParseError(t *testing.T) {
	parseErrorSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics/json" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("{"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer parseErrorSrv.Close()

	hostInfos := []*hostparse.HostInfo{
		{Original: "parseHost", Host: "parseHost"},
	}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	orchestrator.hosts[0].APIClient = NewSptAPIClient(parseErrorSrv.URL)
	orchestrator.hosts[0].SetPhase(NodePhaseMetricsFlowing)

	wrapper := NewMultiHostTestOrchestrator(orchestrator)

	results := wrapper.pollNodesConcurrently(
		context.Background(),
		[]*HostConnection{orchestrator.hosts[0]},
	)

	status, ok := results.Status["parseHost"]
	if !ok {
		t.Fatal("expected status for parseHost")
	}
	if status.IsConnected != true {
		t.Errorf("expected parseHost IsConnected=true after parse failure, got %+v", status)
	}
	if status.IsActive {
		t.Errorf("expected parseHost IsActive=false after parse failure, got %+v", status)
	}
	if status.Phase != NodePhaseAPIReady {
		t.Errorf("expected parseHost Phase api_ready, got %s", status.Phase)
	}
	if orchestrator.hosts[0].GetPhase() != NodePhaseAPIReady {
		t.Errorf("expected host phase to downgrade to api_ready, got %s", orchestrator.hosts[0].GetPhase())
	}
}

func TestPollerMarksIdleAsInactive(t *testing.T) {
	idleSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics/json" {
			step := newTestStep()
			step.TestState = 0
			step.Operations.SuccessRateLast = 0
			step.Bandwidth.BytesRateLast = 0
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(marshalSteps(t, []JSONMetricsStep{step})))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer idleSrv.Close()

	hostInfos := []*hostparse.HostInfo{{Original: "idleHost", Host: "idleHost"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	orchestrator.hosts[0].APIClient = NewSptAPIClient(idleSrv.URL)

	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	results := wrapper.pollNodesConcurrently(context.Background(), orchestrator.GetReadyHosts())

	status, ok := results.Status["idleHost"]
	if !ok {
		t.Fatalf("expected idleHost status entry")
	}
	if !status.IsConnected {
		t.Fatalf("expected idleHost to be connected")
	}
	if status.IsActive {
		t.Fatalf("expected idleHost IsActive=false, got %+v", status)
	}
}

func TestPollerSkipsNodeDuringBackoff(t *testing.T) {
	var hits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics/json" {
			atomic.AddInt32(&hits, 1)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(marshalSteps(t, []JSONMetricsStep{newTestStep()})))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	hostInfos := []*hostparse.HostInfo{{Original: "nodeA", Host: "nodeA"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	orchestrator.hosts[0].APIClient = NewSptAPIClient(server.URL)

	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	state := wrapper.getPollState("nodeA")
	state.recordFailure(time.Now(), errors.New("boom"), pollErrorRetryable, wrapper.backoffCfg, func(time.Duration) time.Duration { return 0 })

	results := wrapper.pollNodesConcurrently(context.Background(), orchestrator.GetReadyHosts())
	if atomic.LoadInt32(&hits) != 0 {
		t.Fatalf("expected no HTTP calls while in backoff, saw %d", hits)
	}
	status, ok := results.Status["nodeA"]
	if !ok {
		t.Fatalf("expected nodeA status entry")
	}
	if status.IsConnected {
		t.Fatalf("expected nodeA to be marked disconnected during backoff")
	}
}

func TestPollerKeepsNodeConnectedOnParseError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics/json" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	hostInfos := []*hostparse.HostInfo{{Original: "parseHost", Host: "parseHost"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	orchestrator.hosts[0].APIClient = NewSptAPIClient(server.URL)

	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	results := wrapper.pollNodesConcurrently(context.Background(), orchestrator.GetReadyHosts())

	status, ok := results.Status["parseHost"]
	if !ok {
		t.Fatalf("expected parseHost status entry")
	}
	if !status.IsConnected {
		t.Fatalf("expected parseHost to remain connected despite parse error: %#v", status)
	}
	if status.Error == nil {
		t.Fatalf("expected parseHost error to be recorded")
	}
	if _, ok := results.Metrics["parseHost"]; ok {
		t.Fatalf("expected no metrics recorded for parseHost on parse error")
	}
}

func TestPollerLogsVerbosePayloadOnParseError(t *testing.T) {
	t.Setenv("SPT_LOG_METRICS_BODY", "1")
	var verboseHits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics/json" {
			if strings.Contains(r.URL.RawQuery, "verbose=1") {
				atomic.AddInt32(&verboseHits, 1)
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("[]"))
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("{"))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	hostInfos := []*hostparse.HostInfo{{Original: "nodeVerbose", Host: "nodeVerbose"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	orchestrator.hosts[0].APIClient = NewSptAPIClient(server.URL)

	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	results := wrapper.pollNodesConcurrently(context.Background(), orchestrator.GetReadyHosts())
	if atomic.LoadInt32(&verboseHits) != 1 {
		t.Fatalf("expected one verbose fetch, got %d", verboseHits)
	}
	if _, ok := results.Metrics["nodeVerbose"]; ok {
		t.Fatal("did not expect metrics on parse error")
	}
	status := results.Status["nodeVerbose"]
	if !status.IsConnected {
		t.Fatalf("expected nodeVerbose to remain connected despite parse error")
	}
	if status.Error == nil {
		t.Fatal("expected parse error to be recorded")
	}
}

func TestMultiHostCompatibilityWarning(t *testing.T) {
	legacySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics/json" {
			s := newTestStep()
			s.MetricsSchema = 1
			_, _ = w.Write([]byte(marshalSteps(t, []JSONMetricsStep{s})))
		}
	}))
	defer legacySrv.Close()

	hostInfos := []*hostparse.HostInfo{{Original: "legacy", Host: "legacy"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	orchestrator.hosts[0].APIClient = NewSptAPIClient(legacySrv.URL)

	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	var messages []string
	wrapper.SetMessageSink(func(msg string) {
		messages = append(messages, msg)
	})

	wrapper.pollNodesConcurrently(context.Background(), orchestrator.GetReadyHosts())
	if len(messages) != 1 {
		t.Fatalf("expected one compatibility warning, got %d", len(messages))
	}

	wrapper.pollNodesConcurrently(context.Background(), orchestrator.GetReadyHosts())
	if len(messages) != 1 {
		t.Fatalf("expected warning only once, got %d", len(messages))
	}
	if !strings.Contains(strings.ToLower(messages[0]), "schema") {
		t.Errorf("warning should mention schema, got %q", messages[0])
	}
}
