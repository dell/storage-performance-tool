package tui

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

func TestParseSchema4DeleteMetricsPreservesRequestAndObjectUnits(t *testing.T) {
	step := newTestStep()
	step.MetricsSchema = 4
	step.OpType = "DELETE"
	step.Operations.SuccessCount = 1
	step.Operations.FailedCount = 2
	step.Operations.SuccessRateLast = 1.5
	step.Bandwidth = JSONMetricsBandwidth{}
	step.Timing.TTFB = nil
	step.Delete = testDeleteMetrics("batch", 100, 3, 175)

	metrics, err := NewSptAPIClient("").ParseJSONMetrics(marshalSteps(t, []JSONMetricsStep{step}))
	if err != nil {
		t.Fatalf("parse schema 4 DELETE metrics: %v", err)
	}
	if len(metrics) != 1 || metrics[0].Delete == nil {
		t.Fatalf("missing stored DELETE metrics: %+v", metrics)
	}
	got := metrics[0]
	if got.OpsPerSec != 2 { // rounded generic logical-request success rate
		t.Fatalf("generic operation rate = %d, want logical request rate 2", got.OpsPerSec)
	}
	if got.SuccessCount != 1 || got.FailedCount != 2 {
		t.Fatalf("generic counts = %d/%d, want request counts 1/2", got.SuccessCount, got.FailedCount)
	}
	if got.Delete.Requests.Attempted != 3 || got.Delete.Objects.Attempted != 175 {
		t.Fatalf("explicit units lost: %+v", got.Delete)
	}
	if got.Delete.Completion.RequestPercent != 100 || got.Delete.Completion.ObjectPercent != 100 {
		t.Fatalf("DELETE completion lost: %+v", got.Delete.Completion)
	}
	if got.Delete.OutcomeTerminology != "accepted" || got.Delete.Verification.Enabled {
		t.Fatalf("unsafe DELETE terminology: %+v", got.Delete)
	}
	if got.HasTTFB || got.MiBPerSec != 0 {
		t.Fatalf("DELETE fabricated transfer metric: ttfb=%t mib/s=%d", got.HasTTFB, got.MiBPerSec)
	}
}

func TestParserAcceptsRunningOutcomeForNodeContributorButRequiresTerminalFleetOutcome(t *testing.T) {
	step := newTestStep()
	step.MetricsSchema = deletemetrics.SchemaVersion
	step.OpType = "DELETE"
	step.TestState = constants.TestStateCompleted
	step.DeleteDetailExpected = true
	step.Delete = testDeleteMetrics("single", 1, 1, 1)
	step.Delete.FailurePolicy.Outcome = deletemetrics.OutcomeRunning
	step.Delete.Buckets = []DeleteBucketMetrics{{
		Bucket: "bucket-a", Selected: 1, Attempted: 1, Accepted: 1,
	}}
	step.Delete.Timing.Latency = testTimingStat(1, 7)
	step.Delete.Timing.Duration = testTimingStat(1, 11)

	node, err := NewSptAPIClient("").ParseJSONMetrics(marshalSteps(t, []JSONMetricsStep{step}))
	if err != nil || len(node) != 1 {
		t.Fatalf("parse terminal node contributor: metrics=%+v err=%v", node, err)
	}
	if node[0].Partial {
		t.Fatalf("controller-owned running outcome made a complete node contributor partial: %+v", node[0])
	}
	step.Delete.Objects.Accepted = 0
	malformed, err := NewSptAPIClient("").ParseJSONMetrics(marshalSteps(t, []JSONMetricsStep{step}))
	if err != nil || len(malformed) != 1 || !malformed[0].Partial {
		t.Fatalf("malformed terminal node contributor did not fail closed: metrics=%+v err=%v", malformed, err)
	}
	step.Delete.Objects.Accepted = 1

	step.Scope = "fleet"
	step.Role = "aggregate"
	fleet, err := NewSptAPIClient("").ParseFleetJSONMetrics(marshalSteps(t, []JSONMetricsStep{step}))
	if err != nil || len(fleet) != 1 {
		t.Fatalf("parse terminal fleet authority: metrics=%+v err=%v", fleet, err)
	}
	if !fleet[0].Partial {
		t.Fatalf("terminal fleet authority accepted non-terminal outcome: %+v", fleet[0])
	}
}

func TestParserRejectsControllerImpossibleSuccessForNodeAndFleet(t *testing.T) {
	step := newTestStep()
	step.MetricsSchema = deletemetrics.SchemaVersion
	step.Scope = "node"
	step.Role = "worker"
	step.OpType = "DELETE"
	step.TestState = constants.TestStateCompleted
	step.DeleteDetailExpected = true
	step.Operations.SuccessCount = 0
	step.Operations.FailedCount = 1
	step.Delete = testDeleteMetrics("single", 1, 1, 1)
	step.Delete.Requests.FullSuccess = 0
	step.Delete.Requests.Failed = 1
	step.Delete.Objects.Accepted = 0
	step.Delete.Objects.Failed = 1
	step.Delete.FailurePolicy.OperationalFailedObjects = 1
	step.Delete.FailurePolicy.ObservedFailurePercent = 100
	step.Delete.FailurePolicy.Outcome = deletemetrics.OutcomeCompletedCleanly
	step.Delete.Buckets = []DeleteBucketMetrics{{
		Bucket: "bucket-a", Selected: 1, Attempted: 1, Failed: 1,
	}}
	step.Delete.Timing.Latency = testTimingStat(1, 7)
	step.Delete.Timing.Duration = testTimingStat(1, 11)

	node, err := NewSptAPIClient("").ParseJSONMetrics(
		marshalSteps(t, []JSONMetricsStep{step}))
	if err != nil || len(node) != 1 || !node[0].Partial {
		t.Fatalf("controller-impossible node success did not fail closed: metrics=%+v err=%v", node, err)
	}

	step.Scope = "fleet"
	step.Role = aggregateLabel
	step.NodesCount = 1
	step.NodesPresent = []string{constants.MetricsLocalContributorID}
	step.ContributorsPresent = []string{constants.MetricsLocalContributorID}
	fleet, err := NewSptAPIClient("").ParseFleetJSONMetrics(
		marshalSteps(t, []JSONMetricsStep{step}))
	if err != nil || len(fleet) != 1 || !fleet[0].Partial {
		t.Fatalf("controller-impossible fleet success did not fail closed: metrics=%+v err=%v", fleet, err)
	}
	if terminalMetricsReady(&MultiNodeMetricsUpdate{
		Aggregated: fleet[0], PerOpType: map[string]*PerformanceMetric{"DELETE": fleet[0]},
	}) {
		t.Fatal("controller-impossible fleet success authorized completion")
	}
}

func TestMetricsAggregatorSumsDeleteUnitsAndRetainsPerNodeIdentity(t *testing.T) {
	nodeA := deleteNodeMetric("batch", 100, 2, 150)
	nodeB := deleteNodeMetric("batch", 100, 1, 25)

	got := NewMetricsAggregator().Aggregate(map[string]*PerformanceMetric{"a": nodeA, "b": nodeB})
	if got == nil || got.Delete == nil {
		t.Fatalf("missing aggregate DELETE metrics: %+v", got)
	}
	if got.Delete.Requests.Attempted != 3 || got.Delete.Objects.Attempted != 175 {
		t.Fatalf("wrong aggregate units: %+v", got.Delete)
	}
	if got.Delete.Batches.ActualRequestCount != 3 || got.Delete.Batches.ActualObjectCount != 175 {
		t.Fatalf("wrong aggregate batch counts: %+v", got.Delete.Batches)
	}
	if len(got.Delete.Buckets) != 2 {
		t.Fatalf("bucket metrics = %+v, want two bounded entries", got.Delete.Buckets)
	}
	if got.Delete.Identity.Mode != "batch" || got.Delete.Identity.SelectionOrder != "canonical" {
		t.Fatalf("result identity lost: %+v", got.Delete.Identity)
	}
	if got.Delete.Phases.ScheduledDeleteSeconds == nil || *got.Delete.Phases.ScheduledDeleteSeconds != 1.5 {
		t.Fatalf("aggregate scheduled phase = %+v, want max wall time 1.5", got.Delete.Phases)
	}
	if got.Delete.Timing.Latency != nil || got.Delete.Timing.Duration != nil {
		t.Fatalf("client aggregator fabricated multi-node quantiles: %+v", got.Delete.Timing)
	}
}

func TestMetricsAggregatorRejectsMixedNodeFailureOutcomes(t *testing.T) {
	nodeA := deleteNodeMetric("batch", 100, 2, 150)
	nodeB := deleteNodeMetric("batch", 100, 1, 25)
	nodeB.Delete.FailurePolicy.Outcome = deletemetrics.OutcomeCompletedWithinFailureBudget

	if got := NewMetricsAggregator().Aggregate(map[string]*PerformanceMetric{"a": nodeA, "b": nodeB}); got != nil {
		t.Fatalf("mixed node-owned failure outcomes must not aggregate: %+v", got.Delete.FailurePolicy)
	}
}

func TestMetricsAggregatorMarksSchema4DeleteWithoutDetailPartialButKeepsSchema3Compatible(t *testing.T) {
	missingA := deleteNodeMetric("batch", 100, 2, 150)
	missingA.Delete = nil
	missingB := deleteNodeMetric("batch", 100, 1, 25)
	missingB.Delete = nil

	got := NewMetricsAggregator().Aggregate(map[string]*PerformanceMetric{"a": missingA, "b": missingB})
	if got == nil {
		t.Fatal("schema-v4 DELETE without detail should remain visible as a partial aggregate")
	}
	if !got.Partial || got.Delete != nil {
		t.Fatalf("schema-v4 DELETE without detail must be partial: %+v", got)
	}

	legacy := *missingA
	legacy.DeleteDetailExpected = false
	legacyGot := NewMetricsAggregator().Aggregate(map[string]*PerformanceMetric{"legacy": &legacy})
	if legacyGot == nil || legacyGot.Partial {
		t.Fatalf("generic schema-v4 DELETE compatibility regressed: %+v", legacyGot)
	}
}

func TestParserMarksExpectedSchema4DeleteWithoutDetailPartialButKeepsLegacyV4Compatible(t *testing.T) {
	step := newTestStep()
	step.OpType = "DELETE"
	step.MetricsSchema = 4
	step.DeleteDetailExpected = true

	metrics, err := NewSptAPIClient("").ParseJSONMetrics(marshalSteps(t, []JSONMetricsStep{step}))
	if err != nil || len(metrics) != 1 || !metrics[0].Partial {
		t.Fatalf("expected schema-v4 DELETE without detail must parse as partial: metrics=%+v err=%v", metrics, err)
	}

	step.DeleteDetailExpected = false
	metrics, err = NewSptAPIClient("").ParseJSONMetrics(marshalSteps(t, []JSONMetricsStep{step}))
	if err != nil || len(metrics) != 1 || metrics[0].Partial {
		t.Fatalf("generic schema-v4 DELETE compatibility regressed: metrics=%+v err=%v", metrics, err)
	}
}

func TestMultiNodeDeleteWithoutAuthoritativeFleetTimingCannotComplete(t *testing.T) {
	nodeA := deleteNodeMetric("batch", 100, 2, 150)
	nodeB := deleteNodeMetric("batch", 100, 1, 25)
	got := NewMetricsAggregator().Aggregate(map[string]*PerformanceMetric{"a": nodeA, "b": nodeB})
	got.TestState = constants.TestStateCompleted
	got = requireAuthoritativeDeleteTiming(got)

	if !got.Partial {
		t.Fatalf("multi-node DELETE without fleet timing must remain partial: %+v", got)
	}
	if shouldSignalCompletion(got) {
		t.Fatal("multi-node DELETE without authoritative fleet timing signaled completion")
	}
}

func TestSingleNodeDeleteWithAvailableTimingCanComplete(t *testing.T) {
	node := deleteNodeMetric("single", 1, 1, 1)
	node.Delete.Timing.Latency = testTimingStat(1, 7)
	node.Delete.Timing.Duration = testTimingStat(1, 11)
	got := NewMetricsAggregator().Aggregate(map[string]*PerformanceMetric{"only": node})
	got.TestState = constants.TestStateCompleted
	got = requireAuthoritativeDeleteTiming(got)

	if got.Partial || !shouldSignalCompletion(got) {
		t.Fatalf("complete single-node DELETE with local timing was rejected: %+v", got)
	}
}

func TestFleetDeleteMetricsProvideAuthoritativeMergedQuantiles(t *testing.T) {
	step := newTestStep()
	step.MetricsSchema = 4
	step.Scope = "fleet"
	step.Role = "aggregate"
	step.OpType = "DELETE"
	step.Delete = testDeleteMetrics("batch", 100, 2, 175)
	step.NodesCount = 2
	step.NodesPresent = []string{"remote-node:1099"}
	step.ContributorsPresent = []string{"local", "remote-node:1099"}
	step.TestState = constants.TestStateCompleted
	step.Delete.Batches.FullBatchCount = 1
	step.Delete.Batches.PartialBatchCount = 1
	step.Delete.Batches.FullBatchPercent = 50
	step.Delete.Buckets = []DeleteBucketMetrics{{
		Bucket: "bucket-a", Selected: 175, Attempted: 175, Accepted: 175,
	}}
	step.Delete.Timing.Latency = &JSONTimingStat{
		Count: 2, MeanUs: 20, MinUs: 5, P50Us: 11, P90Us: 22, P99Us: 33, P999Us: 44, MaxUs: 50,
	}
	step.Delete.Timing.Duration = &JSONTimingStat{
		Count: 2, MeanUs: 70, MinUs: 50, P50Us: 55, P90Us: 66, P99Us: 77, P999Us: 88, MaxUs: 90,
	}

	metrics, err := NewSptAPIClient("").ParseFleetJSONMetrics(marshalSteps(t, []JSONMetricsStep{step}))
	if err != nil || len(metrics) != 1 {
		t.Fatalf("parse fleet DELETE metrics: metrics=%+v err=%v", metrics, err)
	}
	if metrics[0].Delete.Timing.Latency.P999Us != 44 || metrics[0].Delete.Timing.Duration.P99Us != 77 {
		t.Fatalf("authoritative fleet quantiles lost: %+v", metrics[0].Delete.Timing)
	}
	expected := deleteNodeMetric("batch", 100, 2, 175)
	expected.StepID = step.StepID
	expected.NodesCount = 2
	expected.NodesPresent = []string{"cli-entry", "cli-worker"}
	expected.ContributorsPresent = []string{"remote-node:1099", "local"}
	if !compatibleAuthoritativeDelete(expected, metrics[0]) {
		t.Fatalf("compatible fleet DELETE metric rejected: %+v", metrics[0])
	}
}

func TestFleetDeleteMetricsRejectDuplicateAuthoritativeRows(t *testing.T) {
	var output bytes.Buffer
	previousLogger := logging.GetLogger()
	logging.SetLogger(slog.New(slog.NewTextHandler(&output, nil)))
	defer logging.SetLogger(previousLogger)
	var duplicateWarning sync.Once

	expected := deleteNodeMetric("single", 1, 1, 1)
	expected.NodesCount = 1
	expected.NodesPresent = []string{constants.MetricsLocalContributorID}
	expected.ContributorsPresent = []string{constants.MetricsLocalContributorID}

	candidate := newTestStep()
	candidate.MetricsSchema = deletemetrics.SchemaVersion
	candidate.Scope = "fleet"
	candidate.Role = aggregateLabel
	candidate.ClusterID = expected.ClusterID
	candidate.RunID = expected.RunID
	candidate.StepID = expected.StepID
	candidate.OpType = "DELETE"
	candidate.NodesCount = 1
	candidate.NodesPresent = []string{constants.MetricsLocalContributorID}
	candidate.ContributorsPresent = []string{constants.MetricsLocalContributorID}
	candidate.DeleteDetailExpected = true
	candidate.Delete = testDeleteMetrics("single", 1, 1, 1)
	candidate.Delete.Timing.Latency = testTimingStat(1, 7)
	candidate.Delete.Timing.Duration = testTimingStat(1, 11)
	candidate.Delete.FailurePolicy.Outcome = deletemetrics.OutcomeCompletedCleanly

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(marshalSteps(t, []JSONMetricsStep{candidate, candidate})))
	}))
	defer server.Close()

	for range 3 {
		if got := fetchAuthoritativeFleetDelete(
			NewSptAPIClient(server.URL), expected,
			map[string]*PerformanceMetric{"DELETE": expected}, &duplicateWarning,
		); got != nil {
			t.Fatalf("duplicate fleet authorities were accepted: %+v", got)
		}
	}
	if count := strings.Count(output.String(), "duplicate authoritative fleet DELETE metrics rejected"); count != 1 {
		t.Fatalf("duplicate fleet warnings = %d, want 1:\n%s", count, output.String())
	}
}

func TestFleetDeleteMetricsRejectPartialStaleOrIncompleteSamples(t *testing.T) {
	expected := deleteNodeMetric("batch", 100, 3, 175)
	expected.NodesCount = 2
	expected.NodesPresent = []string{"cli-entry", "cli-worker"}
	expected.ContributorsPresent = []string{"node-a", "node-b"}
	candidate := deleteNodeMetric("batch", 100, 3, 175)
	candidate.Scope = "fleet"
	candidate.Role = "aggregate"
	candidate.NodesCount = 2
	candidate.NodesPresent = []string{"remote-node:1099"}
	candidate.ContributorsPresent = []string{"node-b", "node-a"}
	candidate.Delete.Timing.Latency = &JSONTimingStat{Count: 3, P50Us: 11}
	candidate.Delete.Timing.Duration = &JSONTimingStat{Count: 3, P50Us: 22}
	expected.Delete.FailurePolicy.Outcome = deletemetrics.OutcomeRunning
	candidate.Delete.FailurePolicy.Outcome = deletemetrics.OutcomeCompletedWithinFailureBudget

	if !compatibleAuthoritativeDelete(expected, candidate) {
		t.Fatal("controller-owned terminal outcome must replace the running node aggregate")
	}
	candidate.OpsPerSec = 999
	merged := withAuthoritativeDeleteTerminal(expected, candidate)
	if merged.OpsPerSec != expected.OpsPerSec || merged.Delete.Timing.Latency.P50Us != 11 ||
		merged.Delete.FailurePolicy.Outcome != deletemetrics.OutcomeCompletedWithinFailureBudget {
		t.Fatalf("fleet timing replacement changed local aggregate values: %+v", merged)
	}
	candidate.Partial = true
	if compatibleAuthoritativeDelete(expected, candidate) {
		t.Fatal("partial fleet sample accepted")
	}
	candidate.Partial = false
	candidate.Delete.Objects.Accepted--
	if compatibleAuthoritativeDelete(expected, candidate) {
		t.Fatal("stale fleet counters accepted")
	}
	candidate.Delete.Objects.Accepted++
	candidate.ContributorsPresent = []string{"node-a"}
	if compatibleAuthoritativeDelete(expected, candidate) {
		t.Fatal("incomplete fleet contributors accepted")
	}
	candidate.ContributorsPresent = []string{"node-b", "node-a"}
	for name, clear := range map[string]func(){
		"run":     func() { candidate.RunID = "" },
		"cluster": func() { candidate.ClusterID = "" },
		"step":    func() { candidate.StepID = "" },
	} {
		t.Run("missing schema-v4 fleet "+name, func(t *testing.T) {
			candidate.RunID = expected.RunID
			candidate.ClusterID = expected.ClusterID
			candidate.StepID = expected.StepID
			clear()
			if compatibleAuthoritativeDelete(expected, candidate) {
				t.Fatalf("fleet candidate missing %s identity accepted", name)
			}
		})
	}
	candidate.RunID = expected.RunID
	candidate.ClusterID = "cluster-other"
	candidate.StepID = expected.StepID
	if compatibleAuthoritativeDelete(expected, candidate) {
		t.Fatal("cross-cluster fleet candidate accepted")
	}
}

func TestLocalFleetDeleteRequiresExactlyOneLocalContributor(t *testing.T) {
	expected := deleteNodeMetric("single", 1, 1, 1)
	expected.NodesCount = 0
	expected.NodesPresent = nil
	expected.ContributorsPresent = nil
	update, err := buildLocalMetricsUpdateForRun([]*PerformanceMetric{expected}, expected.RunID)
	if err != nil {
		t.Fatalf("build local metrics update: %v", err)
	}
	expected = update.Aggregated
	if expected.NodesCount != 1 || !sameStringSet(
		expected.ContributorsPresent, []string{constants.MetricsLocalContributorID},
	) {
		t.Fatalf("local contributor identity was not normalized: %+v", expected)
	}

	candidate := deleteNodeMetric("single", 1, 1, 1)
	candidate.Scope = "fleet"
	candidate.Role = aggregateLabel
	candidate.NodesCount = 1
	candidate.NodesPresent = []string{constants.MetricsLocalContributorID}
	candidate.ContributorsPresent = []string{constants.MetricsLocalContributorID}
	candidate.Delete.Timing.Latency = testTimingStat(1, 7)
	candidate.Delete.Timing.Duration = testTimingStat(1, 11)
	candidate.Delete.FailurePolicy.Outcome = deletemetrics.OutcomeCompletedCleanly
	if !compatibleAuthoritativeDelete(expected, candidate) {
		t.Fatal("complete local fleet authority was rejected")
	}

	for name, mutate := range map[string]func(*PerformanceMetric){
		"absent": func(metric *PerformanceMetric) {
			metric.NodesCount = 0
			metric.ContributorsPresent = nil
		},
		"duplicate": func(metric *PerformanceMetric) {
			metric.NodesCount = 2
			metric.ContributorsPresent = []string{
				constants.MetricsLocalContributorID,
				constants.MetricsLocalContributorID,
			}
		},
		"mismatched": func(metric *PerformanceMetric) {
			metric.ContributorsPresent = []string{"other-node"}
		},
	} {
		t.Run(name, func(t *testing.T) {
			invalid := *candidate
			invalid.ContributorsPresent = append([]string(nil), candidate.ContributorsPresent...)
			mutate(&invalid)
			if compatibleAuthoritativeDelete(expected, &invalid) {
				t.Fatalf("%s local fleet contributor evidence was accepted: %+v", name, invalid)
			}
		})
	}
}

func TestFleetDeleteMetricsAcceptEquivalentReorderedFractionalRates(t *testing.T) {
	expected := deleteNodeMetric("batch", 100, 3, 175)
	expected.NodesCount = 3
	expected.NodesPresent = []string{"cli-entry", "cli-worker-a", "cli-worker-b"}
	expected.ContributorsPresent = []string{"node-a", "node-b", "node-c"}
	candidate := deleteNodeMetric("batch", 100, 3, 175)
	candidate.Scope = "fleet"
	candidate.Role = "aggregate"
	candidate.NodesCount = 3
	candidate.NodesPresent = []string{"remote-a:1099", "remote-b:1099"}
	candidate.ContributorsPresent = []string{"node-c", "node-b", "node-a"}
	candidate.Delete.Timing.Latency = &JSONTimingStat{Count: 3, P50Us: 11}
	candidate.Delete.Timing.Duration = &JSONTimingStat{Count: 3, P50Us: 22}

	rates := []float64{0.1, 0.2, 0.3}
	expected.Delete.Requests.PerSecond = rates[0] + rates[1] + rates[2]
	expected.Delete.Objects.PerSecond = rates[0] + rates[1] + rates[2]
	candidate.Delete.Requests.PerSecond = rates[2] + rates[1] + rates[0]
	candidate.Delete.Objects.PerSecond = rates[2] + rates[1] + rates[0]
	if expected.Delete.Requests.PerSecond == candidate.Delete.Requests.PerSecond {
		t.Fatal("test setup did not produce distinct floating-point sums")
	}

	if !compatibleAuthoritativeDelete(expected, candidate) {
		t.Fatal("equivalent fleet sample rejected because fractional rates were summed in a different order")
	}
}

func TestFleetDeleteMetricsAcceptLatencySubsetForPreResponseFailures(t *testing.T) {
	expected := deleteNodeMetric("batch", 100, 3, 175)
	expected.Delete.Requests.FullSuccess = 2
	expected.Delete.Requests.Failed = 1
	candidate := deleteNodeMetric("batch", 100, 3, 175)
	candidate.Scope = "fleet"
	candidate.Role = "aggregate"
	candidate.Delete.Requests.FullSuccess = 2
	candidate.Delete.Requests.Failed = 1
	candidate.Delete.Timing.Latency = &JSONTimingStat{Count: 2, P50Us: 11}
	candidate.Delete.Timing.Duration = &JSONTimingStat{Count: 2, P50Us: 22}

	if !compatibleAuthoritativeDelete(expected, candidate) {
		t.Fatal("requests without a last-byte sample must not invalidate authoritative available timing")
	}
}

func TestFleetDeleteMetricsRejectImpossibleTimingSampleCounts(t *testing.T) {
	expected := deleteNodeMetric("batch", 100, 3, 175)
	tests := []struct {
		name     string
		latency  int64
		duration int64
	}{
		{name: "negative latency", latency: -1, duration: 2},
		{name: "negative duration", latency: 0, duration: -1},
		{name: "latency exceeds duration", latency: 3, duration: 2},
		{name: "duration exceeds terminal requests", latency: 3, duration: 4},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := deleteNodeMetric("batch", 100, 3, 175)
			candidate.Scope = "fleet"
			candidate.Role = "aggregate"
			candidate.Delete.Timing.Latency = &JSONTimingStat{Count: test.latency}
			candidate.Delete.Timing.Duration = &JSONTimingStat{Count: test.duration}
			if compatibleAuthoritativeDelete(expected, candidate) {
				t.Fatal("impossible timing population was accepted")
			}
		})
	}
}

func TestSingleNodeDeleteAggregationRetainsExactTimingDistribution(t *testing.T) {
	node := deleteNodeMetric("single", 1, 1, 1)
	node.Delete.Timing.Latency = testTimingStat(1, 9)
	node.Delete.Timing.Duration = testTimingStat(1, 19)

	got := NewMetricsAggregator().Aggregate(map[string]*PerformanceMetric{"only": node})
	if got.Delete.Timing.Latency.P50Us != 9 || got.Delete.Timing.Duration.P50Us != 19 {
		t.Fatalf("single-node exact timing lost: %+v", got.Delete.Timing)
	}
}

func TestMetricsAggregatorRejectsMixedDeleteResultIdentity(t *testing.T) {
	batch := deleteNodeMetric("batch", 100, 2, 150)
	single := deleteNodeMetric("single", 1, 1, 1)

	if got := NewMetricsAggregator().Aggregate(map[string]*PerformanceMetric{"batch": batch, "single": single}); got != nil {
		t.Fatalf("mixed DELETE result identities must not be merged: %+v", got.Delete)
	}
}

func TestMetricsAggregatorRejectsMixedDeleteFailurePolicy(t *testing.T) {
	nodes := map[string]*PerformanceMetric{
		"node-a": deleteNodeMetric("batch", 100, 2, 150),
		"node-b": deleteNodeMetric("batch", 100, 1, 25),
	}
	nodes["node-b"].Delete.FailurePolicy.MaxFailedObjects++

	if got := NewMetricsAggregator().Aggregate(nodes); got != nil {
		t.Fatalf("expected incompatible DELETE failure policies to be rejected, got %#v", got.Delete)
	}
}

func TestMetricsAggregatorUsesControllerFailureBudgetDenominator(t *testing.T) {
	nodeA := deleteNodeMetric("batch", 100, 1, 75)
	nodeA.Delete.Objects.Accepted = 70
	nodeA.Delete.Objects.Failed = 5
	nodeA.Delete.FailurePolicy.OperationalFailedObjects = 3
	nodeA.Delete.FailurePolicy.ExcludedFailedObjects = 2
	nodeB := deleteNodeMetric("batch", 100, 1, 103)
	nodeB.Delete.Objects.Accepted = 100
	nodeB.Delete.Objects.Failed = 3
	nodeB.Delete.FailurePolicy.OperationalFailedObjects = 1
	nodeB.Delete.FailurePolicy.ExcludedFailedObjects = 2

	got := NewMetricsAggregator().Aggregate(map[string]*PerformanceMetric{"a": nodeA, "b": nodeB})
	policy := got.Delete.FailurePolicy
	if policy.OperationalFailedObjects != 4 || policy.ExcludedFailedObjects != 4 {
		t.Fatalf("aggregate failure classifications = %+v", policy)
	}
	want := 4.0 * 100 / 174.0
	if policy.ObservedFailurePercent != want {
		t.Fatalf("observed failure percent = %v, want %v", policy.ObservedFailurePercent, want)
	}
}

func TestMetricsAggregatorSumsEnabledDeleteVerificationClassifications(t *testing.T) {
	nodeA := deleteNodeMetric("batch", 2, 1, 2)
	nodeA.Delete.Verification = DeleteVerificationMetrics{
		Enabled: true, PreValidationEnabled: true, PostVerificationEnabled: true,
		PreValidationComplete: true, PostVerificationComplete: true,
		TimeoutSeconds: 30, VerifiedAbsent: 2, AcceptedAbsent: 2,
		RemovalConfirmed: true,
	}
	nodeA.Delete.Phases.PreValidationSeconds = float64Pointer(0.4)
	nodeA.Delete.Phases.PostVerificationSeconds = float64Pointer(0.8)

	nodeB := deleteNodeMetric("batch", 2, 1, 2)
	nodeB.Delete.Verification = DeleteVerificationMetrics{
		Enabled: true, PreValidationEnabled: true, PostVerificationEnabled: true,
		PreValidationComplete: true, PostVerificationComplete: true,
		TimeoutSeconds: 30, VerifiedAbsent: 1, StillPresent: 1,
		AcceptedAbsent: 1, AcceptedPresent: 1, CorrectnessFailures: 1, Residual: 1,
		RemovalConfirmed: false,
	}
	nodeB.Delete.Phases.PreValidationSeconds = float64Pointer(0.6)
	nodeB.Delete.Phases.PostVerificationSeconds = float64Pointer(0.5)

	got := NewMetricsAggregator().Aggregate(map[string]*PerformanceMetric{"a": nodeA, "b": nodeB})
	if got == nil || got.Delete == nil {
		t.Fatal("enabled DELETE verification metrics were not aggregated")
	}
	verification := got.Delete.Verification
	if verification.VerifiedAbsent != 3 || verification.StillPresent != 1 ||
		verification.AcceptedAbsent != 3 || verification.AcceptedPresent != 1 ||
		verification.CorrectnessFailures != 1 || verification.Residual != 1 ||
		verification.RemovalConfirmed {
		t.Fatalf("aggregate verification classifications = %+v", verification)
	}
	if got.Delete.Phases.PreValidationSeconds == nil || *got.Delete.Phases.PreValidationSeconds != 0.6 ||
		got.Delete.Phases.PostVerificationSeconds == nil || *got.Delete.Phases.PostVerificationSeconds != 0.8 {
		t.Fatalf("aggregate verification phase timings = %+v", got.Delete.Phases)
	}
}

func TestMetricsAggregatorPreservesStrictPreValidationAbortCompletion(t *testing.T) {
	strictAbortNode := func(preValidationFailures int64) *PerformanceMetric {
		node := deleteNodeMetric("single", 1, 1, 1)
		zero, pre, total := 0.0, 0.25, 0.25
		node.Delete.Requests = DeleteRequestMetrics{}
		node.Delete.Objects = DeleteObjectMetrics{Selected: 1, Unattempted: 1}
		node.Delete.Batches = DeleteBatchMetrics{ConfiguredSize: 1}
		node.Delete.Completion = DeleteCompletionMetrics{
			RequestPercent: 100, ObjectPercent: 100, TerminalReconciled: true,
		}
		node.Delete.Versions = DeleteVersionMetrics{CurrentKey: 1}
		node.Delete.Buckets = []DeleteBucketMetrics{{Bucket: "bucket-b", Selected: 1}}
		node.Delete.Phases = DeletePhaseMetrics{
			PreValidationSeconds: &pre, ScheduledDeleteSeconds: &zero,
			DrainSeconds: &zero, TotalWallSeconds: &total,
		}
		node.Delete.FailurePolicy.Outcome = deletemetrics.OutcomeFailed
		node.Delete.Timing.Latency = testTimingStat(0, 0)
		node.Delete.Timing.Duration = testTimingStat(0, 0)
		node.Delete.Verification = DeleteVerificationMetrics{
			Enabled: true, PreValidationEnabled: true, PostVerificationEnabled: true,
			PreValidationComplete: true, PostVerificationSkipped: true,
			TimeoutSeconds: 30, PreValidationFailures: preValidationFailures,
			Notice: deletemetrics.PostVerificationSkippedNotice,
		}
		return node
	}

	got := NewMetricsAggregator().Aggregate(map[string]*PerformanceMetric{
		"passing-slice": strictAbortNode(0),
		"failing-slice": strictAbortNode(1),
	})
	if got == nil || got.Delete == nil {
		t.Fatalf("strict pre-validation abort was not aggregated: %+v", got)
	}
	if got.Delete.Verification.PreValidationFailures != 1 ||
		!got.Delete.Verification.PostVerificationSkipped ||
		got.Delete.Verification.PostVerificationComplete ||
		got.Delete.Objects.Unattempted != 2 {
		t.Fatalf("distributed strict pre-validation abort = %+v", got.Delete)
	}
}

func float64Pointer(value float64) *float64 {
	return &value
}

func deleteNodeMetric(mode string, batchSize int, requests, objects int64) *PerformanceMetric {
	now := time.Now()
	deleteMetrics := testDeleteMetrics(mode, batchSize, requests, objects)
	bucket := "bucket-b"
	if objects > 100 {
		bucket = "bucket-a"
	}
	deleteMetrics.Buckets = []DeleteBucketMetrics{{
		Bucket: bucket, Selected: objects, Attempted: objects, Accepted: objects, Failed: 0,
	}}
	deleteMetrics.Batches.FullBatchCount = objects / int64(batchSize)
	deleteMetrics.Batches.PartialBatchCount = 0
	if objects%int64(batchSize) != 0 {
		deleteMetrics.Batches.PartialBatchCount = 1
	}
	return &PerformanceMetric{
		MetricsSchema:        4,
		DeleteDetailExpected: true,
		Scope:                "node",
		Role:                 "worker",
		ClusterID:            "cluster-test",
		RunID:                "run-1",
		StepID:               "delete-step",
		OpType:               "DELETE",
		OpsPerSec:            requests,
		SuccessCount:         requests,
		Delete:               deleteMetrics,
		SampleTimestamp:      now,
		Timestamp:            now,
	}
}

func testDeleteMetrics(mode string, batchSize int, requests, objects int64) *DeleteMetrics {
	scheduled := 1.5
	drain := 0.5
	total := 2.0
	return &DeleteMetrics{
		Units: DeleteMetricUnits{Requests: "logical_api_requests", Objects: "object_identities", Batches: "logical_api_requests"},
		Requests: DeleteRequestMetrics{
			Attempted: requests, FullSuccess: requests, PerSecond: float64(requests),
		},
		Objects: DeleteObjectMetrics{
			Selected: objects, Attempted: objects, Accepted: objects, PerSecond: float64(objects),
		},
		Batches: DeleteBatchMetrics{
			ConfiguredSize: batchSize, ActualRequestCount: requests, ActualObjectCount: objects,
			MeanObjectsPerRequest: float64(objects) / float64(requests), FullBatchCount: requests, FullBatchPercent: 100,
		},
		Completion: DeleteCompletionMetrics{RequestPercent: 100, ObjectPercent: 100, TerminalReconciled: true},
		Versions:   DeleteVersionMetrics{CurrentKey: objects},
		Phases: DeletePhaseMetrics{
			ScheduledDeleteSeconds: &scheduled, DrainSeconds: &drain, TotalWallSeconds: &total,
		},
		Identity:      DeleteResultIdentity{Mode: mode, ConfiguredBatchSize: batchSize, SelectionOrder: "canonical"},
		FailurePolicy: DeleteFailurePolicy{Mode: "fixed", Outcome: deletemetrics.OutcomeCompletedCleanly, MaxFailedObjects: 100000, GraceSeconds: 30},
		Timing: DeleteTimingMetrics{
			LatencyDefinition:  "first_request_byte_sent_to_first_response_byte_received",
			DurationDefinition: "request_formulation_to_last_response_byte_received",
		},
		Performance: DeletePerformanceApplicability{
			ObjectSize: "not_applicable", DataMoved: "not_applicable", Bandwidth: "not_applicable", TTFB: "not_applicable",
		},
		OutcomeTerminology: "accepted",
		Verification: DeleteVerificationMetrics{
			Enabled: false, RemovalConfirmed: false,
			Notice: "Verification disabled; results describe logical DELETE API outcomes, not confirmed object removal.",
		},
		TerminalReconciled: true,
	}
}

func testTimingStat(count, value int64) *JSONTimingStat {
	return &JSONTimingStat{
		Count: count, MeanUs: float64(value), MinUs: value, P50Us: value,
		P90Us: value, P99Us: value, P999Us: value, MaxUs: value,
	}
}
