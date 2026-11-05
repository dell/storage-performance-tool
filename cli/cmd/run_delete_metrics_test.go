package cmd

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/results"
)

// The steps of the staged seeded DELETE run: planned, then as the engine names them.
const (
	stagedPlannedSeed    = "mt-001-20260823.100000.000-seed"
	stagedPlannedDelete  = "mt-002-20260823.100000.000-delete"
	stagedPlannedCleanup = "mt-003-20260823.100000.000-cleanup"
	stagedSeed           = "mt-001-20260823.100000.002-seed"
	stagedMeasured       = "mt-002-20260823.100000.002-delete"
	stagedCleanup        = "mt-003-20260823.100000.002-cleanup"
	stagedBucket         = "staged-bucket"
)

var stagedDeleteSteps = []string{stagedSeed, stagedMeasured, stagedCleanup}

// stagedLocalHosts is a run on the CLI's own host.
var stagedLocalHosts = []runHostMetadata{{Host: constants.MetricsLocalContributorID, IsLocal: true}}

// A seeded DELETE run's cleanup can outlast the engine's retention of the measured step's row.
// The measured step's metrics are stored all the same, from the row seen while the cleanup ran.
func TestAutoResultsStoresTheDeleteMetricsOfAStepWhoseRowTheEngineDroppedDuringCleanup(t *testing.T) {
	measured, cleanup := stagedMeasured, stagedCleanup
	undecided := stagedDeleteMetrics()
	undecided.FailurePolicy.Outcome = deletemetrics.OutcomeRunning
	outcome, stored, raw := monitorStagedDelete(t, [][]any{
		// The measured step is running.
		{stagedDeleteRow(measured, false, nil)},
		// It has ended, and the engine has not published its failure-budget outcome yet.
		{stagedDeleteRow(measured, true, undecided)},
		// The outcome is published; the cleanup is running.
		{stagedDeleteRow(measured, true, stagedDeleteMetrics()), stagedDeleteRow(cleanup, false, nil)},
		// The retention period of the measured row is over; the cleanup has ended.
		{stagedDeleteRow(cleanup, true, nil)},
	})

	if outcome.DeleteMetricsErr != nil || outcome.ArtifactErr != nil || outcome.DeleteTerminalErr != nil {
		t.Fatalf("outcome = DELETE metrics %v, artifacts %v, terminal %v",
			outcome.DeleteMetricsErr, outcome.ArtifactErr, outcome.DeleteTerminalErr)
	}
	want := map[string]*deletemetrics.Metrics{measured: stagedDeleteMetrics()}
	if !reflect.DeepEqual(stored.DeleteMetrics, want) {
		t.Fatalf("stored DELETE metrics in %s, want the measured step's", raw)
	}
}

// A row that ends unsettled and is gone before its outcome was read leaves the step without
// evidence, and the capture still says so.
func TestAutoResultsRefusesTheDeleteMetricsOfAStepWhoseSettledRowWasNeverRead(t *testing.T) {
	measured, cleanup := stagedMeasured, stagedCleanup
	undecided := stagedDeleteMetrics()
	undecided.FailurePolicy.Outcome = deletemetrics.OutcomeRunning
	outcome, stored, raw := monitorStagedDelete(t, [][]any{
		{stagedDeleteRow(measured, true, undecided), stagedDeleteRow(cleanup, false, nil)},
		{stagedDeleteRow(cleanup, true, nil)},
	})

	if outcome.DeleteMetricsErr == nil ||
		!strings.Contains(outcome.DeleteMetricsErr.Error(), "missing authoritative terminal DELETE step") {
		t.Fatalf("DeleteMetricsErr = %v, want the measured step missing", outcome.DeleteMetricsErr)
	}
	if stored.DeleteMetrics != nil {
		t.Fatalf("stored DELETE metrics in %s, want none", raw)
	}
}

func TestDeleteMetricsCollectorKeepsTheLatestJudgementOfEachStep(t *testing.T) {
	const step = stagedMeasured
	original := observeDeleteMetricsFunc
	t.Cleanup(func() { observeDeleteMetricsFunc = original })
	model := stagedDeleteMetrics()
	observations := []*results.DeleteMetricsObservation{
		{Steps: map[string]*deletemetrics.Metrics{step: model}},
		nil,
		{Refused: map[string]error{step: errors.New("incomplete contributor evidence")}},
	}
	var calls []string
	observeDeleteMetricsFunc = func(
		_ context.Context, _ string, _ int64, stepIDs, _ []string, _ bool,
	) (*results.DeleteMetricsObservation, error) {
		calls = append(calls, strings.Join(stepIDs, ","))
		next := observations[0]
		observations = observations[1:]
		if next == nil {
			return nil, errors.New("connection refused")
		}
		return next, nil
	}
	metadata := &runMetadata{
		WorkloadType:    WorkloadTypeDelete,
		ExpectedStepIDs: []string{stagedPlannedDelete},
		Hosts:           stagedLocalHosts,
	}
	collector := &deleteMetricsCollector{}

	collector.observe(context.Background(), "http://engine", 42, []string{step}, metadata)
	if got := collector.observed(); !reflect.DeepEqual(got, map[string]*deletemetrics.Metrics{step: model}) {
		t.Fatalf("observed after a settled row = %+v", got)
	}
	// A response that cannot be read says nothing about the step.
	collector.observe(context.Background(), "http://engine", 42, []string{step}, metadata)
	if got := collector.observed(); len(got) != 1 {
		t.Fatalf("observed after an unreadable response = %+v", got)
	}
	collector.observe(context.Background(), "http://engine", 42, []string{step}, metadata)
	if got := collector.observed(); len(got) != 0 {
		t.Fatalf("observed after a refusal = %+v, want nothing kept", got)
	}
	// Not a DELETE run, or no runtime step bound to the planned DELETE yet: nothing is read.
	collector.observe(context.Background(), "http://engine", 42, []string{step},
		&runMetadata{WorkloadType: WorkloadTypeRead, Hosts: metadata.Hosts})
	collector.observe(context.Background(), "http://engine", 42, []string{stagedCleanup}, metadata)
	if want := []string{step, step, step}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("observation calls = %v, want %v", calls, want)
	}
	if (*deleteMetricsCollector)(nil).observed() != nil {
		t.Fatal("a nil collector observed something")
	}
}

// monitorStagedDelete monitors a single-host seeded DELETE run 42 with cleanup, whose engine
// reports the given fleet rows stage by stage, and returns its outcome and what it stored.
func monitorStagedDelete(t *testing.T, stages [][]any) (autoResultsOutcome, storedDriverStats, string) {
	t.Helper()
	original := observeDeleteMetricsFunc
	t.Cleanup(func() { observeDeleteMetricsFunc = original })
	observeDeleteMetricsFunc = engineObserveDeleteMetrics

	resultsRoot := t.TempDir()
	metadata := &runMetadata{
		ResultsRoot:     resultsRoot,
		WorkloadType:    WorkloadTypeDelete,
		ExpectedStepIDs: []string{stagedPlannedSeed, stagedPlannedDelete, stagedPlannedCleanup},
		Hosts:           stagedLocalHosts,
	}
	outcome := monitorStagedRun(t, stagedDeleteSteps, metadata.ExpectedStepIDs, metadata, stages)
	stored, raw := readStoredDriverStats(t, resultsRoot)
	return outcome, stored, raw
}

// stagedDeleteRow is the fleet row of a DELETE step of run 42 on a single engine, with the
// given DELETE detail.
func stagedDeleteRow(stepID string, terminal bool, model *deletemetrics.Metrics) map[string]any {
	row := stagedFleetRowWithoutStats(stepID, "DELETE", 0, terminal)
	row["metrics_schema"] = deletemetrics.SchemaVersion
	row["delete_detail_expected"] = true
	row["delete"] = model
	return row
}

// stagedDeleteMetrics is a complete terminal DELETE model of a step that deleted two objects.
func stagedDeleteMetrics() *deletemetrics.Metrics {
	scheduled, drain, totalWall := 1.0, 0.25, 1.5
	return &deletemetrics.Metrics{
		Units: deletemetrics.Units{
			Requests: deletemetrics.RequestUnit, Objects: deletemetrics.ObjectUnit, Batches: deletemetrics.RequestUnit,
		},
		Requests: deletemetrics.Requests{Attempted: 1, FullSuccess: 1, PerSecond: 1},
		Objects:  deletemetrics.Objects{Selected: 2, Attempted: 2, Accepted: 2, PerSecond: 2},
		Batches: deletemetrics.Batches{
			ConfiguredSize: 2, ActualRequestCount: 1, ActualObjectCount: 2,
			MeanObjectsPerRequest: 2, FullBatchCount: 1, FullBatchPercent: 100,
		},
		Completion: deletemetrics.Completion{RequestPercent: 100, ObjectPercent: 100, TerminalReconciled: true},
		Versions:   deletemetrics.Versions{CurrentKey: 2},
		Buckets:    []deletemetrics.Bucket{{Bucket: stagedBucket, Selected: 2, Attempted: 2, Accepted: 2}},
		Phases: deletemetrics.Phases{
			ScheduledDeleteSeconds: &scheduled, DrainSeconds: &drain, TotalWallSeconds: &totalWall,
		},
		Identity: deletemetrics.Identity{Mode: "batch", ConfiguredBatchSize: 2, SelectionOrder: "canonical"},
		FailurePolicy: deletemetrics.FailurePolicy{
			Mode: "fixed", MaxFailedObjects: 100000, Outcome: deletemetrics.OutcomeCompletedCleanly,
		},
		Timing: deletemetrics.Timing{
			LatencyDefinition:  deletemetrics.LatencyDefinition,
			DurationDefinition: deletemetrics.DurationDefinition,
			Latency: &deletemetrics.TimingStat{
				Count: 1, MeanUs: 5, MinUs: 5, P50Us: 5, P90Us: 5, P99Us: 5, P999Us: 5, MaxUs: 5,
			},
			Duration: &deletemetrics.TimingStat{
				Count: 1, MeanUs: 7, MinUs: 7, P50Us: 7, P90Us: 7, P99Us: 7, P999Us: 7, MaxUs: 7,
			},
		},
		Performance: deletemetrics.Performance{
			ObjectSize: deletemetrics.NotApplicable, DataMoved: deletemetrics.NotApplicable,
			Bandwidth: deletemetrics.NotApplicable, TTFB: deletemetrics.NotApplicable,
		},
		OutcomeTerminology: deletemetrics.OutcomeAccepted,
		Verification:       deletemetrics.Verification{Notice: deletemetrics.VerificationNotice},
		TerminalReconciled: true,
	}
}
