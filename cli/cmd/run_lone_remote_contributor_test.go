/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/driverstats"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// A lone remote engine runs through the multi-host orchestrator, so its run resolves contributors
// from the orchestrator, never from the one-host fallback. Once that engine has started, the
// orchestrator answers with the entry's own slice (cli/tui tests prove it); these tests give the
// auto-results path that answer and check what the run stores and how it exits.
func loneRemoteEngineContributors() ([]string, error) {
	return []string{constants.MetricsLocalContributorID}, nil
}

func twoHostFleetContributors() ([]string, error) {
	return []string{constants.MetricsLocalContributorID, "192.0.2.25:1099"}, nil
}

// standaloneEngineFleet serves the fleet rows of an engine without workers: one contributor, the
// entry's own slice, with terminal driver statistics for each step.
func standaloneEngineFleet(t *testing.T, runID int64, opTypes map[string]string) *httptest.Server {
	t.Helper()
	rows := make([]any, 0, len(opTypes))
	for stepID, opType := range opTypes {
		rows = append(rows, map[string]any{
			"scope": "fleet", "role": "aggregate", "run_id": "42", "cluster_id": constants.RunClusterID(runID),
			"step_id": stepID, "op_type": opType, "terminal": true, "partial": false,
			"nodes_count": 1, "contributors_present": []string{constants.MetricsLocalContributorID},
			"driver_stats": map[string]any{
				"driver": "s3-rdma", "counters": map[string]int64{"rdma_transferred": 8},
				"reporting": 1, "partial": false,
			},
		})
	}
	mux := http.NewServeMux()
	mux.HandleFunc(constants.SptFleetMetricsEndpoint, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(rows)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

type deleteCaptureCall struct {
	stepIDs, contributorIDs []string
	allowNodeFallback       bool
}

// stubCleanDeleteCapture replaces the terminal DELETE capture with one that reports every
// requested step as fully deleted, and records its calls.
func stubCleanDeleteCapture(t *testing.T) *[]deleteCaptureCall {
	t.Helper()
	original := captureDeleteMetricsFunc
	t.Cleanup(func() { captureDeleteMetricsFunc = original })
	calls := &[]deleteCaptureCall{}
	captureDeleteMetricsFunc = func(
		_ context.Context, _ string, _ int64, stepIDs, contributorIDs []string,
		allowNodeFallback bool, _ map[string]*deletemetrics.Metrics,
	) (map[string]*deletemetrics.Metrics, error) {
		*calls = append(*calls, deleteCaptureCall{
			stepIDs:           append([]string(nil), stepIDs...),
			contributorIDs:    append([]string(nil), contributorIDs...),
			allowNodeFallback: allowNodeFallback,
		})
		captured := make(map[string]*deletemetrics.Metrics, len(stepIDs))
		for _, stepID := range stepIDs {
			captured[stepID] = &deletemetrics.Metrics{
				Objects:            deletemetrics.Objects{Selected: 20, Attempted: 20, Accepted: 20},
				TerminalReconciled: true,
			}
		}
		return captured, nil
	}
	return calls
}

func TestAutoResultsOfALoneRemoteDeleteExitZeroAndStoreDriverStats(t *testing.T) {
	const runID = int64(42)
	tests := map[string]struct {
		planned, runtime []string
		opTypes          map[string]string
		deleteStep       string
	}{
		"seeded DELETE": {
			planned: []string{"mt-001-20261111.153331.000-seed", "mt-002-20261111.153331.000-delete"},
			runtime: []string{"mt-001-20261111.153331.233-seed", "mt-002-20261111.153331.233-delete"},
			opTypes: map[string]string{
				"mt-001-20261111.153331.233-seed":   "CREATE",
				"mt-002-20261111.153331.233-delete": "DELETE",
			},
			deleteStep: "mt-002-20261111.153331.233-delete",
		},
		"--items-file DELETE": {
			planned:    []string{"mt-001-20261111.153331.000-delete"},
			runtime:    []string{"mt-001-20261111.153331.233-delete"},
			opTypes:    map[string]string{"mt-001-20261111.153331.233-delete": "DELETE"},
			deleteStep: "mt-001-20261111.153331.233-delete",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			server := standaloneEngineFleet(t, runID, tc.opTypes)
			installAutoResultsFakes(t, tc.runtime)
			deleteCalls := stubCleanDeleteCapture(t)
			stub := captureDriverStatsFunc
			t.Cleanup(func() { captureDriverStatsFunc = stub })
			captureDriverStatsFunc = engineCaptureDriverStats

			resultsRoot := t.TempDir()
			metadata := &runMetadata{
				ResultsRoot:        resultsRoot,
				WorkloadType:       WorkloadTypeDelete,
				ExpectedStepIDs:    tc.planned,
				Hosts:              []runHostMetadata{{Host: "wrk1.example"}},
				metricContributors: loneRemoteEngineContributors,
			}
			monitor := startAutoResultsMonitor(
				context.Background(), server.URL, "mt", resultsRoot, tc.planned, runID,
				false, nil, "9999", false, 0, "", metadata, io.Discard, io.Discard, "", nil)
			monitor.Arm()
			outcome := <-monitor.done

			want := []deleteCaptureCall{{
				stepIDs:           []string{tc.deleteStep},
				contributorIDs:    []string{constants.MetricsLocalContributorID},
				allowNodeFallback: true,
			}}
			if !reflect.DeepEqual(*deleteCalls, want) {
				t.Fatalf("DELETE capture calls = %+v, want %+v", *deleteCalls, want)
			}
			if outcome.DeleteMetricsErr != nil || outcome.DeleteTerminalErr != nil {
				t.Fatalf("DELETE outcome errors = %v / %v", outcome.DeleteMetricsErr, outcome.DeleteTerminalErr)
			}
			if err := resolveRunCompletionError(
				nil, outcome, true, scenario.Params{WorkloadType: scenario.WorkloadTypeDelete}); err != nil {
				t.Fatalf("clean lone-remote DELETE failed the run: %v", err)
			}
			stored, raw := readStoredDriverStats(t, resultsRoot)
			if stored.DriverStatsError != "" || len(stored.DriverStats) != len(tc.opTypes) {
				t.Fatalf("stored statistics = %+v error %q in %s", stored.DriverStats, stored.DriverStatsError, raw)
			}
			for stepID := range tc.opTypes {
				want := &driverstats.Stats{
					Driver: "s3-rdma", Counters: map[string]int64{"rdma_transferred": 8}, Reporting: 1,
				}
				if !reflect.DeepEqual(stored.DriverStats[stepID], want) {
					t.Fatalf("stored statistics of %s = %+v, want %+v", stepID, stored.DriverStats[stepID], want)
				}
			}
			if strings.Contains(raw, "driverStatsError") || stored.DeleteMetrics[tc.deleteStep] == nil {
				t.Fatalf("stored run metadata = %s", raw)
			}
		})
	}
}

// A real step failure on one remote host exits non-zero for that failure alone, exactly as the
// same failure does on two hosts.
func TestAutoResultsOfALoneRemoteDeleteReportAStepFailureLikeTwoHosts(t *testing.T) {
	const (
		runID      = int64(42)
		planned    = "mt-001-20261111.153331.000-delete"
		deleteStep = "mt-001-20261111.153331.233-delete"
		failure    = "failed to start metadata-mode step slice 0"
	)
	runErr := errors.New("owned engine run failed: step " + deleteStep + " (execution): " + failure)
	messages := map[string]string{}
	for name, topology := range map[string]struct {
		hosts        []runHostMetadata
		contributors func() ([]string, error)
	}{
		"one remote host": {
			hosts:        []runHostMetadata{{Host: "wrk1.example"}},
			contributors: loneRemoteEngineContributors,
		},
		"two hosts": {
			hosts:        []runHostMetadata{{Host: "wrk1.example"}, {Host: "wrk2.example"}},
			contributors: twoHostFleetContributors,
		},
	} {
		t.Run(name, func(t *testing.T) {
			installAutoResultsFakes(t, []string{deleteStep})
			newRunTrackerFunc = func(string) autoResultsRunTracker {
				return &fakeRunTracker{result: &portcheck.RunResult{
					FinalState: constants.StateFailed, FailureStepID: deleteStep,
					FailureCategory: "execution", FailureMessage: failure,
				}}
			}
			stubCleanDeleteCapture(t)
			stubDriverStatsCapture(t, nil, nil)

			resultsRoot := t.TempDir()
			metadata := &runMetadata{
				ResultsRoot:        resultsRoot,
				WorkloadType:       WorkloadTypeDelete,
				ExpectedStepIDs:    []string{planned},
				Hosts:              topology.hosts,
				metricContributors: topology.contributors,
			}
			monitor := startAutoResultsMonitor(
				context.Background(), "http://wrk1.example", "mt", resultsRoot, []string{planned}, runID,
				false, nil, "9999", false, 0, "", metadata, io.Discard, io.Discard, "", nil)
			monitor.Arm()
			outcome := <-monitor.done

			err := resolveRunCompletionError(
				runErr, outcome, true, scenario.Params{WorkloadType: scenario.WorkloadTypeDelete})
			var exitErr *ExitCodeError
			if !errors.As(err, &exitErr) || exitErr.Code != constants.ExitCodeWorkloadFailure {
				t.Fatalf("resolveRunCompletionError() = %#v, want a workload failure", err)
			}
			if strings.Contains(err.Error(), "contributor identity") ||
				strings.Contains(err.Error(), "terminal DELETE metrics are incomplete") ||
				!strings.HasPrefix(err.Error(), "step "+deleteStep+" failed (execution): "+failure+";") {
				t.Fatalf("run error = %q, want the step failure as its only cause", err)
			}
			messages[name] = err.Error()
		})
	}
	if messages["one remote host"] != messages["two hosts"] {
		t.Fatalf("one remote host reports %q, two hosts %q", messages["one remote host"], messages["two hosts"])
	}
}
