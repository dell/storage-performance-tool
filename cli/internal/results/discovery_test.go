package results

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
)

func TestDiscoverStepIDs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
          {"step_id":"mt-001-20250101.000000.000-create"},
          {"step_id":"mt-002-20250101.000000.000-delete"}
        ]`))
	}))
	defer srv.Close()

	ids, err := DiscoverStepIDs(srv.URL)
	if err != nil {
		t.Fatalf("DiscoverStepIDs error: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %d", len(ids))
	}
}

func TestDiscoverFleetStepIDs(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/fleet/json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
          {"step_id":"fleet-001-20250101.000000.000-create"},
          {"step_id":"fleet-002-20250101.000000.000-delete"}
        ]`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ids, err := DiscoverFleetStepIDs(srv.URL)
	if err != nil {
		t.Fatalf("DiscoverFleetStepIDs error: %v", err)
	}
	if len(ids) != 2 {
		t.Fatalf("expected 2 ids, got %d", len(ids))
	}
	if ids[0] != "fleet-001-20250101.000000.000-create" {
		t.Fatalf("expected first id fleet-001-20250101.000000.000-create, got %s", ids[0])
	}
	if ids[1] != "fleet-002-20250101.000000.000-delete" {
		t.Fatalf("expected second id fleet-002-20250101.000000.000-delete, got %s", ids[1])
	}
}

func TestDiscoverFleetStepIDs_NotFound(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/fleet/json", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ids, err := DiscoverFleetStepIDs(srv.URL)
	if err != nil {
		t.Fatalf("expected nil error on 404, got: %v", err)
	}
	if ids != nil {
		t.Fatalf("expected nil ids on 404, got: %v", ids)
	}
}

func TestDiscoverStepIDs_EmptyStepIDs(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
          {"step_id":""},
          {"step_id":""},
          {"step_id":""}
        ]`))
	}))
	defer srv.Close()

	ids, err := DiscoverStepIDs(srv.URL)
	if err != nil {
		t.Fatalf("DiscoverStepIDs error: %v", err)
	}
	if len(ids) != 0 {
		t.Fatalf("expected 0 ids, got %d: %v", len(ids), ids)
	}
}

func TestDiscoverStepIDs_DeduplicatesAndSorts(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`[
          {"step_id":"zebra-step"},
          {"step_id":"alpha-step"},
          {"step_id":"zebra-step"},
          {"step_id":"middle-step"},
          {"step_id":"alpha-step"}
        ]`))
	}))
	defer srv.Close()

	ids, err := DiscoverStepIDs(srv.URL)
	if err != nil {
		t.Fatalf("DiscoverStepIDs error: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 unique ids, got %d: %v", len(ids), ids)
	}
	expected := []string{"alpha-step", "middle-step", "zebra-step"}
	for i, want := range expected {
		if ids[i] != want {
			t.Fatalf("ids[%d] = %q, want %q", i, ids[i], want)
		}
	}
}

func TestDiscoverFleetStepIDs_ServerError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/fleet/json", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	ids, err := DiscoverFleetStepIDs(srv.URL)
	if err == nil {
		t.Fatalf("expected error on 500, got nil with ids: %v", ids)
	}
	if ids != nil {
		t.Fatalf("expected nil ids on error, got: %v", ids)
	}
	if !strings.Contains(err.Error(), "/metrics/fleet/json status: 500") {
		t.Fatalf("server error diagnostic = %q", err)
	}
}

func TestDiscoverStepIDsForRunFiltersNodeAndFleetRetainedMetrics(t *testing.T) {
	mux := http.NewServeMux()
	response := `[
      {"run_id":"16","step_id":"prior-verify"},
      {"run_id":"17","step_id":"current-verify"},
      {"run_id":17,"step_id":"current-create"}
    ]`
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(response))
	})
	mux.HandleFunc("/metrics/fleet/json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(response))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	for _, discover := range []struct {
		name string
		fn   func(context.Context, string, int64) ([]string, error)
	}{
		{name: "node", fn: DiscoverStepIDsForRunContext},
		{name: "fleet", fn: DiscoverFleetStepIDsForRunContext},
	} {
		t.Run(discover.name, func(t *testing.T) {
			ids, err := discover.fn(context.Background(), server.URL, 17)
			if err != nil {
				t.Fatal(err)
			}
			want := []string{"current-create", "current-verify"}
			if !reflect.DeepEqual(ids, want) {
				t.Fatalf("filtered IDs = %v, want %v", ids, want)
			}
		})
	}
}

func TestCaptureTerminalDeleteMetricsPrefersCompleteFleetV4ForCurrentRun(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/fleet/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]any{
			capturedDeleteStep(17, "spt-run-17", "delete-current", "fleet", "aggregate", validTerminalDeleteMetrics()),
			capturedDeleteStep(16, "spt-run-16", "delete-prior", "fleet", "aggregate", nil),
		})
	})
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]any{
			capturedDeleteStep(17, "spt-run-17", "delete-current", "node", "worker", validTerminalDeleteMetrics()),
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	metrics, err := CaptureTerminalDeleteMetricsForRunContext(
		context.Background(), server.URL, 17, []string{"delete-current"},
		[]string{"local", "node-a"}, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 || metrics["delete-current"] == nil || metrics["delete-current"].Objects.Accepted != 2 {
		t.Fatalf("captured DELETE metrics = %+v", metrics)
	}
}

func TestCaptureTerminalDeleteMetricsFallsBackToNodeWhenFleetIsUnavailable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/fleet/json", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]any{
			capturedDeleteStep(17, "spt-run-17", "delete-current", "node", "worker", validTerminalDeleteMetrics()),
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	metrics, err := CaptureTerminalDeleteMetricsForRunContext(
		context.Background(), server.URL, 17, []string{"delete-current"},
		[]string{"local"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if len(metrics) != 1 || metrics["delete-current"].Objects.Accepted != 2 {
		t.Fatalf("node fallback DELETE metrics = %+v", metrics)
	}
}

func TestCaptureTerminalDeleteMetricsRejectsInvalidNodeFallbackRole(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/fleet/json", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]any{
			capturedDeleteStep(17, "spt-run-17", "delete-current", "node", "aggregate", validTerminalDeleteMetrics()),
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	if _, err := CaptureTerminalDeleteMetricsForRunContext(
		context.Background(), server.URL, 17, []string{"delete-current"},
		[]string{"local"}, true); err == nil {
		t.Fatal("single-node fallback accepted an aggregate role on the node endpoint")
	}
}

func TestCaptureTerminalDeleteMetricsRejectsNodeFallbackForMultiNodeRun(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/fleet/json", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	if _, err := CaptureTerminalDeleteMetricsForRunContext(
		context.Background(), server.URL, 17, []string{"delete-current"},
		[]string{"local", "node-a"}, false); err == nil {
		t.Fatal("multi-node run accepted an unproven node fallback")
	}
}

func TestCaptureTerminalDeleteMetricsRequiresExactExpectedContributorIdentitySet(t *testing.T) {
	for name, contributors := range map[string][]string{
		"missing":   nil,
		"empty":     {"local", " "},
		"duplicate": {"local", "local"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := CaptureTerminalDeleteMetricsForRunContext(
				context.Background(), "http://127.0.0.1:1", 17,
				[]string{"delete-current"}, contributors, false); err == nil {
				t.Fatal("invalid expected contributor identity set was accepted")
			}
		})
	}
}

func TestCaptureTerminalDeleteMetricsFailsClosedOnInvalidAuthoritativeModel(t *testing.T) {
	tests := []struct {
		name    string
		payload func() []any
	}{
		{name: "empty", payload: func() []any { return nil }},
		{name: "wrong cluster", payload: func() []any {
			return []any{capturedDeleteStep(17, "other-cluster", "delete-current", "fleet", "aggregate", validTerminalDeleteMetrics())}
		}},
		{name: "partial", payload: func() []any {
			step := capturedDeleteStep(17, "spt-run-17", "delete-current", "fleet", "aggregate", validTerminalDeleteMetrics())
			step["partial"] = true
			return []any{step}
		}},
		{name: "missing expected node count", payload: func() []any {
			step := capturedDeleteStep(17, "spt-run-17", "delete-current", "fleet", "aggregate", validTerminalDeleteMetrics())
			delete(step, "nodes_count")
			return []any{step}
		}},
		{name: "missing contributor identities", payload: func() []any {
			step := capturedDeleteStep(17, "spt-run-17", "delete-current", "fleet", "aggregate", validTerminalDeleteMetrics())
			delete(step, "contributors_present")
			return []any{step}
		}},
		{name: "duplicate contributor identity", payload: func() []any {
			step := capturedDeleteStep(17, "spt-run-17", "delete-current", "fleet", "aggregate", validTerminalDeleteMetrics())
			step["contributors_present"] = []string{"local", "local"}
			return []any{step}
		}},
		{name: "incomplete contributor identity set", payload: func() []any {
			step := capturedDeleteStep(17, "spt-run-17", "delete-current", "fleet", "aggregate", validTerminalDeleteMetrics())
			step["contributors_present"] = []string{"local"}
			return []any{step}
		}},
		{name: "stale contributor identity", payload: func() []any {
			step := capturedDeleteStep(17, "spt-run-17", "delete-current", "fleet", "aggregate", validTerminalDeleteMetrics())
			step["contributors_present"] = []string{"local", "node-stale"}
			return []any{step}
		}},
		{name: "missing detail", payload: func() []any {
			return []any{capturedDeleteStep(17, "spt-run-17", "delete-current", "fleet", "aggregate", nil)}
		}},
		{name: "missing timing", payload: func() []any {
			model := validTerminalDeleteMetrics()
			model.Timing.Latency = nil
			return []any{capturedDeleteStep(17, "spt-run-17", "delete-current", "fleet", "aggregate", model)}
		}},
		{name: "mismatched batch identity", payload: func() []any {
			model := validTerminalDeleteMetrics()
			model.Identity.ConfiguredBatchSize++
			return []any{capturedDeleteStep(17, "spt-run-17", "delete-current", "fleet", "aggregate", model)}
		}},
		{name: "invalid timing distribution", payload: func() []any {
			model := validTerminalDeleteMetrics()
			model.Timing.Latency.P90Us = 4
			return []any{capturedDeleteStep(17, "spt-run-17", "delete-current", "fleet", "aggregate", model)}
		}},
		{name: "unbounded named buckets", payload: func() []any {
			model := validTerminalDeleteMetrics()
			for i := 0; i < deletemetrics.MaxBucketMetrics; i++ {
				model.Buckets = append(model.Buckets, deletemetrics.Bucket{Bucket: fmt.Sprintf("empty-%d", i)})
			}
			return []any{capturedDeleteStep(17, "spt-run-17", "delete-current", "fleet", "aggregate", model)}
		}},
		{name: "duplicate", payload: func() []any {
			return []any{
				capturedDeleteStep(17, "spt-run-17", "delete-current", "fleet", "aggregate", validTerminalDeleteMetrics()),
				capturedDeleteStep(17, "spt-run-17", "delete-current", "fleet", "aggregate", validTerminalDeleteMetrics()),
			}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(test.payload())
			}))
			defer server.Close()
			if _, err := CaptureTerminalDeleteMetricsForRunContext(
				context.Background(), server.URL, 17, []string{"delete-current"},
				[]string{"local", "node-a"}, false); err == nil {
				t.Fatal("invalid authoritative terminal model was accepted")
			}
		})
	}
}

func capturedDeleteStep(
	runID int64,
	clusterID string,
	stepID string,
	scope string,
	role string,
	model *deletemetrics.Metrics,
) map[string]any {
	step := map[string]any{
		"metrics_schema":         4,
		"delete_detail_expected": true,
		"scope":                  scope,
		"role":                   role,
		"run_id":                 runID,
		"cluster_id":             clusterID,
		"step_id":                stepID,
		"op_type":                "DELETE",
		"terminal":               true,
		"partial":                false,
		"delete":                 model,
	}
	if scope == "fleet" {
		step["nodes_count"] = 2
		step["contributors_present"] = []string{"local", "node-a"}
	}
	return step
}

func validTerminalDeleteMetrics() *deletemetrics.Metrics {
	scheduled := 1.0
	drain := 0.25
	totalWall := 1.5
	return &deletemetrics.Metrics{
		Units: deletemetrics.Units{
			Requests: deletemetrics.RequestUnit,
			Objects:  deletemetrics.ObjectUnit,
			Batches:  deletemetrics.RequestUnit,
		},
		Requests: deletemetrics.Requests{Attempted: 1, FullSuccess: 1, PerSecond: 1},
		Objects:  deletemetrics.Objects{Selected: 2, Attempted: 2, Accepted: 2, PerSecond: 2},
		Batches: deletemetrics.Batches{
			ConfiguredSize: 2, ActualRequestCount: 1, ActualObjectCount: 2,
			MeanObjectsPerRequest: 2, FullBatchCount: 1, FullBatchPercent: 100,
		},
		Completion: deletemetrics.Completion{RequestPercent: 100, ObjectPercent: 100, TerminalReconciled: true},
		Versions:   deletemetrics.Versions{CurrentKey: 2},
		Buckets:    []deletemetrics.Bucket{{Bucket: "bucket", Selected: 2, Attempted: 2, Accepted: 2}},
		Phases: deletemetrics.Phases{
			ScheduledDeleteSeconds: &scheduled,
			DrainSeconds:           &drain,
			TotalWallSeconds:       &totalWall,
		},
		Identity: deletemetrics.Identity{Mode: "batch", ConfiguredBatchSize: 2, SelectionOrder: "canonical"},
		FailurePolicy: deletemetrics.FailurePolicy{
			Mode: "fixed", MaxFailedObjects: 100000,
			Outcome: deletemetrics.OutcomeCompletedCleanly,
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
			ObjectSize: deletemetrics.NotApplicable,
			DataMoved:  deletemetrics.NotApplicable,
			Bandwidth:  deletemetrics.NotApplicable,
			TTFB:       deletemetrics.NotApplicable,
		},
		OutcomeTerminology: deletemetrics.OutcomeAccepted,
		Verification: deletemetrics.Verification{
			Notice: deletemetrics.VerificationNotice,
		},
		TerminalReconciled: true,
	}
}

func TestValidateTerminalRejectsConfiguredFullBatchReportedAsPartial(t *testing.T) {
	model := validTerminalDeleteMetrics()
	model.Batches.FullBatchCount = 0
	model.Batches.PartialBatchCount = 1
	model.Batches.FullBatchPercent = 0
	if err := deletemetrics.ValidateTerminal(model); err == nil {
		t.Fatal("configured-size batch reported as partial was accepted")
	}
}

func TestValidateTerminalRejectsImpossiblePerBucketLifecycle(t *testing.T) {
	model := validTerminalDeleteMetrics()
	model.Buckets = []deletemetrics.Bucket{
		{Bucket: "bucket-a", Selected: 1, Attempted: 2, Accepted: 2},
		{Bucket: "bucket-b", Selected: 1},
	}
	if err := deletemetrics.ValidateTerminal(model); err == nil {
		t.Fatal("per-bucket attempted count above selection was accepted")
	}

	model = validTerminalDeleteMetrics()
	model.Buckets = []deletemetrics.Bucket{
		{Bucket: "bucket-a", Selected: 2, Attempted: 1, Accepted: 2},
		{Bucket: "bucket-b", Attempted: 1},
	}
	if err := deletemetrics.ValidateTerminal(model); err == nil {
		t.Fatal("per-bucket terminal outcomes above attempts were accepted")
	}
}

func TestCaptureTerminalDeleteMetricsRejectsInvalidTerminalContract(t *testing.T) {
	tests := map[string]func(*deletemetrics.Metrics){
		"unknown failure policy": func(model *deletemetrics.Metrics) {
			model.FailurePolicy.Mode = "unbounded"
		},
		"missing terminal outcome": func(model *deletemetrics.Metrics) {
			model.FailurePolicy.Outcome = ""
		},
		"running terminal outcome": func(model *deletemetrics.Metrics) {
			model.FailurePolicy.Outcome = deletemetrics.OutcomeRunning
		},
		"inconsistent policy failures": func(model *deletemetrics.Metrics) {
			model.FailurePolicy.OperationalFailedObjects = 1
		},
		"inconsistent batch mean": func(model *deletemetrics.Metrics) {
			model.Batches.MeanObjectsPerRequest = 3
		},
		"single mode with batch size": func(model *deletemetrics.Metrics) {
			model.Identity.Mode = "single"
		},
		"unverified removal confirmation": func(model *deletemetrics.Metrics) {
			model.Verification.RemovalConfirmed = true
		},
		"missing scheduled phase": func(model *deletemetrics.Metrics) {
			model.Phases.ScheduledDeleteSeconds = nil
		},
		"missing drain phase": func(model *deletemetrics.Metrics) {
			model.Phases.DrainSeconds = nil
		},
		"missing total wall phase": func(model *deletemetrics.Metrics) {
			model.Phases.TotalWallSeconds = nil
		},
		"premature pre-validation phase": func(model *deletemetrics.Metrics) {
			value := 0.5
			model.Phases.PreValidationSeconds = &value
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			model := validTerminalDeleteMetrics()
			mutate(model)
			if err := deletemetrics.ValidateTerminal(model); err == nil {
				t.Fatal("invalid terminal DELETE contract was accepted")
			}
		})
	}
}
