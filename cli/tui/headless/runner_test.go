/*
Copyright © 2025 Dell Technologies
*/

package headless

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	resultsummary "github.com/dell/storage-performance-tool/cli/internal/results/summary"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
)

func TestNewHeadlessRunner(t *testing.T) {
	mockDocker := &tui.MockDockerManager{}

	tests := []struct {
		name        string
		options     HeadlessOptions
		wantErr     bool
		description string
	}{
		{
			name:        "Basic runner creation",
			options:     HeadlessOptions{},
			wantErr:     false,
			description: "Should create runner with default options",
		},
		{
			name: "Runner with valid trace file",
			options: HeadlessOptions{
				TraceFile: filepath.Join(t.TempDir(), "test.log"),
			},
			wantErr:     false,
			description: "Should create runner with trace file",
		},
		{
			name: "Runner with all options",
			options: HeadlessOptions{
				TraceFile:   filepath.Join(t.TempDir(), "full.log"),
				TraceAppend: true,
				Verbose:     true,
				JSONMode:    true,
				MetricsOnly: true,
				DryRun:      true,
			},
			wantErr:     false,
			description: "Should create runner with all options enabled",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			runner, err := NewHeadlessRunner(mockDocker, tt.options)

			if tt.wantErr && err == nil {
				t.Errorf("NewHeadlessRunner() expected error but got none for %s", tt.description)
			}

			if !tt.wantErr && err != nil {
				t.Errorf("NewHeadlessRunner() unexpected error: %v for %s", err, tt.description)
			}

			if runner != nil {
				defer runner.Close()

				// Verify options were set correctly
				if runner.verbose != tt.options.Verbose {
					t.Errorf("Expected verbose=%v, got %v", tt.options.Verbose, runner.verbose)
				}

				if runner.jsonMode != tt.options.JSONMode {
					t.Errorf("Expected jsonMode=%v, got %v", tt.options.JSONMode, runner.jsonMode)
				}

				if runner.metricsOnly != tt.options.MetricsOnly {
					t.Errorf("Expected metricsOnly=%v, got %v", tt.options.MetricsOnly, runner.metricsOnly)
				}

				if runner.dryRun != tt.options.DryRun {
					t.Errorf("Expected dryRun=%v, got %v", tt.options.DryRun, runner.dryRun)
				}

				// keepScenario is now set via ScenarioParams, not HeadlessOptions
				// So we don't test it here
			}
		})
	}
}

func TestHeadlessRunner_DryRun(t *testing.T) {
	// Create a temporary scenario file
	tempDir := t.TempDir()
	scenarioPath := filepath.Join(tempDir, "test-scenario.js")
	scenarioContent := `Load.config({"test": true}).run();`

	err := os.WriteFile(scenarioPath, []byte(scenarioContent), 0644)
	if err != nil {
		t.Fatalf("Failed to create test scenario file: %v", err)
	}

	mockDocker := &tui.MockDockerManager{}

	options := HeadlessOptions{
		DryRun: true,
	}

	runner, err := NewHeadlessRunner(mockDocker, options)
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Close()

	// Use mock scenario parameters to avoid endpoint validation errors
	params := scenario.ScenarioParams{
		WorkloadType: "mock",
		Threads:      1,
		ObjectSize:   "1MB",
		Duration:     "30s",
	}

	ctx := context.Background()
	err = runner.RunWithParams(ctx, "test-image", scenarioPath, params)

	if err != nil {
		t.Errorf("DryRun should not return error, got: %v", err)
	}

	// In dry run mode, no container should be started
	calls := mockDocker.GetScenarioCalls()
	if len(calls) != 0 {
		t.Errorf("Expected no container starts in dry run mode, got %d calls", len(calls))
	}
}

func TestHeadlessRunnerDryRunReportsIdentityWithoutGeneratedContent(t *testing.T) {
	const (
		accessKey      = "DRY_RUN_GENERATED_ACCESS_7f3a"
		secretKey      = "DRY_RUN_GENERATED_SECRET_91bc"
		overrideSecret = "DRY_RUN_GENERATED_OVERRIDE_c421"
	)
	params := scenario.ScenarioParams{
		WorkloadType:  "write",
		Endpoint:      "http://s3.example:9000",
		AccessKey:     accessKey,
		SecretKey:     secretKey,
		Bucket:        "qualification",
		Threads:       1,
		ObjectSize:    "1KB",
		ObjectCount:   1,
		BaseTimestamp: "20260803.120000.000",
		EngineOverrides: []string{
			"storage.auth.secret=" + overrideSecret,
		},
	}
	scenarioContent, err := scenario.GenerateScenario(params)
	if err != nil {
		t.Fatal(err)
	}
	defaultsContent, err := scenario.GenerateDefaults(params)
	if err != nil {
		t.Fatal(err)
	}
	tracePath := filepath.Join(t.TempDir(), "generated.trace")
	runner, err := NewHeadlessRunner(nil, HeadlessOptions{
		TraceFile: tracePath,
		DryRun:    true,
		APIPort:   "9999",
	})
	if err != nil {
		t.Fatal(err)
	}
	var runErr error
	stdout := captureHeadlessStdout(t, func() {
		runErr = runner.runDryModeWithParams("identity-image", "scenario.js", params)
	})
	if runErr != nil {
		t.Fatal(runErr)
	}
	if err = runner.Close(); err != nil {
		t.Fatal(err)
	}
	trace := readHeadlessTrace(t, tracePath)
	assertDryRunContentHidden(t, stdout, trace, accessKey, secretKey, overrideSecret)
	assertDryRunContentIdentity(t, stdout, trace, "Generated scenario content", []byte(scenarioContent))
	assertDryRunContentIdentity(t, stdout, trace, "Generated defaults configuration", defaultsContent)
	for _, output := range []string{stdout, trace} {
		if !strings.Contains(output, "Image: identity-image") ||
			!strings.Contains(output, "API Port: 9999") {
			t.Fatalf("dry-run output lost non-sensitive launch details: %s", output)
		}
	}
}

func TestHeadlessRunnerDryRunReportsIdentityWithoutProvidedContent(t *testing.T) {
	const (
		scenarioSecret = "DRY_RUN_PROVIDED_SCENARIO_7f3a"
		defaultsSecret = "DRY_RUN_PROVIDED_DEFAULTS_91bc"
	)
	scenarioContent := []byte("Load.run({credential: \"" + scenarioSecret + "\"});\n")
	defaultsContent := []byte("storage:\n  auth:\n    secret: " + defaultsSecret + "\n")
	originalScenario := append([]byte(nil), scenarioContent...)
	originalDefaults := append([]byte(nil), defaultsContent...)
	tracePath := filepath.Join(t.TempDir(), "provided.trace")
	runner, err := NewHeadlessRunner(nil, HeadlessOptions{
		TraceFile: tracePath,
		DryRun:    true,
		APIPort:   "9999",
	})
	if err != nil {
		t.Fatal(err)
	}
	stdout := captureHeadlessStdout(t, func() {
		runner.runDryModeWithContent(
			"identity-image", "scenario.js", scenarioContent, defaultsContent)
	})
	if err = runner.Close(); err != nil {
		t.Fatal(err)
	}
	trace := readHeadlessTrace(t, tracePath)
	assertDryRunContentHidden(t, stdout, trace, scenarioSecret, defaultsSecret)
	assertDryRunContentIdentity(t, stdout, trace, "Provided scenario content", scenarioContent)
	assertDryRunContentIdentity(t, stdout, trace, "Provided defaults configuration", defaultsContent)
	if !bytes.Equal(scenarioContent, originalScenario) || !bytes.Equal(defaultsContent, originalDefaults) {
		t.Fatal("dry-run identity reporting changed the exact submission bytes")
	}
}

func TestHeadlessRunner_TraceFile(t *testing.T) {
	oldArgs := os.Args
	os.Args = []string{"spt", "run", "write", "--secret-key", "verysecret", "--access-key=OPENKEY"}
	defer func() { os.Args = oldArgs }()

	tempDir := t.TempDir()
	traceFile := filepath.Join(tempDir, "trace.log")

	mockDocker := &tui.MockDockerManager{}

	options := HeadlessOptions{
		TraceFile: traceFile,
		DryRun:    true, // Use dry run to avoid actual container operations
	}

	runner, err := NewHeadlessRunner(mockDocker, options)
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Close()

	// Create scenario file
	scenarioPath := filepath.Join(tempDir, "scenario.js")
	err = os.WriteFile(scenarioPath, []byte("test"), 0644)
	if err != nil {
		t.Fatalf("Failed to create scenario file: %v", err)
	}

	// Use mock scenario parameters to avoid endpoint validation errors
	params := scenario.ScenarioParams{
		WorkloadType: "mock",
		Threads:      2,
		ObjectSize:   "512KB",
		Duration:     "15s",
	}

	ctx := context.Background()
	err = runner.RunWithParams(ctx, "test-image", scenarioPath, params)
	if err != nil {
		t.Errorf("Run should not return error, got: %v", err)
	}

	// Check that trace file was created and contains expected content
	if _, err := os.Stat(traceFile); os.IsNotExist(err) {
		t.Errorf("Trace file was not created")
	}

	content, err := os.ReadFile(traceFile)
	if err != nil {
		t.Errorf("Failed to read trace file: %v", err)
	}

	traceContent := string(content)

	// Check for header
	if !strings.Contains(traceContent, "# Trace file:") {
		t.Errorf("Trace file missing header")
	}
	if !strings.Contains(traceContent, "# Command:") {
		t.Errorf("Trace file missing command header")
	}
	if strings.Contains(traceContent, "verysecret") || strings.Contains(traceContent, "OPENKEY") {
		t.Errorf("Trace file leaked credential in header")
	}
	if !strings.Contains(traceContent, "--secret-key ***") || !strings.Contains(traceContent, "--access-key=***") {
		t.Errorf("Trace file should mask command credentials in header")
	}

	// Check for some expected log entries
	if !strings.Contains(traceContent, "[INIT]") {
		t.Errorf("Trace file missing INIT entries")
	}

	if !strings.Contains(traceContent, "[DRY-RUN]") {
		t.Errorf("Trace file missing DRY-RUN entries")
	}
}

func TestHeadlessRunner_MetricsOutput(t *testing.T) {
	mockDocker := &tui.MockDockerManager{}

	runner, err := NewHeadlessRunner(mockDocker, HeadlessOptions{})
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Close()

	// Test metric output formatting
	testMetric := tui.PerformanceMetric{
		OpsPerSec:       45,
		MeanLatency:     1024,
		OpType:          "CREATE",
		SuccessCount:    100,
		FailedCount:     0,
		ConcurrencyMean: 8.0,
		Timestamp:       time.Now(),
	}

	// This test verifies the output functions don't panic
	// In a real test environment, we'd capture the output
	runner.outputMetrics(testMetric)
	runner.outputMetricsJSON(testMetric)
}

func TestHeadlessRunner_MetricsOutputOmitsUnavailableTTFB(t *testing.T) {
	mockDocker := &tui.MockDockerManager{}
	tmpDir := t.TempDir()
	traceFile := filepath.Join(tmpDir, "metrics_trace.log")

	runner, err := NewHeadlessRunner(mockDocker, HeadlessOptions{TraceFile: traceFile})
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Close()

	runner.outputMetrics(tui.PerformanceMetric{
		OpsPerSec:       45,
		MeanLatency:     1024,
		OpType:          "CREATE",
		SuccessCount:    100,
		ConcurrencyMean: 8.0,
	})

	content, err := os.ReadFile(traceFile)
	if err != nil {
		t.Fatalf("Failed to read trace file: %v", err)
	}
	if strings.Contains(string(content), "ttfb=") {
		t.Fatalf("headless metrics output must omit unavailable TTFB, got %q", string(content))
	}
}

func TestHeadlessRunner_MetricsJSONOmitsUnavailableTTFB(t *testing.T) {
	mockDocker := &tui.MockDockerManager{}
	tmpDir := t.TempDir()
	traceFile := filepath.Join(tmpDir, "json_trace.log")

	runner, err := NewHeadlessRunner(mockDocker, HeadlessOptions{TraceFile: traceFile, JSONMode: true})
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Close()

	runner.outputMetricsJSON(tui.PerformanceMetric{
		OpsPerSec:       45,
		MeanLatency:     1024,
		OpType:          "CREATE",
		SuccessCount:    100,
		ConcurrencyMean: 8.0,
	})

	content, err := os.ReadFile(traceFile)
	if err != nil {
		t.Fatalf("Failed to read trace file: %v", err)
	}
	if strings.Contains(string(content), "ttfb_us") {
		t.Fatalf("headless JSON metrics output must omit unavailable TTFB, got %q", string(content))
	}
}

func TestHeadlessRunnerDeleteMetricsExposeAcceptedOutcomesAndNotApplicableFields(t *testing.T) {
	mockDocker := &tui.MockDockerManager{}
	traceFile := filepath.Join(t.TempDir(), "delete-metrics.log")
	runner, err := NewHeadlessRunner(mockDocker, HeadlessOptions{TraceFile: traceFile})
	if err != nil {
		t.Fatalf("create runner: %v", err)
	}
	defer runner.Close()

	metric := tui.PerformanceMetric{
		OpType: "DELETE", OpsPerSec: 3, SuccessCount: 1, FailedCount: 2,
		Partial: true, NodesCount: 2, NodesPresent: []string{"remote-node:1099"},
		ContributorsPresent: []string{"local"},
		Delete: &tui.DeleteMetrics{
			Units:      tui.DeleteMetricUnits{Requests: "logical_api_requests", Objects: "object_identities", Batches: "logical_api_requests"},
			Requests:   tui.DeleteRequestMetrics{Attempted: 3, FullSuccess: 1, Partial: 1, Failed: 1, PerSecond: 3},
			Objects:    tui.DeleteObjectMetrics{Selected: 175, Attempted: 175, Accepted: 170, Failed: 5, PerSecond: 175},
			Batches:    tui.DeleteBatchMetrics{ConfiguredSize: 100, ActualRequestCount: 3, ActualObjectCount: 175, MeanObjectsPerRequest: 58.333, FullBatchCount: 1, PartialBatchCount: 2, FullBatchPercent: 33.333},
			Completion: tui.DeleteCompletionMetrics{RequestPercent: 100, ObjectPercent: 100, TerminalReconciled: true},
			Versions:   tui.DeleteVersionMetrics{CurrentKey: 150, ExactVersion: 25},
			Buckets:    []tui.DeleteBucketMetrics{{Bucket: "bucket-a", Selected: 175, Attempted: 175, Accepted: 170, Failed: 5}},
			Identity:   tui.DeleteResultIdentity{Mode: "batch", ConfiguredBatchSize: 100, SelectionOrder: "canonical"},
			FailurePolicy: tui.DeleteFailurePolicy{
				Mode: "fixed", MaxFailedObjects: 100000,
				Outcome:                  deletemetrics.OutcomeCompletedWithinFailureBudget,
				OperationalFailedObjects: 3, ExcludedFailedObjects: 2,
			},
			Timing: tui.DeleteTimingMetrics{
				LatencyDefinition:  "first_request_byte_sent_to_first_response_byte_received",
				DurationDefinition: "request_formulation_to_last_response_byte_received",
				Latency:            &tui.JSONTimingStat{Count: 3, P50Us: 10, P90Us: 20, P99Us: 30, P999Us: 40},
				Duration:           &tui.JSONTimingStat{Count: 3, P50Us: 50, P90Us: 60, P99Us: 70, P999Us: 80},
			},
			Performance: tui.DeletePerformanceApplicability{
				ObjectSize: "not_applicable", DataMoved: "not_applicable",
				Bandwidth: "not_applicable", TTFB: "not_applicable",
			},
			OutcomeTerminology: "accepted",
			TerminalReconciled: true,
			Verification: tui.DeleteVerificationMetrics{
				Enabled: false,
				Notice:  "Verification disabled; results describe logical DELETE API outcomes, not confirmed object removal.",
			},
		},
	}
	runner.outputMetrics(metric)
	runner.outputMetricsJSON(metric)

	content, err := os.ReadFile(traceFile)
	if err != nil {
		t.Fatalf("read trace: %v", err)
	}
	got := string(content)
	for _, want := range []string{
		"accepted=170", "failed_objects=5", "policy=fixed", "Verification disabled",
		"units=requests:logical_api_requests,objects:object_identities,batches:logical_api_requests",
		"failure_budget_outcome=completed_within_failure_budget",
		"operational_failed_objects=3", "excluded_failed_objects=2",
		"object_size=N/A", "data_moved=N/A", "bandwidth=N/A", "ttfb=N/A",
		"mean_batch=58.333", "full_batch_pct=33.333", "current_keys=150", "exact_versions=25",
		"request_completion_pct=100.000", "bucket-a(selected:175", "object_latency=N/A",
		`latency_definition="first_request_byte_sent_to_first_response_byte_received"`, "terminal_reconciled=true",
		"latency_stats=count:3", "p999_us:40", "duration_stats=count:3", "p999_us:80",
		"partial=true", "nodes=2", "nodes_present=remote-node:1099", "contributors_present=local",
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("DELETE headless output missing %q:\n%s", want, got)
		}
	}
	if strings.Contains(got, "removed=") {
		t.Fatalf("unverified DELETE output claims removal:\n%s", got)
	}
	for _, want := range []string{
		`"delete":`, `"outcome_terminology":"accepted"`,
		`"object_size":"not_applicable"`, `"removal_confirmed":false`,
		`"partial":true`, `"nodes_count":2`, `"nodes_present":["remote-node:1099"]`,
		`"contributors_present":["local"]`,
	} {
		if !strings.Contains(got, want) {
			t.Fatalf("DELETE headless JSON output missing %q:\n%s", want, got)
		}
	}
}

func TestDeleteMetricsCrossViewFixtureKeepsEngineAPIHeadlessStoredAggregatePerNodeAndTUIConsistent(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate cross-view test source")
	}
	fixturePath := filepath.Clean(filepath.Join(
		filepath.Dir(sourceFile),
		"../../../engine/core/spt-base/src/test/resources/delete-metrics-v4-cross-view.json"))
	payload, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read engine/API cross-view fixture: %v", err)
	}
	api := tui.NewSptAPIClient("")
	fleet, err := api.ParseFleetJSONMetrics(string(payload))
	if err != nil || len(fleet) != 1 {
		t.Fatalf("parse authoritative engine API view: metrics=%+v err=%v", fleet, err)
	}
	nodes, err := api.ParseJSONMetrics(string(payload))
	if err != nil || len(nodes) != 2 {
		t.Fatalf("parse per-node engine API views: metrics=%+v err=%v", nodes, err)
	}
	byNode := make(map[string]*tui.PerformanceMetric, len(nodes))
	for _, node := range nodes {
		byNode[node.NodeID] = node
	}
	aggregate := tui.NewMetricsAggregator().Aggregate(byNode)
	if aggregate == nil || aggregate.Delete == nil {
		t.Fatal("aggregate view lost DELETE detail")
	}
	want := fleet[0].Delete
	if want.FailurePolicy.Outcome != deletemetrics.OutcomeCompletedWithinFailureBudget {
		t.Fatalf("engine fleet view lost controller outcome: %+v", want.FailurePolicy)
	}
	if aggregate.Delete.Units != want.Units ||
		aggregate.Delete.Requests != want.Requests ||
		aggregate.Delete.Objects != want.Objects ||
		aggregate.Delete.Batches != want.Batches {
		t.Fatalf("aggregate units/counters differ from engine fleet view:\naggregate=%+v\nfleet=%+v",
			aggregate.Delete, want)
	}
	if err := deletemetrics.ValidateTerminal(want); err != nil {
		t.Fatalf("shared engine terminal model is invalid: %v", err)
	}
	for _, metric := range append(nodes, fleet[0]) {
		human := formatMetricsMessage(*metric)
		expectedOutcome := deletemetrics.OutcomeRunning
		if metric.Scope == "fleet" {
			expectedOutcome = want.FailurePolicy.Outcome
		}
		for _, expected := range []string{
			"units=requests:logical_api_requests,objects:object_identities,batches:logical_api_requests",
			"outcome_terminology=accepted",
			"failure_budget_outcome=" + expectedOutcome,
		} {
			if !strings.Contains(human, expected) {
				t.Fatalf("headless per-node/aggregate view omitted %q: %s", expected, human)
			}
		}
	}
	if fleet[0].OpsPerSec != 3 || fleet[0].SuccessCount != 2 || fleet[0].FailedCount != 1 {
		t.Fatalf("unchanged TUI generic request view changed units: %+v", fleet[0])
	}
	report := resultsummary.NewRenderer(resultsummary.RenderOptions{}).FullReport(
		&resultsummary.RunSummary{
			RunID: "cross-view",
			Steps: []resultsummary.StepSummary{{
				PhaseLabel: "Delete", Operation: "DELETE", Delete: want,
			}},
		})
	for _, expected := range []string{
		"requests=logical_api_requests, objects=object_identities, batches=logical_api_requests",
		"attempted 3, full success 2, partial 1, failed 0",
		"selected 175, attempted 175, accepted 170, failed 5",
		"completed within failure budget",
	} {
		if !strings.Contains(report, expected) {
			t.Fatalf("stored view omitted shared value %q:\n%s", expected, report)
		}
	}
}

func TestStrictPreValidationAbortFixtureReachesGoHeadlessAndStoredViews(t *testing.T) {
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate strict pre-validation fixture source")
	}
	fixturePath := filepath.Clean(filepath.Join(
		filepath.Dir(sourceFile),
		"../../../engine/core/spt-base/src/test/resources/delete-metrics-v4-strict-pre-abort.json"))
	payload, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read strict pre-validation fixture: %v", err)
	}
	fleet, err := tui.NewSptAPIClient("").ParseFleetJSONMetrics(string(payload))
	if err != nil || len(fleet) != 1 || fleet[0].Delete == nil {
		t.Fatalf("parse strict pre-validation fixture: metrics=%+v err=%v", fleet, err)
	}
	metric := fleet[0]
	if metric.Partial {
		t.Fatalf("valid strict pre-validation abort was marked partial: %+v", metric.Delete)
	}
	if err := deletemetrics.ValidateTerminal(metric.Delete); err != nil {
		t.Fatalf("strict pre-validation abort failed shared validation: %v", err)
	}
	if !metric.Delete.Verification.PreValidationComplete ||
		metric.Delete.Verification.PostVerificationComplete ||
		!metric.Delete.Verification.PostVerificationSkipped ||
		metric.Delete.Phases.PostVerificationSeconds != nil {
		t.Fatalf("strict pre-validation abort phase state = %+v", metric.Delete)
	}
	human := formatMetricsMessage(*metric)
	for _, expected := range []string{
		"pre_validation=true", "pre_validation_complete=true",
		"post_verification=true", "post_verification_complete=false",
		"post_verification_skipped=true", deletemetrics.PostVerificationSkippedNotice,
	} {
		if !strings.Contains(human, expected) {
			t.Fatalf("headless strict-abort view omitted %q: %s", expected, human)
		}
	}
	report := resultsummary.NewRenderer(resultsummary.RenderOptions{}).FullReport(
		&resultsummary.RunSummary{
			RunID: "strict-pre-abort",
			Steps: []resultsummary.StepSummary{{
				PhaseLabel: "Delete", Operation: "DELETE", Delete: metric.Delete,
			}},
		})
	for _, expected := range []string{
		"pre-validation true (complete true)",
		"post-verification true (complete false, skipped true)",
		deletemetrics.PostVerificationSkippedNotice,
	} {
		if !strings.Contains(report, expected) {
			t.Fatalf("stored strict-abort view omitted %q:\n%s", expected, report)
		}
	}
}

func TestHeadlessRunner_MetricsOutputIncludesAvailableTTFB(t *testing.T) {
	mockDocker := &tui.MockDockerManager{}
	tmpDir := t.TempDir()
	traceFile := filepath.Join(tmpDir, "metrics_trace.log")

	runner, err := NewHeadlessRunner(mockDocker, HeadlessOptions{TraceFile: traceFile})
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Close()

	runner.outputMetrics(tui.PerformanceMetric{
		OpsPerSec:       45,
		MeanLatency:     1024,
		MeanTTFB:        512,
		HasTTFB:         true,
		OpType:          "READ",
		SuccessCount:    100,
		ConcurrencyMean: 8.0,
	})

	content, err := os.ReadFile(traceFile)
	if err != nil {
		t.Fatalf("Failed to read trace file: %v", err)
	}
	if !strings.Contains(string(content), "ttfb=512µs") {
		t.Fatalf("headless metrics output should include available TTFB, got %q", string(content))
	}
}

func TestHeadlessRunner_MixedMetricsPerOpOutput(t *testing.T) {
	mockDocker := &tui.MockDockerManager{}

	// Use a trace file so we can inspect what was written
	tmpDir := t.TempDir()
	traceFile := filepath.Join(tmpDir, "metrics_trace.log")

	runner, err := NewHeadlessRunner(mockDocker, HeadlessOptions{TraceFile: traceFile})
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Close()

	// Simulate a mixed workload metrics update with 4 op types
	update := &tui.MultiNodeMetricsUpdate{
		Aggregated: &tui.PerformanceMetric{
			OpsPerSec:       1000,
			MeanLatency:     1500,
			OpType:          "MIXED",
			SuccessCount:    50000,
			FailedCount:     3,
			ConcurrencyMean: 10.0,
		},
		PerOpType: map[string]*tui.PerformanceMetric{
			"READ": {
				OpsPerSec:       450,
				MeanLatency:     900,
				OpType:          "READ",
				SuccessCount:    22000,
				ConcurrencyMean: 4.5,
			},
			"CREATE": {
				OpsPerSec:       300,
				MeanLatency:     2100,
				OpType:          "CREATE",
				SuccessCount:    15000,
				ConcurrencyMean: 3.0,
			},
			"DELETE": {
				OpsPerSec:       150,
				MeanLatency:     1300,
				OpType:          "DELETE",
				SuccessCount:    7500,
				ConcurrencyMean: 1.5,
			},
			"STAT": {
				OpsPerSec:       100,
				MeanLatency:     800,
				OpType:          "STAT",
				SuccessCount:    5500,
				ConcurrencyMean: 1.0,
			},
		},
	}

	// Invoke the metrics callback logic directly
	metric := update.Aggregated
	runner.outputMetrics(*metric)
	for _, opMetric := range update.PerOpType {
		runner.outputMetrics(*opMetric)
	}

	// Read trace file and verify all op types appear
	content, err := os.ReadFile(traceFile)
	if err != nil {
		t.Fatalf("Failed to read trace file: %v", err)
	}
	trace := string(content)

	for _, opType := range []string{"MIXED", "READ", "CREATE", "DELETE", "STAT"} {
		if !strings.Contains(trace, "type="+opType) {
			t.Errorf("trace output missing per-op metric for type=%s", opType)
		}
	}
}

func TestHeadlessRunner_MixedMetricsPerOpJSON(t *testing.T) {
	mockDocker := &tui.MockDockerManager{}

	tmpDir := t.TempDir()
	traceFile := filepath.Join(tmpDir, "json_trace.log")

	runner, err := NewHeadlessRunner(mockDocker, HeadlessOptions{
		TraceFile: traceFile,
		JSONMode:  true,
	})
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Close()

	update := &tui.MultiNodeMetricsUpdate{
		Aggregated: &tui.PerformanceMetric{
			OpsPerSec:       500,
			MeanLatency:     1200,
			OpType:          "MIXED",
			SuccessCount:    25000,
			ConcurrencyMean: 8.0,
		},
		PerOpType: map[string]*tui.PerformanceMetric{
			"READ": {
				OpsPerSec: 300, MeanLatency: 1000, OpType: "READ",
				SuccessCount: 15000, ConcurrencyMean: 5.0,
			},
			"STAT": {
				OpsPerSec: 200, MeanLatency: 800, OpType: "STAT",
				SuccessCount: 10000, ConcurrencyMean: 3.0,
			},
		},
	}

	// Output aggregated + per-op
	runner.outputMetricsJSON(*update.Aggregated)
	for _, opMetric := range update.PerOpType {
		runner.outputMetricsJSON(*opMetric)
	}

	content, err := os.ReadFile(traceFile)
	if err != nil {
		t.Fatalf("Failed to read trace file: %v", err)
	}
	trace := string(content)

	for _, opType := range []string{"MIXED", "READ", "STAT"} {
		needle := `"operation_type":"` + opType + `"`
		if !strings.Contains(trace, needle) {
			t.Errorf("JSON trace output missing operation_type %q", opType)
		}
	}
}

func TestHeadlessRunner_SingleOpNoPerOpLines(t *testing.T) {
	mockDocker := &tui.MockDockerManager{}

	tmpDir := t.TempDir()
	traceFile := filepath.Join(tmpDir, "single_trace.log")

	runner, err := NewHeadlessRunner(mockDocker, HeadlessOptions{TraceFile: traceFile})
	if err != nil {
		t.Fatalf("Failed to create runner: %v", err)
	}
	defer runner.Close()

	// Single-op update: PerOpType has only one entry → no per-op lines emitted
	update := &tui.MultiNodeMetricsUpdate{
		Aggregated: &tui.PerformanceMetric{
			OpsPerSec: 500, MeanLatency: 900, OpType: "READ",
			SuccessCount: 10000, ConcurrencyMean: 5.0,
		},
		PerOpType: map[string]*tui.PerformanceMetric{
			"READ": {
				OpsPerSec: 500, MeanLatency: 900, OpType: "READ",
				SuccessCount: 10000, ConcurrencyMean: 5.0,
			},
		},
	}

	// Replicate the callback logic
	metric := update.Aggregated
	runner.outputMetrics(*metric)
	if len(update.PerOpType) > 1 {
		for _, opMetric := range update.PerOpType {
			runner.outputMetrics(*opMetric)
		}
	}

	content, err := os.ReadFile(traceFile)
	if err != nil {
		t.Fatalf("Failed to read trace file: %v", err)
	}
	trace := string(content)

	// Should have exactly one METRICS line (aggregated only, no per-op duplication)
	count := strings.Count(trace, "[METRICS]")
	if count != 1 {
		t.Errorf("expected exactly 1 METRICS line for single-op workload, got %d", count)
	}
}

func TestMultiHostHeadlessRunnerEmitsAggregateAndPerNodeDeleteMetrics(t *testing.T) {
	tmpDir := t.TempDir()
	traceFile := filepath.Join(tmpDir, "multi-delete-metrics.log")
	runner, err := NewMultiHostHeadlessRunner(
		tui.NewMultiHostOrchestrator(nil, 1),
		HeadlessOptions{TraceFile: traceFile})
	if err != nil {
		t.Fatalf("new multi-host runner: %v", err)
	}
	defer runner.Close()

	deleteMetric := &tui.PerformanceMetric{
		MetricsSchema: 4,
		Scope:         "fleet",
		Role:          "aggregate",
		OpType:        "DELETE",
		NodeID:        "local",
		Delete: &tui.DeleteMetrics{
			Requests: tui.DeleteRequestMetrics{Attempted: 1, FullSuccess: 1},
			Objects:  tui.DeleteObjectMetrics{Selected: 1, Attempted: 1, Accepted: 1},
			Performance: tui.DeletePerformanceApplicability{
				ObjectSize: "not_applicable", DataMoved: "not_applicable",
				Bandwidth: "not_applicable", TTFB: "not_applicable",
			},
		},
	}
	runner.outputMetricsUpdate(&tui.MultiNodeMetricsUpdate{
		Aggregated: deleteMetric,
		PerNode:    map[string]*tui.PerformanceMetric{"entry": deleteMetric},
		PerOpType:  map[string]*tui.PerformanceMetric{"DELETE": deleteMetric},
	})

	content, err := os.ReadFile(traceFile)
	if err != nil {
		t.Fatalf("read trace: %v", err)
	}
	trace := string(content)
	if !strings.Contains(trace, "view=aggregate") ||
		!strings.Contains(trace, "view=node contributor=entry") ||
		!strings.Contains(trace, "object_size=N/A") {
		t.Fatalf("multi-host DELETE views missing: %q", trace)
	}
}

func TestMultiHostHeadlessRunnerJSONLabelsDeleteMetricViews(t *testing.T) {
	traceFile := filepath.Join(t.TempDir(), "multi-delete-metrics.jsonl")
	runner, err := NewMultiHostHeadlessRunner(
		tui.NewMultiHostOrchestrator(nil, 1),
		HeadlessOptions{TraceFile: traceFile, JSONMode: true})
	if err != nil {
		t.Fatalf("new multi-host runner: %v", err)
	}
	defer runner.Close()

	metric := &tui.PerformanceMetric{
		MetricsSchema: 4,
		OpType:        "DELETE",
		Delete:        &tui.DeleteMetrics{},
	}
	runner.outputMetricsUpdate(&tui.MultiNodeMetricsUpdate{
		Aggregated: metric,
		PerNode:    map[string]*tui.PerformanceMetric{"entry": metric},
	})

	content, err := os.ReadFile(traceFile)
	if err != nil {
		t.Fatalf("read trace: %v", err)
	}
	trace := string(content)
	if !strings.Contains(trace, `"view":"aggregate"`) ||
		!strings.Contains(trace, `"view":"node","contributor":"entry"`) {
		t.Fatalf("multi-host JSON DELETE views missing: %q", trace)
	}
}

func TestSchema4DeleteUsesRealTimingDistributionAndRendersEmptyPopulationUnavailable(t *testing.T) {
	metric := tui.PerformanceMetric{
		MetricsSchema: 4,
		OpType:        "DELETE",
		MeanLatency:   999,
		Delete: &tui.DeleteMetrics{
			Timing: tui.DeleteTimingMetrics{
				Latency: &tui.JSONTimingStat{},
			},
		},
	}
	if text := formatMetricsMessage(metric); !strings.Contains(text, "latency=N/A") || strings.Contains(text, "latency=999") {
		t.Fatalf("zero-sample DELETE latency was fabricated: %q", text)
	}
	encoded, err := marshalMetricsJSON(metric, "aggregate", "")
	if err != nil {
		t.Fatal(err)
	}
	var payload struct {
		Data struct {
			LatencyUS *int64 `json:"latency_us"`
		} `json:"data"`
	}
	if err = json.Unmarshal(encoded, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Data.LatencyUS != nil {
		t.Fatalf("zero-sample DELETE JSON latency = %d, want null: %s", *payload.Data.LatencyUS, encoded)
	}

	metric.Delete.Timing.Latency = &tui.JSONTimingStat{Count: 1, MeanUs: 5, P50Us: 7}
	if text := formatMetricsMessage(metric); !strings.Contains(text, "latency=7µs") {
		t.Fatalf("DELETE headline did not use its real timing distribution: %q", text)
	}
	encoded, err = marshalMetricsJSON(metric, "aggregate", "")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(encoded, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Data.LatencyUS == nil || *payload.Data.LatencyUS != 7 {
		t.Fatalf("DELETE JSON latency = %v, want 7: %s", payload.Data.LatencyUS, encoded)
	}
}

func TestHeadlessOptions_AllFields(t *testing.T) {
	// Test that all option fields can be set and retrieved
	options := HeadlessOptions{
		TraceFile:              "/tmp/test.log",
		TraceAppend:            true,
		Verbose:                true,
		JSONMode:               true,
		MetricsOnly:            true,
		DryRun:                 true,
		DelegateNormalShutdown: true,
	}

	// Verify all fields are accessible
	if options.TraceFile != "/tmp/test.log" {
		t.Errorf("TraceFile not set correctly")
	}
	if !options.TraceAppend {
		t.Errorf("TraceAppend not set correctly")
	}
	if !options.Verbose {
		t.Errorf("Verbose not set correctly")
	}
	if !options.JSONMode {
		t.Errorf("JSONMode not set correctly")
	}
	if !options.MetricsOnly {
		t.Errorf("MetricsOnly not set correctly")
	}
	if !options.DryRun {
		t.Errorf("DryRun not set correctly")
	}
	if !options.DelegateNormalShutdown {
		t.Errorf("DelegateNormalShutdown not set correctly")
	}
	// KeepScenario removed from HeadlessOptions - now in ScenarioParams
}

func TestStartHeadlessMode_Integration(t *testing.T) {
	tmpDir := t.TempDir()
	tracePath := filepath.Join(tmpDir, "trace.log")
	scenarioPath := filepath.Join(tmpDir, "test-scenario.js")

	// Create a scenario file
	err := os.WriteFile(scenarioPath, []byte("Load.config({test: true}).run();"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test scenario file: %v", err)
	}

	params := scenario.ScenarioParams{
		WorkloadType: "mock",
		Threads:      4,
		ObjectSize:   "1MB",
		Duration:     "30s",
	}

	options := HeadlessOptions{
		TraceFile:   tracePath,
		TraceAppend: false,
		Verbose:     false,
		DryRun:      true, // Use dry run to avoid actually running Docker
	}

	err = StartHeadlessModeWithParams("test-image", scenarioPath, params, options)
	if err != nil {
		t.Errorf("StartHeadlessModeWithParams failed: %v", err)
	}

	// Verify trace file was created
	if _, err := os.Stat(tracePath); os.IsNotExist(err) {
		t.Error("Trace file was not created")
	}
}

// TestHeadlessHybridIntegration tests the integration of BuildEndpointArgs with headless mode
func TestHeadlessHybridIntegration(t *testing.T) {
	tests := []struct {
		name        string
		params      scenario.ScenarioParams
		expectError bool
	}{
		{
			name: "S3 write with valid endpoint",
			params: scenario.ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://minio:9000",
				AccessKey:    "testkey",
				SecretKey:    "testsecret",
				Bucket:       "testbucket",
				Threads:      4,
				ObjectSize:   "1MB",
				ObjectCount:  100,
			},
			expectError: false,
		},
		{
			name: "HTTPS S3 endpoint",
			params: scenario.ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "https://s3.amazonaws.com",
				AccessKey:    "AKIAEXAMPLE",
				SecretKey:    "secretkey",
				Bucket:       "bucket",
				Threads:      2,
			},
			expectError: false,
		},
		{
			name: "Mock workload (no endpoint args needed)",
			params: scenario.ScenarioParams{
				WorkloadType: "mock",
				Threads:      8,
				ObjectSize:   "512KB",
				Duration:     "30s",
			},
			expectError: false,
		},
		{
			name: "Invalid endpoint URL",
			params: scenario.ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "ftp://invalid-scheme.com",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.params.BaseTimestamp = "20260803.120000.000"
			tmpDir := t.TempDir()
			tracePath := filepath.Join(tmpDir, "trace.log")
			scenarioPath := filepath.Join(tmpDir, "scenario.js")

			// Generate scenario content based on parameters
			scenarioContent, err := scenario.GenerateScenario(tt.params)
			if err != nil {
				t.Errorf("Failed to generate scenario: %v", err)
				return
			}

			// Write scenario to file
			err = os.WriteFile(scenarioPath, []byte(scenarioContent), 0644)
			if err != nil {
				t.Errorf("Failed to write scenario file: %v", err)
				return
			}

			// Create a headless runner with dry run enabled
			options := HeadlessOptions{
				TraceFile:   tracePath,
				TraceAppend: false,
				Verbose:     true, // Enable verbose to see endpoint args in output
				DryRun:      true,
			}

			err = StartHeadlessModeWithParams("test-image", scenarioPath, tt.params, options)

			if tt.expectError {
				if err == nil {
					t.Error("Expected error from headless mode with invalid endpoint, got none")
				}
				return
			}

			if err != nil {
				t.Errorf("Unexpected error from headless mode: %v", err)
				return
			}

			// Verify trace file was created
			if _, err := os.Stat(tracePath); os.IsNotExist(err) {
				t.Error("Trace file was not created")
				return
			}

			// Read trace file to verify endpoint args were processed
			traceContent, err := os.ReadFile(tracePath)
			if err != nil {
				t.Errorf("Failed to read trace file: %v", err)
				return
			}

			traceStr := string(traceContent)

			defaultsContent, err := scenario.GenerateDefaults(tt.params)
			if err != nil {
				t.Fatalf("Failed to generate defaults: %v", err)
			}
			assertDryRunContentIdentity(
				t, traceStr, traceStr, "Generated scenario content", []byte(scenarioContent))
			assertDryRunContentIdentity(
				t, traceStr, traceStr, "Generated defaults configuration", defaultsContent)

			// Verify dry run output
			if !strings.Contains(traceStr, "DRY-RUN") {
				t.Error("Trace should contain dry run indicators")
			}
		})
	}
}

func captureHeadlessStdout(t *testing.T, run func()) string {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	previous := os.Stdout
	os.Stdout = writer
	output := make(chan struct {
		data []byte
		err  error
	}, 1)
	go func() {
		data, readErr := io.ReadAll(reader)
		output <- struct {
			data []byte
			err  error
		}{data: data, err: readErr}
	}()
	run()
	os.Stdout = previous
	if err = writer.Close(); err != nil {
		t.Fatal(err)
	}
	result := <-output
	if err = reader.Close(); err != nil {
		t.Fatal(err)
	}
	if result.err != nil {
		t.Fatal(result.err)
	}
	return string(result.data)
}

func readHeadlessTrace(t *testing.T, path string) string {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(content)
}

func assertDryRunContentHidden(t *testing.T, stdout, trace string, secrets ...string) {
	t.Helper()
	for _, secret := range secrets {
		if strings.Contains(stdout, secret) || strings.Contains(trace, secret) {
			t.Fatalf("dry-run output disclosed sensitive content %q", secret)
		}
	}
}

func assertDryRunContentIdentity(t *testing.T, stdout, trace, label string, content []byte) {
	t.Helper()
	digest := sha256.Sum256(content)
	want := fmt.Sprintf("%s: bytes=%d sha256=%x", label, len(content), digest)
	if !strings.Contains(stdout, want) || !strings.Contains(trace, want) {
		t.Fatalf("dry-run output missing content identity %q", want)
	}
}

// TestHeadlessRunner_ReturnsPromptlyOnEngineCompletion reproduces the bug where RunWithParams
// blocks at <-ctx.Done() indefinitely after the engine container finishes all scenario steps
// and the /status endpoint reports COMPLETED. The runner should detect completion via the
// status callback and return without waiting for the auto-terminate timeout.
//
// Failure mode: runBenchmarkWithParams calls <-ctx.Done() with no other select branch, so it
// can only exit when the auto-terminate context fires (up to 600s for the compaction smoketest).
// Even though monitorStatus detects COMPLETED and returns, it never cancels the context or
// signals the runner — so RunWithParams hangs for the full auto-terminate duration.
func TestHeadlessRunner_ReturnsPromptlyOnEngineCompletion(t *testing.T) {
	// Bind a listener on a random port so we can tell the runner which port to use.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to allocate listener: %v", err)
	}
	port := strconv.Itoa(ln.Addr().(*net.TCPAddr).Port)

	// Fake engine API: ready immediately, starts the run, reports COMPLETED on /status.
	// After the first COMPLETED the engine would normally shut down, so /status keeps
	// returning COMPLETED (as WaitForLinger expects).
	mux := http.NewServeMux()
	mux.HandleFunc("/ready", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ready":true,"status":"ready","scope":"node","role":"entry","node_id":"node-0"}`))
	})
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	})
	mux.HandleFunc("/run", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"runId":"test-run-001"}`))
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"state":"COMPLETED","message":"all steps done"}`))
	})
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, _ *http.Request) {
		// Return 404 — metrics errors are tolerated, this keeps the metrics poller busy
		// without affecting the completion-detection path under test.
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/shutdown", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewUnstartedServer(mux)
	srv.Listener = ln
	srv.Start()
	defer srv.Close()

	mockDocker := tui.NewMockDockerManager()

	// Use a generous auto-terminate (30s) — the test deadline is much tighter (5s).
	// If the bug is present, RunWithParams will block for the full 30s.
	options := HeadlessOptions{
		APIPort:              port,
		AutoTerminateSeconds: 30,
	}
	runner, err := NewHeadlessRunner(mockDocker, options)
	if err != nil {
		t.Fatalf("NewHeadlessRunner: %v", err)
	}
	defer runner.Close()

	params := scenario.ScenarioParams{
		WorkloadType: "mock",
		Threads:      1,
	}

	// Write a minimal scenario file — the content does not matter for this test.
	tmpDir := t.TempDir()
	scenarioPath := filepath.Join(tmpDir, "scenario.js")
	if err := os.WriteFile(scenarioPath, []byte(`Load.config({}).start().await();`), 0600); err != nil {
		t.Fatalf("write scenario: %v", err)
	}

	done := make(chan error, 1)
	go func() {
		ctx := context.Background() // no external timeout; auto-terminate drives it
		done <- runner.RunWithParams(ctx, "test-image", scenarioPath, params)
	}()

	// The engine immediately reports COMPLETED. RunWithParams should detect this via
	// monitorStatus and return well within 10 seconds (2s status poll + 5s WaitForLinger
	// + cleanup overhead). With the bug present it would block for the full 30s
	// auto-terminate timeout.
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("RunWithParams returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("RunWithParams did not return after engine reported COMPLETED " +
			"(blocks at <-ctx.Done() waiting for auto-terminate instead of exiting on completion)")
	}
}

func TestLocalHeadlessSessionDefersCleanupUntilSessionFinalization(t *testing.T) {
	manager := tui.NewMockDockerManager()
	manager.SetContainerID("container-1")
	session := runcontrol.NewSession()
	hooks := tui.NewSessionLaunchHooks(session, nil)
	runner, err := NewHeadlessRunner(manager, HeadlessOptions{LaunchHooks: hooks})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runner.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	err = runner.runBenchmark(ctx, "", func(*tui.TestOrchestrator) error {
		hooks.NotifySubmitted()
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runBenchmark() error = %v, want cancellation", err)
	}
	if got := manager.GetCleanupCallCount(); got != 0 {
		t.Fatalf("presentation cleanup calls = %d, want 0 before evidence/finalization", got)
	}
	if !manager.HasManagedResources() {
		t.Fatal("presentation adapter released resources before session finalization")
	}

	outcome := session.FinalizeResources(context.Background())
	if outcome.Error() != nil || outcome.Resources != runcontrol.ResourceDispositionRemoved {
		t.Fatalf("session finalization = %+v", outcome)
	}
	if got := manager.GetCleanupCallCount(); got != 1 {
		t.Fatalf("session cleanup calls = %d, want exactly 1", got)
	}
}

func TestLocalHeadlessSessionReturnsOnAuthoritativeWorkloadTerminal(t *testing.T) {
	manager := tui.NewMockDockerManager()
	session := runcontrol.NewSession()
	hooks := tui.NewSessionLaunchHooks(session, nil)
	runner, err := NewHeadlessRunner(manager, HeadlessOptions{LaunchHooks: hooks})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = runner.Close() }()

	done := make(chan error, 1)
	go func() {
		done <- runner.runBenchmark(context.Background(), "", func(*tui.TestOrchestrator) error {
			hooks.NotifySubmitted()
			return nil
		})
	}()
	session.MarkWorkloadTerminal()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runBenchmark() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("session-managed headless presentation did not return on authoritative terminal signal")
	}
	if got := manager.GetCleanupCallCount(); got != 0 {
		t.Fatalf("presentation cleanup calls = %d, want session-owned cleanup", got)
	}
}

// Helper function to check if Docker is available
func isDockerAvailable() bool { //nolint:unused
	// Try to create a Docker manager
	dm, err := tui.NewDockerManager()
	if err != nil {
		return false
	}
	defer dm.Close()
	return true
}
