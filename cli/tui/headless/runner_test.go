/*
Copyright © 2025 Dell Technologies
*/

package headless

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

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

func TestHeadlessRunner_TraceFile(t *testing.T) {
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

func TestHeadlessOptions_AllFields(t *testing.T) {
	// Test that all option fields can be set and retrieved
	options := HeadlessOptions{
		TraceFile:   "/tmp/test.log",
		TraceAppend: true,
		Verbose:     true,
		JSONMode:    true,
		MetricsOnly: true,
		DryRun:      true,
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
		name         string
		params       scenario.ScenarioParams
		expectError  bool
		expectNoArgs bool
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
			expectError:  false,
			expectNoArgs: false,
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
			expectError:  false,
			expectNoArgs: false,
		},
		{
			name: "Mock workload (no endpoint args needed)",
			params: scenario.ScenarioParams{
				WorkloadType: "mock",
				Threads:      8,
				ObjectSize:   "512KB",
				Duration:     "30s",
			},
			expectError:  false,
			expectNoArgs: true,
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
			expectError:  true,
			expectNoArgs: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			// In the new API-based flow, configuration is done via defaults.yaml
			// not CLI arguments, so we check for defaults content instead
			if tt.expectNoArgs {
				// For mock workloads, we should see dummy-mock configuration
				if !strings.Contains(traceStr, "dummy-mock") && !strings.Contains(traceStr, "mock") {
					t.Error("Mock workload should have dummy-mock driver in trace or scenario")
				}
			} else {
				// For S3 workloads, we should see S3 configuration in generated content
				// Check that scenario was generated with S3 driver
				if !strings.Contains(traceStr, `"type": "s3"`) && !strings.Contains(traceStr, "s3") {
					t.Error("S3 workload should have S3 driver configuration in generated scenario")
				}
				// The actual endpoint/auth configuration is now in defaults.yaml
				// which is generated but not shown in dry-run output
			}

			// Verify scenario generation worked
			if !strings.Contains(traceStr, "Load") {
				t.Error("Trace should contain generated scenario with 'Load'")
			}

			// Verify dry run output
			if !strings.Contains(traceStr, "DRY-RUN") {
				t.Error("Trace should contain dry run indicators")
			}
		})
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
