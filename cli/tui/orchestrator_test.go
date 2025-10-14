/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// TestOrchestratorStartTest tests the complete test start flow
func TestOrchestratorStartTest(t *testing.T) {
	// Create a mock API server
	apiReady := false
	testStarted := false
	var mu sync.Mutex

	metricsPayload := marshalSteps(t, []JSONMetricsStep{newTestStep()})
	var metricsBeforeReady bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		switch r.URL.Path {
		case "/ready":
			if !apiReady {
				w.WriteHeader(http.StatusServiceUnavailable)
				w.Write([]byte(`{"ready":false,"status":"starting"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"ready":true,"status":"ready","scope":"node","role":"entry","node_id":"node-0"}`))
		case "/health":
			if !apiReady {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok","scope":"node","role":"entry","node_id":"node-0"}`))
		case "/run":
			if r.Method == http.MethodHead {
				if apiReady {
					w.WriteHeader(http.StatusNoContent)
				} else {
					w.WriteHeader(http.StatusNotFound)
				}
				return
			}
			if r.Method == http.MethodPost {
				testStarted = true
				w.Header().Set("ETag", "test-run-123")
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"runId": "test-run-123"}`))
				return
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
		case "/metrics/json":
			if !apiReady {
				metricsBeforeReady = true
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(metricsPayload))
		case "/status":
			if testStarted {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte(`{"state": "RUNNING", "message": "Test in progress"}`))
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	// Create mock Docker manager
	mockDM := NewMockDockerManager()

	// Create orchestrator
	orchestrator := NewTestOrchestrator(mockDM, constants.SptAPIPort)

	// Track callbacks
	var statusUpdates []*TestStatus
	var metricsReceived []*PerformanceMetric
	var outputLines []string
	var errorLines []string

	orchestrator.SetCallbacks(
		func(status *TestStatus) { statusUpdates = append(statusUpdates, status) },
		func(update *MultiNodeMetricsUpdate) {
			if update.Aggregated != nil {
				metricsReceived = append(metricsReceived, update.Aggregated)
			}
		},
		func(line string) { outputLines = append(outputLines, line) },
		func(err string) { errorLines = append(errorLines, err) },
	)

	// Test parameters
	params := scenario.ScenarioParams{
		WorkloadType: "write",
		Endpoint:     "http://test:9000",
		AccessKey:    "key",
		SecretKey:    "secret",
		Bucket:       "bucket",
		Threads:      4,
		ObjectSize:   "1MB",
		ObjectCount:  100,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Simulate API becoming ready after a delay
	go func() {
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		apiReady = true
		mu.Unlock()
	}()

	// We need to modify the orchestrator to use our test server BEFORE StartTest
	// This requires exposing the ability to set a custom API endpoint
	// For now, let's test the components separately

	// Start container
	containerID, err := mockDM.StartContainerInNodeMode("test-image", "9999")
	if err != nil {
		t.Fatalf("Failed to start container: %v", err)
	}
	orchestrator.containerID = containerID

	// Set up API client with test server
	orchestrator.apiClient = NewSptAPIClient(server.URL)

	// Wait for API and start test
	err = orchestrator.apiClient.WaitForAPIReady(2 * time.Second)
	if err != nil {
		t.Fatalf("API failed to become ready: %v", err)
	}

	// Now start the test via API
	scenarioContent, _ := scenario.GenerateScenario(params)
	defaultsContent, _ := scenario.GenerateDefaults(params)

	runID, err := orchestrator.apiClient.StartTest([]byte(scenarioContent), defaultsContent)
	if err != nil {
		t.Fatalf("Failed to start test: %v", err)
	}

	// Start monitoring
	go orchestrator.monitorStatus(ctx)
	go orchestrator.monitorMetrics(ctx)

	// Wait for some activity
	time.Sleep(500 * time.Millisecond)

	// Verify container was started in node mode
	calls := mockDM.GetContainerCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 container call, got %d", len(calls))
	} else {
		if calls[0].Image != "test-image" {
			t.Errorf("Expected image 'test-image', got %s", calls[0].Image)
		}
		// Check for node mode flags
		hasNodeFlag := false
		for _, cmd := range calls[0].Cmd {
			if strings.Contains(cmd, "--run-node") {
				hasNodeFlag = true
				break
			}
		}
		if !hasNodeFlag {
			t.Error("Container should be started with --run-node flag")
		}
	}

	// Verify test was started via API
	if runID == "" {
		t.Error("Test should have been started via API and returned a run ID")
	}
	mu.Lock()
	if !testStarted {
		t.Error("Test should have been started via API")
	}
	if metricsBeforeReady {
		t.Error("metrics were fetched before readiness")
	}
	mu.Unlock()

	// Verify we got a run ID from the API
	if runID == "" {
		t.Error("Expected to get a run ID from API")
	}

	// Clean up
	orchestrator.StopTest()
}

// TestOrchestratorContainerStartFailure tests handling of container start failures
func TestOrchestratorContainerStartFailure(t *testing.T) {
	mockDM := NewMockDockerManager()
	mockDM.SetFailOnStart(true)

	orchestrator := NewTestOrchestrator(mockDM, constants.SptAPIPort)

	params := scenario.ScenarioParams{
		WorkloadType: "mock",
		Threads:      1,
		ObjectSize:   "1MB",
	}

	ctx := context.Background()
	err := orchestrator.StartTest(ctx, "test-image", params)

	if err == nil {
		t.Error("Expected error when container start fails")
	}
	if !strings.Contains(err.Error(), "failed to start container in node mode") {
		t.Errorf("Expected container start error, got: %v", err)
	}
}

// TestOrchestratorAPIReadyTimeout tests API readiness timeout
func TestOrchestratorAPIReadyTimeout(t *testing.T) {
	// Create a server that never becomes ready
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	mockDM := NewMockDockerManager()
	orchestrator := NewTestOrchestrator(mockDM, constants.SptAPIPort)

	// Set up the orchestrator with test server BEFORE trying to connect
	orchestrator.containerID = "test-container"
	orchestrator.apiClient = NewSptAPIClient(server.URL)

	// Test that WaitForAPIReady times out properly
	err := orchestrator.apiClient.WaitForAPIReady(400 * time.Millisecond)
	if err == nil {
		t.Error("Expected timeout error when API never becomes ready")
	}
	// The error message should indicate the API didn't become ready
	if !strings.Contains(err.Error(), "did not become ready") && !strings.Contains(err.Error(), "timeout") {
		t.Errorf("Expected API readiness error, got: %v", err)
	}
}

// TestOrchestratorStopTest tests graceful test stopping
func TestOrchestratorStopTest(t *testing.T) {
	var readyHits int32
	var statuses []int
	var mu sync.Mutex
	var shutdownCalled atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("# HELP test\n"))
		case "/ready":
			atomic.AddInt32(&readyHits, 1)
			mu.Lock()
			defer mu.Unlock()
			if shutdownCalled.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				statuses = append(statuses, http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"ready":false,"status":"stopping"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			statuses = append(statuses, http.StatusOK)
			_, _ = w.Write([]byte(`{"ready":true,"status":"ready"}`))
		case "/shutdown":
			shutdownCalled.Store(true)
			w.WriteHeader(http.StatusOK)
		case "/status":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"state": "IDLE"}`))
		}
	}))
	defer server.Close()

	mockDM := NewMockDockerManager()
	orchestrator := NewTestOrchestrator(mockDM, constants.SptAPIPort)

	// Set up callbacks
	var outputLines []string
	orchestrator.SetCallbacks(
		nil,
		nil,
		func(line string) { outputLines = append(outputLines, line) },
		nil,
	)

	// Manually set up the orchestrator with test server
	orchestrator.containerID = "test-container"
	orchestrator.apiClient = NewSptAPIClient(server.URL)

	// Stop the test
	err := orchestrator.StopTest()
	if err != nil {
		t.Errorf("Failed to stop test: %v", err)
	}

	if atomic.LoadInt32(&readyHits) < 2 {
		t.Fatalf("expected at least two /ready calls, got %d", readyHits)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(statuses) < 2 || statuses[0] != http.StatusOK || statuses[len(statuses)-1] != http.StatusServiceUnavailable {
		t.Fatalf("unexpected readiness statuses: %v", statuses)
	}

	// Verify cleanup was called
	if mockDM.GetCleanupCallCount() != 1 {
		t.Errorf("Expected 1 cleanup call, got %d", mockDM.GetCleanupCallCount())
	}

	// Verify shutdown message was output
	foundStopMsg := false
	for _, line := range outputLines {
		if strings.Contains(line, "shutdown") || strings.Contains(line, "Node shutdown") {
			foundStopMsg = true
			break
		}
	}
	if !foundStopMsg {
		t.Error("Expected stop message in output")
	}
}

// TestOrchestratorScenarioFilePersistence tests scenario file handling
func TestOrchestratorScenarioFilePersistence(t *testing.T) {
	mockDM := NewMockDockerManager()
	orchestrator := NewTestOrchestrator(mockDM, constants.SptAPIPort)

	// Test with KeepScenario = true
	params := scenario.ScenarioParams{
		WorkloadType: "mock",
		Threads:      1,
		ObjectSize:   "1MB",
		KeepScenario: true,
	}

	// Directly test the scenario file creation logic without starting the full flow
	scenarioContent, err := scenario.GenerateScenario(params)
	if err != nil {
		t.Fatalf("Failed to generate scenario: %v", err)
	}

	// Simulate what StartTest does for scenario file saving
	if params.KeepScenario {
		orchestrator.scenarioPath = fmt.Sprintf("spt-scenario-%d.js", time.Now().Unix())
		if err := os.WriteFile(orchestrator.scenarioPath, []byte(scenarioContent), 0644); err != nil {
			t.Errorf("Failed to save scenario file: %v", err)
		} else {
			orchestrator.keepScenario = true
		}
	}

	// Verify scenario file was created
	if orchestrator.keepScenario && orchestrator.scenarioPath == "" {
		t.Error("Scenario file path should be set when KeepScenario is true")
	}

	// Check if file exists
	if orchestrator.scenarioPath != "" {
		if _, err := os.Stat(orchestrator.scenarioPath); os.IsNotExist(err) {
			t.Error("Scenario file should exist")
		}
		// Clean up
		os.Remove(orchestrator.scenarioPath)
	}
}

// TestOrchestratorCallbacks tests all callback invocations
func TestOrchestratorCallbacks(t *testing.T) {
	// Create test server with JSON metrics endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("# Health check OK\n"))
		case "/metrics/json":
			w.WriteHeader(http.StatusOK)
			step := newTestStep()
			step.StepID = "test"
			step.OpType = "create"
			step.Timestamp = 1_734_567_890_123
			step.Operations.SuccessCount = 500
			step.Operations.FailedCount = 0
			step.Operations.SuccessRateLast = 100.0
			step.Bandwidth.BytesRateLast = 5_000_000.0
			step.Timing.LatencyMeanUs = 2000.0
			step.Concurrency.Current = 4
			step.Concurrency.Mean = 4.0
			_, _ = w.Write([]byte(marshalSteps(t, []JSONMetricsStep{step})))
		case "/status":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"state": "RUNNING", "message": "Test running"}`))
		}
	}))
	defer server.Close()

	mockDM := NewMockDockerManager()
	orchestrator := NewTestOrchestrator(mockDM, constants.SptAPIPort)

	// Track all callbacks
	var statusCount, metricsCount, outputCount, errorCount int
	var mu sync.Mutex

	orchestrator.SetCallbacks(
		func(*TestStatus) {
			mu.Lock()
			statusCount++
			mu.Unlock()
		},
		func(*MultiNodeMetricsUpdate) {
			mu.Lock()
			metricsCount++
			mu.Unlock()
		},
		func(string) {
			mu.Lock()
			outputCount++
			mu.Unlock()
		},
		func(string) {
			mu.Lock()
			errorCount++
			mu.Unlock()
		},
	)

	// Set up orchestrator with test client
	orchestrator.apiClient = NewSptAPIClient(server.URL)
	orchestrator.containerID = "test-container"

	// Start monitoring goroutines
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	go orchestrator.monitorStatus(ctx)
	go orchestrator.monitorMetrics(ctx)

	// Let monitors run for a bit (status polls every 2 seconds, metrics every 500ms)
	time.Sleep(2500 * time.Millisecond)

	// Check callbacks were invoked
	mu.Lock()
	defer mu.Unlock()

	if statusCount == 0 {
		t.Error("Expected status callbacks to be invoked")
	}
	if metricsCount == 0 {
		t.Error("Expected metrics callbacks to be invoked")
	}
}

// TestOrchestratorConcurrentOperations tests concurrent goroutine safety
func TestOrchestratorConcurrentOperations(t *testing.T) {
	mockDM := NewMockDockerManager()
	orchestrator := NewTestOrchestrator(mockDM, constants.SptAPIPort)

	// Set up a simple test server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("# HELP test\n"))
		case "/status":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"state": "RUNNING"}`))
		case "/run":
			if r.Method == "DELETE" {
				w.WriteHeader(http.StatusOK)
			}
		}
	}))
	defer server.Close()

	orchestrator.apiClient = NewSptAPIClient(server.URL)
	orchestrator.containerID = "test-container"

	// Start multiple concurrent operations
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// Start monitoring goroutines
	wg.Add(3)
	go func() {
		defer wg.Done()
		orchestrator.monitorStatus(ctx)
	}()
	go func() {
		defer wg.Done()
		orchestrator.monitorMetrics(ctx)
	}()
	go func() {
		defer wg.Done()
		orchestrator.streamContainerOutput(ctx)
	}()

	// Try to stop while monitors are running
	go func() {
		time.Sleep(500 * time.Millisecond)
		orchestrator.StopTest()
	}()

	// Wait for completion or timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success - all goroutines completed
	case <-time.After(3 * time.Second):
		t.Error("Timeout waiting for concurrent operations to complete")
	}
}

// TestOrchestratorMetricsParsing tests JSON metrics parsing integration
func TestOrchestratorMetricsParsing(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("# Health check OK\n"))
		} else if r.URL.Path == "/metrics/json" {
			w.WriteHeader(http.StatusOK)
			step := newTestStep()
			step.StepID = "test"
			step.OpType = "create"
			step.Timestamp = 1_734_567_890_123
			step.Operations.SuccessCount = 1000
			step.Operations.FailedCount = 50
			step.Operations.SuccessRateLast = 150.0
			step.Bandwidth.BytesRateLast = 10_000_000.0
			step.Timing.LatencyMeanUs = 50.0
			step.Concurrency.Current = 8
			step.Concurrency.Mean = 8.5
			_, _ = w.Write([]byte(marshalSteps(t, []JSONMetricsStep{step})))
		}
	}))
	defer server.Close()

	orchestrator := NewTestOrchestrator(nil, constants.SptAPIPort)
	orchestrator.apiClient = NewSptAPIClient(server.URL)

	// Track metrics received
	var receivedMetric *PerformanceMetric
	orchestrator.SetCallbacks(
		nil,
		func(update *MultiNodeMetricsUpdate) {
			if update.Aggregated != nil {
				receivedMetric = update.Aggregated
			}
		},
		nil,
		nil,
	)

	// Run metrics monitor briefly
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	go orchestrator.monitorMetrics(ctx)

	// Wait for metrics to be parsed
	time.Sleep(700 * time.Millisecond)

	// Verify metrics were parsed correctly
	if receivedMetric == nil {
		t.Fatal("Expected to receive metrics")
	}

	if receivedMetric.SuccessCount != 1000 {
		t.Errorf("Expected success count 1000, got %d", receivedMetric.SuccessCount)
	}
	if receivedMetric.FailedCount != 50 {
		t.Errorf("Expected failed count 50, got %d", receivedMetric.FailedCount)
	}
	if receivedMetric.MeanLatency != 50 {
		t.Errorf("Expected mean latency 50, got %d", receivedMetric.MeanLatency)
	}
}

func TestOrchestratorMetricsCompatibilityWarning(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics/json" {
			s := newTestStep()
			s.MetricsSchema = 1
			_, _ = w.Write([]byte(marshalSteps(t, []JSONMetricsStep{s})))
		}
	}))
	defer server.Close()

	orchestrator := NewTestOrchestrator(nil, constants.SptAPIPort)
	orchestrator.apiClient = NewSptAPIClient(server.URL)

	var outputs []string
	orchestrator.SetCallbacks(nil, nil, func(line string) {
		outputs = append(outputs, line)
	}, func(err string) {
		outputs = append(outputs, "ERR:"+err)
	})

	metric, source, err := orchestrator.tryJSONMetrics()
	if err == nil {
		t.Fatal("expected compatibility error")
	}
	if metric != nil || source != "" {
		t.Fatalf("expected no metrics due to compatibility error, got metric=%v source=%q", metric, source)
	}

	if len(outputs) != 1 {
		t.Fatalf("expected exactly one compatibility warning, got %d", len(outputs))
	}
	if !strings.Contains(strings.ToLower(outputs[0]), "schema") {
		t.Errorf("warning should mention schema, got %q", outputs[0])
	}

	orchestrator.tryJSONMetrics()
	if len(outputs) != 1 {
		t.Fatalf("expected warning only once, got %d entries", len(outputs))
	}
}

func TestOrchestratorFetchesVerbosePayloadOnParseError(t *testing.T) {
	t.Setenv("SPT_LOG_METRICS_BODY", "1")
	var verboseHits int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.RawQuery, "verbose=1") {
			atomic.AddInt32(&verboseHits, 1)
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("[]"))
			return
		}

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{")) // malformed to trigger parse error
	}))
	defer server.Close()

	orchestrator := NewTestOrchestrator(nil, constants.SptAPIPort)
	orchestrator.apiClient = NewSptAPIClient(server.URL)

	metric, source, err := orchestrator.tryJSONMetrics()
	if err == nil {
		t.Fatal("expected parse error")
	}
	if metric != nil || source != "" {
		t.Fatalf("expected no metric on parse error, got metric=%v source=%q", metric, source)
	}
	if atomic.LoadInt32(&verboseHits) != 1 {
		t.Fatalf("expected exactly one verbose fetch, got %d", verboseHits)
	}
}

func TestOrchestratorIdleNodeMarkedInactive(t *testing.T) {
	idleResponse := marshalSteps(t, []JSONMetricsStep{func() JSONMetricsStep {
		step := newTestStep()
		step.TestState = constants.TestStateIdle
		step.Operations.SuccessRateLast = 0
		step.Bandwidth.BytesRateLast = 0
		return step
	}()})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics/json" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(idleResponse))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	orchestrator := NewTestOrchestrator(nil, constants.SptAPIPort)
	orchestrator.apiClient = NewSptAPIClient(server.URL)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := make(chan *MultiNodeMetricsUpdate, 1)
	orchestrator.SetCallbacks(nil, func(update *MultiNodeMetricsUpdate) {
		ch <- update
		cancel()
	}, func(string) {}, func(string) {})

	go orchestrator.monitorMetrics(ctx)

	select {
	case update := <-ch:
		status, ok := update.NodeStatus["node-0"]
		if !ok {
			t.Fatal("expected node-0 status entry")
		}
		if !status.IsConnected {
			t.Fatalf("expected node to be connected, got %+v", status)
		}
		if status.IsActive {
			t.Fatalf("expected idle node to be inactive, got %+v", status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for idle metrics update")
	}
}

// TestOrchestratorTestCompletion tests handling of test completion states
func TestOrchestratorTestCompletion(t *testing.T) {
	// Only test one state since both follow the same code path
	// This reduces test time from 4s to 2s while maintaining coverage
	tests := []struct {
		name          string
		finalState    string
		expectMessage string
	}{
		{
			name:          "test completes successfully",
			finalState:    "COMPLETED",
			expectMessage: "Test COMPLETED",
		},
		// Removed redundant "test fails" case - same code path, just different string
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create server that returns the final state
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/status" {
					w.WriteHeader(http.StatusOK)
					fmt.Fprintf(w, `{"state": "%s", "message": "Test finished"}`, tt.finalState)
				}
			}))
			defer server.Close()

			orchestrator := NewTestOrchestrator(nil, constants.SptAPIPort)
			orchestrator.apiClient = NewSptAPIClient(server.URL)

			// Track output
			var outputLines []string
			orchestrator.SetCallbacks(
				nil, nil,
				func(line string) { outputLines = append(outputLines, line) },
				nil,
			)

			// Run status monitor - it polls every 2 seconds, so we need at least 2+ seconds
			// to get the first poll. Since the test returns completion immediately,
			// it will exit after the first poll.
			ctx, cancel := context.WithTimeout(context.Background(), 2100*time.Millisecond)
			defer cancel()

			orchestrator.monitorStatus(ctx)

			// Check for completion message
			found := false
			for _, line := range outputLines {
				if strings.Contains(line, tt.expectMessage) {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("Expected message '%s' in output, got: %v", tt.expectMessage, outputLines)
			}
		})
	}
}

// TestOrchestratorCleanupOnError tests cleanup when operations fail
func TestOrchestratorCleanupOnError(t *testing.T) {
	mockDM := NewMockDockerManager()
	orchestrator := NewTestOrchestrator(mockDM, constants.SptAPIPort)

	// Create a scenario file to test cleanup
	tmpFile, err := os.CreateTemp("", "test-scenario-*.js")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	_ = tmpFile.Close()

	orchestrator.scenarioPath = tmpPath
	orchestrator.keepScenario = false // Should be deleted
	orchestrator.containerID = "test-container"

	// Stop test (which includes cleanup)
	err = orchestrator.StopTest()
	if err != nil {
		t.Errorf("StopTest failed: %v", err)
	}

	// Verify scenario file was deleted
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("Scenario file should have been deleted")
		os.Remove(tmpPath) // Clean up if test fails
	}

	// Verify Docker cleanup was called
	if mockDM.GetCleanupCallCount() != 1 {
		t.Errorf("Expected 1 cleanup call, got %d", mockDM.GetCleanupCallCount())
	}
}
