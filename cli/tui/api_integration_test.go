/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// TestAPIIntegrationFullFlow tests the complete API integration flow
func TestAPIIntegrationFullFlow(t *testing.T) {
	// Track server state
	var mu sync.Mutex
	apiReady := false
	testRunning := false
	runID := ""
	metricsCount := 0
	metricsBeforeReady := false

	// Create a full mock API server
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
			w.Write([]byte(`{"ready":true,"status":"ready","scope":"node","role":"entry","node_id":"entry-0"}`))

		case "/health":
			if !apiReady {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok","scope":"node","role":"entry","node_id":"entry-0"}`))

		case "/metrics/json":
			if !apiReady {
				metricsBeforeReady = true
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			metricsCount++
			w.WriteHeader(http.StatusOK)
			// Return different JSON metrics over time to simulate progress
			if metricsCount < 3 {
				step := newTestStep()
				step.StepID = "integration_test"
				step.Timestamp = time.Now().UnixMilli()
				step.Operations.SuccessCount = int64(metricsCount * 100)
				step.Operations.SuccessRateLast = float64(metricsCount * 100)
				step.Bandwidth.BytesRateLast = float64(metricsCount * 1_000_000)
				step.Timing.LatencyMeanUs = 2000.0
				step.Concurrency.Current = 4
				step.Concurrency.Mean = 4.0
				_, _ = w.Write([]byte(marshalSteps(t, []JSONMetricsStep{step})))
			} else {
				step := newTestStep()
				step.StepID = "integration_test"
				step.Timestamp = time.Now().UnixMilli()
				step.Operations.SuccessCount = 1000
				step.Operations.SuccessRateLast = 1000
				step.Bandwidth.BytesRateLast = 5_000_000
				step.Timing.LatencyMeanUs = 1800.0
				step.Concurrency.Current = 4
				step.Concurrency.Mean = 4.0
				_, _ = w.Write([]byte(marshalSteps(t, []JSONMetricsStep{step})))
			}

		case "/run":
			switch r.Method { // QF1003: tagged switch for clarity
			case "POST":
				// Parse multipart form
				err := r.ParseMultipartForm(10 << 20)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}

				// Verify scenario file is present
				scenarioFile, _, err := r.FormFile("scenario")
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte("Missing scenario file"))
					return
				}
				defer func() { _ = scenarioFile.Close() }()

				// Read scenario content
				scenarioContent, _ := io.ReadAll(scenarioFile)
				if !strings.Contains(string(scenarioContent), "Load") {
					w.WriteHeader(http.StatusBadRequest)
					_, _ = w.Write([]byte("Invalid scenario content"))
					return
				}

				// Check for optional defaults file
				defaultsFile, _, err := r.FormFile("defaults")
				if err == nil {
					defer func() { _ = defaultsFile.Close() }()
					defaultsContent, _ := io.ReadAll(defaultsFile)
					if len(defaultsContent) == 0 {
						w.WriteHeader(http.StatusBadRequest)
						_, _ = w.Write([]byte("Empty defaults file"))
						return
					}
				}

				// Start test
				testRunning = true
				runID = fmt.Sprintf("run-%d", time.Now().Unix())
				w.Header().Set("ETag", runID)
				w.WriteHeader(http.StatusOK)
				_, _ = fmt.Fprintf(w, `{"runId": "%s"}`, runID)

			case "DELETE":
				if !testRunning {
					w.WriteHeader(http.StatusNotFound)
					return
				}
				testRunning = false
				w.WriteHeader(http.StatusOK)
			case http.MethodHead:
				// Implement readiness semantics for /run HEAD
				// - 200 when a run is active
				// - 204 when idle but servlet is registered
				if testRunning {
					w.WriteHeader(http.StatusOK)
				} else {
					w.WriteHeader(http.StatusNoContent)
				}
			default:
				w.WriteHeader(http.StatusMethodNotAllowed)
			}

		case "/status":
			state := "RUNNING"
			if !testRunning {
				state = "IDLE"
			} else if metricsCount > 5 {
				state = "COMPLETED"
				testRunning = false
			}

			w.WriteHeader(http.StatusOK)
			_, _ = fmt.Fprintf(w, `{"state": "%s", "message": "Test %s", "runId": "%s"}`, state, strings.ToLower(state), runID)

		case "/shutdown":
			// Accept shutdown and flip running state
			testRunning = false
			w.WriteHeader(http.StatusAccepted)

		case "/logs":
			if !testRunning {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("2025-01-24 12:00:00 Starting test...\n"))
			_, _ = w.Write([]byte("2025-01-24 12:00:01 Test running...\n"))
		}
	}))
	defer server.Close()

	// Create mock Docker manager
	mockDM := NewMockDockerManager()

	// Create orchestrator
	orchestrator := NewTestOrchestrator(mockDM, constants.SptAPIPort)

	// Track all interactions
	var statusUpdates []string
	var metricsUpdates []int
	var outputLines []string

	orchestrator.SetCallbacks(
		func(status *TestStatus) {
			statusUpdates = append(statusUpdates, status.State)
		},
		func(update *MultiNodeMetricsUpdate) {
			if update.Aggregated != nil {
				metricsUpdates = append(metricsUpdates, int(update.Aggregated.SuccessCount))
			}
		},
		func(line string) {
			outputLines = append(outputLines, line)
		},
		nil,
	)

	// Test parameters
	params := scenario.ScenarioParams{
		WorkloadType: "write",
		Endpoint:     "http://test:9000",
		AccessKey:    "testkey",
		SecretKey:    "testsecret",
		Bucket:       "testbucket",
		Threads:      4,
		ObjectSize:   "1MB",
		ObjectCount:  1000,
	}

	// Simulate API becoming ready
	go func() {
		time.Sleep(200 * time.Millisecond)
		mu.Lock()
		apiReady = true
		mu.Unlock()
	}()

	// Start the test using the orchestrator components separately
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start container
	containerID, err := mockDM.StartContainerInNodeMode("test-image", constants.SptAPIPort, constants.DefaultNetworkMode)
	if err != nil {
		t.Fatalf("Failed to start container: %v", err)
	}
	orchestrator.containerID = containerID

	// Override API client to use test server
	orchestrator.apiClient = NewSptAPIClient(server.URL)

	// Wait for API ready
	err = orchestrator.apiClient.WaitForAPIReady(3 * time.Second)
	if err != nil {
		t.Fatalf("API failed to become ready: %v", err)
	}

	// Start test via API
	scenarioContent, _ := scenario.GenerateScenario(params)
	defaultsContent, _ := scenario.GenerateDefaults(params)

	testRunID, err := orchestrator.apiClient.StartTest([]byte(scenarioContent), defaultsContent)
	if err != nil {
		t.Fatalf("Failed to start test: %v", err)
	}

	// Start monitoring
	go orchestrator.monitorStatus(ctx)
	go orchestrator.monitorMetrics(ctx)

	// Let the test run briefly - just enough for monitoring to start
	// Metrics poll every 500ms, status every 2s, so 1s is enough for metrics
	time.Sleep(1 * time.Second)

	// Verify the complete flow
	mu.Lock()
	if !apiReady {
		t.Error("API should have become ready")
	}
	if testRunID == "" {
		t.Error("Test should have been started and assigned a run ID")
	}
	if metricsCount == 0 {
		t.Error("Metrics should have been collected")
	}
	if metricsBeforeReady {
		t.Error("metrics were fetched before readiness")
	}
	mu.Unlock()

	// Verify container was started in node mode
	calls := mockDM.GetContainerCalls()
	if len(calls) != 1 {
		t.Errorf("Expected 1 container call, got %d", len(calls))
	}

	// Verify metrics were collected
	if len(metricsUpdates) == 0 {
		t.Error("Expected metrics updates")
	}

	// Stop the test
	err = orchestrator.StopTest()
	if err != nil {
		t.Errorf("Failed to stop test: %v", err)
	}

	// Verify test was stopped
	mu.Lock()
	if testRunning {
		t.Error("Test should have been stopped")
	}
	mu.Unlock()
}

// TestAPIIntegrationWithDefaults tests the defaults.yaml configuration flow
func TestAPIIntegrationWithDefaults(t *testing.T) {
	var receivedDefaults []byte
	var receivedScenario []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/metrics":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("# HELP test\n"))

		case "/run":
			if r.Method == "POST" {
				// Parse and capture the uploaded files
				r.ParseMultipartForm(10 << 20)

				if file, _, err := r.FormFile("scenario"); err == nil {
					receivedScenario, _ = io.ReadAll(file)
					file.Close()
				}

				if file, _, err := r.FormFile("defaults"); err == nil {
					receivedDefaults, _ = io.ReadAll(file)
					file.Close()
				}

				w.Header().Set("ETag", "test-run")
				w.WriteHeader(http.StatusOK)
			}
		}
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)

	// Generate test scenario and defaults
	params := scenario.ScenarioParams{
		WorkloadType: "write",
		Endpoint:     "https://s3.amazonaws.com",
		AccessKey:    "AKIAEXAMPLE",
		SecretKey:    "secretkey",
		Bucket:       "mybucket",
		Threads:      8,
		ObjectSize:   "10MB",
		Duration:     "5m",
	}

	scenarioContent, err := scenario.GenerateScenario(params)
	if err != nil {
		t.Fatalf("Failed to generate scenario: %v", err)
	}

	defaultsContent, err := scenario.GenerateDefaults(params)
	if err != nil {
		t.Fatalf("Failed to generate defaults: %v", err)
	}

	// Start test with scenario and defaults
	_, err = client.StartTest([]byte(scenarioContent), defaultsContent)
	if err != nil {
		t.Fatalf("Failed to start test: %v", err)
	}

	// Verify scenario was sent
	if len(receivedScenario) == 0 {
		t.Error("Scenario file was not received by server")
	}
	if !strings.Contains(string(receivedScenario), "Load") {
		t.Error("Scenario should contain Load configuration")
	}

	// Verify defaults were sent
	if len(receivedDefaults) == 0 {
		t.Error("Defaults file was not received by server")
	}
	if !strings.Contains(string(receivedDefaults), "storage:") {
		t.Error("Defaults should contain storage configuration")
	}
}

// TestAPIIntegrationErrorHandling tests error scenarios in the API flow
func TestAPIIntegrationErrorHandling(t *testing.T) {
	tests := []struct {
		name        string
		serverFunc  func() *httptest.Server
		expectError string
	}{
		{
			name: "API never becomes ready",
			serverFunc: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.WriteHeader(http.StatusServiceUnavailable)
				}))
			},
			expectError: "timeout",
		},
		{
			name: "test start fails",
			serverFunc: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/metrics" {
						w.WriteHeader(http.StatusOK)
						w.Write([]byte("# HELP test\n"))
					} else if r.URL.Path == "/run" {
						w.WriteHeader(http.StatusInternalServerError)
						w.Write([]byte("Internal error"))
					}
				}))
			},
			expectError: "500",
		},
		{
			name: "invalid JSON metrics format",
			serverFunc: func() *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/metrics" {
						w.WriteHeader(http.StatusOK)
						w.Write([]byte("# Basic health OK\n"))
					} else if r.URL.Path == "/metrics/json" {
						w.WriteHeader(http.StatusOK)
						w.Write([]byte("invalid json data"))
					}
				}))
			},
			expectError: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := tt.serverFunc()
			defer server.Close()

			client := NewSptAPIClient(server.URL)

			// Test different error scenarios
			switch tt.name {
			case "API never becomes ready":
				err := client.WaitForAPIReady(500 * time.Millisecond)
				if err == nil {
					t.Error("Expected timeout error")
				}

			case "test start fails":
				_, err := client.StartTest([]byte("test"), nil)
				if err == nil {
					t.Error("Expected error starting test")
				}
				if !strings.Contains(err.Error(), tt.expectError) {
					t.Errorf("Expected error containing '%s', got: %v", tt.expectError, err)
				}

			case "invalid JSON metrics format":
				data, _ := client.GetJSONMetrics()
				metrics, err := client.ParseJSONMetrics(data)
				// JSON parsing should return an error for invalid data
				if err == nil {
					t.Error("JSON parser should return error for invalid data")
				}
				if metrics != nil {
					t.Error("Should return nil metrics when parsing fails")
				}
			}
		})
	}
}

// TestAPIIntegrationMultipartFormCreation tests multipart form creation
func TestAPIIntegrationMultipartFormCreation(t *testing.T) {
	var capturedContentType string
	var capturedBody []byte

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/run" && r.Method == "POST" {
			capturedContentType = r.Header.Get("Content-Type")
			capturedBody, _ = io.ReadAll(r.Body)
			w.Header().Set("ETag", "test-run")
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)

	scenario := []byte(`Load.config({}).run();`)
	defaults := []byte(`storage: {driver: {type: "s3"}}`)

	_, err := client.StartTest(scenario, defaults)
	if err != nil {
		t.Fatalf("Failed to start test: %v", err)
	}

	// Verify multipart form was created correctly
	if !strings.Contains(capturedContentType, "multipart/form-data") {
		t.Errorf("Expected multipart/form-data content type, got: %s", capturedContentType)
	}

	// Verify body contains both files
	bodyStr := string(capturedBody)
	if !strings.Contains(bodyStr, "scenario") {
		t.Error("Request body should contain scenario field")
	}
	if !strings.Contains(bodyStr, "Load.config") {
		t.Error("Request body should contain scenario content")
	}
	if !strings.Contains(bodyStr, "defaults") {
		t.Error("Request body should contain defaults field")
	}
}

// TestAPIIntegrationConcurrentRequests tests handling of concurrent API requests
func TestAPIIntegrationConcurrentRequests(t *testing.T) {
	var mu sync.Mutex
	requestCount := 0

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requestCount++
		mu.Unlock()

		// Simulate some processing time
		time.Sleep(10 * time.Millisecond)

		switch r.URL.Path {
		case "/metrics":
			w.WriteHeader(http.StatusOK)
			fmt.Fprintf(w, `spt_duration_count{status="success"} %d`, requestCount*100)
		case "/status":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"state": "RUNNING"}`))
		}
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)

	// Make concurrent requests
	var wg sync.WaitGroup
	errors := make([]error, 10)

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			if idx%2 == 0 {
				_, errors[idx] = client.GetJSONMetrics()
			} else {
				_, errors[idx] = client.GetStatus()
			}
		}(i)
	}

	wg.Wait()

	// Check for errors
	for i, err := range errors {
		if err != nil {
			t.Errorf("Request %d failed: %v", i, err)
		}
	}

	// Verify all requests were processed
	mu.Lock()
	if requestCount != 10 {
		t.Errorf("Expected 10 requests, got %d", requestCount)
	}
	mu.Unlock()
}

// TestMultipartBoundaryHandling tests edge cases in multipart form handling
func TestMultipartBoundaryHandling(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/run" && r.Method == "POST" {
			// Parse the multipart form
			reader, err := r.MultipartReader()
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}

			parts := make(map[string]bool)
			for {
				part, err := reader.NextPart()
				if err == io.EOF {
					break
				}
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				parts[part.FormName()] = true
				part.Close()
			}

			// Verify expected parts
			if !parts["scenario"] {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("Missing scenario part"))
				return
			}

			w.Header().Set("ETag", "test-run")
			w.WriteHeader(http.StatusOK)
		}
	}))
	defer server.Close()

	// Test with content that might interfere with boundaries
	problematicScenario := []byte(`
--boundary123
Content-Disposition: form-data; name="fake"

This is not a real boundary
--boundary123--
Load.config({}).run();
`)

	client := NewSptAPIClient(server.URL)
	_, err := client.StartTest(problematicScenario, nil)

	if err != nil {
		t.Errorf("Multipart encoding should handle content with boundary-like strings: %v", err)
	}
}

// Helper to create a proper multipart request for testing
// removed unused test helper to satisfy unused linter
func createMultipartRequest(scenario, defaults []byte) (*bytes.Buffer, string, error) { //nolint:unused
	body := &bytes.Buffer{}
	writer := multipart.NewWriter(body)

	// Add scenario file
	part, err := writer.CreateFormFile("scenario", "scenario.js")
	if err != nil {
		return nil, "", err
	}
	if _, err := part.Write(scenario); err != nil {
		return nil, "", err
	}

	// Add defaults file if provided
	if defaults != nil {
		part, err := writer.CreateFormFile("defaults", "defaults.yaml")
		if err != nil {
			return nil, "", err
		}
		if _, err := part.Write(defaults); err != nil {
			return nil, "", err
		}
	}

	err = writer.Close()
	if err != nil {
		return nil, "", err
	}

	return body, writer.FormDataContentType(), nil
}
