/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// TestSptAPIClient_GetJSONMetrics tests the JSON metrics endpoint retrieval
func TestSptAPIClient_GetJSONMetrics(t *testing.T) {
	tests := []struct {
		name           string
		serverFunc     func(t *testing.T) *httptest.Server
		expectError    bool
		expectedStatus int
		expectedSubstr string
	}{
		{
			name: "successful JSON metrics retrieval",
			serverFunc: func(t *testing.T) *httptest.Server {
				payload := marshalSteps(t, []JSONMetricsStep{newTestStep()})
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/metrics/json" {
						w.WriteHeader(http.StatusOK)
						w.Write([]byte(payload))
					}
				}))
			},
			expectError:    false,
			expectedSubstr: "step_id",
		},
		{
			name: "JSON endpoint returns 404",
			serverFunc: func(t *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/metrics/json" {
						w.WriteHeader(http.StatusNotFound)
						w.Write([]byte("Not found"))
					}
				}))
			},
			expectError:    true,
			expectedStatus: http.StatusNotFound,
			expectedSubstr: "",
		},
		{
			name: "JSON endpoint returns 500",
			serverFunc: func(t *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/metrics/json" {
						w.WriteHeader(http.StatusInternalServerError)
						w.Write([]byte("Internal server error"))
					}
				}))
			},
			expectError:    true,
			expectedStatus: http.StatusInternalServerError,
			expectedSubstr: "",
		},
		{
			name: "empty JSON response",
			serverFunc: func(t *testing.T) *httptest.Server {
				return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.URL.Path == "/metrics/json" {
						w.WriteHeader(http.StatusOK)
						w.Write([]byte("[]"))
					}
				}))
			},
			expectError:    false,
			expectedSubstr: "[]",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := tt.serverFunc(t)
			defer server.Close()

			client := NewSptAPIClient(server.URL)
			jsonData, err := client.GetJSONMetrics()

			if tt.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if tt.expectError && tt.expectedStatus != 0 {
				var statusErr *HTTPStatusError
				if !errors.As(err, &statusErr) {
					t.Fatalf("expected HTTPStatusError, got %T", err)
				}
				if statusErr.StatusCode != tt.expectedStatus {
					t.Errorf("expected status %d, got %d", tt.expectedStatus, statusErr.StatusCode)
				}
			}
			if !tt.expectError && tt.expectedSubstr != "" && !strings.Contains(jsonData, tt.expectedSubstr) {
				t.Errorf("Expected JSON to contain %q, got: %s", tt.expectedSubstr, jsonData)
			}
		})
	}
}

func TestSptAPIClient_GetJSONMetricsVerbose(t *testing.T) {
	var sawVerbose bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics/json" {
			if r.URL.Query().Get("verbose") == "1" {
				sawVerbose = true
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("[]"))
		}
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)
	jsonData, err := client.GetJSONMetricsVerbose()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if jsonData != "[]" {
		t.Fatalf("expected [] payload, got %q", jsonData)
	}
	if !sawVerbose {
		t.Fatal("expected verbose query parameter to be set")
	}
}

func TestHTTPStatusErrorBodyPreviewTruncated(t *testing.T) {
	const bodyLen = 512
	var saw int
	payload := strings.Repeat("x", bodyLen)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/metrics/json" {
			saw++
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(payload))
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)
	_, err := client.GetJSONMetrics()
	if err == nil {
		t.Fatal("expected HTTPStatusError")
	}
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf("expected HTTPStatusError, got %T", err)
	}
	if statusErr.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %d", statusErr.StatusCode)
	}
	if len(statusErr.BodyPreview) != apiLogPreviewLen {
		t.Fatalf("expected preview length %d, got %d", apiLogPreviewLen, len(statusErr.BodyPreview))
	}
	expected := payload[:apiLogPreviewLen-3] + "..."
	if statusErr.BodyPreview != expected {
		t.Fatalf("unexpected preview content")
	}
	if saw != 1 {
		t.Fatalf("expected one metrics call, saw %d", saw)
	}
}

// TestSptAPIClient_ParseJSONMetrics tests JSON metrics parsing with various data formats
func TestSptAPIClient_ParseJSONMetrics(t *testing.T) {
	baseStep := newTestStep()
	validJSON := marshalSteps(t, []JSONMetricsStep{baseStep})
	oldSchemaStep := newTestStep()
	oldSchemaStep.MetricsSchema = 1
	oldSchemaJSON := marshalSteps(t, []JSONMetricsStep{oldSchemaStep})
	badSampleStep := newTestStep()
	badSampleStep.SampleTimestampRaw = "not-a-timestamp"
	badSampleJSON := marshalSteps(t, []JSONMetricsStep{badSampleStep})
	secondStep := newTestStep()
	secondStep.StepID = "step2"
	secondStep.Operations.SuccessCount = 42
	secondStep.Timestamp = baseStep.Timestamp + 1000
	secondStep.SampleTimestampRaw = time.UnixMilli(secondStep.Timestamp).Format(time.RFC3339Nano)
	multipleStepsJSON := marshalSteps(t, []JSONMetricsStep{baseStep, secondStep})
	missingSchemaJSON := fmt.Sprintf(`[{"scope":"node","role":"entry","node_id":"node-1","run_id":"run-1","sample_ts":"%s","step_id":"legacy","op_type":"create"}]`, testSampleTimestamp)
	wrappedJSON := marshalWrappedSteps(t, []JSONMetricsStep{baseStep})
	scopeMismatch := newTestStep()
	scopeMismatch.Scope = "fleet"
	scopeMismatchJSON := marshalSteps(t, []JSONMetricsStep{scopeMismatch})
	missingRole := newTestStep()
	missingRole.Role = ""
	missingRoleJSON := marshalSteps(t, []JSONMetricsStep{missingRole})
	idleStep := newTestStep()
	idleStep.RunID = ""
	idleStep.StepID = ""
	idleStep.OpType = "none"
	idleStep.TestState = 0
	idleStep.CompletionPercent = 0
	idleStep.OverallCompletion = 0
	idleStep.Operations = JSONMetricsOperations{}
	idleStep.Bandwidth = JSONMetricsBandwidth{}
	idleStep.Timing = JSONMetricsTiming{}
	idleStep.Concurrency = JSONMetricsConcurrency{}
	idleStepJSON := marshalSteps(t, []JSONMetricsStep{idleStep})
	rfc3339Step := newTestStep()
	rfc3339Step.SampleTimestampRaw = "2025-09-23T16:18:56Z"
	rfc3339JSON := marshalSteps(t, []JSONMetricsStep{rfc3339Step})

	baseSampleTime, err := time.Parse(time.RFC3339Nano, testSampleTimestamp)
	if err != nil {
		t.Fatalf("failed to parse test sample timestamp: %v", err)
	}
	rfc3339SampleTime, err := time.Parse(time.RFC3339, rfc3339Step.SampleTimestampRaw)
	if err != nil {
		t.Fatalf("failed to parse RFC3339 sample timestamp: %v", err)
	}

	tests := []struct {
		name        string
		jsonData    string
		expectError bool
		assert      func(t *testing.T, metrics []*PerformanceMetric)
	}{
		{
			name:     "valid node metrics",
			jsonData: validJSON,
			assert: func(t *testing.T, metrics []*PerformanceMetric) {
				if len(metrics) == 0 {
					t.Fatal("expected at least one metric")
				}
				metric := metrics[0]
				if metric.MetricsSchema != 2 {
					t.Errorf("expected metrics schema 2, got %d", metric.MetricsSchema)
				}
				if metric.Scope != "node" {
					t.Errorf("expected scope 'node', got %q", metric.Scope)
				}
				if metric.Role != "entry" {
					t.Errorf("expected role 'entry', got %q", metric.Role)
				}
				if metric.StepID != baseStep.StepID {
					t.Errorf("expected step id %q, got %q", baseStep.StepID, metric.StepID)
				}
				if metric.SampleTimestamp != baseSampleTime {
					t.Errorf("unexpected sample timestamp: want %v, got %v", baseSampleTime, metric.SampleTimestamp)
				}
				if !metric.Timestamp.Equal(time.UnixMilli(baseStep.Timestamp)) {
					t.Errorf("unexpected timestamp: want %v, got %v", time.UnixMilli(baseStep.Timestamp), metric.Timestamp)
				}
				if metric.LimitType != "time" || metric.LimitTimeSec != 300 || !metric.HasLimit {
					t.Errorf("expected limit time=300s, got type=%q time=%d present=%v", metric.LimitType, metric.LimitTimeSec, metric.HasLimit)
				}
				if metric.CompletionPercent != float64(baseStep.CompletionPercent) {
					t.Errorf("expected completion %% %.1f, got %.1f", float64(baseStep.CompletionPercent), metric.CompletionPercent)
				}
				if metric.OverallCompletionPercent != float64(baseStep.OverallCompletion) {
					t.Errorf("expected overall completion %% %.1f, got %.1f", float64(baseStep.OverallCompletion), metric.OverallCompletionPercent)
				}
			},
		},
		{
			name:        "wrapped metrics array incompatible",
			jsonData:    wrappedJSON,
			expectError: true,
		},
		{
			name:     "multiple steps uses latest sample",
			jsonData: multipleStepsJSON,
			assert: func(t *testing.T, metrics []*PerformanceMetric) {
				if len(metrics) == 0 || metrics[0].StepID != secondStep.StepID {
					t.Fatalf("expected latest step to be first")
				}
				if len(metrics) != 2 {
					t.Fatalf("expected 2 metrics, got %d", len(metrics))
				}
			},
		},
		{
			name:     "idle sample accepted",
			jsonData: idleStepJSON,
			assert: func(t *testing.T, metrics []*PerformanceMetric) {
				if len(metrics) == 0 {
					t.Fatal("expected at least one metric for idle sample")
				}
				metric := metrics[0]
				if metric.RunID != "" {
					t.Errorf("expected empty run id, got %q", metric.RunID)
				}
				if metric.StepID != "" {
					t.Errorf("expected empty step id, got %q", metric.StepID)
				}
				if metric.OpType != "none" {
					t.Errorf("expected op_type 'none', got %q", metric.OpType)
				}
				if metric.TestState != 0 {
					t.Errorf("expected test_state 0 for idle sample, got %d", metric.TestState)
				}
				if metric.OpsPerSec != 0 || metric.MiBPerSec != 0 {
					t.Errorf("expected zero rates for idle sample, got ops=%d mb=%d", metric.OpsPerSec, metric.MiBPerSec)
				}
			},
		},
		{
			name:     "RFC3339 sample timestamp without nanos",
			jsonData: rfc3339JSON,
			assert: func(t *testing.T, metrics []*PerformanceMetric) {
				if len(metrics) == 0 {
					t.Fatal("expected at least one metric for RFC3339 sample")
				}
				metric := metrics[0]
				if !metric.SampleTimestamp.Equal(rfc3339SampleTime) {
					t.Errorf("expected sample timestamp %v, got %v", rfc3339SampleTime, metric.SampleTimestamp)
				}
			},
		},
		{
			name:        "scope mismatch",
			jsonData:    scopeMismatchJSON,
			expectError: true,
		},
		{
			name:        "missing role",
			jsonData:    missingRoleJSON,
			expectError: true,
		},
		{
			name:        "schema too low",
			jsonData:    oldSchemaJSON,
			expectError: true,
		},
		{
			name:        "missing schema field",
			jsonData:    missingSchemaJSON,
			expectError: true,
		},
		{
			name:        "invalid sample timestamp",
			jsonData:    badSampleJSON,
			expectError: true,
		},
		{
			name:        "empty array",
			jsonData:    "[]",
			expectError: true,
		},
		{
			name:        "invalid json",
			jsonData:    "{invalid json}",
			expectError: true,
		},
		{
			name:        "null json",
			jsonData:    "null",
			expectError: true,
		},
		{
			name:        "non-array json",
			jsonData:    `{"step_id": "wrong_format"}`,
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := NewSptAPIClient("http://test")
			metrics, err := client.ParseJSONMetrics(tt.jsonData)

			if tt.expectError {
				if err == nil {
					t.Fatalf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.assert != nil {
				tt.assert(t, metrics)
			}
		})
	}
}

// TestOrchestratorJSONMetricsRetrieval tests JSON-only metrics retrieval behavior
func TestOrchestratorJSONMetricsRetrieval(t *testing.T) {
	tests := []struct {
		name              string
		jsonEndpointWorks bool
		expectedSource    string
		expectNilMetric   bool
	}{
		{
			name:              "JSON works - should return JSON metrics",
			jsonEndpointWorks: true,
			expectedSource:    "JSON",
			expectNilMetric:   false,
		},
		{
			name:              "JSON fails - should return nil",
			jsonEndpointWorks: false,
			expectedSource:    "",
			expectNilMetric:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test server that responds based on test configuration
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/metrics/json" {
					if tt.jsonEndpointWorks {
						w.WriteHeader(http.StatusOK)
						step := newTestStep()
						step.StepID = "json_test"
						step.Operations.SuccessCount = 500
						step.Operations.FailedCount = 2
						step.Operations.SuccessRateLast = 75.0
						step.Operations.FailedRateLast = 0.1
						step.Bandwidth.BytesTotal = 26_214_400
						step.Bandwidth.BytesRateLast = 13_107_200.0
						step.Timing.LatencyMeanUs = 1800.0
						step.Timing.DurationMeanUs = 2200.0
						step.Concurrency.Current = 6
						step.Concurrency.Mean = 5.8
						w.Write([]byte(marshalSteps(t, []JSONMetricsStep{step})))
					} else {
						w.WriteHeader(http.StatusInternalServerError)
						w.Write([]byte("JSON endpoint failed"))
					}
				}
			}))
			defer server.Close()

			// Create orchestrator with test API client
			orchestrator := NewTestOrchestrator(nil, constants.SptAPIPort, "")
			orchestrator.apiClient = NewSptAPIClient(server.URL)

			// Call the optimized metrics method
			metrics, source, err := orchestrator.getMetricsWithOptimizedFallback()

			// Validate results
			if tt.expectNilMetric {
				if err == nil {
					t.Error("Expected error when JSON endpoint fails")
				}
				if metrics != nil {
					t.Error("Expected nil metric when JSON endpoint fails")
				}
				if source != "" {
					t.Errorf("Expected empty source when JSON fails, got %q", source)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(metrics) == 0 {
				t.Error("Expected non-empty metrics when JSON endpoint works")
			}
			if source != tt.expectedSource {
				t.Errorf("Expected source %q, got %q", tt.expectedSource, source)
			}

			// Verify metric contains expected JSON data
			if source == "JSON" {
				metric := metrics[0]
				if metric.StepID != "json_test" {
					t.Errorf("Expected JSON StepID 'json_test', got %q", metric.StepID)
				}
				if metric.SuccessCount != 500 {
					t.Errorf("Expected JSON SuccessCount 500, got %d", metric.SuccessCount)
				}
				if metric.OpsPerSec != 75 {
					t.Errorf("Expected JSON OpsPerSec 75, got %d", metric.OpsPerSec)
				}
			}
		})
	}
}

// TestJSONMetricsTimestampConversion tests timestamp handling in JSON parsing
func TestJSONMetricsTimestampConversion(t *testing.T) {
	tests := []struct {
		name         string
		timestampMs  int64
		expectedTime time.Time
	}{
		{
			name:         "valid timestamp",
			timestampMs:  1734567890123,
			expectedTime: time.UnixMilli(1734567890123),
		},
		{
			name:         "zero timestamp",
			timestampMs:  0,
			expectedTime: time.UnixMilli(0),
		},
		{
			name:         "future timestamp",
			timestampMs:  2000000000000, // Year 2033
			expectedTime: time.UnixMilli(2000000000000),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := newTestStep()
			step.StepID = "timestamp_test"
			step.Timestamp = tt.timestampMs
			step.Operations.SuccessCount = 1
			step.Operations.SuccessRateLast = 1.0
			step.Bandwidth.BytesTotal = 0
			step.Bandwidth.BytesRateLast = 0
			step.Timing.LatencyMeanUs = 100.0
			step.Timing.DurationMeanUs = 100.0
			step.Concurrency.Current = 1
			step.Concurrency.Mean = 1.0
			jsonData := marshalSteps(t, []JSONMetricsStep{step})

			client := NewSptAPIClient("http://test")
			metrics, err := client.ParseJSONMetrics(jsonData)

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if len(metrics) == 0 {
				t.Fatal("Expected at least one metric")
			}
			metric := metrics[0]
			if !metric.Timestamp.Equal(tt.expectedTime) {
				t.Errorf("Expected timestamp %v, got %v", tt.expectedTime, metric.Timestamp)
			}
		})
	}
}

// TestJSONMetricsUnitConversions tests the various unit conversions in JSON parsing
func TestJSONMetricsUnitConversions(t *testing.T) {
	tests := []struct {
		name              string
		bytesRateLast     float64
		expectedMiBPerSec int64
		opsRateLast       float64
		expectedOpsPerSec int64
		latencyMeanUs     float64
		expectedLatencyUs int64
	}{
		{
			name:              "bytes to MiB conversion",
			bytesRateLast:     52428800.0, // 50 MiB/s in bytes
			expectedMiBPerSec: 50,
			opsRateLast:       150.5,
			expectedOpsPerSec: 151, // math.Round(150.5) = 151
			latencyMeanUs:     2500.0,
			expectedLatencyUs: 2500, // Already in microseconds
		},
		{
			name:              "zero values",
			bytesRateLast:     0.0,
			expectedMiBPerSec: 0,
			opsRateLast:       0.0,
			expectedOpsPerSec: 0,
			latencyMeanUs:     0.0,
			expectedLatencyUs: 0,
		},
		{
			name:              "fractional values rounded",
			bytesRateLast:     1500000.9,
			expectedMiBPerSec: 1,
			opsRateLast:       99.9,
			expectedOpsPerSec: 100, // math.Round(99.9) = 100
			latencyMeanUs:     1234.7,
			expectedLatencyUs: 1235, // math.Round(1234.7) = 1235
		},
		{
			name:              "large values",
			bytesRateLast:     1073741824.0, // 1 GiB/s in bytes
			expectedMiBPerSec: 1024,
			opsRateLast:       1000000.0,
			expectedOpsPerSec: 1000000,
			latencyMeanUs:     999999.0,
			expectedLatencyUs: 999999,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			step := newTestStep()
			step.StepID = "conversion_test"
			step.Timestamp = 1734567890123
			step.Operations.SuccessCount = 100
			step.Operations.SuccessRateLast = tt.opsRateLast
			step.Bandwidth.BytesTotal = 1_000_000
			step.Bandwidth.BytesRateLast = tt.bytesRateLast
			step.Timing.LatencyMeanUs = tt.latencyMeanUs
			step.Timing.DurationMeanUs = 2000.0
			step.Concurrency.Current = 1
			step.Concurrency.Mean = 1.0
			jsonData := marshalSteps(t, []JSONMetricsStep{step})

			client := NewSptAPIClient("http://test")
			metrics, err := client.ParseJSONMetrics(jsonData)

			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if len(metrics) == 0 {
				t.Fatal("Expected at least one metric")
			}
			metric := metrics[0]

			if metric.MiBPerSec != tt.expectedMiBPerSec {
				t.Errorf("Expected MiBPerSec %d, got %d", tt.expectedMiBPerSec, metric.MiBPerSec)
			}
			if metric.OpsPerSec != tt.expectedOpsPerSec {
				t.Errorf("Expected OpsPerSec %d, got %d", tt.expectedOpsPerSec, metric.OpsPerSec)
			}
			if metric.MeanLatency != tt.expectedLatencyUs {
				t.Errorf("Expected MeanLatency %d, got %d", tt.expectedLatencyUs, metric.MeanLatency)
			}
		})
	}
}

// TestJSONMetricsPerformanceMetricStructure tests that JSON parsing produces
// PerformanceMetric structs compatible with the existing metrics pipeline
func TestJSONMetricsPerformanceMetricStructure(t *testing.T) {
	step := newTestStep()
	step.StepID = "compatibility_test"
	step.OpType = "write"
	step.Timestamp = 1734567890123
	step.ElapsedTimeSeconds = 45.2
	step.TestState = 1
	step.Operations.SuccessCount = 2000
	step.Operations.FailedCount = 10
	step.Operations.SuccessRateLast = 250.7
	step.Operations.FailedRateLast = 0.3
	step.Bandwidth.BytesTotal = 2_097_152_000
	step.Bandwidth.BytesRateLast = 104_857_600.0
	step.Timing.LatencyMeanUs = 1850.0
	step.Timing.DurationMeanUs = 2250.0
	step.Timing.TTFB = &JSONTimingStat{Count: 10, MeanUs: 950.0}
	step.Concurrency.Current = 12
	step.Concurrency.Mean = 11.8
	jsonData := marshalSteps(t, []JSONMetricsStep{step})

	client := NewSptAPIClient("http://test")
	metrics, err := client.ParseJSONMetrics(jsonData)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if len(metrics) == 0 {
		t.Fatal("Expected at least one metric")
	}
	metric := metrics[0]

	// Verify all expected fields are populated and have correct types
	expectedFields := map[string]interface{}{
		"MetricsSchema":            2,
		"Scope":                    "node",
		"Role":                     "entry",
		"RunID":                    step.RunID,
		"NodeID":                   step.NodeID,
		"Timestamp":                time.UnixMilli(1734567890123),
		"SampleTimestamp":          step.SampleTimestampRaw,
		"StepID":                   "compatibility_test",
		"OpType":                   "write",
		"ConcurrencyCurrent":       int64(12),
		"ConcurrencyMean":          11.8,
		"SuccessCount":             int64(2000),
		"FailedCount":              int64(10),
		"StepTime":                 45.2,
		"OpsPerSec":                int64(251), // math.Round(250.7) = 251
		"MiBPerSec":                int64(100),
		"MeanLatency":              int64(1850),
		"MeanDuration":             int64(2250),
		"MeanTTFB":                 int64(950),
		"HasTTFB":                  true,
		"CompletionPercent":        float64(step.CompletionPercent),
		"OverallCompletionPercent": float64(step.OverallCompletion),
		"Unbounded":                false,
		"OverallUnbounded":         false,
		"LimitType":                step.Limit.Type,
		"LimitTimeSec":             int64(step.Limit.TimeSec),
		"HasLimit":                 true,
	}

	// Check each field using reflection to ensure type compatibility
	metricValue := *metric
	for fieldName, expectedValue := range expectedFields {
		switch fieldName {
		case "Timestamp":
			if !metricValue.Timestamp.Equal(expectedValue.(time.Time)) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.Timestamp)
			}
		case "SampleTimestamp":
			if metricValue.SampleTimestampRaw != expectedValue.(string) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.SampleTimestampRaw)
			}
		case "StepID":
			if metricValue.StepID != expectedValue.(string) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.StepID)
			}
		case "MetricsSchema":
			if metricValue.MetricsSchema != expectedValue.(int) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.MetricsSchema)
			}
		case "Scope":
			if metricValue.Scope != expectedValue.(string) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.Scope)
			}
		case "Role":
			if metricValue.Role != expectedValue.(string) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.Role)
			}
		case "RunID":
			if metricValue.RunID != expectedValue.(string) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.RunID)
			}
		case "NodeID":
			if metricValue.NodeID != expectedValue.(string) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.NodeID)
			}
		case "OpType":
			if metricValue.OpType != expectedValue.(string) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.OpType)
			}
		case "ConcurrencyCurrent":
			if metricValue.ConcurrencyCurrent != expectedValue.(int64) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.ConcurrencyCurrent)
			}
		case "ConcurrencyMean":
			if metricValue.ConcurrencyMean != expectedValue.(float64) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.ConcurrencyMean)
			}
		case "SuccessCount":
			if metricValue.SuccessCount != expectedValue.(int64) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.SuccessCount)
			}
		case "FailedCount":
			if metricValue.FailedCount != expectedValue.(int64) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.FailedCount)
			}
		case "StepTime":
			if metricValue.StepTime != expectedValue.(float64) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.StepTime)
			}
		case "OpsPerSec":
			if metricValue.OpsPerSec != expectedValue.(int64) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.OpsPerSec)
			}
		case "MiBPerSec":
			if metricValue.MiBPerSec != expectedValue.(int64) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.MiBPerSec)
			}
		case "MeanLatency":
			if metricValue.MeanLatency != expectedValue.(int64) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.MeanLatency)
			}
		case "MeanDuration":
			if metricValue.MeanDuration != expectedValue.(int64) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.MeanDuration)
			}
		case "MeanTTFB":
			if metricValue.MeanTTFB != expectedValue.(int64) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.MeanTTFB)
			}
		case "HasTTFB":
			if metricValue.HasTTFB != expectedValue.(bool) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.HasTTFB)
			}
		case "CompletionPercent":
			if metricValue.CompletionPercent != expectedValue.(float64) {
				t.Errorf("Field %s: expected %v, got %.1f", fieldName, expectedValue, metricValue.CompletionPercent)
			}
		case "OverallCompletionPercent":
			if metricValue.OverallCompletionPercent != expectedValue.(float64) {
				t.Errorf("Field %s: expected %v, got %.1f", fieldName, expectedValue, metricValue.OverallCompletionPercent)
			}
		case "Unbounded":
			if metricValue.Unbounded != expectedValue.(bool) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.Unbounded)
			}
		case "OverallUnbounded":
			if metricValue.OverallUnbounded != expectedValue.(bool) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.OverallUnbounded)
			}
		case "LimitType":
			if metricValue.LimitType != expectedValue.(string) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.LimitType)
			}
		case "LimitTimeSec":
			if metricValue.LimitTimeSec != expectedValue.(int64) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.LimitTimeSec)
			}
		case "HasLimit":
			if metricValue.HasLimit != expectedValue.(bool) {
				t.Errorf("Field %s: expected %v, got %v", fieldName, expectedValue, metricValue.HasLimit)
			}
		}
	}

	// Verify the metric can be used with existing pipeline (e.g., MetricsCollector)
	collector := NewMetricsCollector()
	collector.AddSample(*metric)

	samples := collector.GetSamples()
	if len(samples) != 1 {
		t.Error("JSON metric should be compatible with MetricsCollector")
	}
	if samples[0].StepID != "compatibility_test" {
		t.Error("Metric should retain all data through collection pipeline")
	}
}

func TestSchema3TimingPrefersLatencyP50(t *testing.T) {
	step := newTestStep()
	step.MetricsSchema = 3
	step.Timing.LatencyMeanUs = 10_000
	step.Timing.DurationMeanUs = 20_000
	step.Timing.Latency = &JSONTimingStat{Count: 3, MeanUs: 10_000, P50Us: 2_500}
	step.Timing.Duration = &JSONTimingStat{Count: 3, MeanUs: 20_000, P50Us: 3_500}
	step.Timing.TTFB = &JSONTimingStat{Count: 3, MeanUs: 4_000, P50Us: 750}

	client := NewSptAPIClient("http://test")
	metrics, err := client.ParseJSONMetrics(marshalSteps(t, []JSONMetricsStep{step}))
	if err != nil {
		t.Fatalf("ParseJSONMetrics failed: %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(metrics))
	}
	if metrics[0].MeanLatency != 2_500 {
		t.Fatalf("expected schema-v3 latency display value to use p50, got %d", metrics[0].MeanLatency)
	}
	if metrics[0].MeanDuration != 3_500 {
		t.Fatalf("expected schema-v3 duration display value to use p50, got %d", metrics[0].MeanDuration)
	}
	if metrics[0].MeanTTFB != 750 {
		t.Fatalf("expected schema-v3 TTFB display value to use p50, got %d", metrics[0].MeanTTFB)
	}
	if !metrics[0].HasTTFB {
		t.Fatal("expected schema-v3 TTFB sample to be marked available")
	}
}

func TestParseJSONMetricsMarksMissingTTFBUnavailable(t *testing.T) {
	step := newTestStep()
	step.MetricsSchema = 3
	step.Timing.TTFB = nil

	client := NewSptAPIClient("http://test")
	metrics, err := client.ParseJSONMetrics(marshalSteps(t, []JSONMetricsStep{step}))
	if err != nil {
		t.Fatalf("ParseJSONMetrics failed: %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(metrics))
	}
	if metrics[0].HasTTFB {
		t.Fatal("expected missing TTFB sample to be marked unavailable")
	}
	if metrics[0].MeanTTFB != 0 {
		t.Fatalf("expected unavailable TTFB value to remain zero internally, got %d", metrics[0].MeanTTFB)
	}
}

func TestParseJSONMetricsMarksNullCountTTFBUnavailable(t *testing.T) {
	step := newTestStep()
	step.MetricsSchema = 3
	step.Timing.TTFB = &JSONTimingStat{Count: 0, MeanUs: 950.0, P50Us: 900}

	client := NewSptAPIClient("http://test")
	metrics, err := client.ParseJSONMetrics(marshalSteps(t, []JSONMetricsStep{step}))
	if err != nil {
		t.Fatalf("ParseJSONMetrics failed: %v", err)
	}
	if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %d", len(metrics))
	}
	if metrics[0].HasTTFB {
		t.Fatal("expected zero-count TTFB sample to be marked unavailable")
	}
	if metrics[0].MeanTTFB != 0 {
		t.Fatalf("expected zero-count TTFB value to remain zero internally, got %d", metrics[0].MeanTTFB)
	}
}

func TestParseJSONMetricsPrefersLatestSample(t *testing.T) {
	older := newTestStep()
	older.StepID = "create-step"
	older.OpType = "CREATE"
	older.Timestamp = 1_700_000_000_000
	older.SampleTimestampRaw = "2025-10-17T19:30:00Z"
	older.Operations.SuccessCount = 6000
	older.Concurrency.Current = 48.0

	newer := newTestStep()
	newer.StepID = "delete-step"
	newer.OpType = "DELETE"
	newer.Timestamp = older.Timestamp + 5_000
	newer.SampleTimestampRaw = "2025-10-17T19:40:00Z"
	newer.Operations.SuccessCount = 1000
	newer.Concurrency.Current = 0.0

	payload := marshalSteps(t, []JSONMetricsStep{older, newer})

	client := NewSptAPIClient("http://test")
	metrics, err := client.ParseJSONMetrics(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(metrics) == 0 {
		t.Fatal("expected at least one metric")
	}

	// First element should be the latest (DELETE step has higher timestamp).
	metric := metrics[0]
	if metric.StepID != "delete-step" {
		t.Fatalf("expected latest step to be first, got %q", metric.StepID)
	}
	if metric.SuccessCount != 1000 {
		t.Fatalf("expected success count from latest step to be 1000, got %d", metric.SuccessCount)
	}
	if metric.ConcurrencyCurrent != 0 {
		t.Fatalf("expected concurrency current to be 0 from latest sample, got %d", metric.ConcurrencyCurrent)
	}
	if metric.Timestamp.UnixMilli() != newer.Timestamp {
		t.Fatalf("expected timestamp %d, got %d", newer.Timestamp, metric.Timestamp.UnixMilli())
	}
	if metric.SampleTimestamp.IsZero() || metric.SampleTimestamp.Format(time.RFC3339) != "2025-10-17T19:40:00Z" {
		t.Fatalf("expected sample timestamp to match latest sample, got %s", metric.SampleTimestamp.Format(time.RFC3339))
	}

	// Both entries should be present.
	if len(metrics) != 2 {
		t.Fatalf("expected 2 metrics, got %d", len(metrics))
	}
	if metrics[1].StepID != "create-step" {
		t.Fatalf("expected second metric to be the older step, got %q", metrics[1].StepID)
	}
}

// TestAggregateByOpType_FourOps verifies that AggregateByOpType correctly handles
// a 4-operation mixed workload (READ, CREATE, DELETE, STAT) and that STAT counts
// are not dropped during aggregation.
func TestAggregateByOpType_FourOps(t *testing.T) {
	metrics := []*PerformanceMetric{
		{
			OpType: "READ", OpsPerSec: 4000, MiBPerSec: 4,
			MeanLatency: 900, MeanDuration: 950, MeanTTFB: 300, HasTTFB: true,
			SuccessCount: 240000, FailedCount: 2,
			ConcurrencyCurrent: 5, ConcurrencyMean: 4.5,
			StepID: "mixed-1", TestState: 2, StepTime: 60.0,
		},
		{
			OpType: "CREATE", OpsPerSec: 20, MiBPerSec: 1,
			MeanLatency: 2500, MeanDuration: 2800, MeanTTFB: 700,
			SuccessCount: 1200, FailedCount: 0,
			ConcurrencyCurrent: 1, ConcurrencyMean: 0.1,
			StepID: "mixed-1", TestState: 2, StepTime: 60.0,
		},
		{
			OpType: "DELETE", OpsPerSec: 18, MiBPerSec: 0,
			MeanLatency: 1300, MeanDuration: 1500, MeanTTFB: 450,
			SuccessCount: 1080, FailedCount: 0,
			ConcurrencyCurrent: 1, ConcurrencyMean: 0.04,
			StepID: "mixed-1", TestState: 2, StepTime: 60.0,
		},
		{
			OpType: "STAT", OpsPerSec: 1400, MiBPerSec: 0,
			MeanLatency: 1500, MeanDuration: 1650, MeanTTFB: 500,
			SuccessCount: 84000, FailedCount: 5,
			ConcurrencyCurrent: 2, ConcurrencyMean: 1.4,
			StepID: "mixed-1", TestState: 2, StepTime: 60.0,
		},
	}

	combined, perOp := AggregateByOpType(metrics)
	if combined == nil {
		t.Fatal("combined aggregate must not be nil")
	}
	if combined.OpType != "MIXED" {
		t.Errorf("expected OpType 'MIXED', got %q", combined.OpType)
	}

	// Additive fields
	wantOps := int64(4000 + 20 + 18 + 1400)
	if combined.OpsPerSec != wantOps {
		t.Errorf("OpsPerSec: want %d, got %d", wantOps, combined.OpsPerSec)
	}
	wantSuccess := int64(240000 + 1200 + 1080 + 84000)
	if combined.SuccessCount != wantSuccess {
		t.Errorf("SuccessCount: want %d, got %d", wantSuccess, combined.SuccessCount)
	}
	wantFailed := int64(2 + 0 + 0 + 5)
	if combined.FailedCount != wantFailed {
		t.Errorf("FailedCount: want %d, got %d", wantFailed, combined.FailedCount)
	}
	wantMiB := int64(4 + 1 + 0 + 0)
	if combined.MiBPerSec != wantMiB {
		t.Errorf("MiBPerSec: want %d, got %d", wantMiB, combined.MiBPerSec)
	}

	// Per-op map must contain all 4 types
	if len(perOp) != 4 {
		t.Fatalf("expected 4 entries in perOp map, got %d", len(perOp))
	}
	for _, opType := range []string{"READ", "CREATE", "DELETE", "STAT"} {
		m, ok := perOp[opType]
		if !ok {
			t.Errorf("perOp map missing key %q", opType)
			continue
		}
		if m.OpType != opType {
			t.Errorf("perOp[%q].OpType = %q", opType, m.OpType)
		}
	}

	// STAT-specific checks: counts must not be zeroed or merged into another type
	statMetric := perOp["STAT"]
	if statMetric.SuccessCount != 84000 {
		t.Errorf("STAT SuccessCount: want 84000, got %d", statMetric.SuccessCount)
	}
	if statMetric.OpsPerSec != 1400 {
		t.Errorf("STAT OpsPerSec: want 1400, got %d", statMetric.OpsPerSec)
	}
	if statMetric.FailedCount != 5 {
		t.Errorf("STAT FailedCount: want 5, got %d", statMetric.FailedCount)
	}

	// Weighted latency: (4000*900 + 20*2500 + 18*1300 + 1400*1500) / (4000+20+18+1400) = ~1045
	// Just verify it's between the min and max input latencies.
	if combined.MeanLatency < 900 || combined.MeanLatency > 2500 {
		t.Errorf("weighted MeanLatency %d outside expected range [900, 2500]", combined.MeanLatency)
	}
	if combined.HasTTFB {
		t.Error("mixed READ/CREATE/DELETE/STAT aggregate must not publish a blended TTFB")
	}
	if combined.MeanTTFB != 0 {
		t.Errorf("mixed aggregate TTFB should remain unset internally, got %d", combined.MeanTTFB)
	}
}

func TestAggregateByOpTypeAggregatesSameOpTTFB(t *testing.T) {
	metrics := []*PerformanceMetric{
		{OpType: "READ", OpsPerSec: 100, MeanLatency: 1000, MeanDuration: 1200, MeanTTFB: 200, HasTTFB: true},
		{OpType: "READ", OpsPerSec: 300, MeanLatency: 2000, MeanDuration: 2200, MeanTTFB: 400, HasTTFB: true},
	}

	combined, _ := AggregateByOpType(metrics)
	if combined == nil {
		t.Fatal("combined aggregate must not be nil")
	}
	if !combined.HasTTFB {
		t.Fatal("expected same-op READ aggregate TTFB to be available")
	}
	if combined.MeanTTFB != 350 {
		t.Fatalf("expected ops-weighted TTFB 350, got %d", combined.MeanTTFB)
	}
}

// TestParseJSONMetricsMixedOpTypes verifies that a mixed workload step with
// multiple op-types sharing the same step_id returns all entries.
func TestParseJSONMetricsMixedOpTypes(t *testing.T) {
	ts := int64(1_700_000_000_000)
	sampleTS := "2025-10-17T19:30:00Z"

	createStep := newTestStep()
	createStep.StepID = "mixed-step-1"
	createStep.OpType = "CREATE"
	createStep.Timestamp = ts
	createStep.SampleTimestampRaw = sampleTS
	createStep.Operations.SuccessCount = 5000
	createStep.Operations.SuccessRateLast = 100.0
	createStep.Bandwidth.BytesRateLast = 52_428_800.0

	readStep := newTestStep()
	readStep.StepID = "mixed-step-1"
	readStep.OpType = "READ"
	readStep.Timestamp = ts
	readStep.SampleTimestampRaw = sampleTS
	readStep.Operations.SuccessCount = 3000
	readStep.Operations.SuccessRateLast = 60.0
	readStep.Bandwidth.BytesRateLast = 31_457_280.0

	deleteStep := newTestStep()
	deleteStep.StepID = "mixed-step-1"
	deleteStep.OpType = "DELETE"
	deleteStep.Timestamp = ts
	deleteStep.SampleTimestampRaw = sampleTS
	deleteStep.Operations.SuccessCount = 1000
	deleteStep.Operations.SuccessRateLast = 20.0
	deleteStep.Bandwidth.BytesRateLast = 0

	payload := marshalSteps(t, []JSONMetricsStep{createStep, readStep, deleteStep})

	client := NewSptAPIClient("http://test")
	metrics, err := client.ParseJSONMetrics(payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(metrics) != 3 {
		t.Fatalf("expected 3 metrics for mixed step, got %d", len(metrics))
	}

	// All entries share the same step_id and timestamp.
	opTypes := make(map[string]bool)
	for _, m := range metrics {
		if m.StepID != "mixed-step-1" {
			t.Errorf("expected step_id 'mixed-step-1', got %q", m.StepID)
		}
		opTypes[m.OpType] = true
	}
	for _, expected := range []string{"CREATE", "READ", "DELETE"} {
		if !opTypes[expected] {
			t.Errorf("missing op_type %q in metrics slice", expected)
		}
	}
}
