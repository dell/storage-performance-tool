/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

const apiLogPreviewLen = 160

// ErrMetricsIncompatible indicates that the metrics payload cannot be consumed by this client.
var ErrMetricsIncompatible = errors.New("spt metrics payload incompatible with this client")

// SptAPIClient handles communication with the Spt REST API
type SptAPIClient struct {
	baseURL    string
	httpClient *http.Client
	runID      string
	mu         sync.Mutex
}

// HTTPStatusError is returned when the Spt API responds with a non-200 status code.
type HTTPStatusError struct {
	StatusCode  int
	URL         string
	BodyPreview string
}

func (e *HTTPStatusError) Error() string {
	return fmt.Sprintf("http %d calling %s", e.StatusCode, e.URL)
}

type readinessResponse struct {
	Ready  bool   `json:"ready"`
	Status string `json:"status"`
	Scope  string `json:"scope"`
	Role   string `json:"role"`
	NodeID string `json:"node_id"`
}

type healthResponse struct {
	Status    string `json:"status"`
	Scope     string `json:"scope"`
	Role      string `json:"role"`
	NodeID    string `json:"node_id"`
	ClusterID string `json:"cluster_id"`
}

// TestStatus represents the status of a running test
type TestStatus struct {
	State    string  // RUNNING, COMPLETED, FAILED, IDLE
	Progress float64 // Progress percentage if available
	Message  string  // Status message or error
	RunID    string  // Current run ID
}

// JSONMetricsStep represents a single load step's metrics from the JSON endpoint
type JSONMetricsStep struct {
	MetricsSchema      int                    `json:"metrics_schema"`
	Scope              string                 `json:"scope"`
	Role               string                 `json:"role"`
	ClusterID          string                 `json:"cluster_id,omitempty"`
	NodeID             string                 `json:"node_id"`
	RunID              string                 `json:"run_id"`
	SampleTimestampRaw string                 `json:"sample_ts"`
	StepID             string                 `json:"step_id"`
	OpType             string                 `json:"op_type"`
	Timestamp          int64                  `json:"timestamp"`
	ElapsedTimeSeconds float64                `json:"elapsed_time_seconds"`
	TestState          int                    `json:"test_state"`
	CompletionPercent  int                    `json:"completion_percent"`
	OverallCompletion  int                    `json:"overall_completion_percent"`
	Unbounded          bool                   `json:"unbounded"`
	OverallUnbounded   bool                   `json:"overall_unbounded"`
	Operations         JSONMetricsOperations  `json:"operations"`
	Bandwidth          JSONMetricsBandwidth   `json:"bandwidth"`
	Timing             JSONMetricsTiming      `json:"timing"`
	Concurrency        JSONMetricsConcurrency `json:"concurrency"`
	Limit              *JSONMetricsLimit      `json:"limit,omitempty"`
	NodesCount         int                    `json:"nodes_count,omitempty"`
	NodesPresent       []string               `json:"nodes_present,omitempty"`
	Partial            bool                   `json:"partial,omitempty"`
}

// JSONMetricsOperations contains operation-related metrics
type JSONMetricsOperations struct {
	SuccessCount    int64   `json:"success_count"`
	FailedCount     int64   `json:"failed_count"`
	SuccessRateLast float64 `json:"success_rate_last"`
	FailedRateLast  float64 `json:"failed_rate_last"`
}

// JSONMetricsBandwidth contains bandwidth-related metrics
type JSONMetricsBandwidth struct {
	BytesTotal    int64   `json:"bytes_total"`
	BytesRateLast float64 `json:"bytes_rate_last"`
}

// JSONMetricsTiming contains timing-related metrics
type JSONMetricsTiming struct {
	LatencyMeanUs  float64 `json:"latency_mean_us"`
	DurationMeanUs float64 `json:"duration_mean_us"`
}

// JSONMetricsConcurrency contains concurrency-related metrics
type JSONMetricsConcurrency struct {
	Current int     `json:"current"`
	Mean    float64 `json:"mean"`
}

// JSONMetricsLimit summarizes which limit governs completion
type JSONMetricsLimit struct {
	Type    string `json:"type"` // "op_count" | "time" | "none"
	OpCount int64  `json:"op_count,omitempty"`
	TimeSec int64  `json:"time_sec,omitempty"`
}

// NewSptAPIClient creates a new API client
func NewSptAPIClient(baseURL string) *SptAPIClient {
	return &SptAPIClient{
		baseURL: baseURL,
		httpClient: &http.Client{
			Timeout: 10 * time.Second, // Default timeout for API calls
		},
	}
}

// WaitForAPIReady waits for the Spt API to become ready
func (c *SptAPIClient) WaitForAPIReady(timeout time.Duration) error {
	logging.LogInfo("spt-api", "waiting for API to become ready", "timeout", timeout, "url", c.baseURL)

	deadline := time.Now().Add(timeout)
	attempt := 0
	firstReadyLogged := false
	identityLogged := false

	for time.Now().Before(deadline) {
		attempt++
		ready, info, body, statusCode, err := c.probeReady()
		if err != nil {
			logging.LogDebug("spt-api", "readiness probe failed",
				"attempt", attempt,
				"error", err)
			goto wait
		}

		if !firstReadyLogged {
			firstReadyLogged = true
			logging.LogDebug("spt-api", "initial /ready status",
				"attempt", attempt,
				"status_code", statusCode,
				"status", info.Status,
				"role", info.Role,
				"scope", info.Scope,
				"node", info.NodeID,
				"body", truncateForLog(string(body), apiLogPreviewLen))
			if !identityLogged {
				c.logHealthIdentity()
				identityLogged = true
			}
		}

		if ready {
			logging.LogInfo("spt-api", "API is ready",
				"attempts", attempt,
				"status", info.Status,
				"role", info.Role,
				"scope", info.Scope,
				"node", info.NodeID)
			if !identityLogged {
				c.logHealthIdentity()
			}
			c.logRunServletStatus()
			return nil
		}

		logging.LogDebug("spt-api", "API not ready yet",
			"attempt", attempt,
			"status", info.Status,
			"role", info.Role,
			"scope", info.Scope,
			"body", truncateForLog(string(body), apiLogPreviewLen))

	wait:
		time.Sleep(500 * time.Millisecond)
	}

	return fmt.Errorf("spt API did not become ready after %v", timeout)
}

// StartTest starts a new test run with the provided scenario and defaults
func (c *SptAPIClient) StartTest(scenario, defaults []byte) (string, error) {
	logging.LogInfo("spt-api", "starting test", "scenario_size", len(scenario), "defaults_size", len(defaults))

	// Helper to build a fresh request each attempt (multipart cannot be reused)
	buildRequest := func() (*http.Request, string, error) {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)

		scenPart, err := writer.CreateFormFile("scenario", "scenario.js")
		if err != nil {
			return nil, "", fmt.Errorf("failed to create scenario part: %w", err)
		}
		if _, err := scenPart.Write(scenario); err != nil {
			return nil, "", fmt.Errorf("failed to write scenario: %w", err)
		}

		defPart, err := writer.CreateFormFile("defaults", "defaults.yaml")
		if err != nil {
			return nil, "", fmt.Errorf("failed to create defaults part: %w", err)
		}
		if _, err := defPart.Write(defaults); err != nil {
			return nil, "", fmt.Errorf("failed to write defaults: %w", err)
		}

		if err := writer.Close(); err != nil {
			return nil, "", fmt.Errorf("failed to close multipart writer: %w", err)
		}

		req, err := http.NewRequest("POST", c.baseURL+"/run", &body)
		if err != nil {
			return nil, "", fmt.Errorf("failed to create request: %w", err)
		}
		contentType := writer.FormDataContentType()
		req.Header.Set("Content-Type", contentType)
		return req, contentType, nil
	}

	// Retry a few times on 405 (race while /run is being registered)
	const maxAttempts = 5
	const retryDelay = 250 * time.Millisecond

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		req, _, err := buildRequest()
		if err != nil {
			return "", err
		}

		resp, err := c.httpClient.Do(req)
		if err != nil {
			return "", fmt.Errorf("failed to send request: %w", err)
		}

		// Read body on error paths; defer close per attempt
		if resp.StatusCode == 200 || resp.StatusCode == 201 || resp.StatusCode == 202 {
			// Success path
			runID := resp.Header.Get("ETag")
			if runID == "" {
				bodyData, _ := io.ReadAll(resp.Body)
				logging.LogDebug("spt-api", "no ETag header, checking response body", "body", string(bodyData))
				runID = fmt.Sprintf("run-%d", time.Now().Unix())
			}
			_ = resp.Body.Close()
			c.mu.Lock()
			c.runID = strings.Trim(runID, `"`)
			c.mu.Unlock()
			logging.LogInfo("spt-api", "test started successfully", "runID", c.runID)
			return c.runID, nil
		}

		// Handle 405 specifically with retry
		if resp.StatusCode == http.StatusMethodNotAllowed && attempt < maxAttempts {
			bodyData, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			logging.LogDebug("spt-api", "received 405 from /run, retrying", "attempt", attempt, "body", string(bodyData))
			time.Sleep(retryDelay)
			continue
		}

		// Other non-success statuses: return error
		bodyData, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return "", fmt.Errorf("failed to start test (status %d): %s", resp.StatusCode, string(bodyData))
	}

	return "", fmt.Errorf("failed to start test: exceeded retry attempts")
}

// GetStatus retrieves the current test status
func (c *SptAPIClient) GetStatus() (*TestStatus, error) {
	// Try the /status endpoint first
	resp, err := c.httpClient.Get(c.baseURL + "/status")
	if err != nil {
		return nil, fmt.Errorf("failed to get status: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Handle different response codes
	if resp.StatusCode == 404 {
		// No test running
		return &TestStatus{
			State:   constants.StateIdle,
			Message: "No test running",
			RunID:   c.getRunID(),
		}, nil
	}

	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// Parse JSON response
	bodyData, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	var statusData map[string]interface{}
	if err := json.Unmarshal(bodyData, &statusData); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	status := &TestStatus{
		RunID: c.getRunID(),
	}

	// Extract fields from JSON
	if state, ok := statusData["state"].(string); ok {
		status.State = state
	} else {
		status.State = constants.UIStateUnknown
	}

	if msg, ok := statusData["message"].(string); ok {
		status.Message = msg
	}

	if runID, ok := statusData["runId"].(string); ok && runID != "" {
		status.RunID = runID
		// Update stored run ID
		c.mu.Lock()
		c.runID = runID
		c.mu.Unlock()
	}

	if progress, ok := statusData["progress"].(float64); ok {
		status.Progress = progress
	}

	return status, nil
}

// getTestDetails removed as unused (dead code)

// StopTest stops the currently running test gracefully
func (c *SptAPIClient) StopTest() error {
	runID := c.getRunID()

	// If we don't have a run ID, try to get it from HEAD request
	if runID == "" {
		if status, err := c.GetStatus(); err == nil && status.RunID != "" {
			runID = status.RunID
		}
	}

	logging.LogInfo("spt-api", "stopping test", "runID", runID)

	// Build the DELETE request URL
	url := c.baseURL + "/run"
	if runID != "" {
		url = fmt.Sprintf("%s?runId=%s", url, runID)
	}

	// Send DELETE request
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create DELETE request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to send DELETE request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Check response status
	if resp.StatusCode != 200 && resp.StatusCode != 204 {
		bodyData, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("failed to stop test (status %d): %s", resp.StatusCode, string(bodyData))
	}

	// Clear the stored run ID
	c.mu.Lock()
	c.runID = ""
	c.mu.Unlock()

	logging.LogInfo("spt-api", "test stopped successfully")
	return nil
}

// GetJSONMetrics fetches the current metrics from the JSON endpoint.
func (c *SptAPIClient) GetJSONMetrics() (string, error) {
	return c.getJSONMetrics(false)
}

// GetJSONMetricsVerbose fetches the metrics payload with verbose diagnostics enabled.
func (c *SptAPIClient) GetJSONMetricsVerbose() (string, error) {
	return c.getJSONMetrics(true)
}

func (c *SptAPIClient) getJSONMetrics(verbose bool) (string, error) {
	url := c.baseURL + constants.SptMetricsEndpoint
	if verbose {
		url += "?verbose=1"
	}

	resp, err := c.httpClient.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to fetch JSON metrics: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	bodyData, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read JSON metrics response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", &HTTPStatusError{
			StatusCode:  resp.StatusCode,
			URL:         url,
			BodyPreview: truncateForLog(string(bodyData), apiLogPreviewLen),
		}
	}

	return string(bodyData), nil
}

// ParseJSONMetrics parses JSON format metrics into PerformanceMetric
func (c *SptAPIClient) ParseJSONMetrics(data string) (*PerformanceMetric, error) {
	var steps []JSONMetricsStep

	if err := json.Unmarshal([]byte(data), &steps); err != nil {
		return nil, fmt.Errorf("failed to parse JSON metrics: %w", err)
	}

	if len(steps) == 0 {
		return nil, fmt.Errorf("no metrics steps found in JSON response: %w", ErrMetricsIncompatible)
	}

	// Always operate on the most recent element (first entry).
	step := steps[0]

	if step.MetricsSchema == 0 {
		return nil, fmt.Errorf("%w: metrics_schema missing", ErrMetricsIncompatible)
	}
	if step.MetricsSchema < 2 {
		return nil, fmt.Errorf("%w: metrics_schema=%d below minimum", ErrMetricsIncompatible, step.MetricsSchema)
	}

	scope := strings.ToLower(step.Scope)
	if scope != "node" {
		return nil, fmt.Errorf("%w: metrics scope %q incompatible", ErrMetricsIncompatible, step.Scope)
	}
	if strings.TrimSpace(step.Role) == "" {
		return nil, fmt.Errorf("%w: metrics role missing", ErrMetricsIncompatible)
	}
	if strings.TrimSpace(step.SampleTimestampRaw) == "" {
		return nil, fmt.Errorf("%w: sample_ts missing", ErrMetricsIncompatible)
	}

	sampleTimestamp, err := parseSampleTimestamp(step.SampleTimestampRaw)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid sample_ts value: %s", ErrMetricsIncompatible, err.Error())
	}

	// Convert to our standard PerformanceMetric format
	metric := &PerformanceMetric{
		Timestamp:                time.UnixMilli(step.Timestamp),
		SampleTimestamp:          sampleTimestamp,
		SampleTimestampRaw:       step.SampleTimestampRaw,
		StepID:                   step.StepID,
		OpType:                   step.OpType,
		TestState:                step.TestState,
		ConcurrencyCurrent:       int64(step.Concurrency.Current),
		ConcurrencyMean:          step.Concurrency.Mean,
		SuccessCount:             step.Operations.SuccessCount,
		FailedCount:              step.Operations.FailedCount,
		StepTime:                 step.ElapsedTimeSeconds,
		OpsPerSec:                int64(step.Operations.SuccessRateLast),
		MBPerSec:                 int64(step.Bandwidth.BytesRateLast / 1_000_000), // Convert bytes to MB
		MeanLatency:              int64(step.Timing.LatencyMeanUs),                // Already in microseconds
		MeanDuration:             int64(step.Timing.DurationMeanUs),               // Already in microseconds
		CompletionPercent:        float64(step.CompletionPercent),
		OverallCompletionPercent: float64(step.OverallCompletion),
		Unbounded:                step.Unbounded,
		OverallUnbounded:         step.OverallUnbounded,
		MetricsSchema:            step.MetricsSchema,
		Scope:                    scope,
		Role:                     step.Role,
		ClusterID:                step.ClusterID,
		NodeID:                   step.NodeID,
		RunID:                    step.RunID,
		NodesCount:               step.NodesCount,
		NodesPresent:             append([]string(nil), step.NodesPresent...),
		Partial:                  step.Partial,
	}

	if step.Limit != nil {
		metric.HasLimit = true
		metric.LimitType = step.Limit.Type
		metric.LimitOpCount = step.Limit.OpCount
		metric.LimitTimeSec = step.Limit.TimeSec
	}

	return metric, nil
}

func parseSampleTimestamp(raw string) (time.Time, error) {
	if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return ts, nil
	}
	return time.Parse(time.RFC3339, raw)
}

func (c *SptAPIClient) probeReady() (bool, readinessResponse, []byte, int, error) {
	var info readinessResponse
	resp, err := c.httpClient.Get(c.baseURL + "/ready")
	if err != nil {
		return false, info, nil, 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, info, nil, resp.StatusCode, fmt.Errorf("failed to read /ready body: %w", err)
	}

	if len(body) > 0 {
		if err := json.Unmarshal(body, &info); err != nil {
			logging.LogDebug("spt-api", "failed to decode /ready body",
				"error", err,
				"body", truncateForLog(string(body), apiLogPreviewLen))
		}
	}

	switch resp.StatusCode {
	case http.StatusOK:
		if !info.Ready {
			info.Ready = true
			if info.Status == "" {
				info.Status = "ready"
			}
		}
		return true, info, body, resp.StatusCode, nil
	case http.StatusServiceUnavailable:
		if info.Status == "" {
			info.Status = "starting"
		}
		return false, info, body, resp.StatusCode, nil
	default:
		return false, info, body, resp.StatusCode, fmt.Errorf("unexpected /ready status %d", resp.StatusCode)
	}
}

func (c *SptAPIClient) logHealthIdentity() {
	resp, err := c.httpClient.Get(c.baseURL + "/health")
	if err != nil {
		logging.LogDebug("spt-api", "health probe failed", "error", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logging.LogDebug("spt-api", "failed to read /health body", "error", err)
		return
	}

	var payload healthResponse
	if len(body) > 0 {
		if err := json.Unmarshal(body, &payload); err != nil {
			logging.LogDebug("spt-api", "failed to decode /health body",
				"error", err,
				"body", truncateForLog(string(body), apiLogPreviewLen))
		}
	}

	logging.LogDebug("spt-api", "node identity",
		"status", payload.Status,
		"role", payload.Role,
		"scope", payload.Scope,
		"node", payload.NodeID,
		"cluster", payload.ClusterID,
		"http_status", resp.StatusCode)
}

func (c *SptAPIClient) logRunServletStatus() {
	req, err := http.NewRequest(http.MethodHead, c.baseURL+"/run", nil)
	if err != nil {
		logging.LogDebug("spt-api", "failed to construct HEAD /run request", "error", err)
		return
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logging.LogDebug("spt-api", "HEAD /run probe failed", "error", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	logging.LogDebug("spt-api", "HEAD /run status",
		"status", resp.StatusCode)
}

// LogReadySnapshot emits a single-line readiness snapshot using the /ready endpoint.
func (c *SptAPIClient) LogReadySnapshot(label string) {
	if c == nil {
		return
	}
	ready, info, _, statusCode, err := c.probeReady()
	if err != nil {
		logging.LogDebug("spt-api", "ready snapshot failed",
			"label", label,
			"error", err)
		return
	}
	status := info.Status
	if status == "" {
		if ready {
			status = "ready"
		} else {
			status = "unknown"
		}
	}
	logging.LogInfo("spt-api", "ready snapshot",
		"label", label,
		"ready", ready,
		"status_code", statusCode,
		"status", status,
		"role", info.Role,
		"scope", info.Scope,
		"node", info.NodeID)
}

// Shutdown requests a graceful node shutdown via /shutdown.
func (c *SptAPIClient) Shutdown() error {
	req, err := http.NewRequest(http.MethodPost, c.baseURL+"/shutdown", nil)
	if err != nil {
		return fmt.Errorf("failed to create shutdown request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("shutdown request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("/shutdown status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

// WaitForLinger waits for /status to keep returning a terminal state for the given duration.
func (c *SptAPIClient) WaitForLinger(linger time.Duration) error {
	if linger <= 0 {
		return nil
	}
	deadline := time.Now().Add(linger)
	sawTerminal := false
	for time.Now().Before(deadline) {
		st, err := c.GetStatus()
		if err != nil {
			return fmt.Errorf("status probe during linger: %w", err)
		}
		switch st.State {
		case constants.StateCompleted, constants.StateFailed, constants.StateStopped, constants.StateIdle:
			sawTerminal = true
		default:
			return fmt.Errorf("non-terminal state during linger: %s", st.State)
		}
		time.Sleep(200 * time.Millisecond)
	}
	if !sawTerminal {
		return fmt.Errorf("no terminal status observed during linger")
	}
	return nil
}

// getRunID safely retrieves the current run ID
func (c *SptAPIClient) getRunID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.runID
}

// SetRunID sets the run ID (useful for testing)
func (c *SptAPIClient) SetRunID(runID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.runID = runID
}
