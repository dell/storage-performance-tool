/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// SptAPIClient handles interactions with the Spt REST API
type SptAPIClient struct {
	BaseURL       string
	HTTPClient    *http.Client
	Clock         Clock
	expectedRunID int64
}

type readinessPayload struct {
	Ready  bool   `json:"ready"`
	Status string `json:"status"`
	Scope  string `json:"scope"`
	Role   string `json:"role"`
	NodeID string `json:"node_id"`
}

type healthPayload struct {
	Status    string `json:"status"`
	Scope     string `json:"scope"`
	Role      string `json:"role"`
	NodeID    string `json:"node_id"`
	ClusterID string `json:"cluster_id"`
}

// NewSptAPIClient creates a new Spt API client
func NewSptAPIClient(baseURL string, timeout time.Duration) *SptAPIClient {
	return &SptAPIClient{
		BaseURL: strings.TrimSuffix(baseURL, "/"),
		HTTPClient: &http.Client{
			Timeout: timeout,
		},
		Clock: &RealClock{},
	}
}

// NewSptAPIClientWithClock creates a client with a custom clock (useful for tests)
func NewSptAPIClientWithClock(baseURL string, timeout time.Duration, clock Clock) *SptAPIClient {
	c := NewSptAPIClient(baseURL, timeout)
	if clock != nil {
		c.Clock = clock
	}
	return c
}

// SetExpectedRunID binds shutdown-status evidence to the run owned by the
// caller. A zero value preserves the legacy unbound behavior.
func (c *SptAPIClient) SetExpectedRunID(runID int64) {
	if c == nil {
		return
	}
	c.expectedRunID = runID
}

// sptRunResponse represents the JSON response from /run endpoint
type sptRunResponse struct {
	RunID     string                 `json:"runId"`
	Status    string                 `json:"status"`
	StartTime string                 `json:"startTimeISO"`
	EndTime   string                 `json:"endTimeISO,omitempty"`
	Config    map[string]interface{} `json:"config,omitempty"`
}

// sptMetricsResponse represents a simplified metrics response
type sptMetricsResponse struct {
	LastUpdateTimeISO string  `json:"lastUpdateTimeISO"`
	OpType            string  `json:"opType"`
	SuccessCount      int64   `json:"successCount"`
	FailCount         int64   `json:"failCount"`
	BytesCount        int64   `json:"bytesCount"`
	OpsPerSec         float64 `json:"opsPerSec"`
	MeanLatency       float64 `json:"meanLatencyMicros"`
	Concurrency       float64 `json:"concurrency"`
}

// GetSptStatus retrieves comprehensive status from Spt API
func GetSptStatus(ctx context.Context, baseURL string) (*SptStatus, error) {
	client := NewSptAPIClient(baseURL, DefaultHTTPTimeout)
	return client.GetStatus(ctx)
}

// GetStatus retrieves the current test status
func (c *SptAPIClient) GetStatus(ctx context.Context) (*SptStatus, error) {
	status := &SptStatus{}

	// First try to get run information
	runInfo, err := c.getRunInfo(ctx)
	if err != nil {
		logging.LogDebug("portcheck", "failed to get run info from Spt API",
			"baseURL", c.BaseURL,
			"error", err.Error())
		// Continue - we might still get some info from metrics
	}

	if runInfo != nil {
		status.RunID = runInfo.RunID
		status.State = runInfo.Status

		// Parse timestamps if available
		if runInfo.StartTime != "" {
			if startTime, parseErr := time.Parse(time.RFC3339, runInfo.StartTime); parseErr == nil {
				status.StartTime = startTime
				status.Duration = time.Since(startTime)
			}
		}

		if runInfo.EndTime != "" {
			if endTime, parseErr := time.Parse(time.RFC3339, runInfo.EndTime); parseErr == nil {
				status.EndTime = endTime
				if !status.StartTime.IsZero() {
					status.Duration = endTime.Sub(status.StartTime)
				}
			}
		}
	}

	// Try to get metrics for performance data
	metrics, err := c.getMetrics(ctx)
	if err != nil {
		logging.LogDebug("portcheck", "failed to get metrics from Spt API",
			"baseURL", c.BaseURL,
			"error", err.Error())
		// Not critical - we can still return run info
	}

	if metrics != nil {
		status.OpsPerSec = int64(metrics.OpsPerSec)
		status.CompletedItems = metrics.SuccessCount

		// Calculate progress if we have total items (this would come from config analysis)
		// Note: total item count detection requires deeper config parsing; omitted by design.
	}

	// Set a default message if we got some information
	if status.RunID != "" {
		if status.State == "" {
			status.State = constants.UIStateDetected
		}
		if status.Message == "" {
			if status.OpsPerSec > 0 {
				status.Message = fmt.Sprintf("Active test, %d ops/sec", status.OpsPerSec)
			} else {
				status.Message = "Test detected via API"
			}
		}
	}

	return status, nil
}

// getRunInfo retrieves information from the /run endpoint
func (c *SptAPIClient) getRunInfo(ctx context.Context) (*sptRunResponse, error) {
	url := c.BaseURL + "/run"

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Handle different status codes
	switch resp.StatusCode {
	case http.StatusOK:
		// Test is running or completed
		break
	case http.StatusNotFound:
		// No test running
		return nil, fmt.Errorf("no test currently running")
	default:
		// Unexpected status
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var runInfo sptRunResponse
	if err := json.Unmarshal(body, &runInfo); err != nil {
		return nil, fmt.Errorf("failed to parse JSON response: %w", err)
	}

	return &runInfo, nil
}

// getMetrics retrieves performance metrics from /metrics endpoint
func (c *SptAPIClient) getMetrics(ctx context.Context) (*sptMetricsResponse, error) {
	url := c.BaseURL + "/metrics"

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics endpoint returned status %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	// Try to parse as JSON first
	var metrics sptMetricsResponse
	if err := json.Unmarshal(body, &metrics); err != nil {
		// If JSON parsing fails, might be in a different format
		return nil, fmt.Errorf("failed to parse metrics JSON: %w", err)
	}

	return &metrics, nil
}

// terminalStatus is the stable structured /status payload used by completion tracking.
type terminalStatus struct {
	State    string `json:"state"`
	RunID    int64  `json:"run_id"`
	StepID   string `json:"step_id"`
	Category string `json:"category"`
	Message  string `json:"message"`
}

func (c *SptAPIClient) getStatusDetail(ctx context.Context) (terminalStatus, error) {
	url := c.BaseURL + "/status"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return terminalStatus{}, fmt.Errorf("failed to create request: %w", err)
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return terminalStatus{}, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return terminalStatus{}, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	var payload terminalStatus
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return terminalStatus{}, fmt.Errorf("failed to parse JSON: %w", err)
	}
	payload.State = strings.ToUpper(strings.TrimSpace(payload.State))
	if payload.State == "" {
		return terminalStatus{}, fmt.Errorf("missing state")
	}
	return payload, nil
}

// Shutdown requests a graceful node shutdown via /shutdown.
func (c *SptAPIClient) Shutdown(ctx context.Context) error {
	url := c.BaseURL + "/shutdown"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return fmt.Errorf("create shutdown request: %w", err)
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("shutdown http: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted, http.StatusNoContent:
		return nil
	default:
		b, _ := io.ReadAll(resp.Body)
		if len(b) > 0 {
			return fmt.Errorf("shutdown status %d: %s", resp.StatusCode, string(b))
		}
		return fmt.Errorf("shutdown status %d", resp.StatusCode)
	}
}

// WaitForTerminal waits until an accepted shutdown exposes an attributed
// terminal state. Distributed coordinators use this as a barrier before
// shutting down worker nodes whose RMI services are still owned by the entry
// node's active run.
func (c *SptAPIClient) WaitForTerminal(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.Clock.After(constants.APILingerPollInterval):
		}
		status, err := c.getStatusDetail(ctx)
		if err != nil {
			return fmt.Errorf("status probe while waiting for terminal shutdown: %w", err)
		}
		terminal, err := c.classifyShutdownStatus(status)
		if err != nil {
			return err
		}
		if terminal {
			return nil
		}
	}
}

// WaitForLinger allows the accepted shutdown to transition through its current
// active state, then requires attributed terminal/idle status for the remainder
// of the linger window. A nonterminal state after terminal evidence is a state
// regression and fails immediately.
func (c *SptAPIClient) WaitForLinger(ctx context.Context, linger time.Duration) error {
	if linger <= 0 {
		return nil
	}
	start := c.Clock.Now()
	terminalOnce := false
	lastState := ""
	for c.Clock.Now().Sub(start) < linger {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.Clock.After(constants.APILingerPollInterval):
		}
		status, err := c.getStatusDetail(ctx)
		if err != nil {
			return fmt.Errorf("status probe during linger: %w", err)
		}
		lastState = status.State
		terminal, err := c.classifyShutdownStatus(status)
		if err != nil {
			return err
		}
		if terminal {
			terminalOnce = true
		} else if terminalOnce {
			return fmt.Errorf(
				"non-terminal state after terminal status during linger: %s", status.State)
		}
	}
	if !terminalOnce {
		if lastState == "" {
			return fmt.Errorf("no terminal status observed during linger")
		}
		return fmt.Errorf("no terminal status observed during linger; last state: %s", lastState)
	}
	return nil
}

func (c *SptAPIClient) classifyShutdownStatus(status terminalStatus) (bool, error) {
	if c.expectedRunID > 0 && status.State != constants.StateIdle && status.RunID != c.expectedRunID {
		return false, fmt.Errorf(
			"status for run %d does not match the owned run %d",
			status.RunID, c.expectedRunID)
	}
	switch status.State {
	case constants.StateCompleted, constants.StateFailed, constants.StateStopped, constants.StateIdle:
		return true, nil
	case constants.StateStarting, constants.StateInitializing, constants.StateRunning:
		return false, nil
	default:
		return false, fmt.Errorf("unexpected state during shutdown: %s", status.State)
	}
}

// jsonMetricsStep mirrors minimal fields of /metrics/json for idle detection.
type jsonMetricsStep struct {
	RunID      flexibleInt64 `json:"run_id"`
	StepID     string        `json:"step_id"`
	Timestamp  int64         `json:"timestamp"`
	Terminal   bool          `json:"terminal"`
	Operations struct {
		SuccessRateLast float64 `json:"success_rate_last"`
	} `json:"operations"`
	Bandwidth struct {
		BytesRateLast float64 `json:"bytes_rate_last"`
	} `json:"bandwidth"`
}

// flexibleInt64 accepts the engine's string run_id as well as numeric fixtures.
type flexibleInt64 int64

func (value *flexibleInt64) UnmarshalJSON(data []byte) error {
	var number int64
	if err := json.Unmarshal(data, &number); err == nil {
		*value = flexibleInt64(number)
		return nil
	}
	var text string
	if err := json.Unmarshal(data, &text); err != nil {
		return err
	}
	if strings.TrimSpace(text) == "" {
		*value = 0
		return nil
	}
	parsed, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid run_id %q: %w", text, err)
	}
	*value = flexibleInt64(parsed)
	return nil
}

// getMetricsJSON fetches /metrics/json and decodes into steps.
func (c *SptAPIClient) getMetricsJSON(ctx context.Context) ([]jsonMetricsStep, error) {
	url := c.BaseURL + "/metrics/json"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("HTTP request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("metrics/json status %d", resp.StatusCode)
	}
	var steps []jsonMetricsStep
	if err := json.NewDecoder(resp.Body).Decode(&steps); err != nil {
		return nil, fmt.Errorf("failed to decode metrics/json: %w", err)
	}
	return steps, nil
}

// StopSptTest attempts to stop a running Spt test gracefully
func StopSptTest(ctx context.Context, baseURL string) error {
	client := NewSptAPIClient(baseURL, DefaultHTTPTimeout)
	return client.StopTest(ctx)
}

// StopTest sends a stop command to the Spt API
func (c *SptAPIClient) StopTest(ctx context.Context) error {
	url := c.BaseURL + "/run"

	logging.LogInfo("portcheck", "sending stop command to Spt API", "url", url)

	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("failed to create stop request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("stop request failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Handle different response codes
	switch resp.StatusCode {
	case http.StatusOK, http.StatusAccepted:
		// Success
		logging.LogInfo("portcheck", "Spt test stop command accepted",
			"status", resp.StatusCode)
		return nil
	case http.StatusNotFound:
		// No test was running
		logging.LogDebug("portcheck", "no test was running to stop")
		return nil
	default:
		return fmt.Errorf("stop command failed with status %d", resp.StatusCode)
	}
}

// IsSptAPI checks if the given URL appears to be a Spt API
func IsSptAPI(ctx context.Context, baseURL string) bool {
	client := NewSptAPIClient(baseURL, 3*time.Second)

	// Try a simple GET to /run endpoint
	url := client.BaseURL + "/run"
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return false
	}

	resp, err := client.HTTPClient.Do(req)
	if err != nil {
		return false
	}
	defer func() { _ = resp.Body.Close() }()

	// Spt API should respond with either 200 (test running) or 404 (no test)
	// Other HTTP services might return 405 (method not allowed) or other codes
	return resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusNotFound
}

// WaitForAPIReady waits for the Spt API to become responsive
func (c *SptAPIClient) WaitForAPIReady(ctx context.Context, timeout time.Duration) error {
	deadline := c.Clock.Now().Add(timeout)

	logging.LogInfo("portcheck", "waiting for Spt API to become ready",
		"baseURL", c.BaseURL,
		"timeout", timeout)

	attempt := 0
	initialStatusLogged := false
	identityLogged := false
	for c.Clock.Now().Before(deadline) {
		attempt++

		ready, payload, statusCode, err := c.probeReadyOnce(ctx)
		if err != nil {
			logging.LogDebug("portcheck", "readiness probe failed",
				"attempt", attempt,
				"error", err)
		} else {
			if !initialStatusLogged {
				logging.LogDebug("portcheck", "initial /ready status",
					"baseURL", c.BaseURL,
					"attempt", attempt,
					"status_code", statusCode,
					"status", payload.Status,
					"role", payload.Role,
					"scope", payload.Scope,
					"node", payload.NodeID)
				initialStatusLogged = true
				if !identityLogged {
					c.logHealthIdentity(ctx)
					identityLogged = true
				}
			}

			if ready {
				logging.LogInfo("portcheck", "Spt API is ready",
					"baseURL", c.BaseURL,
					"attempts", attempt,
					"status", payload.Status,
					"role", payload.Role,
					"scope", payload.Scope,
					"node", payload.NodeID)
				if !identityLogged {
					c.logHealthIdentity(ctx)
				}
				return nil
			}

			logging.LogDebug("portcheck", "Spt API not ready",
				"attempt", attempt,
				"status", payload.Status,
				"role", payload.Role,
				"scope", payload.Scope,
				"node", payload.NodeID)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.Clock.After(500 * time.Millisecond):
		}
	}

	return fmt.Errorf("spt API did not become ready within %v", timeout)
}

func (c *SptAPIClient) probeReadyOnce(ctx context.Context) (bool, readinessPayload, int, error) {
	var payload readinessPayload
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/ready", nil)
	if err != nil {
		return false, payload, 0, err
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return false, payload, 0, err
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, payload, resp.StatusCode, fmt.Errorf("failed to read /ready body: %w", err)
	}

	if len(body) > 0 {
		_ = json.Unmarshal(body, &payload)
	}

	switch resp.StatusCode {
	case http.StatusOK:
		if !payload.Ready {
			payload.Ready = true
			if payload.Status == "" {
				payload.Status = "ready"
			}
		}
		return true, payload, resp.StatusCode, nil
	case http.StatusServiceUnavailable:
		if payload.Status == "" {
			payload.Status = "starting"
		}
		return false, payload, resp.StatusCode, nil
	default:
		return false, payload, resp.StatusCode, fmt.Errorf("unexpected /ready status %d", resp.StatusCode)
	}
}

func (c *SptAPIClient) logHealthIdentity(ctx context.Context) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.BaseURL+"/health", nil)
	if err != nil {
		logging.LogDebug("portcheck", "failed to construct /health request", "error", err)
		return
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		logging.LogDebug("portcheck", "health probe failed", "error", err)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logging.LogDebug("portcheck", "failed to read /health body", "error", err)
		return
	}

	var payload healthPayload
	if len(body) > 0 {
		_ = json.Unmarshal(body, &payload)
	}

	logging.LogDebug("portcheck", "Spt node identity",
		"baseURL", c.BaseURL,
		"status", payload.Status,
		"role", payload.Role,
		"scope", payload.Scope,
		"node", payload.NodeID,
		"cluster", payload.ClusterID,
		"http_status", resp.StatusCode)
}

// GetFormattedDuration returns a human-readable duration string
func (s *SptStatus) GetFormattedDuration() string {
	if s.Duration == 0 {
		return "unknown"
	}

	// Format as HH:MM:SS
	hours := int(s.Duration.Hours())
	minutes := int(s.Duration.Minutes()) % 60
	seconds := int(s.Duration.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%d:%02d:%02d", hours, minutes, seconds)
	}
	return fmt.Sprintf("%d:%02d", minutes, seconds)
}

// GetFormattedProgress returns a formatted progress string
func (s *SptStatus) GetFormattedProgress() string {
	if s.Progress > 0 {
		return fmt.Sprintf("%.1f%%", s.Progress)
	}

	if s.CompletedItems > 0 && s.TotalItems > 0 {
		progress := float64(s.CompletedItems) / float64(s.TotalItems) * 100
		return fmt.Sprintf("%.1f%% (%d/%d)", progress, s.CompletedItems, s.TotalItems)
	}

	if s.CompletedItems > 0 {
		return fmt.Sprintf("%d completed", s.CompletedItems)
	}

	return "unknown"
}
