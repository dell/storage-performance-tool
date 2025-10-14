/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/mathutil"
)

const (
	endpointMetricsJSON = "/metrics/json"
	endpointMetrics     = "/metrics"
	endpointRun         = "/run"
)

const (
	// DefaultConnectTimeout is the TCP dial timeout used for port probes.
	DefaultConnectTimeout = 2 * time.Second
	// DefaultHTTPTimeout is the timeout used for HTTP-based detection.
	DefaultHTTPTimeout = 5 * time.Second
	// DefaultRetryAttempts is the default number of retries when probing services.
	DefaultRetryAttempts = 3
)

// PortChecker handles port availability detection and service identification
type PortChecker struct {
	Port           string
	Host           string        // Default to localhost
	ConnectTimeout time.Duration // Timeout for TCP connection attempts
	HTTPTimeout    time.Duration // Timeout for HTTP requests
	RetryAttempts  int           // Number of retry attempts
	httpClient     *http.Client
	clock          Clock
}

// NewPortChecker creates a new port checker with default settings
func NewPortChecker(port string) *PortChecker {
	return &PortChecker{
		Port:           port,
		Host:           "localhost",
		ConnectTimeout: DefaultConnectTimeout,
		HTTPTimeout:    DefaultHTTPTimeout,
		RetryAttempts:  DefaultRetryAttempts,
		httpClient: &http.Client{
			Timeout: DefaultHTTPTimeout,
		},
		clock: &RealClock{},
	}
}

// NewPortCheckerWithOptions creates a port checker with custom settings
func NewPortCheckerWithOptions(port, host string, connectTimeout, httpTimeout time.Duration, retryAttempts int) *PortChecker {
	return &PortChecker{
		Port:           port,
		Host:           host,
		ConnectTimeout: connectTimeout,
		HTTPTimeout:    httpTimeout,
		RetryAttempts:  retryAttempts,
		httpClient: &http.Client{
			Timeout: httpTimeout,
		},
		clock: &RealClock{},
	}
}

// IsPortInUse checks if the configured port is listening
// Returns true if the port is in use, false if available
func (p *PortChecker) IsPortInUse() bool {
	address := net.JoinHostPort(p.Host, p.Port)

	logging.LogDebug("portcheck", "checking if port is in use",
		"host", p.Host,
		"port", p.Port,
		"address", address,
		"timeout", p.ConnectTimeout)

	// Use DialTimeout for a quick connection test
	conn, err := net.DialTimeout("tcp", address, p.ConnectTimeout)
	if err != nil {
		// Connection failed - port is likely available
		logging.LogDebug("portcheck", "port appears available",
			"address", address,
			"error", err.Error())
		return false
	}

	// Connection successful - port is in use
	// Close and ignore error; connection was only used as a probe
	_ = conn.Close()
	logging.LogDebug("portcheck", "port is in use", "address", address)
	return true
}

// CheckService performs comprehensive service detection on the port
// Returns ServiceInfo with details about what's running on the port
func (p *PortChecker) CheckService(ctx context.Context) (*ServiceInfo, error) {
	info := &ServiceInfo{
		IsListening: false,
		IsSpt:       false,
		Status:      nil,
		Container:   nil,
	}

	// First check if port is listening
	if !p.IsPortInUse() {
		logging.LogDebug("portcheck", "port is not in use", "port", p.Port)
		return info, nil
	}

	info.IsListening = true
	logging.LogInfo("portcheck", "port is in use, identifying service", "port", p.Port)

	// Try to identify if it's Spt by making HTTP requests
	sptStatus, err := p.detectSptService(ctx)
	if err != nil {
		logging.LogDebug("portcheck", "failed to detect Spt service via HTTP",
			"port", p.Port,
			"error", err.Error())
		// HTTP detection failed - try Docker container detection
		info = p.enhanceInfoWithDockerDetection(ctx, info)
		return info, nil
	}

	if sptStatus != nil {
		info.IsSpt = true
		info.Status = sptStatus
		logging.LogInfo("portcheck", "detected Spt service via HTTP API",
			"port", p.Port,
			"runId", sptStatus.RunID,
			"state", sptStatus.State)
	} else {
		// HTTP detection was inconclusive - also try Docker for more complete info
		info = p.enhanceInfoWithDockerDetection(ctx, info)
	}

	return info, nil
}

// detectSptService attempts to identify if the service is Spt
// Returns SptStatus if Spt is detected, nil if not
func (p *PortChecker) detectSptService(ctx context.Context) (*SptStatus, error) {
	baseURL := fmt.Sprintf("http://%s:%s", p.Host, p.Port)
	if status := p.probeMetricsJSON(ctx, baseURL); status != nil {
		return status, nil
	}
	if status := p.probeMetrics(ctx, baseURL); status != nil {
		return status, nil
	}
	if status := p.probeRun(ctx, baseURL); status != nil {
		return status, nil
	}
	return nil, NewError("spt_detection", p.Port, fmt.Errorf("service does not appear to be Spt"))
}

func (p *PortChecker) probeMetricsJSON(ctx context.Context, baseURL string) *SptStatus {
	url := baseURL + endpointMetricsJSON
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil
	}
	resp, err := p.httpClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil
	}
	defer func() { _ = resp.Body.Close() }()
	if p.validateSptMetricsResponse(resp) {
		return p.getSptStatus(ctx, baseURL)
	}
	return nil
}

func (p *PortChecker) probeMetrics(ctx context.Context, baseURL string) *SptStatus {
	url := baseURL + endpointMetrics
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil
	}
	resp, err := p.httpClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return nil
	}
	defer func() { _ = resp.Body.Close() }()
	if p.validateSptMetricsResponse(resp) {
		return p.getSptStatus(ctx, baseURL)
	}
	return nil
}

func (p *PortChecker) probeRun(ctx context.Context, baseURL string) *SptStatus {
	url := baseURL + endpointRun
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("If-Match", "*")
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil
	}
	defer func() { _ = resp.Body.Close() }()
	switch resp.StatusCode {
	case http.StatusOK:
		if p.validateSptRunResponse(resp) {
			return p.getSptStatus(ctx, baseURL)
		}
	case http.StatusBadRequest:
		if p.isSptBadRequestError(resp) {
			return p.getSptStatus(ctx, baseURL)
		}
		return nil
	}
	return nil
}

// validateSptMetricsResponse checks if an HTTP response contains valid Spt metrics JSON
// Handles both array and object responses from Spt
func (p *PortChecker) validateSptMetricsResponse(resp *http.Response) bool {
	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logging.LogDebug("portcheck", "failed to read response body", "error", err.Error())
		return false
	}

	bodyStr := strings.TrimSpace(string(body))
	logging.LogDebug("portcheck", "received metrics response", "body", bodyStr)

	// Check if response is an empty array (common when no metrics yet)
	if bodyStr == "[]" {
		logging.LogDebug("portcheck", "empty array response - valid Spt with no metrics")
		return true
	}

	// Try to parse as array first (Spt v5 sometimes returns arrays)
	var metricsArray []map[string]interface{}
	if err := json.Unmarshal(body, &metricsArray); err == nil {
		if len(metricsArray) == 0 {
			logging.LogDebug("portcheck", "empty metrics array - valid Spt")
			return true
		}

		// Check first element for Spt fields
		if len(metricsArray) > 0 {
			logging.LogDebug("portcheck", "validating first element of metrics array")
			return p.validateSptFields(metricsArray[0])
		}
	}

	// Try to parse as object (traditional format)
	var metrics map[string]interface{}
	if err := json.Unmarshal(body, &metrics); err != nil {
		logging.LogDebug("portcheck", "response is not valid JSON array or object", "error", err.Error())
		return false
	}

	return p.validateSptFields(metrics)
}

// validateSptFields checks if a JSON object contains Spt-specific fields
func (p *PortChecker) validateSptFields(metrics map[string]interface{}) bool {
	// Check for Spt-specific fields that should be present in metrics
	sptFields := []string{
		"lastUpdateTimeISO", // Spt timestamp format
		"opType",            // Operation type (create, read, etc.)
		"opsPerSec",         // Operations per second
		"meanLatencyMicros", // Mean latency in microseconds
	}

	foundFields := 0
	for _, field := range sptFields {
		if _, exists := metrics[field]; exists {
			foundFields++
		}
	}

	// Require at least 2 Spt-specific fields to be confident
	if foundFields >= 2 {
		logging.LogDebug("portcheck", "validated Spt metrics fields",
			"found_fields", foundFields,
			"required_fields", 2)
		return true
	}

	// Also check for alternative field names or structures that might indicate Spt
	alternativeIndicators := []string{
		"concurrency",  // Spt concurrency metric
		"successCount", // Success count
		"failCount",    // Failure count
		"bytesCount",   // Bytes transferred
	}

	for _, field := range alternativeIndicators {
		if _, exists := metrics[field]; exists {
			foundFields++
		}
	}

	// With alternative fields, still require at least 2 total
	isSpt := foundFields >= 2
	logging.LogDebug("portcheck", "Spt metrics validation result",
		"is_spt", isSpt,
		"total_fields_found", foundFields)

	return isSpt
}

// validateSptRunResponse checks if an HTTP response from /run endpoint contains valid Spt run info
func (p *PortChecker) validateSptRunResponse(resp *http.Response) bool {
	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logging.LogDebug("portcheck", "failed to read /run response body", "error", err.Error())
		return false
	}

	// Try to parse as JSON
	var runInfo map[string]interface{}
	if err := json.Unmarshal(body, &runInfo); err != nil {
		logging.LogDebug("portcheck", "/run response is not valid JSON", "error", err.Error())
		return false
	}

	// Check for Spt-specific fields in run response
	sptRunFields := []string{
		"runId",        // Spt run identifier
		"status",       // Test status
		"startTimeISO", // Spt timestamp format
	}

	foundFields := 0
	for _, field := range sptRunFields {
		if _, exists := runInfo[field]; exists {
			foundFields++
		}
	}

	// Alternative field names
	if _, exists := runInfo["startTime"]; exists {
		foundFields++
	}

	// Require at least 2 Spt-specific fields for run info
	isSptRun := foundFields >= 2
	logging.LogDebug("portcheck", "Spt /run validation result",
		"is_spt_run", isSptRun,
		"fields_found", foundFields)

	return isSptRun
}

// isSptResponse checks if an HTTP response indicates Spt
// isSptResponse moved to test helpers (unused in production)

// getSptStatus retrieves detailed status from Spt API
func (p *PortChecker) getSptStatus(ctx context.Context, baseURL string) *SptStatus {
	client := NewSptAPIClient(baseURL, p.HTTPTimeout)
	// Share the same clock so tests can inject a fake clock
	client.Clock = p.clock

	status, err := client.GetStatus(ctx)
	if err != nil {
		logging.LogDebug("portcheck", "failed to retrieve Spt status",
			"baseURL", baseURL,
			"error", err.Error())
		return &SptStatus{Message: "Spt API detected but status unavailable"}
	}
	return status
}

// Close cleans up resources used by the port checker
func (p *PortChecker) Close() error {
	// Close HTTP client if it has custom configuration
	if p.httpClient != nil {
		p.httpClient.CloseIdleConnections()
	}
	return nil
}

// ValidatePort checks if the port string is valid
func ValidatePort(port string) error {
	if port == "" {
		return fmt.Errorf("port cannot be empty")
	}

	// Remove leading/trailing whitespace
	port = strings.TrimSpace(port)

	// Check if result is empty after trimming
	if port == "" {
		return fmt.Errorf("port cannot be empty or whitespace only")
	}

	// Try to parse as network address to validate
	_, err := net.ResolveTCPAddr("tcp", "localhost:"+port)
	if err != nil {
		return fmt.Errorf("invalid port format: %w", err)
	}

	return nil
}

// GetServiceDescription returns a human-readable description of the service
func (info *ServiceInfo) GetServiceDescription() string {
	if !info.IsListening {
		return "Port is available"
	}

	if info.IsSpt && info.Status != nil {
		if info.Status.RunID != "" && info.Status.State != "" {
			return fmt.Sprintf("Spt test (Run ID: %s, State: %s)",
				info.Status.RunID, info.Status.State)
		}
		return "Spt service"
	}

	if info.Container != nil {
		return fmt.Sprintf("Container: %s (%s)", info.Container.Name, info.Container.Image)
	}

	return "Unknown service"
}

// IsTestActive returns true if there's an active Spt test
func (info *ServiceInfo) IsTestActive() bool {
	if !info.IsSpt || info.Status == nil {
		return false
	}

	// Consider test active if it's in a running state
	activeStates := []string{constants.StateRunning, constants.StateStarting, constants.StateInitializing}
	for _, state := range activeStates {
		if strings.EqualFold(info.Status.State, state) {
			return true
		}
	}

	return false
}

// enhanceInfoWithDockerDetection attempts to enhance ServiceInfo using Docker container detection
func (p *PortChecker) enhanceInfoWithDockerDetection(ctx context.Context, info *ServiceInfo) *ServiceInfo {
	logging.LogDebug("portcheck", "attempting Docker container detection", "port", p.Port)

	// Create Docker container finder
	dockerFinder, err := NewDockerContainerFinder()
	if err != nil {
		logging.LogDebug("portcheck", "Docker not available for container detection",
			"port", p.Port,
			"error", err.Error())
		return info // Return original info if Docker not available
	}
	defer func() { _ = dockerFinder.Close() }()

	// Find container using the port
	container, err := dockerFinder.FindContainerUsingPort(ctx, p.Port)
	if err != nil {
		logging.LogDebug("portcheck", "failed to find Docker container using port",
			"port", p.Port,
			"error", err.Error())
		return info // Return original info if Docker search fails
	}

	if container == nil {
		logging.LogDebug("portcheck", "no Docker container found using port", "port", p.Port)
		return info
	}

	info.Container = container
	logging.LogInfo("portcheck", "found Docker container using port",
		"port", p.Port, "container_name", container.Name, "image", container.Image)

	if IsSptContainer(container) {
		info.IsSpt = true
		logging.LogInfo("portcheck", "identified Spt container",
			"port", p.Port, "container_name", container.Name, "image", container.Image)
		if info.Status == nil {
			info.Status = &SptStatus{RunID: "detected", State: constants.StateRunning, Message: fmt.Sprintf("Spt container detected: %s", container.Name)}
		}
	} else {
		logging.LogDebug("portcheck", "container is not Spt",
			"port", p.Port, "container_name", container.Name, "image", container.Image)
	}

	return info
}

// isSptBadRequestError checks if a 400 Bad Request error is from Spt
func (p *PortChecker) isSptBadRequestError(resp *http.Response) bool {
	// Read the response body to check for Spt-specific error messages
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		logging.LogDebug("portcheck", "failed to read 400 error response body", "error", err.Error())
		return false
	}

	bodyStr := string(body)

	// Check for Spt-specific error patterns
	sptErrorPatterns := []string{
		"Missing header: If-Match",  // Specific Spt error we saw
		"com.dell.spt.base.control", // Spt servlet package name
		"Powered by Jetty",          // Spt uses Jetty server
	}

	for _, pattern := range sptErrorPatterns {
		if strings.Contains(bodyStr, pattern) {
			logging.LogDebug("portcheck", "recognized Spt error pattern",
				"pattern", pattern,
				"response", bodyStr[:mathutil.MinInt(100, len(bodyStr))]) // Log first 100 chars
			return true
		}
	}

	return false
}

// min moved to internal/mathutil.MinInt
