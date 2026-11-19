/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
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
	// newFinder creates the Docker finder used for container detection;
	// nil means NewDockerContainerFinder. Tests supply a fake.
	newFinder func() (*DockerContainerFinder, error)
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
		info.APIConfirmed = true
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

// probeMetrics checks the Prometheus endpoint. No engine is identified here:
// an engine answers /metrics with Prometheus text, or an empty body before its
// first step, and neither passes validateSptMetricsResponse, which accepts only
// a /metrics/json schema row. The probe is kept for a service that serves
// that row on /metrics.
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
	// Only the 400 answer identifies an engine: RunServlet rejects the
	// probe's If-Match: * with "Invalid run ID format". It answers a matching
	// run ID with 200 and no body, which this probe never sends, so a 200 is
	// not SPT evidence.
	if resp.StatusCode == http.StatusBadRequest && p.isSptBadRequestError(resp) {
		return p.getSptStatus(ctx, baseURL)
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

	// An empty body ([], [ ] or null) is not SPT evidence: no engine since
	// v5.0.1 sends one on a probed path. /metrics/json always holds at least
	// an idle node row, and /metrics is Prometheus text or empty. Such a body
	// falls through to the object parse below: [] and [ ] fail it, and null
	// parses to a nil map with no SPT field.
	var metricsArray []map[string]interface{}
	if err := json.Unmarshal(body, &metricsArray); err == nil && len(metricsArray) > 0 {
		logging.LogDebug("portcheck", "validating first element of metrics array")
		return p.validateSptFields(metricsArray[0])
	}

	// Try to parse as object (traditional format)
	var metrics map[string]interface{}
	if err := json.Unmarshal(body, &metrics); err != nil {
		logging.LogDebug("portcheck", "response is not valid JSON array or object", "error", err.Error())
		return false
	}

	return p.validateSptFields(metrics)
}

// validateSptFields reports whether a JSON object is an engine metrics row.
// Every engine from v5.0.1 on writes the snake_case /metrics/json contract
// (see isSptMetricsSchemaObject). No engine has written camelCase fields such
// as successCount or opType, so they are not SPT evidence.
func (p *PortChecker) validateSptFields(metrics map[string]interface{}) bool {
	if isSptMetricsSchemaObject(metrics) {
		logging.LogDebug("portcheck", "validated Spt metrics schema object",
			"metrics_schema", metrics["metrics_schema"])
		return true
	}
	logging.LogDebug("portcheck", "metrics object is not an Spt metrics row")
	return false
}

// isSptMetricsSchemaObject reports whether a JSON object has the shape of the
// engine's /metrics/json contract (engine doc METRICS.md): an integral, positive
// metrics_schema version and the always-present string fields scope and
// op_type. Requiring the companion fields keeps an unrelated service that
// happens to emit a "metrics_schema" key from being taken for SPT.
func isSptMetricsSchemaObject(metrics map[string]interface{}) bool {
	version, ok := metrics["metrics_schema"].(float64)
	if !ok || version < 1 || version != math.Trunc(version) {
		return false
	}
	for _, field := range []string{"scope", "op_type"} {
		if _, isString := metrics[field].(string); !isString {
			return false
		}
	}
	return true
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
	if info.Status.RunActive {
		return true
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
	newFinder := p.newFinder
	if newFinder == nil {
		newFinder = NewDockerContainerFinder
	}
	dockerFinder, err := newFinder()
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
			info.Status = &SptStatus{Message: fmt.Sprintf("Spt container detected: %s", container.Name)}
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
		// RunServlet's error without an If-Match header. probeRun always sends
		// If-Match: *, so this cannot match from that probe; it stays because
		// the text is SPT-specific.
		"Missing header: If-Match",
		"Invalid run ID format",     // Spt RunServlet answer to the probe's If-Match: *
		"com.dell.spt.base.control", // Spt servlet package name
		// No generic Jetty pattern such as "Powered by Jetty": any Jetty
		// service answers a 400 with that page.
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
