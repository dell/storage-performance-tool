/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"mime/multipart"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

const apiLogPreviewLen = 160

// ErrMetricsIncompatible indicates that the metrics payload cannot be consumed by this client.
var ErrMetricsIncompatible = errors.New("spt metrics payload incompatible with this client")

// ErrRunOwnershipUnknown prevents destructive run operations when the client
// has not established ownership through a confirmed submission or bounded
// submission reconciliation.
var ErrRunOwnershipUnknown = errors.New("cannot stop test without a session-owned run ID")

// SptAPIClient handles communication with the Spt REST API
type SptAPIClient struct {
	baseURL                         string
	httpClient                      *http.Client
	runID                           string
	requireStatusAttribution        bool
	submissionReconcileTimeout      time.Duration
	submissionReconcilePollInterval time.Duration
	startRetryDelay                 time.Duration
	mu                              sync.Mutex
}

// StartTestOutcome preserves the submission boundary even when POST /run
// returns a transport error. Callers must not retry SubmissionUnknown.
type StartTestOutcome struct {
	RunID      string
	Submission SubmissionState
}

// AmbiguousSubmissionError means POST /run was dispatched but the response
// and bounded run-id reconciliation did not prove whether it was accepted.
type AmbiguousSubmissionError struct {
	ExpectedRunID int64
	Cause         error
}

func (e *AmbiguousSubmissionError) Error() string {
	return fmt.Sprintf("submission of run %d remains ambiguous after bounded /status reconciliation: %v",
		e.ExpectedRunID, e.Cause)
}

func (e *AmbiguousSubmissionError) Unwrap() error { return e.Cause }

// SubmissionIdentityError means POST /run was accepted, but its response did
// not establish the configured run identity. The submission remains cleanup
// owned even though the launch must not proceed.
type SubmissionIdentityError struct {
	ExpectedRunID int64
	Cause         error
}

func (e *SubmissionIdentityError) Error() string {
	return fmt.Sprintf("accepted submission did not establish configured run %d identity: %v",
		e.ExpectedRunID, e.Cause)
}

func (e *SubmissionIdentityError) Unwrap() error { return e.Cause }

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

// statusResponse mirrors StatusServlet's canonical wire contract. RunID is a
// numeric snake_case field; LegacyRunID is accepted only for older engines.
type statusResponse struct {
	State       string   `json:"state"`
	Progress    *float64 `json:"progress"`
	Message     string   `json:"message"`
	RunID       *int64   `json:"run_id"`
	LegacyRunID string   `json:"runId"`
}

// TestStatus represents the status of a running test
type TestStatus struct {
	State            string  // RUNNING, COMPLETED, FAILED, IDLE
	Progress         float64 // Progress percentage if available
	Message          string  // Status message or error
	RunID            string  // Current run ID
	runIDFromPayload bool
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
	CorruptCount    *int64  `json:"corrupt_count"`
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
	LatencyMeanUs  float64         `json:"latency_mean_us"`
	DurationMeanUs float64         `json:"duration_mean_us"`
	Latency        *JSONTimingStat `json:"latency,omitempty"`
	Duration       *JSONTimingStat `json:"duration,omitempty"`
	TTFB           *JSONTimingStat `json:"ttfb,omitempty"`
}

// JSONTimingStat contains schema 3 timing distribution fields.
type JSONTimingStat struct {
	Count         int64   `json:"count"`
	MeanUs        float64 `json:"mean_us"`
	MinUs         int64   `json:"min_us"`
	P50Us         int64   `json:"p50_us"`
	P90Us         int64   `json:"p90_us"`
	P99Us         int64   `json:"p99_us"`
	P999Us        int64   `json:"p999_us"`
	MaxUs         int64   `json:"max_us"`
	OverflowCount int64   `json:"overflow_count"`
}

// JSONMetricsConcurrency contains concurrency-related metrics
type JSONMetricsConcurrency struct {
	Current float64 `json:"current"`
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
		baseURL:                         baseURL,
		submissionReconcileTimeout:      constants.SubmissionReconciliationTimeout,
		submissionReconcilePollInterval: constants.APIReadinessPollInterval,
		startRetryDelay:                 250 * time.Millisecond,
		httpClient: &http.Client{
			Timeout: 10 * time.Second, // Default timeout for API calls
		},
	}
}

// WaitForAPIReady waits for the Spt API to become ready
func (c *SptAPIClient) WaitForAPIReady(timeout time.Duration) error {
	return c.WaitForAPIReadyContext(context.Background(), timeout)
}

// WaitForAPIReadyContext waits for readiness within both the caller lifecycle and timeout.
func (c *SptAPIClient) WaitForAPIReadyContext(ctx context.Context, timeout time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	logging.LogInfo("spt-api", "waiting for API to become ready", "timeout", timeout, "url", c.baseURL)

	readyCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	attempt := 0
	firstReadyLogged := false
	identityLogged := false

	for {
		if err := readyCtx.Err(); err != nil {
			return fmt.Errorf("spt API did not become ready after %v: %w", timeout, err)
		}
		attempt++
		ready, info, body, statusCode, err := c.probeReadyContext(readyCtx)
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
		timer := time.NewTimer(constants.APIReadinessPollInterval)
		select {
		case <-readyCtx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return fmt.Errorf("spt API did not become ready after %v: %w", timeout, readyCtx.Err())
		case <-timer.C:
		}
	}
}

// StartTest starts a new test run with the provided scenario and defaults.
// It is retained as a compatibility wrapper; production launchers use
// StartTestContext with their configured numeric run id.
func (c *SptAPIClient) StartTest(scenario, defaults []byte) (string, error) {
	outcome, err := c.StartTestContext(context.Background(), scenario, defaults, 0)
	return outcome.RunID, err
}

// StartTestContext posts an exact scenario/defaults pair within the caller's
// launch context. expectedRunID is optional for compatibility; production
// callers provide it so an ambiguous transport result can be reconciled using
// structured /status evidence. An unknown submission is never retried.
func (c *SptAPIClient) StartTestContext(
	ctx context.Context, scenario, defaults []byte, expectedRunIDs ...int64,
) (StartTestOutcome, error) {
	ctx = normalizeContext(ctx)
	expectedRunID := int64(0)
	if len(expectedRunIDs) > 0 {
		expectedRunID = expectedRunIDs[0]
	}
	logging.LogInfo("spt-api", "starting test", "scenario_size", len(scenario), "defaults_size", len(defaults))

	buildRequest := func() (*http.Request, error) {
		var body bytes.Buffer
		writer := multipart.NewWriter(&body)
		scenPart, err := writer.CreateFormFile("scenario", "scenario.js")
		if err != nil {
			return nil, fmt.Errorf("failed to create scenario part: %w", err)
		}
		if _, err := scenPart.Write(scenario); err != nil {
			return nil, fmt.Errorf("failed to write scenario: %w", err)
		}
		defPart, err := writer.CreateFormFile("defaults", "defaults.yaml")
		if err != nil {
			return nil, fmt.Errorf("failed to create defaults part: %w", err)
		}
		if _, err := defPart.Write(defaults); err != nil {
			return nil, fmt.Errorf("failed to write defaults: %w", err)
		}
		if err := writer.Close(); err != nil {
			return nil, fmt.Errorf("failed to close multipart writer: %w", err)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/run", &body)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		req.Header.Set("Content-Type", writer.FormDataContentType())
		return req, nil
	}

	const maxAttempts = 5
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err := ctx.Err(); err != nil {
			return StartTestOutcome{Submission: SubmissionNotSubmitted}, err
		}
		req, err := buildRequest()
		if err != nil {
			return StartTestOutcome{Submission: SubmissionNotSubmitted}, err
		}
		resp, err := c.httpClient.Do(req)
		if err != nil {
			cause := fmt.Errorf("failed to send POST /run: %w", err)
			return c.reconcileAmbiguousSubmission(ctx, expectedRunID, cause)
		}

		if resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated ||
			resp.StatusCode == http.StatusAccepted {
			bodyData, bodyErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			etag := resp.Header.Get("ETag")
			if expectedRunID > 0 {
				runID, identityPresent, identityErr := acceptedResponseRunID(
					etag, bodyData, expectedRunID)
				if bodyErr != nil {
					identityErr = errors.Join(identityErr,
						fmt.Errorf("read successful response identity: %w", bodyErr))
				}
				if identityErr != nil {
					c.clearRunID()
					return StartTestOutcome{Submission: SubmissionSubmitted},
						&SubmissionIdentityError{ExpectedRunID: expectedRunID, Cause: identityErr}
				}
				if !identityPresent {
					cause := errors.New("successful response omitted run identity")
					c.clearRunID()
					return c.reconcileAcceptedSubmissionIdentity(ctx, expectedRunID, cause)
				}
				c.setRunID(runID)
				logging.LogInfo("spt-api", "test started successfully", "runID", runID)
				return StartTestOutcome{RunID: runID, Submission: SubmissionSubmitted}, nil
			}
			runID := acceptedRunID(etag, bodyData, expectedRunID)
			c.setCompatibilityRunID(runID)
			logging.LogInfo("spt-api", "test started successfully", "runID", runID)
			return StartTestOutcome{RunID: runID, Submission: SubmissionSubmitted}, nil
		}

		if resp.StatusCode == http.StatusMethodNotAllowed && attempt < maxAttempts {
			bodyData, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			logging.LogDebug("spt-api", "received 405 from /run, retrying", "attempt", attempt, "body", string(bodyData))
			timer := time.NewTimer(c.startRetryDelay)
			select {
			case <-ctx.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				return StartTestOutcome{Submission: SubmissionNotSubmitted}, ctx.Err()
			case <-timer.C:
			}
			continue
		}

		bodyData, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		return StartTestOutcome{Submission: SubmissionNotSubmitted},
			fmt.Errorf("failed to start test (status %d): %s", resp.StatusCode, string(bodyData))
	}
	return StartTestOutcome{Submission: SubmissionNotSubmitted},
		fmt.Errorf("failed to start test: exceeded retry attempts")
}

func acceptedRunID(etag string, body []byte, expectedRunID int64) string {
	if runID := strings.Trim(etag, `"`); runID != "" {
		return runID
	}
	var payload struct {
		RunID string `json:"runId"`
	}
	if json.Unmarshal(body, &payload) == nil && strings.TrimSpace(payload.RunID) != "" {
		return strings.TrimSpace(payload.RunID)
	}
	if expectedRunID > 0 {
		return strconv.FormatInt(expectedRunID, 10)
	}
	return fmt.Sprintf("run-%d", time.Now().Unix())
}

type responseRunIdentity struct {
	source string
	value  int64
}

func acceptedResponseRunID(etag string, body []byte, expectedRunID int64) (string, bool, error) {
	evidence := make([]responseRunIdentity, 0, 3)
	if value, present, err := parseETagRunID(etag); err != nil {
		return "", true, err
	} else if present {
		evidence = append(evidence, responseRunIdentity{source: "ETag", value: value})
	}
	bodyEvidence, err := parseBodyRunIDs(body)
	if err != nil {
		return "", true, err
	}
	evidence = append(evidence, bodyEvidence...)
	if len(evidence) == 0 {
		return "", false, nil
	}
	for _, identity := range evidence {
		if identity.value != expectedRunID {
			return "", true, fmt.Errorf(
				"%s run ID %d does not match expected run ID %d",
				identity.source, identity.value, expectedRunID)
		}
	}
	return strconv.FormatInt(expectedRunID, 10), true, nil
}

func parseETagRunID(etag string) (int64, bool, error) {
	value := strings.TrimSpace(etag)
	if value == "" {
		return 0, false, nil
	}
	if strings.HasPrefix(value, `"`) || strings.HasSuffix(value, `"`) {
		if len(value) < 2 || !strings.HasPrefix(value, `"`) || !strings.HasSuffix(value, `"`) {
			return 0, true, fmt.Errorf("ETag run identity is malformed")
		}
		value = value[1 : len(value)-1]
	}
	runID, err := strconv.ParseInt(value, 10, 64)
	if err != nil || runID <= 0 {
		return 0, true, fmt.Errorf("ETag run identity is not a positive integer")
	}
	return runID, true, nil
}

func parseBodyRunIDs(body []byte) ([]responseRunIdentity, error) {
	if len(bytes.TrimSpace(body)) == 0 {
		return nil, nil
	}
	if !json.Valid(body) {
		return nil, errors.New("successful response body is malformed or incomplete JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	first, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("decode successful response body: %w", err)
	}
	start, ok := first.(json.Delim)
	if !ok || start != '{' {
		return nil, nil
	}

	evidence := make([]responseRunIdentity, 0, 2)
	seen := make(map[string]struct{}, 2)
	for decoder.More() {
		keyToken, tokenErr := decoder.Token()
		if tokenErr != nil {
			return nil, fmt.Errorf("decode successful response field: %w", tokenErr)
		}
		name, ok := keyToken.(string)
		if !ok {
			return nil, errors.New("successful response object contained a non-string field name")
		}
		var raw json.RawMessage
		if decodeErr := decoder.Decode(&raw); decodeErr != nil {
			return nil, fmt.Errorf("decode successful response field %q: %w", name, decodeErr)
		}

		source := ""
		switch name {
		case "run_id":
			source = "response run_id"
		case "runId":
			source = "response runId"
		default:
			continue
		}
		if _, duplicate := seen[name]; duplicate {
			return nil, fmt.Errorf("%s appears more than once", source)
		}
		seen[name] = struct{}{}
		runID, parseErr := parseJSONRunID(raw)
		if parseErr != nil {
			return nil, fmt.Errorf("%s is malformed: %w", source, parseErr)
		}
		evidence = append(evidence, responseRunIdentity{source: source, value: runID})
	}
	end, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("close successful response object: %w", err)
	}
	if end != json.Delim('}') {
		return nil, errors.New("successful response object has an invalid closing delimiter")
	}
	var trailing json.RawMessage
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("successful response body contains a trailing JSON value")
		}
		return nil, fmt.Errorf("decode trailing successful response content: %w", err)
	}
	return evidence, nil
}

func parseJSONRunID(raw json.RawMessage) (int64, error) {
	var number json.Number
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err == nil {
		runID, parseErr := strconv.ParseInt(number.String(), 10, 64)
		if parseErr == nil && runID > 0 {
			return runID, nil
		}
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		runID, parseErr := strconv.ParseInt(strings.TrimSpace(text), 10, 64)
		if parseErr == nil && runID > 0 {
			return runID, nil
		}
	}
	return 0, errors.New("run identity is not a positive integer")
}

func (c *SptAPIClient) reconcileAmbiguousSubmission(
	launchCtx context.Context, expectedRunID int64, cause error,
) (StartTestOutcome, error) {
	outcome := StartTestOutcome{Submission: SubmissionUnknown}
	if expectedRunID <= 0 {
		return outcome, &AmbiguousSubmissionError{ExpectedRunID: expectedRunID, Cause: cause}
	}
	reconcileBase := context.WithoutCancel(normalizeContext(launchCtx))
	reconcileCtx, cancel := context.WithTimeout(reconcileBase, c.submissionReconcileTimeout)
	defer cancel()
	ticker := time.NewTicker(c.submissionReconcilePollInterval)
	defer ticker.Stop()
	for {
		status, err := c.observeStatusContext(reconcileCtx)
		if err == nil && statusMatchesSubmission(status, expectedRunID) {
			outcome.RunID = strconv.FormatInt(expectedRunID, 10)
			outcome.Submission = SubmissionSubmitted
			c.setRunID(outcome.RunID)
			return outcome, cause
		}
		select {
		case <-reconcileCtx.Done():
			return outcome, &AmbiguousSubmissionError{ExpectedRunID: expectedRunID, Cause: cause}
		case <-ticker.C:
		}
	}
}

func (c *SptAPIClient) reconcileAcceptedSubmissionIdentity(
	launchCtx context.Context, expectedRunID int64, cause error,
) (StartTestOutcome, error) {
	outcome := StartTestOutcome{Submission: SubmissionSubmitted}
	reconcileBase := context.WithoutCancel(normalizeContext(launchCtx))
	reconcileCtx, cancel := context.WithTimeout(reconcileBase, c.submissionReconcileTimeout)
	defer cancel()
	ticker := time.NewTicker(c.submissionReconcilePollInterval)
	defer ticker.Stop()
	for {
		status, err := c.observeStatusContext(reconcileCtx)
		if err == nil && statusMatchesSubmission(status, expectedRunID) {
			outcome.RunID = strconv.FormatInt(expectedRunID, 10)
			c.setRunID(outcome.RunID)
			return outcome, nil
		}
		select {
		case <-reconcileCtx.Done():
			return outcome, &SubmissionIdentityError{
				ExpectedRunID: expectedRunID,
				Cause: errors.Join(cause, fmt.Errorf(
					"bounded /status reconciliation: %w", reconcileCtx.Err())),
			}
		case <-ticker.C:
		}
	}
}

func statusMatchesSubmission(status *TestStatus, expectedRunID int64) bool {
	if status == nil {
		return false
	}
	if !status.runIDFromPayload {
		return false
	}
	runID, err := strconv.ParseInt(strings.TrimSpace(status.RunID), 10, 64)
	if err != nil || runID != expectedRunID {
		return false
	}
	switch strings.ToUpper(strings.TrimSpace(status.State)) {
	case constants.StateStarting, constants.StateInitializing, constants.StateRunning,
		constants.StateCompleted, constants.StateFailed, constants.StateStopped:
		return true
	default:
		return false
	}
}

func (c *SptAPIClient) setRunID(runID string) {
	c.mu.Lock()
	c.runID = runID
	c.requireStatusAttribution = true
	c.mu.Unlock()
}

func (c *SptAPIClient) setCompatibilityRunID(runID string) {
	c.mu.Lock()
	c.runID = runID
	c.requireStatusAttribution = false
	c.mu.Unlock()
}

func (c *SptAPIClient) clearRunID() {
	c.mu.Lock()
	c.runID = ""
	c.requireStatusAttribution = false
	c.mu.Unlock()
}

func (c *SptAPIClient) statusMatchesOwnedRun(status *TestStatus) bool {
	if status == nil {
		return false
	}
	c.mu.Lock()
	ownedRunID := c.runID
	requireAttribution := c.requireStatusAttribution
	c.mu.Unlock()
	if !status.runIDFromPayload {
		return !requireAttribution
	}
	return ownedRunID != "" && strings.TrimSpace(status.RunID) == ownedRunID
}

// GetStatus retrieves the current test status
func (c *SptAPIClient) GetStatus() (*TestStatus, error) {
	return c.GetStatusContext(context.Background())
}

// GetStatusContext retrieves observed status within the caller's cancellation
// budget. Observing another run never changes this client's owned run ID.
func (c *SptAPIClient) GetStatusContext(ctx context.Context) (*TestStatus, error) {
	return c.getStatusContext(ctx)
}

func (c *SptAPIClient) observeStatusContext(ctx context.Context) (*TestStatus, error) {
	return c.getStatusContext(ctx)
}

func (c *SptAPIClient) getStatusContext(ctx context.Context) (*TestStatus, error) {
	// Try the /status endpoint first
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/status", nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create status request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
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

	var statusData statusResponse
	if err := json.Unmarshal(bodyData, &statusData); err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	status := &TestStatus{}

	// Extract fields from JSON
	if statusData.State != "" {
		status.State = statusData.State
	} else {
		status.State = constants.UIStateUnknown
	}

	status.Message = statusData.Message

	if statusData.RunID != nil {
		status.RunID = strconv.FormatInt(*statusData.RunID, 10)
		status.runIDFromPayload = true
	} else if legacyRunID := strings.TrimSpace(statusData.LegacyRunID); legacyRunID != "" {
		status.RunID = legacyRunID
		status.runIDFromPayload = true
	}
	if statusData.Progress != nil {
		status.Progress = *statusData.Progress
	}

	return status, nil
}

// getTestDetails removed as unused (dead code)

// StopTest stops the currently running test gracefully
func (c *SptAPIClient) StopTest() error {
	runID := c.getRunID()
	if runID == "" {
		return ErrRunOwnershipUnknown
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

	// Clear the stored run identity.
	c.clearRunID()

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

// ParseJSONMetrics parses JSON format metrics into a slice of PerformanceMetric.
// For non-mixed workloads the slice contains a single element. For mixed workloads
// the engine emits one entry per op-type (e.g. CREATE, READ, DELETE) sharing the
// same step_id; all valid entries are returned. The slice is sorted by timestamp
// descending so callers that only need a single metric can use [0].
func (c *SptAPIClient) ParseJSONMetrics(data string) ([]*PerformanceMetric, error) {
	var steps []JSONMetricsStep

	if err := json.Unmarshal([]byte(data), &steps); err != nil {
		return nil, fmt.Errorf("failed to parse JSON metrics: %w", err)
	}

	if len(steps) == 0 {
		return nil, fmt.Errorf("no metrics steps found in JSON response: %w", ErrMetricsIncompatible)
	}

	type validated struct {
		metric          *PerformanceMetric
		timestamp       int64
		sampleTimestamp time.Time
	}

	var (
		results []validated
		lastErr error
	)

	for i := range steps {
		step := &steps[i]

		if step.MetricsSchema == 0 {
			lastErr = fmt.Errorf("%w: metrics_schema missing", ErrMetricsIncompatible)
			continue
		}
		if step.MetricsSchema < 2 {
			lastErr = fmt.Errorf("%w: metrics_schema=%d below minimum", ErrMetricsIncompatible, step.MetricsSchema)
			continue
		}

		scope := strings.ToLower(step.Scope)
		if scope != "node" {
			continue
		}
		if strings.TrimSpace(step.Role) == "" {
			lastErr = fmt.Errorf("%w: metrics role missing", ErrMetricsIncompatible)
			continue
		}
		if strings.TrimSpace(step.SampleTimestampRaw) == "" {
			lastErr = fmt.Errorf("%w: sample_ts missing", ErrMetricsIncompatible)
			continue
		}

		sampleTimestamp, err := parseSampleTimestamp(step.SampleTimestampRaw)
		if err != nil {
			lastErr = fmt.Errorf("%w: invalid sample_ts value: %s", ErrMetricsIncompatible, err.Error())
			continue
		}

		metric := stepToMetric(step, scope, sampleTimestamp)
		results = append(results, validated{
			metric:          metric,
			timestamp:       step.Timestamp,
			sampleTimestamp: sampleTimestamp,
		})
	}

	if len(results) == 0 {
		if lastErr != nil {
			return nil, lastErr
		}
		return nil, fmt.Errorf("%w: no compatible node metrics", ErrMetricsIncompatible)
	}

	// Sort by timestamp descending, break ties by sample timestamp descending.
	sort.Slice(results, func(i, j int) bool {
		if results[i].timestamp != results[j].timestamp {
			return results[i].timestamp > results[j].timestamp
		}
		return results[i].sampleTimestamp.After(results[j].sampleTimestamp)
	})

	out := make([]*PerformanceMetric, len(results))
	for i, v := range results {
		out[i] = v.metric
	}
	return out, nil
}

// stepToMetric converts a validated JSONMetricsStep into a PerformanceMetric.
func stepToMetric(step *JSONMetricsStep, scope string, sampleTimestamp time.Time) *PerformanceMetric {
	latencyUs := displayTimingUs(step.MetricsSchema, step.Timing.LatencyMeanUs, step.Timing.Latency)
	durationUs := displayTimingUs(step.MetricsSchema, step.Timing.DurationMeanUs, step.Timing.Duration)
	hasTTFB := step.Timing.TTFB != nil && step.Timing.TTFB.Count > 0
	ttfbUs := float64(0)
	if hasTTFB {
		ttfbUs = displayStatTimingUs(step.Timing.TTFB)
	}
	corruptCount := int64(0)
	hasCorruptCount := step.Operations.CorruptCount != nil
	if hasCorruptCount {
		corruptCount = *step.Operations.CorruptCount
	}
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
		CorruptCount:             corruptCount,
		HasCorruptCount:          hasCorruptCount,
		StepTime:                 step.ElapsedTimeSeconds,
		OpsPerSec:                int64(math.Round(step.Operations.SuccessRateLast)),
		MiBPerSec:                int64(step.Bandwidth.BytesRateLast / float64(constants.BytesPerMiB)),
		MeanLatency:              int64(math.Round(latencyUs)), // Schema 3 prefers p50.
		MeanDuration:             int64(math.Round(durationUs)),
		MeanTTFB:                 int64(math.Round(ttfbUs)),
		HasTTFB:                  hasTTFB,
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

	return metric
}

func displayTimingUs(schema int, legacyMeanUs float64, stat *JSONTimingStat) float64 {
	if schema >= 3 && stat != nil && stat.P50Us > 0 {
		return float64(stat.P50Us)
	}
	return legacyMeanUs
}

func displayStatTimingUs(stat *JSONTimingStat) float64 {
	if stat == nil {
		return 0
	}
	if stat.P50Us > 0 {
		return float64(stat.P50Us)
	}
	if stat.MeanUs > 0 {
		return stat.MeanUs
	}
	return 0
}

func parseSampleTimestamp(raw string) (time.Time, error) {
	if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return ts, nil
	}
	return time.Parse(time.RFC3339, raw)
}

func (c *SptAPIClient) probeReadyContext(ctx context.Context) (bool, readinessResponse, []byte, int, error) {
	var info readinessResponse
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/ready", nil)
	if err != nil {
		return false, info, nil, 0, err
	}
	resp, err := c.httpClient.Do(req)
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
	c.LogReadySnapshotContext(context.Background(), label)
}

// LogReadySnapshotContext emits a readiness snapshot within the caller's cancellation budget.
func (c *SptAPIClient) LogReadySnapshotContext(ctx context.Context, label string) {
	if c == nil {
		return
	}
	ready, info, _, statusCode, err := c.probeReadyContext(ctx)
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
	return c.ShutdownContext(context.Background())
}

// ShutdownContext requests graceful shutdown within the caller's cancellation budget.
func (c *SptAPIClient) ShutdownContext(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/shutdown", nil)
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
	return c.WaitForLingerContext(context.Background(), linger)
}

// WaitForLingerContext observes terminal status within the caller's cancellation budget.
func (c *SptAPIClient) WaitForLingerContext(ctx context.Context, linger time.Duration) error {
	if linger <= 0 {
		return nil
	}
	deadline := time.Now().Add(linger)
	sawTerminal := false
	for time.Now().Before(deadline) {
		if err := ctx.Err(); err != nil {
			return err
		}
		st, err := c.GetStatusContext(ctx)
		if err != nil {
			return fmt.Errorf("status probe during linger: %w", err)
		}
		switch st.State {
		case constants.StateIdle:
			// IDLE (including /status 404) is node-level evidence that shutdown
			// has removed the active run and intentionally needs no run ID.
			sawTerminal = true
		case constants.StateCompleted, constants.StateFailed, constants.StateStopped:
			if !c.statusMatchesOwnedRun(st) {
				return fmt.Errorf(
					"terminal status for observed run %q does not match the owned run",
					st.RunID)
			}
			sawTerminal = true
		default:
			return fmt.Errorf("non-terminal state during linger: %s", st.State)
		}
		timer := time.NewTimer(constants.APILingerPollInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return ctx.Err()
		case <-timer.C:
		}
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
	c.setRunID(runID)
}
