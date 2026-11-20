/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"strconv"
	"strings"
	"sync/atomic"
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
			Timeout:       timeout,
			Transport:     redirectsAsAnswers(nil),
			CheckRedirect: noRedirects,
		},
		Clock: &RealClock{},
	}
}

// noRedirects is the redirect policy of every HTTP client of the port checker:
// an SPT engine does not redirect, and a request that a check guards must reach
// the address checked, so the redirect is the answer.
func noRedirects(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

// redirectAnswers is the transport of every HTTP client of the port checker,
// with noRedirects. net/http parses a 3xx answer's Location before it asks
// CheckRedirect, and a Location it cannot parse becomes an error with no
// answer. So this drops the Location of every 3xx answer the transport
// receives: the client then returns the answer itself, and its caller reads
// the 3xx status, whatever the Location said.
type redirectAnswers struct{ base http.RoundTripper }

// redirectsAsAnswers wraps base, or http.DefaultTransport when it is nil.
func redirectsAsAnswers(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return &redirectAnswers{base: base}
}

func (t *redirectAnswers) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err == nil && isRedirect(resp.StatusCode) {
		resp.Header.Del("Location")
	}
	return resp, err
}

// CloseIdleConnections closes the idle connections of the transport wrapped.
func (t *redirectAnswers) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
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

	// /status answers without a run ID; GET /run would need one in If-Match.
	detail, err := c.getStatusDetail(ctx)
	statusKnown := err == nil
	if statusKnown {
		status.State = detail.State
		if detail.RunID != 0 {
			status.RunID = strconv.FormatInt(detail.RunID, 10)
		}
	} else {
		logging.LogDebug("portcheck", "failed to get status from Spt API",
			"baseURL", c.BaseURL,
			"error", err.Error())
		// Continue - we might still get some info from metrics
	}

	// /status describes only runs this engine started, or its last one: a worker
	// of a distributed run reports IDLE. HEAD /run names the run active now.
	runID, active, err := c.activeRunID(ctx)
	if err != nil {
		logging.LogDebug("portcheck", "failed to get the active run from Spt API",
			"baseURL", c.BaseURL,
			"error", err.Error())
	} else if active {
		status.RunActive = true
		if statusKnown {
			execution := executionFromStatus(runID, detail)
			status.RunningSince, status.Worker = execution.Since, execution.Worker
		}
		if activeID := strconv.FormatInt(runID, 10); status.RunID != activeID {
			status.RunID = activeID
			if statusKnown {
				status.State = constants.StateRunning
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

// activeRunID reads the active run's ID from HEAD /run, which needs no If-Match:
// 200 carries the ID in ETag and 204 means no run is active.
func (c *SptAPIClient) activeRunID(ctx context.Context) (runID int64, active bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.BaseURL+endpointRun, nil)
	if err != nil {
		return 0, false, fmt.Errorf("create run request: %w", err)
	}
	var conn atomic.Pointer[countingConn]
	var otherConn atomic.Bool
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if counting, ok := info.Conn.(*countingConn); ok {
				conn.Store(counting)
			} else {
				otherConn.Store(true)
			}
		},
	}))
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		err = fmt.Errorf("run request: %w", err)
		// Only a connection that delivered no byte got no answer. A connection
		// that was not counted may have delivered some, so it counts as answered.
		if counting := conn.Load(); !otherConn.Load() && (counting == nil || counting.bytesRead() == 0) {
			return 0, false, &noAnswerError{err: err}
		}
		return 0, false, &unreadableAnswerError{err: err}
	}
	defer func() { _ = resp.Body.Close() }()
	if isRedirect(resp.StatusCode) {
		return 0, false, redirectRefusal("the read of its active run (HEAD /run)", resp.StatusCode)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		etag := strings.TrimSpace(resp.Header.Get("ETag"))
		runID, err = strconv.ParseInt(etag, 10, 64)
		if err != nil || runID == 0 {
			return 0, false, &unreadableAnswerError{err: fmt.Errorf("active run has an invalid ETag %q", etag), http: true}
		}
		return runID, true, nil
	case http.StatusNoContent:
		return 0, false, nil
	default:
		return 0, false, &unreadableAnswerError{err: fmt.Errorf("run status %d", resp.StatusCode), http: true}
	}
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
	// Since is when the state began; for RUNNING, when the run started.
	Since string `json:"since"`
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
	if isRedirect(resp.StatusCode) {
		return terminalStatus{}, redirectRefusal("the read of its status (GET /status)", resp.StatusCode)
	}
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
	if isRedirect(resp.StatusCode) {
		return redirectRefusal("the shutdown request", resp.StatusCode)
	}
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

// RunExecution is one execution of a run, as far as the engine tells executions
// apart. The engine takes a caller's run ID and lets a later run reuse it, so the
// ID alone does not identify an execution. A zero ID means no run.
type RunExecution struct {
	ID int64
	// Since is the run's start time: the since of a RUNNING /status that names
	// this run. Only the engine that started the run reports it; it is empty when
	// unknown. The shipped engine writes it as canonical UTC from milliseconds, so
	// two reads of one execution give the same string, and they are compared as
	// strings.
	Since string
	// Worker marks an engine whose /status names no run or another one. That is a
	// worker of the run, or an entry engine whose status a refused POST /run
	// overwrote; the CLI cannot tell the two apart.
	Worker bool
	// Terminal marks a /status that names this run in a terminal state: the run
	// ended and its cleanup still runs. Its since is then the end, not the start.
	Terminal bool
	// Known is set when /status was read and parsed; without it, nothing above
	// but ID is known, and statusErr says why.
	Known     bool
	statusErr error
	// statusRunID is the run /status named.
	statusRunID int64
}

// RunIdentityError is a refusal because the active run cannot be shown to be the
// run expected: the one shown, or the one just stopped. The run is left alone,
// and so is anything that would end it, such as its container.
type RunIdentityError struct {
	reason string
	// redirect marks a refusal because a request was answered with a redirect.
	redirect bool
}

func (e *RunIdentityError) Error() string { return e.reason }

func identityRefusal(format string, args ...any) error {
	return &RunIdentityError{reason: fmt.Sprintf(format, args...)}
}

// IsRunIdentityRefusal reports whether err is, or wraps, a RunIdentityError.
func IsRunIdentityRefusal(err error) bool {
	var identity *RunIdentityError
	return errors.As(err, &identity)
}

func executionFromStatus(runID int64, status terminalStatus) RunExecution {
	execution := RunExecution{ID: runID, Known: true, statusRunID: status.RunID}
	switch {
	case status.RunID != runID:
		execution.Worker = true
	case status.State == constants.StateRunning:
		execution.Since = status.Since
	case isTerminalState(status.State):
		execution.Terminal = true
	}
	return execution
}

func isTerminalState(state string) bool {
	return state == constants.StateCompleted || state == constants.StateStopped || state == constants.StateFailed
}

// execution describes the active run runID from /status. A failed read leaves it
// with only its ID, not Known.
func (c *SptAPIClient) execution(ctx context.Context, runID int64) RunExecution {
	status, err := c.getStatusDetail(ctx)
	if err != nil {
		logging.LogDebug("portcheck", "no status for the active run", "run_id", runID, "error", err.Error())
		return RunExecution{ID: runID, statusErr: err}
	}
	return executionFromStatus(runID, status)
}

// redirected returns the refusal when /status was answered with a redirect,
// which is an answer that is not the engine's, not a status that could not be
// read, or nil.
func (e RunExecution) redirected() error {
	if isRedirectRefusal(e.statusErr) {
		return e.statusErr
	}
	return nil
}

// noAnswerError is a request that got no answer: the connection was refused,
// or it broke, ended or timed out before it delivered a byte.
type noAnswerError struct{ err error }

func (e *noAnswerError) Error() string { return e.err.Error() }

func (e *noAnswerError) Unwrap() error { return e.err }

// unreadableAnswerError is an answer to HEAD /run that names no active run and
// does not say that none is: an HTTP status other than 200, 204 and a redirect
// (see redirectRefusal), an ETag that is not a run ID, or bytes that do not
// make an HTTP answer, such as a status line and then a closed connection. http
// marks an HTTP answer.
type unreadableAnswerError struct {
	err  error
	http bool
}

func (e *unreadableAnswerError) Error() string { return e.err.Error() }

func (e *unreadableAnswerError) Unwrap() error { return e.err }

// countingConn is a connection that counts the bytes it delivers, so that a
// request that failed can tell no answer from part of one.
type countingConn struct {
	net.Conn
	read atomic.Int64
}

func (c *countingConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	c.read.Add(int64(n))
	return n, err
}

func (c *countingConn) bytesRead() int64 { return c.read.Load() }

// countingDial wraps dial so that each connection it opens counts its bytes.
func countingDial(dial func(ctx context.Context, network, address string) (net.Conn, error)) func(ctx context.Context, network, address string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		conn, err := dial(ctx, network, address)
		if err != nil {
			return nil, err
		}
		return &countingConn{Conn: conn}, nil
	}
}

// unconfirmedReason says why now cannot be shown to be the execution expected,
// which has the same ID, or returns "" when it is. Without a start time for
// expected, the ID is all there is to compare, but a /status that cannot be read
// still leaves the run unknown. With one, only a RUNNING /status of this run with
// that start time confirms it; allowEnded also accepts the run in a terminal
// state, as a run just stopped shows during its cleanup.
func unconfirmedReason(expected, now RunExecution, allowEnded bool) string {
	switch {
	case !now.Known:
		return fmt.Sprintf("the engine's /status could not be read (%v)", now.statusErr)
	case expected.Since == "":
		if expected.Worker && now.Since != "" {
			return fmt.Sprintf("run %d now has a start time on this engine and had none before, so it is a later run that reuses the ID", now.ID)
		}
		return ""
	case now.Worker:
		return fmt.Sprintf("the engine's /status names run %d, not run %d", now.statusRunID, now.ID)
	case now.Terminal && allowEnded:
		return ""
	case now.Terminal:
		return fmt.Sprintf("run %d has ended and its cleanup is still running; try again shortly", now.ID)
	case now.Since == "":
		return fmt.Sprintf("the engine's /status gives no start time for run %d", now.ID)
	case now.Since != expected.Since:
		return fmt.Sprintf("run %d is a later run that reuses the ID", now.ID)
	}
	return ""
}

// StopActiveRun stops the active run for the force choice, --force or f, which
// acts only on the run detection showed active, shown (a zero ID for none). A
// run with another ID is left alone and returned as a RunIdentityError, and so
// is one whose /status shows another execution: see unconfirmedReason. Unlike
// StopShownRun, a /status that cannot be read does not refuse when detection
// itself had no start time for the run, as for a worker: the ID is all there
// was to compare, and the run keeps detection's worker mark, so a worker's stop
// is not reported as the run's end. When detection had a start time, a /status
// that cannot be read now refuses. A /status that reads the run as ended keeps
// detection's start time, so the check before the shutdown still tells a later
// run with the same ID apart. It returns the stopped run, or a zero ID when no
// run is active.
func (c *SptAPIClient) StopActiveRun(ctx context.Context, shown RunExecution) (RunExecution, error) {
	runID, active, err := c.activeRunID(ctx)
	if err != nil {
		return RunExecution{}, err
	}
	if !active {
		return RunExecution{}, nil
	}
	if err := otherRunRefusal(runID, shown.ID); err != nil {
		return RunExecution{}, err
	}
	now := c.execution(ctx, runID)
	if err := now.redirected(); err != nil {
		return RunExecution{}, err
	}
	if !now.Known && shown.Since == "" {
		now.Worker = shown.Worker
		return c.stopReportedRun(ctx, now)
	}
	if reason := unconfirmedReason(shown, now, true); reason != "" {
		return RunExecution{}, identityRefusal("run %d cannot be shown to be the run shown: %s; it was not stopped. "+
			"Retry, or stop the run yourself", runID, reason)
	}
	if now.Terminal {
		now.Since = shown.Since
	}
	return c.stopReportedRun(ctx, now)
}

// otherRunRefusal refuses to stop active run runID when it is not shownID, the
// run shown (zero for none).
func otherRunRefusal(runID, shownID int64) error {
	switch {
	case runID == shownID:
		return nil
	case shownID == 0:
		return identityRefusal("run %d is active, and detection showed no active run on the engine, or could "+
			"not read it; it was not stopped", runID)
	default:
		return identityRefusal("run %d replaced the run shown, %d; it was not stopped", runID, shownID)
	}
}

// StopShownRun stops the active run only if it is shown, the run the operator was
// shown (a zero ID for none). Another run is left alone and returned as an error,
// and so is a run that cannot be shown to be the same execution: see
// unconfirmedReason. It returns the stopped run, or a zero ID when no run is active.
func (c *SptAPIClient) StopShownRun(ctx context.Context, shown RunExecution) (RunExecution, error) {
	runID, active, err := c.activeRunID(ctx)
	if err != nil {
		return RunExecution{}, err
	}
	if !active {
		return RunExecution{}, nil
	}
	if err := otherRunRefusal(runID, shown.ID); err != nil {
		return RunExecution{}, err
	}
	now := c.execution(ctx, runID)
	if err := now.redirected(); err != nil {
		return RunExecution{}, err
	}
	if reason := unconfirmedReason(shown, now, false); reason != "" {
		return RunExecution{}, identityRefusal("run %d cannot be shown to be the run shown: %s; it was not stopped. "+
			"Retry, or stop the run yourself", runID, reason)
	}
	return c.stopReportedRun(ctx, now)
}

// ConfirmNoOtherRun returns an error unless the only run active is stopped, or
// none is. A stopped run may stay active until its cleanup returns, so it is
// allowed, in a terminal state. A run with another ID, a later run with the same
// ID, a run that cannot be shown to be stopped (see unconfirmedReason), a run
// stopped with nothing but its ID known, and an active run that cannot be read
// are errors, all of them a RunIdentityError. ended reports that the stopped run
// is no longer active, or that /status shows it in a terminal state. The
// engine's shutdown takes no run ID: a run that starts after this check is not
// seen, nor is a later run that reuses the ID on an engine without start times.
func (c *SptAPIClient) ConfirmNoOtherRun(ctx context.Context, stopped RunExecution) (ended bool, err error) {
	runID, active, err := c.activeRunID(ctx)
	if isRedirectRefusal(err) {
		return false, err
	}
	if err != nil {
		return false, identityRefusal("the engine's active run could not be read again (%v), so another run may be active", err)
	}
	if !active {
		return true, nil
	}
	if runID != stopped.ID {
		return false, identityRefusal("run %d is active on the engine", runID)
	}
	if !stopped.Known && stopped.Since == "" {
		return false, identityRefusal("run %d is still active, its /status could not be read when it was stopped, and "+
			"no start time was shown for it, so it cannot be shown to be the run stopped", runID)
	}
	now := c.execution(ctx, runID)
	if err := now.redirected(); err != nil {
		return false, err
	}
	if reason := unconfirmedReason(stopped, now, true); reason != "" {
		return false, identityRefusal("run %d is still active and cannot be shown to be the run stopped: %s", runID, reason)
	}
	return now.Known && now.Terminal, nil
}

// RemovalCheck is what the check before a Docker write found on the engine's API.
type RemovalCheck int

const (
	// RemovalNoAnswer means that no HTTP answer came back, so no SPT API answers there.
	RemovalNoAnswer RemovalCheck = iota + 1
	// RemovalIdle means that the engine reports no active run.
	RemovalIdle
	// RemovalIdentified means that the active run is the run identified.
	RemovalIdentified
)

// CheckBeforeRemoval is the check made right before a container is stopped or
// removed: one HEAD /run, and /status when a run is active. identified is the
// run detection showed active (a zero ID for none). No answer at all (not a
// byte), an idle engine (204) and the identified run are returned as such. The
// identified run has its ID and, where detection had one, its start time, which
// must be read again. Another run, a later run with the ID, and a redirect of
// either read are a RunIdentityError; any other answer, an HTTP status other
// than 200 and 204, or bytes that do not make an HTTP answer, is an
// *unreadableAnswerError. A run that starts after this check is not seen.
func (c *SptAPIClient) CheckBeforeRemoval(ctx context.Context, identified RunExecution) (RemovalCheck, error) {
	runID, active, err := c.activeRunID(ctx)
	var none *noAnswerError
	switch {
	case errors.As(err, &none):
		return RemovalNoAnswer, nil
	case isRedirectRefusal(err):
		return 0, err
	case err != nil:
		var unreadable *unreadableAnswerError
		if errors.As(err, &unreadable) {
			return 0, unreadable
		}
		return 0, &unreadableAnswerError{err: err}
	case !active:
		return RemovalIdle, nil
	case identified.ID == 0:
		return 0, identityRefusal("run %d is active on the engine, and detection showed no active run on it, or "+
			"could not read it", runID)
	case runID != identified.ID:
		return 0, identityRefusal("run %d is active on the engine, not run %d, which was shown", runID, identified.ID)
	}
	now := c.execution(ctx, runID)
	if err := now.redirected(); err != nil {
		return 0, err
	}
	if !now.Known && identified.Since == "" {
		return RemovalIdentified, nil
	}
	if reason := unconfirmedReason(identified, now, false); reason != "" {
		return 0, identityRefusal("run %d is active and cannot be shown to be the run shown: %s", runID, reason)
	}
	return RemovalIdentified, nil
}

// stopReportedRun stops the run the engine just reported as active.
func (c *SptAPIClient) stopReportedRun(ctx context.Context, execution RunExecution) (RunExecution, error) {
	runID := execution.ID
	logging.LogInfo("portcheck", "stopping the active run", "url", c.BaseURL, "run_id", runID)
	stopped, err := c.StopRun(ctx, runID)
	if err != nil {
		return RunExecution{}, err
	}
	if stopped {
		return execution, nil
	}
	// The engine found no matching run: it ended, or another one took over.
	currentID, active, err := c.activeRunID(ctx)
	if isRedirectRefusal(err) {
		return RunExecution{}, err
	}
	if err != nil {
		return RunExecution{}, identityRefusal("the engine found no run %d to stop, and its active run could not be read again (%v)", runID, err)
	}
	if !active {
		return RunExecution{}, nil
	}
	if currentID == runID {
		// The engine answers a stop of its active run with 200, so a run with
		// this ID active after a miss is a later run that reuses the ID.
		return RunExecution{}, identityRefusal("the engine found no run %[1]d to stop, and then reported run %[1]d "+
			"active, so it is a later run that reuses the ID; it was not stopped", runID)
	}
	return RunExecution{}, identityRefusal("run %d was replaced by run %d, which was not stopped", runID, currentID)
}

// StopRun asks the engine to stop the run with runID through DELETE /run,
// leaving the engine up. The engine matches the If-Match header against its
// active run: 200 means the stop was requested, 204 that no run is active and
// 404 that the active run is another one; neither of the last two stops
// anything, and both return stopped false with a nil error, so the caller's
// terminal reconciliation decides how the run ended.
func (c *SptAPIClient) StopRun(ctx context.Context, runID int64) (stopped bool, err error) {
	if runID == 0 {
		return false, fmt.Errorf("stop run: run ID is unknown")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.BaseURL+"/run", nil)
	if err != nil {
		return false, fmt.Errorf("create stop request: %w", err)
	}
	req.Header.Set("If-Match", strconv.FormatInt(runID, 10))
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return false, fmt.Errorf("stop request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if isRedirect(resp.StatusCode) {
		return false, redirectRefusal("the stop of run "+strconv.FormatInt(runID, 10), resp.StatusCode)
	}
	switch resp.StatusCode {
	case http.StatusOK:
		return true, nil
	case http.StatusNoContent, http.StatusNotFound:
		logging.LogInfo("portcheck", "stop request found no matching active run",
			"run_id", runID, "status", resp.StatusCode)
		return false, nil
	default:
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		if len(b) > 0 {
			return false, fmt.Errorf("stop status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
		}
		return false, fmt.Errorf("stop status %d", resp.StatusCode)
	}
}

func isRedirect(code int) bool { return code >= 300 && code < 400 }

// redirectRefusal is the RunIdentityError for a request, a write or a read that
// guards one, that was answered with a redirect, which an SPT engine does not
// send: the answer is not the engine's, so the redirect is not followed and
// nothing more is sent or removed. It names the status only: a Location can
// carry credentials.
func redirectRefusal(request string, status int) error {
	return &RunIdentityError{redirect: true, reason: fmt.Sprintf("the service on the port answered %s with a "+
		"redirect (status %d), which an SPT engine does not send, so the answer is not taken as an SPT "+
		"engine's; the redirect was not followed", request, status)}
}

// isRedirectRefusal reports whether err is, or wraps, a redirectRefusal.
func isRedirectRefusal(err error) bool {
	var identity *RunIdentityError
	return errors.As(err, &identity) && identity.redirect
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
