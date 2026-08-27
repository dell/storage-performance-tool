/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

const (
	apiReadyTimeout     = 60 * time.Second
	startupLogTailLines = 120

	// Optimized polling intervals
	defaultMetricsInterval = constants.DefaultMetricsInterval
	defaultStatusInterval  = constants.DefaultStatusInterval
)

// TestOrchestrator manages the complete lifecycle of an API-based Spt test
type TestOrchestrator struct {
	dockerManager      DockerInterface
	apiClient          *SptAPIClient
	containerID        string
	scenarioPath       string
	keepScenario       bool
	apiPort            string // Configurable API port
	networkMode        string
	resultsRoot        string
	mu                 sync.Mutex
	compatOnce         sync.Once
	duplicateFleetOnce sync.Once
	stopOnce           sync.Once
	stoppedOnce        sync.Once
	finalizeMu         sync.Mutex
	finalizeAttempt    *cleanupAttempt
	diagnosticsTimeout time.Duration
	cleanupTimeout     time.Duration
	statusInterval     time.Duration

	// Callbacks for status updates
	onStatusUpdate func(status *TestStatus)
	onMetrics      func(update *MultiNodeMetricsUpdate)
	onOutput       func(line string)
	onError        func(err string)

	// Control channels
	stopCh       chan struct{}
	stoppedCh    chan struct{}
	completionCh chan struct{} // closed when the run reaches a terminal state

	// Performance optimization
	lastSuccessfulMetrics time.Time // Track last successful metrics retrieval for health monitoring

	metricsState      *nodePollState
	metricsBackoffCfg backoffConfig
	randMu            sync.Mutex
	randSource        *rand.Rand
	logJSONBodies     bool

	terminalMetricsMu       sync.Mutex
	terminalMetricsAttempts int
	detailedDeleteObserved  bool
	completionErr           error
}

type containerDiagnosticTailer interface {
	ContainerDiagnosticTail(containerID string, maxLines int) (string, error)
}

type fileMountConfigurer interface {
	SetFileMounts([]scenario.FileMount) error
}

type nodeLogResultsRootConfigurer interface {
	setNodeLogResultsRoot(string)
}

func configureDockerFileMounts(ctx context.Context, dm DockerInterface, mounts []scenario.FileMount) error {
	if len(mounts) == 0 {
		return nil
	}
	return setFileMountsContext(ctx, dm, mounts)
}

func cleanupSingleHostStartFailure(ctx context.Context, dm DockerInterface) error {
	if dm == nil {
		return nil
	}
	cleanupCtx, cancel := boundedDetachedContext(ctx, constants.StartupRollbackTimeout)
	defer cancel()
	return dm.CleanupContext(cleanupCtx)
}

func cleanupSingleHostAmbiguousSubmission(ctx context.Context, dm DockerInterface, hooks LaunchHooks) error {
	if hooks.SessionManaged() {
		return nil
	}
	return cleanupSingleHostStartFailure(ctx, dm)
}

// NewTestOrchestrator creates a new test orchestrator
func NewTestOrchestrator(dm DockerInterface, apiPort string, nodeLogResultsRoot string) *TestOrchestrator {
	if apiPort == "" {
		apiPort = constants.SptAPIPort
	}
	if configurer, ok := dm.(nodeLogResultsRootConfigurer); ok {
		configurer.setNodeLogResultsRoot(nodeLogResultsRoot)
	}
	return &TestOrchestrator{
		dockerManager:         dm,
		apiPort:               apiPort,
		resultsRoot:           nodeLogResultsRoot,
		networkMode:           constants.DefaultNetworkMode,
		stopCh:                make(chan struct{}),
		stoppedCh:             make(chan struct{}),
		completionCh:          make(chan struct{}),
		lastSuccessfulMetrics: time.Now(),
		metricsState:          newNodePollState(),
		metricsBackoffCfg:     singleNodeBackoff,
		statusInterval:        defaultStatusInterval,
		randSource:            rand.New(rand.NewSource(time.Now().UnixNano())), // #nosec G404 -- non-crypto jitter source
		logJSONBodies:         os.Getenv("SPT_LOG_METRICS_BODY") == "1",
	}
}

// SetNetworkMode selects the Docker network mode used for API node startup.
func (o *TestOrchestrator) SetNetworkMode(networkMode string) {
	networkMode = strings.TrimSpace(networkMode)
	if networkMode == "" {
		networkMode = constants.DefaultNetworkMode
	}
	o.networkMode = networkMode
}

// SetCallbacks sets the callback functions for status updates
func (o *TestOrchestrator) SetCallbacks(
	onStatus func(*TestStatus),
	onMetrics func(*MultiNodeMetricsUpdate),
	onOutput func(string),
	onError func(string),
) {
	o.onStatusUpdate = onStatus
	o.onMetrics = onMetrics
	o.onOutput = onOutput
	o.onError = onError
}

func (o *TestOrchestrator) containerStartupDiagnostics() string {
	if o.dockerManager == nil || o.containerID == "" {
		return ""
	}
	tailer, ok := o.dockerManager.(containerDiagnosticTailer)
	if !ok {
		return ""
	}
	diagnostics, err := tailer.ContainerDiagnosticTail(o.containerID, startupLogTailLines)
	if err != nil {
		logging.LogDebug("orchestrator", "failed to collect container startup diagnostics", "error", err.Error())
		return ""
	}
	return strings.TrimSpace(diagnostics)
}

// StartTest starts a Spt test using the API approach
func (o *TestOrchestrator) StartTest(ctx context.Context, image string, params scenario.Params) error {
	return o.StartTestWithLaunchHooks(ctx, image, params, LaunchHooks{})
}

// StartTestWithLaunchHooks starts a Spt test and reports its API submission
// through explicit orchestration hooks.
func (o *TestOrchestrator) StartTestWithLaunchHooks(
	ctx context.Context, image string, params scenario.Params, hooks LaunchHooks,
) error {
	var err error
	params, err = scenario.PrepareExternalItemFiles(params)
	if err != nil {
		return err
	}

	// Generate scenario
	scenarioContent, err := scenario.GenerateScenario(params)
	if err != nil {
		return fmt.Errorf("failed to generate scenario: %w", err)
	}

	// Generate defaults configuration
	defaultsContent, err := scenario.GenerateDefaults(params)
	if err != nil {
		return fmt.Errorf("failed to generate defaults: %w", err)
	}

	return o.StartTestWithContentAndLaunchHooks(
		ctx, image, params, []byte(scenarioContent), defaultsContent, hooks)
}

// StartTestWithContent starts a Spt test with caller-provided scenario and defaults content.
func (o *TestOrchestrator) StartTestWithContent(ctx context.Context, image string, params scenario.Params, scenarioContent, defaultsContent []byte) error {
	return o.StartTestWithContentAndLaunchHooks(
		ctx, image, params, scenarioContent, defaultsContent, LaunchHooks{})
}

// StartTestWithContentAndLaunchHooks starts caller-provided content and reports
// the accepted API submission through explicit orchestration hooks.
func (o *TestOrchestrator) StartTestWithContentAndLaunchHooks(
	ctx context.Context,
	image string,
	params scenario.Params,
	scenarioContent, defaultsContent []byte,
	hooks LaunchHooks,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	o.mu.Lock()
	locked := true
	defer func() {
		if locked {
			o.mu.Unlock()
		}
	}()

	if len(scenarioContent) == 0 {
		return fmt.Errorf("scenario content is empty")
	}
	startupArgs, err := scenario.BuildEngineStartupArgs(params)
	if err != nil {
		return fmt.Errorf("resolve engine startup settings: %w", err)
	}

	// Save scenario to file if requested
	if params.KeepScenario {
		o.scenarioPath = fmt.Sprintf("spt-scenario-%d.js", time.Now().Unix())
		if err := os.WriteFile(o.scenarioPath, scenarioContent, 0600); err != nil {
			logging.LogError("orchestrator", "failed to save scenario file", err)
		} else {
			o.keepScenario = true
			logging.LogInfo("orchestrator", "saved scenario file", "path", o.scenarioPath)
		}
	}

	if err := configureDockerFileMounts(ctx, o.dockerManager, params.ItemFileMounts); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, cleanupSingleHostStartFailure(ctx, o.dockerManager))
	}

	// Start container in node mode
	logging.LogInfo("orchestrator", "starting container in node mode", "image", image, "port", o.apiPort, "network_mode", o.networkMode)
	containerID, err := startContainerInNodeModeContext(
		ctx, o.dockerManager, image, o.apiPort, o.networkMode, startupArgs)
	if err != nil {
		return errors.Join(
			fmt.Errorf("failed to start container in node mode: %w", err),
			cleanupSingleHostStartFailure(ctx, o.dockerManager),
		)
	}
	o.containerID = containerID
	if err := ctx.Err(); err != nil {
		return errors.Join(err, cleanupSingleHostStartFailure(ctx, o.dockerManager))
	}

	// Create API client
	if o.apiClient == nil {
		o.apiClient = NewSptAPIClient(fmt.Sprintf("http://localhost:%s", o.apiPort))
	}

	// Wait for API to be ready
	logging.LogInfo("orchestrator", "waiting for Spt API to be ready")
	if o.onOutput != nil {
		o.onOutput("Waiting for Spt API to become ready...")
	}

	if err := o.apiClient.WaitForAPIReadyContext(ctx, apiReadyTimeout); err != nil {
		// Clean up the started container since API is not ready
		logging.LogError("orchestrator", "API readiness check failed, cleaning up container", err)
		diagnostics := o.containerStartupDiagnostics()
		reportedDiagnostics := false
		if diagnostics != "" && o.onError != nil {
			o.onError("Container startup diagnostics:\n" + diagnostics)
			reportedDiagnostics = true
		}
		cleanupErr := cleanupSingleHostStartFailure(ctx, o.dockerManager)
		if cleanupErr != nil {
			logging.LogError("orchestrator", "failed to cleanup container after API failure", cleanupErr)
		}
		var startupErr error
		if diagnostics != "" {
			if reportedDiagnostics {
				startupErr = fmt.Errorf("spt API failed to become ready: %w; see container startup diagnostics above", err)
			} else {
				startupErr = fmt.Errorf("spt API failed to become ready: %w\n\nContainer startup diagnostics:\n%s", err, diagnostics)
			}
		} else {
			startupErr = fmt.Errorf("spt API failed to become ready: %w", err)
		}
		return errors.Join(startupErr, cleanupErr)
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, cleanupSingleHostStartFailure(ctx, o.dockerManager))
	}

	if scenario.RequiresIntegrityCapability(params) {
		if err := o.apiClient.VerifyScenarioIntegrityCapabilityContext(ctx, image, params); err != nil {
			logging.LogError("orchestrator", "integrity capability check failed, cleaning up container", err)
			cleanupErr := cleanupSingleHostStartFailure(ctx, o.dockerManager)
			if cleanupErr != nil {
				logging.LogError("orchestrator", "failed to cleanup container after capability failure", cleanupErr)
			}
			return errors.Join(err, cleanupErr)
		}
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, cleanupSingleHostStartFailure(ctx, o.dockerManager))
	}

	if o.apiClient != nil {
		o.apiClient.LogReadySnapshot("pre-start")
	}

	if o.onOutput != nil {
		o.onOutput("Spt API is ready")
	}
	if err := runAndEmitPreSubmissionCheck(ctx, hooks, o.onOutput); err != nil {
		logging.LogError("orchestrator", "pre-submission check failed, cleaning up container", err)
		return errors.Join(err, cleanupSingleHostStartFailure(ctx, o.dockerManager))
	}

	// Start the test via API
	logging.LogInfo("orchestrator", "starting test via API")
	if o.onOutput != nil {
		o.onOutput("Starting test via API...")
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, cleanupSingleHostStartFailure(ctx, o.dockerManager))
	}

	submission, submitErr := o.apiClient.StartTestContext(ctx, scenarioContent, defaultsContent, params.RunID)
	if submission.Submission == SubmissionUnknown {
		hooks.NotifySubmissionUnknown()
		logging.LogError("orchestrator", "API test submission remains ambiguous; preserving conservative cleanup ownership", submitErr)
		cleanupErr := cleanupSingleHostAmbiguousSubmission(
			ctx, o.dockerManager, hooks)
		return errors.Join(fmt.Errorf("failed to establish whether the engine accepted POST /run: %w", submitErr), cleanupErr)
	}
	if submission.Submission == SubmissionNotSubmitted {
		logging.LogError("orchestrator", "API test start failed, cleaning up container", submitErr)
		cleanupErr := cleanupSingleHostStartFailure(ctx, o.dockerManager)
		if cleanupErr != nil {
			logging.LogError("orchestrator", "failed to cleanup container after API start failure", cleanupErr)
		}
		return errors.Join(fmt.Errorf("failed to start test via API: %w", submitErr), cleanupErr)
	}
	logging.LogInfo("orchestrator", "test submission confirmed", "runID", submission.RunID)
	if o.onOutput != nil {
		o.onOutput(fmt.Sprintf("Test started with run ID: %s", submission.RunID))
	}

	if submitErr == nil {
		// Start monitoring goroutines only while the launch context remains active.
		go o.monitorStatus(ctx)
		go o.monitorMetrics(ctx)
		go o.streamContainerOutput(ctx)
	}

	// Submission state is fully committed before caller-controlled notification.
	// Keep acknowledgment synchronous, but never hold the lifecycle mutex while
	// the hook prepares result artifacts or waits for its launch gate.
	o.mu.Unlock()
	locked = false
	notifyAcceptedSubmission(hooks, submitErr)
	if submitErr != nil {
		return fmt.Errorf("engine submission was confirmed after POST /run returned an error: %w", submitErr)
	}

	return nil
}

func isPresentationTerminalState(state string) bool {
	switch state {
	case constants.StateCompleted, constants.StateFailed, constants.StateStopped:
		return true
	default:
		return false
	}
}

// monitorStatus polls the test status periodically
func (o *TestOrchestrator) monitorStatus(ctx context.Context) {
	ticker := time.NewTicker(o.statusInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-o.stopCh:
			return
		case <-ticker.C:
			status, err := o.apiClient.GetStatus()
			if err != nil {
				logging.LogDebug("orchestrator", "failed to get status", "error", err)
				continue
			}

			if o.onStatusUpdate != nil && status != nil {
				o.onStatusUpdate(status)
			}

			// Check if test has completed
			if status != nil && isPresentationTerminalState(status.State) {
				if !o.apiClient.statusMatchesOwnedRun(status) {
					logging.LogDebug(
						"orchestrator", "ignoring terminal status not attributed to owned run",
						"state", status.State, "observed_run_id", status.RunID,
						"owned_run_id", o.apiClient.getRunID())
					continue
				}
				logging.LogInfo("orchestrator", "test finished", "state", status.State)
				ready, terminalErr, captureErr := o.publishTerminalMetrics()
				if captureErr != nil {
					o.failTerminalMetricsCapture(captureErr)
					o.signalCompletion()
					return
				}
				if !ready {
					continue
				}
				terminalErr = errors.Join(terminalErr, ownedTerminalStatusFailure(status))
				if terminalErr != nil {
					o.failOwnedTerminalRun(terminalErr)
				}
				if o.CompletionError() != nil {
					o.signalCompletion()
					return
				}
				if o.onOutput != nil {
					o.onOutput(fmt.Sprintf("Test %s", status.State))
				}
				o.signalCompletion()
				return
			}
		}
	}
}

func ownedTerminalStatusFailure(status *TestStatus) error {
	if status == nil || status.State != constants.StateFailed {
		return nil
	}
	detail := strings.TrimSpace(status.Message)
	if detail == "" {
		detail = "engine reported FAILED"
	}
	return &runcontrol.OwnedEngineTerminalFailure{Detail: detail}
}

func (o *TestOrchestrator) publishTerminalMetrics() (bool, error, error) {
	update, _, err := o.collectMetricsUpdate()
	if err != nil {
		if o.terminalMetricsCompatibilityFallback(err) {
			return true, nil, nil
		}
		logging.LogWarn(
			"orchestrator",
			"terminal metrics fetch failed before completion",
			"error",
			err.Error())
		ready, captureErr := o.recordTerminalMetricsRetry(err)
		return ready, nil, captureErr
	}
	o.observeDetailedDeleteMetrics(update)
	if !terminalMetricsReady(update) {
		logging.LogDebug(
			"orchestrator",
			"waiting for controller-authoritative terminal DELETE metrics")
		ready, captureErr := o.recordTerminalMetricsRetry(
			fmt.Errorf("controller-authoritative terminal DELETE metrics are unavailable"))
		return ready, nil, captureErr
	}
	o.resetTerminalMetricsRetries()
	if o.onMetrics != nil {
		o.onMetrics(update)
	}
	return true, terminalDeletePolicyFailure(update), nil
}

func terminalDeletePolicyFailure(update *MultiNodeMetricsUpdate) error {
	if update == nil {
		return nil
	}
	metric := update.Aggregated
	if metric == nil || metric.Delete == nil {
		metric = update.PerOpType["DELETE"]
	}
	if metric == nil || metric.MetricsSchema < deletemetrics.SchemaVersion ||
		!metric.DeleteDetailExpected || metric.Delete == nil ||
		metric.Delete.FailurePolicy.Outcome != deletemetrics.OutcomeFailed {
		return nil
	}
	if deletemetrics.HasVerificationFailure(metric.Delete.Verification) {
		return fmt.Errorf(
			"DELETE verification failed for step %q",
			metric.StepID)
	}
	return fmt.Errorf(
		"DELETE failure policy rejected the terminal outcome for step %q",
		metric.StepID)
}

func (o *TestOrchestrator) observeDetailedDeleteMetrics(update *MultiNodeMetricsUpdate) {
	metric := detailedDeleteMetric(update)
	if metric == nil {
		return
	}
	o.terminalMetricsMu.Lock()
	o.detailedDeleteObserved = true
	o.terminalMetricsMu.Unlock()
}

func detailedDeleteMetric(update *MultiNodeMetricsUpdate) *PerformanceMetric {
	if update == nil {
		return nil
	}
	metric := update.Aggregated
	if metric == nil || !strings.EqualFold(metric.OpType, "DELETE") {
		metric = update.PerOpType["DELETE"]
	}
	if metric == nil || metric.MetricsSchema < deletemetrics.SchemaVersion ||
		!metric.DeleteDetailExpected || !strings.EqualFold(metric.OpType, "DELETE") {
		return nil
	}
	return metric
}

func (o *TestOrchestrator) terminalMetricsCompatibilityFallback(err error) bool {
	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) || statusErr.StatusCode != http.StatusNotFound {
		return false
	}
	o.terminalMetricsMu.Lock()
	defer o.terminalMetricsMu.Unlock()
	return !o.detailedDeleteObserved
}

func (o *TestOrchestrator) recordTerminalMetricsRetry(cause error) (bool, error) {
	o.terminalMetricsMu.Lock()
	defer o.terminalMetricsMu.Unlock()
	o.terminalMetricsAttempts++
	if o.terminalMetricsAttempts < constants.TerminalMetricsCaptureAttempts {
		return false, nil
	}
	return false, fmt.Errorf(
		"terminal DELETE metrics capture failed after %d attempts: %w",
		o.terminalMetricsAttempts,
		cause)
}

func (o *TestOrchestrator) resetTerminalMetricsRetries() {
	o.terminalMetricsMu.Lock()
	o.terminalMetricsAttempts = 0
	o.terminalMetricsMu.Unlock()
}

func (o *TestOrchestrator) failTerminalMetricsCapture(cause error) {
	logging.LogError("orchestrator", "terminal DELETE metrics capture failed", cause)
	o.recordCompletionFailure(cause)
}

func (o *TestOrchestrator) failOwnedTerminalRun(cause error) {
	logging.LogError("orchestrator", "owned engine run failed", cause)
	o.recordCompletionFailure(cause)
}

func (o *TestOrchestrator) recordCompletionFailure(cause error) {
	message := fmt.Sprintf("Test FAILED: %v", cause)
	o.terminalMetricsMu.Lock()
	recorded := o.completionErr == nil
	if o.completionErr == nil {
		o.completionErr = cause
	}
	o.terminalMetricsMu.Unlock()
	if !recorded {
		return
	}
	if o.onError != nil {
		o.onError(message)
	} else if o.onOutput != nil {
		o.onOutput(message)
	}
}

// monitorMetrics polls the metrics endpoint periodically with optimized endpoint preference
func (o *TestOrchestrator) monitorMetrics(ctx context.Context) {
	interval := o.metricsBackoffCfg.Base
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-o.stopCh:
			return
		case <-timer.C:
			now := time.Now()
			update, source, err := o.collectMetricsUpdate()

			if err == nil {
				o.metricsState.recordSuccess(now)
				o.updateMetricsSuccess(source)
				o.observeDetailedDeleteMetrics(update)
				if o.onMetrics != nil {
					o.onMetrics(update)
				}
			}
			if err != nil {
				kind := classifyPollError(err)
				delay, warn := o.metricsState.recordFailure(now, err, kind, o.metricsBackoffCfg, o.randomJitter)
				if warn {
					logging.LogWarn("metrics-poll", "JSON metrics polling has been failing", "node", "node-0", "error", err.Error())
				} else {
					logging.LogDebug("metrics-poll", "JSON metrics poll failed", "node", "node-0", "error", err.Error(), "next_delay", delay.String())
				}
				interval = delay
			} else {
				interval = o.metricsBackoffCfg.Base
			}

			if interval <= 0 {
				interval = o.metricsBackoffCfg.Base
			}

			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			timer.Reset(interval)
		}
	}
}

func (o *TestOrchestrator) collectMetricsUpdate() (*MultiNodeMetricsUpdate, string, error) {
	allMetrics, source, err := o.getMetricsWithOptimizedFallback()
	if err != nil {
		return nil, source, err
	}
	if len(allMetrics) == 0 {
		return nil, source, fmt.Errorf("%w: local metrics are empty", ErrMetricsIncompatible)
	}
	update, err := buildLocalMetricsUpdateForRun(allMetrics, o.apiClient.getRunID())
	if err != nil {
		return nil, source, err
	}
	if fleetDelete := fetchAuthoritativeFleetDelete(
		o.apiClient, update.Aggregated, update.PerOpType,
		&o.duplicateFleetOnce); fleetDelete != nil {
		update.Aggregated = applyAuthoritativeFleetDelete(
			update.Aggregated, update.PerOpType, fleetDelete)
	}
	return update, source, nil
}

func terminalMetricsReady(update *MultiNodeMetricsUpdate) bool {
	if update == nil {
		return true
	}
	metric := update.Aggregated
	if metric == nil || !strings.EqualFold(metric.OpType, "DELETE") {
		metric = update.PerOpType["DELETE"]
	}
	if metric == nil || metric.MetricsSchema < deletemetrics.SchemaVersion ||
		!metric.DeleteDetailExpected || !strings.EqualFold(metric.OpType, "DELETE") {
		return true
	}
	return !metric.Partial && deletemetrics.ValidateTerminal(metric.Delete) == nil
}

func buildLocalMetricsUpdateForRun(
	allMetrics []*PerformanceMetric, ownedRunID string,
) (*MultiNodeMetricsUpdate, error) {
	currentMetrics, err := selectCurrentMetricSetForRun(allMetrics, nil, ownedRunID)
	if err != nil {
		return nil, err
	}
	currentMetrics = normalizeLocalContributorIdentity(currentMetrics)
	aggregated, perOpType := AggregateByOpType(currentMetrics)
	if aggregated == nil {
		return nil, fmt.Errorf("%w: current local metrics cannot be aggregated", ErrMetricsIncompatible)
	}
	isActive := aggregated.TestState == constants.TestStateRunning
	return &MultiNodeMetricsUpdate{
		Aggregated: aggregated,
		PerNode: map[string]*PerformanceMetric{
			"node-0": aggregated,
		},
		PerOpType: perOpType,
		NodeStatus: map[string]NodeConnectionStatus{
			"node-0": {
				LastSeen: aggregated.Timestamp, IsConnected: true, IsActive: isActive,
				Phase: NodePhaseMetricsFlowing,
			},
		},
	}, nil
}

func normalizeLocalContributorIdentity(metrics []*PerformanceMetric) []*PerformanceMetric {
	result := make([]*PerformanceMetric, len(metrics))
	for index, metric := range metrics {
		result[index] = metric
		if metric == nil || metric.MetricsSchema < deletemetrics.SchemaVersion ||
			!metric.DeleteDetailExpected || !strings.EqualFold(metric.OpType, "DELETE") {
			continue
		}
		copyMetric := *metric
		copyMetric.NodesCount = 1
		copyMetric.NodesPresent = []string{constants.MetricsLocalContributorID}
		copyMetric.ContributorsPresent = []string{constants.MetricsLocalContributorID}
		result[index] = &copyMetric
	}
	return result
}

// getMetricsWithOptimizedFallback gets metrics from JSON endpoint only
func (o *TestOrchestrator) getMetricsWithOptimizedFallback() ([]*PerformanceMetric, string, error) {
	metrics, source, err := o.tryJSONMetrics()
	if err != nil {
		return nil, source, err
	}
	return metrics, source, nil
}

// tryJSONMetrics attempts to get metrics from JSON endpoint
func (o *TestOrchestrator) tryJSONMetrics() ([]*PerformanceMetric, string, error) {
	if o.apiClient == nil {
		return nil, "", fmt.Errorf("api client not initialized")
	}

	jsonData, err := o.apiClient.GetJSONMetrics()
	if err != nil {
		return nil, "", err
	}

	metrics, parseErr := o.apiClient.ParseJSONMetrics(jsonData)
	if parseErr != nil || len(metrics) == 0 {
		if parseErr != nil {
			if o.logJSONBodies {
				o.logVerboseMetrics(parseErr)
			}
			if errors.Is(parseErr, ErrMetricsIncompatible) {
				o.handleCompatibilityError(parseErr)
			} else {
				logging.LogDebug("metrics-poll", "failed to parse JSON metrics", "error", parseErr.Error())
			}
		}
		return nil, "", parseErr
	}

	return metrics, "JSON", nil
}

func (o *TestOrchestrator) logVerboseMetrics(parseErr error) {
	if o.apiClient == nil {
		return
	}

	payload, err := o.apiClient.GetJSONMetricsVerbose()
	if err != nil {
		logging.LogDebug("metrics-poll", "failed to fetch verbose metrics payload", "node", "node-0", "error", err.Error(), "parse_error", parseErr.Error())
		return
	}

	head := truncateForLog(payload, metricsPayloadPreviewLen)
	logging.LogDebug("metrics-poll", "metrics payload (verbose)",
		"node", "node-0",
		"len", len(payload),
		"head", head,
		"parse_error", parseErr.Error())
}

func (o *TestOrchestrator) randomJitter(limit time.Duration) time.Duration {
	if limit <= 0 {
		return 0
	}
	o.randMu.Lock()
	defer o.randMu.Unlock()
	return time.Duration(o.randSource.Int63n(int64(limit) + 1))
}

func (o *TestOrchestrator) handleCompatibilityError(err error) {
	if err == nil {
		return
	}
	o.compatOnce.Do(func() {
		msg := fmt.Sprintf("Incompatible metrics JSON from server: %v. This spt build requires metrics_schema >= 2. Please upgrade the Spt image.", err)
		logging.LogError("orchestrator", "metrics compatibility error", err)
		if o.onError != nil {
			o.onError(msg)
		} else if o.onOutput != nil {
			o.onOutput(msg)
		}
	})
}

// updateMetricsSuccess updates optimization state when metrics are successfully retrieved
func (o *TestOrchestrator) updateMetricsSuccess(_ string) {
	o.mu.Lock()
	defer o.mu.Unlock()

	o.lastSuccessfulMetrics = time.Now()
}

// streamContainerOutput streams the container logs
func (o *TestOrchestrator) streamContainerOutput(_ context.Context) {
	if o.containerID == "" {
		return
	}

	// Stream container output for logs
	o.dockerManager.StreamOutput(o.containerID,
		func(line string) {
			if o.onOutput != nil {
				o.onOutput(line)
			}
		},
		func(line string) {
			if o.onError != nil {
				o.onError(line)
			}
		})
}

// signalCompletion closes completionCh exactly once.
func (o *TestOrchestrator) signalCompletion() {
	select {
	case <-o.completionCh:
		// already closed
	default:
		close(o.completionCh)
	}
}

// CompletionCh returns a channel that is closed when the run reaches a terminal state.
func (o *TestOrchestrator) CompletionCh() <-chan struct{} {
	return o.completionCh
}

// CompletionError returns a terminal presentation failure, if one prevented a valid final result.
func (o *TestOrchestrator) CompletionError() error {
	o.terminalMetricsMu.Lock()
	defer o.terminalMetricsMu.Unlock()
	return o.completionErr
}

// StopTest gracefully stops the test and cleans up
func (o *TestOrchestrator) StopTest() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Signal monitoring goroutines to stop
	o.stopOnce.Do(func() { close(o.stopCh) })

	// Stop the node via API
	if o.apiClient != nil {
		logging.LogInfo("orchestrator", "requesting node shutdown via API")
		if o.onOutput != nil {
			o.onOutput("Requesting node shutdown gracefully...")
		}

		o.apiClient.LogReadySnapshot("pre-shutdown")

		if err := o.apiClient.Shutdown(); err != nil {
			logging.LogError("orchestrator", "failed to request shutdown via API", err)
			// Continue with cleanup even if API stop fails
		} else {
			// Wait for /status linger window
			_ = o.apiClient.WaitForLinger(constants.APILingerDefault)
			logging.LogInfo("orchestrator", "shutdown request accepted")
			if o.onOutput != nil {
				o.onOutput("Node shutdown requested")
			}
		}

		// Give the test a moment to stop cleanly (shorter in tests)
		time.Sleep(100 * time.Millisecond)

		o.apiClient.LogReadySnapshot("post-shutdown")
	}

	// Cleanup container
	if o.dockerManager != nil {
		logging.LogInfo("orchestrator", "cleaning up container")
		if err := o.dockerManager.Cleanup(); err != nil {
			logging.LogError("orchestrator", "failed to cleanup container", err)
			return err
		}
	}

	// Clean up scenario file if not keeping it
	if o.scenarioPath != "" && !o.keepScenario {
		logging.LogDebug("orchestrator", "removing scenario file", "path", o.scenarioPath)
		_ = os.Remove(o.scenarioPath)
	}

	o.stoppedOnce.Do(func() { close(o.stoppedCh) })
	return nil
}

// Wait waits for the orchestrator to finish
func (o *TestOrchestrator) Wait() {
	<-o.stoppedCh
}
