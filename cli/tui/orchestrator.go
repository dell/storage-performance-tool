/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
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
	dockerManager DockerInterface
	apiClient     *SptAPIClient
	containerID   string
	scenarioPath  string
	keepScenario  bool
	apiPort       string // Configurable API port
	networkMode   string
	mu            sync.Mutex
	compatOnce    sync.Once

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

func configureDockerFileMounts(dm DockerInterface, mounts []scenario.FileMount) error {
	if len(mounts) == 0 {
		return nil
	}
	configurer, ok := dm.(fileMountConfigurer)
	if !ok {
		return fmt.Errorf("docker manager does not support external item file mounts")
	}
	return configurer.SetFileMounts(mounts)
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
		networkMode:           constants.DefaultNetworkMode,
		stopCh:                make(chan struct{}),
		stoppedCh:             make(chan struct{}),
		completionCh:          make(chan struct{}),
		lastSuccessfulMetrics: time.Now(),
		metricsState:          newNodePollState(),
		metricsBackoffCfg:     singleNodeBackoff,
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

	return o.StartTestWithContent(ctx, image, params, []byte(scenarioContent), defaultsContent)
}

// StartTestWithContent starts a Spt test with caller-provided scenario and defaults content.
func (o *TestOrchestrator) StartTestWithContent(ctx context.Context, image string, params scenario.Params, scenarioContent, defaultsContent []byte) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	if len(scenarioContent) == 0 {
		return fmt.Errorf("scenario content is empty")
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

	if err := configureDockerFileMounts(o.dockerManager, params.ItemFileMounts); err != nil {
		return err
	}

	// Start container in node mode
	logging.LogInfo("orchestrator", "starting container in node mode", "image", image, "port", o.apiPort, "network_mode", o.networkMode)
	containerID, err := o.dockerManager.StartContainerInNodeMode(image, o.apiPort, o.networkMode)
	if err != nil {
		return fmt.Errorf("failed to start container in node mode: %w", err)
	}
	o.containerID = containerID

	// Create API client
	if o.apiClient == nil {
		o.apiClient = NewSptAPIClient(fmt.Sprintf("http://localhost:%s", o.apiPort))
	}

	// Wait for API to be ready
	logging.LogInfo("orchestrator", "waiting for Spt API to be ready")
	if o.onOutput != nil {
		o.onOutput("Waiting for Spt API to become ready...")
	}

	if err := o.apiClient.WaitForAPIReady(apiReadyTimeout); err != nil {
		// Clean up the started container since API is not ready
		logging.LogError("orchestrator", "API readiness check failed, cleaning up container", err)
		diagnostics := o.containerStartupDiagnostics()
		reportedDiagnostics := false
		if diagnostics != "" && o.onError != nil {
			o.onError("Container startup diagnostics:\n" + diagnostics)
			reportedDiagnostics = true
		}
		if cleanupErr := o.dockerManager.Cleanup(); cleanupErr != nil {
			logging.LogError("orchestrator", "failed to cleanup container after API failure", cleanupErr)
		}
		if diagnostics != "" {
			if reportedDiagnostics {
				return fmt.Errorf("spt API failed to become ready: %w; see container startup diagnostics above", err)
			}
			return fmt.Errorf("spt API failed to become ready: %w\n\nContainer startup diagnostics:\n%s", err, diagnostics)
		}
		return fmt.Errorf("spt API failed to become ready: %w", err)
	}

	if o.apiClient != nil {
		o.apiClient.LogReadySnapshot("pre-start")
	}

	if o.onOutput != nil {
		o.onOutput("Spt API is ready")
	}

	// Start the test via API
	logging.LogInfo("orchestrator", "starting test via API")
	if o.onOutput != nil {
		o.onOutput("Starting test via API...")
	}

	runID, err := o.apiClient.StartTest(scenarioContent, defaultsContent)
	if err != nil {
		// Clean up the started container since API test start failed
		logging.LogError("orchestrator", "API test start failed, cleaning up container", err)
		if cleanupErr := o.dockerManager.Cleanup(); cleanupErr != nil {
			logging.LogError("orchestrator", "failed to cleanup container after API start failure", cleanupErr)
		}
		return fmt.Errorf("failed to start test via API: %w", err)
	}

	logging.LogInfo("orchestrator", "test started successfully", "runID", runID)
	if o.onOutput != nil {
		o.onOutput(fmt.Sprintf("Test started with run ID: %s", runID))
	}

	// Start monitoring goroutines
	go o.monitorStatus(ctx)
	go o.monitorMetrics(ctx)
	go o.streamContainerOutput(ctx)

	return nil
}

// monitorStatus polls the test status periodically
func (o *TestOrchestrator) monitorStatus(ctx context.Context) {
	ticker := time.NewTicker(defaultStatusInterval)
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
			if status != nil && (status.State == constants.StateCompleted || status.State == constants.StateFailed) {
				logging.LogInfo("orchestrator", "test finished", "state", status.State)
				if o.onOutput != nil {
					o.onOutput(fmt.Sprintf("Test %s", status.State))
				}
				o.signalCompletion()
				return
			}
		}
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
			allMetrics, source, err := o.getMetricsWithOptimizedFallback()

			if err == nil && len(allMetrics) > 0 {
				o.metricsState.recordSuccess(now)
				o.updateMetricsSuccess(source)

				if o.onMetrics != nil {
					// Aggregate all op-type metrics into a combined view.
					aggregated, perOpType := AggregateByOpType(allMetrics)
					isActive := aggregated.TestState == constants.TestStateRunning
					update := &MultiNodeMetricsUpdate{
						Aggregated: aggregated,
						PerNode: map[string]*PerformanceMetric{
							"node-0": aggregated,
						},
						PerOpType: perOpType,
						NodeStatus: map[string]NodeConnectionStatus{
							"node-0": {
								LastSeen:    aggregated.Timestamp,
								IsConnected: true,
								IsActive:    isActive,
								Error:       nil,
								Phase:       NodePhaseMetricsFlowing,
							},
						},
					}
					o.onMetrics(update)
				}

				interval = o.metricsBackoffCfg.Base
			} else if err != nil {
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

// StopTest gracefully stops the test and cleans up
func (o *TestOrchestrator) StopTest() error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// Signal monitoring goroutines to stop
	close(o.stopCh)

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

	close(o.stoppedCh)
	return nil
}

// Wait waits for the orchestrator to finish
func (o *TestOrchestrator) Wait() {
	<-o.stoppedCh
}
