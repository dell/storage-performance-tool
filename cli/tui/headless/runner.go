/*
Copyright © 2025 Dell Technologies
*/

// Package headless provides non-interactive execution flows for running
// Spt via Docker without the TUI.
package headless

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/cmdline"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
)

const notAvailableDisplay = "N/A"

// HeadlessRunner manages headless execution of spt benchmarks
//
//revive:disable-next-line:exported
type HeadlessRunner struct {
	dockerManager tui.DockerInterface
	orchestrator  *tui.TestOrchestrator
	traceFile     *os.File
	verbose       bool
	jsonMode      bool
	metricsOnly   bool
	dryRun        bool
	keepScenario  bool
	scenarioPath  string
	apiPort       string
	networkMode   string
	resultsRoot   string
	launchHooks   tui.LaunchHooks
}

// HeadlessOptions holds configuration for headless mode
//
//revive:disable-next-line:exported
type HeadlessOptions struct {
	Context     context.Context
	TraceFile   string
	TraceAppend bool
	Verbose     bool
	APIPort     string
	NetworkMode string
	JSONMode    bool
	MetricsOnly bool
	DryRun      bool
	ResultsRoot string
	// KeepScenario removed - this belongs in scenario.Params
	AutoTerminateSeconds int // 0 = unlimited
	// In multi-host runs, let auto-results fetch artifacts and request shutdown
	// after normal completion instead of stopping containers immediately.
	DelegateNormalShutdown bool
	ExpectedStepIDs        []string
	LaunchHooks            tui.LaunchHooks
	ScenarioContent        []byte
	DefaultsContent        []byte
}

// AutoTerminateError indicates a headless multi-host run reached the configured
// auto-terminate deadline. CleanupComplete reports whether the runner already
// stopped managed containers before returning.
type AutoTerminateError struct {
	CleanupComplete bool
}

func stopAllContainersAfterRun(ctx context.Context, orchestrator *tui.MultiHostOrchestrator) error {
	if orchestrator == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(ctx), constants.ContainerCleanupTimeout)
	defer cancel()
	return orchestrator.StopAllContainers(cleanupCtx)
}

func (e *AutoTerminateError) Error() string {
	return "auto-terminate deadline reached"
}

// AutoTerminateState reports whether err represents a configured auto-terminate
// deadline and whether the runner completed container cleanup before returning.
func AutoTerminateState(err error) (bool, bool) {
	var autoErr *AutoTerminateError
	if !errors.As(err, &autoErr) {
		return false, false
	}
	return true, autoErr.CleanupComplete
}

// NewHeadlessRunner creates a new headless runner
func NewHeadlessRunner(dockerManager tui.DockerInterface, options HeadlessOptions) (*HeadlessRunner, error) {
	// Use default port if not specified
	apiPort := options.APIPort
	if apiPort == "" {
		apiPort = constants.SptAPIPort
	}
	networkMode := strings.TrimSpace(options.NetworkMode)
	if networkMode == "" {
		networkMode = constants.DefaultNetworkMode
	}

	runner := &HeadlessRunner{
		dockerManager: dockerManager,
		verbose:       options.Verbose,
		jsonMode:      options.JSONMode,
		metricsOnly:   options.MetricsOnly,
		dryRun:        options.DryRun,
		apiPort:       apiPort,
		networkMode:   networkMode,
		resultsRoot:   options.ResultsRoot,
		launchHooks:   options.LaunchHooks,
		// keepScenario will be set when RunWithScenario is called
	}

	// Set up trace file if specified
	if options.TraceFile != "" {
		var err error
		flags := os.O_CREATE | os.O_WRONLY
		if options.TraceAppend {
			flags |= os.O_APPEND
		} else {
			flags |= os.O_TRUNC
		}

		// #nosec G304: Accept user-specified output path for trace file
		runner.traceFile, err = os.OpenFile(options.TraceFile, flags, 0600)
		if err != nil {
			return nil, fmt.Errorf("failed to open trace file: %w", err)
		}

		// Write trace file header
		runner.writeTraceHeader(options.TraceFile)
	}

	return runner, nil
}

// MultiHostHeadlessRunner manages headless execution across multiple hosts
type MultiHostHeadlessRunner struct {
	orchestrator           *tui.MultiHostOrchestrator
	traceFile              *os.File
	verbose                bool
	jsonMode               bool
	metricsOnly            bool
	delegateNormalShutdown bool
	expectedStepIDs        []string
	launchHooks            tui.LaunchHooks
}

// NewMultiHostHeadlessRunner creates a new multi-host headless runner
func NewMultiHostHeadlessRunner(orchestrator *tui.MultiHostOrchestrator, options HeadlessOptions) (*MultiHostHeadlessRunner, error) {
	runner := &MultiHostHeadlessRunner{
		orchestrator:           orchestrator,
		verbose:                options.Verbose,
		jsonMode:               options.JSONMode,
		metricsOnly:            options.MetricsOnly,
		delegateNormalShutdown: options.DelegateNormalShutdown,
		expectedStepIDs:        append([]string(nil), options.ExpectedStepIDs...),
		launchHooks:            options.LaunchHooks,
	}

	// Set up trace file if specified
	if options.TraceFile != "" {
		var err error
		flags := os.O_CREATE | os.O_WRONLY
		if options.TraceAppend {
			flags |= os.O_APPEND
		} else {
			flags |= os.O_TRUNC
		}

		// #nosec G304: Accept user-specified output path for trace file
		runner.traceFile, err = os.OpenFile(options.TraceFile, flags, 0600)
		if err != nil {
			return nil, fmt.Errorf("failed to open trace file: %w", err)
		}

		// Write multi-host trace file header
		runner.writeTraceHeader(options.TraceFile, orchestrator.GetHostCount())
	}

	return runner, nil
}

// RunWithParams executes the multi-host benchmark in headless mode.
func (r *MultiHostHeadlessRunner) RunWithParams(ctx context.Context, image string, scenarioPath string, params scenario.Params) error {
	return r.runWithParams(ctx, image, scenarioPath, params, nil, nil)
}

// RunWithScenarioContentAndParams executes a multi-host orchestrated run with
// caller-provided scenario/defaults content.
func (r *MultiHostHeadlessRunner) RunWithScenarioContentAndParams(ctx context.Context, image string, scenarioPath string, params scenario.Params, scenarioContent, defaultsContent []byte) error {
	return r.runWithParams(ctx, image, scenarioPath, params, scenarioContent, defaultsContent)
}

func (r *MultiHostHeadlessRunner) runWithParams(ctx context.Context, image string, scenarioPath string, params scenario.Params, scenarioContent, defaultsContent []byte) error {
	r.output("INIT", fmt.Sprintf("Starting multi-host headless benchmark on %d hosts", r.orchestrator.GetHostCount()))
	r.output("INIT", fmt.Sprintf("Image: %s", image))
	r.output("INIT", fmt.Sprintf("Scenario: %s", scenarioPath))

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigChan)

	// Start the distributed test on all hosts via orchestrator wrapper
	testOrchestrator := tui.NewMultiHostTestOrchestrator(r.orchestrator)
	if err := r.launchHooks.RegisterResourceFinalizer(
		r.orchestrator.FinalizeDiagnosticsAndCleanupOutcome); err != nil {
		return fmt.Errorf("register session resource finalizer: %w", err)
	}
	testOrchestrator.SetExpectedStepIDs(r.expectedStepIDs)
	testOrchestrator.SetCallbacks(
		func(status *tui.TestStatus) {
			if status != nil && !r.metricsOnly {
				r.output("STATUS", fmt.Sprintf("Test %s - %s", status.State, status.Message))
			}
		},
		r.outputMetricsUpdate,
		func(line string) {
			if !r.metricsOnly {
				r.output("SPT", line)
			}
		},
		func(message string) {
			r.output("ERROR", message)
		},
	)
	// Standardize progress output in headless mode as well
	r.orchestrator.SetNotifier(func(msg string) {
		if !r.metricsOnly {
			r.output("spt", msg)
		}
	})
	// In headless mode, ensure entry-node relay and progress messages are echoed.
	// Route message sink lines (entry logs and [spt] messages) to stdout/trace.
	testOrchestrator.SetMessageSink(func(msg string) {
		if !r.metricsOnly {
			r.output("spt", msg)
		}
	})
	var err error
	if scenarioContent != nil {
		err = testOrchestrator.StartTestWithContentAndLaunchHooks(
			ctx, image, params, scenarioContent, defaultsContent, r.launchHooks)
	} else {
		err = testOrchestrator.StartTestWithLaunchHooks(ctx, image, params, r.launchHooks)
	}
	if err != nil {
		r.output("ERROR", fmt.Sprintf("Failed to start test: %v", err))
		if stopErr := stopMultiHostAfterLaunchError(ctx, r.orchestrator, r.launchHooks); stopErr != nil {
			r.output("ERROR", fmt.Sprintf("Failed to stop containers: %v", stopErr))
			if err == nil {
				err = stopErr
			}
		}
		return err
	}
	r.output("INIT", "Distributed test started successfully; monitoring...")

	// Monitor the containers until they complete or are interrupted
	done := make(chan error, 1)
	go func() {
		// Simple monitoring loop - in a full implementation this would
		// collect and display metrics from all hosts
		select {
		case <-ctx.Done():
			done <- ctx.Err()
		case <-testOrchestrator.CompletionCh():
			done <- nil
		case <-r.launchHooks.WorkloadTerminal():
			done <- nil
		case sig := <-sigChan:
			r.output("SIGNAL", fmt.Sprintf("Received signal: %v", sig))
			done <- fmt.Errorf("interrupted by signal: %v", sig)
		}
	}()

	// Wait for completion or interruption
	err = <-done
	completionErr := testOrchestrator.CompletionError()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			r.output("SHUTDOWN", "Auto-terminate deadline reached")
		} else {
			r.output("SHUTDOWN", fmt.Sprintf("Shutting down due to: %v", err))
		}
	} else if r.launchHooks.SessionManaged() {
		// Release presentation-only metrics and log polling; resource
		// finalization remains owned by the RunSession.
		if stopErr := testOrchestrator.StopTest(); stopErr != nil {
			return fmt.Errorf("stop multi-host presentation monitoring: %w", stopErr)
		}
		if completionErr != nil {
			return completionErr
		}
		r.output("SHUTDOWN", "Normal completion detected; RunSession owns evidence and resource finalization")
		return nil
	} else if r.delegateNormalShutdown {
		r.output("SHUTDOWN", "Normal completion detected; auto-results will fetch artifacts and stop containers")
		return completionErr
	}

	if r.launchHooks.SessionManaged() {
		return err
	}

	// Stop all containers
	r.output("SHUTDOWN", "Stopping all containers...")
	stopErr := stopAllContainersAfterRun(ctx, r.orchestrator)
	if stopErr != nil {
		r.output("ERROR", fmt.Sprintf("Failed to stop containers: %v", stopErr))
		if err == nil {
			err = stopErr
		}
	} else {
		r.output("SHUTDOWN", "All containers stopped successfully")
	}

	return multiHostShutdownResult(errors.Join(err, completionErr), stopErr)
}

func multiHostShutdownResult(runErr, stopErr error) error {
	if stopErr != nil {
		if runErr != nil && !errors.Is(runErr, context.DeadlineExceeded) {
			return errors.Join(runErr, stopErr)
		}
		if errors.Is(runErr, context.DeadlineExceeded) {
			return errors.Join(&AutoTerminateError{CleanupComplete: false}, stopErr)
		}
		return stopErr
	}
	if errors.Is(runErr, context.DeadlineExceeded) {
		return &AutoTerminateError{CleanupComplete: true}
	}
	return runErr
}

func stopMultiHostAfterLaunchError(ctx context.Context, orchestrator *tui.MultiHostOrchestrator, hooks tui.LaunchHooks) error {
	if hooks.SessionManaged() {
		return nil
	}
	return stopAllContainersAfterRun(ctx, orchestrator)
}

// Close cleans up resources
func (r *MultiHostHeadlessRunner) Close() error {
	if r.traceFile != nil {
		_ = r.traceFile.Close()
	}
	return nil
}

// output writes a message with timestamp and category
func (r *MultiHostHeadlessRunner) output(category, message string) {
	timestamp := time.Now().Format("2006-01-02 15:04:05.000")
	output := fmt.Sprintf("[%s] [%s] %s", timestamp, category, message)

	fmt.Println(output)

	if r.traceFile != nil {
		// Best-effort logging; ignore write/sync errors
		_, _ = r.traceFile.WriteString(output + "\n") //nolint:errcheck
		_ = r.traceFile.Sync()                        //nolint:errcheck
	}
}

func (r *MultiHostHeadlessRunner) outputMetricsUpdate(update *tui.MultiNodeMetricsUpdate) {
	if update == nil {
		return
	}
	if update.Aggregated != nil {
		r.outputMetricView("aggregate", "", update.Aggregated)
	}
	if len(update.PerOpType) > 1 {
		operations := make([]string, 0, len(update.PerOpType))
		for operation := range update.PerOpType {
			operations = append(operations, operation)
		}
		sort.Strings(operations)
		for _, operation := range operations {
			r.outputMetricView("operation", operation, update.PerOpType[operation])
		}
	}
	contributors := make([]string, 0, len(update.PerNode))
	for contributor := range update.PerNode {
		contributors = append(contributors, contributor)
	}
	sort.Strings(contributors)
	for _, contributor := range contributors {
		r.outputMetricView("node", contributor, update.PerNode[contributor])
	}
}

func (r *MultiHostHeadlessRunner) outputMetricView(
	view, contributor string, metric *tui.PerformanceMetric,
) {
	if metric == nil {
		return
	}
	if r.jsonMode {
		encoded, err := marshalMetricsJSON(*metric, view, contributor)
		if err != nil {
			r.output("METRICS", fmt.Sprintf("failed to encode metrics JSON: %v", err))
			return
		}
		r.outputJSONLine(encoded)
		return
	}
	label := "view=" + view
	if contributor != "" {
		if view == "operation" {
			label += " operation=" + contributor
		} else {
			label += " contributor=" + contributor
		}
	}
	r.output("METRICS", label+" "+formatMetricsMessage(*metric))
}

func (r *MultiHostHeadlessRunner) outputJSONLine(encoded []byte) {
	line := string(encoded)
	fmt.Println(line)
	if r.traceFile != nil {
		_, _ = r.traceFile.WriteString(line + "\n") //nolint:errcheck
		_ = r.traceFile.Sync()                      //nolint:errcheck
	}
}

// writeTraceHeader writes the trace file header
func (r *MultiHostHeadlessRunner) writeTraceHeader(filename string, hostCount int) {
	header := fmt.Sprintf("Multi-Host Headless Mode Trace - %d hosts\n", hostCount)
	header += fmt.Sprintf("Started: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	header += fmt.Sprintf("Runtime: %s %s\n", runtime.GOOS, runtime.GOARCH)
	header += fmt.Sprintf("Trace File: %s\n", filename)
	header += fmt.Sprintf("Command: %s\n", cmdline.FormatForArtifact(os.Args))
	header += strings.Repeat("=", 50) + "\n"

	fmt.Print(header)

	if r.traceFile != nil {
		_, _ = r.traceFile.WriteString(header) //nolint:errcheck
		_ = r.traceFile.Sync()                 //nolint:errcheck
	}
}

// Run executes the benchmark in headless mode
func (r *HeadlessRunner) Run(ctx context.Context, image string, scenarioPath string) error {
	// Create empty params for backward compatibility - endpoint args will be empty
	params := scenario.Params{}
	return r.RunWithParams(ctx, image, scenarioPath, params)
}

// RunWithParams executes the benchmark in headless mode with scenario parameters
func (r *HeadlessRunner) RunWithParams(ctx context.Context, image string, scenarioPath string, params scenario.Params) error {
	return r.runWithParams(ctx, image, scenarioPath, params, nil, nil)
}

// RunWithScenarioContentAndParams executes the benchmark with caller-provided scenario and defaults content.
func (r *HeadlessRunner) RunWithScenarioContentAndParams(ctx context.Context, image string, scenarioPath string, params scenario.Params, scenarioContent, defaultsContent []byte) error {
	return r.runWithParams(ctx, image, scenarioPath, params, scenarioContent, defaultsContent)
}

func (r *HeadlessRunner) runWithParams(ctx context.Context, image string, scenarioPath string, params scenario.Params, scenarioContent, defaultsContent []byte) error {
	r.scenarioPath = scenarioPath
	r.keepScenario = params.KeepScenario // Get from params

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create context that can be cancelled
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Handle signals in a goroutine
	go func() {
		sig := <-sigChan
		r.output("SIGNAL", "Received signal %s, shutting down gracefully...", sig)
		cancel()
	}()

	// Initialize
	r.output("INIT", "Starting spt in headless mode")
	if r.traceFile != nil {
		r.output("INIT", "Trace file: %s", r.traceFile.Name())
	}

	// Dry run mode - just show what would be executed
	if r.dryRun {
		if scenarioContent != nil {
			r.runDryModeWithContent(image, scenarioPath, scenarioContent, defaultsContent)
			return nil
		}
		return r.runDryModeWithParams(image, scenarioPath, params)
	}

	// Start the actual benchmark
	if scenarioContent != nil {
		return r.runBenchmarkWithContent(runCtx, image, scenarioPath, params, scenarioContent, defaultsContent)
	}
	return r.runBenchmarkWithParams(runCtx, image, scenarioPath, params)
}

// runDryMode removed as unused (dead code); use runDryModeWithParams instead

// runDryModeWithParams shows what would be executed without running, with scenario parameters
func (r *HeadlessRunner) runDryModeWithParams(image string, scenarioPath string, params scenario.Params) error {
	r.output("DRY-RUN", "Would execute benchmark with API mode:")
	r.output("DRY-RUN", "  Image: %s", image)
	r.output("DRY-RUN", "  Input scenario: %s", scenarioPath)
	r.output("DRY-RUN", "  API Port: %s", r.apiPort)

	// Generate scenario content and disclose only its identity. Scenario and
	// defaults bytes may contain credentials supplied through overrides.
	scenarioContent, err := scenario.GenerateScenario(params)
	if err != nil {
		r.output("ERROR", "Failed to generate scenario: %v", err)
		return err
	}

	r.outputDryRunContentIdentity("Generated scenario content", []byte(scenarioContent))

	// Generate defaults configuration and disclose only its identity.
	defaultsContent, err := scenario.GenerateDefaults(params)
	if err != nil {
		r.output("ERROR", "Failed to generate defaults: %v", err)
		return err
	}

	r.outputDryRunContentIdentity("Generated defaults configuration", defaultsContent)

	r.output("DRY-RUN", "Would start container with --run-node")
	r.output("DRY-RUN", "Would wait for API to be ready on http://localhost:%s", r.apiPort)
	r.output("DRY-RUN", "Would POST scenario and defaults to /run endpoint")
	r.output("DRY-RUN", "Would poll /status every 2 seconds")
	r.output("DRY-RUN", "Would poll /metrics every 500ms")
	r.output("DRY-RUN", "Would POST /shutdown for graceful node termination")

	r.output("COMPLETE", "Dry run completed")
	return nil
}

func (r *HeadlessRunner) runDryModeWithContent(image string, scenarioPath string, scenarioContent, defaultsContent []byte) {
	r.output("DRY-RUN", "Would execute benchmark with API mode:")
	r.output("DRY-RUN", "  Image: %s", image)
	r.output("DRY-RUN", "  Input scenario: %s", scenarioPath)
	r.output("DRY-RUN", "  API Port: %s", r.apiPort)

	r.outputDryRunContentIdentity("Provided scenario content", scenarioContent)
	r.outputDryRunContentIdentity("Provided defaults configuration", defaultsContent)

	r.output("DRY-RUN", "Would start container with --run-node")
	r.output("DRY-RUN", "Would wait for API to be ready on http://localhost:%s", r.apiPort)
	r.output("DRY-RUN", "Would POST scenario and defaults to /run endpoint")
	r.output("DRY-RUN", "Would poll /status every 2 seconds")
	r.output("DRY-RUN", "Would poll /metrics every 500ms")
	r.output("DRY-RUN", "Would POST /shutdown for graceful node termination")

	r.output("COMPLETE", "Dry run completed")
}

func (r *HeadlessRunner) outputDryRunContentIdentity(label string, content []byte) {
	digest := sha256.Sum256(content)
	r.output("DRY-RUN", "%s: bytes=%d sha256=%x", label, len(content), digest)
}

// runBenchmark removed as unused (dead code); use runBenchmarkWithParams instead

// runBenchmarkWithParams executes the actual benchmark with scenario parameters
func (r *HeadlessRunner) runBenchmarkWithParams(ctx context.Context, image string, scenarioPath string, params scenario.Params) error {
	return r.runBenchmark(ctx, scenarioPath, func(orchestrator *tui.TestOrchestrator) error {
		return orchestrator.StartTestWithLaunchHooks(ctx, image, params, r.launchHooks)
	})
}

func (r *HeadlessRunner) runBenchmarkWithContent(ctx context.Context, image string, scenarioPath string, params scenario.Params, scenarioContent, defaultsContent []byte) error {
	return r.runBenchmark(ctx, scenarioPath, func(orchestrator *tui.TestOrchestrator) error {
		return orchestrator.StartTestWithContentAndLaunchHooks(
			ctx, image, params, scenarioContent, defaultsContent, r.launchHooks)
	})
}

func (r *HeadlessRunner) runBenchmark(ctx context.Context, scenarioPath string, startTest func(*tui.TestOrchestrator) error) error {
	// Create the orchestrator for API-based control
	r.orchestrator = tui.NewTestOrchestrator(r.dockerManager, r.apiPort, r.resultsRoot)
	r.orchestrator.SetNetworkMode(r.networkMode)
	if err := r.launchHooks.RegisterResourceFinalizer(
		r.orchestrator.FinalizeDiagnosticsAndCleanupOutcome); err != nil {
		return fmt.Errorf("register session resource finalizer: %w", err)
	}

	// Set up callbacks to handle orchestrator events
	r.orchestrator.SetCallbacks(
		// Status updates
		func(status *tui.TestStatus) {
			if status != nil {
				r.output("STATUS", "Test %s - %s", status.State, status.Message)
			}
		},
		// Metrics updates from API
		func(update *tui.MultiNodeMetricsUpdate) {
			if update != nil && update.Aggregated != nil {
				metric := update.Aggregated
				if r.jsonMode {
					r.outputMetricsJSON(*metric)
				} else {
					r.outputMetrics(*metric)
				}

				// Show per-op breakdown when multiple op types are present
				if len(update.PerOpType) > 1 {
					for _, opMetric := range update.PerOpType {
						if r.jsonMode {
							r.outputMetricsJSON(*opMetric)
						} else {
							r.outputMetrics(*opMetric)
						}
					}
				}

				// Log successful parsing
				logging.LogMetricsParsing("received API metrics in headless mode",
					"success", true,
					"ops_per_sec", metric.OpsPerSec,
					"source", "multi-node-api")
			}
		},
		// Container output (display only; metrics are sourced from JSON endpoint)
		func(line string) {
			if !r.metricsOnly {
				r.output("SPT", "%s", line)
			}
		},
		// Error messages
		func(err string) {
			r.output("ERROR", "%s", err)
		},
	)

	// Start the test via orchestrator
	r.output("INIT", "Starting Spt in API mode...")

	if err := startTest(r.orchestrator); err != nil {
		r.output("ERROR", "Failed to start test: %v", err)
		// Try to clean up any partially created resources
		if stopErr := stopLocalAfterLaunchError(r.orchestrator, r.launchHooks); stopErr != nil {
			r.output("ERROR", "Failed to cleanup after start failure: %v", stopErr)
		} else {
			r.output("CLEANUP", "Cleaned up after start failure")
		}
		return err
	}

	r.output("API", "Test started successfully via API")

	// Wait for context cancellation or natural test completion (whichever comes first).
	// Previously this was a bare <-ctx.Done(), which meant RunWithParams blocked for the
	// full auto-terminate duration even after the engine finished all scenario steps and
	// monitorStatus detected COMPLETED/FAILED.
	select {
	case <-ctx.Done():
	case <-r.orchestrator.CompletionCh():
	case <-r.launchHooks.WorkloadTerminal():
	}
	completionErr := r.orchestrator.CompletionError()
	if r.launchHooks.SessionManaged() {
		r.output("COMPLETE", "Presentation completed; RunSession owns evidence and resource finalization")
		if completionErr != nil {
			return completionErr
		}
		return ctx.Err()
	}

	// Stop the test gracefully
	r.output("CLEANUP", "Stopping test via API...")
	if err := r.orchestrator.StopTest(); err != nil {
		r.output("ERROR", "Failed to stop test cleanly: %v", err)
	} else {
		r.output("CLEANUP", "Test stopped successfully")
	}

	// Clean up input scenario file unless keeping it
	if !r.keepScenario && scenarioPath != "" {
		if err := os.Remove(scenarioPath); err != nil {
			r.output("WARNING", "Failed to remove input scenario file: %v", err)
		} else {
			r.output("CLEANUP", "Removed input scenario file: %s", scenarioPath)
		}
	} else if r.keepScenario {
		r.output("INFO", "Keeping input scenario file: %s", scenarioPath)
	}

	if completionErr != nil {
		r.output("ERROR", "Benchmark failed: %v", completionErr)
		return completionErr
	}
	r.output("COMPLETE", "Benchmark completed")
	return nil
}

func stopLocalAfterLaunchError(orchestrator *tui.TestOrchestrator, hooks tui.LaunchHooks) error {
	if hooks.SessionManaged() || orchestrator == nil {
		return nil
	}
	return orchestrator.StopTest()
}

// output writes a formatted message to both console and trace file
func (r *HeadlessRunner) output(category, format string, args ...interface{}) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	message := fmt.Sprintf(format, args...)

	line := fmt.Sprintf("[%s] [%s] %s", timestamp, category, message)

	// Write to console
	fmt.Println(line)

	// Write to trace file if available
	if r.traceFile != nil {
		if _, err := fmt.Fprintln(r.traceFile, line); err != nil { // #nosec G705 -- writing to local trace file, not HTTP response
			fmt.Fprintf(os.Stderr, "Failed to write to trace file: %v\n", err)
		}
	}
}

// outputMetrics outputs parsed metrics in human-readable format
func (r *HeadlessRunner) outputMetrics(metric tui.PerformanceMetric) {
	r.output("METRICS", "%s", formatMetricsMessage(metric))
}

func formatMetricsMessage(metric tui.PerformanceMetric) string {
	if metric.Delete != nil {
		deleteMetrics := metric.Delete
		latency := formatOptionalMicros(deleteTimingMicros(deleteMetrics.Timing.Latency))
		return fmt.Sprintf(
			"ops/sec=%d latency=%s type=%s success=%d failed=%d partial=%t nodes=%d nodes_present=%s contributors_present=%s units=requests:%s,objects:%s,batches:%s requests=%d full_success=%d partial_requests=%d failed_requests=%d unresolved_requests=%d request_rate=%.3f selected=%d attempted_objects=%d accepted=%d failed_objects=%d unattempted_objects=%d unresolved_objects=%d object_rate=%.3f batches=%d batch_objects=%d mean_batch=%.3f full_batches=%d partial_batches=%d full_batch_pct=%.3f current_keys=%d exact_versions=%d request_completion_pct=%.3f object_completion_pct=%.3f mode=%s batch_size=%d selection_order=%s policy=%s failure_budget_outcome=%s max_failed_objects=%d max_failure_pct=%.3f grace_seconds=%d operational_failed_objects=%d excluded_failed_objects=%d observed_failure_pct=%.3f phases=%s buckets=%s latency_definition=%q latency_stats=%s duration_definition=%q duration_stats=%s object_latency=N/A object_size=N/A data_moved=N/A bandwidth=N/A ttfb=N/A outcome_terminology=%s terminal_reconciled=%t verification=%s",
			metric.OpsPerSec, latency, metric.OpType, metric.SuccessCount, metric.FailedCount,
			metric.Partial, metric.NodesCount, strings.Join(metric.NodesPresent, ","),
			strings.Join(metric.ContributorsPresent, ","),
			deleteMetrics.Units.Requests, deleteMetrics.Units.Objects, deleteMetrics.Units.Batches,
			deleteMetrics.Requests.Attempted, deleteMetrics.Requests.FullSuccess, deleteMetrics.Requests.Partial,
			deleteMetrics.Requests.Failed, deleteMetrics.Requests.Unresolved, deleteMetrics.Requests.PerSecond,
			deleteMetrics.Objects.Selected, deleteMetrics.Objects.Attempted, deleteMetrics.Objects.Accepted,
			deleteMetrics.Objects.Failed, deleteMetrics.Objects.Unattempted,
			deleteMetrics.Objects.Unresolved, deleteMetrics.Objects.PerSecond,
			deleteMetrics.Batches.ActualRequestCount, deleteMetrics.Batches.ActualObjectCount,
			deleteMetrics.Batches.MeanObjectsPerRequest, deleteMetrics.Batches.FullBatchCount,
			deleteMetrics.Batches.PartialBatchCount, deleteMetrics.Batches.FullBatchPercent,
			deleteMetrics.Versions.CurrentKey, deleteMetrics.Versions.ExactVersion,
			deleteMetrics.Completion.RequestPercent, deleteMetrics.Completion.ObjectPercent,
			deleteMetrics.Identity.Mode, deleteMetrics.Identity.ConfiguredBatchSize,
			deleteMetrics.Identity.SelectionOrder,
			deleteMetrics.FailurePolicy.Mode, deleteMetrics.FailurePolicy.Outcome,
			deleteMetrics.FailurePolicy.MaxFailedObjects,
			deleteMetrics.FailurePolicy.MaxFailurePercent, deleteMetrics.FailurePolicy.GraceSeconds,
			deleteMetrics.FailurePolicy.OperationalFailedObjects,
			deleteMetrics.FailurePolicy.ExcludedFailedObjects,
			deleteMetrics.FailurePolicy.ObservedFailurePercent,
			formatDeletePhases(deleteMetrics.Phases), formatDeleteBuckets(deleteMetrics.Buckets),
			deleteMetrics.Timing.LatencyDefinition, formatDeleteTimingStat(deleteMetrics.Timing.Latency),
			deleteMetrics.Timing.DurationDefinition, formatDeleteTimingStat(deleteMetrics.Timing.Duration),
			deleteMetrics.OutcomeTerminology, deleteMetrics.TerminalReconciled,
			deleteMetrics.Verification.Notice)
	}
	format := "ops/sec=%d latency=%dµs type=%s success=%d concurrency=%.1f"
	args := []interface{}{metric.OpsPerSec, metric.MeanLatency, metric.OpType, metric.SuccessCount, metric.ConcurrencyMean}
	if metric.HasTTFB {
		format = "ops/sec=%d latency=%dµs ttfb=%dµs type=%s success=%d concurrency=%.1f"
		args = []interface{}{metric.OpsPerSec, metric.MeanLatency, metric.MeanTTFB, metric.OpType, metric.SuccessCount, metric.ConcurrencyMean}
	}
	return fmt.Sprintf(format, args...)
}

func deleteTimingMicros(stat *tui.JSONTimingStat) *int64 {
	if stat == nil || stat.Count <= 0 {
		return nil
	}
	value := stat.P50Us
	return &value
}

func formatOptionalMicros(value *int64) string {
	if value == nil {
		return notAvailableDisplay
	}
	return fmt.Sprintf("%dµs", *value)
}

func formatDeleteTimingStat(stat *tui.JSONTimingStat) string {
	if stat == nil || stat.Count == 0 {
		return notAvailableDisplay
	}
	return fmt.Sprintf("count:%d,mean_us:%.3f,min_us:%d,p50_us:%d,p90_us:%d,p99_us:%d,p999_us:%d,max_us:%d,overflow:%d",
		stat.Count, stat.MeanUs, stat.MinUs, stat.P50Us, stat.P90Us, stat.P99Us,
		stat.P999Us, stat.MaxUs, stat.OverflowCount)
}

func formatDeletePhases(phases tui.DeletePhaseMetrics) string {
	return fmt.Sprintf(
		"seed:%s,discovery:%s,pre_validation:%s,scheduled_delete:%s,drain:%s,post_verification:%s,cleanup:%s,total_wall:%s",
		formatOptionalSeconds(phases.SeedSeconds), formatOptionalSeconds(phases.DiscoverySeconds),
		formatOptionalSeconds(phases.PreValidationSeconds), formatOptionalSeconds(phases.ScheduledDeleteSeconds),
		formatOptionalSeconds(phases.DrainSeconds), formatOptionalSeconds(phases.PostVerificationSeconds),
		formatOptionalSeconds(phases.CleanupSeconds), formatOptionalSeconds(phases.TotalWallSeconds))
}

func formatOptionalSeconds(value *float64) string {
	if value == nil {
		return notAvailableDisplay
	}
	return fmt.Sprintf("%.3fs", *value)
}

func formatDeleteBuckets(buckets []tui.DeleteBucketMetrics) string {
	if len(buckets) == 0 {
		return "[]"
	}
	parts := make([]string, 0, len(buckets))
	for _, bucket := range buckets {
		parts = append(parts, fmt.Sprintf("%s(selected:%d,attempted:%d,accepted:%d,failed:%d)",
			bucket.Bucket, bucket.Selected, bucket.Attempted, bucket.Accepted, bucket.Failed))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

// outputMetricsJSON outputs parsed metrics in JSON format
func (r *HeadlessRunner) outputMetricsJSON(metric tui.PerformanceMetric) {
	encoded, err := marshalMetricsJSON(metric, "", "")
	if err != nil {
		r.output("METRICS", "failed to encode metrics JSON: %v", err)
		return
	}
	r.outputJSONLine(encoded)
}

func marshalMetricsJSON(metric tui.PerformanceMetric, view, contributor string) ([]byte, error) {
	timestamp := time.Now().Format("2006-01-02T15:04:05.000Z")
	var ttfb *int64
	if metric.HasTTFB {
		value := metric.MeanTTFB
		ttfb = &value
	}
	payload := struct {
		Timestamp   string `json:"timestamp"`
		Type        string `json:"type"`
		View        string `json:"view,omitempty"`
		Contributor string `json:"contributor,omitempty"`
		Operation   string `json:"operation,omitempty"`
		Data        struct {
			OpsPerSec           int64              `json:"ops_per_sec"`
			LatencyUS           *int64             `json:"latency_us"`
			TTFBUS              *int64             `json:"ttfb_us,omitempty"`
			Operation           string             `json:"operation_type"`
			SuccessCount        int64              `json:"success_count"`
			FailedCount         int64              `json:"failed_count"`
			Concurrency         float64            `json:"concurrency"`
			Partial             bool               `json:"partial"`
			NodesCount          int                `json:"nodes_count"`
			NodesPresent        []string           `json:"nodes_present"`
			ContributorsPresent []string           `json:"contributors_present"`
			Delete              *tui.DeleteMetrics `json:"delete,omitempty"`
		} `json:"data"`
	}{Timestamp: timestamp, Type: "metrics", View: view}
	if view == "operation" {
		payload.Operation = contributor
	} else {
		payload.Contributor = contributor
	}
	payload.Data.OpsPerSec = metric.OpsPerSec
	latency := metric.MeanLatency
	payload.Data.LatencyUS = &latency
	if metric.Delete != nil {
		payload.Data.LatencyUS = deleteTimingMicros(metric.Delete.Timing.Latency)
	}
	payload.Data.TTFBUS = ttfb
	payload.Data.Operation = metric.OpType
	payload.Data.SuccessCount = metric.SuccessCount
	payload.Data.FailedCount = metric.FailedCount
	payload.Data.Concurrency = metric.ConcurrencyMean
	payload.Data.Partial = metric.Partial
	payload.Data.NodesCount = metric.NodesCount
	payload.Data.NodesPresent = append([]string(nil), metric.NodesPresent...)
	payload.Data.ContributorsPresent = append([]string(nil), metric.ContributorsPresent...)
	payload.Data.Delete = metric.Delete
	return json.Marshal(payload)
}

func (r *HeadlessRunner) outputJSONLine(encoded []byte) {
	jsonLine := string(encoded)
	fmt.Println(jsonLine)

	if r.traceFile != nil {
		_, _ = fmt.Fprintln(r.traceFile, jsonLine) //nolint:errcheck
	}
}

// writeTraceHeader writes the trace file header with metadata
func (r *HeadlessRunner) writeTraceHeader(filename string) {
	if r.traceFile == nil {
		return
	}

	command := cmdline.FormatForArtifact(os.Args)
	if command == "" {
		command = "<unknown>"
	}
	header := fmt.Sprintf(`# Trace file: %s
# Generated: %s
# Command: %s
# System: %s/%s
#
`, filename, time.Now().Format("2006-01-02 15:04:05"), command, runtime.GOOS, runtime.GOARCH)

	_, _ = r.traceFile.WriteString(header) //nolint:errcheck
}

// Close cleans up resources
func (r *HeadlessRunner) Close() error {
	if r.traceFile != nil {
		if err := r.traceFile.Close(); err != nil {
			return fmt.Errorf("failed to close trace file: %w", err)
		}
	}
	return nil
}

// StartHeadlessMode is the public entry point for headless mode execution
func StartHeadlessMode(image string, scenarioPath string, options HeadlessOptions) error {
	// Create empty params for backward compatibility - endpoint args will be empty
	params := scenario.Params{}
	return StartHeadlessModeWithParams(image, scenarioPath, params, options)
}

// StartHeadlessModeWithParams is the entry point for headless mode with scenario parameters
func StartHeadlessModeWithParams(image string, scenarioPath string, params scenario.Params, options HeadlessOptions) error {
	return startHeadlessModeWithParams(
		image, scenarioPath, params, options, options.ScenarioContent, options.DefaultsContent)
}

// StartHeadlessModeWithScenarioContentAndParams runs headless mode with caller-provided scenario/defaults content.
func StartHeadlessModeWithScenarioContentAndParams(image string, scenarioPath string, params scenario.Params, options HeadlessOptions, scenarioContent, defaultsContent []byte) error {
	return startHeadlessModeWithParams(image, scenarioPath, params, options, scenarioContent, defaultsContent)
}

func startHeadlessModeWithParams(image string, scenarioPath string, params scenario.Params, options HeadlessOptions, scenarioContent, defaultsContent []byte) error {
	// Create Docker manager
	dockerManager, err := tui.NewDockerManager()
	if err != nil {
		return fmt.Errorf("failed to create Docker manager: %w", err)
	}
	defer dockerManager.Close()

	// Create headless runner
	runner, err := NewHeadlessRunner(dockerManager, options)
	if err != nil {
		return fmt.Errorf("failed to create headless runner: %w", err)
	}
	defer func() { _ = runner.Close() }()

	// Run the benchmark with optional auto-terminate timeout
	baseCtx := options.Context
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	if options.AutoTerminateSeconds > 0 {
		tctx, cancel := context.WithTimeout(baseCtx, time.Duration(options.AutoTerminateSeconds)*time.Second)
		defer cancel()
		if scenarioContent != nil {
			return runner.RunWithScenarioContentAndParams(tctx, image, scenarioPath, params, scenarioContent, defaultsContent)
		}
		return runner.RunWithParams(tctx, image, scenarioPath, params)
	}
	if scenarioContent != nil {
		return runner.RunWithScenarioContentAndParams(baseCtx, image, scenarioPath, params, scenarioContent, defaultsContent)
	}
	return runner.RunWithParams(baseCtx, image, scenarioPath, params)
}

// StartHeadlessModeWithOrchestrator is the entry point for multi-host headless mode
func StartHeadlessModeWithOrchestrator(orchestrator *tui.MultiHostOrchestrator, image string, scenarioPath string, params scenario.Params, options HeadlessOptions) error {
	return startHeadlessModeWithOrchestrator(
		orchestrator, image, scenarioPath, params, options, options.ScenarioContent, options.DefaultsContent)
}

// StartHeadlessModeWithOrchestratorContent is the entry point for multi-host
// headless mode using caller-provided scenario/defaults content.
func StartHeadlessModeWithOrchestratorContent(orchestrator *tui.MultiHostOrchestrator, image string, scenarioPath string, params scenario.Params, options HeadlessOptions, scenarioContent, defaultsContent []byte) error {
	return startHeadlessModeWithOrchestrator(orchestrator, image, scenarioPath, params, options, scenarioContent, defaultsContent)
}

func startHeadlessModeWithOrchestrator(orchestrator *tui.MultiHostOrchestrator, image string, scenarioPath string, params scenario.Params, options HeadlessOptions, scenarioContent, defaultsContent []byte) error {
	// Create a multi-host headless runner
	runner, err := NewMultiHostHeadlessRunner(orchestrator, options)
	if err != nil {
		return fmt.Errorf("failed to create multi-host headless runner: %w", err)
	}
	defer func() { _ = runner.Close() }()

	// Run the multi-host benchmark with optional auto-terminate timeout
	baseCtx := options.Context
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	if options.AutoTerminateSeconds > 0 {
		tctx, cancel := context.WithTimeout(baseCtx, time.Duration(options.AutoTerminateSeconds)*time.Second)
		defer cancel()
		if scenarioContent != nil {
			return runner.RunWithScenarioContentAndParams(tctx, image, scenarioPath, params, scenarioContent, defaultsContent)
		}
		return runner.RunWithParams(tctx, image, scenarioPath, params)
	}
	if scenarioContent != nil {
		return runner.RunWithScenarioContentAndParams(baseCtx, image, scenarioPath, params, scenarioContent, defaultsContent)
	}
	return runner.RunWithParams(baseCtx, image, scenarioPath, params)
}
