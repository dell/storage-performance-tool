/*
Copyright © 2025 Dell Technologies
*/

// Package headless provides non-interactive execution flows for running
// Spt via Docker without the TUI.
package headless

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
)

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
}

// HeadlessOptions holds configuration for headless mode
//
//revive:disable-next-line:exported
type HeadlessOptions struct {
	TraceFile   string
	TraceAppend bool
	Verbose     bool
	APIPort     string
	JSONMode    bool
	MetricsOnly bool
	DryRun      bool
	// KeepScenario removed - this belongs in scenario.Params
	AutoTerminateSeconds int // 0 = unlimited
}

// NewHeadlessRunner creates a new headless runner
func NewHeadlessRunner(dockerManager tui.DockerInterface, options HeadlessOptions) (*HeadlessRunner, error) {
	// Use default port if not specified
	apiPort := options.APIPort
	if apiPort == "" {
		apiPort = constants.SptAPIPort
	}

	runner := &HeadlessRunner{
		dockerManager: dockerManager,
		verbose:       options.Verbose,
		jsonMode:      options.JSONMode,
		metricsOnly:   options.MetricsOnly,
		dryRun:        options.DryRun,
		apiPort:       apiPort,
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
	orchestrator *tui.MultiHostOrchestrator
	traceFile    *os.File
	verbose      bool
}

// NewMultiHostHeadlessRunner creates a new multi-host headless runner
func NewMultiHostHeadlessRunner(orchestrator *tui.MultiHostOrchestrator, options HeadlessOptions) (*MultiHostHeadlessRunner, error) {
	runner := &MultiHostHeadlessRunner{
		orchestrator: orchestrator,
		verbose:      options.Verbose,
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

// RunWithParams executes the multi-host benchmark in headless mode
func (r *MultiHostHeadlessRunner) RunWithParams(ctx context.Context, image string, scenarioPath string, params scenario.Params) error {
	r.output("INIT", fmt.Sprintf("Starting multi-host headless benchmark on %d hosts", r.orchestrator.GetHostCount()))
	r.output("INIT", fmt.Sprintf("Image: %s", image))
	r.output("INIT", fmt.Sprintf("Scenario: %s", scenarioPath))

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start the distributed test on all hosts via orchestrator wrapper
	testOrchestrator := tui.NewMultiHostTestOrchestrator(r.orchestrator)
	// Standardize progress output in headless mode as well
	r.orchestrator.SetNotifier(func(msg string) {
		r.output("spt", msg)
	})
	// In headless mode, ensure entry-node relay and progress messages are echoed.
	// Route message sink lines (entry logs and [spt] messages) to stdout/trace.
	testOrchestrator.SetMessageSink(func(msg string) {
		r.output("spt", msg)
	})
	err := testOrchestrator.StartTest(ctx, image, params)
	if err != nil {
		r.output("ERROR", fmt.Sprintf("Failed to start test: %v", err))
		if stopErr := r.orchestrator.StopAllContainers(ctx); stopErr != nil {
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
		case sig := <-sigChan:
			r.output("SIGNAL", fmt.Sprintf("Received signal: %v", sig))
			done <- fmt.Errorf("interrupted by signal: %v", sig)
		}
	}()

	// Wait for completion or interruption
	err = <-done
	if err != nil {
		r.output("SHUTDOWN", fmt.Sprintf("Shutting down due to: %v", err))
	}

	// Stop all containers
	r.output("SHUTDOWN", "Stopping all containers...")
	if stopErr := r.orchestrator.StopAllContainers(ctx); stopErr != nil {
		r.output("ERROR", fmt.Sprintf("Failed to stop containers: %v", stopErr))
		if err == nil {
			err = stopErr
		}
	} else {
		r.output("SHUTDOWN", "All containers stopped successfully")
	}

	return err
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

// writeTraceHeader writes the trace file header
func (r *MultiHostHeadlessRunner) writeTraceHeader(filename string, hostCount int) {
	header := fmt.Sprintf("Multi-Host Headless Mode Trace - %d hosts\n", hostCount)
	header += fmt.Sprintf("Started: %s\n", time.Now().Format("2006-01-02 15:04:05"))
	header += fmt.Sprintf("Runtime: %s %s\n", runtime.GOOS, runtime.GOARCH)
	header += fmt.Sprintf("Trace File: %s\n", filename)
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
		return r.runDryModeWithParams(image, scenarioPath, params)
	}

	// Start the actual benchmark
	return r.runBenchmarkWithParams(runCtx, image, scenarioPath, params)
}

// runDryMode removed as unused (dead code); use runDryModeWithParams instead

// runDryModeWithParams shows what would be executed without running, with scenario parameters
func (r *HeadlessRunner) runDryModeWithParams(image string, scenarioPath string, params scenario.Params) error {
	r.output("DRY-RUN", "Would execute benchmark with API mode:")
	r.output("DRY-RUN", "  Image: %s", image)
	r.output("DRY-RUN", "  Input scenario: %s", scenarioPath)
	r.output("DRY-RUN", "  API Port: %s", r.apiPort)

	// Generate and display scenario content
	scenarioContent, err := scenario.GenerateScenario(params)
	if err != nil {
		r.output("ERROR", "Failed to generate scenario: %v", err)
		return err
	}

	r.output("DRY-RUN", "Generated scenario content:")
	r.output("DRY-RUN", "%s", scenarioContent)

	// Generate and display defaults configuration
	defaultsContent, err := scenario.GenerateDefaults(params)
	if err != nil {
		r.output("ERROR", "Failed to generate defaults: %v", err)
		return err
	}

	if defaultsContent != nil {
		r.output("DRY-RUN", "Defaults configuration:")
		r.output("DRY-RUN", "%s", string(defaultsContent))
	} else {
		r.output("DRY-RUN", "Defaults configuration: (none - mock workload)")
	}

	r.output("DRY-RUN", "Would start container with --run-node")
	r.output("DRY-RUN", "Would wait for API to be ready on http://localhost:%s", r.apiPort)
	r.output("DRY-RUN", "Would POST scenario and defaults to /run endpoint")
	r.output("DRY-RUN", "Would poll /status every 2 seconds")
	r.output("DRY-RUN", "Would poll /metrics every 500ms")
	r.output("DRY-RUN", "Would POST /shutdown for graceful node termination")

	r.output("COMPLETE", "Dry run completed")
	return nil
}

// runBenchmark removed as unused (dead code); use runBenchmarkWithParams instead

// runBenchmarkWithParams executes the actual benchmark with scenario parameters
func (r *HeadlessRunner) runBenchmarkWithParams(ctx context.Context, image string, scenarioPath string, params scenario.Params) error {
	// Create the orchestrator for API-based control
	r.orchestrator = tui.NewTestOrchestrator(r.dockerManager, r.apiPort)

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

	if err := r.orchestrator.StartTest(ctx, image, params); err != nil {
		r.output("ERROR", "Failed to start test: %v", err)
		// Try to clean up any partially created resources
		if stopErr := r.orchestrator.StopTest(); stopErr != nil {
			r.output("ERROR", "Failed to cleanup after start failure: %v", stopErr)
		} else {
			r.output("CLEANUP", "Cleaned up after start failure")
		}
		return err
	}

	r.output("API", "Test started successfully via API")

	// Wait for context cancellation or test completion
	<-ctx.Done()

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

	r.output("COMPLETE", "Benchmark completed")
	return nil
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
	r.output("METRICS", "ops/sec=%d latency=%dµs type=%s success=%d concurrency=%.1f",
		metric.OpsPerSec,
		metric.MeanLatency,
		metric.OpType,
		metric.SuccessCount,
		metric.ConcurrencyMean,
	)
}

// outputMetricsJSON outputs parsed metrics in JSON format
func (r *HeadlessRunner) outputMetricsJSON(metric tui.PerformanceMetric) {
	timestamp := time.Now().Format("2006-01-02T15:04:05.000Z")
	jsonLine := fmt.Sprintf(`{"timestamp":"%s","type":"metrics","data":{"ops_per_sec":%d,"latency_us":%d,"operation_type":"%s","success_count":%d,"failed_count":%d,"concurrency":%.1f}}`,
		timestamp,
		metric.OpsPerSec,
		metric.MeanLatency,
		metric.OpType,
		metric.SuccessCount,
		metric.FailedCount,
		metric.ConcurrencyMean,
	)

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

	header := fmt.Sprintf(`# Trace file: %s
# Generated: %s
# System: %s/%s
#
`, filename, time.Now().Format("2006-01-02 15:04:05"), runtime.GOOS, runtime.GOARCH)

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
	baseCtx := context.Background()
	if options.AutoTerminateSeconds > 0 {
		tctx, cancel := context.WithTimeout(baseCtx, time.Duration(options.AutoTerminateSeconds)*time.Second)
		defer cancel()
		return runner.RunWithParams(tctx, image, scenarioPath, params)
	}
	return runner.RunWithParams(baseCtx, image, scenarioPath, params)
}

// StartHeadlessModeWithOrchestrator is the entry point for multi-host headless mode
func StartHeadlessModeWithOrchestrator(orchestrator *tui.MultiHostOrchestrator, image string, scenarioPath string, params scenario.Params, options HeadlessOptions) error {
	// Create a multi-host headless runner
	runner, err := NewMultiHostHeadlessRunner(orchestrator, options)
	if err != nil {
		return fmt.Errorf("failed to create multi-host headless runner: %w", err)
	}
	defer func() { _ = runner.Close() }()

	// Run the multi-host benchmark with optional auto-terminate timeout
	baseCtx := context.Background()
	if options.AutoTerminateSeconds > 0 {
		tctx, cancel := context.WithTimeout(baseCtx, time.Duration(options.AutoTerminateSeconds)*time.Second)
		defer cancel()
		return runner.RunWithParams(tctx, image, scenarioPath, params)
	}
	return runner.RunWithParams(baseCtx, image, scenarioPath, params)
}
