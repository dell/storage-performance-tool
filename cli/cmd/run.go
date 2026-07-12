/*
Copyright © 2025 Dell Technologies
*/
//revive:disable:package-comments
package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/config"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/internal/sizeparse"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
)

const (
	flagSkipImagePull         = "skip-image-pull"
	flagSptImage              = "spt-image"
	flagAttachExistingWorkers = "attach-existing"
	flagReadShuffle           = "shuffle"
	flagReadShuffleBatchSize  = "shuffle-batch-size"
	flagEngineOverride        = "engine-override"
)

// resolvePortConflictFunc is a test seam for port conflict resolution.
// In production it points to portcheck.ResolvePortConflict; tests can override.
var resolvePortConflictFunc = portcheck.ResolvePortConflict

type autoResultsRunTracker interface {
	WaitForCompletion(context.Context, []string) (*portcheck.RunResult, error)
	SetDebug(bool)
}

type runTrackerAdapter struct {
	*portcheck.RunTracker
}

func (r *runTrackerAdapter) SetDebug(debug bool) {
	r.Debug = debug
}

type autoResultsFetcher interface {
	FetchArtifactsForSteps(context.Context, []string) (*results.Manifest, error)
}

type fetcherAdapter struct {
	*results.Fetcher
}

func (f *fetcherAdapter) FetchArtifactsForSteps(ctx context.Context, stepIDs []string) (*results.Manifest, error) {
	return f.Fetcher.FetchArtifactsForSteps(ctx, stepIDs)
}

var (
	newRunTrackerFunc = func(baseURL string) autoResultsRunTracker {
		return &runTrackerAdapter{portcheck.NewRunTracker(baseURL)}
	}
	// connectMultiHostOrchestratorFunc is a test seam for the command's host
	// orchestration boundary. It keeps routing tests independent of Docker and
	// SSH while production continues to use the concrete orchestrator.
	connectMultiHostOrchestratorFunc = func(ctx context.Context, orchestrator *tui.MultiHostOrchestrator) error {
		return orchestrator.ConnectHosts(ctx)
	}
	discoverStepIDsFunc      = results.DiscoverStepIDs
	discoverFleetStepIDsFunc = results.DiscoverFleetStepIDs
	newResultsFetcherFunc    = func(baseURL, outputDir string) autoResultsFetcher {
		return &fetcherAdapter{results.NewFetcher(baseURL, outputDir)}
	}
	generateRunSummaryFunc = generateRunSummary
	requestShutdownAllFunc = requestShutdownAll
)

// shouldRunHeadless determines if the application should run in headless mode
func shouldRunHeadless(cmd *cobra.Command) bool {
	// Check if explicitly requested
	if headlessFlag, _ := cmd.Flags().GetBool("headless"); headlessFlag {
		return true
	}

	// Auto-detect TTY availability
	if _, err := os.OpenFile("/dev/tty", os.O_RDWR, 0); err != nil {
		fmt.Fprintln(os.Stderr, "[INFO] No TTY detected, running in headless mode")
		return true
	}

	return false
}

// getAPIPort gets the API port from the command flags with default fallback
func getAPIPort(cmd *cobra.Command) string {
	apiPort, _ := cmd.Flags().GetString("api-port")
	if apiPort == "" {
		apiPort = constants.SptAPIPort // Default to Spt standard port
	}

	// Log deprecation notice if user specifies legacy port
	if apiPort == constants.SptLegacyAPIPort {
		fmt.Printf("Note: spt now defaults to port %s (Spt standard). Using legacy port %s.\n", constants.SptAPIPort, constants.SptLegacyAPIPort)
	}

	return apiPort
}

// shouldUseMultiHostOrchestrator selects the path which manages Docker on the
// configured hosts. A lone remote host must use this path too: the single-host
// runner owns Docker on the CLI/controller host and would otherwise ignore the
// remote HostInfo entirely.
func shouldUseMultiHostOrchestrator(hosts []*hostparse.HostInfo) bool {
	if len(hosts) > 1 {
		return true
	}
	return len(hosts) == 1 && hosts[0] != nil && !hosts[0].IsLocal
}

// deriveBaseURL builds the Spt API base URL for results retrieval.
func deriveBaseURL(apiPort string, hosts []*hostparse.HostInfo) string {
	if len(hosts) == 0 || (len(hosts) == 1 && (hosts[0] == nil || hosts[0].IsLocal)) {
		return fmt.Sprintf("http://localhost:%s", apiPort)
	}
	// The first host is the entry node for both remote single-host and
	// distributed runs.
	entry := hosts[0]
	return fmt.Sprintf("http://%s:%s", entry.Host, apiPort)
}

// startAutoResults kicks off background completion tracking and artifact fetching.
// After successful fetch, optionally requests /shutdown across all hosts and waits for API linger.
// preSummaryHook, if non-nil, runs after shutdown completes but before the run
// summary is generated — see its call site below for why that ordering matters.
func startAutoResults(baseURL, label, resultsDir string, expectedStepIDs []string, debug bool, allHosts []*hostparse.HostInfo, apiPort string, shutdownOn bool, lingerSec int, scenarioPath string, metadata *runMetadata, progressOut io.Writer, summaryOut io.Writer, traceFile string, preSummaryHook func()) chan struct{} {
	done := make(chan struct{}, 1)
	go func() {
		defer func() { done <- struct{}{} }()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		if progressOut == nil {
			progressOut = os.Stdout
		}
		writeProgress := func(format string, args ...interface{}) {
			if progressOut == nil {
				return
			}
			_, _ = fmt.Fprintf(progressOut, format, args...)
		}
		writeProgress("Auto-results: monitoring %s for completion...\n", baseURL)
		// Precompute output root and announce
		root := ""
		if metadata != nil && metadata.ResultsRoot != "" {
			root = metadata.ResultsRoot
		} else {
			ts := time.Now().UTC().Format("20060102.150405.000")
			root = filepath.Join(resultsDir, fmt.Sprintf("%s-%s", label, ts))
		}
		writeProgress("Auto-results: will save results under %s\n", root)
		if metadata != nil {
			metadata.ResultsRoot = root
			metadata.BaseURL = baseURL
		}
		// ensure results directory exists early so we can stage metadata
		if err := os.MkdirAll(root, 0o750); err != nil {
			logging.LogError("auto-results", "create results dir", err, "path", root)
		} else {
			if storedPath, copyErr := copyScenarioForResults(scenarioPath, root); copyErr != nil {
				logging.LogError("auto-results", "copy scenario", copyErr, "src", scenarioPath, "dest", root)
			} else if metadata != nil {
				metadata.ScenarioStoredPath = storedPath
			}
		}
		tracker := newRunTrackerFunc(baseURL)
		// Wait for terminal run state; step IDs discovered later via metrics/json
		if len(expectedStepIDs) > 0 {
			writeProgress("Auto-results: expecting %d step(s)\n", len(expectedStepIDs))
		}
		tracker.SetDebug(debug)
		// While we wait, continuously discover step IDs so we have exact IDs on completion
		var discovered []string
		var fleetDiscovered []string
		var mu sync.Mutex
		seen := make(map[string]struct{})
		fleetSeen := make(map[string]struct{})
		stopCh := make(chan struct{})
		var pollWG sync.WaitGroup
		pollWG.Add(1)
		go func() {
			defer pollWG.Done()
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopCh:
					return
				case <-ticker.C:
					if ids, err := discoverStepIDsFunc(baseURL); err == nil && len(ids) > 0 {
						mu.Lock()
						updated := false
						for _, id := range ids {
							if id == "" {
								continue
							}
							if _, ok := seen[id]; ok {
								continue
							}
							seen[id] = struct{}{}
							discovered = append(discovered, id)
							updated = true
						}
						snapshot := append([]string(nil), discovered...)
						mu.Unlock()
						if debug && updated && len(snapshot) > 0 {
							logging.LogDebug("auto-results", "discover", "steps", strings.Join(snapshot, ","))
						}
					}
					// Also poll fleet endpoint for distributed step IDs
					if fids, err := discoverFleetStepIDsFunc(baseURL); err == nil && len(fids) > 0 {
						mu.Lock()
						updated := false
						for _, id := range fids {
							if id == "" {
								continue
							}
							if _, ok := fleetSeen[id]; ok {
								continue
							}
							fleetSeen[id] = struct{}{}
							fleetDiscovered = append(fleetDiscovered, id)
							updated = true
						}
						snapshot := append([]string(nil), fleetDiscovered...)
						mu.Unlock()
						if debug && updated && len(snapshot) > 0 {
							logging.LogDebug("auto-results", "discover-fleet", "steps", strings.Join(snapshot, ","))
						}
					}
				}
			}
		}()
		_, _ = tracker.WaitForCompletion(ctx, expectedStepIDs)
		close(stopCh)
		// Wait for the polling goroutine to actually observe stopCh and return
		// before touching discoverStepIDsFunc/discoverFleetStepIDsFunc below —
		// otherwise a still-in-flight tick could race a caller's test-time
		// reassignment of those (package-level, test-injectable) function
		// variables once this function's goroutine finishes.
		pollWG.Wait()
		// One final fleet discovery after completion (may not have been picked up during polling)
		if fids, err := discoverFleetStepIDsFunc(baseURL); err == nil && len(fids) > 0 {
			mu.Lock()
			for _, id := range fids {
				if id == "" {
					continue
				}
				if _, ok := fleetSeen[id]; ok {
					continue
				}
				fleetSeen[id] = struct{}{}
				fleetDiscovered = append(fleetDiscovered, id)
			}
			mu.Unlock()
		}
		writeProgress("Auto-results: completion detected; fetching artifacts to %s...\n", root)

		// Discover step IDs from JSON metrics.
		// When we have expectedStepIDs (parsed from the scenario), only keep discovered IDs that
		// belong to this run. The discovery poller may have accumulated IDs from a prior run's
		// lingering container (which keeps /metrics/json alive during its API linger window), so
		// we intersect rather than union to prevent stale IDs from reaching FetchArtifactsForSteps.
		mu.Lock()
		cachedDiscovered := append([]string(nil), discovered...)
		cachedFleet := append([]string(nil), fleetDiscovered...)
		mu.Unlock()
		if len(expectedStepIDs) > 0 {
			expectedSet := make(map[string]struct{}, len(expectedStepIDs))
			for _, id := range expectedStepIDs {
				expectedSet[id] = struct{}{}
			}
			filtered := cachedDiscovered[:0:0]
			for _, id := range cachedDiscovered {
				if _, ok := expectedSet[id]; ok {
					filtered = append(filtered, id)
				}
			}
			cachedDiscovered = filtered
		}
		// Fleet-discovered IDs bypass the stale-ID intersection filter because they
		// represent the engine's actual runtime step IDs (which may differ from
		// CLI-generated expected IDs in distributed/multi-host mode due to timestamp
		// reassignment). Fleet IDs go first so they take precedence when present.
		stepIDs := uniqueStepIDs(cachedFleet, expectedStepIDs, cachedDiscovered)
		var discoverErr error
		if len(stepIDs) == 0 {
			var fallback []string
			fallback, discoverErr = discoverStepIDsFunc(baseURL)
			// Also try fleet endpoint as fallback
			fleetFallback, _ := discoverFleetStepIDsFunc(baseURL)
			stepIDs = uniqueStepIDs(fleetFallback, stepIDs, fallback)
		}
		if len(stepIDs) == 0 {
			var logErr error
			if discoverErr != nil {
				logErr = fmt.Errorf("discover step IDs: %w", discoverErr)
			} else {
				logErr = fmt.Errorf("no steps reported by metrics/json or metrics/fleet/json")
			}
			logging.LogError("auto-results", "no step IDs discovered; skipping fetch", logErr)
			return
		}
		writeProgress("Auto-results: fetching %d step(s): %s\n", len(stepIDs), strings.Join(stepIDs, ", "))

		if metadata != nil {
			metadata.ActualStepIDs = append([]string(nil), stepIDs...)
			metadata.DiscoveredStepIDs = uniqueStepIDs(metadata.DiscoveredStepIDs, cachedFleet, cachedDiscovered, stepIDs)
			if err := writeRunMetadata(metadata, root); err != nil {
				logging.LogError("auto-results", "write run metadata", err, "dest", root)
			}
		}
		fetch := newResultsFetcherFunc(baseURL, root)
		if _, ferr := fetch.FetchArtifactsForSteps(ctx, stepIDs); ferr != nil {
			logging.LogError("auto-results", "artifact fetch failed", ferr, "base_url", baseURL, "out", root)
			writeProgress("Results fetch encountered errors; see log. Output: %s\n", root)
			return
		}
		if traceFile != "" {
			if err := appendTraceToResultsManifest(root, traceFile); err != nil {
				logging.LogError("auto-results", "trace artifact append failed", err, "trace_file", traceFile, "out", root)
			}
		}
		writeProgress("Auto-results: results saved to %s\n", root)

		// Optional: graceful shutdown of all nodes via /shutdown
		if shutdownOn {
			// small settle delay before shutdown to avoid races
			time.Sleep(1 * time.Second)
			writeProgress("Auto-results: requesting shutdown on all hosts...\n")
			shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancelShutdown()
			// default linger 5s if invalid
			if lingerSec <= 0 {
				lingerSec = int(constants.APILingerDefault / time.Second)
			}
			reqErr := requestShutdownAllFunc(shutdownCtx, allHosts, apiPort, time.Duration(lingerSec)*time.Second, debug)
			if reqErr != nil {
				logging.LogError("auto-results", "shutdown encountered issues", reqErr)
				writeProgress("Shutdown completed with warnings; see logs for details.\n")
			} else {
				writeProgress("Shutdown completed successfully.\n")
			}

			// Diagnostics collection and full container/staging cleanup must
			// happen before the summary below, not after: a consumer that
			// treats the summary (or its results_summary.txt file) as "the
			// run is done" should not be able to observe it before
			// diagnostics/diagnostics_manifest.json and copied GC/JFR files
			// exist.
			if preSummaryHook != nil {
				preSummaryHook()
			}
		}

		writer := summaryOut
		if writer == nil {
			writer = io.Discard
		}
		if err := generateRunSummaryFunc(context.Background(), root, writer); err != nil {
			logging.LogError("auto-results", "generate summary", err)
			writeProgress("Auto-results: summary generation encountered issues; see log.\n")
		}
	}()
	return done
}

// uniqueStepIDs merges ordered slices of step IDs, removing duplicates and empty entries.
// The order of the first occurrence of each ID is preserved across the provided lists.
func uniqueStepIDs(lists ...[]string) []string {
	total := 0
	for _, ids := range lists {
		total += len(ids)
	}
	out := make([]string, 0, total)
	seen := make(map[string]struct{}, total)
	for _, ids := range lists {
		for _, id := range ids {
			if id == "" {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out = append(out, id)
		}
	}
	return out
}

type traceOptions struct {
	Path   string
	Append bool
	Auto   bool
}

func buildHeadlessOptions(traceOpts traceOptions, verbose bool, apiPort string, autoTerminate int, delegateNormalShutdown bool, expectedStepIDs []string) headless.HeadlessOptions {
	return headless.HeadlessOptions{
		TraceFile:              traceOpts.Path,
		TraceAppend:            traceOpts.Append,
		Verbose:                verbose,
		APIPort:                apiPort,
		AutoTerminateSeconds:   autoTerminate,
		DelegateNormalShutdown: delegateNormalShutdown,
		ExpectedStepIDs:        append([]string(nil), expectedStepIDs...),
	}
}

func prepareTraceOptions(explicitPath string, explicitAppend bool, autoResults bool, plannedResultsRoot string, runToken string, warnOut io.Writer) (traceOptions, error) {
	path := strings.TrimSpace(explicitPath)
	if path != "" {
		if err := ensureTracePathWritable(path, explicitAppend); err != nil {
			return traceOptions{}, fmt.Errorf("failed to initialize trace file: %w", err)
		}
		return traceOptions{Path: path, Append: explicitAppend, Auto: false}, nil
	}
	if !autoResults || plannedResultsRoot == "" {
		return traceOptions{}, nil
	}

	name := fmt.Sprintf("spt-%s.trace.log", runToken)
	primaryPath := filepath.Join(plannedResultsRoot, name)
	primaryErr := ensureTracePathWritable(primaryPath, false)
	if primaryErr == nil {
		return traceOptions{Path: primaryPath, Append: false, Auto: true}, nil
	}

	fallbackPath := filepath.Join(".", name)
	if warnOut != nil {
		_, _ = fmt.Fprintf(
			warnOut,
			"Warning: could not initialize auto trace file at %s (%v); falling back to %s\n",
			primaryPath, primaryErr, fallbackPath,
		)
	}
	if fallbackErr := ensureTracePathWritable(fallbackPath, false); fallbackErr != nil {
		return traceOptions{}, fmt.Errorf("failed to initialize fallback trace file: %w", fallbackErr)
	}
	return traceOptions{Path: fallbackPath, Append: false, Auto: true}, nil
}

func ensureTracePathWritable(path string, appendMode bool) error {
	if path == "" {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return err
	}
	flags := os.O_CREATE | os.O_WRONLY
	if appendMode {
		flags |= os.O_APPEND
	} else {
		flags |= os.O_TRUNC
	}
	f, err := os.OpenFile(path, flags, 0o600) // #nosec G304 -- validated local output path
	if err != nil {
		return err
	}
	return f.Close()
}

func appendTraceToResultsManifest(resultsRoot string, tracePath string) error {
	if resultsRoot == "" || tracePath == "" {
		return nil
	}
	manifestPath := filepath.Join(resultsRoot, constants.ResultsManifestFileName)
	content, err := os.ReadFile(manifestPath) // #nosec G304 -- path derived from local results root
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read results manifest: %w", err)
	}
	manifest := &results.Manifest{}
	if err := json.Unmarshal(content, manifest); err != nil {
		return fmt.Errorf("decode results manifest: %w", err)
	}

	destName := filepath.Base(tracePath)
	destPath := filepath.Join(resultsRoot, destName)
	srcAbs, _ := filepath.Abs(tracePath)
	destAbs, _ := filepath.Abs(destPath)
	if srcAbs != destAbs {
		if err := copyFileAtomic(tracePath, destPath); err != nil {
			return fmt.Errorf("copy trace file into results root: %w", err)
		}
	}

	stat, err := os.Stat(destPath)
	if err != nil {
		return fmt.Errorf("stat results trace file: %w", err)
	}
	traceStatus := results.FileStatus{
		Name:        destName,
		Size:        stat.Size(),
		Status:      "ok",
		Modified:    stat.ModTime().UTC().Format(time.RFC3339),
		ContentType: "text/plain",
	}
	replaced := false
	for i := range manifest.RunFiles {
		if manifest.RunFiles[i].Name == destName {
			manifest.RunFiles[i] = traceStatus
			replaced = true
			break
		}
	}
	if !replaced {
		manifest.RunFiles = append(manifest.RunFiles, traceStatus)
	}

	updated, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal updated results manifest: %w", err)
	}
	tmp, err := os.CreateTemp(resultsRoot, ".index.json.trace-*")
	if err != nil {
		return fmt.Errorf("create temp manifest: %w", err)
	}
	tmpPath := tmp.Name()
	if _, err = tmp.Write(updated); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return fmt.Errorf("write temp manifest: %w", err)
	}
	if err = tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("close temp manifest: %w", err)
	}
	if err = os.Rename(tmpPath, manifestPath); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("replace results manifest: %w", err)
	}
	return nil
}

func copyFileAtomic(srcPath string, destPath string) error {
	src, err := os.Open(srcPath) // #nosec G304 -- validated local trace path
	if err != nil {
		return err
	}
	defer func() { _ = src.Close() }()

	if err := os.MkdirAll(filepath.Dir(destPath), 0o750); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(destPath), ".trace-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	if _, err = io.Copy(tmp, src); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err = tmp.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err = os.Rename(tmpPath, destPath); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

// requestShutdownAll posts /shutdown to all provided hosts and waits for linger.
func requestShutdownAll(ctx context.Context, hosts []*hostparse.HostInfo, apiPort string, linger time.Duration, debug bool) error {
	if len(hosts) == 0 {
		// default to localhost
		hosts = []*hostparse.HostInfo{{Host: "localhost", Original: "localhost"}}
	}
	type result struct {
		host string
		err  error
	}
	ch := make(chan result, len(hosts))
	for _, h := range hosts {
		host := h
		go func() {
			base := fmt.Sprintf("http://%s:%s", host.Host, apiPort)
			client := portcheck.NewSptAPIClient(base, constants.APIPollingTimeout)
			// 1) POST /shutdown
			if err := client.Shutdown(ctx); err != nil {
				ch <- result{host: host.Original, err: fmt.Errorf("shutdown error: %w", err)}
				return
			}
			if debug {
				logging.LogDebug("auto-results", "shutdown requested", "host", host.Original)
			}
			// 2) Wait for linger on /status
			if err := client.WaitForLinger(ctx, linger); err != nil {
				ch <- result{host: host.Original, err: fmt.Errorf("linger wait error: %w", err)}
				return
			}
			ch <- result{host: host.Original, err: nil}
		}()
	}
	var errs []string
	for i := 0; i < len(hosts); i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case r := <-ch:
			if r.err != nil {
				errs = append(errs, fmt.Sprintf("%s: %v", r.host, r.err))
				logging.LogError("auto-results", "shutdown host error", r.err, "host", r.host)
			} else if debug {
				logging.LogDebug("auto-results", "shutdown host ok", "host", r.host)
			}
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("some hosts failed shutdown: %s", strings.Join(errs, "; "))
	}
	return nil
}

// checkPortConflicts checks for and resolves port conflicts before launching Spt
func checkPortConflicts(cmd *cobra.Command) error {
	// Get force mode setting
	forceMode, _ := cmd.Flags().GetBool("force")

	// Get API port
	sptPort := getAPIPort(cmd)

	logger := logging.GetLogger()
	logger.Debug("Checking for port conflicts before launching Spt",
		"port", sptPort,
		"force_mode", forceMode)

	// Create context with reasonable timeout for port checking
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Perform port conflict resolution
	result, err := resolvePortConflictFunc(ctx, sptPort, forceMode)
	if err != nil {
		logger.Error("Port conflict resolution failed", "error", err.Error())
		return fmt.Errorf("failed to resolve port conflict on %s: %w", sptPort, err)
	}

	// Handle resolution result
	if !result.Success {
		switch result.Strategy {
		case portcheck.StrategyAbort:
			logger.Info("User chose to abort due to port conflict")
			fmt.Printf("\n❌ Operation cancelled - port %s conflict not resolved\n", sptPort)
			return fmt.Errorf("operation cancelled by user")

		case portcheck.StrategyRetry:
			// For retry failures, show retry information
			if result.RetryAfter > 0 {
				fmt.Printf("\n⏳ Port %s still in use - you may retry after resolving the conflict\n", sptPort)
				fmt.Printf("Suggested retry delay: %v\n", result.RetryAfter)
			}
			return fmt.Errorf("port %s is still in use: %s", sptPort, result.Message)

		default:
			logger.Error("Port conflict resolution unsuccessful",
				"strategy", string(result.Strategy),
				"message", result.Message)
			fmt.Printf("\n❌ Failed to resolve port %s conflict: %s\n", sptPort, result.Message)
			return fmt.Errorf("port %s conflict resolution failed", sptPort)
		}
	}

	// Success - log and continue
	if result.Strategy != "" {
		logger.Info("Port conflict resolved successfully",
			"port", sptPort,
			"strategy", string(result.Strategy),
			"message", result.Message)

		// Only show success message if there was actually a conflict to resolve
		if result.Strategy != "retry" || !strings.Contains(result.Message, "available") {
			fmt.Printf("✅ Port %s conflict resolved via %s\n", sptPort, result.Strategy)
		}
	} else {
		logger.Debug("No port conflicts detected", "port", sptPort)
	}

	return nil
}

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run <type>",
	Short: "Executes a benchmark test with a specified workload type.",
	Long: `The 'run' command executes a benchmark test. The <type> argument specifies the workload.

Available workload types:
  write: Perform a write-only test, creating new objects.
  list: Benchmark object listing throughput against a bucket.
  read: Perform a read-only test on pre-existing objects.
  mixed: Perform a test with a specified mix of read and write operations.
  delete: Perform a test to measure object deletion performance.
  mock: Run a mock test using the dummy-mock driver (no S3 endpoint required).
  tables: Benchmark S3 Tables (Iceberg) operations: TPS, compaction, or catalog discovery.`,
	Args:         cobra.ExactArgs(1), // Enforce that exactly one argument (workload type) is provided
	SilenceUsage: true,               // Suppress usage on runtime errors; validation will re-enable
	PersistentPreRunE: func(cmd *cobra.Command, _ []string) error {
		// Ensure .env and logger are initialized at the subcommand level.
		_ = config.LoadDotEnv()
		if err := initializeLogger(); err != nil { // idempotent
			return err
		}
		// Allow .env/OS env to populate flags before validation.
		return applyEnvDefaultsToRunFlags(cmd)
	},
	PreRunE: ValidateRunCommand,
	RunE: func(cmd *cobra.Command, args []string) error {
		workloadType := args[0]

		// Validate workload type
		if err := ValidateWorkloadType(workloadType); err != nil {
			return err
		}

		// Build scenario parameters
		params, err := buildScenarioParams(workloadType, cmd)
		if err != nil {
			return err
		}

		// Seed size warning for read workloads
		if workloadType == WorkloadTypeRead {
			warnSeedSize(params)
		}

		// Lock the base timestamp so that every GenerateScenario call
		// (here and later in the orchestrator) produces identical step IDs.
		params.BaseTimestamp = scenario.BaseTimestamp()
		params, err = scenario.PrepareExternalItemFiles(params)
		if err != nil {
			return err
		}

		// Generate scenario content
		scenarioContent, err := scenario.GenerateScenario(params)
		if err != nil {
			return err
		}

		// Extract expected step IDs to drive completion detection
		plan, planErr := scenario.BuildStepPlanFromScenario(scenarioContent)
		var expectedStepIDs []string
		if planErr == nil && len(plan.Steps) > 0 {
			expectedStepIDs = make([]string, 0, len(plan.Steps))
			for _, s := range plan.Steps {
				expectedStepIDs = append(expectedStepIDs, s.ID)
			}
		}

		// Create temporary scenario file
		scenarioFile := fmt.Sprintf("spt-scenario-%d.js", time.Now().Unix())
		scenarioPath := filepath.Join(".", scenarioFile)

		if err := os.WriteFile(scenarioPath, []byte(scenarioContent), 0600); err != nil {
			return fmt.Errorf("failed to write scenario file: %w", err)
		}

		// Log scenario content in debug mode
		logger := logging.GetLogger()
		logger.Debug("Generated scenario file",
			"file", scenarioPath,
			"content", scenarioContent)

		// Phase 1: Parse results options (flags) and log them for visibility
		resultsOpts := buildResultsOptions(cmd)
		logger.Debug("Results options parsed",
			"auto_results", resultsOpts.AutoResults,
			"results_dir", resultsOpts.ResultsDir,
			"label", resultsOpts.Label,
			"debug", resultsOpts.Debug)

		// Check if generate-only flag is set
		generateOnly, _ := cmd.Flags().GetBool("generate-only")
		if generateOnly {
			fmt.Printf("Generated scenario file: %s\n", scenarioPath)
			fmt.Printf("\nSelected Options:\n%s\n", formatScenarioParams(params))
			fmt.Printf("\nScenario content:\n%s\n", scenarioContent)
			return nil
		}

		skipImagePull, _ := cmd.Flags().GetBool(flagSkipImagePull)
		if skipImagePull {
			_ = os.Setenv(constants.EnvSkipImagePull, "true")
			fmt.Println("Skipping Docker image pull; using locally cached image.")
		} else {
			_ = os.Unsetenv(constants.EnvSkipImagePull)
		}

		// --spt-image overrides the engine image (precedence: flag > SPT_IMAGE env >
		// version-matched default). Set the env so constants.EffectiveSptImage picks it up.
		if sptImageFlag, _ := cmd.Flags().GetString(flagSptImage); strings.TrimSpace(sptImageFlag) != "" {
			_ = os.Setenv(constants.EnvSptImage, strings.TrimSpace(sptImageFlag))
		}

		switch params.S3Driver {
		case scenario.S3DriverRdma:
			_ = os.Setenv(constants.EnvRdmaEnabled, "true")
			fmt.Println("RDMA mode enabled: using s3-rdma driver with device passthrough.")
		case scenario.S3DriverAws:
			fmt.Println("Using AWS SDK S3 driver (s3-aws).")
		}

		// Parse and validate test hosts
		testHostsStr, _ := cmd.Flags().GetString("test-hosts")
		minHosts, _ := cmd.Flags().GetInt("min-hosts")
		attachExisting, _ := cmd.Flags().GetBool(flagAttachExistingWorkers)

		hostInfos, err := hostparse.ParseTestHosts(testHostsStr)
		if err != nil {
			// Clean up scenario file on host parsing failure
			if removeErr := os.Remove(scenarioPath); removeErr != nil {
				logger.Debug("Failed to clean up scenario file after host parsing error",
					"file", scenarioPath,
					"error", removeErr.Error())
			}
			return fmt.Errorf("invalid test hosts: %w", err)
		}

		// Set default min-hosts to total host count if not specified
		if minHosts == 0 {
			minHosts = len(hostInfos)
		}

		// Validate min-hosts doesn't exceed total hosts
		if minHosts > len(hostInfos) {
			// Clean up scenario file
			if removeErr := os.Remove(scenarioPath); removeErr != nil {
				logger.Debug("Failed to clean up scenario file after min-hosts validation error",
					"file", scenarioPath,
					"error", removeErr.Error())
			}
			return fmt.Errorf("min-hosts (%d) cannot exceed total hosts (%d)", minHosts, len(hostInfos))
		}

		if attachExisting && len(hostInfos) < 2 {
			if removeErr := os.Remove(scenarioPath); removeErr != nil {
				logger.Debug("Failed to clean up scenario file after attach validation error",
					"file", scenarioPath,
					"error", removeErr.Error())
			}
			return fmt.Errorf("--%s requires at least two hosts (entry + worker)", flagAttachExistingWorkers)
		}

		logger.Debug("Parsed test hosts",
			"total_hosts", len(hostInfos),
			"min_hosts", minHosts,
			"hosts", testHostsStr,
			"attach_existing", attachExisting)
		useMultiHostOrchestrator := shouldUseMultiHostOrchestrator(hostInfos)

		// The local single-host runner owns Docker on the controller, so it
		// needs the controller port check. Remote and multi-host runs perform
		// port preflight on each configured Docker host instead.
		if !useMultiHostOrchestrator {
			if err := checkPortConflicts(cmd); err != nil {
				// Clean up scenario file on conflict resolution failure
				if removeErr := os.Remove(scenarioPath); removeErr != nil {
					logger.Debug("Failed to clean up scenario file after port conflict",
						"file", scenarioPath,
						"error", removeErr.Error())
				}
				return err
			}
		}

		sptImage := constants.EffectiveSptImage()
		networkMode, _ := cmd.Flags().GetString("network-mode")
		rmiPortStart, _ := cmd.Flags().GetInt("rmi-port-start")
		rmiPortCount, _ := cmd.Flags().GetInt("rmi-port-count")
		autoTerminate, _ := cmd.Flags().GetInt("auto-terminate-seconds")
		apiPort := getAPIPort(cmd)
		baseURL := deriveBaseURL(apiPort, hostInfos)
		traceFileFlag, _ := cmd.Flags().GetString("trace-file")
		traceAppendFlag, _ := cmd.Flags().GetBool("trace-append")
		runToken := time.Now().UTC().Format("20060102.150405.000")
		plannedResultsRoot := ""
		if resultsOpts.AutoResults {
			plannedResultsRoot = filepath.Join(resultsOpts.ResultsDir, fmt.Sprintf("%s-%s", resultsOpts.Label, runToken))
		}
		traceOpts, err := prepareTraceOptions(traceFileFlag, traceAppendFlag, resultsOpts.AutoResults, plannedResultsRoot, runToken, os.Stdout)
		if err != nil {
			if removeErr := os.Remove(scenarioPath); removeErr != nil {
				logger.Debug("Failed to clean up scenario file after trace initialization error",
					"file", scenarioPath,
					"error", removeErr.Error())
			}
			return err
		}
		metadata := buildRunMetadata(runMetadataInput{
			WorkloadType:         workloadType,
			Params:               params,
			ScenarioPath:         scenarioPath,
			ResultsOptions:       resultsOpts,
			HostInfos:            hostInfos,
			TestHostsRaw:         testHostsStr,
			MinHosts:             minHosts,
			AttachExisting:       attachExisting,
			NetworkMode:          networkMode,
			RMIPortStart:         rmiPortStart,
			RMIPortCount:         rmiPortCount,
			APIPort:              apiPort,
			BaseURL:              baseURL,
			ExpectedStepIDs:      expectedStepIDs,
			SptImage:             sptImage,
			Command:              cmd,
			AutoTerminateSeconds: autoTerminate,
		})
		metadata.TraceFile = traceOpts.Path
		metadata.TraceAuto = traceOpts.Auto
		if plannedResultsRoot != "" {
			metadata.ResultsRoot = plannedResultsRoot
		}

		headlessMode := shouldRunHeadless(cmd)

		var summaryWriter *summaryMessageWriter
		var setSummarySink func(func(string))
		progressOut := io.Writer(os.Stdout)
		if resultsOpts.AutoResults {
			summaryWriter = newSummaryMessageWriter()
			if headlessMode {
				summaryWriter.SetSink(func(line string) {
					fmt.Println(line)
				})
			} else {
				progressOut = summaryWriter
			}
			setSummarySink = summaryWriter.SetSink
		}

		// Construct the host orchestrator now (before starting the
		// auto-results background tracker below) so FinalizeDiagnosticsAndCleanup
		// can run as startAutoResults' pre-summary hook. Construction itself is
		// cheap (no I/O) — ConnectHosts and everything else that actually talks
		// to the hosts still happens later, in its original place.
		var multiHostOrchestrator *tui.MultiHostOrchestrator
		if useMultiHostOrchestrator {
			rmiConfig := tui.RMIConfig{
				NetworkMode: networkMode,
				PortStart:   rmiPortStart,
				PortCount:   rmiPortCount,
			}
			multiHostOrchestrator = tui.NewMultiHostOrchestratorWithRMI(hostInfos, minHosts, rmiConfig)
			// Provide image to orchestrator for preflight image checks
			multiHostOrchestrator.SetImage(sptImage)
			multiHostOrchestrator.SetAttachExistingWorkers(attachExisting)
			multiHostOrchestrator.SetResultsRoot(plannedResultsRoot)
		}
		finalizeMultiHost := func() {
			if multiHostOrchestrator == nil || plannedResultsRoot == "" {
				return
			}
			diagCtx, diagCancel := context.WithTimeout(context.Background(), constants.DiagnosticsCollectionTimeout)
			defer diagCancel()
			if err := multiHostOrchestrator.FinalizeDiagnosticsAndCleanup(diagCtx); err != nil {
				logger.Warn("Diagnostics collection/cleanup completed with warnings", "error", err.Error())
			}
		}

		// Start background auto-results tracker/fetcher if enabled
		var autoResultsDone chan struct{}
		if resultsOpts.AutoResults {
			autoResultsDone = startAutoResults(baseURL, resultsOpts.Label, resultsOpts.ResultsDir, expectedStepIDs, resultsOpts.Debug, hostInfos, apiPort, resultsOpts.ShutdownOnComplete, resultsOpts.ShutdownLingerSec, scenarioPath, metadata, progressOut, summaryWriter, traceOpts.Path, finalizeMultiHost)
		}
		waitForAutoResults := func(required bool) bool {
			if autoResultsDone != nil {
				select {
				case <-autoResultsDone:
					return true
				case <-time.After(func() time.Duration {
					if required {
						return 2 * time.Minute
					}
					return 2 * time.Second
				}()):
					return false
				}
			}
			return true
		}
		finalizeTraceArtifact := func() {
			if err := appendTraceToResultsManifest(plannedResultsRoot, traceOpts.Path); err != nil {
				logger.Debug("Failed to finalize trace artifact in results manifest",
					"results_root", plannedResultsRoot,
					"trace_file", traceOpts.Path,
					"error", err.Error())
			}
		}

		// Get endpoint for display
		endpoint, _ := cmd.Flags().GetString("endpoint")
		if workloadType == WorkloadTypeMock {
			endpoint = "dummy-mock"
		}

		fmt.Printf("Launching %s workload against %s...\n", workloadType, endpoint)
		fmt.Printf("Container: %s\n", sptImage)

		// The host orchestrator also handles a single remote host. The legacy
		// single-host runner owns local Docker and is appropriate only when the
		// one configured host is local.
		if useMultiHostOrchestrator {
			// Warn if using bridge mode with multiple hosts (distributed testing)
			if networkMode == constants.BridgeNetworkMode {
				fmt.Println("⚠️  WARNING: Bridge networking may not work for distributed testing.")
				fmt.Println("   Java RMI requires host networking for inter-node communication.")
				fmt.Println("   Consider using --network-mode host (the default).")
				fmt.Println()
			}

			if len(hostInfos) == 1 {
				fmt.Printf("Remote-host mode: %s\n", hostInfos[0].Original)
			} else {
				fmt.Printf("Multi-host mode: %d hosts, minimum required: %d\n", len(hostInfos), minHosts)
			}
			if attachExisting {
				fmt.Println("Attach mode enabled: expecting worker nodes prestarted with --run-node; spt will launch the entry node.")
			}

			// Already constructed above (before startAutoResults) so its
			// FinalizeDiagnosticsAndCleanup could be wired in as a pre-summary hook.
			orchestrator := multiHostOrchestrator
			ctx := context.Background()

			// Respect force cleanup for preflight conflicts
			forceMode, _ := cmd.Flags().GetBool("force")
			orchestrator.SetForceCleanup(forceMode)

			// Connect to all hosts
			err := connectMultiHostOrchestratorFunc(ctx, orchestrator)
			if err != nil {
				// Clean up scenario file on connection failure
				if removeErr := os.Remove(scenarioPath); removeErr != nil {
					logger.Debug("Failed to clean up scenario file after connection error",
						"file", scenarioPath,
						"error", removeErr.Error())
				}
				return fmt.Errorf("failed to connect to required hosts: %w", err)
			}

			// Check if we should run in headless mode
			if headlessMode {
				verbose, _ := cmd.Flags().GetBool("verbose")

				delegateShutdownToAutoResults := resultsOpts.AutoResults && resultsOpts.ShutdownOnComplete
				options := buildHeadlessOptions(traceOpts, verbose, "", autoTerminate, delegateShutdownToAutoResults, expectedStepIDs)

				if autoTerminate > 0 {
					fmt.Printf("Auto-terminate: will stop after %d seconds\n", autoTerminate)
				}

				err := headless.StartHeadlessModeWithOrchestrator(orchestrator, sptImage, scenarioPath, params, options)
				autoTerminated, normalizedErr := normalizeHeadlessAutoTerminate(err, orchestrator, 30*time.Second)
				if autoTerminated {
					finalizeTraceArtifact()
					return normalizedErr
				}
				autoResultsComplete := waitForAutoResults(delegateShutdownToAutoResults)
				// finalizeMultiHost normally already ran as startAutoResults'
				// pre-summary hook by the time we get here; calling it again
				// is a safe, near-instant no-op (FinalizeDiagnosticsAndCleanup
				// runs at most once). This remains the fallback path for
				// AutoResults-without-ShutdownOnComplete, and for auto-terminate
				// (which bypasses startAutoResults' shutdown step below via the
				// autoTerminated return above).
				if autoResultsComplete && delegateShutdownToAutoResults {
					finalizeMultiHost()
				}
				if !autoResultsComplete && delegateShutdownToAutoResults {
					// The pre-summary hook may still be running (e.g. a slow
					// diagnostics copy) — finalizeMultiHost/
					// FinalizeDiagnosticsAndCleanup is safe to call concurrently
					// with it and will simply wait for that single execution
					// rather than duplicating it.
					logger.Warn("Timed out waiting for auto-results; forcing diagnostics collection and container cleanup")
					finalizeMultiHost()
				}
				finalizeTraceArtifact()
				return normalizedErr
			}

			fmt.Printf("Starting multi-host TUI...\n\n")
			if autoTerminate > 0 {
				fmt.Printf("Auto-terminate: will stop after %d seconds\n", autoTerminate)
				err = tui.StartTUIWithMultiHostOrchestratorTimeoutWithTrace(orchestrator, sptImage, scenarioPath, params, autoTerminate, setSummarySink, traceOpts.Path, traceOpts.Append)
				autoResultsComplete := waitForAutoResults(false)
				if autoResultsComplete && resultsOpts.AutoResults && resultsOpts.ShutdownOnComplete {
					finalizeMultiHost()
				}
				finalizeTraceArtifact()
				return err
			}
			err = tui.StartTUIWithMultiHostOrchestratorWithTrace(orchestrator, sptImage, scenarioPath, params, setSummarySink, traceOpts.Path, traceOpts.Append)
			autoResultsComplete := waitForAutoResults(false)
			if autoResultsComplete && resultsOpts.AutoResults && resultsOpts.ShutdownOnComplete {
				finalizeMultiHost()
			}
			finalizeTraceArtifact()
			return err
		}

		// Single host mode (existing logic)
		// Check if we should run in headless mode
		if headlessMode {
			verbose, _ := cmd.Flags().GetBool("verbose")

			options := buildHeadlessOptions(traceOpts, verbose, apiPort, autoTerminate, false, nil)
			options.NetworkMode = networkMode
			options.ResultsRoot = plannedResultsRoot
			// KeepScenario is now passed via params, not options.
			if autoTerminate > 0 {
				fmt.Printf("Auto-terminate: will stop after %d seconds\n", autoTerminate)
			}

			err := headless.StartHeadlessModeWithParams(sptImage, scenarioPath, params, options)
			waitForAutoResults(false)
			finalizeTraceArtifact()
			return err
		}

		fmt.Printf("Starting TUI...\n\n")
		// Launch TUI with the scenario file
		if autoTerminate > 0 {
			fmt.Printf("Auto-terminate: will stop after %d seconds\n", autoTerminate)
			err = tui.StartTUIWithScenarioAndParamsNetworkModeTimeoutWithTrace(sptImage, scenarioPath, params, apiPort, networkMode, plannedResultsRoot, autoTerminate, setSummarySink, traceOpts.Path, traceOpts.Append)
		} else {
			err = tui.StartTUIWithScenarioAndParamsNetworkModeWithTrace(sptImage, scenarioPath, params, apiPort, networkMode, plannedResultsRoot, setSummarySink, traceOpts.Path, traceOpts.Append)
		}
		waitForAutoResults(false)
		finalizeTraceArtifact()
		return err
	},
}

func init() {
	rootCmd.AddCommand(runCmd)

	// Target Connection Options (Required for S3, optional for mock)
	runCmd.Flags().StringSliceP("endpoints", "e", []string{}, "One or more S3 endpoint URLs (comma-separated or repeatable)")
	runCmd.Flags().String("endpoint", "", "Deprecated: alias for --endpoints (single value)")
	_ = runCmd.Flags().MarkHidden("endpoint")
	runCmd.Flags().Bool("slice-endpoints", false, "Partition endpoints across nodes in distributed runs")
	runCmd.Flags().StringP("access-key", "a", "", "The S3 access key credential")
	runCmd.Flags().StringP("secret-key", "s", "", "The S3 secret key credential")
	runCmd.Flags().StringP("bucket", "b", "", "The target bucket to use for the test")
	runCmd.Flags().String("prefix", "", "List workload: optional object key prefix to constrain listings")
	runCmd.Flags().Int("auth-version", 4, "S3 authentication signature version (2 or 4; default 4)")

	// Workload Definition Options
	runCmd.Flags().IntP("threads", "t", 1, "Number of parallel client threads to run (e.g., 16)")
	runCmd.Flags().StringP("object-size", "o", "", "The size of each object using human-readable units (e.g., 1MB, 256KB, 4GB)")
	runCmd.Flags().Float64("object-data-compressibility", 0.0, "Compressibility percentage of object payloads (0.0 to 100.0, default 0.0)")
	runCmd.Flags().Bool("object-data-dedupable", true, "Allow object payloads to be deduplicated by the storage array (default true)")
	runCmd.Flags().IntP("object-count", "n", 0, "Defines the workload by a fixed number of objects to process")
	runCmd.Flags().StringP("duration", "d", "", "Defines the workload by a fixed time duration (e.g., 5m, 1h)")

	// Multipart Upload Options
	runCmd.Flags().String("part-size", "", "Enable multipart upload with the given part size (e.g., 5MB, 64MB, 256MB)")
	runCmd.Flags().Int("mpu-concurrent-objects", 0, "Max concurrent multipart objects in flight (0 = unlimited)")
	runCmd.Flags().Int("mpu-concurrent-parts", 0, "Max concurrent parts in flight per multipart object (0 = unlimited)")

	// Test Behavior Options
	runCmd.Flags().Int("seed-objects", 2500, "Number of objects to pre-create for read benchmarks (default: 2500)")
	runCmd.Flags().Bool("cleanup", false, "A boolean flag to automatically delete all created objects after the test completes")
	runCmd.Flags().Bool("create-prefix", false, "A boolean flag to ensure the target prefix (directory) is created if it doesn't exist")
	runCmd.Flags().StringP("output-dir", "O", "", "Specifies a local directory on the host machine to save the detailed Spt report files (e.g., ./results/test-01)")
	runCmd.Flags().Bool("generate-only", false, "Generate the scenario file without running Docker or Spt")
	// Headless auto-termination
	runCmd.Flags().Int("auto-terminate-seconds", 0, "Automatically terminate headless runs after N seconds (0 = unlimited)")
	runCmd.Flags().Bool("keep-scenario", false, "Keep the scenario file after the test completes (default: delete on success)")
	runCmd.Flags().Bool("save-items", false, "Save items.csv to the results directory (write workloads only; can be large for high-throughput runs)")
	runCmd.Flags().String("items-file", "", "Path to a local items.csv to use for read workload (skips seed phase)")
	runCmd.Flags().Bool(flagReadShuffle, false, "Read workload: shuffle items within each fetched batch before issuing reads (widens randomness, increases engine buffer usage, and does not guarantee storage-cache avoidance)")
	runCmd.Flags().Int(flagReadShuffleBatchSize, 0, fmt.Sprintf("Read workload: batch size to use with --shuffle (0 = use the bounded default, max %d)", constants.ReadShuffleMaxBatchSize))
	runCmd.Flags().Bool("force", false, "Automatically resolve port conflicts without user interaction")

	// Mixed Workload Distribution Options (defaults: GET 45 / STAT 30 / PUT 15 / DELETE 10)
	runCmd.Flags().Int("get-distrib", scenario.MixedDefaultGetDistrib, "Percentage of GET (read) operations for mixed workload (default: 45)")
	runCmd.Flags().Int("stat-distrib", scenario.MixedDefaultStatDistrib, "Percentage of STAT (HEAD) operations for mixed workload (default: 30)")
	runCmd.Flags().Int("put-distrib", scenario.MixedDefaultPutDistrib, "Percentage of PUT (write) operations for mixed workload (default: 15)")
	runCmd.Flags().Int("delete-distrib", scenario.MixedDefaultDeleteDistrib, "Percentage of DELETE operations for mixed workload (default: 10)")
	runCmd.Flags().String("read-items-file", "", "Items file for mixed workload READ pool (skips seed phase; --cleanup not allowed)")
	runCmd.Flags().String("delete-items-file", "", "Items file to pre-populate mixed workload DELETE queue (relaxes delete<=put constraint; --cleanup not allowed)")
	runCmd.Flags().Int("service-threads", 0, "Engine virtual-thread carrier parallelism (0 = JVM default, env: SPT_SERVICE_THREADS)")
	runCmd.Flags().StringArray(flagEngineOverride, []string{}, "Advanced engine defaults override as path=value; repeat for multiple overrides (for example: storage.driver.threads=6, env: SPT_ENGINE_OVERRIDES)")
	runCmd.Flags().String("api-port", "", "Spt API port (defaults to 9999, legacy: 43234)")
	runCmd.Flags().Bool(flagSkipImagePull, false, "Use the locally cached Docker image without pulling the latest tag (env: SPT_SKIP_IMAGE_PULL)")
	runCmd.Flags().String(flagSptImage, "", "Override the engine image ref (default: matches the CLI version, e.g. ...:v5.10.3; dev builds use ...:spt_dev; env: SPT_IMAGE)")

	// Results Retrieval Options (Phase 1: flags + parsing only)
	runCmd.Flags().Bool("auto-results", true, "Automatically retrieve results artifacts at end of run")
	runCmd.Flags().String("results-dir", "./results", "Directory to write retrieved results artifacts")
	runCmd.Flags().String("label", "", "Optional label used for output directory naming and as the step ID prefix (default: mt)")
	runCmd.Flags().Bool("auto-results-debug", false, "Enable verbose debug logs for auto-results completion detection")
	// Phase 5 shutdown controls
	runCmd.Flags().Bool("shutdown-on-complete", true, "After fetching results, request /shutdown on all Spt hosts")
	runCmd.Flags().Int("shutdown-linger", 5, "Seconds to wait for /status linger after /shutdown")

	// Multi-Host Options
	runCmd.Flags().String("test-hosts", "127.0.0.1",
		`Comma-separated list of Docker hosts for distributed testing.
Format: [user@]host[,[user@]host...]
Examples:
  - Single local: "127.0.0.1" (default)
  - Multiple remote: "root@node1,root@node2"
  - Mixed: "127.0.0.1,admin@worker1,root@worker2"

Requirements:
  - SSH key-based authentication must be configured for remote hosts
  - Docker must be accessible to the SSH user on remote hosts
  - No password prompts are supported (use ssh-agent or passwordless keys)

For remote hosts, spt will connect via SSH to manage Docker containers.
Ensure your SSH keys are configured: ssh-add ~/.ssh/id_rsa`)

	runCmd.Flags().Int("min-hosts", 0,
		`Minimum number of hosts that must connect successfully.
Default: All hosts specified in --test-hosts must connect.
Example: --test-hosts "host1,host2,host3" --min-hosts 2
         (continues if at least 2 hosts connect)`)
	runCmd.Flags().Bool(flagAttachExistingWorkers, false, "Attach to prestarted worker nodes; spt still launches the entry node")

	// RMI Configuration Options (for distributed testing)
	runCmd.Flags().String("network-mode", "host", "Docker network mode: 'host' (default, required for RMI) or 'bridge'")
	runCmd.Flags().Int("rmi-port-start", 40000, "Starting port for RMI range")
	runCmd.Flags().Int("rmi-port-count", 10, "Number of RMI ports to verify")

	// RDMA Acceleration Options
	runCmd.Flags().String("s3-driver", "default",
		`S3 storage driver backend: "default" (Netty), "aws" (AWS SDK), "rdma" (RDMA-accelerated).
Shorthand: --use-rdma is equivalent to --s3-driver rdma. (env: SPT_S3_DRIVER)`)
	runCmd.Flags().Bool("use-rdma", false, "Use RDMA-accelerated S3 driver (requires RDMA hardware and device passthrough)")
	runCmd.Flags().String("rdma-local-ip", "", "Local RDMA interface IP address (env: RDMA_LOCAL_IP)")
	runCmd.Flags().String("rdma-threshold", "1MB", "Minimum object size for RDMA transfer, e.g. 0, 256KB, 1MB (env: RDMA_THRESHOLD_BYTES)")
	runCmd.Flags().Bool("rdma-fallback", false, "Fall back to HTTP if RDMA initialization fails (env: RDMA_FALLBACK_ENABLED)")
	runCmd.Flags().String("rdma-device", "auto", "RDMA device name or 'auto' for auto-detection (env: RDMA_DEVICE)")
	runCmd.Flags().String("rdma-log-level", "WARN", "RDMA native library log level (env: RDMA_LOG_LEVEL)")
	runCmd.Flags().Int64("rdma-timeout-ms", 30000, "RDMA operation timeout in milliseconds (env: RDMA_TIMEOUT_MS)")

	// Checksum Options
	runCmd.Flags().String("checksum", "",
		`Enable checksum validation with the specified algorithm: crc32, crc32c, sha1, sha256, crc64-nvme.
Omit to disable checksums. (env: SPT_CHECKSUM)`)

	// S3 Tables Options
	runCmd.Flags().String("test-vector", "tps", "Tables test vector: tps | compaction | catalog")
	runCmd.Flags().String("table-bucket", "spt-tables", "S3 Table bucket name")
	runCmd.Flags().String("namespace", "default", "Namespace within the table bucket")
	runCmd.Flags().String("table-name", "spt-bench", "Table name (auto-suffixed with timestamp if default)")
	runCmd.Flags().Int("concurrent-writers", 10, "Concurrent Iceberg commit threads")
	runCmd.Flags().Int("commit-freq-ms", 500, "Target ms between commits per writer")
	runCmd.Flags().String("target-file-size", "64MB", "Target Parquet file size (e.g. 64MB)")
	runCmd.Flags().String("ingest-file-size", "100KB", "Small Parquet file size for compaction seed")
	runCmd.Flags().String("total-ingest", "1GB", "Total data volume to ingest for compaction seed")
	runCmd.Flags().Int("namespace-count", 100, "Namespaces to create for catalog test")
	runCmd.Flags().Int("tables-per-ns", 100, "Tables per namespace for catalog test")
	runCmd.Flags().Int("read-concurrency", 10, "Concurrent catalog readers for catalog test")
	runCmd.Flags().String("compaction-timeout", "4h", "Max wait for compaction to complete")
	runCmd.Flags().Bool("no-provision", false, "Skip table bucket/namespace/table creation (reuse existing)")

	// Headless Mode Options
	// TUI Layout Options
	runCmd.Flags().Bool("minimal", false, "Start TUI with only the live stats panel visible (graphs and messages collapsed)")

	// Headless Mode Options
	runCmd.Flags().Bool("headless", false, "Force headless (non-interactive) mode")
	runCmd.Flags().String("trace-file", "", "Save all output to specified trace file")
	runCmd.Flags().Bool("trace-append", false, "Append to existing trace file (default: overwrite)")
	runCmd.Flags().Bool("verbose", false, "Show detailed Docker API calls and debug information")
}

// buildScenarioParams builds scenario parameters from command flags
func buildScenarioParams(workloadType string, cmd *cobra.Command) (scenario.Params, error) {
	params := scenario.Params{
		WorkloadType:        workloadType,
		ObjectDataDedupable: true,
	}

	// Get connection parameters (not required for mock)
	if workloadType != WorkloadTypeMock {
		endpoint, _ := cmd.Flags().GetString("endpoint")
		// Multi-endpoint support
		var endpoints []string
		if f := cmd.Flags().Lookup("endpoints"); f != nil {
			endpoints, _ = cmd.Flags().GetStringSlice("endpoints")
		}
		if f := cmd.Flags().Lookup("slice-endpoints"); f != nil {
			se, _ := cmd.Flags().GetBool("slice-endpoints")
			params.SliceEndpoints = se
		}

		combined := make([]string, 0, len(endpoints)+1)
		seen := make(map[string]struct{}, len(endpoints)+1)
		appendEndpoint := func(val string) {
			trimmed := strings.TrimSpace(val)
			if trimmed == "" {
				return
			}
			if _, ok := seen[trimmed]; ok {
				return
			}
			seen[trimmed] = struct{}{}
			combined = append(combined, trimmed)
		}

		appendEndpoint(endpoint)
		for _, ep := range endpoints {
			appendEndpoint(ep)
		}

		if len(combined) > 0 {
			params.Endpoints = combined
			if len(combined) == 1 {
				params.Endpoint = combined[0]
			}
		} else {
			params.Endpoint = strings.TrimSpace(endpoint)
		}

		accessKey, _ := cmd.Flags().GetString("access-key")
		params.AccessKey = accessKey

		secretKey, _ := cmd.Flags().GetString("secret-key")
		params.SecretKey = secretKey

		bucket, _ := cmd.Flags().GetString("bucket")
		params.Bucket = bucket

		prefixFlag := cmd.Flags().Lookup("prefix")
		if prefixFlag != nil {
			prefix, _ := cmd.Flags().GetString("prefix")
			params.Prefix = prefix
		}

		authVersionFlag := cmd.Flags().Lookup("auth-version")
		if authVersionFlag != nil {
			authVersion, _ := cmd.Flags().GetInt("auth-version")
			params.AuthVersion = authVersion
		}
	}

	// Get workload parameters
	threads, _ := cmd.Flags().GetInt("threads")
	params.Threads = threads

	objectSize, _ := cmd.Flags().GetString("object-size")
	params.ObjectSize = objectSize

	partSize, _ := cmd.Flags().GetString("part-size")
	params.PartSize = partSize

	mpuObjects, _ := cmd.Flags().GetInt("mpu-concurrent-objects")
	params.MpuObjects = mpuObjects

	mpuParts, _ := cmd.Flags().GetInt("mpu-concurrent-parts")
	params.MpuParts = mpuParts

	objectCount, _ := cmd.Flags().GetInt("object-count")
	params.ObjectCount = objectCount

	duration, _ := cmd.Flags().GetString("duration")
	params.Duration = duration

	// Get behavior flags
	cleanup, _ := cmd.Flags().GetBool("cleanup")
	params.Cleanup = cleanup

	seedObjects, _ := cmd.Flags().GetInt("seed-objects")
	params.SeedCount = seedObjects

	keepScenario, _ := cmd.Flags().GetBool("keep-scenario")
	params.KeepScenario = keepScenario

	saveItems, _ := cmd.Flags().GetBool("save-items")
	params.SaveItems = saveItems

	itemsFile, _ := cmd.Flags().GetString("items-file")
	params.ItemsFile = itemsFile

	readShuffle, _ := cmd.Flags().GetBool(flagReadShuffle)
	params.ReadShuffle = readShuffle

	readShuffleBatchSize, _ := cmd.Flags().GetInt(flagReadShuffleBatchSize)
	params.ReadShuffleBatchSize = readShuffleBatchSize

	serviceThreads, _ := cmd.Flags().GetInt("service-threads")
	params.ServiceThreads = serviceThreads

	engineOverrides, _ := cmd.Flags().GetStringArray(flagEngineOverride)
	if len(engineOverrides) > 0 {
		params.EngineOverrides = engineOverrides
	}

	// S3 Tables parameters
	if workloadType == WorkloadTypeTables {
		testVector, _ := cmd.Flags().GetString("test-vector")
		tableBucket, _ := cmd.Flags().GetString("table-bucket")
		ns, _ := cmd.Flags().GetString("namespace")
		tableName, _ := cmd.Flags().GetString("table-name")
		concurrentWriters, _ := cmd.Flags().GetInt("concurrent-writers")
		commitFreqMs, _ := cmd.Flags().GetInt("commit-freq-ms")
		namespaceCount, _ := cmd.Flags().GetInt("namespace-count")
		tablesPerNs, _ := cmd.Flags().GetInt("tables-per-ns")
		readConcurrency, _ := cmd.Flags().GetInt("read-concurrency")
		noProvision, _ := cmd.Flags().GetBool("no-provision")

		targetFileSizeStr, _ := cmd.Flags().GetString("target-file-size")
		targetFileSizeBytes, err := sizeparse.Parse(targetFileSizeStr)
		if err != nil {
			return params, fmt.Errorf("invalid --target-file-size %q: %w", targetFileSizeStr, err)
		}

		ingestFileSizeStr, _ := cmd.Flags().GetString("ingest-file-size")
		ingestFileSizeBytes, err := sizeparse.Parse(ingestFileSizeStr)
		if err != nil {
			return params, fmt.Errorf("invalid --ingest-file-size %q: %w", ingestFileSizeStr, err)
		}

		totalIngestStr, _ := cmd.Flags().GetString("total-ingest")
		totalIngestBytes, err := sizeparse.Parse(totalIngestStr)
		if err != nil {
			return params, fmt.Errorf("invalid --total-ingest %q: %w", totalIngestStr, err)
		}

		compactionTimeoutStr, _ := cmd.Flags().GetString("compaction-timeout")
		compactionTimeoutMs, err := parseTablesTimeoutMs(compactionTimeoutStr)
		if err != nil {
			return params, fmt.Errorf("invalid --compaction-timeout %q: %w", compactionTimeoutStr, err)
		}

		params.Tables = scenario.TablesParams{
			TestVector:          testVector,
			TableBucket:         tableBucket,
			Namespace:           ns,
			TableName:           tableName,
			ConcurrentWriters:   concurrentWriters,
			CommitFreqMs:        commitFreqMs,
			TargetFileSizeBytes: targetFileSizeBytes,
			IngestFileSizeBytes: ingestFileSizeBytes,
			TotalIngestBytes:    totalIngestBytes,
			NamespaceCount:      namespaceCount,
			TablesPerNs:         tablesPerNs,
			ReadConcurrency:     readConcurrency,
			CompactionTimeoutMs: compactionTimeoutMs,
			NoProvision:         noProvision,
		}
	}

	// S3 driver selection: --s3-driver takes precedence, --use-rdma is a synonym for --s3-driver rdma
	s3Driver, _ := cmd.Flags().GetString("s3-driver")
	useRdma, _ := cmd.Flags().GetBool("use-rdma")

	s3DriverExplicit := cmd.Flags().Changed("s3-driver")
	useRdmaExplicit := cmd.Flags().Changed("use-rdma")

	if s3DriverExplicit && useRdmaExplicit && useRdma && s3Driver != scenario.S3DriverRdma {
		return params, fmt.Errorf("conflicting flags: --s3-driver %q and --use-rdma cannot be used together", s3Driver)
	}

	if useRdma && !s3DriverExplicit {
		s3Driver = scenario.S3DriverRdma
	}

	// Validate the driver value
	switch s3Driver {
	case scenario.S3DriverDefault, scenario.S3DriverNetty, scenario.S3DriverAws, scenario.S3DriverRdma, "":
		// valid ("" treated as default)
	default:
		return params, fmt.Errorf("invalid --s3-driver value %q: must be one of: default, netty, aws, rdma", s3Driver)
	}

	params.S3Driver = s3Driver
	if s3Driver == scenario.S3DriverRdma {
		params.RdmaLocalIP, _ = cmd.Flags().GetString("rdma-local-ip")
		thresholdStr, _ := cmd.Flags().GetString("rdma-threshold")
		thresholdBytes, err := sizeparse.Parse(thresholdStr)
		if err != nil {
			return params, fmt.Errorf("invalid --rdma-threshold value %q: %w", thresholdStr, err)
		}
		params.RdmaThresholdBytes = thresholdBytes
		params.RdmaFallback, _ = cmd.Flags().GetBool("rdma-fallback")
		params.RdmaDevice, _ = cmd.Flags().GetString("rdma-device")
		params.RdmaLogLevel, _ = cmd.Flags().GetString("rdma-log-level")
		params.RdmaTimeoutMs, _ = cmd.Flags().GetInt64("rdma-timeout-ms")
	}

	// Checksum validation
	checksumAlgo, _ := cmd.Flags().GetString("checksum")
	if checksumAlgo != "" {
		checksumAlgo = strings.ToLower(strings.TrimSpace(checksumAlgo))
		switch checksumAlgo {
		case scenario.ChecksumCRC32, scenario.ChecksumCRC32C,
			scenario.ChecksumSHA1, scenario.ChecksumSHA256, scenario.ChecksumCRC64NVME:
			// valid
		default:
			return params, fmt.Errorf("invalid --checksum value %q: must be one of: crc32, crc32c, sha1, sha256, crc64-nvme", checksumAlgo)
		}
		params.Checksum = checksumAlgo
	}

	objectDataCompressibility, _ := cmd.Flags().GetFloat64("object-data-compressibility")
	if objectDataCompressibility < 0.0 || objectDataCompressibility > 100.0 {
		return params, fmt.Errorf("invalid --object-data-compressibility value %v: must be in range [0, 100]", objectDataCompressibility)
	}
	params.ObjectDataCompressibility = objectDataCompressibility
	objectDataDedupable, _ := cmd.Flags().GetBool("object-data-dedupable")
	params.ObjectDataDedupable = objectDataDedupable

	// TUI layout
	minimalTUI, _ := cmd.Flags().GetBool("minimal")
	params.MinimalTUI = minimalTUI

	// Set defaults (tables workload has no object size concept)
	if params.ObjectSize == "" && params.WorkloadType != WorkloadTypeList && params.WorkloadType != WorkloadTypeTables {
		params.ObjectSize = "1MB"
	}

	// Mixed workload distribution and seed file parameters
	if workloadType == WorkloadTypeMixed {
		params.GetDistrib, _ = cmd.Flags().GetInt("get-distrib")
		params.PutDistrib, _ = cmd.Flags().GetInt("put-distrib")
		params.DeleteDistrib, _ = cmd.Flags().GetInt("delete-distrib")
		params.StatDistrib, _ = cmd.Flags().GetInt("stat-distrib")
		params.ReadItemsFile, _ = cmd.Flags().GetString("read-items-file")
		params.DeleteItemsFile, _ = cmd.Flags().GetString("delete-items-file")
	}

	return params, nil
}

// parseTablesTimeoutMs parses a Go duration string to milliseconds for the tables compaction timeout.
func parseTablesTimeoutMs(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, err
	}
	return d.Milliseconds(), nil
}

// ResultsOptions captures Phase 1 results-related CLI settings.
type ResultsOptions struct {
	AutoResults        bool
	ResultsDir         string
	Label              string // sanitized label used for output root and step ID prefix
	Debug              bool
	ShutdownOnComplete bool
	ShutdownLingerSec  int
}

// buildResultsOptions reads results-related flags and returns sanitized options.
// Phase 1: parsing/validation only; no retrieval wiring yet.
func buildResultsOptions(cmd *cobra.Command) ResultsOptions {
	auto, _ := cmd.Flags().GetBool("auto-results")
	dir, _ := cmd.Flags().GetString("results-dir")
	rawLabel, _ := cmd.Flags().GetString("label")
	dbg, _ := cmd.Flags().GetBool("auto-results-debug")
	shutOn, _ := cmd.Flags().GetBool("shutdown-on-complete")
	shutLinger, _ := cmd.Flags().GetInt("shutdown-linger")

	return ResultsOptions{
		AutoResults:        auto,
		ResultsDir:         dir,
		Label:              sanitizeLabel(rawLabel),
		Debug:              dbg,
		ShutdownOnComplete: shutOn,
		ShutdownLingerSec:  shutLinger,
	}
}

// sanitizeLabel enforces the allowed character set and length for labels.
// Allowed: A–Z, a–z, 0–9, dot, underscore, hyphen. Others become '_'.
// Empty after trimming yields default "mt".
func sanitizeLabel(s string) string {
	const (
		allowed = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._-"
		maxLen  = 64
		def     = "mt"
	)

	// Trim whitespace
	s = strings.TrimSpace(s)
	if s == "" {
		return def
	}

	// Replace any disallowed rune with '_'
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if strings.ContainsRune(allowed, r) {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	out := b.String()

	// Enforce max length
	if len(out) > maxLen {
		out = out[:maxLen]
	}

	// Avoid empty (could happen if input had only spaces)
	if out == "" {
		return def
	}
	return out
}

// formatScenarioParams formats scenario parameters for display
func formatScenarioParams(params scenario.Params) string {
	var lines []string
	lines = append(lines, fmt.Sprintf("Workload Type: %s", params.WorkloadType))

	// Always show endpoint info, even if empty (except for mock)
	if params.WorkloadType != WorkloadTypeMock {
		lines = appendS3DisplayLines(lines, params)
	}

	// Always show all workload parameters
	lines = append(lines, fmt.Sprintf("Threads: %d", params.Threads))
	if params.WorkloadType == WorkloadTypeList {
		lines = append(lines, "Object Size: (not applicable)")
	} else {
		lines = append(lines, fmt.Sprintf("Object Size: %s", params.ObjectSize))
	}

	// Show part size if multipart upload is enabled
	if params.PartSize != "" {
		lines = append(lines, fmt.Sprintf("Part Size: %s (multipart upload)", params.PartSize))
	}

	// Always show object count (0 means not set)
	if params.ObjectCount > 0 {
		lines = append(lines, fmt.Sprintf("Object Count: %d", params.ObjectCount))
	} else {
		lines = append(lines, "Object Count: (not set)")
	}

	// Always show duration
	if params.Duration != "" {
		lines = append(lines, fmt.Sprintf("Duration: %s", params.Duration))
	} else {
		lines = append(lines, "Duration: (not set)")
	}

	// Show seed count for read workloads
	if params.WorkloadType == WorkloadTypeRead {
		seedCount := params.SeedCount
		if seedCount <= 0 {
			seedCount = 2500
		}
		lines = append(lines, fmt.Sprintf("Seed Objects: %d", seedCount))
	}

	// Always show cleanup status
	if params.Cleanup {
		lines = append(lines, "Cleanup: Yes (delete objects after test)")
	} else {
		lines = append(lines, "Cleanup: No")
	}

	// Always show keep-scenario status
	if params.KeepScenario {
		lines = append(lines, "Keep Scenario: Yes")
	} else {
		lines = append(lines, "Keep Scenario: No")
	}

	return strings.Join(lines, "\n")
}

// seedSizeWarnBytes is 50 GB — warn if the total seed footprint exceeds this.
const seedSizeWarnBytes = 50 * 1024 * 1024 * 1024

// warnSeedSize emits a warning if seed phase will write more than 50 GB.
func warnSeedSize(params scenario.Params) {
	if params.ObjectSize == "" {
		return
	}
	objBytes, err := sizeparse.Parse(params.ObjectSize)
	if err != nil || objBytes <= 0 {
		return // skip warning if we can't parse
	}
	seedCount := params.SeedCount
	if seedCount <= 0 {
		seedCount = 2500
	}
	totalBytes := objBytes * int64(seedCount)
	if totalBytes > seedSizeWarnBytes {
		totalGB := float64(totalBytes) / float64(1024*1024*1024)
		fmt.Printf("Warning: seed phase will write ~%.0fGB (%d x %s).\n", totalGB, seedCount, params.ObjectSize)
		fmt.Println("This may take significant time before the read benchmark begins.")
		fmt.Println("Consider reducing --seed-objects or --object-size for faster test startup.")
	}
}

// appendS3DisplayLines appends S3-related display lines for non-mock workloads.
func appendS3DisplayLines(lines []string, params scenario.Params) []string {
	// Prefer multi-endpoint summary when provided
	if len(params.Endpoints) > 0 && params.Endpoint == "" {
		shown := params.Endpoints
		if len(shown) > 3 {
			shown = shown[:3]
		}
		more := ""
		if len(params.Endpoints) > len(shown) {
			more = fmt.Sprintf(" (+%d more)", len(params.Endpoints)-len(shown))
		}
		lines = append(lines, fmt.Sprintf("Endpoints: %d total %s", len(params.Endpoints), more))
	} else {
		if params.Endpoint != "" {
			lines = append(lines, fmt.Sprintf("Endpoint: %s", params.Endpoint))
		} else {
			lines = append(lines, "Endpoint: (not set)")
		}
	}

	if params.Bucket != "" {
		lines = append(lines, fmt.Sprintf("Bucket: %s", params.Bucket))
	} else {
		lines = append(lines, "Bucket: (not set)")
	}

	if params.WorkloadType == WorkloadTypeList {
		if params.Prefix != "" {
			lines = append(lines, fmt.Sprintf("Prefix: %s", params.Prefix))
		} else {
			lines = append(lines, "Prefix: (not set)")
		}
	}

	authVersion := params.AuthVersion
	if authVersion == 0 {
		authVersion = 4
	}
	lines = append(lines, fmt.Sprintf("Auth Version: %d", authVersion))

	// Always show access key status
	if params.AccessKey != "" {
		masked := maskedPlaceholder
		if len(params.AccessKey) > 3 {
			masked = params.AccessKey[:3] + maskedPlaceholder
		}
		lines = append(lines, fmt.Sprintf("Access Key: %s", masked))
	} else {
		lines = append(lines, "Access Key: (not set)")
	}

	// Always show secret key status
	if params.SecretKey != "" {
		masked := maskedPlaceholder
		if len(params.SecretKey) > 3 {
			masked = params.SecretKey[:3] + maskedPlaceholder
		}
		lines = append(lines, fmt.Sprintf("Secret Key: %s", masked))
	} else {
		lines = append(lines, "Secret Key: (not set)")
	}

	return lines
}
