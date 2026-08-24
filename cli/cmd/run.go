/*
Copyright © 2025 Dell Technologies
*/
//revive:disable:package-comments
package cmd

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/config"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/deletemetrics"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/integrityplan"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/preflight"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/internal/secretmask"
	"github.com/dell/storage-performance-tool/cli/internal/sizeparse"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
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
	flagReadPhasePauseSeconds = "read-phase-pause-seconds"
	flagEngineOverride        = "engine-override"
	flagPrefixShards          = "prefix-shards"
	itemNamingShardsPath      = "item.naming.shards"
	prefixShardsAuto          = -1
)

// resolvePortConflictFunc is a test seam for port conflict resolution.
// In production it points to portcheck.ResolvePortConflict; tests can override.
var resolvePortConflictFunc = portcheck.ResolvePortConflict

// validateRunWorkloadTypeFunc keeps command-path tests behind the same workload gate as
// production while allowing prerelease workload slices to exercise later safety seams.
var validateRunWorkloadTypeFunc = ValidateWorkloadType

type autoResultsRunTracker interface {
	WaitForCompletion(context.Context, []string) (*portcheck.RunResult, error)
	SetDebug(bool)
	SetExpectedRunID(int64)
	SetRequireTerminalState(bool)
}

type runTrackerAdapter struct {
	*portcheck.RunTracker
}

func (r *runTrackerAdapter) SetDebug(debug bool) {
	r.Debug = debug
}

func (r *runTrackerAdapter) SetRequireTerminalState(required bool) {
	r.RequireTerminalState = required
}

type autoResultsFetcher interface {
	FetchArtifactsForSteps(context.Context, []string) (*results.Manifest, error)
}

type fetcherAdapter struct {
	*results.Fetcher
}

// autoResultsOutcome combines the shared lifecycle result with integrity-specific evidence.
type autoResultsOutcome struct {
	Lifecycle            runcontrol.Outcome
	Tracker              *portcheck.RunResult
	TrackerErr           error
	ArtifactErr          error
	DeleteMetricsErr     error
	DeleteTerminalErr    error
	Finalization         *integrity.FinalizeOutcome
	FinalizationErr      error
	ObservedCorruptCount *int64
	CorruptionMetricsErr error
	SummaryErr           error
	ShutdownErr          error
	StepIDs              []string
}

type postRunBudgets struct {
	Artifacts      time.Duration
	CancelSalvage  time.Duration
	Shutdown       time.Duration
	PreparedInputs time.Duration
	Summary        time.Duration
}

var autoResultsPhaseBudgets = postRunBudgets{
	Artifacts:      constants.AutoResultsArtifactTimeout,
	CancelSalvage:  constants.AutoResultsCancelCleanupTimeout,
	Shutdown:       constants.AutoResultsShutdownTimeout,
	PreparedInputs: constants.ContainerCleanupTimeout,
	Summary:        constants.AutoResultsSummaryTimeout,
}

// autoResultsMonitor separates monitor construction from the launch boundary.
// No engine evidence is consumed until Arm is called.
type autoResultsMonitor struct {
	done        chan autoResultsOutcome
	armed       chan struct{}
	armAck      chan struct{}
	armOnce     sync.Once
	cleanupOnly atomic.Bool
	cancel      context.CancelFunc
	sessionMu   sync.RWMutex
	session     *runcontrol.Session
}

func (m *autoResultsMonitor) Arm() {
	if m == nil {
		return
	}
	m.armOnce.Do(func() { close(m.armed) })
	if m.armAck != nil {
		<-m.armAck
	}
}

func (m *autoResultsMonitor) Cancel() {
	if m == nil {
		return
	}
	resolveCleanupOnly := false
	m.armOnce.Do(func() {
		m.cleanupOnly.Store(true)
		resolveCleanupOnly = true
	})
	if m.cancel != nil {
		m.cancel()
	}
	if resolveCleanupOnly && m.armed != nil {
		close(m.armed)
	}
}

// BindSession connects tracker completion to presentation adapters without
// transferring resource-disposal authority away from the run session.
func (m *autoResultsMonitor) BindSession(session *runcontrol.Session) {
	if m == nil {
		return
	}
	m.sessionMu.Lock()
	m.session = session
	m.sessionMu.Unlock()
}

func (m *autoResultsMonitor) markWorkloadTerminal() {
	if m == nil {
		return
	}
	m.sessionMu.RLock()
	session := m.session
	m.sessionMu.RUnlock()
	if session != nil {
		session.MarkWorkloadTerminal()
	}
}

func configureAutoResultsTracker(tracker autoResultsRunTracker, expectedRunID int64, debug, requireTerminal bool) {
	tracker.SetExpectedRunID(expectedRunID)
	tracker.SetDebug(debug)
	tracker.SetRequireTerminalState(requireTerminal)
}

func recordTrackedStepLifecycles(metadata *runMetadata, result *portcheck.RunResult) {
	if metadata == nil || result == nil {
		return
	}
	metadata.StepLifecycles = make(map[string]string, len(result.Steps))
	for stepID, step := range result.Steps {
		metadata.StepLifecycles[stepID] = string(step.Lifecycle)
	}
}

func captureStoredDeleteMetrics(
	ctx context.Context,
	baseURL string,
	expectedRunID int64,
	runtimeStepIDs []string,
	metadata *runMetadata,
) (captureErr error) {
	if metadata == nil || metadata.WorkloadType != WorkloadTypeDelete {
		return nil
	}
	defer func() {
		if captureErr != nil {
			metadata.DeleteMetrics = nil
			metadata.DeleteMetricsError = captureErr.Error()
		} else {
			metadata.DeleteMetricsError = ""
		}
	}()
	expectedDeleteSteps, err := bindRuntimeDeleteSteps(metadata.ExpectedStepIDs, runtimeStepIDs)
	if err != nil {
		return fmt.Errorf("capture terminal DELETE metrics: %w", err)
	}
	metadata.DeleteArtifactStepIDs = append([]string(nil), expectedDeleteSteps...)
	expectedContributorIDs, err := expectedDeleteContributorIDs(metadata)
	if err != nil {
		return fmt.Errorf("capture terminal DELETE metrics: %w", err)
	}
	captured, err := captureDeleteMetricsFunc(
		ctx, baseURL, expectedRunID, expectedDeleteSteps, expectedContributorIDs,
		len(expectedContributorIDs) == 1)
	if err != nil {
		return fmt.Errorf("capture terminal DELETE metrics: %w", err)
	}
	if len(captured) != len(expectedDeleteSteps) {
		return fmt.Errorf(
			"capture terminal DELETE metrics: captured %d of %d expected steps",
			len(captured), len(expectedDeleteSteps))
	}
	for _, stepID := range expectedDeleteSteps {
		if captured[stepID] == nil {
			return fmt.Errorf("capture terminal DELETE metrics: expected step %q is missing", stepID)
		}
	}
	metadata.DeleteMetrics = captured
	return nil
}

func terminalDeleteOutcomeError(metrics map[string]*deletemetrics.Metrics) error {
	failedSteps := make([]string, 0)
	for stepID, metric := range metrics {
		if metric != nil && metric.FailurePolicy.Outcome == deletemetrics.OutcomeFailed {
			failedSteps = append(failedSteps, stepID)
		}
	}
	if len(failedSteps) == 0 {
		return nil
	}
	sort.Strings(failedSteps)
	return fmt.Errorf(
		"DELETE failure policy rejected terminal outcome for step(s): %s",
		strings.Join(failedSteps, ", "))
}

func expectedDeleteContributorIDs(metadata *runMetadata) ([]string, error) {
	if metadata == nil {
		return nil, fmt.Errorf("expected DELETE contributor identity is unavailable")
	}
	if metadata.deleteContributors != nil {
		contributorIDs, err := metadata.deleteContributors()
		if err != nil {
			return nil, fmt.Errorf("resolve DELETE contributor identity: %w", err)
		}
		if len(contributorIDs) == 0 {
			return nil, fmt.Errorf("expected DELETE contributor identity is unavailable")
		}
		return append([]string(nil), contributorIDs...), nil
	}
	if len(metadata.Hosts) == 1 {
		return []string{constants.MetricsLocalContributorID}, nil
	}
	return nil, fmt.Errorf("expected DELETE contributor identity is unavailable for fleet capture")
}

// bindRuntimeDeleteSteps binds the planned DELETE role to runtime identity by
// stable scenario ordinal. Runtime timestamps may differ from the generated
// plan, so planned IDs must never be used when discovery evidence is present.
func bindRuntimeDeleteSteps(plannedStepIDs, runtimeStepIDs []string) ([]string, error) {
	plannedDeleteNumbers := make([]int, 0, 1)
	plannedDeletes := make([]string, 0, 1)
	for _, stepID := range plannedStepIDs {
		stepID = strings.TrimSpace(stepID)
		if !strings.HasSuffix(strings.ToLower(stepID), "-delete") {
			continue
		}
		number, ok := integrityplan.RuntimeStepNumber(stepID)
		if !ok {
			return nil, fmt.Errorf("planned DELETE step identity %q has no stable ordinal", stepID)
		}
		plannedDeleteNumbers = append(plannedDeleteNumbers, number)
		plannedDeletes = append(plannedDeletes, stepID)
	}
	if len(plannedDeletes) == 0 {
		return nil, fmt.Errorf("expected DELETE step identity is unavailable")
	}
	if len(runtimeStepIDs) == 0 {
		return plannedDeletes, nil
	}
	byNumber := make(map[int]string, len(runtimeStepIDs))
	seen := make(map[string]struct{}, len(runtimeStepIDs))
	for _, stepID := range runtimeStepIDs {
		stepID = strings.TrimSpace(stepID)
		if stepID == "" {
			return nil, fmt.Errorf("runtime step identity is empty")
		}
		if _, duplicate := seen[stepID]; duplicate {
			return nil, fmt.Errorf("runtime step identity %q is duplicated", stepID)
		}
		seen[stepID] = struct{}{}
		number, ok := integrityplan.RuntimeStepNumber(stepID)
		if !ok {
			continue
		}
		if existing, conflict := byNumber[number]; conflict {
			return nil, fmt.Errorf(
				"runtime step identities %q and %q conflict for ordinal %d", existing, stepID, number)
		}
		byNumber[number] = stepID
	}
	bound := make([]string, 0, len(plannedDeletes))
	for i, number := range plannedDeleteNumbers {
		stepID := byNumber[number]
		if stepID == "" {
			return nil, fmt.Errorf("runtime evidence is missing planned DELETE step %q", plannedDeletes[i])
		}
		bound = append(bound, stepID)
	}
	return bound, nil
}

// markDiscoveredStoppedStepsStarted reconciles runtime discovery with a
// STOPPED status carrier. The engine's top-level status may not name the
// scenario step, while metrics discovery only reports steps which actually
// started. Preserve that stronger evidence without inventing downstream work.
func markDiscoveredStoppedStepsStarted(result *portcheck.RunResult, discoveredStepIDs []string) {
	if result == nil || result.FinalState != constants.StateStopped {
		return
	}
	for _, stepID := range discoveredStepIDs {
		step, ok := result.Steps[stepID]
		if !ok || (step.Lifecycle != portcheck.StepLifecycleNotStarted &&
			step.Lifecycle != portcheck.StepLifecyclePlanned) {
			continue
		}
		step.Lifecycle = portcheck.StepLifecycleStarted
		step.Started = true
		result.Steps[stepID] = step
	}
}

func mergeTrackedRunResults(observed, terminal *portcheck.RunResult) *portcheck.RunResult {
	if terminal == nil {
		return observed
	}
	if observed == nil {
		return terminal
	}
	if observed.FinalState == constants.StateFailed && terminal.FinalState != constants.StateFailed {
		terminal.FinalState = observed.FinalState
		terminal.FailureStepID = observed.FailureStepID
		terminal.FailureCategory = observed.FailureCategory
		terminal.FailureMessage = observed.FailureMessage
	}
	if terminal.RunID == 0 {
		terminal.RunID = observed.RunID
	}
	if terminal.Steps == nil {
		terminal.Steps = make(map[string]portcheck.StepCompletion, len(observed.Steps))
	}
	for stepID, prior := range observed.Steps {
		current, ok := terminal.Steps[stepID]
		if !ok || trackedLifecycleRank(prior.Lifecycle) > trackedLifecycleRank(current.Lifecycle) {
			terminal.Steps[stepID] = prior
		}
	}
	return terminal
}

func trackedLifecycleRank(lifecycle portcheck.StepLifecycle) int {
	const (
		lifecycleRankUnknown = iota
		lifecycleRankNotStarted
		lifecycleRankStarted
		lifecycleRankCompleted
		lifecycleRankFailed
	)

	switch lifecycle {
	case portcheck.StepLifecycleFailed:
		return lifecycleRankFailed
	case portcheck.StepLifecycleCompleted:
		return lifecycleRankCompleted
	case portcheck.StepLifecycleStarted:
		return lifecycleRankStarted
	case portcheck.StepLifecycleNotStarted:
		return lifecycleRankNotStarted
	default:
		return lifecycleRankUnknown
	}
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
	prepareDistributedIntegrityRuntimeIdentityFunc = func(
		ctx context.Context, orchestrator *tui.MultiHostOrchestrator, image string,
	) (tui.DistributedRuntimeIdentityEvidence, error) {
		return orchestrator.PrepareDistributedIntegrityRuntimeIdentity(ctx, image)
	}
	startLocalHeadlessRunFunc     = headless.StartHeadlessModeWithParams
	startMultiHostHeadlessRunFunc = headless.StartHeadlessModeWithOrchestrator
	startLocalTUIRunFunc          = tui.StartTUIWithScenarioRunOptions
	startMultiHostTUIRunFunc      = tui.StartTUIWithMultiHostRunOptions
	startAutoResultsFunc          = startAutoResultsMonitor
	discoverStepIDsFunc           = results.DiscoverStepIDsForRunContext
	discoverFleetStepIDsFunc      = results.DiscoverFleetStepIDsForRunContext
	captureDeleteMetricsFunc      = results.CaptureTerminalDeleteMetricsForRunContext
	newResultsFetcherFunc         = func(baseURL, outputDir string) autoResultsFetcher {
		return &fetcherAdapter{results.NewFetcher(baseURL, outputDir)}
	}
	makeResultsDirFunc           = os.MkdirAll
	archivePreparedRunInputsFunc = archivePreparedRunInputs
	generateRunSummaryFunc       = generateRunSummary
	requestShutdownAllFunc       = requestShutdownAll
)

var integrityRuntimeGOOS = runtime.GOOS

const integritySupportedGOOS = "linux"

func prepareExternalItemFilesForRun(params scenario.Params) (scenario.Params, error) {
	if (scenario.IsIntegrityWorkload(params) ||
		(params.WorkloadType == WorkloadTypeDelete && params.ItemsFile != "")) &&
		integrityRuntimeGOOS != integritySupportedGOOS {
		return params, fmt.Errorf(
			"%s is unsupported on %s: crash-durable verification evidence requires "+
				"a parent-directory synchronization primitive; use the Linux CLI",
			params.WorkloadType,
			integrityRuntimeGOOS,
		)
	}
	return scenario.PrepareExternalItemFiles(params)
}

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
func startAutoResultsMonitor(parentCtx context.Context, baseURL, label, resultsDir string, expectedStepIDs []string, expectedRunID int64, debug bool, allHosts []*hostparse.HostInfo, apiPort string, shutdownOn bool, lingerSec int, scenarioPath string, metadata *runMetadata, progressOut io.Writer, summaryOut io.Writer, traceFile string, preSummaryHook func(context.Context), integrityOptions ...*integrity.FinalizeOptions) *autoResultsMonitor {
	if parentCtx == nil {
		parentCtx = context.Background()
	}
	monitorCtx, cancel := context.WithCancel(parentCtx)
	done := make(chan autoResultsOutcome, 1)
	monitor := &autoResultsMonitor{
		done: done, armed: make(chan struct{}), armAck: make(chan struct{}), cancel: cancel,
	}
	go func() {
		outcome := autoResultsOutcome{
			Lifecycle: runcontrol.Outcome{
				Resources: runcontrol.ResourceDispositionUnknown,
			},
		}
		defer func() {
			done <- outcome
			cancel()
		}()
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
		if err := makeResultsDirFunc(root, 0o750); err != nil {
			outcome.ArtifactErr = errors.Join(
				outcome.ArtifactErr, fmt.Errorf("create results directory: %w", err))
			logging.LogError("auto-results", "create results dir", err, "path", root)
		} else {
			if archiveErr := archivePreparedRunInputsFunc(metadata, scenarioPath, root); archiveErr != nil {
				outcome.ArtifactErr = errors.Join(
					outcome.ArtifactErr, fmt.Errorf("archive prepared inputs: %w", archiveErr))
				logging.LogError("auto-results", "archive prepared inputs", archiveErr, "dest", root)
			}
		}
		<-monitor.armed
		if monitor.armAck != nil {
			close(monitor.armAck)
		}
		parentCtx = monitorCtx
		phaseBase := context.WithoutCancel(parentCtx)
		verificationRun := len(integrityOptions) > 0 && integrityOptions[0] != nil
		shutdownPerformed := false
		runShutdown := func(reconcileTerminal bool) {
			shutdownPerformed = true
			shutdownCtx, cancelShutdown := context.WithTimeout(phaseBase, autoResultsPhaseBudgets.Shutdown)
			defer cancelShutdown()
			outcome.Lifecycle.Shutdown.Started = true
			writeProgress("Auto-results: requesting shutdown on all hosts...\n")
			if lingerSec <= 0 {
				lingerSec = int(constants.APILingerDefault / time.Second)
			}

			type reconciliationOutcome struct {
				result *portcheck.RunResult
				err    error
			}
			var reconciliationDone chan reconciliationOutcome
			if reconcileTerminal {
				reconciliationDone = make(chan reconciliationOutcome, 1)
				go func() {
					reconciler := newRunTrackerFunc(baseURL)
					configureAutoResultsTracker(reconciler, expectedRunID, debug, true)
					result, err := reconciler.WaitForCompletion(shutdownCtx, expectedStepIDs)
					reconciliationDone <- reconciliationOutcome{result: result, err: err}
				}()
			}

			outcome.ShutdownErr = requestShutdownAllFunc(
				shutdownCtx, allHosts, apiPort, time.Duration(lingerSec)*time.Second, expectedRunID, debug)
			if reconciliationDone != nil {
				reconciled := <-reconciliationDone
				if reconciled.result != nil {
					outcome.Tracker = mergeTrackedRunResults(outcome.Tracker, reconciled.result)
				}
				if reconciled.err != nil {
					outcome.TrackerErr = fmt.Errorf(
						"post-shutdown terminal reconciliation: %w", reconciled.err)
				} else {
					outcome.TrackerErr = nil
				}
			}
			outcome.Lifecycle.Shutdown = runcontrol.CompletedPhase(outcome.ShutdownErr)
			if outcome.ShutdownErr != nil {
				logging.LogError("auto-results", "shutdown encountered issues", outcome.ShutdownErr)
				writeProgress("Shutdown completed with warnings; see logs for details.\n")
			} else {
				writeProgress("Shutdown completed successfully.\n")
			}
		}
		cleanupOnly := monitor.cleanupOnly.Load()
		if cleanupOnly {
			outcome.TrackerErr = monitorCtx.Err()
			writeProgress("Auto-results: launch did not establish trusted evidence; skipping ordinary tracking and finalizing resources.\n")
		} else {
			writeProgress("Auto-results: launch armed for run %d\n", expectedRunID)
			tracker := newRunTrackerFunc(baseURL)
			configureAutoResultsTracker(tracker, expectedRunID, debug, verificationRun)
			// Wait for terminal run state; step IDs discovered later via metrics/json
			if len(expectedStepIDs) > 0 {
				writeProgress("Auto-results: expecting %d step(s)\n", len(expectedStepIDs))
			}
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
				ticker := time.NewTicker(constants.AutoResultsDiscoveryInterval)
				defer ticker.Stop()
				for {
					select {
					case <-stopCh:
						return
					case <-parentCtx.Done():
						return
					case <-ticker.C:
						if ids, err := discoverStepIDsFunc(parentCtx, baseURL, expectedRunID); err == nil && len(ids) > 0 {
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
						if fids, err := discoverFleetStepIDsFunc(parentCtx, baseURL, expectedRunID); err == nil && len(fids) > 0 {
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
			outcome.Lifecycle.Workload.Started = true
			outcome.Tracker, outcome.TrackerErr = tracker.WaitForCompletion(parentCtx, expectedStepIDs)
			close(stopCh)
			// Wait for the polling goroutine to actually observe stopCh and return
			// before touching discoverStepIDsFunc/discoverFleetStepIDsFunc below.
			pollWG.Wait()

			// Cancellation can leave the engine RUNNING. Stop and reconcile the
			// owned run before deciding which artifacts and roles are applicable.
			interruptedTracking := parentCtx.Err() != nil && outcome.TrackerErr != nil
			if interruptedTracking && shutdownOn {
				runShutdown(true)
			}
			outcome.Lifecycle.Workload = runcontrol.CompletedPhase(outcome.TrackerErr)
			monitor.markWorkloadTerminal()
			if outcome.TrackerErr != nil {
				logging.LogError("auto-results", "completion tracking failed", outcome.TrackerErr)
			}
			outcome.Lifecycle.Artifacts.Started = true
			artifactBudget := autoResultsPhaseBudgets.Artifacts
			if parentCtx.Err() != nil || outcome.TrackerErr != nil {
				artifactBudget = autoResultsPhaseBudgets.CancelSalvage
			}
			artifactCtx, cancelArtifacts := context.WithTimeout(phaseBase, artifactBudget)
			defer cancelArtifacts()
			// One final fleet discovery after completion (may not have been picked up during polling)
			if fids, err := discoverFleetStepIDsFunc(artifactCtx, baseURL, expectedRunID); err == nil && len(fids) > 0 {
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

			// Discovery is already filtered to expectedRunID. Fleet and node metrics
			// therefore describe execution facts and may legitimately use IDs that
			// differ from the CLI-generated plan.
			mu.Lock()
			cachedDiscovered := append([]string(nil), discovered...)
			cachedFleet := append([]string(nil), fleetDiscovered...)
			mu.Unlock()
			stepIDs, runtimeStepIDs := selectResultStepIDs(expectedStepIDs, cachedFleet, cachedDiscovered)
			var discoverErr error
			if len(runtimeStepIDs) == 0 {
				nodeFallback, nodeErr := discoverStepIDsFunc(artifactCtx, baseURL, expectedRunID)
				fleetFallback, fleetErr := discoverFleetStepIDsFunc(artifactCtx, baseURL, expectedRunID)
				stepIDs, runtimeStepIDs = selectResultStepIDs(expectedStepIDs, fleetFallback, nodeFallback)
				if nodeErr != nil || fleetErr != nil {
					discoverErr = errors.Join(nodeErr, fleetErr)
				}
			}
			outcome.StepIDs = append([]string(nil), stepIDs...)
			markDiscoveredStoppedStepsStarted(outcome.Tracker, runtimeStepIDs)
			recordTrackedStepLifecycles(metadata, outcome.Tracker)
			artifactsReady := len(stepIDs) > 0
			if len(stepIDs) == 0 {
				var logErr error
				if discoverErr != nil {
					logErr = fmt.Errorf("discover step IDs: %w", discoverErr)
				} else {
					logErr = fmt.Errorf("no steps reported by metrics/json or metrics/fleet/json")
				}
				outcome.ArtifactErr = errors.Join(outcome.ArtifactErr, logErr)
				logging.LogError("auto-results", "no step IDs discovered; skipping fetch", logErr)
				writeProgress("Results fetch could not start; preserving run metadata and continuing shutdown. Output: %s\n", root)
			} else {
				writeProgress("Auto-results: fetching %d step(s): %s\n", len(stepIDs), strings.Join(stepIDs, ", "))
			}

			var observedRuntimeRoles integrity.StepRoles
			var plannedRoles integrity.StepRoles
			if verificationRun {
				plan := integrityOptions[0].Plan
				deferredVerification := plan.Valid() && plan.VerificationDeferred()
				plannedRoles = integrity.PlannedStepRoles(plan)
				if plan.Valid() {
					allowMissingVerifier := stepRoleLifecycle(
						outcome.Tracker, "", plannedRoles.Read) == portcheck.StepLifecycleNotStarted
					binding, bindErr := integrity.BindObservedStepRoles(plan, runtimeStepIDs, allowMissingVerifier)
					if bindErr != nil {
						outcome.ArtifactErr = errors.Join(outcome.ArtifactErr, bindErr)
						if !deferredVerification {
							outcome.CorruptionMetricsErr = bindErr
						}
						artifactsReady = false
					} else {
						observedRuntimeRoles = binding.Roles
					}
				} else {
					observedRuntimeRoles = integrity.ResolveStepRoles(stepIDs, nil)
					plannedRoles = integrity.ResolveStepRoles(expectedStepIDs, nil)
				}
				if !deferredVerification {
					runtimeRoles := observedRuntimeRoles
					readLifecycle := stepRoleLifecycle(outcome.Tracker, runtimeRoles.Read, plannedRoles.Read)
					// A dependent READ which never started cannot publish runtime metrics.
					// Its planned lifecycle remains part of finalization, while runtime
					// evidence and ActualStepIDs stay exact.
					if readLifecycle != portcheck.StepLifecycleNotStarted {
						if runtimeRoles.Read == "" {
							outcome.CorruptionMetricsErr = fmt.Errorf("verification READ step could not be identified")
						} else {
							corrupt, observeErr := integrity.ObserveJSONCorruptCountContext(artifactCtx, baseURL, runtimeRoles.Read)
							outcome.CorruptionMetricsErr = observeErr
							if observeErr == nil {
								outcome.ObservedCorruptCount = &corrupt
							}
						}
					}
				}
			}

			if metadata != nil {
				metadata.ActualStepIDs = append([]string(nil), stepIDs...)
				metadata.DiscoveredStepIDs = uniqueStepIDs(metadata.DiscoveredStepIDs, runtimeStepIDs)
				if captureErr := captureStoredDeleteMetrics(
					artifactCtx, baseURL, expectedRunID, runtimeStepIDs, metadata,
				); captureErr != nil {
					outcome.DeleteMetricsErr = captureErr
					outcome.ArtifactErr = errors.Join(outcome.ArtifactErr, captureErr)
				} else {
					outcome.DeleteTerminalErr = terminalDeleteOutcomeError(metadata.DeleteMetrics)
				}
				if err := writeRunMetadata(metadata, root); err != nil {
					logging.LogError("auto-results", "write run metadata", err, "dest", root)
				}
			}
			integrityFinalizationReady := artifactsReady
			if artifactsReady {
				fetch := newResultsFetcherFunc(baseURL, root)
				if _, ferr := fetch.FetchArtifactsForSteps(artifactCtx, stepIDs); ferr != nil {
					outcome.ArtifactErr = errors.Join(outcome.ArtifactErr, ferr)
					logging.LogError("auto-results", "artifact fetch failed", ferr, "base_url", baseURL, "out", root)
					writeProgress("Results fetch encountered errors; preserving available evidence and continuing shutdown. Output: %s\n", root)
				}
			}
			if integrityFinalizationReady && verificationRun {
				options := *integrityOptions[0]
				if outcome.Tracker != nil {
					options.StepLifecycles = make(map[string]string, len(outcome.Tracker.Steps))
					for stepID, step := range outcome.Tracker.Steps {
						options.StepLifecycles[stepID] = string(step.Lifecycle)
					}
				}
				options.ResultsRoot = root
				options.BaseURL = baseURL
				options.StepIDs = append([]string(nil), stepIDs...)
				options.PlannedStepIDs = append([]string(nil), expectedStepIDs...)
				options.ObservedStepIDs = append([]string(nil), runtimeStepIDs...)
				options.AllowPlannedRoleFallback = len(runtimeStepIDs) == 0
				options.AllowMissingRuntimeVerifier = stepRoleLifecycle(
					outcome.Tracker, observedRuntimeRoles.Read, plannedRoles.Read) == portcheck.StepLifecycleNotStarted
				options.Context = artifactCtx
				finalized, finalizeErr := integrity.FinalizeResults(options)
				outcome.Finalization = &finalized
				outcome.FinalizationErr = finalizeErr
				if finalizeErr != nil {
					logging.LogError("auto-results", "integrity result finalization failed", finalizeErr, "out", root)
					writeProgress("Integrity result finalization failed; preserved evidence under %s\n", root)
				} else {
					if finalized.VerificationDeferred {
						writeProgress("Integrity: verification deferred; selected=%d attempted=0 verified=0\n",
							finalized.SelectionCount)
					} else {
						writeProgress("Integrity: selected=%d attempted=%d verified=%d remaining=%d corrupt=%d\n",
							finalized.SelectionCount, finalized.VerificationAttemptedCount, finalized.VerifiedCount, finalized.RemainingCount, finalized.CorruptCount)
					}
					digest := finalized.DigestPerformance
					writeProgress("Integrity digest work: objects=%d bytes=%d worker_seconds=%.6f mean_worker_mib_per_second=%.3f additional_payload_passes=%d\n",
						digest.Objects, digest.Bytes, digest.HashWorkerSeconds, digest.MeanWorkerHashMiBPerSecond, digest.AdditionalPayloadPasses)
					if digest.InitialWriteDelaySecondsMaxNode != nil {
						writeProgress("Integrity initial write delay: maximum_node_seconds=%.6f\n", *digest.InitialWriteDelaySecondsMaxNode)
					}
					for _, sample := range finalized.FailureSamples {
						writeProgress("Integrity failure: key=%q version=%q returned_version=%q reason=%s expected_digest=%s actual_digest=%s expected_size=%s actual_size=%s request_id=%q\n",
							sample.Key, sample.RequestedVersion, sample.ReturnedVersion, sample.Reason, sample.ExpectedDigest, sample.ActualDigest, sample.ExpectedSize, sample.ActualSize, sample.RequestID)
					}
					writeProgress("Integrity artifacts: %s\n", root)
				}
			}
			if traceFile != "" {
				if err := appendTraceToResultsManifest(root, traceFile); err != nil {
					logging.LogError("auto-results", "trace artifact append failed", err, "trace_file", traceFile, "out", root)
				}
			}
			writeProgress("Auto-results: results saved to %s\n", root)
			outcome.Lifecycle.Artifacts.Completed = true
			outcome.Lifecycle.Artifacts.Err = errors.Join(outcome.ArtifactErr, outcome.CorruptionMetricsErr, outcome.FinalizationErr)
		}

		// Normal terminal runs collect their completed evidence before shutdown.
		// Interrupted runs already shut down and reconciled before salvage.
		if shutdownOn && !shutdownPerformed {
			if !cleanupOnly {
				settleTimer := time.NewTimer(constants.AutoResultsShutdownSettleDelay)
				select {
				case <-phaseBase.Done():
					if !settleTimer.Stop() {
						select {
						case <-settleTimer.C:
						default:
						}
					}
				case <-settleTimer.C:
				}
			}
			runShutdown(false)
		}

		// Diagnostics and mandatory removal are required even when graceful API
		// shutdown is disabled. Their canonical finalizer owns independent bounded
		// phase contexts, so summary waits for the complete bounded attempt.
		if preSummaryHook != nil {
			preSummaryHook(phaseBase)
			if metadata != nil && metadata.resourceFinalization != nil {
				outcome.Lifecycle.MergeFinalization(*metadata.resourceFinalization)
			}
		}

		// Prepared inputs remain available through artifact collection and
		// resource finalization, then are removed under an independent budget
		// before the durable summary observes the lifecycle outcome.
		if metadata != nil && metadata.preparedCleanup != nil {
			preparedCtx, cancelPrepared := context.WithTimeout(
				phaseBase, autoResultsPhaseBudgets.PreparedInputs)
			preparedErr := metadata.preparedCleanup(preparedCtx)
			cancelPrepared()
			outcome.Lifecycle.PreparedInputs = runcontrol.CompletedPhase(preparedErr)
			metadata.preparedScenarioJS = nil
			metadata.preparedDefaultsYAML = nil
			if preparedErr != nil {
				logging.LogError("auto-results", "prepared input cleanup failed", preparedErr)
			}
		}

		updateRunLifecycleMetadata(metadata, &outcome)
		if err := writeRunMetadata(metadata, root); err != nil {
			logging.LogError("auto-results", "write pre-summary lifecycle metadata", err)
		}

		writer := summaryOut
		if writer == nil {
			writer = io.Discard
		}
		outcome.Lifecycle.Summary.Started = true
		summaryCtx, cancelSummary := context.WithTimeout(phaseBase, autoResultsPhaseBudgets.Summary)
		var traceOutput *summaryTraceWriter
		if traceFile != "" {
			var traceErr error
			traceOutput, traceErr = newSummaryTraceWriter(writer, traceFile)
			if traceErr != nil {
				outcome.SummaryErr = errors.Join(
					outcome.SummaryErr, fmt.Errorf("open trace for final summary: %w", traceErr))
			} else {
				writer = traceOutput
			}
		}
		if err := generateRunSummaryFunc(summaryCtx, root, writer); err != nil {
			outcome.SummaryErr = errors.Join(outcome.SummaryErr, err)
		}
		if traceOutput != nil {
			if closeErr := traceOutput.Close(); closeErr != nil {
				outcome.SummaryErr = errors.Join(
					outcome.SummaryErr, fmt.Errorf("close trace after final summary: %w", closeErr))
			}
			if traceErr := traceOutput.Err(); traceErr != nil {
				outcome.SummaryErr = errors.Join(
					outcome.SummaryErr, fmt.Errorf("append final summary to trace: %w", traceErr))
			}
		}
		if traceFile != "" {
			if traceErr := appendTraceToResultsManifest(root, traceFile); traceErr != nil {
				outcome.SummaryErr = errors.Join(
					outcome.SummaryErr, fmt.Errorf("refresh final trace artifact: %w", traceErr))
			}
		}
		if outcome.SummaryErr != nil {
			logging.LogError("auto-results", "generate or persist summary", outcome.SummaryErr)
			writeProgress("Auto-results: summary generation encountered issues; see log.\n")
		}
		cancelSummary()
		outcome.Lifecycle.Summary = runcontrol.CompletedPhase(outcome.SummaryErr)
		updateRunLifecycleMetadata(metadata, &outcome)
		if err := writeRunMetadata(metadata, root); err != nil {
			outcome.SummaryErr = errors.Join(outcome.SummaryErr, fmt.Errorf("write final lifecycle metadata: %w", err))
			outcome.Lifecycle.Summary.Err = outcome.SummaryErr
			logging.LogError("auto-results", "write final lifecycle metadata", err)
		}
	}()
	return monitor
}

func buildIntegrityFinalizeOptions(params scenario.Params, plan integrityplan.Plan) *integrity.FinalizeOptions {
	options := &integrity.FinalizeOptions{
		Workload:            plan.Workload,
		RunID:               plan.RunID,
		AllowEmptySelection: plan.AllowEmpty,
		MaxConsoleFailures:  params.IntegrityMaxConsoleFailures,
		Multipart:           plan.Multipart,
		Plan:                plan.Clone(),
	}
	for _, mount := range params.ItemFileMounts {
		switch filepath.Base(mount.ContainerPath) {
		case integrity.VerifyInputName:
			options.StagedManifest = mount.HostPath
		case integrity.VerifyInputCompletionName:
			options.StagedCompletion = mount.HostPath
		}
	}
	return options
}

func stepRoleLifecycle(result *portcheck.RunResult, runtimeStepID, plannedStepID string) portcheck.StepLifecycle {
	if result == nil {
		return ""
	}
	for _, stepID := range []string{runtimeStepID, plannedStepID} {
		if stepID == "" {
			continue
		}
		if step, ok := result.Steps[stepID]; ok {
			return step.Lifecycle
		}
	}
	return ""
}

func resolveVerificationRunError(runErr error, outcome autoResultsOutcome, received bool, params scenario.Params) error {
	if !scenario.IsIntegrityWorkload(params) {
		return runErr
	}
	corrupt := int64(0)
	if outcome.ObservedCorruptCount != nil {
		corrupt = *outcome.ObservedCorruptCount
	}
	if outcome.Finalization != nil && outcome.Finalization.CorruptCount > corrupt {
		corrupt = outcome.Finalization.CorruptCount
	}
	primary := ""
	if outcome.Tracker != nil && outcome.Tracker.FinalState == constants.StateFailed {
		primary = fmt.Sprintf("step %s failed (%s): %s", outcome.Tracker.FailureStepID, outcome.Tracker.FailureCategory, outcome.Tracker.FailureMessage)
	}
	if corrupt > 0 {
		message := fmt.Sprintf("integrity verification detected %d corrupt object(s)", corrupt)
		if primary != "" {
			message += "; primary terminal cause: " + primary
		}
		if outcome.FinalizationErr != nil {
			message += "; result finalization also failed: " + outcome.FinalizationErr.Error()
		}
		if runErr != nil {
			message += "; run also failed: " + runErr.Error()
		}
		return &ExitCodeError{
			Code: constants.ExitCodeIntegrityCorruption, Msg: message,
			Cause: errors.Join(runErr, outcome.FinalizationErr),
		}
	}

	var reasons []string
	var causes []error
	if runErr != nil {
		reasons = append(reasons, runErr.Error())
		causes = append(causes, runErr)
	}
	if !received {
		reasons = append(reasons, "automatic verification results did not complete")
	}
	if outcome.TrackerErr != nil {
		reasons = append(reasons, "completion tracking: "+outcome.TrackerErr.Error())
		causes = append(causes, outcome.TrackerErr)
	}
	if outcome.Tracker == nil {
		reasons = append(reasons, "terminal engine status was not captured")
	} else if outcome.Tracker.FinalState != constants.StateCompleted {
		if primary != "" {
			reasons = append(reasons, primary)
		} else {
			reasons = append(reasons, fmt.Sprintf("engine terminal state is %s", outcome.Tracker.FinalState))
		}
	}
	if outcome.CorruptionMetricsErr != nil {
		reasons = append(reasons, "live corruption metrics: "+outcome.CorruptionMetricsErr.Error())
		causes = append(causes, outcome.CorruptionMetricsErr)
	}
	if outcome.ArtifactErr != nil {
		reasons = append(reasons, "artifact retrieval: "+outcome.ArtifactErr.Error())
		causes = append(causes, outcome.ArtifactErr)
	}
	if outcome.ShutdownErr != nil {
		reasons = append(reasons, "graceful shutdown: "+outcome.ShutdownErr.Error())
		causes = append(causes, outcome.ShutdownErr)
	}
	if finalizationErr := errors.Join(
		outcome.Lifecycle.Diagnostics.Err, outcome.Lifecycle.Removal.Err); finalizationErr != nil {
		reasons = append(reasons, "resource finalization: "+finalizationErr.Error())
		causes = append(causes, finalizationErr)
	}
	if outcome.Lifecycle.Resources == runcontrol.ResourceDispositionRetained {
		reasons = append(reasons, "resources remain managed for cleanup retry")
	}
	if outcome.Lifecycle.PreparedInputs.Err != nil {
		reasons = append(reasons, "prepared input cleanup: "+outcome.Lifecycle.PreparedInputs.Err.Error())
		causes = append(causes, outcome.Lifecycle.PreparedInputs.Err)
	}
	if outcome.FinalizationErr != nil {
		reasons = append(reasons, "integrity result finalization: "+outcome.FinalizationErr.Error())
		causes = append(causes, outcome.FinalizationErr)
	}
	if outcome.Finalization == nil {
		reasons = append(reasons, "integrity result finalization did not produce an outcome")
	} else {
		if params.DeferVerification != outcome.Finalization.VerificationDeferred {
			reasons = append(reasons, "integrity finalization mode does not match the requested deferred-verification mode")
		}

		if !outcome.Finalization.Complete {
			reasons = append(reasons, "integrity result finalization is incomplete")
		}
		if outcome.Finalization.EmptySelection && !outcome.Finalization.EmptyAllowed {
			if params.WorkloadType == workload.ReadVerify {
				reasons = append(reasons, "verification selected zero objects; read-verify requires --allow-empty-selection for a clean empty set")
			} else {
				reasons = append(reasons, "write-verify produced no objects eligible for verification")
			}
		}
	}
	if outcome.SummaryErr != nil {
		reasons = append(reasons, "summary generation: "+outcome.SummaryErr.Error())
		causes = append(causes, outcome.SummaryErr)
	}
	if len(reasons) > 0 {
		return &ExitCodeError{
			Code: constants.ExitCodeWorkloadFailure, Msg: strings.Join(reasons, "; "),
			Cause: errors.Join(causes...),
		}
	}
	return nil
}

func resolveRunCompletionError(
	runErr error, outcome autoResultsOutcome, received bool, params scenario.Params,
) error {
	if params.WorkloadType == scenario.WorkloadTypeDelete {
		var reasons []string
		var causes []error
		if outcome.DeleteMetricsErr != nil {
			reasons = append(reasons, "terminal DELETE metrics are incomplete: "+outcome.DeleteMetricsErr.Error())
			causes = append(causes, outcome.DeleteMetricsErr)
		}
		if outcome.DeleteTerminalErr != nil {
			reasons = append(reasons, outcome.DeleteTerminalErr.Error())
			causes = append(causes, outcome.DeleteTerminalErr)
		}
		if outcome.TrackerErr != nil {
			reasons = append(reasons, "completion tracking: "+outcome.TrackerErr.Error())
			causes = append(causes, outcome.TrackerErr)
		}
		if outcome.Tracker != nil && outcome.Tracker.FinalState != constants.StateCompleted {
			if outcome.Tracker.FinalState == constants.StateFailed {
				reasons = append(reasons, fmt.Sprintf(
					"step %s failed (%s): %s",
					outcome.Tracker.FailureStepID,
					outcome.Tracker.FailureCategory,
					outcome.Tracker.FailureMessage))
			} else {
				reasons = append(reasons, fmt.Sprintf(
					"engine terminal state is %s", outcome.Tracker.FinalState))
			}
		} else if received && outcome.Tracker == nil {
			reasons = append(reasons, "terminal engine status was not captured")
		}
		if len(reasons) == 0 {
			return runErr
		}
		if runErr != nil {
			reasons = append(reasons, "run also failed: "+runErr.Error())
			causes = append(causes, runErr)
		}
		return &ExitCodeError{
			Code: constants.ExitCodeWorkloadFailure, Msg: strings.Join(reasons, "; "),
			Cause: errors.Join(causes...),
		}
	}
	return resolveVerificationRunError(runErr, outcome, received, params)
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

func selectResultStepIDs(expected, fleet, node []string) (selected, runtime []string) {
	runtime = uniqueStepIDs(fleet, node)
	if len(runtime) > 0 {
		return append([]string(nil), runtime...), runtime
	}
	// Older engines may not expose current-run discovery. Preserve that
	// compatibility fallback without mixing planned aliases into a known
	// runtime set.
	return uniqueStepIDs(expected), nil
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

// requestShutdownAll stops the entry node first and waits for its owned run to
// become terminal before stopping workers. The entry owns distributed RMI step
// clients, so worker services must remain available through entry cleanup.
func requestShutdownAll(ctx context.Context, hosts []*hostparse.HostInfo, apiPort string, linger time.Duration,
	expectedRunID int64, debug bool) error {
	if len(hosts) == 0 {
		// default to localhost
		hosts = []*hostparse.HostInfo{{Host: "localhost", Original: "localhost"}}
	}
	type target struct {
		host             *hostparse.HostInfo
		client           *portcheck.SptAPIClient
		shutdownAccepted bool
	}
	targets := make([]target, 0, len(hosts))
	for _, host := range hosts {
		base := fmt.Sprintf("http://%s:%s", host.Host, apiPort)
		client := portcheck.NewSptAPIClient(base, constants.APIPollingTimeout)
		client.SetExpectedRunID(expectedRunID)
		targets = append(targets, target{host: host, client: client})
	}

	type result struct {
		host string
		err  error
	}
	var errs []string
	entry := &targets[0]
	if err := entry.client.Shutdown(ctx); err != nil {
		errs = append(errs, fmt.Sprintf("%s: shutdown error: %v", entry.host.Original, err))
	} else {
		entry.shutdownAccepted = true
		if debug {
			logging.LogDebug("auto-results", "entry shutdown requested", "host", entry.host.Original)
		}
		terminalCtx, cancelTerminal := context.WithTimeout(ctx, constants.AutoResultsEntryTerminalTimeout)
		terminalErr := entry.client.WaitForTerminal(terminalCtx)
		cancelTerminal()
		if terminalErr != nil {
			errs = append(errs, fmt.Sprintf(
				"%s: entry terminal wait error: %v", entry.host.Original, terminalErr))
		} else if debug {
			logging.LogDebug("auto-results", "entry run reached terminal shutdown", "host", entry.host.Original)
		}
	}

	ch := make(chan result, len(targets))
	pending := 0
	for index := range targets {
		shutdownTarget := &targets[index]
		if index == 0 && !shutdownTarget.shutdownAccepted {
			continue
		}
		pending++
		go func(current *target) {
			if !current.shutdownAccepted {
				if err := current.client.Shutdown(ctx); err != nil {
					ch <- result{host: current.host.Original, err: fmt.Errorf("shutdown error: %w", err)}
					return
				}
				if debug {
					logging.LogDebug("auto-results", "worker shutdown requested", "host", current.host.Original)
				}
			}
			if err := current.client.WaitForLinger(ctx, linger); err != nil {
				ch <- result{host: current.host.Original, err: fmt.Errorf("linger wait error: %w", err)}
				return
			}
			ch <- result{host: current.host.Original, err: nil}
		}(shutdownTarget)
	}
	for i := 0; i < pending; i++ {
		select {
		case <-ctx.Done():
			if len(errs) == 0 {
				return ctx.Err()
			}
			return fmt.Errorf(
				"some hosts failed shutdown: %s: %w", strings.Join(errs, "; "), ctx.Err())
		case shutdownResult := <-ch:
			if shutdownResult.err != nil {
				errs = append(errs, fmt.Sprintf("%s: %v", shutdownResult.host, shutdownResult.err))
				logging.LogError(
					"auto-results", "shutdown host error", shutdownResult.err, "host", shutdownResult.host)
			} else if debug {
				logging.LogDebug("auto-results", "shutdown host ok", "host", shutdownResult.host)
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

func joinFallbackPreparedCleanup(
	parent context.Context, runErr error, prepared *runcontrol.PreparedRun,
) error {
	if prepared == nil {
		return runErr
	}
	if parent == nil {
		parent = context.Background()
	}
	cleanupCtx, cancel := context.WithTimeout(
		context.WithoutCancel(parent), constants.ContainerCleanupTimeout)
	defer cancel()
	if cleanupErr := prepared.Cleanup(cleanupCtx); cleanupErr != nil {
		return errors.Join(runErr, fmt.Errorf("cleanup prepared run: %w", cleanupErr))
	}
	return runErr
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
  write-verify: Write and verify every successful object, or defer readback for later campaigns.
  read-verify: Verify self-verifying objects from discovery or --items-file.
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
	RunE: func(cmd *cobra.Command, args []string) (runErr error) {
		workloadType := args[0]

		// Validate workload type
		if err := validateRunWorkloadTypeFunc(workloadType); err != nil {
			return err
		}

		// Build scenario parameters
		params, err := buildScenarioParams(workloadType, cmd)
		if err != nil {
			return err
		}

		if workloadType == WorkloadTypeWriteVerify || workloadType == WorkloadTypeReadVerify ||
			workloadType == WorkloadTypeDelete {
			params.RunID = time.Now().UnixMilli()
		}
		writeDeleteSeedConcurrencyWarning(cmd.ErrOrStderr(), params)
		writeDeleteExistingSafetyWarning(cmd.ErrOrStderr(), params)

		// Seed size warning for read workloads
		if workloadType == WorkloadTypeRead {
			warnSeedSize(params)
		}

		// Lock the base timestamp so every generator used during preparation
		// observes one run identity.
		params.BaseTimestamp = scenario.BaseTimestamp()
		generateOnly, _ := cmd.Flags().GetBool("generate-only")
		scenarioFile := fmt.Sprintf("spt-scenario-%d.js", time.Now().Unix())
		scenarioPath := filepath.Join(".", scenarioFile)
		prepared, err := prepareRunBundle(params, scenarioPath, generateOnly || params.KeepScenario)
		if err != nil {
			return err
		}
		preparedCleanupDelegated := false
		defer func() {
			if !preparedCleanupDelegated {
				runErr = joinFallbackPreparedCleanup(commandContext(cmd), runErr, prepared)
			}
		}()
		params = prepared.Params()
		scenarioContent := string(prepared.ScenarioJS())
		expectedStepIDs := prepared.ExpectedStepIDs()
		verificationRun := scenario.IsIntegrityWorkload(params)
		if params.WorkloadType == WorkloadTypeDelete && params.SelectionOrder == scenario.SelectionOrderCanonical {
			fmt.Println("DELETE selection order: canonical. Cross-tool comparisons may be affected by input ordering.")
		}
		for _, notice := range integrityCostNotices(params) {
			fmt.Println(notice)
		}

		// Log scenario identity without retaining potentially sensitive content.
		logger := logging.GetLogger()
		scenarioDigest := sha256.Sum256(prepared.ScenarioJS())
		logger.Debug("Generated scenario file",
			"file", scenarioPath,
			"bytes", len(prepared.ScenarioJS()),
			"sha256", fmt.Sprintf("%x", scenarioDigest))

		// Phase 1: Parse results options (flags) and log them for visibility
		resultsOpts := buildResultsOptions(cmd)
		logger.Debug("Results options parsed",
			"auto_results", resultsOpts.AutoResults,
			"results_dir", resultsOpts.ResultsDir,
			"label", resultsOpts.Label,
			"debug", resultsOpts.Debug)

		// Check if generate-only flag is set
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
			return fmt.Errorf("invalid test hosts: %w", err)
		}
		integrityIdentityTier, _ := cmd.Flags().GetString(flagIntegrityRuntimeIdentityTier)

		// Set default min-hosts to total host count if not specified
		if minHosts == 0 {
			minHosts = len(hostInfos)
		}

		// Validate min-hosts doesn't exceed total hosts
		if minHosts > len(hostInfos) {
			return fmt.Errorf("min-hosts (%d) cannot exceed total hosts (%d)", minHosts, len(hostInfos))
		}

		if attachExisting && len(hostInfos) < 2 {
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
		metadata.preparedInputs = true
		metadata.preparedCleanup = prepared.Cleanup
		metadata.preparedScenarioJS = prepared.ScenarioJS()
		metadata.preparedDefaultsYAML = prepared.DefaultsYAML()
		metadata.TraceFile = traceOpts.Path
		metadata.TraceAuto = traceOpts.Auto
		if plannedResultsRoot != "" {
			metadata.ResultsRoot = plannedResultsRoot
		}
		if verificationRun && !useMultiHostOrchestrator && len(hostInfos) == 1 {
			localHost := hostInfos[0]
			metadata.runtimeIdentityProvider = func() (*tui.DistributedRuntimeIdentityEvidence, error) {
				identity, err := preflight.NewDefaultChecker().InspectImageIdentity(
					context.Background(), localHost, sptImage)
				if err != nil {
					return nil, err
				}
				return &tui.DistributedRuntimeIdentityEvidence{
					Tier:           tui.RuntimeIdentityTierAvailableImage,
					ImageReference: sptImage,
					ImageID:        identity.ID,
					Participants: []tui.DistributedRuntimeIdentityParticipant{{
						Host:        localHost.Original,
						ImageID:     identity.ID,
						RepoDigests: append([]string(nil), identity.RepoDigests...),
					}},
				}, nil
			}
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
			multiHostOrchestrator.SetRuntimeIdentityRecorder(func(evidence tui.DistributedRuntimeIdentityEvidence) {
				metadata.RuntimeIdentity = &evidence
			})
			multiHostOrchestrator.SetIntegrityRuntimeIdentityTier(integrityIdentityTier)
			metadata.deleteContributors = multiHostOrchestrator.MetricContributorIDs
		}
		var runSession *runcontrol.Session
		if resultsOpts.AutoResults {
			runSession = runcontrol.NewSession()
		}
		finalizeRunSession := func(ctx context.Context) {
			if runSession == nil {
				return
			}
			finalization := runSession.FinalizeResources(ctx)
			if metadata != nil {
				metadata.resourceFinalization = &finalization
			}
			if err := finalization.Error(); err != nil {
				logger.Warn("Diagnostics collection/cleanup completed with warnings", "error", err.Error())
			}
		}

		// Start background auto-results tracker/fetcher if enabled.
		runContext := commandContext(cmd)
		var integrityOptions *integrity.FinalizeOptions
		if verificationRun {
			verifyPlan, ok := prepared.VerificationPlan()
			if !ok {
				return fmt.Errorf("prepared verification run is missing its typed plan")
			}
			integrityOptions = buildIntegrityFinalizeOptions(params, verifyPlan)
		}
		var autoMonitor *autoResultsMonitor
		onSubmitted := func() {
			if autoMonitor != nil {
				autoMonitor.Arm()
			}
		}
		launchHooks := tui.NewLaunchHooks(onSubmitted)
		if runSession != nil {
			launchHooks = tui.NewSessionLaunchHooks(runSession, onSubmitted)
		}
		startAutoResultsMonitoring := func() {
			if resultsOpts.AutoResults && autoMonitor == nil {
				autoMonitor = startAutoResultsFunc(runContext, baseURL, resultsOpts.Label, resultsOpts.ResultsDir, expectedStepIDs, params.RunID, resultsOpts.Debug, hostInfos, apiPort, resultsOpts.ShutdownOnComplete, resultsOpts.ShutdownLingerSec, scenarioPath, metadata, progressOut, summaryWriter, traceOpts.Path, finalizeRunSession, integrityOptions)
				if autoMonitor != nil {
					autoMonitor.BindSession(runSession)
					preparedCleanupDelegated = true
				}
			}
		}
		var autoOutcome autoResultsOutcome
		autoOutcomeReceived := false
		waitForAutoResults := func() {
			if autoMonitor == nil {
				return
			}
			// The coordinator's phases are independently bounded. Joining it here
			// prevents Cobra/root os.Exit from abandoning mandatory cleanup.
			autoOutcome = <-autoMonitor.done
			autoOutcomeReceived = true
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
			ctx := runContext

			// Respect force cleanup for preflight conflicts
			forceMode, _ := cmd.Flags().GetBool("force")
			orchestrator.SetForceCleanup(forceMode)

			// Connect to all hosts
			err := connectMultiHostOrchestratorFunc(ctx, orchestrator)
			if err != nil {
				return fmt.Errorf("failed to connect to required hosts: %w", err)
			}
			if verificationRun {
				if _, err := prepareDistributedIntegrityRuntimeIdentityFunc(ctx, orchestrator, sptImage); err != nil {
					return err
				}
			}
			startAutoResultsMonitoring()

			// Check if we should run in headless mode
			if headlessMode {
				verbose, _ := cmd.Flags().GetBool("verbose")

				delegateShutdownToAutoResults := resultsOpts.AutoResults && resultsOpts.ShutdownOnComplete
				options := buildHeadlessOptions(traceOpts, verbose, "", autoTerminate, delegateShutdownToAutoResults, expectedStepIDs)
				options.Context = runContext
				options.LaunchHooks = launchHooks
				options.ScenarioContent = prepared.ScenarioJS()
				options.DefaultsContent = prepared.DefaultsYAML()

				if autoTerminate > 0 {
					fmt.Printf("Auto-terminate: will stop after %d seconds\n", autoTerminate)
				}

				err := startMultiHostHeadlessRunFunc(orchestrator, sptImage, scenarioPath, params, options)
				if err != nil && autoMonitor != nil && !launchHooks.NormalEvidencePermitted() {
					autoMonitor.Cancel()
				}
				autoTerminated, normalizedErr := normalizeHeadlessAutoTerminate(err, orchestrator, 30*time.Second)
				if autoTerminated {
					waitForAutoResults()
					finalizeTraceArtifact()
					return resolveRunCompletionError(normalizedErr, autoOutcome, autoOutcomeReceived, params)
				}
				waitForAutoResults()
				finalizeTraceArtifact()
				return resolveRunCompletionError(normalizedErr, autoOutcome, autoOutcomeReceived, params)
			}

			fmt.Printf("Starting multi-host TUI...\n\n")
			if autoTerminate > 0 {
				fmt.Printf("Auto-terminate: will stop after %d seconds\n", autoTerminate)
			}
			err = startMultiHostTUIRunFunc(
				orchestrator, sptImage, scenarioPath, params, tui.RunOptions{
					Context:              runContext,
					AutoTerminateSeconds: autoTerminate,
					SetSummarySink:       setSummarySink,
					TracePath:            traceOpts.Path,
					TraceAppend:          traceOpts.Append,
					LaunchHooks:          launchHooks,
					ScenarioContent:      prepared.ScenarioJS(),
					DefaultsContent:      prepared.DefaultsYAML(),
				})
			if err != nil && autoMonitor != nil && !launchHooks.NormalEvidencePermitted() {
				autoMonitor.Cancel()
			}
			waitForAutoResults()
			finalizeTraceArtifact()
			return resolveRunCompletionError(err, autoOutcome, autoOutcomeReceived, params)
		}

		// Single host mode (existing logic)
		startAutoResultsMonitoring()
		// Check if we should run in headless mode
		if headlessMode {
			verbose, _ := cmd.Flags().GetBool("verbose")

			options := buildHeadlessOptions(traceOpts, verbose, apiPort, autoTerminate, false, nil)
			options.Context = runContext
			options.NetworkMode = networkMode
			options.ResultsRoot = plannedResultsRoot
			options.LaunchHooks = launchHooks
			options.ScenarioContent = prepared.ScenarioJS()
			options.DefaultsContent = prepared.DefaultsYAML()
			// KeepScenario is now passed via params, not options.
			if autoTerminate > 0 {
				fmt.Printf("Auto-terminate: will stop after %d seconds\n", autoTerminate)
			}

			err := startLocalHeadlessRunFunc(sptImage, scenarioPath, params, options)
			if err != nil && autoMonitor != nil && !launchHooks.NormalEvidencePermitted() {
				autoMonitor.Cancel()
			}
			waitForAutoResults()
			finalizeTraceArtifact()
			return resolveRunCompletionError(err, autoOutcome, autoOutcomeReceived, params)
		}

		fmt.Printf("Starting TUI...\n\n")
		// Launch TUI with the scenario file
		if autoTerminate > 0 {
			fmt.Printf("Auto-terminate: will stop after %d seconds\n", autoTerminate)
		}
		err = startLocalTUIRunFunc(
			sptImage, scenarioPath, params, tui.RunOptions{
				Context:              runContext,
				APIPort:              apiPort,
				NetworkMode:          networkMode,
				ResultsRoot:          plannedResultsRoot,
				AutoTerminateSeconds: autoTerminate,
				SetSummarySink:       setSummarySink,
				TracePath:            traceOpts.Path,
				TraceAppend:          traceOpts.Append,
				LaunchHooks:          launchHooks,
				ScenarioContent:      prepared.ScenarioJS(),
				DefaultsContent:      prepared.DefaultsYAML(),
			})
		if err != nil && autoMonitor != nil && !launchHooks.NormalEvidencePermitted() {
			autoMonitor.Cancel()
		}
		waitForAutoResults()
		finalizeTraceArtifact()
		return resolveRunCompletionError(err, autoOutcome, autoOutcomeReceived, params)
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
	runCmd.Flags().String("prefix", "", "Write-verify: generated-key namespace; seeded DELETE: owned namespace root; guarded existing DELETE: exact S3 prefix without a leading slash; list/read-verify: listing constraint")
	runCmd.Flags().Int("auth-version", 4, "S3 authentication signature version (2 or 4; default 4)")

	// Workload Definition Options
	runCmd.Flags().IntP("threads", "t", 1, "Number of parallel client threads to run (e.g., 16)")
	runCmd.Flags().StringP("object-size", "o", "", "The size of each object using human-readable units (e.g., 1MiB, 256KiB, 4GiB; legacy MB/KB/GB also accepted)")
	runCmd.Flags().Float64("object-data-compressibility", 0.0, "Compressibility percentage of object payloads (0.0 to 100.0, default 0.0)")
	runCmd.Flags().Bool("object-data-dedupable", true, "Allow object payloads to be deduplicated by the storage array (default true)")
	runCmd.Flags().IntP("object-count", "n", 0, "Fixed object count; seeded DELETE creates this many identities, while manifest/existing-prefix DELETE caps the canonical selection")
	runCmd.Flags().Int(flagDeleteBatchSize, scenario.DefaultDeleteBatchSize, "Standalone DELETE logical request size (1 uses DeleteObject; 2-1000 use DeleteObjects)")
	runCmd.Flags().Bool(flagDeleteExisting, false, "Destructive DELETE opt-in: discover and freeze current keys under the exact --bucket/--prefix before timing")
	runCmd.Flags().Bool(flagAllowEmptyPrefix, false, "Second destructive DELETE opt-in required with --delete-existing --prefix='' to select an entire bucket")
	runCmd.Flags().Int64(flagMaxFailedObjects, scenario.DefaultMaxFailedObjects, "Maximum operational DELETE object failures permitted; the budget trips only above this object count")
	runCmd.Flags().Float64(flagMaxFailurePercent, 0, "Maximum cumulative operational DELETE object failure percentage (0-100); mutually exclusive with --max-failed-objects")
	runCmd.Flags().Duration(flagFailureBudgetGrace, scenario.DefaultFailureBudgetGrace, "Measured-phase grace before evaluating a positive DELETE failure percentage")
	runCmd.Flags().Bool(flagValidateInventory, false, "Strictly require every selected DELETE identity to exist before the timed phase; enables post-verification unless --verify is explicitly false")
	runCmd.Flags().Bool(flagVerifyDelete, false, "Verify the full applicable DELETE inventory after timing (default false)")
	runCmd.Flags().Duration(flagVerificationTimeout, scenario.DefaultDeleteVerificationTimeout, "Independent timeout for each enabled DELETE pre-validation or post-verification phase")
	runCmd.Flags().StringP("duration", "d", "", "Defines the workload by a fixed time duration (e.g., 5m, 1h)")
	runCmd.Flags().Int(flagPrefixShards, prefixShardsAuto, "Generated-key prefix directories (-1 = auto from configured aggregate concurrency, 0 = disabled)")

	// Multipart Upload Options
	runCmd.Flags().String("part-size", "", "Enable multipart upload with the given part size (e.g., 5MiB, 64MiB, 256MiB; legacy MB also accepted)")
	runCmd.Flags().Int("mpu-concurrent-objects", 0, "Max concurrent multipart objects in flight (0 = unlimited)")
	runCmd.Flags().Int("mpu-concurrent-parts", 0, "Max concurrent parts in flight per multipart object (0 = unlimited)")

	// Test Behavior Options
	runCmd.Flags().Int("seed-objects", 2500, "Number of objects to pre-create for read benchmarks and duration-based standalone DELETE (default: 2500)")
	runCmd.Flags().Bool("cleanup", false, "A boolean flag to automatically delete all created objects after the test completes")
	runCmd.Flags().Bool(flagDeferVerification, false, "Write-verify only: stop after durable nonempty CREATE evidence and defer readback (env: SPT_DEFER_VERIFICATION)")
	runCmd.Flags().String(flagVersions, scenario.VersionsCurrent, "Read-verify prefix discovery: current or all object versions")
	runCmd.Flags().Bool("create-prefix", false, "A boolean flag to ensure the target prefix (directory) is created if it doesn't exist")
	runCmd.Flags().StringP("output-dir", "O", "", "Specifies a local directory on the host machine to save the detailed Spt report files (e.g., ./results/test-01)")
	runCmd.Flags().Bool("generate-only", false, "Generate the scenario file without running Docker or Spt")
	// Headless auto-termination
	runCmd.Flags().Int("auto-terminate-seconds", 0, "Automatically terminate headless runs after N seconds (0 = unlimited)")
	runCmd.Flags().Bool("keep-scenario", false, "Keep the scenario file after the test completes (default: delete on success)")
	runCmd.Flags().Bool("save-items", false, "Save items.csv to the results directory (write workloads only; can be large for high-throughput runs)")
	runCmd.Flags().String("items-file", "", "Path to a local items.csv for read/read-verify or explicit-manifest DELETE; mutually exclusive with --delete-existing")
	runCmd.Flags().Bool("allow-empty-selection", false, "Allow a clean empty read-verify selection to succeed")
	runCmd.Flags().Int("integrity-max-console-failures", 20, "Maximum integrity failures sampled on the console (0 suppresses samples; env: SPT_INTEGRITY_MAX_CONSOLE_FAILURES)")
	runCmd.Flags().String(flagIntegrityRuntimeIdentityTier, constants.IntegrityRuntimeIdentityTierImage, "Distributed verification identity tier: image or payload (env: SPT_INTEGRITY_RUNTIME_IDENTITY_TIER)")
	runCmd.Flags().Bool(flagReadShuffle, false, "Read workload: shuffle items within each fetched batch before issuing reads (widens randomness, increases engine buffer usage, and does not guarantee storage-cache avoidance)")
	runCmd.Flags().Int(flagReadShuffleBatchSize, 0, fmt.Sprintf("Read workload: batch size to use with --shuffle (0 = use the bounded default, max %d)", constants.ReadShuffleMaxBatchSize))
	runCmd.Flags().Int(flagReadPhasePauseSeconds, scenario.DefaultReadPhasePauseSeconds, "Read workload: seconds to settle between seed, read, and cleanup phases")
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
	params.DeleteBatchSize, _ = cmd.Flags().GetInt(flagDeleteBatchSize)

	duration, _ := cmd.Flags().GetString("duration")
	params.Duration = duration

	// Get behavior flags
	cleanup, _ := cmd.Flags().GetBool("cleanup")
	params.Cleanup = cleanup
	params.Versions, _ = cmd.Flags().GetString(flagVersions)
	params.DeferVerification, _ = cmd.Flags().GetBool(flagDeferVerification)

	seedObjects, _ := cmd.Flags().GetInt("seed-objects")
	params.SeedCount = seedObjects

	keepScenario, _ := cmd.Flags().GetBool("keep-scenario")
	params.KeepScenario = keepScenario

	saveItems, _ := cmd.Flags().GetBool("save-items")
	params.SaveItems = saveItems

	itemsFile, _ := cmd.Flags().GetString("items-file")
	params.ItemsFile = itemsFile
	params.DeleteExisting, _ = cmd.Flags().GetBool(flagDeleteExisting)
	params.AllowEmptyPrefix, _ = cmd.Flags().GetBool(flagAllowEmptyPrefix)
	if workloadType == WorkloadTypeDelete {
		params.FailureBudgetMode = scenario.FailureBudgetModeFixed
		params.MaxFailedObjects = scenario.DefaultMaxFailedObjects
		params.FailureBudgetGrace = scenario.DefaultFailureBudgetGrace
		if cmd.Flags().Lookup(flagMaxFailedObjects) != nil {
			params.MaxFailedObjects, _ = cmd.Flags().GetInt64(flagMaxFailedObjects)
		}
		if flag := cmd.Flags().Lookup(flagMaxFailurePercent); flag != nil {
			params.MaxFailurePercent, _ = cmd.Flags().GetFloat64(flagMaxFailurePercent)
			if flag.Changed {
				params.FailureBudgetMode = scenario.FailureBudgetModePercentage
			}
		}
		if cmd.Flags().Lookup(flagFailureBudgetGrace) != nil {
			params.FailureBudgetGrace, _ = cmd.Flags().GetDuration(flagFailureBudgetGrace)
		}
		if flag := cmd.Flags().Lookup(flagValidateInventory); flag != nil {
			params.ValidateDeleteInventory, _ = cmd.Flags().GetBool(flagValidateInventory)
		}
		if flag := cmd.Flags().Lookup(flagVerifyDelete); flag != nil {
			params.VerifyDelete, _ = cmd.Flags().GetBool(flagVerifyDelete)
			params.VerifyDeleteExplicit = flag.Changed
		}
		if params.ValidateDeleteInventory && !params.VerifyDeleteExplicit {
			params.VerifyDelete = true
		}
		params.VerificationTimeout = scenario.DefaultDeleteVerificationTimeout
		if cmd.Flags().Lookup(flagVerificationTimeout) != nil {
			params.VerificationTimeout, _ = cmd.Flags().GetDuration(flagVerificationTimeout)
		}
	}
	params.AllowEmptySelection, _ = cmd.Flags().GetBool("allow-empty-selection")
	params.IntegrityMaxConsoleFailures, _ = cmd.Flags().GetInt("integrity-max-console-failures")

	readShuffle, _ := cmd.Flags().GetBool(flagReadShuffle)
	params.ReadShuffle = readShuffle

	readShuffleBatchSize, _ := cmd.Flags().GetInt(flagReadShuffleBatchSize)
	params.ReadShuffleBatchSize = readShuffleBatchSize

	readPhasePauseSeconds, _ := cmd.Flags().GetInt(flagReadPhasePauseSeconds)
	params.ReadPhasePauseSeconds = readPhasePauseSeconds

	serviceThreads, _ := cmd.Flags().GetInt("service-threads")
	params.ServiceThreads = serviceThreads

	engineOverrides, _ := cmd.Flags().GetStringArray(flagEngineOverride)
	if len(engineOverrides) > 0 {
		params.EngineOverrides = engineOverrides
	}
	prefixShards, err := resolvePrefixShardCount(cmd, params)
	if err != nil {
		return scenario.Params{}, err
	}
	if err := applyPrefixShards(&params, prefixShards); err != nil {
		return scenario.Params{}, err
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

	// Resolve source-specific defaults only after both selection and duration flags are known.
	deleteWorkload := params.WorkloadType == WorkloadTypeDelete
	if deleteWorkload {
		params.SelectionOrder = scenario.SelectionOrderCanonical
	}
	seededDelete := deleteWorkload && strings.TrimSpace(params.ItemsFile) == "" && !params.DeleteExisting
	if seededDelete {
		if params.ObjectSize == "" {
			params.ObjectSize = scenario.DefaultDeleteObjectSize
		}
		if params.ObjectCount == 0 && strings.TrimSpace(params.Duration) == "" {
			params.ObjectCount = scenario.DefaultDeleteObjectCount
		}
	} else if params.ObjectSize == "" && params.WorkloadType != WorkloadTypeDelete &&
		params.WorkloadType != WorkloadTypeList && params.WorkloadType != WorkloadTypeReadVerify &&
		params.WorkloadType != WorkloadTypeTables {
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

func applyPrefixShards(params *scenario.Params, shardCount int) error {
	if shardCount < 0 {
		return fmt.Errorf("--%s must be non-negative", flagPrefixShards)
	}
	if shardCount == 0 {
		return nil
	}
	if params.WorkloadType != WorkloadTypeWrite && params.WorkloadType != WorkloadTypeWriteVerify &&
		params.WorkloadType != WorkloadTypeRead &&
		params.WorkloadType != WorkloadTypeMixed {
		return fmt.Errorf("--%s is not supported for %s workload", flagPrefixShards, params.WorkloadType)
	}
	if params.WorkloadType == WorkloadTypeRead && params.ItemsFile != "" {
		return fmt.Errorf("--%s cannot change object names supplied by --items-file", flagPrefixShards)
	}
	for _, override := range params.EngineOverrides {
		path, _, _ := strings.Cut(override, "=")
		path = strings.TrimSpace(path)
		if path == itemNamingShardsPath || path == strings.ReplaceAll(itemNamingShardsPath, ".", "-") {
			return fmt.Errorf(
				"--%s conflicts with engine override %q",
				flagPrefixShards, secretmask.EngineOverride(override))
		}
	}
	params.PrefixShards = shardCount
	params.EngineOverrides = append(
		params.EngineOverrides,
		fmt.Sprintf("%s=%d", itemNamingShardsPath, shardCount),
	)
	return nil
}

func resolvePrefixShardCount(cmd *cobra.Command, params scenario.Params) (int, error) {
	flag := cmd.Flags().Lookup(flagPrefixShards)
	if flag == nil {
		return 0, nil
	}
	shardCount, err := cmd.Flags().GetInt(flagPrefixShards)
	if err != nil || shardCount != prefixShardsAuto {
		return shardCount, err
	}
	if params.WorkloadType != WorkloadTypeWrite && params.WorkloadType != WorkloadTypeWriteVerify &&
		params.WorkloadType != WorkloadTypeMixed &&
		(params.WorkloadType != WorkloadTypeRead || params.ItemsFile != "") {
		return 0, nil
	}
	if params.Threads <= 0 {
		return 0, nil
	}
	hostCount := 1
	if cmd.Flags().Lookup("test-hosts") != nil {
		testHosts, getErr := cmd.Flags().GetString("test-hosts")
		if getErr != nil {
			return 0, getErr
		}
		hosts, parseErr := hostparse.ParseTestHosts(testHosts)
		if parseErr != nil {
			return 0, fmt.Errorf("resolve automatic prefix shards: %w", parseErr)
		}
		if len(hosts) > 0 {
			hostCount = len(hosts)
		}
	}
	maxInt := int(^uint(0) >> 1)
	if params.Threads > maxInt/hostCount {
		return 0, fmt.Errorf("automatic prefix shard count exceeds integer range")
	}
	return params.Threads * hostCount, nil
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
	if params.PrefixShards > 0 {
		lines = append(lines, fmt.Sprintf("Prefix Shards: %d", params.PrefixShards))
	}
	if (params.WorkloadType == WorkloadTypeDelete &&
		(strings.TrimSpace(params.ItemsFile) != "" || params.DeleteExisting)) ||
		params.WorkloadType == WorkloadTypeList || params.WorkloadType == WorkloadTypeReadVerify {
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
	if params.WorkloadType == WorkloadTypeDelete && params.Duration != "" &&
		!params.DeleteExisting && strings.TrimSpace(params.ItemsFile) == "" {
		seedCount := params.SeedCount
		if seedCount <= 0 {
			seedCount = scenario.DefaultDeleteObjectCount
		}
		lines = append(lines, fmt.Sprintf("Seed Objects: %d", seedCount))
	}

	// Always show cleanup status
	if params.Cleanup {
		lines = append(lines, "Cleanup: Yes (delete objects after test)")
	} else {
		lines = append(lines, "Cleanup: No")
	}
	if params.WorkloadType == WorkloadTypeWriteVerify {
		readback := "Immediate"
		if params.DeferVerification {
			readback = "Deferred"
		}
		lines = append(lines, "Verification Readback: "+readback)
	}
	if params.WorkloadType == WorkloadTypeDelete {
		lines = append(lines, fmt.Sprintf("DELETE Batch Size: %d", params.DeleteBatchSize))
		if params.DeleteExisting {
			prefix := params.Prefix
			if prefix == "" {
				prefix = "(empty; entire bucket)"
			}
			lines = append(lines,
				"DELETE Source: existing current-key prefix",
				fmt.Sprintf("DELETE Scope: bucket=%s prefix=%s", params.Bucket, prefix),
				"DANGER: deletes the frozen current-key selection from an existing namespace.",
				"Quiescence required: concurrent writers can replace a frozen identity before deletion.",
				"Discovery Phase: setup only; excluded from DELETE request timing.",
			)
		} else if strings.TrimSpace(params.ItemsFile) == "" {
			lines = append(lines, "DELETE Source: seeded owned inventory")
		} else {
			lines = append(lines, "DELETE Source: explicit canonical manifest")
		}
		if params.SelectionOrder != "" {
			lines = append(lines, "Selection Order: "+params.SelectionOrder)
			if params.SelectionSHA256 != "" {
				lines = append(lines, fmt.Sprintf(
					"Selection Records: source=%d unique=%d selected=%d sha256=%s",
					params.SelectionSourceCount,
					params.SelectionUniqueCount,
					params.SelectionSelectedCount,
					params.SelectionSHA256,
				))
			}
			lines = append(lines, "Warning: canonical key order can affect cross-tool DELETE comparisons.")
		}
	}
	if params.KeepScenario {
		lines = append(lines, "Keep Scenario: Yes")
	} else {
		lines = append(lines, "Keep Scenario: No")
	}

	return strings.Join(lines, "\n")
}

func writeDeleteSeedConcurrencyWarning(output io.Writer, params scenario.Params) {
	if output == nil || params.WorkloadType != WorkloadTypeDelete ||
		params.DeleteExisting || strings.TrimSpace(params.ItemsFile) != "" ||
		params.Threads <= 0 || params.DeleteBatchSize <= 0 {
		return
	}
	inventoryCount := params.ObjectCount
	if strings.TrimSpace(params.Duration) != "" {
		inventoryCount = params.SeedCount
	}
	if inventoryCount <= 0 {
		return
	}
	capacity := int64(params.Threads) * int64(params.DeleteBatchSize)
	if int64(inventoryCount) >= capacity {
		return
	}
	fullWaves := int64(inventoryCount) / capacity
	_, _ = fmt.Fprintf(
		output,
		"Warning: seeded DELETE inventory (%d objects) is smaller than --threads * --delete-batch-size (%d * %d = %d); maximum full request waves: %d. The run continues without automatic inventory calibration.\n",
		inventoryCount, params.Threads, params.DeleteBatchSize, capacity, fullWaves,
	)
}

func writeDeleteExistingSafetyWarning(output io.Writer, params scenario.Params) {
	if output == nil || params.WorkloadType != WorkloadTypeDelete || !params.DeleteExisting {
		return
	}
	prefix := params.Prefix
	if prefix == "" {
		prefix = "(empty; entire bucket)"
	}
	_, _ = fmt.Fprintf(
		output,
		"DANGER: --delete-existing will delete the frozen current-key selection under bucket %q prefix %q. Keep the namespace quiescent; concurrent writers can replace a frozen identity before deletion. Discovery is setup and is excluded from DELETE request timing.\n",
		params.Bucket, prefix,
	)
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

	if params.WorkloadType == WorkloadTypeList ||
		params.WorkloadType == WorkloadTypeReadVerify ||
		params.WorkloadType == WorkloadTypeWriteVerify {
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

func integrityCostNotices(params scenario.Params) []string {
	if params.WorkloadType != WorkloadTypeWriteVerify {
		return nil
	}
	var notices []string
	if params.DeferVerification {
		notices = append(notices,
			"Integrity notice: verification readback is deferred; preserve written.csv for later read-verify campaigns.")
	}
	if params.PartSize != "" {
		notices = append(notices,
			"Integrity notice: each multipart object is fully pre-hashed before multipart initiation.")
	}
	if params.Checksum != "" &&
		(params.PartSize != "" || !strings.EqualFold(params.Checksum, scenario.ChecksumSHA256)) {
		notices = append(notices,
			"Integrity notice: the selected transport checksum requires an additional payload digest pass; see integrity.performance.csv.")
	}
	return notices
}
