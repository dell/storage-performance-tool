/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results/summary"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
)

const (
	neverSubmittedShutdownLine = "Auto-results: no run was submitted; nothing to shut down."
	neverSubmittedResultsLine  = "Auto-results: no run was submitted; no results were produced."
	shutdownWarningLine        = "Shutdown completed with warnings"
	summaryIssueLine           = "summary generation encountered issues"
)

// lockedBuffer collects output written from the command and the monitor goroutine.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func assertNeverSubmittedCleanupOutput(t *testing.T, output string) {
	t.Helper()
	for _, want := range []string{neverSubmittedShutdownLine, neverSubmittedResultsLine} {
		if !strings.Contains(output, want) {
			t.Errorf("output lacks %q:\n%s", want, output)
		}
	}
	for _, unwanted := range []string{"Auto-results: requesting shutdown", shutdownWarningLine, summaryIssueLine} {
		if strings.Contains(output, unwanted) {
			t.Errorf("output contains %q after a launch that was never submitted:\n%s", unwanted, output)
		}
	}
}

// assertRejectedRunSummary checks that a start the engine identity gate rejected kept its
// rejected-run summary, without the lines of a start that left nothing to summarize.
func assertRejectedRunSummary(t *testing.T, resultsDir, output string) {
	t.Helper()
	summaryFiles, _ := filepath.Glob(filepath.Join(resultsDir, "*", "*_results_summary.txt"))
	if len(summaryFiles) != 1 {
		t.Fatalf("summary files = %v, want one; output:\n%s", summaryFiles, output)
	}
	saved, err := os.ReadFile(summaryFiles[0]) // #nosec G304 -- test temp directory
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(saved), "Run rejected") {
		t.Fatalf("summary lacks the rejected-run section:\n%s", saved)
	}
	if !strings.Contains(output, "Auto-results: summary saved to") {
		t.Errorf("output lacks the saved summary line:\n%s", output)
	}
	for _, unwanted := range []string{neverSubmittedResultsLine, shutdownWarningLine, summaryIssueLine} {
		if strings.Contains(output, unwanted) {
			t.Errorf("output contains %q:\n%s", unwanted, output)
		}
	}
}

// A results root whose manifests can't be inspected counts as having evidence, so the summary
// step runs and reports the problem instead of the monitor claiming no results were produced.
func TestHasSummarizableEvidence(t *testing.T) {
	empty := t.TempDir()
	if hasSummarizableEvidence(empty) {
		t.Fatal("empty results root reported evidence")
	}
	for _, name := range []string{constants.ResultsManifestFileName, constants.EngineInfoManifestName} {
		root := t.TempDir()
		if err := os.WriteFile(filepath.Join(root, name), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		if !hasSummarizableEvidence(root) {
			t.Fatalf("results root with %s reported no evidence", name)
		}
	}

	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("directory search permission is not enforced here")
	}
	parent := filepath.Join(t.TempDir(), "locked")
	root := filepath.Join(parent, "run")
	if err := os.MkdirAll(root, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(parent, 0o600); err != nil { // #nosec G302 -- removes search permission on a test directory
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(parent, 0o750) }) // #nosec G302 -- restores the test directory for removal
	if _, err := os.Stat(filepath.Join(root, constants.ResultsManifestFileName)); err == nil || errors.Is(err, fs.ErrNotExist) {
		t.Skipf("stat under a directory without search permission returned %v, not a permission error", err)
	}
	if !hasSummarizableEvidence(root) {
		t.Fatal("results root that cannot be inspected reported no evidence")
	}
}

// installStartFailureMonitorSeams keeps the monitor off the network and records the
// shutdown requests and summary attempts it makes.
func installStartFailureMonitorSeams(t *testing.T, shutdownErr error) (shutdowns, summaries *atomic.Int32, events func() []string) {
	t.Helper()
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	origShutdown := requestShutdownAllFunc
	t.Cleanup(func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
		requestShutdownAllFunc = origShutdown
	})
	var mu sync.Mutex
	var order []string
	record := func(event string) {
		mu.Lock()
		defer mu.Unlock()
		order = append(order, event)
	}
	newRunTrackerFunc = func(string) autoResultsRunTracker {
		return &fakeRunTracker{result: &portcheck.RunResult{FinalState: constants.StateCompleted, RunID: 41}}
	}
	discoverStepIDsFunc = func(context.Context, string, int64) ([]string, error) { return []string{"mt-001-write"}, nil }
	discoverFleetStepIDsFunc = func(context.Context, string, int64) ([]string, error) { return nil, nil }
	newResultsFetcherFunc = func(_, output string) autoResultsFetcher {
		return &fakeFetcher{output: output, onFetch: func(string, []string) error {
			record("artifacts")
			return nil
		}}
	}
	shutdowns = &atomic.Int32{}
	summaries = &atomic.Int32{}
	requestShutdownAllFunc = func(context.Context, []*hostparse.HostInfo, string, time.Duration, int64, bool) error {
		shutdowns.Add(1)
		record("shutdown")
		return shutdownErr
	}
	generateRunSummaryFunc = func(context.Context, string, io.Writer, summary.RunEnd) (*summary.RunSummary, error) {
		summaries.Add(1)
		return nil, errors.New("load results: load manifest: index.json: no such file or directory")
	}
	return shutdowns, summaries, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(order)
	}
}

// After a start failure the monitor requests shutdown only when a run may have
// been submitted: a bound session that never dispatched POST /run has no
// workload to stop, so no host is asked and no summary is attempted.
func TestAutoResultsShutdownAfterStartFailureFollowsSubmission(t *testing.T) {
	refused := errors.New(`shutdown http: Post "http://entry.example:9999/shutdown": dial tcp: connect: connection refused`)
	tests := []struct {
		name          string
		bindSession   bool
		markSubmitted bool
		arm           bool
		wantShutdowns int32
		wantSummaries int32
	}{
		{name: "bound session never submitted", bindSession: true},
		{name: "bound session accepted for cleanup", bindSession: true, markSubmitted: true, wantShutdowns: 1, wantSummaries: 1},
		{name: "no bound session", wantShutdowns: 1, wantSummaries: 1},
		{name: "armed normal run", bindSession: true, markSubmitted: true, arm: true, wantShutdowns: 1, wantSummaries: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var shutdownErr error
			if !test.arm {
				shutdownErr = refused
			}
			shutdowns, summaries, events := installStartFailureMonitorSeams(t, shutdownErr)
			var finalizations atomic.Int32
			metadata := &runMetadata{ResultsRoot: t.TempDir()}
			preSummaryHook := func(context.Context) {
				finalizations.Add(1)
				metadata.resourceFinalization = &runcontrol.FinalizationOutcome{
					Removal:   runcontrol.CompletedPhase(nil),
					Resources: runcontrol.ResourceDispositionRemoved,
				}
			}
			output := &lockedBuffer{}
			hosts := []*hostparse.HostInfo{{Host: "entry.example"}, {Host: "worker.example"}}
			monitor := startAutoResultsMonitor(
				context.Background(), "http://entry.example:9999", "mt", t.TempDir(), []string{"mt-001-write"}, 41,
				false, hosts, "9999", true, 1, "", metadata, output, output, "", preSummaryHook,
			)
			session := runcontrol.NewSession()
			if test.markSubmitted {
				session.MarkSubmitted()
			}
			if test.bindSession {
				monitor.BindSession(session)
			}
			if test.arm {
				monitor.Arm()
			} else {
				monitor.Cancel()
			}
			var outcome autoResultsOutcome
			select {
			case outcome = <-monitor.done:
			case <-time.After(5 * time.Second):
				t.Fatal("monitor did not complete")
			}

			if shutdowns.Load() != test.wantShutdowns || summaries.Load() != test.wantSummaries {
				t.Fatalf("shutdown requests=%d summaries=%d, want %d and %d",
					shutdowns.Load(), summaries.Load(), test.wantShutdowns, test.wantSummaries)
			}
			if finalizations.Load() != 1 || !outcome.Lifecycle.Removal.Completed {
				t.Fatalf("resource finalization calls=%d lifecycle=%+v, want one completed removal",
					finalizations.Load(), outcome.Lifecycle)
			}
			if metadata.Lifecycle == nil {
				t.Fatal("lifecycle metadata was not recorded")
			}
			if test.arm {
				if got := events(); !slices.Equal(got, []string{"artifacts", "shutdown"}) {
					t.Fatalf("armed run phase order = %v, want artifacts before shutdown", got)
				}
			}
			if test.wantShutdowns == 0 {
				if outcome.Lifecycle.Shutdown.Started || outcome.ShutdownErr != nil ||
					outcome.Lifecycle.Summary.Started || outcome.SummaryErr != nil {
					t.Fatalf("never-submitted lifecycle = %+v shutdownErr=%v summaryErr=%v",
						outcome.Lifecycle, outcome.ShutdownErr, outcome.SummaryErr)
				}
				if metadata.Lifecycle.Shutdown.Started || metadata.Lifecycle.Shutdown.Error != "" ||
					metadata.Lifecycle.Summary.Started || metadata.Lifecycle.Summary.Error != "" {
					t.Fatalf("never-submitted lifecycle metadata = %+v", metadata.Lifecycle)
				}
				assertNeverSubmittedCleanupOutput(t, output.String())
				return
			}
			if !outcome.Lifecycle.Shutdown.Started || !metadata.Lifecycle.Shutdown.Started {
				t.Fatalf("shutdown lifecycle = %+v metadata = %+v, want started",
					outcome.Lifecycle.Shutdown, metadata.Lifecycle.Shutdown)
			}
			if strings.Contains(output.String(), "no run was submitted") {
				t.Fatalf("output claims no submission for a run that may have been submitted:\n%s", output.String())
			}
			if !test.arm && !strings.Contains(output.String(), shutdownWarningLine) {
				t.Fatalf("refused shutdown did not warn:\n%s", output.String())
			}
		})
	}
}

// spt run, on the local and the host-orchestrated routes, headless and TUI, and in attach
// mode, sends no shutdown request after a start that fails before POST /run, and records that
// in spt_run_params.json. A start that never reached the engine identity gate has nothing to
// summarize, so no summary is attempted. One the gate rejected, or one whose POST /run was
// refused after the gate passed, keeps its summary. A launch accepted for cleanup still asks
// every host to shut down, or in attach mode the entry only.
func TestRunCommandStartFailureBeforeSubmissionSendsNoShutdown(t *testing.T) {
	const (
		gateNotRun   = ""
		gateRejects  = "rejects"
		gateProceeds = "proceeds"
	)
	tests := []struct {
		name               string
		hosts              string
		minHosts           string
		tui                bool
		attach             bool
		gate               string
		acceptedForCleanup bool
		wantLocal          bool
	}{
		{name: "local", hosts: "127.0.0.1", minHosts: "1", wantLocal: true},
		{name: "local tui", hosts: "127.0.0.1", minHosts: "1", tui: true, wantLocal: true},
		{name: "single remote", hosts: "entry.example", minHosts: "1"},
		{name: "multi host", hosts: "entry.example,worker.example", minHosts: "2"},
		{name: "multi host tui", hosts: "entry.example,worker.example", minHosts: "2", tui: true},
		{name: "attach existing workers", hosts: "entry.example,worker.example", minHosts: "2", attach: true},
		{name: "local gate rejects", hosts: "127.0.0.1", minHosts: "1", wantLocal: true, gate: gateRejects},
		{name: "local gate proceeds then POST refused", hosts: "127.0.0.1", minHosts: "1", wantLocal: true, gate: gateProceeds},
		{name: "attach gate rejects", hosts: "entry.example,worker.example", minHosts: "2", attach: true, gate: gateRejects},
		{name: "local accepted for cleanup", hosts: "127.0.0.1", minHosts: "1", wantLocal: true, acceptedForCleanup: true},
		{name: "attach existing workers accepted for cleanup", hosts: "entry.example,worker.example", minHosts: "2", attach: true, acceptedForCleanup: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workDir := t.TempDir()
			t.Chdir(workDir)
			resultsDir := filepath.Join(workDir, "results")
			setRunEngineInfoFlags(t, test.hosts, test.minHosts, "", false, true)
			setGlobalRunFlagForTest(t, "shutdown-on-complete", "true")
			setGlobalRunFlagForTest(t, "results-dir", resultsDir)
			setGlobalRunFlagForTest(t, "label", "startfail")
			setGlobalRunFlagForTest(t, flagAttachExistingWorkers, boolString(test.attach))
			restore := installRunEngineInfoCommandSeams(t)
			defer restore()
			shouldRunHeadlessForRun = func(*cobra.Command) bool { return !test.tui }
			consistency := engineinfo.ConsistencyConsistent
			if test.gate == gateRejects {
				consistency = engineinfo.ConsistencyMismatch
			}
			evaluateEngineIdentityGate = func(
				ctx context.Context, _ engineinfo.FleetCollector, descriptors []engineinfo.ParticipantDescriptor, force bool,
			) (engineinfo.GateOutcome, error) {
				return engineinfo.EvaluateGate(ctx, fixedFleetCollector{
					fleet: runGateFleet(consistency, engineinfo.StatusCollected),
				}, descriptors, force)
			}

			origSummary := generateRunSummaryFunc
			origShutdown := requestShutdownAllFunc
			origAuto := startAutoResultsFunc
			t.Cleanup(func() {
				generateRunSummaryFunc = origSummary
				requestShutdownAllFunc = origShutdown
				startAutoResultsFunc = origAuto
			})
			// The shipped summary runs, so a missing summary or a false summary problem shows.
			var summaries atomic.Int32
			generateRunSummaryFunc = func(ctx context.Context, runDir string, out io.Writer, end summary.RunEnd) (*summary.RunSummary, error) {
				summaries.Add(1)
				return generateRunSummary(ctx, runDir, out, end)
			}
			var shutdownMu sync.Mutex
			var shutdownHosts [][]string
			requestShutdownAllFunc = func(_ context.Context, hosts []*hostparse.HostInfo, _ string, _ time.Duration, _ int64, _ bool) error {
				var names []string
				for _, host := range hosts {
					names = append(names, host.Host)
				}
				shutdownMu.Lock()
				defer shutdownMu.Unlock()
				shutdownHosts = append(shutdownHosts, names)
				return errors.New("shutdown http: connect: connection refused")
			}
			// The shipped monitor writes progress to stdout or the TUI; capture it here.
			output := &lockedBuffer{}
			startAutoResultsFunc = func(
				ctx context.Context, baseURL, label, dir string, steps []string, runID int64, debug bool,
				hosts []*hostparse.HostInfo, apiPort string, shutdownOn bool, linger int, scenarioPath string,
				metadata *runMetadata, _ io.Writer, _ io.Writer, traceFile string, hook func(context.Context),
				options ...*integrity.FinalizeOptions,
			) *autoResultsMonitor {
				return startAutoResultsMonitor(ctx, baseURL, label, dir, steps, runID, debug, hosts, apiPort,
					shutdownOn, linger, scenarioPath, metadata, output, output, traceFile, hook, options...)
			}

			startErr := errors.New("engine did not become ready")
			if test.gate == gateProceeds {
				startErr = errors.New("failed to start test via API: failed to start test (status 500): refused")
			}
			var localCalls, remoteCalls atomic.Int32
			fail := func(ctx context.Context, hooks tui.LaunchHooks) error {
				if test.gate != gateNotRun {
					if _, err := hooks.RunPreSubmissionCheck(ctx); err != nil {
						return err
					}
				}
				if test.acceptedForCleanup {
					hooks.NotifyAcceptedForCleanup()
				}
				return startErr
			}
			checkAttach := func(orchestrator *tui.MultiHostOrchestrator) {
				if orchestrator.AttachExistingWorkersEnabled() != test.attach {
					t.Errorf("orchestrator attach mode = %t, want %t", orchestrator.AttachExistingWorkersEnabled(), test.attach)
				}
			}
			startLocalHeadlessRunFunc = func(_ string, _ string, _ scenario.Params, options headless.HeadlessOptions) error {
				localCalls.Add(1)
				return fail(options.Context, options.LaunchHooks)
			}
			startLocalTUIRunFunc = func(_ string, _ string, _ scenario.Params, options tui.RunOptions) error {
				localCalls.Add(1)
				return fail(options.Context, options.LaunchHooks)
			}
			startMultiHostHeadlessRunFunc = func(orchestrator *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions) error {
				checkAttach(orchestrator)
				remoteCalls.Add(1)
				return fail(options.Context, options.LaunchHooks)
			}
			startMultiHostTUIRunFunc = func(orchestrator *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options tui.RunOptions) error {
				checkAttach(orchestrator)
				remoteCalls.Add(1)
				return fail(options.Context, options.LaunchHooks)
			}

			err := runCmd.RunE(runCmd, []string{WorkloadTypeMock})
			if test.gate == gateRejects {
				if err == nil || !strings.Contains(err.Error(), "mismatch rejected") {
					t.Fatalf("RunE() error = %v, want the gate rejection", err)
				}
			} else if !errors.Is(err, startErr) {
				t.Fatalf("RunE() error = %v, want the start error", err)
			}
			if test.wantLocal != (localCalls.Load() == 1) || test.wantLocal == (remoteCalls.Load() == 1) {
				t.Fatalf("local/remote launch calls = %d/%d, want local %t", localCalls.Load(), remoteCalls.Load(), test.wantLocal)
			}

			metadataFiles, globErr := filepath.Glob(filepath.Join(resultsDir, "*", constants.ResultsMetadataFileName))
			if globErr != nil || len(metadataFiles) != 1 {
				t.Fatalf("run metadata files = %v (%v), want one", metadataFiles, globErr)
			}
			content, readErr := os.ReadFile(metadataFiles[0]) // #nosec G304 -- test temp directory
			if readErr != nil {
				t.Fatal(readErr)
			}
			var recorded struct {
				Lifecycle *runLifecycleMetadata `json:"lifecycle"`
			}
			if err := json.Unmarshal(content, &recorded); err != nil || recorded.Lifecycle == nil {
				t.Fatalf("decode lifecycle from %s: %v\n%s", metadataFiles[0], err, content)
			}
			summaryFiles, _ := filepath.Glob(filepath.Join(filepath.Dir(metadataFiles[0]), "*_results_summary.txt"))

			shutdownMu.Lock()
			defer shutdownMu.Unlock()
			wantHosts := strings.Split(test.hosts, ",")
			if test.attach {
				// Attached workers were started by someone else, so only the entry is asked.
				wantHosts = wantHosts[:1]
			}
			if test.acceptedForCleanup {
				if len(shutdownHosts) != 1 || !slices.Equal(shutdownHosts[0], wantHosts) {
					t.Fatalf("shutdown requests = %v, want one to %v", shutdownHosts, wantHosts)
				}
				if !recorded.Lifecycle.Shutdown.Started || recorded.Lifecycle.Shutdown.Error == "" {
					t.Fatalf("lifecycle.shutdown = %+v, want a started shutdown with the refusal", recorded.Lifecycle.Shutdown)
				}
				return
			}
			if len(shutdownHosts) != 0 {
				t.Fatalf("shutdown requests = %v, want none after a start that was never submitted", shutdownHosts)
			}
			if recorded.Lifecycle.Shutdown.Started || recorded.Lifecycle.Shutdown.Error != "" {
				t.Fatalf("lifecycle.shutdown = %+v, want not started", recorded.Lifecycle.Shutdown)
			}
			if test.gate == gateNotRun {
				if summaries.Load() != 0 || len(summaryFiles) != 0 {
					t.Fatalf("summary attempts = %d files = %v, want none", summaries.Load(), summaryFiles)
				}
				if recorded.Lifecycle.Summary.Started || recorded.Lifecycle.Summary.Error != "" {
					t.Fatalf("lifecycle.summary = %+v, want not started", recorded.Lifecycle.Summary)
				}
				assertNeverSubmittedCleanupOutput(t, output.String())
				return
			}
			// The gate's identity manifest gives the summary something to render.
			text := output.String()
			if len(summaryFiles) != 1 {
				t.Fatalf("summary files = %v, want one; output:\n%s", summaryFiles, text)
			}
			if !recorded.Lifecycle.Summary.Completed || recorded.Lifecycle.Summary.Error != "" {
				t.Fatalf("lifecycle.summary = %+v, want completed without error", recorded.Lifecycle.Summary)
			}
			saved, readErr := os.ReadFile(summaryFiles[0]) // #nosec G304 -- test temp directory
			if readErr != nil {
				t.Fatal(readErr)
			}
			wantSection := "Missing steps"
			if test.gate == gateRejects {
				wantSection = "Run rejected"
			}
			if !strings.Contains(string(saved), wantSection) {
				t.Fatalf("summary lacks %q:\n%s", wantSection, saved)
			}
			for _, want := range []string{neverSubmittedShutdownLine, "Auto-results: summary saved to"} {
				if !strings.Contains(text, want) {
					t.Errorf("output lacks %q:\n%s", want, text)
				}
			}
			for _, unwanted := range []string{neverSubmittedResultsLine, shutdownWarningLine, summaryIssueLine} {
				if strings.Contains(text, unwanted) {
					t.Errorf("output contains %q:\n%s", unwanted, text)
				}
			}
		})
	}
}
