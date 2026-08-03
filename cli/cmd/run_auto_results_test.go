package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/tui"
)

type fakeRunTracker struct {
	mu                    sync.Mutex
	called                bool
	stepIDs               []string
	debugCalled           bool
	debugValue            bool
	requireTerminalCalled bool
	requireTerminalValue  bool
	result                *portcheck.RunResult
	runID                 int64
}

// startAutoResults is a test-only convenience for cases that do not exercise
// the launch boundary itself. Production callers must arm through LaunchHooks.
func startAutoResults(parentCtx context.Context, baseURL, label, resultsDir string, expectedStepIDs []string, debug bool, allHosts []*hostparse.HostInfo, apiPort string, shutdownOn bool, lingerSec int, scenarioPath string, metadata *runMetadata, progressOut io.Writer, summaryOut io.Writer, traceFile string, preSummaryHook func(context.Context), integrityOptions ...*integrity.FinalizeOptions) chan autoResultsOutcome {
	expectedRunID := int64(0)
	if len(integrityOptions) > 0 && integrityOptions[0] != nil {
		expectedRunID = integrityOptions[0].RunID
	}
	monitor := startAutoResultsMonitor(
		parentCtx, baseURL, label, resultsDir, expectedStepIDs, expectedRunID, debug,
		allHosts, apiPort, shutdownOn, lingerSec, scenarioPath, metadata, progressOut,
		summaryOut, traceFile, preSummaryHook, integrityOptions...,
	)
	monitor.Arm()
	return monitor.done
}

func TestSelectResultStepIDsSeparatesRuntimeFactsFromPlannedAliases(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/index.json") {
			stepID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/logs/"), "/index.json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"step_id": stepID,
				"items": []map[string]any{{
					"logger": "metrics.FileTotal",
					"href":   "/logs/" + stepID + "/metrics.FileTotal",
					"size":   5,
				}},
			})
			return
		}
		if strings.HasSuffix(r.URL.Path, "/metrics.FileTotal") {
			_, _ = w.Write([]byte("total"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()

	tests := []struct {
		name     string
		expected []string
		fleet    []string
		node     []string
		want     []string
		runtime  []string
	}{
		{
			name: "equal ids", expected: []string{"create", "verify"},
			fleet: []string{"create", "verify"}, want: []string{"create", "verify"},
			runtime: []string{"create", "verify"},
		},
		{
			name: "reassigned ids", expected: []string{"planned-create", "planned-verify"},
			fleet: []string{"runtime-create", "runtime-verify"},
			want:  []string{"runtime-create", "runtime-verify"}, runtime: []string{"runtime-create", "runtime-verify"},
		},
		{
			name: "discovery unavailable", expected: []string{"planned-create", "planned-verify"},
			want: []string{"planned-create", "planned-verify"},
		},
		{
			name: "partial fleet completed by node", expected: []string{"planned-create", "planned-verify"},
			fleet: []string{"runtime-create"}, node: []string{"runtime-create", "runtime-verify"},
			want: []string{"runtime-create", "runtime-verify"}, runtime: []string{"runtime-create", "runtime-verify"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			selected, runtime := selectResultStepIDs(test.expected, test.fleet, test.node)
			if strings.Join(selected, ",") != strings.Join(test.want, ",") ||
				strings.Join(runtime, ",") != strings.Join(test.runtime, ",") {
				t.Fatalf("selectResultStepIDs() = selected %v runtime %v, want %v / %v",
					selected, runtime, test.want, test.runtime)
			}
			root := t.TempDir()
			fetcher := results.NewFetcher(server.URL, root)
			fetcher.Retries = 1
			fetcher.Artifacts = []results.ArtifactSpec{{
				Loggers: []string{"metrics.FileTotal"}, Suffix: "metrics.total.csv", Required: true,
			}}
			manifest, err := fetcher.FetchArtifactsForSteps(context.Background(), selected)
			if err != nil {
				t.Fatal(err)
			}
			assertManifestStepIDs := func(label string, got results.Manifest) {
				t.Helper()
				ids := make([]string, 0, len(got.Steps))
				for _, step := range got.Steps {
					ids = append(ids, step.StepID)
				}
				if strings.Join(ids, ",") != strings.Join(test.want, ",") {
					t.Fatalf("%s step IDs = %v, want exact selected set %v", label, ids, test.want)
				}
			}
			assertManifestStepIDs("returned manifest", *manifest)
			data, err := os.ReadFile(filepath.Join(root, constants.ResultsManifestFileName))
			if err != nil {
				t.Fatal(err)
			}
			var persisted results.Manifest
			if err = json.Unmarshal(data, &persisted); err != nil {
				t.Fatal(err)
			}
			assertManifestStepIDs("persisted manifest", persisted)
		})
	}
}

func (f *fakeRunTracker) WaitForCompletion(ctx context.Context, expected []string) (*portcheck.RunResult, error) {
	f.mu.Lock()
	f.called = true
	f.stepIDs = append([]string(nil), expected...)
	f.mu.Unlock()
	time.Sleep(550 * time.Millisecond)
	if f.result != nil {
		return f.result, nil
	}
	return &portcheck.RunResult{}, nil
}

func (f *fakeRunTracker) SetDebug(debug bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.debugCalled = true
	f.debugValue = debug
}

func (f *fakeRunTracker) SetExpectedRunID(runID int64) { f.runID = runID }

func (f *fakeRunTracker) SetRequireTerminalState(required bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requireTerminalCalled = true
	f.requireTerminalValue = required
}

type cancelAwareRunTracker struct {
	exited chan struct{}
}

func (t *cancelAwareRunTracker) WaitForCompletion(ctx context.Context, _ []string) (*portcheck.RunResult, error) {
	defer close(t.exited)
	<-ctx.Done()
	return &portcheck.RunResult{Steps: map[string]portcheck.StepCompletion{}}, ctx.Err()
}

func (*cancelAwareRunTracker) SetDebug(bool) {}

func (*cancelAwareRunTracker) SetExpectedRunID(int64) {}

func (*cancelAwareRunTracker) SetRequireTerminalState(bool) {}

type fakeFetcher struct {
	mu        sync.Mutex
	stepIDs   []string
	baseURL   string
	output    string
	manifest  *results.Manifest
	err       error
	onFetch   func(output string, stepIDs []string) error
	onContext func(context.Context)
}

func (f *fakeFetcher) FetchArtifactsForSteps(ctx context.Context, stepIDs []string) (*results.Manifest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.onContext != nil {
		f.onContext(ctx)
	}
	f.stepIDs = append([]string(nil), stepIDs...)
	if f.onFetch != nil {
		if err := f.onFetch(f.output, stepIDs); err != nil {
			return nil, err
		}
	}
	if f.manifest != nil || f.err != nil {
		return f.manifest, f.err
	}
	return &results.Manifest{}, nil
}

type eventWriter struct {
	mu     sync.Mutex
	events []string
}

func (w *eventWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, line := range strings.Split(string(p), "\n") {
		if strings.TrimSpace(line) != "" {
			w.events = append(w.events, line)
		}
	}
	return len(p), nil
}

func (w *eventWriter) snapshot() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make([]string, len(w.events))
	copy(out, w.events)
	return out
}

func indexOfEventContaining(events []string, needle string) int {
	for i, event := range events {
		if strings.Contains(event, needle) {
			return i
		}
	}
	return -1
}

// TestStartAutoResults_StaleDiscoveredIDsFromPriorRun reproduces the bug where the background
// discoverStepIDsFunc poller accumulates step IDs from a previous run's lingering engine container
// (which keeps its /metrics/json endpoint alive during the API linger window). Those stale IDs must
// NOT be forwarded to FetchArtifactsForSteps; only IDs that belong to the current scenario should
// be fetched.
//
// Failure mode: the poller fires during WaitForCompletion's sleep and picks up stale IDs from a
// prior run. uniqueStepIDs merges expectedStepIDs + cachedDiscovered without filtering out IDs
// that don't belong to the current run, so FetchArtifactsForSteps is called with 6 IDs instead of 3,
// then hangs trying to retrieve log artifacts from a dead container.
func TestStartAutoResults_StaleDiscoveredIDsFromPriorRun(t *testing.T) {
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	defer func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
	}()

	tracker := &fakeRunTracker{}
	newRunTrackerFunc = func(baseURL string) autoResultsRunTracker { return tracker }

	// Fleet endpoint unavailable (older engine)
	discoverFleetStepIDsFunc = func(_ context.Context, baseURL string, _ int64) ([]string, error) { return nil, nil }

	// Stale IDs from a prior aborted run (timestamp .367), current run has timestamp .368.
	staleIDs := []string{
		"mt-001-20260303.154534.367-provision",
		"mt-002-20260303.154534.367-write",
		"mt-003-20260303.154534.367-compaction",
	}
	currentIDs := []string{
		"mt-001-20260303.154534.368-provision",
		"mt-002-20260303.154534.368-write",
		"mt-003-20260303.154534.368-compaction",
	}

	// The mock simulates the production current-run filter against a lingering prior container:
	// early calls have no matching current-run IDs, while later calls expose only the current set.
	// The lower-level discovery tests independently prove retained metrics are filtered by run ID.
	var discoverCallCount int
	var discoverMu sync.Mutex
	discoverStepIDsFunc = func(_ context.Context, baseURL string, _ int64) ([]string, error) {
		discoverMu.Lock()
		discoverCallCount++
		n := discoverCallCount
		discoverMu.Unlock()
		if n <= 2 {
			return nil, nil
		}
		return append([]string(nil), currentIDs...), nil
	}

	fetcher := &fakeFetcher{}
	newResultsFetcherFunc = func(baseURL, outputDir string) autoResultsFetcher {
		fetcher.baseURL = baseURL
		fetcher.output = outputDir
		return fetcher
	}

	generateRunSummaryFunc = func(ctx context.Context, runDir string, out io.Writer) error { return nil }

	tmpDir := t.TempDir()
	done := startAutoResults(context.Background(), "http://example", "mt", tmpDir, currentIDs, false, nil, "", false, 0, "", nil, io.Discard, io.Discard, "", nil)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("startAutoResults did not complete in time (possible hang due to stale step IDs)")
	}
	tracker.mu.Lock()
	if !tracker.requireTerminalCalled || tracker.requireTerminalValue {
		tracker.mu.Unlock()
		t.Fatalf("ordinary terminal policy = called %t value %t, want explicit false",
			tracker.requireTerminalCalled, tracker.requireTerminalValue)
	}
	tracker.mu.Unlock()

	fetcher.mu.Lock()
	defer fetcher.mu.Unlock()

	// Only the 3 current-run IDs should be fetched; stale IDs from the prior run must be excluded.
	if len(fetcher.stepIDs) != len(currentIDs) {
		t.Fatalf("FetchArtifactsForSteps called with %d IDs %v, want only the %d current-run IDs %v",
			len(fetcher.stepIDs), fetcher.stepIDs, len(currentIDs), currentIDs)
	}
	for _, id := range fetcher.stepIDs {
		for _, stale := range staleIDs {
			if id == stale {
				t.Fatalf("stale ID %q from prior run leaked into FetchArtifactsForSteps call; got %v", id, fetcher.stepIDs)
			}
		}
	}
}

func TestAutoResultsLaunchGateRejectsAllPreSubmissionEvidence(t *testing.T) {
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

	const expectedRunID = int64(17)
	stepID := "mt-001-current-verify"
	tracker := &fakeRunTracker{result: &portcheck.RunResult{
		FinalState: constants.StateCompleted, RunID: expectedRunID,
	}}
	var trackerCreates atomic.Int32
	newRunTrackerFunc = func(string) autoResultsRunTracker {
		trackerCreates.Add(1)
		return tracker
	}
	var discoveries atomic.Int32
	discoverStepIDsFunc = func(_ context.Context, _ string, runID int64) ([]string, error) {
		discoveries.Add(1)
		if runID != expectedRunID {
			t.Errorf("node discovery run ID = %d", runID)
		}
		return []string{stepID}, nil
	}
	discoverFleetStepIDsFunc = func(_ context.Context, _ string, runID int64) ([]string, error) {
		discoveries.Add(1)
		if runID != expectedRunID {
			t.Errorf("fleet discovery run ID = %d", runID)
		}
		return nil, nil
	}
	var fetches atomic.Int32
	newResultsFetcherFunc = func(_, output string) autoResultsFetcher {
		return &fakeFetcher{output: output, onFetch: func(_ string, _ []string) error {
			fetches.Add(1)
			return nil
		}}
	}
	generateRunSummaryFunc = func(context.Context, string, io.Writer) error { return nil }
	var shutdowns atomic.Int32
	requestShutdownAllFunc = func(context.Context, []*hostparse.HostInfo, string, time.Duration, bool) error {
		shutdowns.Add(1)
		return nil
	}

	monitor := startAutoResultsMonitor(
		context.Background(), "http://example", "mt", t.TempDir(), []string{stepID}, expectedRunID,
		false, nil, "9999", true, 1, "", nil, io.Discard, io.Discard, "", nil,
	)
	time.Sleep(75 * time.Millisecond)
	if trackerCreates.Load() != 0 || discoveries.Load() != 0 || fetches.Load() != 0 || shutdowns.Load() != 0 {
		t.Fatalf("pre-arm effects tracker=%d discovery=%d fetch=%d shutdown=%d",
			trackerCreates.Load(), discoveries.Load(), fetches.Load(), shutdowns.Load())
	}
	monitor.Arm()
	select {
	case outcome := <-monitor.done:
		if outcome.TrackerErr != nil || outcome.Tracker == nil || outcome.Tracker.RunID != expectedRunID {
			t.Fatalf("armed outcome = %+v", outcome)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("armed monitor did not complete")
	}
	if tracker.runID != expectedRunID || fetches.Load() != 1 || shutdowns.Load() != 1 {
		t.Fatalf("armed effects tracker run=%d fetch=%d shutdown=%d",
			tracker.runID, fetches.Load(), shutdowns.Load())
	}
}

func TestAutoResultsBindsLingeringEngineAndFleetEvidenceToArmedRun(t *testing.T) {
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	t.Cleanup(func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
	})

	const expectedRunID = int64(17)
	var requestCount atomic.Int32
	var statusCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		requestCount.Add(1)
		switch call := statusCalls.Add(1); {
		case call == 1:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"state": "IDLE", "run_id": 16, "step_id": "prior-verify",
			})
		case call < 8:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"state": "STARTING", "run_id": expectedRunID, "step_id": "runtime-create",
			})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"state": "COMPLETED", "run_id": expectedRunID, "step_id": "runtime-verify",
			})
		}
	})
	metrics := []map[string]any{
		{
			"run_id": 16, "step_id": "prior-verify", "timestamp": 1000, "terminal": true,
			"operations": map[string]any{"success_rate_last": 0},
			"bandwidth":  map[string]any{"bytes_rate_last": 0},
		},
		{
			"run_id": expectedRunID, "step_id": "runtime-create", "timestamp": 2000,
			"operations": map[string]any{"success_rate_last": 1},
			"bandwidth":  map[string]any{"bytes_rate_last": 0},
		},
		{
			"run_id": expectedRunID, "step_id": "runtime-verify", "timestamp": 2001,
			"operations": map[string]any{"success_rate_last": 0},
			"bandwidth":  map[string]any{"bytes_rate_last": 0},
		},
	}
	for _, endpoint := range []string{"/metrics/json", "/metrics/fleet/json"} {
		mux.HandleFunc(endpoint, func(w http.ResponseWriter, _ *http.Request) {
			requestCount.Add(1)
			_ = json.NewEncoder(w).Encode(metrics)
		})
	}
	mux.HandleFunc("/logs/", func(w http.ResponseWriter, _ *http.Request) {
		requestCount.Add(1)
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mux.ServeHTTP(w, r)
	}))
	defer server.Close()

	newRunTrackerFunc = func(baseURL string) autoResultsRunTracker {
		tracker := portcheck.NewRunTracker(baseURL)
		tracker.PollInterval = 2 * time.Millisecond
		tracker.IdleGrace = time.Millisecond
		tracker.StartupTimeout = 20 * time.Millisecond
		tracker.UnavailableTimeout = 50 * time.Millisecond
		return &runTrackerAdapter{RunTracker: tracker}
	}
	discoverStepIDsFunc = results.DiscoverStepIDsForRunContext
	discoverFleetStepIDsFunc = results.DiscoverFleetStepIDsForRunContext
	fetcher := &fakeFetcher{}
	newResultsFetcherFunc = func(_, output string) autoResultsFetcher {
		fetcher.output = output
		return fetcher
	}
	generateRunSummaryFunc = func(context.Context, string, io.Writer) error { return nil }

	monitor := startAutoResultsMonitor(
		context.Background(), server.URL, "mt", t.TempDir(),
		[]string{"expected-create", "expected-verify"}, expectedRunID,
		false, nil, "", false, 0, "", nil, io.Discard, io.Discard, "", nil,
	)
	time.Sleep(25 * time.Millisecond)
	if got := requestCount.Load(); got != 0 {
		t.Fatalf("pre-arm engine requests = %d, want 0", got)
	}
	monitor.Arm()
	select {
	case outcome := <-monitor.done:
		if outcome.TrackerErr != nil || outcome.Tracker == nil {
			t.Fatalf("monitor outcome = %+v", outcome)
		}
		if outcome.Tracker.RunID != expectedRunID || outcome.Tracker.FinalState != constants.StateCompleted {
			t.Fatalf("terminal tracker result = %+v", outcome.Tracker)
		}
		for _, stale := range []string{"prior-verify"} {
			for _, stepID := range outcome.StepIDs {
				if stepID == stale {
					t.Fatalf("stale step %q reached result processing: %v", stale, outcome.StepIDs)
				}
			}
		}
		if len(outcome.StepIDs) < 2 || outcome.StepIDs[0] != "runtime-create" || outcome.StepIDs[1] != "runtime-verify" {
			t.Fatalf("runtime fleet IDs did not lead result processing: %v", outcome.StepIDs)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("armed current-run monitor did not complete")
	}
}

// Run-filtered node discovery is execution evidence. Once present, it replaces
// rather than augments CLI-generated aliases.
func TestStartAutoResults_NodeRuntimeDiscoveryReplacesExpectedAliases(t *testing.T) {
	t.Parallel()

	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	defer func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
	}()

	tracker := &fakeRunTracker{}
	newRunTrackerFunc = func(baseURL string) autoResultsRunTracker {
		if baseURL != "http://example" {
			t.Fatalf("unexpected baseURL %q", baseURL)
		}
		return tracker
	}

	// Fleet endpoint unavailable (older engine)
	discoverFleetStepIDsFunc = func(_ context.Context, baseURL string, _ int64) ([]string, error) { return nil, nil }

	// Discovery returns the current engine ID with a reassigned timestamp.
	discoverCalls := 0
	discoverStepIDsFunc = func(_ context.Context, baseURL string, _ int64) ([]string, error) {
		discoverCalls++
		return []string{"runtime-create"}, nil
	}

	fetcher := &fakeFetcher{}
	newResultsFetcherFunc = func(baseURL, outputDir string) autoResultsFetcher {
		fetcher.baseURL = baseURL
		fetcher.output = outputDir
		return fetcher
	}

	summaryCalls := 0
	generateRunSummaryFunc = func(ctx context.Context, runDir string, out io.Writer) error {
		summaryCalls++
		fetcher.mu.Lock()
		defer fetcher.mu.Unlock()
		if runDir != fetcher.output {
			t.Fatalf("unexpected runDir %q", runDir)
		}
		return nil
	}

	tmpDir := t.TempDir()
	done := startAutoResults(context.Background(), "http://example", "mt", tmpDir, []string{"expected-create"}, false, nil, "", false, 0, "", nil, io.Discard, io.Discard, "", nil)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("startAutoResults did not complete in time")
	}

	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	if !tracker.called {
		t.Fatal("WaitForCompletion was not called")
	}
	if !tracker.debugCalled {
		t.Fatal("SetDebug was not invoked")
	}

	fetcher.mu.Lock()
	defer fetcher.mu.Unlock()
	if len(fetcher.stepIDs) != 1 || fetcher.stepIDs[0] != "runtime-create" {
		t.Fatalf("unexpected stepIDs %v, want exact runtime set [runtime-create]", fetcher.stepIDs)
	}

	if fetcher.baseURL != "http://example" {
		t.Fatalf("fetcher baseURL = %q", fetcher.baseURL)
	}
	if !strings.HasPrefix(fetcher.output, filepath.Join(tmpDir, "mt-")) {
		t.Fatalf("unexpected output dir %q", fetcher.output)
	}
	if summaryCalls != 1 {
		t.Fatalf("expected 1 summary call, got %d", summaryCalls)
	}
	if discoverCalls == 0 {
		t.Fatal("discoverStepIDsFunc was never called")
	}
}

// TestStartAutoResults_FleetDiscoveryPreferred verifies that when the fleet endpoint
// returns step IDs different from expected IDs (the distributed mode step-ID mismatch),
// the fleet-discovered IDs are used for fetching instead of the CLI-generated expected IDs.
func TestStartAutoResults_FleetDiscoveryPreferred(t *testing.T) {
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	defer func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
	}()

	tracker := &fakeRunTracker{}
	newRunTrackerFunc = func(baseURL string) autoResultsRunTracker { return tracker }

	// CLI-generated expected IDs (with timestamp .519)
	expectedIDs := []string{
		"mt-001-20260312.192453.519-create",
		"mt-002-20260312.192453.519-delete",
	}

	// Engine's actual IDs (with timestamp .387) — returned by fleet endpoint
	fleetIDs := []string{
		"mt-001-20260312.192454.387-create",
		"mt-002-20260312.192454.387-delete",
	}

	// Node-local /metrics/json returns nothing useful in distributed mode
	discoverStepIDsFunc = func(_ context.Context, baseURL string, _ int64) ([]string, error) { return nil, nil }

	// Fleet endpoint returns the engine's actual step IDs
	discoverFleetStepIDsFunc = func(_ context.Context, baseURL string, _ int64) ([]string, error) {
		return fleetIDs, nil
	}

	fetcher := &fakeFetcher{}
	newResultsFetcherFunc = func(baseURL, outputDir string) autoResultsFetcher {
		fetcher.baseURL = baseURL
		fetcher.output = outputDir
		return fetcher
	}
	generateRunSummaryFunc = func(ctx context.Context, runDir string, out io.Writer) error { return nil }

	tmpDir := t.TempDir()
	metadata := &runMetadata{ExpectedStepIDs: append([]string(nil), expectedIDs...)}
	done := startAutoResults(context.Background(), "http://example", "mt", tmpDir, expectedIDs, false, nil, "", false, 0, "", metadata, io.Discard, io.Discard, "", nil)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("startAutoResults did not complete in time")
	}

	fetcher.mu.Lock()
	defer fetcher.mu.Unlock()

	if strings.Join(fetcher.stepIDs, ",") != strings.Join(fleetIDs, ",") {
		t.Fatalf("fetcher step IDs = %v, want exact runtime set %v", fetcher.stepIDs, fleetIDs)
	}
	if strings.Join(metadata.ActualStepIDs, ",") != strings.Join(fleetIDs, ",") {
		t.Fatalf("actual step IDs = %v, want exact runtime set %v", metadata.ActualStepIDs, fleetIDs)
	}
	if strings.Join(metadata.DiscoveredStepIDs, ",") != strings.Join(fleetIDs, ",") {
		t.Fatalf("discovered step IDs = %v, want exact runtime set %v", metadata.DiscoveredStepIDs, fleetIDs)
	}
}

// TestStartAutoResults_FleetUnavailableFallback verifies that when the fleet endpoint
// is unavailable (404, older engine), the system falls back to expected IDs.
func TestStartAutoResults_FleetUnavailableFallback(t *testing.T) {
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	defer func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
	}()

	tracker := &fakeRunTracker{}
	newRunTrackerFunc = func(baseURL string) autoResultsRunTracker { return tracker }

	expectedIDs := []string{
		"mt-001-20260312.192453.519-create",
		"mt-002-20260312.192453.519-delete",
	}

	// Node-local returns matching IDs (non-distributed mode)
	discoverStepIDsFunc = func(_ context.Context, baseURL string, _ int64) ([]string, error) {
		return expectedIDs, nil
	}

	// Fleet endpoint unavailable (404)
	discoverFleetStepIDsFunc = func(_ context.Context, baseURL string, _ int64) ([]string, error) { return nil, nil }

	fetcher := &fakeFetcher{}
	newResultsFetcherFunc = func(baseURL, outputDir string) autoResultsFetcher {
		fetcher.baseURL = baseURL
		fetcher.output = outputDir
		return fetcher
	}
	generateRunSummaryFunc = func(ctx context.Context, runDir string, out io.Writer) error { return nil }

	tmpDir := t.TempDir()
	done := startAutoResults(context.Background(), "http://example", "mt", tmpDir, expectedIDs, false, nil, "", false, 0, "", nil, io.Discard, io.Discard, "", nil)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("startAutoResults did not complete in time")
	}

	fetcher.mu.Lock()
	defer fetcher.mu.Unlock()

	// Should fall back to expected IDs when fleet is unavailable
	if len(fetcher.stepIDs) != len(expectedIDs) {
		t.Fatalf("expected %d step IDs, got %d: %v", len(expectedIDs), len(fetcher.stepIDs), fetcher.stepIDs)
	}
	expectedSet := make(map[string]bool, len(expectedIDs))
	for _, id := range expectedIDs {
		expectedSet[id] = true
	}
	for _, id := range fetcher.stepIDs {
		if !expectedSet[id] {
			t.Fatalf("unexpected step ID %q in fetcher.stepIDs; got %v", id, fetcher.stepIDs)
		}
	}
}

func TestStartAutoResults_AppendsTraceArtifactToManifest(t *testing.T) {
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	defer func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
	}()

	tracker := &fakeRunTracker{}
	newRunTrackerFunc = func(baseURL string) autoResultsRunTracker { return tracker }

	stepID := "mt-001-20260506.204108.064-write"
	discoverStepIDsFunc = func(_ context.Context, baseURL string, _ int64) ([]string, error) {
		return []string{stepID}, nil
	}
	discoverFleetStepIDsFunc = func(_ context.Context, baseURL string, _ int64) ([]string, error) { return nil, nil }

	traceSrcDir := t.TempDir()
	traceFile := filepath.Join(traceSrcDir, "spt-20260506.204108.064.trace.log")
	if err := os.WriteFile(traceFile, []byte("trace-data\n"), 0o600); err != nil {
		t.Fatalf("write trace file: %v", err)
	}

	fetcher := &fakeFetcher{
		onFetch: func(output string, stepIDs []string) error {
			if err := os.MkdirAll(output, 0o755); err != nil {
				return err
			}
			manifest := &results.Manifest{
				BaseURL:   "http://example",
				OutputDir: output,
				Steps: []results.StepManifest{
					{
						StepID: stepID,
						Files: []results.FileStatus{
							{Name: stepID + ".metrics.total.csv", Status: "ok"},
						},
					},
				},
			}
			data, err := json.MarshalIndent(manifest, "", "  ")
			if err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(output, constants.ResultsManifestFileName), data, 0o600)
		},
	}
	newResultsFetcherFunc = func(baseURL, outputDir string) autoResultsFetcher {
		fetcher.baseURL = baseURL
		fetcher.output = outputDir
		return fetcher
	}
	generateRunSummaryFunc = func(ctx context.Context, runDir string, out io.Writer) error { return nil }

	tmpDir := t.TempDir()
	done := startAutoResults(context.Background(), "http://example", "mt", tmpDir, []string{stepID}, false, nil, "", false, 0, "", nil, io.Discard, io.Discard, traceFile, nil)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("startAutoResults did not complete in time")
	}

	manifestPath := filepath.Join(fetcher.output, constants.ResultsManifestFileName)
	content, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var got results.Manifest
	if err := json.Unmarshal(content, &got); err != nil {
		t.Fatalf("unmarshal manifest: %v", err)
	}
	if len(got.RunFiles) != 1 {
		t.Fatalf("RunFiles length = %d, want 1", len(got.RunFiles))
	}
	if got.RunFiles[0].Name != filepath.Base(traceFile) {
		t.Fatalf("RunFiles[0].Name = %q, want %q", got.RunFiles[0].Name, filepath.Base(traceFile))
	}
	if got.RunFiles[0].Status != "ok" {
		t.Fatalf("RunFiles[0].Status = %q, want ok", got.RunFiles[0].Status)
	}

	traceCopyPath := filepath.Join(fetcher.output, filepath.Base(traceFile))
	if _, err := os.Stat(traceCopyPath); err != nil {
		t.Fatalf("expected trace file copied to results root: %v", err)
	}
}

func TestStartAutoResults_EmitsSummaryAfterShutdown(t *testing.T) {
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	origShutdown := requestShutdownAllFunc
	defer func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
		requestShutdownAllFunc = origShutdown
	}()

	stepID := "mt-001-20260604.195722.504-mixed"
	newRunTrackerFunc = func(baseURL string) autoResultsRunTracker { return &fakeRunTracker{} }
	discoverStepIDsFunc = func(_ context.Context, baseURL string, _ int64) ([]string, error) { return []string{stepID}, nil }
	discoverFleetStepIDsFunc = func(_ context.Context, baseURL string, _ int64) ([]string, error) { return nil, nil }
	newResultsFetcherFunc = func(baseURL, outputDir string) autoResultsFetcher {
		return &fakeFetcher{output: outputDir}
	}
	requestShutdownAllFunc = func(context.Context, []*hostparse.HostInfo, string, time.Duration, bool) error {
		return nil
	}
	generateRunSummaryFunc = func(ctx context.Context, runDir string, out io.Writer) error {
		_, err := io.WriteString(out, "FINAL SUMMARY\n")
		return err
	}

	events := &eventWriter{}
	var hookCalled int32
	preSummaryHook := func(context.Context) {
		atomic.AddInt32(&hookCalled, 1)
		_, _ = events.Write([]byte("PRE-SUMMARY HOOK RAN\n"))
	}
	done := startAutoResults(
		context.Background(),
		"http://example",
		"mt",
		t.TempDir(),
		[]string{stepID},
		false,
		nil,
		"9999",
		true,
		1,
		"",
		nil,
		events,
		events,
		"",
		preSummaryHook,
	)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("startAutoResults did not complete in time")
	}

	if atomic.LoadInt32(&hookCalled) != 1 {
		t.Fatalf("preSummaryHook called %d times, want exactly 1", hookCalled)
	}

	got := events.snapshot()
	summaryIndex := indexOfEventContaining(got, "FINAL SUMMARY")
	shutdownIndex := indexOfEventContaining(got, "Shutdown completed successfully.")
	hookIndex := indexOfEventContaining(got, "PRE-SUMMARY HOOK RAN")
	if summaryIndex < 0 {
		t.Fatalf("summary was not emitted; events=%v", got)
	}
	if shutdownIndex < 0 {
		t.Fatalf("shutdown completion was not emitted; events=%v", got)
	}
	if hookIndex < 0 {
		t.Fatalf("pre-summary hook was not run; events=%v", got)
	}
	if summaryIndex <= shutdownIndex {
		t.Fatalf("summary should be emitted after shutdown completion; events=%v", got)
	}
	if hookIndex <= shutdownIndex {
		t.Fatalf("pre-summary hook should run after shutdown completion; events=%v", got)
	}
	if summaryIndex <= hookIndex {
		t.Fatalf("summary should be emitted after the pre-summary hook runs (this is the whole point of "+
			"the hook: diagnostics/cleanup must be observable before the summary is); events=%v", got)
	}
}

func TestStartAutoResults_FetchFailureStillShutsDownAndEmitsSummary(t *testing.T) {
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	origShutdown := requestShutdownAllFunc
	defer func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
		requestShutdownAllFunc = origShutdown
	}()

	stepID := "mt-001-integrity-verify"
	newRunTrackerFunc = func(string) autoResultsRunTracker {
		return &fakeRunTracker{result: &portcheck.RunResult{FinalState: constants.StateFailed}}
	}
	discoverStepIDsFunc = func(context.Context, string, int64) ([]string, error) { return []string{stepID}, nil }
	discoverFleetStepIDsFunc = func(context.Context, string, int64) ([]string, error) { return nil, nil }
	newResultsFetcherFunc = func(string, string) autoResultsFetcher {
		return &fakeFetcher{err: errors.New("truncated integrity artifact")}
	}

	var shutdownCalled int32
	requestShutdownAllFunc = func(context.Context, []*hostparse.HostInfo, string, time.Duration, bool) error {
		atomic.AddInt32(&shutdownCalled, 1)
		return nil
	}
	var summaryCalled int32
	generateRunSummaryFunc = func(context.Context, string, io.Writer) error {
		atomic.AddInt32(&summaryCalled, 1)
		return nil
	}

	done := startAutoResults(
		context.Background(),
		"http://example", "mt", t.TempDir(), []string{stepID}, false, nil, "9999", true, 1,
		"", nil, io.Discard, io.Discard, "", nil,
	)
	select {
	case outcome := <-done:
		if outcome.ArtifactErr == nil || !strings.Contains(outcome.ArtifactErr.Error(), "truncated integrity artifact") {
			t.Fatalf("ArtifactErr = %v, want fetch failure", outcome.ArtifactErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("startAutoResults did not complete in time")
	}
	if atomic.LoadInt32(&shutdownCalled) != 1 {
		t.Fatalf("shutdown calls = %d, want 1", shutdownCalled)
	}
	if atomic.LoadInt32(&summaryCalled) != 1 {
		t.Fatalf("summary calls = %d, want 1", summaryCalled)
	}
}

func TestWriteRunMetadataCapturesAvailableRuntimeIdentity(t *testing.T) {
	root := t.TempDir()
	id := "sha256:" + strings.Repeat("a", 64)
	meta := &runMetadata{
		runtimeIdentityProvider: func() (*tui.DistributedRuntimeIdentityEvidence, error) {
			return &tui.DistributedRuntimeIdentityEvidence{
				Tier:    tui.RuntimeIdentityTierAvailableImage,
				ImageID: id,
			}, nil
		},
	}

	if err := writeRunMetadata(meta, root); err != nil {
		t.Fatal(err)
	}
	if meta.RuntimeIdentity == nil || meta.RuntimeIdentity.ImageID != id ||
		meta.RuntimeIdentityError != "" {
		t.Fatalf("unexpected runtime evidence: identity=%+v error=%q",
			meta.RuntimeIdentity, meta.RuntimeIdentityError)
	}
}

// TestStartAutoResults_FinalizesResourcesWhenGracefulShutdownDisabled proves
// --shutdown-on-complete controls the graceful API request, not mandatory
// launcher-owned container and staging disposal. Resource finalization remains
// ordered after evidence and before summary.
func TestStartAutoResults_FinalizesResourcesWhenGracefulShutdownDisabled(t *testing.T) {
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	defer func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
	}()

	stepID := "mt-001-20260604.195722.504-mixed"
	newRunTrackerFunc = func(baseURL string) autoResultsRunTracker { return &fakeRunTracker{} }
	discoverStepIDsFunc = func(_ context.Context, baseURL string, _ int64) ([]string, error) { return []string{stepID}, nil }
	discoverFleetStepIDsFunc = func(_ context.Context, baseURL string, _ int64) ([]string, error) { return nil, nil }
	newResultsFetcherFunc = func(baseURL, outputDir string) autoResultsFetcher {
		return &fakeFetcher{output: outputDir}
	}
	generateRunSummaryFunc = func(ctx context.Context, runDir string, out io.Writer) error { return nil }

	var hookCalled int32
	preSummaryHook := func(context.Context) { atomic.AddInt32(&hookCalled, 1) }

	// shutdownOn = false
	done := startAutoResults(context.Background(), "http://example", "mt", t.TempDir(), []string{stepID}, false, nil, "", false, 0, "", nil, io.Discard, io.Discard, "", preSummaryHook)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("startAutoResults did not complete in time")
	}

	if atomic.LoadInt32(&hookCalled) != 1 {
		t.Fatalf("preSummaryHook called %d times with shutdown disabled, want 1", hookCalled)
	}
}

func TestStartAutoResultsCancellationTerminatesMonitorWorker(t *testing.T) {
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origSummary := generateRunSummaryFunc
	defer func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		generateRunSummaryFunc = origSummary
	}()

	tracker := &cancelAwareRunTracker{exited: make(chan struct{})}
	newRunTrackerFunc = func(string) autoResultsRunTracker { return tracker }
	discoverStepIDsFunc = func(context.Context, string, int64) ([]string, error) { return nil, nil }
	discoverFleetStepIDsFunc = func(context.Context, string, int64) ([]string, error) { return nil, nil }
	generateRunSummaryFunc = func(context.Context, string, io.Writer) error { return nil }

	ctx, cancel := context.WithCancel(context.Background())
	done := startAutoResults(
		ctx,
		"http://127.0.0.1:1", "mt", t.TempDir(), nil, false, nil, "", false, 0, "", nil,
		io.Discard, io.Discard, "", nil, &integrity.FinalizeOptions{})
	cancel()

	select {
	case outcome := <-done:
		if !errors.Is(outcome.TrackerErr, context.Canceled) {
			t.Fatalf("tracker error = %v, want context.Canceled", outcome.TrackerErr)
		}
	case <-time.After(time.Second):
		t.Fatal("auto-results monitor did not terminate after cancellation")
	}
	select {
	case <-tracker.exited:
	default:
		t.Fatal("tracker worker had not exited when auto-results completed")
	}
}

func TestStartAutoResultsCancellationUsesIndependentPhaseContextsWithShutdown(t *testing.T) {
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	origShutdown := requestShutdownAllFunc
	defer func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
		requestShutdownAllFunc = origShutdown
	}()

	tracker := &cancelAwareRunTracker{exited: make(chan struct{})}
	newRunTrackerFunc = func(string) autoResultsRunTracker { return tracker }
	stepID := "mt-001-verify"
	assertCleanupContext := func(stage string, ctx context.Context, budget time.Duration) {
		t.Helper()
		if err := ctx.Err(); err != nil {
			t.Errorf("%s context is already canceled: %v", stage, err)
		}
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Errorf("%s context has no cleanup deadline", stage)
			return
		}
		remaining := time.Until(deadline)
		if remaining <= 0 || remaining > budget {
			t.Errorf("%s cleanup deadline remaining = %s", stage, remaining)
		}
	}
	discoverStepIDsFunc = func(ctx context.Context, _ string, _ int64) ([]string, error) {
		assertCleanupContext("discovery", ctx, constants.AutoResultsCancelCleanupTimeout)
		return []string{stepID}, nil
	}
	discoverFleetStepIDsFunc = func(ctx context.Context, _ string, _ int64) ([]string, error) {
		assertCleanupContext("fleet discovery", ctx, constants.AutoResultsCancelCleanupTimeout)
		return nil, nil
	}
	newResultsFetcherFunc = func(_, outputDir string) autoResultsFetcher {
		return &fakeFetcher{output: outputDir, onFetch: func(_ string, _ []string) error {
			return nil
		}, onContext: func(ctx context.Context) {
			assertCleanupContext("artifact fetch", ctx, constants.AutoResultsCancelCleanupTimeout)
		}}
	}
	var shutdownCalled int32
	requestShutdownAllFunc = func(ctx context.Context, _ []*hostparse.HostInfo, _ string, _ time.Duration, _ bool) error {
		assertCleanupContext("shutdown", ctx, constants.AutoResultsShutdownTimeout)
		atomic.AddInt32(&shutdownCalled, 1)
		return nil
	}
	var summaryCalled int32
	generateRunSummaryFunc = func(ctx context.Context, _ string, _ io.Writer) error {
		assertCleanupContext("summary", ctx, constants.AutoResultsSummaryTimeout)
		atomic.AddInt32(&summaryCalled, 1)
		return nil
	}
	var hookCalled int32
	hook := func(ctx context.Context) {
		if err := ctx.Err(); err != nil {
			t.Errorf("pre-summary hook context is already canceled: %v", err)
		}
		if _, ok := ctx.Deadline(); ok {
			t.Error("pre-summary hook inherited an aggregate deadline; canonical finalizer must own independent phase budgets")
		}
		atomic.AddInt32(&hookCalled, 1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := startAutoResults(
		ctx, "http://127.0.0.1:1", "mt", t.TempDir(), []string{stepID}, false, nil, "9999",
		true, 1, "", nil, io.Discard, io.Discard, "", hook)
	cancel()

	select {
	case outcome := <-done:
		if !errors.Is(outcome.TrackerErr, context.Canceled) {
			t.Fatalf("tracker error = %v, want context.Canceled", outcome.TrackerErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("auto-results cleanup did not finish within its bounded budget")
	}
	if atomic.LoadInt32(&shutdownCalled) != 1 || atomic.LoadInt32(&hookCalled) != 1 ||
		atomic.LoadInt32(&summaryCalled) != 1 {
		t.Fatalf("cleanup calls shutdown=%d hook=%d summary=%d, want all 1",
			shutdownCalled, hookCalled, summaryCalled)
	}
}

func TestStartAutoResultsKeepsPreparedInputsThroughConsumersAndReportsCleanupFailure(t *testing.T) {
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	defer func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
	}()

	stepID := "mt-001-create"
	newRunTrackerFunc = func(string) autoResultsRunTracker {
		return &fakeRunTracker{result: &portcheck.RunResult{FinalState: constants.StateCompleted}}
	}
	discoverStepIDsFunc = func(context.Context, string, int64) ([]string, error) {
		return []string{stepID}, nil
	}
	discoverFleetStepIDsFunc = func(context.Context, string, int64) ([]string, error) {
		return nil, nil
	}

	workspace := t.TempDir()
	preparedPath := filepath.Join(workspace, "prepared.js")
	if err := os.WriteFile(preparedPath, []byte("exact scenario"), 0o600); err != nil {
		t.Fatal(err)
	}
	assertPreparedPresent := func(stage string) error {
		if _, err := os.Stat(preparedPath); err != nil {
			return errors.New(stage + " observed prepared input missing: " + err.Error())
		}
		return nil
	}
	var fetchErr error
	newResultsFetcherFunc = func(_, outputDir string) autoResultsFetcher {
		return &fakeFetcher{
			output: outputDir,
			onFetch: func(_ string, _ []string) error {
				fetchErr = assertPreparedPresent("artifact fetch")
				return fetchErr
			},
		}
	}
	finalizerChecked := false
	preSummaryHook := func(context.Context) {
		if err := assertPreparedPresent("resource finalizer"); err != nil {
			fetchErr = errors.Join(fetchErr, err)
		}
		finalizerChecked = true
	}
	cleanupErr := errors.New("prepared input removal failed")
	cleanupCalls := 0
	metadata := &runMetadata{
		preparedInputs:       true,
		preparedScenarioJS:   []byte("exact scenario"),
		preparedDefaultsYAML: []byte("exact private defaults"),
		preparedCleanup: func(ctx context.Context) error {
			cleanupCalls++
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := os.Remove(preparedPath); err != nil {
				return err
			}
			return cleanupErr
		},
	}
	generateRunSummaryFunc = func(ctx context.Context, _ string, _ io.Writer) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if _, err := os.Stat(preparedPath); !os.IsNotExist(err) {
			return errors.New("summary ran before prepared input cleanup")
		}
		return nil
	}

	done := startAutoResults(
		context.Background(), "http://example", "mt", filepath.Join(workspace, "results"),
		[]string{stepID}, false, nil, "9999", false, 0, preparedPath, metadata,
		io.Discard, io.Discard, "", preSummaryHook)
	outcome := <-done
	if fetchErr != nil {
		t.Fatal(fetchErr)
	}
	if !finalizerChecked {
		t.Fatal("resource finalizer did not inspect prepared input")
	}
	if cleanupCalls != 1 {
		t.Fatalf("prepared cleanup calls = %d, want 1", cleanupCalls)
	}
	if !errors.Is(outcome.Lifecycle.PreparedInputs.Err, cleanupErr) {
		t.Fatalf("prepared cleanup outcome = %+v, want %v", outcome.Lifecycle.PreparedInputs, cleanupErr)
	}
	if len(metadata.preparedScenarioJS) != 0 || len(metadata.preparedDefaultsYAML) != 0 {
		t.Fatal("private prepared bytes retained after cleanup")
	}
	storedBytes, err := os.ReadFile(filepath.Join(metadata.ResultsRoot, constants.ResultsMetadataFileName))
	if err != nil {
		t.Fatal(err)
	}
	var stored runMetadata
	if err = json.Unmarshal(storedBytes, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.Lifecycle == nil || !stored.Lifecycle.PreparedInputs.Completed ||
		!strings.Contains(stored.Lifecycle.PreparedInputs.Error, cleanupErr.Error()) {
		t.Fatalf("stored prepared-input phase = %+v", stored.Lifecycle)
	}
}

func TestStartAutoResultsArtifactTimeoutCannotStarveFinalization(t *testing.T) {
	origRunTracker := newRunTrackerFunc
	origDiscover := discoverStepIDsFunc
	origFleetDiscover := discoverFleetStepIDsFunc
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	origShutdown := requestShutdownAllFunc
	origBudgets := autoResultsPhaseBudgets
	defer func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		discoverFleetStepIDsFunc = origFleetDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
		requestShutdownAllFunc = origShutdown
		autoResultsPhaseBudgets = origBudgets
	}()

	autoResultsPhaseBudgets = postRunBudgets{
		Artifacts:     20 * time.Millisecond,
		CancelSalvage: 20 * time.Millisecond,
		Shutdown:      time.Second,
		Summary:       time.Second,
	}
	stepID := "mt-001-20260802.120000.000-create"
	newRunTrackerFunc = func(string) autoResultsRunTracker { return &fakeRunTracker{} }
	discoverStepIDsFunc = func(context.Context, string, int64) ([]string, error) {
		return []string{stepID}, nil
	}
	discoverFleetStepIDsFunc = func(context.Context, string, int64) ([]string, error) {
		return nil, nil
	}
	fetchStarted := make(chan struct{})
	newResultsFetcherFunc = func(_, outputDir string) autoResultsFetcher {
		return &fakeFetcher{
			output: outputDir,
			onContext: func(ctx context.Context) {
				close(fetchStarted)
				<-ctx.Done()
			},
			err: context.DeadlineExceeded,
		}
	}
	shutdownStarted := make(chan struct{})
	requestShutdownAllFunc = func(
		ctx context.Context, _ []*hostparse.HostInfo, _ string, _ time.Duration, _ bool,
	) error {
		if err := ctx.Err(); err != nil {
			return errors.Join(errors.New("shutdown received exhausted context"), err)
		}
		close(shutdownStarted)
		return nil
	}
	cleanupErr := errors.New("mandatory removal failed")
	metadata := &runMetadata{}
	hookCalled := make(chan struct{})
	preSummaryHook := func(ctx context.Context) {
		if err := ctx.Err(); err != nil {
			metadata.resourceFinalization = &runcontrol.FinalizationOutcome{WaitErr: err}
		} else {
			metadata.resourceFinalization = &runcontrol.FinalizationOutcome{
				Diagnostics: runcontrol.CompletedPhase(nil),
				Removal:     runcontrol.CompletedPhase(cleanupErr),
				Resources:   runcontrol.ResourceDispositionRetained,
			}
		}
		close(hookCalled)
	}
	generateRunSummaryFunc = func(ctx context.Context, _ string, _ io.Writer) error {
		select {
		case <-hookCalled:
		default:
			return errors.New("summary ran before finalization")
		}
		return ctx.Err()
	}

	done := startAutoResults(
		context.Background(), "http://example", "mt", t.TempDir(), []string{stepID}, false,
		nil, "9999", true, 1, "", metadata, io.Discard, io.Discard, "", preSummaryHook)
	outcome := <-done
	select {
	case <-fetchStarted:
	default:
		t.Fatal("artifact fetch did not start")
	}
	select {
	case <-shutdownStarted:
	default:
		t.Fatal("artifact timeout starved the independent shutdown phase")
	}
	if !errors.Is(outcome.ArtifactErr, context.DeadlineExceeded) {
		t.Fatalf("artifact error = %v, want deadline", outcome.ArtifactErr)
	}
	if outcome.ShutdownErr != nil {
		t.Fatalf("shutdown error = %v, want nil", outcome.ShutdownErr)
	}
	if !errors.Is(outcome.Lifecycle.Removal.Err, cleanupErr) {
		t.Fatalf("resource finalization = %+v, want retained cleanup error", outcome.Lifecycle)
	}
	if outcome.Lifecycle.Resources != runcontrol.ResourceDispositionRetained {
		t.Fatalf("resource disposition = %q, want retained", outcome.Lifecycle.Resources)
	}
	if outcome.SummaryErr != nil {
		t.Fatalf("summary error = %v, want nil after finalization", outcome.SummaryErr)
	}
	storedBytes, err := os.ReadFile(filepath.Join(
		metadata.ResultsRoot, constants.ResultsMetadataFileName))
	if err != nil {
		t.Fatalf("read final lifecycle metadata: %v", err)
	}
	var stored runMetadata
	if err := json.Unmarshal(storedBytes, &stored); err != nil {
		t.Fatalf("decode final lifecycle metadata: %v", err)
	}
	if stored.Lifecycle == nil {
		t.Fatal("final metadata omitted lifecycle outcome")
	}
	if !stored.Lifecycle.Artifacts.Started || !stored.Lifecycle.Artifacts.Completed ||
		!strings.Contains(stored.Lifecycle.Artifacts.Error, context.DeadlineExceeded.Error()) {
		t.Fatalf("stored artifact phase = %+v, want completed timeout", stored.Lifecycle.Artifacts)
	}
	if !stored.Lifecycle.Shutdown.Started || !stored.Lifecycle.Shutdown.Completed ||
		stored.Lifecycle.Shutdown.Error != "" {
		t.Fatalf("stored shutdown phase = %+v, want successful independent shutdown", stored.Lifecycle.Shutdown)
	}
	if !stored.Lifecycle.Removal.Started || !stored.Lifecycle.Removal.Completed ||
		!strings.Contains(stored.Lifecycle.Removal.Error, cleanupErr.Error()) {
		t.Fatalf("stored removal phase = %+v, want retained cleanup failure", stored.Lifecycle.Removal)
	}
	if stored.Lifecycle.ResourceDisposition != runcontrol.ResourceDispositionRetained {
		t.Fatalf("stored disposition = %q, want retained", stored.Lifecycle.ResourceDisposition)
	}
	if !stored.Lifecycle.Summary.Started || !stored.Lifecycle.Summary.Completed {
		t.Fatalf("stored summary phase = %+v, want completed", stored.Lifecycle.Summary)
	}
}
