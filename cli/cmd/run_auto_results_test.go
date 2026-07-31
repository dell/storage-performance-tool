package cmd

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/tui"
)

type fakeRunTracker struct {
	mu          sync.Mutex
	called      bool
	stepIDs     []string
	debugCalled bool
	debugValue  bool
	result      *portcheck.RunResult
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

type fakeFetcher struct {
	mu       sync.Mutex
	stepIDs  []string
	baseURL  string
	output   string
	manifest *results.Manifest
	err      error
	onFetch  func(output string, stepIDs []string) error
}

func (f *fakeFetcher) FetchArtifactsForSteps(ctx context.Context, stepIDs []string) (*results.Manifest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
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
	discoverFleetStepIDsFunc = func(baseURL string) ([]string, error) { return nil, nil }

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

	// The mock simulates the lingering prior container: early calls return stale IDs, later calls
	// return both sets (as the prior container's /metrics/json keeps accumulating).
	var discoverCallCount int
	var discoverMu sync.Mutex
	discoverStepIDsFunc = func(baseURL string) ([]string, error) {
		discoverMu.Lock()
		discoverCallCount++
		n := discoverCallCount
		discoverMu.Unlock()
		if n <= 2 {
			// First two polls hit the still-lingering prior container.
			return staleIDs, nil
		}
		// Later polls see both runs while the current container is active.
		return append(append([]string(nil), staleIDs...), currentIDs...), nil
	}

	fetcher := &fakeFetcher{}
	newResultsFetcherFunc = func(baseURL, outputDir string) autoResultsFetcher {
		fetcher.baseURL = baseURL
		fetcher.output = outputDir
		return fetcher
	}

	generateRunSummaryFunc = func(ctx context.Context, runDir string, out io.Writer) error { return nil }

	tmpDir := t.TempDir()
	done := startAutoResults("http://example", "mt", tmpDir, currentIDs, false, nil, "", false, 0, "", nil, io.Discard, io.Discard, "", nil)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("startAutoResults did not complete in time (possible hang due to stale step IDs)")
	}

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

// TestStartAutoResults_DiscoveredIDsFilteredToExpectedSet verifies that when expectedStepIDs is
// provided, the fetcher receives only those IDs — even if the background discovery poller returns
// additional IDs (e.g. from a different run) alongside the expected ones. Discovered IDs that are
// also in the expected set are kept; foreign IDs are excluded.
func TestStartAutoResults_DiscoveredIDsFilteredToExpectedSet(t *testing.T) {
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
	discoverFleetStepIDsFunc = func(baseURL string) ([]string, error) { return nil, nil }

	// Discovery returns the expected ID mixed in with a foreign one from another run.
	discoverCalls := 0
	discoverStepIDsFunc = func(baseURL string) ([]string, error) {
		discoverCalls++
		return []string{"foreign-step-other-run", "expected-create"}, nil
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
	done := startAutoResults("http://example", "mt", tmpDir, []string{"expected-create"}, false, nil, "", false, 0, "", nil, io.Discard, io.Discard, "", nil)

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
	// Only the expected ID should reach the fetcher; the foreign discovered ID must be excluded.
	if len(fetcher.stepIDs) != 1 || fetcher.stepIDs[0] != "expected-create" {
		t.Fatalf("unexpected stepIDs %v, want [expected-create]", fetcher.stepIDs)
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
	discoverStepIDsFunc = func(baseURL string) ([]string, error) { return nil, nil }

	// Fleet endpoint returns the engine's actual step IDs
	discoverFleetStepIDsFunc = func(baseURL string) ([]string, error) {
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
	done := startAutoResults("http://example", "mt", tmpDir, expectedIDs, false, nil, "", false, 0, "", nil, io.Discard, io.Discard, "", nil)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("startAutoResults did not complete in time")
	}

	fetcher.mu.Lock()
	defer fetcher.mu.Unlock()

	// Fleet IDs should be present in the fetched step IDs
	fleetSet := make(map[string]bool, len(fleetIDs))
	for _, id := range fleetIDs {
		fleetSet[id] = true
	}
	foundFleet := 0
	for _, id := range fetcher.stepIDs {
		if fleetSet[id] {
			foundFleet++
		}
	}
	if foundFleet != len(fleetIDs) {
		t.Fatalf("expected all %d fleet IDs in fetcher.stepIDs, found %d; got %v", len(fleetIDs), foundFleet, fetcher.stepIDs)
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
	discoverStepIDsFunc = func(baseURL string) ([]string, error) {
		return expectedIDs, nil
	}

	// Fleet endpoint unavailable (404)
	discoverFleetStepIDsFunc = func(baseURL string) ([]string, error) { return nil, nil }

	fetcher := &fakeFetcher{}
	newResultsFetcherFunc = func(baseURL, outputDir string) autoResultsFetcher {
		fetcher.baseURL = baseURL
		fetcher.output = outputDir
		return fetcher
	}
	generateRunSummaryFunc = func(ctx context.Context, runDir string, out io.Writer) error { return nil }

	tmpDir := t.TempDir()
	done := startAutoResults("http://example", "mt", tmpDir, expectedIDs, false, nil, "", false, 0, "", nil, io.Discard, io.Discard, "", nil)

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
	discoverStepIDsFunc = func(baseURL string) ([]string, error) {
		return []string{stepID}, nil
	}
	discoverFleetStepIDsFunc = func(baseURL string) ([]string, error) { return nil, nil }

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
	done := startAutoResults("http://example", "mt", tmpDir, []string{stepID}, false, nil, "", false, 0, "", nil, io.Discard, io.Discard, traceFile, nil)

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
	discoverStepIDsFunc = func(baseURL string) ([]string, error) { return []string{stepID}, nil }
	discoverFleetStepIDsFunc = func(baseURL string) ([]string, error) { return nil, nil }
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
	preSummaryHook := func() {
		atomic.AddInt32(&hookCalled, 1)
		_, _ = events.Write([]byte("PRE-SUMMARY HOOK RAN\n"))
	}
	done := startAutoResults(
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
	discoverStepIDsFunc = func(string) ([]string, error) { return []string{stepID}, nil }
	discoverFleetStepIDsFunc = func(string) ([]string, error) { return nil, nil }
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

// TestStartAutoResults_SkipsPreSummaryHookWhenShutdownDisabled guards the
// other half of the contract: the hook drives diagnostics collection and
// full container/staging cleanup, which must only happen when the run
// actually asked for delegated shutdown (shutdownOn/--shutdown-on-complete).
// Running it unconditionally would tear down containers out from under a
// user who explicitly asked to keep them running after completion.
func TestStartAutoResults_SkipsPreSummaryHookWhenShutdownDisabled(t *testing.T) {
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
	discoverStepIDsFunc = func(baseURL string) ([]string, error) { return []string{stepID}, nil }
	discoverFleetStepIDsFunc = func(baseURL string) ([]string, error) { return nil, nil }
	newResultsFetcherFunc = func(baseURL, outputDir string) autoResultsFetcher {
		return &fakeFetcher{output: outputDir}
	}
	generateRunSummaryFunc = func(ctx context.Context, runDir string, out io.Writer) error { return nil }

	var hookCalled int32
	preSummaryHook := func() { atomic.AddInt32(&hookCalled, 1) }

	// shutdownOn = false
	done := startAutoResults("http://example", "mt", t.TempDir(), []string{stepID}, false, nil, "", false, 0, "", nil, io.Discard, io.Discard, "", preSummaryHook)

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("startAutoResults did not complete in time")
	}

	if atomic.LoadInt32(&hookCalled) != 0 {
		t.Fatalf("preSummaryHook called %d times with shutdown disabled, want 0", hookCalled)
	}
}
