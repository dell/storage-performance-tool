package cmd

import (
	"context"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results"
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
}

func (f *fakeFetcher) FetchArtifactsForSteps(ctx context.Context, stepIDs []string) (*results.Manifest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stepIDs = append([]string(nil), stepIDs...)
	if f.manifest != nil || f.err != nil {
		return f.manifest, f.err
	}
	return &results.Manifest{}, nil
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
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	defer func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
		newResultsFetcherFunc = origFetcher
		generateRunSummaryFunc = origSummary
	}()

	tracker := &fakeRunTracker{}
	newRunTrackerFunc = func(baseURL string) autoResultsRunTracker { return tracker }

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
	done := startAutoResults("http://example", "mt", tmpDir, currentIDs, false, nil, "", false, 0, "", nil, io.Discard, io.Discard)

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
	origFetcher := newResultsFetcherFunc
	origSummary := generateRunSummaryFunc
	defer func() {
		newRunTrackerFunc = origRunTracker
		discoverStepIDsFunc = origDiscover
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
	done := startAutoResults("http://example", "mt", tmpDir, []string{"expected-create"}, false, nil, "", false, 0, "", nil, io.Discard, io.Discard)

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
