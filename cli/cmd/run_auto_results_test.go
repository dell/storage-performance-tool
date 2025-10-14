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

func TestStartAutoResultsMergesDiscoveredSteps(t *testing.T) {
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

	discoverCalls := 0
	discoverStepIDsFunc = func(baseURL string) ([]string, error) {
		discoverCalls++
		return []string{"step-002-delete", "step-001-create"}, nil
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
	expectedOrder := []string{"expected-create", "step-002-delete", "step-001-create"}
	if len(fetcher.stepIDs) != len(expectedOrder) {
		t.Fatalf("unexpected stepIDs %v", fetcher.stepIDs)
	}
	for i, want := range expectedOrder {
		if fetcher.stepIDs[i] != want {
			t.Fatalf("step order mismatch at %d: got %q want %q", i, fetcher.stepIDs[i], want)
		}
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
