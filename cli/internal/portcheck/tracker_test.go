package portcheck

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func TestRunTracker_TerminalRun(t *testing.T) {
	var calls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		c := atomic.AddInt32(&calls, 1)
		if c < 2 {
			_ = json.NewEncoder(w).Encode(map[string]any{"state": "RUNNING"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"state": "COMPLETED"})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	tr := NewRunTracker(server.URL)
	tr.PollInterval = 10 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := tr.WaitForCompletion(ctx, []string{"any-step"})
	if err != nil {
		t.Fatalf("WaitForCompletion error: %v", err)
	}
	if res.FinalState != "COMPLETED" {
		t.Fatalf("FinalState = %q, want COMPLETED", res.FinalState)
	}
}

func TestRunTrackerPreservesStructuredFailureAndClassifiesStepLifecycle(t *testing.T) {
	const createStep = "mt-001-create"
	const readStep = "mt-002-verify"
	const deleteStep = "mt-003-delete"
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"state": "FAILED", "run_id": 42, "step_id": readStep,
			"category": "input", "message": "completion marker mismatch",
		})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	tracker := NewRunTracker(server.URL)
	tracker.PollInterval = time.Millisecond
	tracker.RequireTerminalState = true
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := tracker.WaitForCompletion(ctx, []string{createStep, readStep, deleteStep})
	if err != nil {
		t.Fatal(err)
	}
	if result.FinalState != "FAILED" || result.RunID != 42 || result.FailureStepID != readStep ||
		result.FailureCategory != "input" || result.FailureMessage != "completion marker mismatch" {
		t.Fatalf("structured terminal result = %+v", result)
	}
	if got := result.Steps[createStep].Lifecycle; got != StepLifecycleStarted {
		t.Fatalf("create lifecycle = %q, want started", got)
	}
	if got := result.Steps[readStep].Lifecycle; got != StepLifecycleFailed {
		t.Fatalf("read lifecycle = %q, want failed", got)
	}
	if got := result.Steps[deleteStep].Lifecycle; got != StepLifecycleNotStarted {
		t.Fatalf("delete lifecycle = %q, want not_started", got)
	}
}

func TestRunTracker_StepCompletion(t *testing.T) {
	// Step id used by test
	stepID := "mt-001-20250101.000000.000-create"
	var count int32

	mux := http.NewServeMux()
	mux.HandleFunc("/run", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"runId": "r2", "status": "RUNNING"})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"state": "RUNNING"})
	})
	mux.HandleFunc("/logs/"+stepID+"/metrics.FileTotal", func(w http.ResponseWriter, r *http.Request) {
		c := atomic.AddInt32(&count, 1)
		if c < 3 {
			// First two attempts: pretend not ready
			w.WriteHeader(http.StatusNotFound)
			return
		}
		// Then return stable headers for HEAD; allow GET too
		if r.Method == http.MethodHead {
			w.Header().Set("Content-Length", "12")
			w.Header().Set("Last-Modified", time.Now().UTC().Format(time.RFC1123))
			w.WriteHeader(http.StatusOK)
			return
		}
		// Fallback body for GET callers
		_, _ = w.Write([]byte("ts,p50\n1,2\n"))
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	tr := NewRunTracker(server.URL)
	tr.PollInterval = 10 * time.Millisecond
	tr.StableConfirmations = 2

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := tr.WaitForCompletion(ctx, []string{stepID})
	if err != nil {
		t.Fatalf("WaitForCompletion error: %v", err)
	}
	s := res.Steps[stepID]
	if !s.Completed {
		t.Fatalf("step %s not marked completed", stepID)
	}
	// Run may still be RUNNING; we only require step completion path exits
	if res.FinalState == "" {
		t.Fatalf("expected non-empty FinalState (e.g., RUNNING), got empty")
	}
}

func TestRunTracker_IdleFallback(t *testing.T) {
	var ts int64 = 1000
	mux := http.NewServeMux()
	mux.HandleFunc("/run", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, r *http.Request) {
		// Emit zero rates and constant timestamp to trigger idle; include terminal:true
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"timestamp":  ts,
			"terminal":   true,
			"operations": map[string]any{"success_rate_last": 0.0},
			"bandwidth":  map[string]any{"bytes_rate_last": 0.0},
		}})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	tr := NewRunTracker(server.URL)
	tr.PollInterval = 10 * time.Millisecond
	tr.IdleGrace = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := tr.WaitForCompletion(ctx, nil)
	if err != nil {
		t.Fatalf("WaitForCompletion error: %v", err)
	}
	if !res.UsedIdle {
		t.Fatalf("expected UsedIdle = true")
	}
	if res.FinalState != "IDLE" {
		t.Fatalf("FinalState = %q, want IDLE", res.FinalState)
	}
}

func TestRunTracker_NonEmptyMetricsAtIdle(t *testing.T) {
	// /status not available; /metrics/json non-empty with terminal:true should trigger idle
	var ts int64 = 1000
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"timestamp":  ts,
			"terminal":   true,
			"operations": map[string]any{"success_rate_last": 0.0},
			"bandwidth":  map[string]any{"bytes_rate_last": 0.0},
		}})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	tr := NewRunTracker(server.URL)
	tr.PollInterval = 10 * time.Millisecond
	tr.IdleGrace = 50 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := tr.WaitForCompletion(ctx, nil)
	if err != nil {
		t.Fatalf("WaitForCompletion error: %v", err)
	}
	if res.FinalState != "IDLE" {
		t.Fatalf("FinalState = %q, want IDLE", res.FinalState)
	}
}

func TestRunTracker_StatusTransitionsToIdle(t *testing.T) {
	// /status returns RUNNING then IDLE; expect terminal IDLE
	var calls int32
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		c := atomic.AddInt32(&calls, 1)
		if c == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"state": "RUNNING"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"state": "IDLE"})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	tr := NewRunTracker(server.URL)
	tr.PollInterval = 10 * time.Millisecond

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	res, err := tr.WaitForCompletion(ctx, nil)
	if err != nil {
		t.Fatalf("WaitForCompletion error: %v", err)
	}
	if res.FinalState != "IDLE" {
		t.Fatalf("FinalState = %q, want IDLE", res.FinalState)
	}
}

func TestRunTrackerUnavailableReturnsWithinLivenessDeadline(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	baseURL := server.URL
	server.Close()

	tracker := NewRunTracker(baseURL)
	tracker.PollInterval = 2 * time.Millisecond
	tracker.UnavailableTimeout = 15 * time.Millisecond

	started := time.Now()
	result, err := tracker.WaitForCompletion(context.Background(), nil)
	elapsed := time.Since(started)
	if err == nil || !strings.Contains(err.Error(), "completion tracker unavailable") {
		t.Fatalf("WaitForCompletion error = %v, want tracker-unavailable error", err)
	}
	if result == nil || result.Steps == nil {
		t.Fatalf("partial result = %#v, want preserved evidence", result)
	}
	if elapsed > time.Second {
		t.Fatalf("tracker returned after %s, want bounded return", elapsed)
	}
}

func TestRunTrackerExpectedStepErrorResponsesDoNotCountAsAvailability(t *testing.T) {
	for _, status := range []int{http.StatusNotFound, http.StatusServiceUnavailable} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, http.StatusText(status), status)
			}))
			defer server.Close()

			tracker := NewRunTracker(server.URL)
			tracker.PollInterval = 2 * time.Millisecond
			tracker.UnavailableTimeout = 15 * time.Millisecond
			tracker.StartupTimeout = time.Second
			result, err := tracker.WaitForCompletion(context.Background(), []string{"expected-step"})
			if err == nil || !strings.Contains(err.Error(), "completion tracker unavailable") {
				t.Fatalf("WaitForCompletion error = %v, want tracker-unavailable error", err)
			}
			if result == nil || result.Steps["expected-step"].Lifecycle != StepLifecyclePlanned {
				t.Fatalf("partial result = %#v, want planned expected step", result)
			}
		})
	}
}

func TestRunTrackerIdleAndEmptyMetricsHitStartupDeadline(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"state": "IDLE"})
	})
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]any{})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	tracker := NewRunTracker(server.URL)
	tracker.PollInterval = 2 * time.Millisecond
	tracker.StartupTimeout = 15 * time.Millisecond
	tracker.UnavailableTimeout = time.Second
	result, err := tracker.WaitForCompletion(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "no run activity") {
		t.Fatalf("WaitForCompletion error = %v, want startup deadline error", err)
	}
	if result == nil || result.FinalState != "IDLE" || result.UsedIdle {
		t.Fatalf("partial result = %#v, want nonterminal pre-activity IDLE", result)
	}
}

func TestRunTrackerAPIDisappearsAfterActivity(t *testing.T) {
	var statusCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/status" && atomic.AddInt32(&statusCalls, 1) == 1 {
			_ = json.NewEncoder(w).Encode(map[string]any{"state": "RUNNING"})
			return
		}
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	tracker := NewRunTracker(server.URL)
	tracker.PollInterval = 2 * time.Millisecond
	tracker.UnavailableTimeout = 15 * time.Millisecond
	result, err := tracker.WaitForCompletion(context.Background(), nil)
	if err == nil || !strings.Contains(err.Error(), "completion tracker unavailable") {
		t.Fatalf("WaitForCompletion error = %v, want tracker-unavailable error", err)
	}
	if result == nil || result.FinalState != "RUNNING" {
		t.Fatalf("partial result = %#v, want preserved RUNNING state", result)
	}
	if atomic.LoadInt32(&statusCalls) < 2 {
		t.Fatalf("status calls = %d, want API disappearance after activity", statusCalls)
	}
}

func TestRunTrackerDoesNotCapHealthyLongRunningWork(t *testing.T) {
	var statusCalls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		if atomic.AddInt32(&statusCalls, 1) < 8 {
			_ = json.NewEncoder(w).Encode(map[string]any{"state": "RUNNING"})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"state": "COMPLETED"})
	}))
	defer server.Close()

	tracker := NewRunTracker(server.URL)
	tracker.PollInterval = 2 * time.Millisecond
	tracker.UnavailableTimeout = time.Nanosecond
	result, err := tracker.WaitForCompletion(context.Background(), nil)
	if err != nil {
		t.Fatalf("WaitForCompletion error: %v", err)
	}
	if result.FinalState != "COMPLETED" {
		t.Fatalf("FinalState = %q, want COMPLETED", result.FinalState)
	}
	if atomic.LoadInt32(&statusCalls) < 8 {
		t.Fatalf("status calls = %d, want repeated healthy progress", statusCalls)
	}
}

func TestRunTrackerMatchingStartingStateCountsAsCurrentRunActivity(t *testing.T) {
	const expectedRunID = int64(17)
	var statusCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/status" {
			http.NotFound(w, r)
			return
		}
		if statusCalls.Add(1) < 10 {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"state": "STARTING", "run_id": expectedRunID, "step_id": "current-step",
			})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"state": "COMPLETED", "run_id": expectedRunID, "step_id": "current-step",
		})
	}))
	defer server.Close()

	tracker := NewRunTracker(server.URL)
	tracker.ExpectedRunID = expectedRunID
	tracker.PollInterval = 2 * time.Millisecond
	tracker.StartupTimeout = 5 * time.Millisecond
	tracker.RequireTerminalState = true
	result, err := tracker.WaitForCompletion(context.Background(), []string{"current-step"})
	if err != nil {
		t.Fatalf("matching STARTING run hit startup timeout: %v", err)
	}
	if result.RunID != expectedRunID || result.FinalState != "COMPLETED" {
		t.Fatalf("terminal result = %+v", result)
	}
}

func TestRunTrackerIgnoresRetainedPriorRunUntilExpectedRunTransitions(t *testing.T) {
	const expectedRunID = int64(17)
	var statusCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		switch call := statusCalls.Add(1); {
		case call < 4:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"state": "IDLE", "run_id": 16, "step_id": "prior-step",
			})
		case call < 7:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"state": "STARTING", "run_id": expectedRunID, "step_id": "current-step",
			})
		default:
			_ = json.NewEncoder(w).Encode(map[string]any{
				"state": "COMPLETED", "run_id": expectedRunID, "step_id": "current-step",
			})
		}
	})
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"run_id": "16", "step_id": "prior-step", "timestamp": 1000, "terminal": true,
			"operations": map[string]any{"success_rate_last": 0},
			"bandwidth":  map[string]any{"bytes_rate_last": 0},
		}})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	tracker := NewRunTracker(server.URL)
	tracker.ExpectedRunID = expectedRunID
	tracker.PollInterval = 2 * time.Millisecond
	tracker.IdleGrace = time.Millisecond
	tracker.StartupTimeout = time.Second
	tracker.RequireTerminalState = true
	result, err := tracker.WaitForCompletion(context.Background(), []string{"current-step"})
	if err != nil {
		t.Fatal(err)
	}
	if result.UsedIdle || result.RunID != expectedRunID || result.FinalState != "COMPLETED" {
		t.Fatalf("prior-run evidence terminated tracking: %+v", result)
	}
}

func TestRunTrackerTerminalRequiredRejectsStableFileWithoutMatchingStatus(t *testing.T) {
	const expectedRunID = int64(17)
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"state": "IDLE", "run_id": 16, "step_id": "retained-step",
		})
	})
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]any{})
	})
	mux.HandleFunc("/logs/current-step/metrics.FileTotal", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodHead {
			http.Error(w, "HEAD required", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Length", "64")
		w.Header().Set("Last-Modified", "Sat, 01 Aug 2026 12:00:00 GMT")
		w.WriteHeader(http.StatusOK)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	tracker := NewRunTracker(server.URL)
	tracker.ExpectedRunID = expectedRunID
	tracker.PollInterval = 2 * time.Millisecond
	tracker.UnavailableTimeout = 15 * time.Millisecond
	tracker.StartupTimeout = time.Second
	tracker.StableConfirmations = 2
	tracker.RequireTerminalState = true
	result, err := tracker.WaitForCompletion(context.Background(), []string{"current-step"})
	if err == nil || !strings.Contains(err.Error(), "matching status for expected run 17 unavailable") {
		t.Fatalf("WaitForCompletion() = (%+v, %v), want bounded matching-status error", result, err)
	}
	if result.FinalState != "" || result.RunID != 0 {
		t.Fatalf("retained status leaked into current result: %+v", result)
	}
	if !result.Steps["current-step"].Completed {
		t.Fatalf("stable file lifecycle evidence was lost: %+v", result.Steps["current-step"])
	}
}

func TestRunTrackerTerminalRequiredIgnoresMatchingIdleMetricsUntilStatusTerminal(t *testing.T) {
	const expectedRunID = int64(17)
	var statusCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
		state := "RUNNING"
		if statusCalls.Add(1) >= 8 {
			state = "COMPLETED"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"state": state, "run_id": expectedRunID, "step_id": "current-step",
		})
	})
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode([]map[string]any{{
			"run_id": expectedRunID, "step_id": "current-step", "timestamp": 1000, "terminal": true,
			"operations": map[string]any{"success_rate_last": 0},
			"bandwidth":  map[string]any{"bytes_rate_last": 0},
		}})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	tracker := NewRunTracker(server.URL)
	tracker.ExpectedRunID = expectedRunID
	tracker.PollInterval = 2 * time.Millisecond
	tracker.IdleGrace = time.Millisecond
	tracker.UnavailableTimeout = time.Second
	tracker.StartupTimeout = time.Second
	tracker.RequireTerminalState = true
	result, err := tracker.WaitForCompletion(context.Background(), []string{"current-step"})
	if err != nil {
		t.Fatal(err)
	}
	if result.UsedIdle || result.FinalState != constants.StateCompleted || result.RunID != expectedRunID {
		t.Fatalf("idle metrics substituted for structured terminal status: %+v", result)
	}
}

func TestRunTrackerTerminalRequiredBoundsIncompatibleMatchingStatus(t *testing.T) {
	const expectedRunID = int64(818)
	for _, state := range []string{"UNKNOWN_NEW_STATE", constants.StateIdle} {
		t.Run(state, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/status", func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode(map[string]any{
					"state": state, "run_id": expectedRunID, "step_id": "current-step",
				})
			})
			mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, _ *http.Request) {
				_ = json.NewEncoder(w).Encode([]map[string]any{{
					"run_id": expectedRunID, "step_id": "current-step", "timestamp": 1000,
					"operations": map[string]any{"success_rate_last": 1},
					"bandwidth":  map[string]any{"bytes_rate_last": 1},
				}})
			})
			server := httptest.NewServer(mux)
			defer server.Close()

			tracker := NewRunTracker(server.URL)
			tracker.ExpectedRunID = expectedRunID
			tracker.PollInterval = 2 * time.Millisecond
			tracker.UnavailableTimeout = 15 * time.Millisecond
			tracker.StartupTimeout = time.Second
			tracker.RequireTerminalState = true
			result, err := tracker.WaitForCompletion(context.Background(), []string{"current-step"})
			if err == nil || !strings.Contains(err.Error(), "matching status for expected run 818 unavailable") ||
				!strings.Contains(err.Error(), state) {
				t.Fatalf("WaitForCompletion() = (%+v, %v), want bounded incompatible-state error", result, err)
			}
		})
	}
}

func TestRunTrackerCancellationPreservesPartialLifecycle(t *testing.T) {
	tracker := NewRunTracker("http://127.0.0.1:1")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result, err := tracker.WaitForCompletion(ctx, []string{"planned-step"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitForCompletion error = %v, want context.Canceled", err)
	}
	if result == nil {
		t.Fatal("partial result is nil")
	}
	if got := result.Steps["planned-step"].Lifecycle; got != StepLifecyclePlanned {
		t.Fatalf("planned-step lifecycle = %q, want %q", got, StepLifecyclePlanned)
	}
}
