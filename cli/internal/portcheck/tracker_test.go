package portcheck

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
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
