/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"
)

// StopRun stops only the owned run: it sends DELETE /run with the run ID in If-Match, which the
// engine requires, and leaves a 204 (no active run) or 404 (another run) to terminal
// reconciliation.
func TestStopRunSendsTheOwnedRunID(t *testing.T) {
	for _, test := range []struct {
		status      int
		body        string
		wantStopped bool
		wantErr     string
	}{
		{status: http.StatusOK, wantStopped: true},
		{status: http.StatusNoContent},
		{status: http.StatusNotFound},
		{status: http.StatusBadRequest, body: "missing If-Match\n", wantErr: "stop status 400: missing If-Match"},
		{status: http.StatusInternalServerError, wantErr: "stop status 500"},
	} {
		t.Run(http.StatusText(test.status), func(t *testing.T) {
			var method, path, ifMatch string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				method, path, ifMatch = r.Method, r.URL.Path, r.Header.Get("If-Match")
				w.WriteHeader(test.status)
				_, _ = w.Write([]byte(test.body))
			}))
			defer server.Close()

			stopped, err := NewSptAPIClient(server.URL, 5*time.Second).StopRun(context.Background(), 41)
			if method != http.MethodDelete || path != "/run" || ifMatch != "41" {
				t.Fatalf("request = %s %s If-Match %q, want DELETE /run If-Match \"41\"", method, path, ifMatch)
			}
			if test.wantErr == "" {
				if err != nil || stopped != test.wantStopped {
					t.Fatalf("StopRun() = %t, %v, want %t, nil", stopped, err, test.wantStopped)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("StopRun() error = %v, want %q", err, test.wantErr)
			}
		})
	}
}

// Without a known run ID StopRun sends nothing: an unconditional stop could end another run.
func TestStopRunWithoutRunIDSendsNothing(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests++ }))
	defer server.Close()
	if _, err := NewSptAPIClient(server.URL, 5*time.Second).StopRun(context.Background(), 0); err == nil {
		t.Fatal("StopRun(0) returned nil")
	}
	if requests != 0 {
		t.Fatalf("StopRun(0) sent %d requests", requests)
	}
}

// StopActiveRun takes the ID from HEAD /run, reads the run's start time, and
// sends exactly that ID back.
func TestStopActiveRunStopsTheReportedRun(t *testing.T) {
	engine := newFakeEngine(t, 1763456789012)
	stopped, err := NewSptAPIClient(engine.server.URL, 5*time.Second).StopActiveRun(context.Background(), RunExecution{ID: 1763456789012})
	runID := stopped.ID
	if err != nil || runID != 1763456789012 {
		t.Fatalf("StopActiveRun() = %d, %v, want 1763456789012, nil", runID, err)
	}
	want := []engineRequest{
		{http.MethodHead, "/run", ""},
		{http.MethodGet, "/status", ""},
		{http.MethodDelete, "/run", "1763456789012"},
	}
	if got := engine.received(); !slices.Equal(got, want) {
		t.Fatalf("requests = %+v, want %+v", got, want)
	}
}

// The remote API accepts any non-zero signed run ID.
func TestStopActiveRunAcceptsANegativeRunID(t *testing.T) {
	engine := newFakeEngine(t, -7)
	stopped, err := NewSptAPIClient(engine.server.URL, 5*time.Second).StopActiveRun(context.Background(), RunExecution{ID: -7})
	runID := stopped.ID
	if err != nil || runID != -7 {
		t.Fatalf("StopActiveRun() = %d, %v, want -7, nil", runID, err)
	}
	if !slices.Contains(engine.received(), engineRequest{http.MethodDelete, "/run", "-7"}) {
		t.Errorf("requests = %+v, want DELETE /run If-Match -7", engine.received())
	}
}

func TestStopActiveRunWithoutARunSendsNoStop(t *testing.T) {
	engine := newFakeEngine(t, 0)
	stopped, err := NewSptAPIClient(engine.server.URL, 5*time.Second).StopActiveRun(context.Background(), RunExecution{})
	runID := stopped.ID
	if err != nil || runID != 0 {
		t.Fatalf("StopActiveRun() = %d, %v, want 0, nil", runID, err)
	}
	assertReadOnly(t, engine.received())
}

// The run can change between HEAD and DELETE: only its end counts as success.
func TestStopActiveRunWhenTheRunChanges(t *testing.T) {
	tests := []struct {
		name     string
		onDelete func(*fakeEngine, http.ResponseWriter)
		wantErr  string
	}{
		{
			name: "204: the run ended",
			onDelete: func(e *fakeEngine, w http.ResponseWriter) {
				e.runID = 0
				w.WriteHeader(http.StatusNoContent)
			},
		},
		{
			name: "404: the run ended",
			onDelete: func(e *fakeEngine, w http.ResponseWriter) {
				e.runID = 0
				w.WriteHeader(http.StatusNotFound)
			},
		},
		{
			name: "404: another run took over",
			onDelete: func(e *fakeEngine, w http.ResponseWriter) {
				e.runID = 42
				w.WriteHeader(http.StatusNotFound)
			},
			wantErr: "run 41 was replaced by run 42",
		},
		{
			// The engine answers 200 to a stop of its active run, so this is a
			// later run that reuses the ID.
			name:     "404: the same run ID is still active",
			onDelete: func(_ *fakeEngine, w http.ResponseWriter) { w.WriteHeader(http.StatusNotFound) },
			wantErr:  "it is a later run that reuses the ID",
		},
		{
			name:     "500",
			onDelete: func(_ *fakeEngine, w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) },
			wantErr:  "stop status 500",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := newFakeEngine(t, 41)
			engine.onDelete = tt.onDelete
			stopped, err := NewSptAPIClient(engine.server.URL, 5*time.Second).StopActiveRun(context.Background(), RunExecution{ID: 41})
			runID := stopped.ID
			if runID != 0 {
				t.Errorf("StopActiveRun() run = %d, want 0", runID)
			}
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("StopActiveRun() error = %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("StopActiveRun() error = %v, want %q", err, tt.wantErr)
			}
			deletes := 0
			for _, request := range engine.received() {
				if request.method == http.MethodDelete {
					deletes++
				}
			}
			if deletes != 1 {
				t.Errorf("DELETE requests = %d, want 1", deletes)
			}
		})
	}
}
