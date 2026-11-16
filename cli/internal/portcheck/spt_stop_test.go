/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"context"
	"net/http"
	"net/http/httptest"
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
