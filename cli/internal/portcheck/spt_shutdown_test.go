package portcheck

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestSptAPIClient_ShutdownAndLinger_OK(t *testing.T) {
	// Simple server that accepts /shutdown and reports terminal /status
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/shutdown":
			w.WriteHeader(http.StatusAccepted)
		case "/status":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"state":"COMPLETED"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := NewSptAPIClient(srv.URL, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}

	if err := c.WaitForLinger(ctx, 300*time.Millisecond); err != nil {
		t.Fatalf("linger wait failed: %v", err)
	}
}

func TestSptAPIClient_Shutdown_ErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/shutdown" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("boom"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewSptAPIClient(srv.URL, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := c.Shutdown(ctx); err == nil {
		t.Fatal("expected shutdown error, got nil")
	}
}

func TestSptAPIClient_Linger_NonTerminalFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/shutdown":
			w.WriteHeader(http.StatusOK)
		case "/status":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"state":"RUNNING"}`))
		}
	}))
	defer srv.Close()

	c := NewSptAPIClient(srv.URL, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_ = c.Shutdown(ctx)
	if err := c.WaitForLinger(ctx, 200*time.Millisecond); err == nil {
		t.Fatal("expected linger error for non-terminal state, got nil")
	}
}

type advancingLingerClock struct {
	now time.Time
}

func (clock *advancingLingerClock) Now() time.Time { return clock.now }
func (clock *advancingLingerClock) Sleep(duration time.Duration) {
	clock.now = clock.now.Add(duration)
}
func (clock *advancingLingerClock) After(duration time.Duration) <-chan time.Time {
	clock.now = clock.now.Add(duration)
	fired := make(chan time.Time, 1)
	fired <- clock.now
	return fired
}

func TestSptAPIClientLingerAllowsOwnedRunningToStoppedTransition(t *testing.T) {
	var statusCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/status" {
			http.NotFound(writer, request)
			return
		}
		state := "RUNNING"
		if statusCalls.Add(1) >= 2 {
			state = "STOPPED"
		}
		_, _ = writer.Write([]byte(fmt.Sprintf(`{"state":%q,"run_id":77}`, state)))
	}))
	defer server.Close()

	client := NewSptAPIClientWithClock(server.URL, time.Second, &advancingLingerClock{})
	client.SetExpectedRunID(77)
	if err := client.WaitForLinger(context.Background(), 600*time.Millisecond); err != nil {
		t.Fatalf("RUNNING to STOPPED linger failed: %v", err)
	}
}

func TestSptAPIClientWaitForTerminalAllowsOwnedRunningToStoppedTransition(t *testing.T) {
	var statusCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/status" {
			http.NotFound(writer, request)
			return
		}
		state := "RUNNING"
		if statusCalls.Add(1) >= 2 {
			state = "STOPPED"
		}
		_, _ = writer.Write([]byte(fmt.Sprintf(`{"state":%q,"run_id":77}`, state)))
	}))
	defer server.Close()

	client := NewSptAPIClientWithClock(server.URL, time.Second, &advancingLingerClock{})
	client.SetExpectedRunID(77)
	if err := client.WaitForTerminal(context.Background()); err != nil {
		t.Fatalf("WaitForTerminal() error = %v", err)
	}
	if statusCalls.Load() != 2 {
		t.Fatalf("status calls = %d, want 2", statusCalls.Load())
	}
}

func TestSptAPIClientWaitForTerminalRejectsDifferentRun(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`{"state":"STOPPED","run_id":78}`))
	}))
	defer server.Close()

	client := NewSptAPIClientWithClock(server.URL, time.Second, &advancingLingerClock{})
	client.SetExpectedRunID(77)
	err := client.WaitForTerminal(context.Background())
	if err == nil || !strings.Contains(err.Error(), "does not match the owned run") {
		t.Fatalf("WaitForTerminal() error = %v, want owned-run attribution failure", err)
	}
}

func TestSptAPIClientLingerRejectsWrongRunAndTerminalRegression(t *testing.T) {
	tests := []struct {
		name     string
		statuses []string
		runID    int64
		want     string
	}{
		{name: "wrong run", statuses: []string{"STOPPED"}, runID: 78, want: "does not match the owned run"},
		{name: "terminal regression", statuses: []string{"STOPPED", "RUNNING"}, runID: 77, want: "after terminal status"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var statusCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				index := int(statusCalls.Add(1)) - 1
				if index >= len(test.statuses) {
					index = len(test.statuses) - 1
				}
				_, _ = writer.Write([]byte(fmt.Sprintf(
					`{"state":%q,"run_id":%d}`, test.statuses[index], test.runID)))
			}))
			defer server.Close()

			client := NewSptAPIClientWithClock(server.URL, time.Second, &advancingLingerClock{})
			client.SetExpectedRunID(77)
			err := client.WaitForLinger(context.Background(), 400*time.Millisecond)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("WaitForLinger() error = %v, want %q", err, test.want)
			}
		})
	}
}
