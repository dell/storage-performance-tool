/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMonitorStatusRejectsTerminalStateForDifferentOwnedRun(t *testing.T) {
	for _, state := range []string{"COMPLETED", "FAILED", "STOPPED"} {
		t.Run(state, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(
				writer http.ResponseWriter, request *http.Request,
			) {
				if request.URL.Path != "/status" {
					writer.WriteHeader(http.StatusNotFound)
					return
				}
				writer.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(writer, `{"state":%q,"run_id":78}`, state)
			}))
			defer server.Close()

			orchestrator := NewTestOrchestrator(nil, "", "")
			orchestrator.statusInterval = 5 * time.Millisecond
			orchestrator.apiClient = NewSptAPIClient(server.URL)
			orchestrator.apiClient.SetRunID("77")
			updates := make(chan *TestStatus, 1)
			orchestrator.SetCallbacks(func(status *TestStatus) {
				select {
				case updates <- status:
				default:
				}
			}, nil, nil, nil)

			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan struct{})
			go func() {
				defer close(done)
				orchestrator.monitorStatus(ctx)
			}()
			select {
			case status := <-updates:
				if status.State != state || status.RunID != "78" {
					t.Fatalf("display status = %+v", status)
				}
			case <-time.After(time.Second):
				t.Fatal("status callback did not receive unrelated terminal state")
			}
			select {
			case <-orchestrator.CompletionCh():
				t.Fatalf("unrelated %s run completed the owned session", state)
			case <-time.After(25 * time.Millisecond):
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("status monitor did not stop after cancellation")
			}
		})
	}
}

func TestMonitorStatusCompletesOnlyAfterMatchingOwnedRun(t *testing.T) {
	for _, state := range []string{"COMPLETED", "FAILED", "STOPPED"} {
		t.Run(state, func(t *testing.T) {
			var matching atomic.Bool
			server := httptest.NewServer(http.HandlerFunc(func(
				writer http.ResponseWriter, request *http.Request,
			) {
				if request.URL.Path != "/status" {
					writer.WriteHeader(http.StatusNotFound)
					return
				}
				runID := 78
				if matching.Load() {
					runID = 77
				}
				writer.Header().Set("Content-Type", "application/json")
				_, _ = fmt.Fprintf(writer, `{"state":%q,"run_id":%d}`, state, runID)
			}))
			defer server.Close()

			orchestrator := NewTestOrchestrator(nil, "", "")
			orchestrator.statusInterval = 5 * time.Millisecond
			orchestrator.apiClient = NewSptAPIClient(server.URL)
			orchestrator.apiClient.SetRunID("77")
			updates := make(chan *TestStatus, 16)
			outputs := make(chan string, 4)
			orchestrator.SetCallbacks(
				func(status *TestStatus) { updates <- status },
				nil,
				func(output string) { outputs <- output },
				nil,
			)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go orchestrator.monitorStatus(ctx)
			select {
			case status := <-updates:
				if status.RunID != "78" {
					t.Fatalf("first observed run ID = %q, want 78", status.RunID)
				}
			case <-time.After(time.Second):
				t.Fatal("status monitor did not observe unrelated run")
			}
			select {
			case <-orchestrator.CompletionCh():
				t.Fatal("unrelated run completed the session")
			case <-time.After(25 * time.Millisecond):
			}

			matching.Store(true)
			select {
			case <-orchestrator.CompletionCh():
			case <-time.After(time.Second):
				t.Fatalf("matching %s status did not complete the session", state)
			}
			select {
			case output := <-outputs:
				if output != "Test "+state {
					t.Fatalf("completion output = %q", output)
				}
			case <-time.After(time.Second):
				t.Fatal("matching completion produced no output")
			}
			select {
			case extra := <-outputs:
				t.Fatalf("completion was reported more than once: %q", extra)
			case <-time.After(25 * time.Millisecond):
			}
		})
	}
}

func TestMonitorStatusMissingRunIDUsesExplicitCompatibilityPolicy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter, request *http.Request,
	) {
		if request.URL.Path != "/status" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"state":"COMPLETED"}`))
	}))
	defer server.Close()

	t.Run("configured identity requires attribution", func(t *testing.T) {
		orchestrator := NewTestOrchestrator(nil, "", "")
		orchestrator.statusInterval = 5 * time.Millisecond
		orchestrator.apiClient = NewSptAPIClient(server.URL)
		orchestrator.apiClient.SetRunID("77")
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			orchestrator.monitorStatus(ctx)
		}()
		select {
		case <-orchestrator.CompletionCh():
			t.Fatal("unattributed terminal status completed configured run 77")
		case <-time.After(30 * time.Millisecond):
		}
		cancel()
		<-done
	})

	t.Run("legacy unconfigured caller permits missing identity", func(t *testing.T) {
		orchestrator := NewTestOrchestrator(nil, "", "")
		orchestrator.statusInterval = 5 * time.Millisecond
		orchestrator.apiClient = NewSptAPIClient(server.URL)
		orchestrator.apiClient.setCompatibilityRunID("legacy-run")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go orchestrator.monitorStatus(ctx)
		select {
		case <-orchestrator.CompletionCh():
		case <-time.After(time.Second):
			t.Fatal("legacy missing-ID completion policy was not preserved")
		}
	})
}

func TestMonitorStatusMissingRunIDStoppedUsesExplicitCompatibilityPolicy(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter, request *http.Request,
	) {
		if request.URL.Path != "/status" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"state":"STOPPED"}`))
	}))
	defer server.Close()

	t.Run("configured identity requires attribution", func(t *testing.T) {
		orchestrator := NewTestOrchestrator(nil, "", "")
		orchestrator.statusInterval = 5 * time.Millisecond
		orchestrator.apiClient = NewSptAPIClient(server.URL)
		orchestrator.apiClient.SetRunID("77")
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			orchestrator.monitorStatus(ctx)
		}()
		select {
		case <-orchestrator.CompletionCh():
			t.Fatal("unattributed STOPPED status completed configured run 77")
		case <-time.After(30 * time.Millisecond):
		}
		cancel()
		<-done
	})

	t.Run("legacy unconfigured caller permits missing identity", func(t *testing.T) {
		orchestrator := NewTestOrchestrator(nil, "", "")
		orchestrator.statusInterval = 5 * time.Millisecond
		orchestrator.apiClient = NewSptAPIClient(server.URL)
		orchestrator.apiClient.setCompatibilityRunID("legacy-run")
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		go orchestrator.monitorStatus(ctx)
		select {
		case <-orchestrator.CompletionCh():
		case <-time.After(time.Second):
			t.Fatal("legacy missing-ID STOPPED policy was not preserved")
		}
	})
}

func TestWaitForLingerUsesOwnedRunAttributionAndAllowsIdle(t *testing.T) {
	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{name: "matching terminal", body: `{"state":"COMPLETED","run_id":77}`},
		{name: "matching stopped", body: `{"state":"STOPPED","run_id":77}`},
		{name: "different terminal", body: `{"state":"COMPLETED","run_id":78}`, wantErr: true},
		{name: "idle after shutdown", body: `{"state":"IDLE"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(
				writer http.ResponseWriter, request *http.Request,
			) {
				if request.URL.Path != "/status" {
					writer.WriteHeader(http.StatusNotFound)
					return
				}
				writer.Header().Set("Content-Type", "application/json")
				_, _ = writer.Write([]byte(test.body))
			}))
			defer server.Close()

			client := NewSptAPIClient(server.URL)
			client.SetRunID("77")
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := client.WaitForLingerContext(ctx, 100*time.Millisecond)
			if test.wantErr && err == nil {
				t.Fatal("WaitForLingerContext() error = nil, want attribution error")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("WaitForLingerContext() error = %v", err)
			}
		})
	}
}

func TestStoppedStatusOwnershipConcurrentPollingReconciliationAndCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(
		writer http.ResponseWriter, request *http.Request,
	) {
		if request.URL.Path != "/status" {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"state":"STOPPED","run_id":77}`))
	}))
	defer server.Close()

	orchestrator := NewTestOrchestrator(nil, "", "")
	orchestrator.statusInterval = time.Millisecond
	orchestrator.apiClient = NewSptAPIClient(server.URL)
	orchestrator.apiClient.SetRunID("77")
	orchestrator.apiClient.submissionReconcileTimeout = time.Second
	orchestrator.apiClient.submissionReconcilePollInterval = time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	var wait sync.WaitGroup
	wait.Add(2)
	go func() {
		defer wait.Done()
		orchestrator.monitorStatus(ctx)
	}()
	go func() {
		defer wait.Done()
		_, _ = orchestrator.apiClient.reconcileAmbiguousSubmission(
			ctx, 77, errors.New("ambiguous POST"))
	}()
	select {
	case <-orchestrator.CompletionCh():
	case <-time.After(time.Second):
		t.Fatal("matching run did not complete")
	}
	cancel()
	wait.Wait()
	if got := orchestrator.apiClient.getRunID(); got != "77" {
		t.Fatalf("owned run ID = %q, want 77", got)
	}
}

func TestWaitForLingerAllowsOwnedRunningToStoppedTransition(t *testing.T) {
	var statusCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		state := "RUNNING"
		if statusCalls.Add(1) >= 2 {
			state = "STOPPED"
		}
		_, _ = fmt.Fprintf(writer, `{"state":%q,"run_id":77}`, state)
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)
	client.SetRunID("77")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.WaitForLingerContext(ctx, 350*time.Millisecond); err != nil {
		t.Fatalf("RUNNING to STOPPED linger failed: %v", err)
	}
}

func TestWaitForLingerRejectsActiveDifferentRun(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		_, _ = writer.Write([]byte(`{"state":"RUNNING","run_id":78}`))
	}))
	defer server.Close()

	client := NewSptAPIClient(server.URL)
	client.SetRunID("77")
	err := client.WaitForLingerContext(context.Background(), time.Second)
	if err == nil || !strings.Contains(err.Error(), "does not match the owned run") {
		t.Fatalf("WaitForLingerContext() error = %v, want active attribution failure", err)
	}
}
