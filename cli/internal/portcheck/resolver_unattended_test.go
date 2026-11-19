/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func assertUnattendedAbort(t *testing.T, result *ResolutionResult, err error, wantDetails string) {
	t.Helper()
	if err != nil {
		t.Fatalf("CheckAndResolveConflicts() error = %v", err)
	}
	if result.Success || result.Strategy != StrategyAbort {
		t.Fatalf("result = success %t strategy %q, want a failed abort", result.Success, result.Strategy)
	}
	if !errors.Is(result.Error, ErrPortConflictUnattended) {
		t.Errorf("result.Error = %v, want ErrPortConflictUnattended", result.Error)
	}
	for _, want := range []string{wantDetails, "left running", "--api-port", "--rmi-registry-port"} {
		if !strings.Contains(result.Message, want) {
			t.Errorf("Message = %q, want it to contain %q", result.Message, want)
		}
	}
}

// Without a terminal and without --force, a port held by an engine ends the
// new run: the other run may be someone's, so nothing may stop it.
func TestUnattendedConflictLeavesTheEngineAlone(t *testing.T) {
	tests := []struct {
		name        string
		runID       int64
		interactive bool
		wantDetails string
	}{
		{"no terminal, active run", 1763456789012, false, "held by an SPT engine (run 1763456789012, state RUNNING)"},
		{"no terminal, idle engine", 0, false, "held by an SPT engine (state IDLE)"},
		{"end of input at the prompt", 1763456789012, true, "held by an SPT engine (run 1763456789012, state RUNNING)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := newFakeEngine(t, tt.runID)
			resolver := engine.resolver(false, tt.interactive)
			resolver.input = strings.NewReader("")
			defer resolver.Close()

			result, err := resolver.CheckAndResolveConflicts(context.Background())
			assertUnattendedAbort(t, result, err, tt.wantDetails)
			assertReadOnly(t, engine.received())
		})
	}
}

// End of input after an unrecognised answer is still no decision.
func TestPromptEndOfInputAfterInvalidAnswerAborts(t *testing.T) {
	engine := newFakeEngine(t, 41)
	resolver := engine.resolver(false, true)
	resolver.input = strings.NewReader("maybe\n")
	defer resolver.Close()

	result, err := resolver.CheckAndResolveConflicts(context.Background())
	assertUnattendedAbort(t, result, err, "run 41")
	if !strings.Contains(result.Message, "--force") {
		t.Errorf("Message = %q, want it to offer --force", result.Message)
	}
	assertReadOnly(t, engine.received())
}

// A typed abort stays the operator's own cancellation.
func TestPromptAbortAnswerIsNotUnattended(t *testing.T) {
	engine := newFakeEngine(t, 41)
	resolver := engine.resolver(false, true)
	resolver.input = strings.NewReader("a\n")
	defer resolver.Close()

	result, err := resolver.CheckAndResolveConflicts(context.Background())
	if err != nil {
		t.Fatalf("CheckAndResolveConflicts() error = %v", err)
	}
	if result.Strategy != StrategyAbort || errors.Is(result.Error, ErrPortConflictUnattended) {
		t.Fatalf("result = %+v, want the operator's abort", result)
	}
	assertReadOnly(t, engine.received())
}

// End of input with another service on the port names that service, not an
// engine.
func TestPromptEndOfInputWithOtherServiceAborts(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("service received %s %s", r.Method, r.URL.Path)
		}
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprint(w, "not here")
	}))
	defer server.Close()
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	checker := NewPortCheckerWithOptions(port, "127.0.0.1", time.Second, 2*time.Second, 1)
	checker.newFinder = func() (*DockerContainerFinder, error) { return newFakeFinder(&fakeDockerAPI{}), nil }
	resolver := &ConflictResolver{
		Port: port, checker: checker, interactiveMode: true,
		clock: NewFakeClock(), input: strings.NewReader(""),
	}
	defer resolver.Close()

	result, err := resolver.CheckAndResolveConflicts(context.Background())
	assertUnattendedAbort(t, result, err, "port "+port+" is in use (")
	if strings.Contains(result.Message, "SPT engine") {
		t.Errorf("Message = %q names an SPT engine", result.Message)
	}
}

// Container detection's placeholder run ID is not shown as a run.
func TestUnattendedMessageOmitsPlaceholderRunID(t *testing.T) {
	resolver := &ConflictResolver{Port: "9999"}
	message := resolver.unattendedAbortMessage(&ServiceInfo{
		IsListening: true, IsSpt: true, Status: &SptStatus{RunID: "detected", State: "RUNNING"},
	})
	if strings.Contains(message, "run detected") || !strings.Contains(message, "(state RUNNING)") {
		t.Errorf("message = %q", message)
	}
}
