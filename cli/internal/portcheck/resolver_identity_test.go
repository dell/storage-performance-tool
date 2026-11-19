/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	containertypes "github.com/docker/docker/api/types/container"
)

// lookalikeHolder is an ordinary HTTP service on a port published by a container
// whose image name contains "spt". It answers no SPT API.
type lookalikeHolder struct {
	port   string
	docker *fakeDockerAPI

	mu      sync.Mutex
	changes []string
}

func newLookalikeHolder(t *testing.T) *lookalikeHolder {
	t.Helper()
	holder := &lookalikeHolder{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		if r.Method != http.MethodGet {
			holder.mu.Lock()
			holder.changes = append(holder.changes, r.Method+" "+r.URL.Path)
			holder.mu.Unlock()
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	t.Cleanup(server.Close)
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	holder.port = port
	holder.docker = &fakeDockerAPI{containers: []containertypes.Summary{{
		ID: "dashboard", Names: []string{"/dashboard"}, Image: "spt-dashboard",
		State: containertypes.StateRunning,
		Ports: []containertypes.Port{{IP: "0.0.0.0", PublicPort: listenerPort(t, port), PrivatePort: 80, Type: "tcp"}},
	}}}
	return holder
}

func (h *lookalikeHolder) resolver(force, interactive bool) *ConflictResolver {
	checker := NewPortCheckerWithOptions(h.port, "127.0.0.1", time.Second, 2*time.Second, 1)
	checker.newFinder = func() (*DockerContainerFinder, error) { return newFakeFinder(h.docker), nil }
	return &ConflictResolver{
		Port: h.port, checker: checker, dockerFinder: newFakeFinder(h.docker),
		forceMode: force, interactiveMode: interactive, clock: NewFakeClock(),
	}
}

func (h *lookalikeHolder) assertNoAPIChange(t *testing.T) {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.changes) != 0 {
		t.Errorf("service received %v, want reads only", h.changes)
	}
}

func (h *lookalikeHolder) assertUntouched(t *testing.T) {
	t.Helper()
	h.assertNoAPIChange(t)
	if stopped, removed := h.docker.mutations(); len(stopped) != 0 || len(removed) != 0 {
		t.Errorf("stopped %v, removed %v; want none", stopped, removed)
	}
}

// A name match does not make g available, and typing g anyway neither shuts the
// service down nor removes its container.
func TestLookalikeContainerGetsNoGracefulStop(t *testing.T) {
	for _, answer := range []string{"g", "graceful"} {
		t.Run(answer, func(t *testing.T) {
			holder := newLookalikeHolder(t)
			resolver := holder.resolver(false, true)
			resolver.input = strings.NewReader(answer + "\na\n")
			defer resolver.Close()

			result, err := resolver.CheckAndResolveConflicts(context.Background())
			if err != nil {
				t.Fatalf("CheckAndResolveConflicts() error = %v", err)
			}
			if result.Strategy != StrategyAbort {
				t.Fatalf("result = %+v, want the typed abort after g was refused", result)
			}
			holder.assertUntouched(t)
		})
	}
}

// --force goes straight to the container that publishes the port.
func TestForceSendsNoAPIRequestToALookalikeContainer(t *testing.T) {
	holder := newLookalikeHolder(t)
	resolver := holder.resolver(true, false)
	defer resolver.Close()

	if _, err := resolver.CheckAndResolveConflicts(context.Background()); err != nil {
		t.Fatalf("CheckAndResolveConflicts() error = %v", err)
	}
	holder.assertNoAPIChange(t)
	if _, removed := holder.docker.mutations(); !slices.Equal(removed, []string{"dashboard"}) {
		t.Errorf("removed = %v, want the publishing container", removed)
	}
}

func TestGracefulStopNeedsAnAPIAnswer(t *testing.T) {
	engine := newFakeEngine(t, 41)
	resolver := engine.resolver(true, false)
	defer resolver.Close()

	result := resolver.executeGracefulStop(context.Background(), &ServiceInfo{IsListening: true, IsSpt: true})
	if result.Success || !strings.Contains(result.Message, "needs an SPT API") {
		t.Fatalf("result = %+v, want the refusal", result)
	}
	if got := engine.received(); len(got) != 0 {
		t.Errorf("engine received %+v, want nothing", got)
	}
}

// A worker of a distributed run answers /status with IDLE while HEAD /run names
// the run it carries: the report must show that run as active.
func TestWorkerEngineIsReportedWithItsActiveRun(t *testing.T) {
	engine := newFakeEngine(t, 501)
	engine.worker = true
	resolver := engine.resolver(false, false)
	defer resolver.Close()

	info, err := resolver.checkService(context.Background())
	if err != nil {
		t.Fatalf("checkService() error = %v", err)
	}
	if !info.IsTestActive() {
		t.Errorf("IsTestActive() = false for status %+v", info.Status)
	}
	result, err := resolver.CheckAndResolveConflicts(context.Background())
	assertUnattendedAbort(t, result, err, "held by an SPT engine (run 501, state RUNNING)")
	assertReadOnly(t, engine.received())
}

// A finished run in /status does not hide the different run active now.
func TestActiveRunReplacesTheLastRunInStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/status":
			_, _ = w.Write([]byte(`{"state":"COMPLETED","run_id":41}`))
		case r.URL.Path == "/run" && r.Method == http.MethodHead:
			w.Header().Set("ETag", "42")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	status, err := NewSptAPIClient(server.URL, 5*time.Second).GetStatus(context.Background())
	if err != nil {
		t.Fatalf("GetStatus() error = %v", err)
	}
	if status.RunID != "42" || status.State != "RUNNING" || !status.RunActive {
		t.Errorf("status = %+v, want active run 42", status)
	}
}

// A stopped run still cleaning up keeps its own state.
func TestActiveRunKeepsItsOwnStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/status":
			_, _ = w.Write([]byte(`{"state":"STOPPED","run_id":41}`))
		case r.URL.Path == "/run" && r.Method == http.MethodHead:
			w.Header().Set("ETag", "41")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	status, err := NewSptAPIClient(server.URL, 5*time.Second).GetStatus(context.Background())
	if err != nil {
		t.Fatalf("GetStatus() error = %v", err)
	}
	if status.RunID != "41" || status.State != "STOPPED" {
		t.Errorf("status = %+v, want run 41 STOPPED", status)
	}
}
