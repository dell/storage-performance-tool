/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"
)

// engineRequest is one request a fakeEngine received.
type engineRequest struct {
	method  string
	path    string
	ifMatch string
}

// fakeEngine answers as RunServlet, StatusServlet and ShutdownServlet do. A
// zero runID is an idle engine.
type fakeEngine struct {
	t      *testing.T
	server *httptest.Server

	mu       sync.Mutex
	runID    int64
	requests []engineRequest
	// onDelete, when set, replaces the matching-run behaviour of DELETE /run.
	onDelete func(engine *fakeEngine, w http.ResponseWriter)
	// shutdownStatus, when non-zero, replaces the 202 answer of POST /shutdown.
	shutdownStatus int
	// onHead, when set, runs before HEAD /run is answered.
	onHead func(engine *fakeEngine)
	// worker makes /status answer IDLE while HEAD /run reports the run, as a
	// worker engine of a distributed run does.
	worker bool
	// since, when set, is the start time /status reports for a running run.
	since string
	// headStatus, when non-zero, replaces the answer of HEAD /run.
	headStatus int
	// onShutdown, when set, runs before POST /shutdown is answered.
	onShutdown func(engine *fakeEngine)
	// statusCode, when non-zero, replaces the answer of GET /status.
	statusCode int
	// statusState and statusRunID, when set, replace the state and run_id that
	// GET /status reports for an active run.
	statusState string
	statusRunID int64
}

func newFakeEngine(t *testing.T, runID int64) *fakeEngine {
	t.Helper()
	engine := &fakeEngine{t: t, runID: runID}
	engine.server = httptest.NewServer(http.HandlerFunc(engine.serve))
	t.Cleanup(engine.server.Close)
	return engine
}

func (e *fakeEngine) serve(w http.ResponseWriter, r *http.Request) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ifMatch := r.Header.Get("If-Match")
	e.requests = append(e.requests, engineRequest{r.Method, r.URL.Path, ifMatch})
	switch {
	case r.URL.Path == "/metrics/json" && r.Method == http.MethodGet:
		fmt.Fprint(w, `[{"metrics_schema":5,"scope":"node","role":"entry","node_id":"n0","op_type":"none"}]`)
	case r.URL.Path == "/status" && r.Method == http.MethodGet:
		if e.statusCode != 0 {
			w.WriteHeader(e.statusCode)
			return
		}
		if e.runID == 0 || e.worker {
			fmt.Fprint(w, `{"state":"IDLE"}`)
			return
		}
		state, runID := "RUNNING", e.runID
		if e.statusState != "" {
			state = e.statusState
		}
		if e.statusRunID != 0 {
			runID = e.statusRunID
		}
		if e.since != "" {
			fmt.Fprintf(w, `{"state":%q,"run_id":%d,"since":%q}`, state, runID, e.since)
			return
		}
		fmt.Fprintf(w, `{"state":%q,"run_id":%d}`, state, runID)
	case r.URL.Path == "/run" && r.Method == http.MethodHead:
		if e.onHead != nil {
			e.onHead(e)
		}
		if e.headStatus != 0 {
			w.WriteHeader(e.headStatus)
			return
		}
		if e.runID == 0 {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("ETag", strconv.FormatInt(e.runID, 10))
	case r.URL.Path == "/run" && (r.Method == http.MethodGet || r.Method == http.MethodDelete):
		e.serveMatchedRun(w, r.Method, ifMatch)
	case r.URL.Path == "/shutdown" && r.Method == http.MethodPost:
		if e.onShutdown != nil {
			e.onShutdown(e)
		}
		if e.shutdownStatus != 0 {
			w.WriteHeader(e.shutdownStatus)
			return
		}
		w.WriteHeader(http.StatusAccepted)
	default:
		w.WriteHeader(http.StatusNotFound)
	}
}

func (e *fakeEngine) serveMatchedRun(w http.ResponseWriter, method, ifMatch string) {
	if ifMatch == "" {
		http.Error(w, "Missing header: If-Match", http.StatusBadRequest)
		return
	}
	runID, err := strconv.ParseInt(ifMatch, 10, 64)
	if err != nil {
		http.Error(w, "Invalid run ID format: "+ifMatch, http.StatusBadRequest)
		return
	}
	if method == http.MethodDelete && e.onDelete != nil {
		e.onDelete(e, w)
		return
	}
	switch {
	case e.runID == 0:
		w.WriteHeader(http.StatusNoContent)
	case e.runID != runID && method == http.MethodDelete:
		w.WriteHeader(http.StatusNotFound)
	case e.runID != runID:
		w.WriteHeader(http.StatusNoContent)
	case method == http.MethodDelete:
		e.runID = 0
	}
}

func (e *fakeEngine) port() string {
	e.t.Helper()
	_, port, err := net.SplitHostPort(e.server.Listener.Addr().String())
	if err != nil {
		e.t.Fatalf("engine address: %v", err)
	}
	return port
}

func (e *fakeEngine) received() []engineRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]engineRequest(nil), e.requests...)
}

// resolver returns a resolver for the engine's port with no Docker access.
func (e *fakeEngine) resolver(force, interactive bool) *ConflictResolver {
	return &ConflictResolver{
		Port:            e.port(),
		checker:         NewPortCheckerWithOptions(e.port(), "127.0.0.1", time.Second, 2*time.Second, 1),
		forceMode:       force,
		interactiveMode: interactive,
		// Advances on every wait, so a wait that should not happen hits its deadline.
		clock: &pollClock{},
	}
}

// assertReadOnly fails if the engine received anything but reads.
func assertReadOnly(t *testing.T, requests []engineRequest) {
	t.Helper()
	for _, request := range requests {
		if request.method != http.MethodGet && request.method != http.MethodHead {
			t.Errorf("engine received %s %s, want reads only", request.method, request.path)
		}
	}
}
