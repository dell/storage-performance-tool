package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

type attachedAPIReply struct {
	statusCode, metricsCode int
	statusBody, metricsBody string
	disconnect              bool
}

type attachedAPIFixture struct {
	mu    sync.Mutex
	reply attachedAPIReply
}

func (f *attachedAPIFixture) set(reply attachedAPIReply) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reply = reply
}

func attachedReply(runID, stamp string, count int) attachedAPIReply {
	return attachedAPIReply{statusCode: 200, metricsCode: 200,
		// Legacy runId is supported both before and after PR #17's decoder fix.
		statusBody:  fmt.Sprintf(`{"runId":%q,"state":"RUNNING"}`, runID),
		metricsBody: fmt.Sprintf(`[{"sample_ts":%q,"test_state":1,"step_id":"create","op_type":"CREATE","scope":"node","role":"entry","elapsed_time_seconds":2,"operations":{"success_count":%d,"failed_count":0,"success_rate_last":5},"bandwidth":{"bytes_total":1048576,"bytes_rate_last":524288},"timing":{"latency":{"count":1,"p50_us":42}}}]`, stamp, count),
	}
}

func newAttachedAPIFixture(t *testing.T) (*app, *attachedAPIFixture) {
	t.Helper()
	f := &attachedAPIFixture{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		reply := f.reply
		f.mu.Unlock()
		if r.Method != http.MethodGet {
			t.Errorf("unexpected engine method: %s", r.Method)
		}
		if r.URL.Path == "/status" && reply.disconnect {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			_ = conn.Close()
			return
		}
		switch r.URL.Path {
		case "/status":
			w.WriteHeader(reply.statusCode)
			_, _ = fmt.Fprint(w, reply.statusBody)
		case "/metrics/json":
			w.WriteHeader(reply.metricsCode)
			_, _ = fmt.Fprint(w, reply.metricsBody)
		default:
			t.Errorf("unexpected engine path: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	_, port, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	a, _ := newRunHandlerTestApp(t)
	a.sptAPIPort = port
	return a, f
}

func attachedRequest(t *testing.T, a *app, method, path, body string, want int) map[string]any {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Host = "localhost:8000"
	r.Header.Set("Origin", "http://localhost:8000")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	a.httpServer().Handler.ServeHTTP(w, r)
	if w.Code != want {
		t.Fatalf("%s %s: %d %s; want %d", method, path, w.Code, w.Body.String(), want)
	}
	var state map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func assertAttachedCounts(t *testing.T, state map[string]any, want []float64) {
	t.Helper()
	metrics, ok := state["metrics"].(map[string]any)
	if !ok {
		t.Fatalf("missing metrics: %#v", state)
	}
	rows, _ := metrics["chart_samples"].([]any)
	counts := []float64{}
	for _, row := range rows {
		sample, ok := row.(map[string]any)
		if !ok {
			t.Fatalf("invalid sample: %#v", row)
		}
		count, ok := sample["cumulative_ops"].(float64)
		if !ok {
			t.Fatalf("missing measured count: %#v", sample)
		}
		counts = append(counts, count)
	}
	if !reflect.DeepEqual(counts, want) {
		t.Fatalf("chart counts = %v; want %v", counts, want)
	}
}

func TestAttachedStatusFailureAndRecovery(t *testing.T) {
	for _, name := range []string{"HTTP error", "malformed JSON", "connection closed", "run ended", "missing run ID"} {
		t.Run(name, func(t *testing.T) {
			a, api := newAttachedAPIFixture(t)
			first := attachedReply("run-a", "2026-10-30T12:00:01Z", 10)
			api.set(first)
			state := attachedRequest(t, a, "POST", "/api/attach", `{"host":"127.0.0.1"}`, 200)
			assertAttachedCounts(t, state, []float64{10})
			broken := first
			switch name {
			case "HTTP error":
				broken.statusCode = 503
			case "malformed JSON":
				broken.statusBody = "{"
			case "connection closed":
				broken.disconnect = true
			case "run ended":
				broken.statusBody = `{"runId":"run-a","state":"IDLE"}`
			case "missing run ID":
				broken.statusBody = `{"state":"RUNNING"}`
			}
			api.set(broken)
			failure := attachedRequest(t, a, "GET", "/api/state", "", 502)
			if str(failure["error"]) == "" || failure["job"] != nil || failure["metrics"] != nil {
				t.Fatalf("poll failure did not return a clear error: %#v", failure)
			}
			if a.attachedHost != "127.0.0.1" || a.attachedRunID != "run-a" || len(a.attachedRows) != 1 {
				t.Fatal("temporary status failure discarded the attachment")
			}
			api.set(attachedReply("run-a", "2026-10-30T12:00:02Z", 20))
			recovered := attachedRequest(t, a, "GET", "/api/state", "", 200)
			assertAttachedCounts(t, recovered, []float64{10, 20})
			connection, _ := recovered["connection"].(map[string]any)
			if connection["status"] != "connected" || connection["read_only"] != true {
				t.Fatalf("wrong recovered connection: %#v", connection)
			}
		})
	}
}
