package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// engineRunningStatus is GET /status from the engine on the Dev VM during
// `spt run mock` (Oct 24, 2026). run_id is a JSON number.
const engineRunningStatus = `{"state":"RUNNING","since":"2026-10-24T22:57:41.187Z","run_id":1792882655698,"step_id":"none-20261024.225738.837"}`

func TestAttachReadsAnExistingRunAndCanDisconnect(t *testing.T) {
	stamp := time.Now().UTC().Format(time.RFC3339Nano)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/status":
			_, _ = fmt.Fprint(w, engineRunningStatus)
		case "/metrics/json":
			_, _ = fmt.Fprintf(w, `[{"sample_ts":%q,"test_state":1,"step_id":"create","op_type":"CREATE","scope":"node","role":"entry","elapsed_time_seconds":2,"operations":{"success_count":10,"failed_count":1,"success_rate_last":8},"bandwidth":{"bytes_total":10485760,"bytes_rate_last":8388608},"timing":{"latency":{"count":10,"p50_us":42}}}]`, stamp)
		default:
			http.NotFound(w, r)
		}
	}))
	defer api.Close()
	_, port, err := net.SplitHostPort(strings.TrimPrefix(api.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	a := &app{port: "8000", sptAPIPort: port, storage: storageConfig{Hosts: []string{"127.0.0.1"}}, settings: defaultSettings()}
	handler := a.httpServer().Handler
	post := httptest.NewRequest(http.MethodPost, "/api/attach", strings.NewReader(`{"host":"127.0.0.1"}`))
	post.Host = "localhost:8000"
	post.Header.Set("Origin", "http://localhost:8000")
	post.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, post)
	if response.Code != http.StatusOK {
		t.Fatalf("attach status=%d body=%s", response.Code, response.Body.String())
	}
	var state map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	connection := state["connection"].(map[string]any)
	job := state["job"].(map[string]any)
	metrics := state["metrics"].(map[string]any)
	if connection["status"] != "connected" || connection["read_only"] != true || job["run_id"] != "1792882655698" || connection["run_id"] != "1792882655698" {
		t.Fatalf("unexpected attached state: %#v", state)
	}
	if got := len(metrics["samples"].([]any)); got != 1 {
		t.Fatalf("attached live samples=%d; want 1", got)
	}

	get := httptest.NewRequest(http.MethodGet, "/api/state", nil)
	get.Host = "localhost:8000"
	getResponse := httptest.NewRecorder()
	handler.ServeHTTP(getResponse, get)
	if getResponse.Code != http.StatusOK || !strings.Contains(getResponse.Body.String(), `"run_id":"1792882655698"`) {
		t.Fatalf("attached poll status=%d body=%s", getResponse.Code, getResponse.Body.String())
	}

	detach := httptest.NewRequest(http.MethodDelete, "/api/attach", nil)
	detach.Host = "localhost:8000"
	detach.Header.Set("Origin", "http://localhost:8000")
	detachResponse := httptest.NewRecorder()
	handler.ServeHTTP(detachResponse, detach)
	if detachResponse.Code != http.StatusOK || a.attachedHost != "" {
		t.Fatalf("detach status=%d, attached host=%q", detachResponse.Code, a.attachedHost)
	}
}

func TestAttachRejectsUnconfiguredHostsAndReportsUnreachableAPI(t *testing.T) {
	for _, test := range []struct {
		name       string
		configured string
		requested  string
		apiPort    string
		wantStatus int
		wantText   string
	}{
		{name: "unconfigured host", configured: "127.0.0.1", requested: "example.com", apiPort: "1", wantStatus: http.StatusBadRequest, wantText: "already listed"},
		{name: "unreachable API", configured: "127.0.0.1", requested: "127.0.0.1", apiPort: "1", wantStatus: http.StatusBadGateway, wantText: "Could not reach SPT"},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := &app{port: "8000", sptAPIPort: test.apiPort, storage: storageConfig{Hosts: []string{test.configured}}, settings: defaultSettings()}
			request := httptest.NewRequest(http.MethodPost, "/api/attach", strings.NewReader(fmt.Sprintf(`{"host":%q}`, test.requested)))
			request.Host = "localhost:8000"
			request.Header.Set("Origin", "http://localhost:8000")
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			a.httpServer().Handler.ServeHTTP(response, request)
			if response.Code != test.wantStatus || !strings.Contains(response.Body.String(), test.wantText) {
				t.Fatalf("status=%d body=%s; wanted status %d containing %q", response.Code, response.Body.String(), test.wantStatus, test.wantText)
			}
		})
	}
}

func TestAttachRequiresSameOrigin(t *testing.T) {
	a := &app{port: "8000", storage: storageConfig{Hosts: []string{"127.0.0.1"}}}
	request := httptest.NewRequest(http.MethodPost, "/api/attach", strings.NewReader(`{"host":"127.0.0.1"}`))
	request.Host = "localhost:8000"
	request.Header.Set("Origin", "https://attacker.example")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	a.httpServer().Handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("status=%d body=%s; want forbidden", response.Code, response.Body.String())
	}
}

func TestFetchAttachedStatusReadsTheEngineStatusShape(t *testing.T) {
	for _, test := range []struct {
		name string
		body string
		want attachedStatus
	}{
		{name: "engine running", body: engineRunningStatus, want: attachedStatus{RunID: "1792882655698", State: "RUNNING", StepID: "none-20261024.225738.837"}},
		{name: "id above 2^53", body: `{"state":"RUNNING","run_id":9007199254740993}`, want: attachedStatus{RunID: "9007199254740993", State: "RUNNING"}},
		{name: "engine idle", body: `{"state":"IDLE","since":"2026-10-24T22:50:00Z"}`, want: attachedStatus{State: "IDLE"}},
		{name: "engine failed", body: `{"state":"FAILED","run_id":7,"category":"engine","message":"boom"}`, want: attachedStatus{RunID: "7", State: "FAILED", Message: "boom"}},
		{name: "string run_id", body: `{"state":"RUNNING","run_id":" 42 "}`, want: attachedStatus{RunID: "42", State: "RUNNING"}},
		{name: "legacy runId", body: `{"runId":"run-9","state":"running","message":"Run active","progress":12.5}`, want: attachedStatus{RunID: "run-9", State: "running", Message: "Run active", Progress: 12.5}},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/status" || r.Method != http.MethodGet {
					t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
				}
				_, _ = fmt.Fprint(w, test.body)
			}))
			defer server.Close()
			got, err := fetchAttachedStatus(server.URL)
			if err != nil || got != test.want {
				t.Fatalf("status=%+v error=%v; want %+v", got, err, test.want)
			}
		})
	}
}

func TestFetchAttachedStatusRejectsBadRunIDs(t *testing.T) {
	for _, body := range []string{`{"state":"RUNNING","run_id":1.5}`, `{"state":"RUNNING","run_id":{"id":1}}`, `{"state":"RUNNING","run_id":true}`} {
		var status attachedStatus
		if err := json.Unmarshal([]byte(body), &status); err == nil {
			t.Fatalf("%s decoded as %+v; want an error", body, status)
		}
	}
}

func TestAttachRefusesAnEngineWithoutARunningRun(t *testing.T) {
	for _, body := range []string{
		`{"state":"IDLE","since":"2026-10-24T22:50:00Z"}`,
		`{"state":"COMPLETED","since":"2026-10-24T23:00:00Z","run_id":1792882655698,"step_id":"none-20261024.225738.837"}`,
		`{"state":"RUNNING","since":"2026-10-24T22:57:41.187Z"}`,
	} {
		api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_, _ = fmt.Fprint(w, body)
		}))
		_, port, err := net.SplitHostPort(strings.TrimPrefix(api.URL, "http://"))
		if err != nil {
			t.Fatal(err)
		}
		a := &app{port: "8000", sptAPIPort: port, storage: storageConfig{Hosts: []string{"127.0.0.1"}}, settings: defaultSettings()}
		request := httptest.NewRequest(http.MethodPost, "/api/attach", strings.NewReader(`{"host":"127.0.0.1"}`))
		request.Host = "localhost:8000"
		request.Header.Set("Origin", "http://localhost:8000")
		request.Header.Set("Content-Type", "application/json")
		response := httptest.NewRecorder()
		a.httpServer().Handler.ServeHTTP(response, request)
		api.Close()
		if response.Code != http.StatusBadGateway || !strings.Contains(response.Body.String(), "not reporting an active run") || a.attachedHost != "" {
			t.Fatalf("%s: status=%d body=%s attached=%q", body, response.Code, response.Body.String(), a.attachedHost)
		}
	}
}
