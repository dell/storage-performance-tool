package cmd

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// engineRunningStatus is the /status body StatusServlet writes for a running run: run_id is a number.
const engineRunningStatus = `{"state":"RUNNING","since":"2026-11-18T10:00:00.000Z","run_id":1757500000000,"step_id":"s1"}`

// newReadyEntryServer serves a ready entry node running a test at 42% completion.
func newReadyEntryServer(t *testing.T) *httptest.Server {
	t.Helper()
	return newEntryServerWithStatus(t, engineRunningStatus)
}

// newEntryServerWithStatus serves a ready entry node whose /status answers statusBody.
func newEntryServerWithStatus(t *testing.T, statusBody string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ready":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"ready":true,"status":"ready","scope":"node","role":"entry","node_id":"entry-0"}`))
		case "/health":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"ok","scope":"node","role":"entry","node_id":"entry-0"}`))
		case "/status":
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(statusBody))
		case "/metrics/json":
			w.WriteHeader(http.StatusOK)
			sample := time.Now().UTC().Format(time.RFC3339)
			payload := `[{
				"completion_percent": 42,
				"overall_completion_percent": 41,
				"unbounded": false,
				"test_state": 1,
				"sample_ts": "` + sample + `",
				"operations": {"success_rate_last": 1234.0},
				"bandwidth": {"bytes_rate_last": 5242880}
			}]`
			w.Write([]byte(payload))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func TestStatusCommand_SingleHostReady(t *testing.T) {
	t.Helper()

	server := newReadyEntryServer(t)
	port := extractPort(t, server.URL)

	isolateLoggingGlobals(t)
	logFile = filepath.Join(t.TempDir(), "status.log")
	logAppend = false
	logLevel = "info"

	root := GetTestRootCmd()
	helper := NewTestHelper(t)

	_, output, err := helper.ExecuteCommand(root, "status", "--api-port", port, "--test-hosts", "127.0.0.1")
	if err != nil {
		t.Fatalf("status command returned error: %v\noutput: %s", err, output)
	}

	if !strings.Contains(output, "✅ [entry]") {
		t.Fatalf("expected success icon in output, got: %s", output)
	}
	if !strings.Contains(output, "run: state=RUNNING, run=1757500000000") {
		t.Fatalf("expected run state and numeric run id in output, got: %s", output)
	}
	if !strings.Contains(output, "completion=42%") {
		t.Fatalf("expected completion percentage in output, got: %s", output)
	}
	if !strings.Contains(output, "throughput=5.0MiB/s") {
		t.Fatalf("expected binary throughput label in output, got: %s", output)
	}
}

// Status takes its hosts from HOSTS and ignores the run's other environment, such as
// SPT_INTERFACES, which spt verify reads.
func TestStatusCommand_HostsFromEnvWithInterfacesSet(t *testing.T) {
	clearEnvDefaultsTestEnv(t)
	t.Setenv("HOSTS", "127.0.0.1")
	t.Setenv(constants.EnvInterfaces, "auto")

	server := newReadyEntryServer(t)
	port := extractPort(t, server.URL)

	isolateLoggingGlobals(t)
	logFile = filepath.Join(t.TempDir(), "status.log")
	logAppend = false
	logLevel = "info"

	root := GetTestRootCmd()
	helper := NewTestHelper(t)

	_, output, err := helper.ExecuteCommand(root, "status", "--api-port", port)
	if err != nil {
		t.Fatalf("status command returned error: %v\noutput: %s", err, output)
	}
	if !strings.Contains(output, "✅ [entry]") {
		t.Fatalf("expected the HOSTS entry to be reported ready, got: %s", output)
	}
}

func TestStatusCommand_HostError(t *testing.T) {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ready":
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("boom"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	port := extractPort(t, server.URL)

	isolateLoggingGlobals(t)
	logFile = filepath.Join(t.TempDir(), "status.log")
	logAppend = false
	logLevel = "info"

	root := GetTestRootCmd()
	helper := NewTestHelper(t)

	_, output, err := helper.ExecuteCommand(root, "status", "--api-port", port, "--test-hosts", "127.0.0.1")
	if err == nil {
		t.Fatalf("expected error but got none; output: %s", output)
	}
	if !strings.Contains(err.Error(), "status failed") {
		t.Fatalf("expected aggregated error, got: %v", err)
	}
	if !strings.Contains(output, "❌ [entry]") {
		t.Fatalf("expected failure icon in output, got: %s", output)
	}
	if !strings.Contains(output, "warn: ready probe failed") {
		t.Fatalf("expected warning about ready probe failure, got: %s", output)
	}
}

func extractPort(t *testing.T, rawURL string) string {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("failed to parse test server URL: %v", err)
	}
	port := u.Port()
	if port == "" {
		t.Fatalf("no port found in URL %s", rawURL)
	}
	return port
}

func runStatusCommandAgainst(t *testing.T, statusBody string) string {
	t.Helper()
	server := newEntryServerWithStatus(t, statusBody)
	port := extractPort(t, server.URL)

	isolateLoggingGlobals(t)
	logFile = filepath.Join(t.TempDir(), "status.log")
	logAppend = false
	logLevel = "info"

	root := GetTestRootCmd()
	helper := NewTestHelper(t)
	_, output, err := helper.ExecuteCommand(root, "status", "--api-port", port, "--test-hosts", "127.0.0.1")
	if err != nil {
		t.Fatalf("status command returned error: %v\noutput: %s", err, output)
	}
	return output
}

// A completed run keeps its run id in /status until the next run, so status still names it.
func TestStatusCommand_PrintsRunIDOfCompletedRun(t *testing.T) {
	output := runStatusCommandAgainst(t,
		`{"state":"COMPLETED","since":"2026-11-18T10:05:00.000Z","run_id":1757500000000,"step_id":"s2"}`)
	if !strings.Contains(output, "run: state=COMPLETED, run=1757500000000") {
		t.Fatalf("expected the completed run's id in output, got: %s", output)
	}
}

// A string runId, the shape status read before, is still printed.
func TestStatusCommand_AcceptsLegacyStringRunID(t *testing.T) {
	output := runStatusCommandAgainst(t, `{"runId":"run-123","state":"running","message":"Active test"}`)
	if !strings.Contains(output, `run: state=RUNNING, run=run-123, message="Active test"`) {
		t.Fatalf("expected the legacy run id in output, got: %s", output)
	}
}

// An idle node's /status carries no run id, and status prints none.
func TestStatusCommand_IdleNodeShowsNoRunID(t *testing.T) {
	output := runStatusCommandAgainst(t, `{"state":"IDLE","since":"2026-11-18T09:00:00.000Z"}`)
	if !strings.Contains(output, "run: state=IDLE") {
		t.Fatalf("expected the idle run state in output, got: %s", output)
	}
	if strings.Contains(output, "run=") {
		t.Fatalf("expected no run id for an idle node, got: %s", output)
	}
}
