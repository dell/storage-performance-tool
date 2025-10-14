package cmd

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestStatusCommand_SingleHostReady(t *testing.T) {
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
			w.Write([]byte(`{"runId":"run-123","state":"running","message":"Active test","progress":37.5}`))
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
				"bandwidth": {"bytes_rate_last": 5000000}
			}]`
			w.Write([]byte(payload))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	port := extractPort(t, server.URL)

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
	if !strings.Contains(output, "run: state=RUNNING") {
		t.Fatalf("expected run state in output, got: %s", output)
	}
	if !strings.Contains(output, "completion=42%") {
		t.Fatalf("expected completion percentage in output, got: %s", output)
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
