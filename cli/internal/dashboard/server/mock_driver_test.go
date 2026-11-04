package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const mockRunRequest = `{"object_count":25,"object_size":"1MiB","threads":2,"max_runtime_seconds":60,"driver":"mock"}`

func postRun(a *app, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/api/runs", strings.NewReader(body))
	req.Host = "localhost:8000"
	req.Header.Set("Origin", "http://localhost:8000")
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	a.handleRun(rec, req)
	return rec
}

func passNodes(a *app, endpointPassed bool) {
	status := "failed"
	if endpointPassed {
		status = "passed"
	}
	a.verification = verification{Status: status, Signature: configSignature(a.storage.Hosts, "https://storage.example.invalid"),
		Nodes: []verifyNode{{Host: "127.0.0.1", Status: "passed"}}, Endpoint: verifyEndpoint{Status: status}}
}

func TestMockRunNeedsOnlyVerifiedClientMachines(t *testing.T) {
	a, marker := newRunHandlerTestApp(t)
	a.verification = verification{}
	assertRejectedRun(t, a, marker, postRun(a, mockRunRequest), http.StatusConflict)

	a, marker = newRunHandlerTestApp(t)
	a.verification.Nodes = []verifyNode{{Host: "127.0.0.1", Status: "failed"}}
	assertRejectedRun(t, a, marker, postRun(a, mockRunRequest), http.StatusConflict)

	a, marker = newRunHandlerTestApp(t)
	passNodes(a, true)
	a.sptBin = filepath.Join(a.root, "missing-spt")
	assertRejectedRun(t, a, marker, postRun(a, mockRunRequest), http.StatusServiceUnavailable)

	a, marker = newRunHandlerTestApp(t)
	passNodes(a, true)
	assertRejectedRun(t, a, marker, postRun(a, strings.Replace(mockRunRequest, `"mock"`, `"nfs"`, 1)), http.StatusBadRequest)

	// An S3 run still needs the endpoint check to pass, even when the nodes passed.
	a, marker = newRunHandlerTestApp(t)
	passNodes(a, false)
	assertRejectedRun(t, a, marker, postRun(a, `{"object_count":25,"object_size":"1MiB","threads":2,"max_runtime_seconds":60}`), http.StatusConflict)
}

func TestMockRunStartsWithoutS3SettingsOrCredentials(t *testing.T) {
	a, _ := newRunHandlerTestApp(t)
	t.Setenv("S3_SECRET_KEY", "") // S3 is not ready, a mock run must still start
	passNodes(a, false)
	capture := filepath.Join(a.root, "capture.txt")
	t.Setenv("DASHBOARD_TEST_CAPTURE", capture)
	stub := "#!/bin/sh\n{ printf 'ARGS %s\\n' \"$*\"; env; } > \"$DASHBOARD_TEST_CAPTURE\"\nexit 0\n"
	if err := os.WriteFile(a.sptBin, []byte(stub), 0700); err != nil {
		t.Fatal(err)
	}
	if target := a.targetConfiguration(); target["ready"] == true || target["mock_ready"] != true {
		t.Fatalf("expected S3 not ready and mock ready: %#v", target)
	}
	rec := postRun(a, mockRunRequest)
	if rec.Code != http.StatusAccepted {
		t.Fatalf("mock run status %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Job map[string]any `json:"job"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	done := make(chan struct{})
	go func() { a.processes.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("mock run did not finish")
	}
	out, err := os.ReadFile(capture)
	if err != nil {
		t.Fatal(err)
	}
	text := string(out)
	if !strings.Contains(text, "ARGS run mock --headless --object-count 25 --object-size 1MiB --threads 2 --test-hosts 127.0.0.1") {
		t.Errorf("mock argv: %s", strings.SplitN(text, "\n", 2)[0])
	}
	for _, forbidden := range []string{"--endpoints", "--bucket", "S3_ACCESS_KEY=", "S3_SECRET_KEY=", "S3_BUCKET=", "S3_ENDPOINTS=", "test-access-key"} {
		if strings.Contains(text, forbidden) {
			t.Errorf("mock run received %q", forbidden)
		}
	}
	if got := jobWorkload(a.readStateFile()); got != "mock" {
		t.Errorf("saved job workload = %q, want mock", got)
	}
}

func TestJobWorkloadAndMockArgv(t *testing.T) {
	if jobWorkload(map[string]any{"options": options{Driver: driverMock}}) != "mock" || jobWorkload(map[string]any{"options": map[string]any{"driver": "mock"}}) != "mock" || jobWorkload(map[string]any{}) != "write-verify" {
		t.Error("jobWorkload")
	}
	a := &app{sptBin: "spt"}
	argv := strings.Join(a.buildArgv("/r", "id", options{"", 25, "1MiB", 2, 60, driverMock}, map[string]any{"hosts": []string{"10.0.0.1", "10.0.0.2"}, "endpoint": "https://s3", "bucket": "b"}), " ")
	if !strings.Contains(argv, "run mock") || !strings.Contains(argv, "--test-hosts 10.0.0.1,10.0.0.2") || strings.Contains(argv, "https://s3") {
		t.Errorf("mock argv: %s", argv)
	}
}

func TestLiveNodesReadsEachMachineSeparately(t *testing.T) {
	stamp := time.Now().UTC().Format(time.RFC3339)
	node := func(id string, ops float64) []byte {
		b, _ := json.Marshal([]map[string]any{
			{"scope": "fleet", "role": "aggregate", "node_id": "fleet", "sample_ts": stamp, "test_state": 1, "step_id": "s", "op_type": "CREATE", "operations": map[string]any{"success_count": 999.0}},
			{"scope": "node", "role": "worker", "node_id": id, "sample_ts": stamp, "test_state": 1, "step_id": "s", "op_type": "CREATE", "elapsed_time_seconds": 2.0,
				"operations": map[string]any{"success_count": ops, "failed_count": 1.0, "corrupt_count": 0.0, "success_rate_last": ops / 2},
				"bandwidth":  map[string]any{"bytes_total": ops * 1048576, "bytes_rate_last": ops / 2 * 1048576},
				"timing":     map[string]any{"latency": map[string]any{"count": ops, "p50_us": 40.0, "p99_us": 900.0}}},
		})
		return b
	}
	one := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(node("node-a", 10)) }))
	defer one.Close()
	a := &app{nodeMetricsURL: func(host string) string {
		if host == "node-a" {
			return one.URL
		}
		return "http://127.0.0.1:1/metrics/json"
	}}
	rows := a.liveNodes(map[string]any{"started_epoch": float64(time.Now().Add(-time.Minute).Unix())}, []string{"node-a", "root@node-b"})
	if len(rows) != 2 {
		t.Fatalf("rows: %#v", rows)
	}
	if rows[0]["status"] != "reporting" || rows[0]["node_id"] != "node-a" || rows[0]["successful_ops"] != 10.0 || rows[0]["p99_latency_us"] != 900.0 || rows[0]["failed_ops"] != 1.0 {
		t.Errorf("reporting node row (fleet row must be skipped, not summed): %#v", rows[0])
	}
	if rows[1]["status"] != "unavailable" || rows[1]["host"] != "node-b" {
		t.Errorf("unreachable node row: %#v", rows[1])
	}
	a.storage = storageConfig{Hosts: []string{"root@node2"}}
	if got := a.clusterMetricsEndpoint(); got != "http://node2:9999/metrics/cluster/json" {
		t.Errorf("cluster endpoint %q", got)
	}
}
