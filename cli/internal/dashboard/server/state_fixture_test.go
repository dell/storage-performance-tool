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

const pollingRunID = "mt-state-polling"

// Use a real advisory lock to represent a still-running child owned by another
// dashboard process. The tests themselves never launch SPT or contact storage.
func newPollingApp(t *testing.T) *app {
	t.Helper()
	a := newHistoryApp(t, t.TempDir())
	for _, dir := range []string{a.results, a.stateDir} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	return a
}

func pollingJob(a *app) map[string]any {
	return map[string]any{
		"run_id": pollingRunID, "status": jobRunning,
		"results_dir":   filepath.Join(a.results, pollingRunID),
		"started_epoch": float64(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).Unix()),
		"options":       map[string]any{"object_count": 10, "object_size": "1MiB", "threads": 1},
	}
}

func holdPollingLock(t *testing.T, a *app) {
	t.Helper()
	lock, err := a.lockRun()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unlockClose(lock) })
}

func pollingState(t *testing.T, a *app) map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	a.handleState(rec, httptest.NewRequest(http.MethodGet, "/api/state", nil))
	if rec.Code != http.StatusOK || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("state = %d, headers %v, body %s", rec.Code, rec.Header(), rec.Body.String())
	}
	var state map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &state); err != nil {
		t.Fatal(err)
	}
	return state
}

func pollingRows(t *testing.T, metrics map[string]any, key string) []map[string]any {
	t.Helper()
	rows, ok := metrics[key].([]any)
	if !ok {
		t.Fatalf("%s is not a JSON array: %#v", key, metrics)
	}
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		var ok bool
		if out[i], ok = row.(map[string]any); !ok {
			t.Fatalf("%s[%d] is not an object: %#v", key, i, row)
		}
	}
	return out
}

func pollingSample(ops float64) map[string]any {
	return map[string]any{"timestamp": "2026-10-01T00:00:01Z", "op_type": "CREATE", "step_id": "create-1", "cumulative_ops": ops, "count_failed": 0.0, "interval_iops": 7.0}
}

func writePollingSnapshot(t *testing.T, a *app, runID string, rows []map[string]any) {
	t.Helper()
	b, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(a.stateDir, runID+".samples.json"), b, 0600); err != nil {
		t.Fatal(err)
	}
}

func pollingEntry(phase string, ops, failed float64) map[string]any {
	return map[string]any{
		"sample_ts": "2026-10-01T00:00:02Z", "test_state": 1, "step_id": strings.ToLower(phase) + "-1", "op_type": phase,
		"scope": "node", "role": "entry", "elapsed_time_seconds": 2,
		"operations": map[string]any{"success_count": ops, "failed_count": failed, "success_rate_last": 7},
		"bandwidth":  map[string]any{"bytes_total": 1048576, "bytes_rate_last": 2097152},
	}
}
