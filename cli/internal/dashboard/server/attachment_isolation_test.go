package server

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAttachedRunSwitchAndDetachIsolateHistory(t *testing.T) {
	a, api := newAttachedAPIFixture(t)
	localID := "mt-dashboard-20261030T110000-abcd"
	a.activeJob = map[string]any{"run_id": localID, "status": jobCompleted, "returncode": 0}
	api.set(attachedReply("run-a", "2026-10-30T12:00:01Z", 100))
	state := attachedRequest(t, a, "POST", "/api/attach", `{"host":"127.0.0.1"}`, 200)
	assertAttachedCounts(t, state, []float64{100})
	api.set(attachedReply("run-a", "2026-10-30T12:00:02Z", 200))
	state = attachedRequest(t, a, "GET", "/api/state", "", 200)
	assertAttachedCounts(t, state, []float64{100, 200})
	// Even without a first measurement for B, A's samples must disappear.
	next := attachedReply("run-b", "2026-10-30T12:00:03Z", 1)
	next.metricsCode = 503
	api.set(next)
	state = attachedRequest(t, a, "GET", "/api/state", "", 200)
	assertAttachedCounts(t, state, []float64{})
	if state["job"].(map[string]any)["run_id"] != "run-b" || state["metrics"].(map[string]any)["run_id"] != "run-b" {
		t.Fatalf("run identity did not switch: %#v", state)
	}
	api.set(attachedReply("run-b", "2026-10-30T12:00:03Z", 1))
	state = attachedRequest(t, a, "GET", "/api/state", "", 200)
	assertAttachedCounts(t, state, []float64{1})
	api.set(attachedReply("run-b", "2026-10-30T12:00:04Z", 2))
	state = attachedRequest(t, a, "GET", "/api/state", "", 200)
	assertAttachedCounts(t, state, []float64{1, 2})
	attachedRequest(t, a, "DELETE", "/api/attach", "", 200)
	if a.attachedHost != "" || a.attachedRunID != "" || len(a.attachedRows) != 0 {
		t.Fatal("disconnect left attachment state behind")
	}
	state = attachedRequest(t, a, "GET", "/api/state", "", 200)
	if state["job"].(map[string]any)["run_id"] != localID || state["connection"] != nil {
		t.Fatalf("disconnect did not restore local state: %#v", state)
	}
	state = attachedRequest(t, a, "POST", "/api/attach", `{"host":"127.0.0.1"}`, 200)
	assertAttachedCounts(t, state, []float64{2})
	if a.activeJob["run_id"] != localID || a.activeJob["status"] != jobCompleted || a.activeCmd != nil || a.activeLock != nil {
		t.Fatal("read-only monitoring changed the local job")
	}
	for _, path := range []string{filepath.Join(a.root, "spt-invoked"), a.stateFile} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Fatalf("monitoring unexpectedly wrote %s: %v", filepath.Base(path), err)
		}
	}
	if entries, err := os.ReadDir(a.results); !os.IsNotExist(err) || len(entries) != 0 {
		t.Fatalf("monitoring created results: %v, %v", entries, err)
	}
}
