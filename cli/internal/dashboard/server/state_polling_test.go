package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func TestStatePollingLiveMeasurementsTakePrecedence(t *testing.T) {
	for _, phase := range []struct{ op, label string }{
		{"CREATE", "Create / write"}, {"WRITE", "Create / write"}, {"VERIFY", "Verify"},
		{"READ", "Verify"}, {"DELETE", "Cleanup"}, {"CLEANUP", "Cleanup"},
	} {
		t.Run(phase.op, func(t *testing.T) {
			a := newPollingApp(t)
			holdPollingLock(t, a)
			a.activeJob = pollingJob(a)
			writeSavedRun(t, a.results, pollingRunID, "write-verify", 10, 999, false)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodGet || r.Header.Get("Accept") != "application/json" {
					t.Errorf("unexpected metrics request: %s %v", r.Method, r.Header)
				}
				_ = json.NewEncoder(w).Encode(pollingEntry(phase.op, 12, 2))
			}))
			defer srv.Close()
			a.metricsURL = srv.URL
			state := pollingState(t, a)
			job := mapValue(state["job"])
			metrics := mapValue(state["metrics"])
			rows := pollingRows(t, metrics, "samples")
			if job["status"] != jobRunning || job["phase"] != phase.label || len(rows) != 1 || rows[0]["cumulative_ops"] != 12.0 || rows[0]["count_failed"] != 2.0 {
				t.Fatalf("live data did not win over conflicting CSV totals: %#v", state)
			}
			if metrics["run_id"] != pollingRunID || metrics["is_active"] != true || metrics["is_aggregate"] != false || len(pollingRows(t, metrics, "chart_samples")) != 1 {
				t.Fatalf("incorrect live envelope: %#v", metrics)
			}
			if !strings.Contains(str(job["message"]), "12 successful, 2 failed") || a.readStateFile()["phase"] != phase.label {
				t.Fatalf("live progress was not reported and persisted: %#v", job)
			}
		})
	}
}

func TestStatePollingPhaseGapAndRecovery(t *testing.T) {
	a := newPollingApp(t)
	holdPollingLock(t, a)
	a.activeJob = pollingJob(a)
	var stage atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		switch stage.Load() {
		case 0:
			_ = json.NewEncoder(w).Encode(pollingEntry("CREATE", 10, 0))
		case 1:
			w.WriteHeader(http.StatusServiceUnavailable)
		case 2:
			entry := pollingEntry("VERIFY", 0, 0)
			entry["bandwidth"] = map[string]any{}
			_ = json.NewEncoder(w).Encode(entry)
		default:
			_ = json.NewEncoder(w).Encode(pollingEntry("VERIFY", 20, 1))
		}
	}))
	defer srv.Close()
	a.metricsURL = srv.URL
	for _, step := range []struct {
		name, phase  string
		stage, count int
		ops          float64
	}{
		{"first measurement", "Create / write", 0, 1, 10},
		{"temporary polling failure", "Finalizing results", 1, 1, 10},
		{"repeated failure", "Finalizing results", 1, 1, 10},
		{"next phase has no measurements yet", "Verify", 2, 1, 10},
		{"next phase reports", "Verify", 3, 2, 20},
	} {
		t.Run(step.name, func(t *testing.T) {
			stage.Store(int32(step.stage))
			state := pollingState(t, a)
			metrics := mapValue(state["metrics"])
			rows := pollingRows(t, metrics, "samples")
			if mapValue(state["job"])["phase"] != step.phase || len(rows) != 1 || rows[0]["cumulative_ops"] != step.ops || len(pollingRows(t, metrics, "chart_samples")) != step.count {
				t.Fatalf("phase transition lost or fabricated measurements: %#v", state)
			}
			if step.stage == 2 && !strings.Contains(str(mapValue(state["job"])["message"]), "waiting for its first measured operation") {
				t.Fatalf("empty new phase was reported as measured: %#v", state["job"])
			}
		})
	}
}

func TestStatePollingRestoresSnapshotDuringRestartGap(t *testing.T) {
	for _, csv := range []bool{false, true} {
		t.Run(map[bool]string{false: "no CSV yet", true: "only totals available"}[csv], func(t *testing.T) {
			a := newPollingApp(t)
			holdPollingLock(t, a)
			if err := a.writeState(pollingJob(a)); err != nil {
				t.Fatal(err)
			}
			writePollingSnapshot(t, a, pollingRunID, []map[string]any{pollingSample(10)})
			if csv {
				writeSavedRun(t, a.results, pollingRunID, "write-verify", 10, 15, false)
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
			defer srv.Close()
			a.metricsURL = srv.URL
			for i := 0; i < 2; i++ {
				state := pollingState(t, a)
				metrics := mapValue(state["metrics"])
				chart := pollingRows(t, metrics, "chart_samples")
				if len(chart) != 1 || chart[0]["cumulative_ops"] != 10.0 || metrics["run_id"] != pollingRunID || mapValue(state["job"])["status"] != jobRunning {
					t.Fatalf("restart gap lost saved measurements: %#v", state)
				}
				rows := pollingRows(t, metrics, "samples")
				if len(rows) != 1 || rows[0]["cumulative_ops"] != 10.0 {
					t.Fatalf("inactive totals replaced the retained live sample: %#v", metrics)
				}
			}
		})
	}
}
