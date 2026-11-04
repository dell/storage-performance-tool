package server

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Minimal interval data isolates source selection from the parser's own tests.
func writeStateInterval(t *testing.T, dir, phase string, ops int) string {
	t.Helper()
	path := filepath.Join(dir, "state-create.metrics.csv")
	body := fmt.Sprintf("DateTimeISO8601,OpType,CountSucc,CountFail,TPAvg[op/s],BWAvg[MiB/s]\n2026-10-01T00:00:01Z,%s,%d,0,5,2\n", phase, ops)
	if err := os.WriteFile(path, []byte(body), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func unavailablePollingServer(t *testing.T, a *app) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }))
	t.Cleanup(srv.Close)
	a.metricsURL = srv.URL
}

func TestStatePollingCSVAndMemoryFallback(t *testing.T) {
	for _, test := range []struct {
		name, phase         string
		sampleOps, chartOps float64
	}{
		{"measured CSV chart", "CREATE", 15, 15},
		{"CSV sample without a plottable phase", "unrecognized", 15, 10},
		{"CSV has no measured rows yet", "empty", 10, 10},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := newPollingApp(t)
			holdPollingLock(t, a)
			a.activeJob = pollingJob(a)
			unavailablePollingServer(t, a)
			writeSavedRun(t, a.results, pollingRunID, "write-verify", 10, 99, false)
			path := writeStateInterval(t, filepath.Join(a.results, pollingRunID, "dashboard-20261023.214759.259"), test.phase, 15)
			if test.phase == "empty" {
				if err := os.WriteFile(path, []byte("DateTimeISO8601,OpType,CountSucc,CountFail,TPAvg[op/s],BWAvg[MiB/s]\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			// Simulate measurements retained despite a failed snapshot write.
			a.liveRunID, a.liveSamples = pollingRunID, []map[string]any{pollingSample(10)}
			state := pollingState(t, a)
			metrics := mapValue(state["metrics"])
			rows := pollingRows(t, metrics, "samples")
			chart := pollingRows(t, metrics, "chart_samples")
			if len(rows) != 1 || rows[0]["cumulative_ops"] != test.sampleOps || len(chart) != 1 || chart[0]["cumulative_ops"] != test.chartOps || metrics["is_active"] != true || metrics["is_aggregate"] != false {
				t.Fatalf("CSV samples or chart fallback were lost: %#v", state)
			}
		})
	}
}

func TestStatePollingMemoryHistoryWinsOverStaleSnapshot(t *testing.T) {
	a := newPollingApp(t)
	holdPollingLock(t, a)
	a.activeJob = pollingJob(a)
	unavailablePollingServer(t, a)
	writePollingSnapshot(t, a, pollingRunID, []map[string]any{pollingSample(3)})
	a.liveRunID, a.liveSamples = pollingRunID, []map[string]any{pollingSample(10)}
	state := pollingState(t, a)
	metrics := mapValue(state["metrics"])
	rows := pollingRows(t, metrics, "samples")
	if len(rows) != 1 || rows[0]["cumulative_ops"] != 10.0 {
		t.Fatalf("disk fallback displaced newer in-memory measurements: %#v", metrics)
	}
}

func TestStatePollingRejectsUnrelatedOrUnusableHistory(t *testing.T) {
	for _, test := range []struct{ name, snapshot string }{
		{"another run only", ""}, {"broken JSON", "{"}, {"empty history", "[]"},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := newPollingApp(t)
			holdPollingLock(t, a)
			a.activeJob = pollingJob(a)
			unavailablePollingServer(t, a)
			a.liveRunID, a.liveSamples = "mt-another-run", []map[string]any{pollingSample(999)}
			writePollingSnapshot(t, a, "mt-another-run", a.liveSamples)
			if test.snapshot != "" {
				if err := os.WriteFile(filepath.Join(a.stateDir, pollingRunID+".samples.json"), []byte(test.snapshot), 0600); err != nil {
					t.Fatal(err)
				}
			}
			state := pollingState(t, a)
			metrics := mapValue(state["metrics"])
			if metrics["run_id"] != pollingRunID || metrics["is_active"] != true || len(pollingRows(t, metrics, "samples")) != 0 || len(pollingRows(t, metrics, "chart_samples")) != 0 {
				t.Fatalf("unrelated or invalid measurements appeared in the active run: %#v", state)
			}
		})
	}
}

func TestStatePollingSnapshotFallbackYieldsToLiveRecovery(t *testing.T) {
	a := newPollingApp(t)
	holdPollingLock(t, a)
	if err := a.writeState(pollingJob(a)); err != nil {
		t.Fatal(err)
	}
	writePollingSnapshot(t, a, pollingRunID, []map[string]any{pollingSample(10)})
	// A different run's cache must not displace the matching disk snapshot.
	a.liveRunID, a.liveSamples = "mt-other-cached", []map[string]any{pollingSample(999)}
	var available atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !available.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(pollingEntry("VERIFY", 20, 1))
	}))
	defer srv.Close()
	a.metricsURL = srv.URL
	for _, step := range []struct {
		live  bool
		ops   float64
		count int
	}{{false, 10, 1}, {true, 20, 2}, {false, 20, 2}} {
		available.Store(step.live)
		state := pollingState(t, a)
		metrics := mapValue(state["metrics"])
		rows, chart := pollingRows(t, metrics, "samples"), pollingRows(t, metrics, "chart_samples")
		if len(rows) != 1 || rows[0]["cumulative_ops"] != step.ops || len(chart) != step.count || chart[0]["cumulative_ops"] != 10.0 {
			t.Fatalf("recovery lost the saved prefix or used another run: %#v", state)
		}
	}
}

func TestStatePollingConcurrentReadersKeepOneMeasurement(t *testing.T) {
	a := newPollingApp(t)
	holdPollingLock(t, a)
	a.activeJob = pollingJob(a)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(pollingEntry("CREATE", 10, 0))
	}))
	defer srv.Close()
	a.metricsURL = srv.URL
	const readers = 8
	var wg sync.WaitGroup
	start := make(chan struct{})
	responses := make(chan *httptest.ResponseRecorder, readers)
	for i := 0; i < readers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			rec := httptest.NewRecorder()
			a.handleState(rec, httptest.NewRequest(http.MethodGet, "/api/state", nil))
			responses <- rec
		}()
	}
	close(start)
	wg.Wait()
	close(responses)
	for rec := range responses {
		var state map[string]any
		if rec.Code != http.StatusOK || json.Unmarshal(rec.Body.Bytes(), &state) != nil {
			t.Fatalf("concurrent reader failed: %d %s", rec.Code, rec.Body.String())
		}
		metrics := mapValue(state["metrics"])
		if metrics["run_id"] != pollingRunID || len(pollingRows(t, metrics, "chart_samples")) != 1 {
			t.Fatalf("concurrent polling duplicated or mixed measurements: %#v", metrics)
		}
	}
	if samples := a.savedLiveSamples(pollingRunID); len(samples) != 1 || samples[0]["cumulative_ops"] != 10.0 {
		t.Fatalf("concurrent polls damaged saved history: %#v", samples)
	}
}

func TestStatePollingTerminalJobUsesSavedDataWithoutLiveRequest(t *testing.T) {
	for _, status := range []string{jobCompleted, jobFailed, jobInterrupted} {
		t.Run(status, func(t *testing.T) {
			a := newPollingApp(t)
			job := pollingJob(a)
			job["status"] = status
			if err := a.writeState(job); err != nil {
				t.Fatal(err)
			}
			writeSavedRun(t, a.results, pollingRunID, "write-verify", 10, 8, false)
			var calls atomic.Int32
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				_ = json.NewEncoder(w).Encode(pollingEntry("CREATE", 999, 0))
			}))
			defer srv.Close()
			a.metricsURL = srv.URL
			state := pollingState(t, a)
			metrics := mapValue(state["metrics"])
			rows := pollingRows(t, metrics, "samples")
			if calls.Load() != 0 || mapValue(state["job"])["status"] != status || metrics["is_active"] != false || len(rows) != 1 || rows[0]["cumulative_ops"] != 8.0 {
				t.Fatalf("terminal state used current telemetry instead of saved data: calls=%d state=%#v", calls.Load(), state)
			}
		})
	}
}

func TestStatePollingPersistenceFailureDoesNotHideLiveData(t *testing.T) {
	a := newPollingApp(t)
	holdPollingLock(t, a)
	a.activeJob = pollingJob(a)
	if err := a.writeState(a.activeJob); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(a.stateFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(a.stateFile+".tmp", 0700); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(pollingEntry("VERIFY", 20, 1))
	}))
	defer srv.Close()
	a.metricsURL = srv.URL
	state := pollingState(t, a)
	if mapValue(state["job"])["phase"] != "Verify" || pollingRows(t, mapValue(state["metrics"]), "samples")[0]["cumulative_ops"] != 20.0 {
		t.Fatalf("best-effort state persistence hid live data: %#v", state)
	}
	after, err := os.ReadFile(a.stateFile)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatalf("failed save damaged previous state: %q, %v", after, err)
	}
	if err := os.Remove(a.stateFile + ".tmp"); err != nil {
		t.Fatal(err)
	}
	state = pollingState(t, a)
	if a.readStateFile()["phase"] != "Verify" || len(pollingRows(t, mapValue(state["metrics"]), "chart_samples")) != 1 {
		t.Fatalf("next poll failed to recover or duplicated the measurement: %#v", state)
	}
}

func TestStatePollingElapsedTimeBoundaries(t *testing.T) {
	for _, test := range []struct {
		name    string
		started any
		want    float64
	}{
		{"future start", float64(time.Now().Add(time.Hour).Unix()), 0},
		{"missing start", nil, 17},
	} {
		t.Run(test.name, func(t *testing.T) {
			a := newPollingApp(t)
			holdPollingLock(t, a)
			a.activeJob = pollingJob(a)
			a.activeJob["started_epoch"], a.activeJob["elapsed_seconds"] = test.started, 17.0
			unavailablePollingServer(t, a)
			state := pollingState(t, a)
			if mapValue(state["job"])["elapsed_seconds"] != test.want || mapValue(state["job"])["status"] != jobRunning {
				t.Fatalf("elapsed time or externally owned running state changed: %#v", state["job"])
			}
		})
	}
}
