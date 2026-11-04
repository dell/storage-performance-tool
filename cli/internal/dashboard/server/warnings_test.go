package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestBuildRunWarningsFleetPartialAndNodeGap(t *testing.T) {
	now := time.Date(2026, 10, 30, 22, 0, 0, 0, time.UTC)
	metrics := map[string]any{
		keyRunID:        "mt-dashboard-20261030T120000-abcd",
		keyIsActive:     true,
		"fleet_partial": true,
		keyNodesCount:   3.0,
		keyNodesPresent: []string{"worker-a:1099"},
		keyContributors: []string{"local", "worker-a:1099"},
		keySamples: []map[string]any{
			{keyCountFailed: 2.0, keyTimestamp: now.Add(-30 * time.Second).Format(time.RFC3339)},
		},
		keyUpdatedAt: now.Add(-30 * time.Second).Format(time.RFC3339),
		"nodes": []map[string]any{
			{keyHost: "node-a", keyStatus: "reporting", keySampleTS: now.Add(-1 * time.Second).Format(time.RFC3339), metricFailed: 0.0},
			{keyHost: "node-b", keyStatus: "unavailable"},
			{keyHost: "node-c", keyStatus: "reporting", keySampleTS: now.Add(-45 * time.Second).Format(time.RFC3339), metricFailed: 4.0},
		},
	}
	list := buildRunWarnings(metrics, now)
	codes := map[string]int{}
	for _, w := range list {
		codes[w.Code]++
		if w.Observed == "" {
			t.Fatalf("empty observed text: %#v", w)
		}
		// AC4: must not invent a root cause phrase.
		low := w.Observed
		for _, bad := range []string{"because", "network failed", "crashed", "disk full"} {
			if strings.Contains(strings.ToLower(low), bad) {
				t.Errorf("warning claims a cause (%q): %s", bad, w.Observed)
			}
		}
	}
	if codes[warnFleetPartial] != 1 || codes[warnNodeNoData] != 1 || codes[warnSampleDelayed] < 1 || codes[warnFailedOps] < 2 {
		t.Fatalf("unexpected warning codes: %#v from %#v", codes, list)
	}
	if metrics[keyDataStatus] != dataDelayed {
		t.Fatalf("fleet data_status=%v, want delayed", metrics[keyDataStatus])
	}
	nodes := metrics["nodes"].([]map[string]any)
	if nodes[1][keyDataStatus] != dataNoData || nodes[2][keyDataStatus] != dataDelayed || nodes[0][keyDataStatus] != dataPresent {
		t.Fatalf("node data_status: %#v", nodes)
	}
}

func TestBuildRunWarningsEmptySamplesIsGapNotZero(t *testing.T) {
	now := time.Now().UTC()
	metrics := map[string]any{
		keyIsActive: true,
		keySamples:  []map[string]any{},
	}
	list := buildRunWarnings(metrics, now)
	if len(list) != 1 || list[0].Code != warnMetricsGap {
		t.Fatalf("want metrics_gap only, got %#v", list)
	}
	if metrics[keyDataStatus] != dataNoData {
		t.Fatalf("data_status=%v", metrics[keyDataStatus])
	}
	if !strings.Contains(strings.ToLower(list[0].Observed), "not a measured value of zero") {
		t.Fatalf("gap text should distinguish zero: %s", list[0].Observed)
	}
}

func TestLiveMetricsPassesFleetCompletenessAndWarnings(t *testing.T) {
	stamp := time.Now().UTC().Truncate(time.Second)
	entry := map[string]any{
		"sample_ts": stamp.Format(time.RFC3339), "test_state": 1, "step_id": "s", "scope": "fleet", "role": "aggregate",
		"op_type": "CREATE", "elapsed_time_seconds": 2.0, "partial": true, "nodes_count": 2.0,
		"nodes_present": []any{"worker-a:1099"}, "contributors_present": []any{"local"},
		"operations": map[string]any{"success_count": 10.0, "failed_count": 1.0, "success_rate_last": 5.0},
		"bandwidth":  map[string]any{"bytes_total": 10.0 * 1048576, "bytes_rate_last": 5.0 * 1048576},
		"timing":     map[string]any{"latency": map[string]any{"count": 10.0, "p50_us": 40.0}},
	}
	body, _ := json.Marshal([]map[string]any{entry})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) }))
	defer srv.Close()
	a := &app{metricsURL: srv.URL, stateDir: t.TempDir()}
	got := a.liveMetrics(map[string]any{"started_epoch": float64(stamp.Add(-time.Minute).Unix()), keyRunID: "mt-dashboard-20261030T120000-warn"})
	if got == nil || got["fleet_partial"] != true {
		t.Fatalf("fleet_partial missing: %#v", got)
	}
	if got[keyNodesCount] != 2.0 {
		t.Fatalf("nodes_count=%v", got[keyNodesCount])
	}
	warns, _ := got[keyWarnings].([]map[string]any)
	if len(warns) == 0 {
		t.Fatalf("expected warnings on partial fleet: %#v", got)
	}
	foundPartial := false
	for _, w := range warns {
		if str(w[keyCode]) == warnFleetPartial {
			foundPartial = true
		}
	}
	if !foundPartial {
		t.Fatalf("fleet_partial warning missing: %#v", warns)
	}
	path := filepath.Join(a.stateDir, "mt-dashboard-20261030T120000-warn.warnings.json")
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("warnings not persisted: %v", err)
	}
}

func TestZeroRateStillPresentNotNoData(t *testing.T) {
	now := time.Now().UTC()
	metrics := map[string]any{
		keyIsActive:  true,
		keyUpdatedAt: now.Format(time.RFC3339),
		keySamples:   []map[string]any{{keyCountFailed: 0.0, keyIntervalIOPS: 0.0, keyTimestamp: now.Format(time.RFC3339)}},
		"nodes": []map[string]any{
			{keyHost: "node-a", keyStatus: "reporting", keySampleTS: now.Format(time.RFC3339), metricIOPS: 0.0, metricFailed: 0.0},
		},
	}
	list := buildRunWarnings(metrics, now)
	for _, w := range list {
		if w.Code == warnMetricsGap || w.Code == warnNodeNoData {
			t.Fatalf("measured zeros must not become gap/no_data: %#v", list)
		}
	}
	if metrics[keyDataStatus] != dataPresent {
		t.Fatalf("data_status=%v", metrics[keyDataStatus])
	}
	if metrics["nodes"].([]map[string]any)[0][keyDataStatus] != dataPresent {
		t.Fatalf("node data_status for measured zero: %#v", metrics["nodes"])
	}
}

func TestMergeRunWarningsDedupesAndCaps(t *testing.T) {
	const src = "live"
	prior := []runWarning{{Code: "a", Observed: "1", Source: src}, {Code: "b", Observed: "2", Source: src}}
	next := []runWarning{{Code: "a", Observed: "1", Source: src}, {Code: "c", Observed: "3", Source: src}}
	got := mergeRunWarnings(prior, next)
	if len(got) != 3 {
		t.Fatalf("len=%d want 3: %#v", len(got), got)
	}
	codes := map[string]bool{}
	for _, w := range got {
		codes[w.Code] = true
	}
	if !codes["a"] || !codes["b"] || !codes["c"] {
		t.Fatalf("codes %#v", codes)
	}
}

func TestSaveRunWarningsMergesAcrossCalls(t *testing.T) {
	dir := t.TempDir()
	a := &app{stateDir: dir}
	runID := "mt-dashboard-20261030T120000-merge"
	const src = "live"
	a.saveRunWarnings(runID, []runWarning{{Code: warnFleetPartial, Observed: "first", Source: src}})
	a.saveRunWarnings(runID, []runWarning{
		{Code: warnFleetPartial, Observed: "first", Source: src},
		{Code: warnFailedOps, Observed: "second", Source: src},
	})
	got := a.loadRunWarningStructs(runID)
	if len(got) != 2 {
		t.Fatalf("want 2 merged warnings, got %#v", got)
	}
	codes := map[string]bool{}
	for _, w := range got {
		codes[w.Code] = true
	}
	if !codes[warnFleetPartial] || !codes[warnFailedOps] {
		t.Fatalf("codes %#v", codes)
	}
}

func TestMergeRunWarningsDoesNotLetDelayFloodEvictOthers(t *testing.T) {
	const src = "live"
	prior := make([]runWarning, 0, 20)
	for i := 0; i < 20; i++ {
		prior = append(prior, runWarning{Code: "stable-" + strconv.Itoa(i), Observed: "x", Source: src})
	}
	next := make([]runWarning, 0, maxSavedWarnings+5)
	for i := 0; i < maxSavedWarnings+5; i++ {
		next = append(next, runWarning{
			Code:     warnSampleDelayed,
			Observed: "Latest live metrics timestamp is T (" + strconv.Itoa(i) + "s ago).",
			Source:   src,
			Metric:   "updated_at",
		})
	}
	got := mergeRunWarnings(prior, next)
	codes := map[string]bool{}
	for _, w := range got {
		codes[w.Code] = true
	}
	for i := 0; i < 20; i++ {
		if !codes["stable-"+strconv.Itoa(i)] {
			t.Fatalf("stable-%d evicted by delay flood; len=%d got=%#v", i, len(got), got)
		}
	}
	if !codes[warnSampleDelayed] {
		t.Fatalf("missing delay warning: %#v", got)
	}
	if len(got) != 21 {
		t.Fatalf("len=%d want 21 (20 stable + 1 coalesced delay)", len(got))
	}
	var delayObserved string
	for _, w := range got {
		if w.Code == warnSampleDelayed {
			delayObserved = w.Observed
		}
	}
	wantObs := "Latest live metrics timestamp is T (" + strconv.Itoa(maxSavedWarnings+4) + "s ago)."
	if delayObserved != wantObs {
		t.Fatalf("delay Observed not refreshed to latest age: %q", delayObserved)
	}
}
