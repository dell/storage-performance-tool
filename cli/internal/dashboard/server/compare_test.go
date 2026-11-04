package server

import (
	"strings"
	"testing"
)

func deltaFor(t *testing.T, body map[string]any, run, phase, metric string) map[string]any {
	t.Helper()
	for _, d := range body["deltas"].([]any) {
		m := d.(map[string]any)
		if m["run_id"] == run && m["phase"] == phase && m["metric"] == metric {
			return m
		}
	}
	t.Fatalf("no delta for %s %s %s", run, phase, metric)
	return nil
}

func TestCompareFlagsRegressionsConservatively(t *testing.T) {
	a := newCatalogApp(t)
	q := "/api/catalog/compare?id=" + fixtureBaseline + "&id=" + fixtureRegressed + "&id=" + fixtureCorrupt + "&id=" + fixtureThreads
	body := catalogGET(t, a, a.handleCatalogCompare, q, 200)
	rule := body["rule"].(map[string]any)
	if rule["label"] != "Threshold check, not statistical significance" || rule["threshold_pct"] != 10.0 || rule["latency_floor_us"] != 500.0 {
		t.Fatalf("rule: %#v", rule)
	}
	if body["baseline"] != fixtureBaseline {
		t.Errorf("baseline defaults to the first run: %v", body["baseline"])
	}
	cases := []struct{ run, phase, metric, flag string }{
		{fixtureRegressed, "CREATE", "iops", "regressed"},           // -20%
		{fixtureRegressed, "CREATE", "p50_latency_us", "regressed"}, // +600 µs, +30%
		{fixtureRegressed, "CREATE", "p99_latency_us", "within"},    // +100 µs is under the floor
		{fixtureRegressed, "VERIFY", "failed_ops", "regressed"},
		{fixtureRegressed, "VERIFY", "iops", "within"}, // -1.5%
		{fixtureCorrupt, "VERIFY", "corrupt_ops", "regressed"},
		{fixtureCorrupt, "CREATE", "iops", "within"},
		{fixtureThreads, "CREATE", "iops", "not_comparable"},
	}
	for _, c := range cases {
		if got := deltaFor(t, body, c.run, c.phase, c.metric)["flag"]; got != c.flag {
			t.Errorf("%s %s %s: flag %v, want %s", c.run, c.phase, c.metric, got, c.flag)
		}
	}
	if d := deltaFor(t, body, fixtureRegressed, "CREATE", "iops"); d["delta_pct"] != -20.0 {
		t.Errorf("delta_pct = %v, want -20", d["delta_pct"])
	}
	if reason := deltaFor(t, body, fixtureThreads, "CREATE", "iops")["reason"].(string); !strings.Contains(reason, "threads differs (2 vs 8)") {
		t.Errorf("not-comparable reason: %q", reason)
	}
	diffs := body["settings_differences"].(map[string]any)
	if len(diffs[fixtureThreads].([]any)) != 1 || len(diffs[fixtureRegressed].([]any)) != 0 {
		t.Errorf("settings differences: %#v", diffs)
	}
	if len(body["series"].(map[string]any)) != 4 {
		t.Error("each compared run should carry its series")
	}
}

func TestCompareThresholdIsConfigurable(t *testing.T) {
	a := newCatalogApp(t)
	q := "/api/catalog/compare?id=" + fixtureBaseline + "&id=" + fixtureRegressed + "&threshold_pct=25&latency_floor_us=50"
	body := catalogGET(t, a, a.handleCatalogCompare, q, 200)
	if got := deltaFor(t, body, fixtureRegressed, "CREATE", "iops")["flag"]; got != "within" {
		t.Errorf("-20%% with a 25%% threshold: %v", got)
	}
	if got := deltaFor(t, body, fixtureRegressed, "CREATE", "p50_latency_us")["flag"]; got != "regressed" {
		t.Errorf("+30%% with a 25%% threshold: %v", got)
	}
	// The baseline can be any compared run; improvements are reported.
	body = catalogGET(t, a, a.handleCatalogCompare, "/api/catalog/compare?id="+fixtureBaseline+"&id="+fixtureRegressed+"&baseline="+fixtureRegressed, 200)
	if got := deltaFor(t, body, fixtureBaseline, "CREATE", "iops")["flag"]; got != "improved" {
		t.Errorf("baseline swap: %v", got)
	}
	a.settings = defaultSettings()
	a.settings.Compare.MinOps = 1000
	body = catalogGET(t, a, a.handleCatalogCompare, "/api/catalog/compare?id="+fixtureBaseline+"&id="+fixtureRegressed, 200)
	if got := deltaFor(t, body, fixtureRegressed, "CREATE", "iops")["flag"]; got != "insufficient" {
		t.Errorf("min_ops from settings: %v", got)
	}
}

func TestCompareRejectsBadRequests(t *testing.T) {
	a := newCatalogApp(t)
	for q, want := range map[string]int{
		"/api/catalog/compare?id=" + fixtureBaseline:                                                             400,
		"/api/catalog/compare?id=" + fixtureBaseline + "&id=" + fixtureBaseline:                                  400,
		"/api/catalog/compare?id=" + fixtureBaseline + "&id=missing/run":                                         404,
		"/api/catalog/compare?id=" + fixtureBaseline + "&id=" + fixtureRegressed + "&threshold_pct=0":            400,
		"/api/catalog/compare?id=" + fixtureBaseline + "&id=" + fixtureRegressed + "&threshold_pct=abc":          400,
		"/api/catalog/compare?id=" + fixtureBaseline + "&id=" + fixtureRegressed + "&min_ops=0":                  400,
		"/api/catalog/compare?id=" + fixtureBaseline + "&id=" + fixtureRegressed + "&baseline=" + fixtureCorrupt: 400,
		"/api/catalog/compare?id=a&id=b&id=c&id=d&id=e&id=f&id=g":                                                400,
	} {
		catalogGET(t, a, a.handleCatalogCompare, q, want)
	}
}

func TestCompareSettingsValidation(t *testing.T) {
	good := defaultCompareSettings()
	if err := good.validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []compareSettings{{0, 500, 10}, {101, 500, 10}, {10, -1, 10}, {10, 500, 0}} {
		if bad.validate() == nil {
			t.Errorf("accepted %+v", bad)
		}
	}
	s := defaultSettings()
	s.Compare.ThresholdPct = 0
	if s.validate() == nil {
		t.Error("dashboard settings accepted an invalid compare threshold")
	}
}

func TestCompareMissingPhaseAndData(t *testing.T) {
	v := 1.0
	base := catalogRun{ID: "b", Phases: []catalogPhase{{Key: "CREATE", Successful: 100, IOPS: &v}}}
	run := catalogRun{ID: "r", Phases: []catalogPhase{{Key: "CREATE", Successful: 100}, {Key: "DELETE", Successful: 100, IOPS: &v}}}
	deltas := compareRuns(base, []catalogRun{base, run}, defaultCompareSettings())
	flags := map[string]string{}
	for _, d := range deltas {
		flags[d.Phase+"/"+d.Metric] = d.Flag
	}
	if flags["CREATE/iops"] != "no_data" || flags["DELETE/iops"] != "not_comparable" || flags["CREATE/corrupt_ops"] != "no_data" {
		t.Errorf("flags: %#v", flags)
	}
}
