package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const (
	fixtureDashboard = "mt-dashboard-20261021T170103-518aaa08/dashboard-20261021.170103.600"
	fixtureBaseline  = "cli-baseline/base-20261020.100000.000"
	fixtureRegressed = "cli-regressed/slow-20261020.110000.000"
	fixtureCorrupt   = "cli-corrupt/bad-20261020.120000.000"
	fixtureThreads   = "cli-threads/wide-20261020.150000.000"
	fixturePartial   = "cli-partial/partial-20261020.130000.000"
	fixtureMalformed = "cli-malformed/broken-20261020.140000.000"
)

// newCatalogApp serves a copy of testdata/catalog/results so tests can add files.
func newCatalogApp(t *testing.T) *app {
	t.Helper()
	root := filepath.Join(t.TempDir(), "results")
	if err := os.CopyFS(root, os.DirFS(filepath.Join("testdata", "catalog", "results"))); err != nil {
		t.Fatal(err)
	}
	return &app{root: root, home: root, port: "8000", results: root, catalogRoot: root, stateDir: t.TempDir()}
}

func catalogGET(t *testing.T, a *app, handler http.HandlerFunc, target string, want int) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, target, nil)
	req.Host = "127.0.0.1:8000"
	rec := httptest.NewRecorder()
	handler(rec, req)
	if rec.Code != want {
		t.Fatalf("%s: status %d, want %d; body %s", target, rec.Code, want, rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "CANARY") {
		t.Fatalf("%s: response contains a credential value from spt_run_params.json", target)
	}
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func runsByID(t *testing.T, body map[string]any) map[string]map[string]any {
	t.Helper()
	out := map[string]map[string]any{}
	for _, r := range body["runs"].([]any) {
		run := r.(map[string]any)
		out[run["id"].(string)] = run
	}
	return out
}

func TestCatalogListsDashboardAndCLIRunsAndToleratesBadFolders(t *testing.T) {
	a := newCatalogApp(t)
	// A symlinked run must not be followed.
	if err := os.Symlink(filepath.Join(a.catalogRoot, "cli-baseline"), filepath.Join(a.catalogRoot, "linked")); err != nil {
		t.Fatal(err)
	}
	body := catalogGET(t, a, a.handleCatalog, "/api/catalog", 200)
	runs := runsByID(t, body)
	if len(runs) != 7 {
		t.Fatalf("got %d runs, want 7: %v", len(runs), keys(runs))
	}
	if _, ok := runs["linked/base-20261020.100000.000"]; ok {
		t.Error("catalog followed a symlink")
	}
	if _, ok := runs["not-a-run"]; ok {
		t.Error("a folder without SPT results was listed as a run")
	}
	want := map[string][3]string{
		fixtureDashboard: {"dashboard", "complete", "passed"},
		fixtureBaseline:  {"cli", "complete", "passed"},
		fixtureRegressed: {"cli", "complete", "failed"},
		fixtureCorrupt:   {"cli", "complete", "failed"},
		fixturePartial:   {"cli", "partial", "incomplete"},
		fixtureMalformed: {"cli", "malformed", "not_recorded"},
	}
	for id, w := range want {
		r := runs[id]
		if r == nil {
			t.Errorf("missing %s", id)
			continue
		}
		if r["source"] != w[0] || r["status"] != w[1] || r["integrity"] != w[2] {
			t.Errorf("%s: source/status/integrity = %v/%v/%v, want %v", id, r["source"], r["status"], r["integrity"], w)
		}
	}
	d := runs[fixtureDashboard]
	if d["endpoint_host"] != "s3.example.test" || d["object_size"] != "1MiB" || d["threads"] != 2.0 || d["host_count"] != 2.0 || d["engine_version"] != "5.16.1" {
		t.Errorf("dashboard run settings: %#v", d)
	}
	if len(runs[fixturePartial]["issues"].([]any)) == 0 || len(runs[fixtureMalformed]["issues"].([]any)) == 0 {
		t.Error("partial and malformed runs should explain their issues")
	}
	order := map[string]int{}
	for i, r := range body["runs"].([]any) {
		order[r.(map[string]any)["id"].(string)] = i
	}
	if !(order[fixtureDashboard] < order[fixtureThreads] && order[fixtureThreads] < order[fixtureRegressed] && order[fixtureRegressed] < order[fixtureBaseline]) {
		t.Errorf("runs should be newest first: %v", order)
	}
	if body["read_only"] != true {
		t.Error("catalog must report read_only")
	}
}

func keys(m map[string]map[string]any) []string {
	out := []string{}
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestCatalogFilters(t *testing.T) {
	a := newCatalogApp(t)
	for target, want := range map[string]int{
		"/api/catalog?source=dashboard": 1,
		"/api/catalog?source=cli":       6,
		"/api/catalog?status=failed":    2,
		"/api/catalog?status=partial":   1,
		"/api/catalog?q=slow":           1,
		"/api/catalog?workload=mock":    0,
	} {
		if got := len(catalogGET(t, a, a.handleCatalog, target, 200)["runs"].([]any)); got != want {
			t.Errorf("%s: %d runs, want %d", target, got, want)
		}
	}
}

func TestCatalogRunDetailSeriesAndCache(t *testing.T) {
	a := newCatalogApp(t)
	body := catalogGET(t, a, a.handleCatalogRun, "/api/catalog/run?id="+fixtureBaseline, 200)
	series := body["series"].(map[string]any)["CREATE"].([]any)
	if len(series) != 6 || series[0].(map[string]any)["t"] != 0.0 || series[5].(map[string]any)["t"] != 10.0 {
		t.Fatalf("series should start at 0 s and cover 10 s: %#v", series)
	}
	phases := body["run"].(map[string]any)["phases"].([]any)
	if len(phases) != 2 || phases[0].(map[string]any)["key"] != "CREATE" || phases[1].(map[string]any)["key"] != "VERIFY" {
		t.Fatalf("phases: %#v", phases)
	}
	if phases[0].(map[string]any)["p99_latency_us"] != 9000.0 {
		t.Errorf("p99 from totals: %#v", phases[0])
	}
	catalogGET(t, a, a.handleCatalogRun, "/api/catalog/run?id=../etc", 404)
	catalogGET(t, a, a.handleCatalogRun, "/api/catalog/run?id=/"+fixtureBaseline, 404)
	catalogGET(t, a, a.handleCatalogRun, "/api/catalog/run?id=not-a-run", 404)

	// A new file in a run folder invalidates its cached entry.
	dir := filepath.Join(a.catalogRoot, filepath.FromSlash(fixturePartial))
	totals, _ := os.ReadFile(filepath.Join(a.catalogRoot, filepath.FromSlash(fixtureBaseline), "base-001-20261020.100000.000-create.metrics.total.csv"))
	if err := os.WriteFile(filepath.Join(dir, "partial-001-20261020.130000.000-create.metrics.total.csv"), totals, 0600); err != nil {
		t.Fatal(err)
	}
	run := catalogGET(t, a, a.handleCatalogRun, "/api/catalog/run?id="+fixturePartial, 200)["run"].(map[string]any)
	if len(run["phases"].([]any)) != 1 {
		t.Errorf("cache was not refreshed after a new totals file: %#v", run["phases"])
	}
	// Removed folders drop out of the cache.
	if err := os.RemoveAll(dir); err != nil {
		t.Fatal(err)
	}
	catalogGET(t, a, a.handleCatalogRun, "/api/catalog/run?id="+fixturePartial, 404)
}

func TestCatalogRejectsForeignHost(t *testing.T) {
	a := newCatalogApp(t)
	for _, h := range []http.HandlerFunc{a.handleCatalog, a.handleCatalogRun, a.handleCatalogCompare} {
		req := httptest.NewRequest(http.MethodGet, "/api/catalog", nil)
		req.Host = "attacker.example:8000"
		rec := httptest.NewRecorder()
		h(rec, req)
		if rec.Code != 403 {
			t.Errorf("foreign Host header got %d, want 403", rec.Code)
		}
	}
}

func TestCatalogNeverDecodesCredentials(t *testing.T) {
	a := newCatalogApp(t)
	run := a.readCatalogRun(filepath.Join(a.catalogRoot, filepath.FromSlash(fixtureDashboard)), fixtureDashboard)
	b, _ := json.Marshal(run)
	for _, bad := range []string{"CANARY", "fixture-bucket", "some/path", "AccessKey", "SecretKey"} {
		if strings.Contains(string(b), bad) {
			t.Errorf("catalog entry contains %q", bad)
		}
	}
}

func TestCatalogCapsAndMissingRoot(t *testing.T) {
	a := &app{port: "8000", catalogRoot: filepath.Join(t.TempDir(), "missing")}
	runs, truncated := a.scanCatalog()
	if len(runs) != 0 || truncated {
		t.Fatalf("missing root: %d runs, truncated=%v", len(runs), truncated)
	}
	if got := (&app{results: "/home/u/results"}).catalogDir(); got != "/home/u/results" {
		t.Errorf("default catalog root = %q", got)
	}
	points := make([]seriesPoint, 5000)
	for i := range points {
		v := float64(i)
		points[i] = seriesPoint{T: v, IOPS: &v}
	}
	if got := downsample(points, catalogMaxPoints); len(got) != catalogMaxPoints || got[len(got)-1].T != 4999 {
		t.Errorf("downsample: %d points ending at %v", len(got), got[len(got)-1].T)
	}
	if got := downsample(points[:10], catalogMaxPoints); len(got) != 10 {
		t.Errorf("short series should be unchanged")
	}
}

func TestEndpointHostAndStepName(t *testing.T) {
	for in, want := range map[string]string{"https://user:pw@s3.example:9000/p?q=1": "s3.example", "": "", "not a url": "", "http://[::1]:9000": "::1"} {
		if got := endpointHost(in); got != want {
			t.Errorf("endpointHost(%q)=%q want %q", in, got, want)
		}
	}
	if got := stepName("label-001-20261023.202619.791-create"); got != "create" {
		t.Errorf("stepName: %q", got)
	}
}

func TestCatalogDisplayRoot(t *testing.T) {
	home := t.TempDir()
	a := &app{home: home, results: filepath.Join(home, "results")}
	if got := a.catalogDisplayRoot(); got != "~/results" {
		t.Fatalf("default root = %q", got)
	}
	a.catalogRoot = filepath.Join(home, "a", "b")
	if got := a.catalogDisplayRoot(); got != "~/a/b" {
		t.Fatalf("nested root = %q", got)
	}
	a.catalogRoot = home
	if got := a.catalogDisplayRoot(); got != "~" {
		t.Fatalf("home root = %q", got)
	}
	outside := filepath.Join(filepath.Dir(home), "..elsewhere")
	a.catalogRoot = outside
	if got := a.catalogDisplayRoot(); got != outside {
		t.Fatalf("outside root = %q, want %q", got, outside)
	}
}
