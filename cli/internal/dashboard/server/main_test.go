package server

import (
	"encoding/csv"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestValidateOptionsBounds(t *testing.T) {
	a := &app{} // no settings file: the built-in default limits apply
	for _, good := range []options{
		{"", 100, "1MiB", 1, 60, ""},
		{"guided", 320, "16MiB", 2, 600, ""},
		{"expert", 1, "1KiB", 1, 10, ""},
		{"expert", 777, "3MiB", 13, 1234, ""},
		{"expert", 100_000, "1MiB", 64, 3600, ""},
		{"expert", 100, "1GiB", 1, 60, ""},
	} {
		if err := a.validateOptions(good); err != nil {
			t.Errorf("rejected %+v: %v", good, err)
		}
	}
	bad := []options{
		{"", 1000, "1MiB", 1, 60, ""},
		{"guided", 100, "100MiB", 1, 60, ""},
		{"guided", 100, "1MiB", 20, 60, ""},
		{"guided", 100, "1MiB", 1, 999, ""},
		{"guided", 777, "3MiB", 13, 1234, ""},
		{"admin", 100, "1MiB", 1, 60, ""},
		{"expert", 0, "1MiB", 1, 60, ""},
		{"expert", 100_001, "1KiB", 1, 60, ""},
		{"expert", 100, "1MiB", 0, 60, ""},
		{"expert", 100, "1MiB", 65, 60, ""},
		{"expert", 100, "1MiB", 1, 9, ""},
		{"expert", 100, "1MiB", 1, 3601, ""},
		{"expert", 100, "2GiB", 1, 60, ""},
		{"expert", 101, "1GiB", 1, 60, ""},
		{"expert", 100, "--help", 1, 60, ""},
	}
	for _, got := range bad {
		if a.validateOptions(got) == nil {
			t.Errorf("accepted unsafe settings: %+v", got)
		}
	}
}

func TestParseSize(t *testing.T) {
	for in, want := range map[string]int64{"1KiB": 1024, "64KiB": 65536, "3MiB": 3 << 20, "1GiB": 1 << 30} {
		if got, ok := parseSize(in); !ok || got != want {
			t.Errorf("parseSize(%q) = %d, %v; want %d", in, got, ok, want)
		}
	}
	for _, in := range []string{"", "-1MiB", "1 MiB", " 1MiB", "0KiB", "01MiB", "1TB", "1MB", "1mib", "--help", "1.5MiB", "12345678KiB"} {
		if _, ok := parseSize(in); ok {
			t.Errorf("parseSize(%q) accepted", in)
		}
	}
}

func TestRunLimitsMatchValidation(t *testing.T) {
	a := &app{}
	_, accepted, _, _ := a.guidedLimitSets()
	guided := a.limitsPayload()["guided"].(map[string]any)
	for _, size := range guided["object_sizes"].([]string) {
		if !accepted[size] {
			t.Errorf("published guided size %q is not accepted", size)
		}
	}
	if sizes := guided["object_sizes"].([]string); sizes[0] != "64KiB" || sizes[len(sizes)-1] != "16MiB" {
		t.Errorf("guided sizes not ordered by bytes: %v", sizes)
	}
}

func TestParseMetricsIntervalAndAggregate(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "create.metrics.csv")
	header := []string{"DateTimeISO8601", "OpType", "Concurrency", "NodeCount", "ConcurrencyCurr", "ConcurrencyMean", "CountSucc", "CountFail", "Size", "StepDuration[s]", "DurationSum[s]", "TPAvg[op/s]", "TPLast[op/s]", "BWAvg[MiB/s]", "BWLast[MiB/s]", "LatencyAvg[us]", "LatencyMed[us]", "LatencyQ_0.5[us]", "LatencyQ_0.99[us]"}
	row := []string{"2026-09-30T19:00:01+00:00", "CREATE", "1", "1", "1", "1", "10", "0", "10485760", "2", "1", "5", "8", "5", "8", "99", "42", "", "120"}
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := csv.NewWriter(f)
	_ = w.Write(header)
	_ = w.Write(row)
	w.Flush()
	_ = f.Close()
	a := &app{}
	interval, err := a.parseMetricsFile(path, "run", "write-verify", false, false)
	if err != nil {
		t.Fatal(err)
	}
	total, err := a.parseMetricsFile(path, "run", "write-verify", false, true)
	if err != nil {
		t.Fatal(err)
	}
	i := interval["samples"].([]map[string]any)[0]
	s := total["samples"].([]map[string]any)[0]
	if i["interval_iops"] != float64(8) || i["bandwidth_mib_s"] != float64(8) || i["p50_latency_us"] != float64(42) {
		t.Fatalf("wrong interval row: %#v", i)
	}
	if s["interval_iops"] != float64(5) || s["bandwidth_mib_s"] != float64(5) {
		t.Fatalf("wrong aggregate row: %#v", s)
	}
}

func TestLiveHistoryPersistenceAndDeduplication(t *testing.T) {
	a := &app{stateDir: t.TempDir()}
	sample := map[string]any{"timestamp": "2026-09-30T19:00:01Z", "step_id": "create", "cumulative_ops": 10, "count_failed": 0, "interval_iops": 8, "bandwidth_mib_s": 8, "p50_latency_us": 42, "op_type": "CREATE"}
	metrics := map[string]any{"run_id": "mt-isolated-test", "samples": []map[string]any{sample}}
	first := a.rememberLive(metrics)
	second := a.rememberLive(metrics)
	if len(first["chart_samples"].([]map[string]any)) != 1 || len(second["chart_samples"].([]map[string]any)) != 1 {
		t.Fatal("duplicate live measurement was retained")
	}
	a.liveRunID = ""
	a.liveSamples = nil
	recovered := a.rememberLive(map[string]any{"run_id": "mt-isolated-test", "samples": []map[string]any{}})
	rows := recovered["chart_samples"].([]map[string]any)
	if len(rows) != 1 || rows[0]["step_id"] != "create" || numberValue(rows[0], "interval_iops") != 8 {
		t.Fatalf("saved sample was not recovered: %#v", recovered)
	}
	next := map[string]any{"timestamp": "2026-09-30T19:00:02Z", "step_id": "create", "cumulative_ops": 20, "count_failed": 0, "interval_iops": 9, "bandwidth_mib_s": 9, "p50_latency_us": 40, "op_type": "CREATE"}
	grown := a.rememberLive(map[string]any{"run_id": "mt-isolated-test", "samples": []map[string]any{next}})
	latest := grown["samples"].([]map[string]any)
	if len(grown["chart_samples"].([]map[string]any)) != 2 || len(latest) != 1 || latest[0]["cumulative_ops"] != 20 {
		t.Fatalf("live history was duplicated or the latest sample was lost: %#v", grown)
	}
}

func TestRetainedLiveMetricsRemainAvailableBetweenPhases(t *testing.T) {
	sample := map[string]any{"timestamp": "2026-10-07T17:02:15Z", "op_type": "CREATE", "cumulative_ops": float64(100)}
	retained := retainedLiveMetrics("mt-dashboard-run-123", "mt-dashboard-run-123", driverWriteVerify, []map[string]any{sample})
	if retained == nil || len(retained["chart_samples"].([]map[string]any)) != 1 {
		t.Fatalf("live chart history was not retained: %#v", retained)
	}
	if got := retainedLiveMetrics("mt-dashboard-other-456", "mt-dashboard-run-123", driverWriteVerify, []map[string]any{sample}); got != nil {
		t.Fatalf("returned samples from a different run: %#v", got)
	}
}

func TestRetainedLiveMetricsKeepMockWorkload(t *testing.T) {
	sample := map[string]any{"timestamp": "2026-10-07T17:02:15Z", "op_type": "CREATE", "cumulative_ops": float64(5)}
	job := map[string]any{"run_id": "mt-dashboard-run-123", "options": map[string]any{"driver": "mock"}}
	retained := retainedLiveMetrics("mt-dashboard-run-123", "mt-dashboard-run-123", jobWorkload(job), []map[string]any{sample})
	if retained == nil || retained["workload_type"] != "mock" {
		t.Fatalf("a mock run's retained live metrics must keep the mock workload: %#v", retained)
	}
}

func TestRunOriginGate(t *testing.T) {
	a := &app{port: "8000"}
	for _, test := range []struct {
		host, origin string
		want         bool
	}{{"localhost:8000", "http://localhost:8000", true}, {"127.0.0.1:8000", "http://127.0.0.1:8000", true}, {"evil.example", "http://evil.example", false}, {"localhost:8000", "https://localhost:8000", false}, {"localhost:8000", "http://localhost:8001", false}} {
		r := httptest.NewRequest("POST", "/api/runs", nil)
		r.Host = test.host
		r.Header.Set("Origin", test.origin)
		if got := a.sameOrigin(r); got != test.want {
			t.Errorf("sameOrigin(%q,%q)=%v; want %v", test.host, test.origin, got, test.want)
		}
	}
}

func TestLiveMetricsRatesAndLatency(t *testing.T) {
	stamp := time.Now().UTC().Truncate(time.Second)
	base := map[string]any{"sample_ts": stamp.Format(time.RFC3339), "test_state": 1, "step_id": "create-step", "scope": "node", "role": "entry", "op_type": "CREATE", "elapsed_time_seconds": 2.0,
		"operations": map[string]any{"success_count": 10.0, "failed_count": 0.0, "success_rate_last": 8.0},
		"bandwidth":  map[string]any{"bytes_total": 10.0 * 1048576, "bytes_rate_last": 8.0 * 1048576},
		"timing":     map[string]any{"latency": map[string]any{"count": 10.0, "p50_us": 42.0}}}
	read := func(entries any) map[string]any {
		t.Helper()
		body, _ := json.Marshal(entries)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) }))
		defer server.Close()
		a := &app{metricsURL: server.URL}
		state := map[string]any{"started_epoch": float64(stamp.Add(-time.Minute).Unix()), "run_id": "mt-isolated-test"}
		return a.liveMetrics(state)
	}

	metrics := read(base)
	if metrics == nil {
		t.Fatal("live metric was not accepted")
	}
	sample := metrics["samples"].([]map[string]any)[0]
	if numOr(sample, "interval_iops", -1) != 8 || numOr(sample, "bandwidth_mib_s", -1) != 8 || numOr(sample, "average_iops", -1) != 5 || numOr(sample, "p50_latency_us", -1) != 42 {
		t.Fatalf("wrong measured live values: %#v", sample)
	}

	zero := map[string]any{}
	for k, v := range base {
		zero[k] = v
	}
	zero["operations"] = map[string]any{"success_count": 10.0, "failed_count": 0.0, "success_rate_last": 0.0}
	zero["bandwidth"] = map[string]any{"bytes_total": 10.0 * 1048576, "bytes_rate_last": 0.0}
	zeroSample := read(zero)["samples"].([]map[string]any)[0]
	if numOr(zeroSample, "interval_iops", -1) != 0 || numOr(zeroSample, "bandwidth_mib_s", -1) != 0 {
		t.Fatalf("real zero rate was replaced: %#v", zeroSample)
	}

	missing := map[string]any{}
	for k, v := range base {
		missing[k] = v
	}
	missing["operations"] = map[string]any{"success_count": 10.0, "failed_count": 0.0}
	missing["bandwidth"] = map[string]any{"bytes_total": 10.0 * 1048576}
	missingSample := read(missing)["samples"].([]map[string]any)[0]
	if numOr(missingSample, "interval_iops", -1) != 5 || numOr(missingSample, "bandwidth_mib_s", -1) != 5 {
		t.Fatalf("missing rates were not derived from measurements: %#v", missingSample)
	}

	noMedian := map[string]any{}
	for k, v := range base {
		noMedian[k] = v
	}
	noMedian["timing"] = map[string]any{"latency": map[string]any{"count": 10.0, "mean_us": 999.0}}
	if got := read(noMedian)["samples"].([]map[string]any)[0]["p50_latency_us"]; got != nil {
		t.Fatalf("mean latency was incorrectly used as P50: %#v", got)
	}

	old := map[string]any{}
	for k, v := range base {
		old[k] = v
	}
	old["test_state"] = 2
	old["terminal"] = true
	old["scope"] = "fleet"
	old["step_id"] = "old-create"
	if got := read(old); got != nil {
		t.Fatalf("terminal SPT sample was accepted: %#v", got)
	}
	active := map[string]any{}
	for k, v := range base {
		active[k] = v
	}
	active["op_type"] = "READ"
	active["step_id"] = "verify-step"
	combined := read([]any{old, active})
	if combined == nil || combined["phase"] != "READ" {
		t.Fatalf("active phase did not win over retained terminal data: %#v", combined)
	}

	if !strings.Contains(str(sample["source_file"]), "live metrics") {
		t.Fatalf("live source provenance missing: %#v", sample)
	}
}

// numOr reads a number like numberValue but returns def when it is missing,
// so a test can tell a real 0 from an absent value.
func numOr(m map[string]any, key string, def float64) float64 {
	if n, ok := num(m, key); ok {
		return n
	}
	return def
}
