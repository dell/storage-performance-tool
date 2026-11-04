package server

import (
	"encoding/csv"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

const csvExportRunID = "mt-dashboard-20261022T120000-1234abcd"

func newCSVExportApp(t *testing.T, totals bool) *app {
	t.Helper()
	a := newHistoryApp(t, t.TempDir())
	dir := filepath.Join(a.results, csvExportRunID, "dashboard-20261022.120000.000")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	files := map[string]string{
		"spt_run_params.json": `{"generatedAt":"2026-10-22T12:00:00Z","workloadType":"write-verify","scenarioParams":{"ObjectCount":30,"ObjectSize":"1MiB","Threads":4}}`,
		"results_summary.txt": "completed\n",
		"001-create.metrics.csv": "2026-10-22T12:00:02Z,CREATE,10,0,2,5,8,1.5,2.5,2,1048576,1,4,,,0\n" +
			"2026-10-22T12:00:04Z,CREATE,20,1,4,5,9,2,3,4,2097152,1,4,200,800,0\n" +
			"2026-10-22T12:00:06Z,CREATE,30,1,6,5,10,2.5,3.5,6,3145728,1,4,250,900,0\n",
		"002-verify.metrics.csv": "2026-10-22T12:00:08Z,READ,10,0,2,6,12,4,4.5,2,1048576,1,4,100,500,0\n" +
			"2026-10-22T12:00:10Z,READ,30,0,4,6,13,4,5,4,3145728,1,4,110,550,0\n",
		"003-delete.metrics.csv": "2026-10-22T12:00:12Z,DELETE,30,0,2,15,14,1,1.5,2,3145728,1,4,10,100,0\n",
	}
	if totals {
		files["001-create.metrics.total.csv"] = "2026-10-22T12:00:06Z,CREATE,30,1,6,5,0,2.5,0,6,3145728,1,4,200,800,0\n"
		files["002-verify.metrics.total.csv"] = "2026-10-22T12:00:10Z,READ,30,0,4,6,0,4,0,4,3145728,1,4,100,500,0\n"
		files["003-delete.metrics.total.csv"] = "2026-10-22T12:00:12Z,DELETE,30,0,2,15,0,1,0,2,3145728,1,4,10,100,0\n"
	}
	header := strings.TrimSuffix(parsingCSVHeader, "\n") + ",LatencyQ_0.5[us],LatencyQ_0.99[us],CountCorrupt\n"
	for name, body := range files {
		if strings.HasSuffix(name, ".csv") {
			body = header + body
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return a
}

func requestCSVExport(a *app, runID, query string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, "/api/runs/"+runID+"/csv"+query, nil)
	req.Host = "127.0.0.1:8000"
	req.SetPathValue("runID", runID)
	rec := httptest.NewRecorder()
	a.handleRunCSV(rec, req)
	return rec
}

func decodeCSVExport(t *testing.T, rec *httptest.ResponseRecorder, filename string) []map[string]string {
	t.Helper()
	if rec.Code != http.StatusOK {
		t.Fatalf("CSV response: %d %s", rec.Code, rec.Body.String())
	}
	for key, want := range map[string]string{
		"Content-Type": "text/csv; charset=utf-8", "Content-Disposition": `attachment; filename="` + filename + `"`,
		"X-Content-Type-Options": "nosniff", "Cache-Control": "no-store",
	} {
		if got := rec.Header().Get(key); got != want {
			t.Errorf("%s = %q, want %q", key, got, want)
		}
	}
	records, err := csv.NewReader(strings.NewReader(rec.Body.String())).ReadAll()
	if err != nil || len(records) < 2 {
		t.Fatalf("CSV records = %v, error = %v", records, err)
	}
	columns := records[0]
	if !sort.StringsAreSorted(columns) {
		t.Errorf("CSV columns are not sorted: %v", columns)
	}
	rows := make([]map[string]string, 0, len(records)-1)
	for _, record := range records[1:] {
		row := map[string]string{}
		for i, key := range columns {
			row[key] = record[i]
		}
		rows = append(rows, row)
	}
	return rows
}

func TestCSVExportSummaryAndSamples(t *testing.T) {
	a := newCSVExportApp(t, true)
	for _, query := range []string{"", "?kind=summary"} {
		rows := decodeCSVExport(t, requestCSVExport(a, csvExportRunID, query), "spt-"+csvExportRunID+"-summary.csv")
		want := []map[string]string{
			{"phase": "Create", "successful_ops": "30", "failed_ops": "1", "iops": "5", "bandwidth_mib_s": "2.5", "p50_latency_us": "200", "latency_p99_us": "800"},
			{"phase": "Verify", "successful_ops": "30", "failed_ops": "0", "iops": "6", "bandwidth_mib_s": "4", "p50_latency_us": "100", "latency_p99_us": "500"},
			{"phase": "Delete", "successful_ops": "30", "failed_ops": "0", "iops": "15", "bandwidth_mib_s": "1", "p50_latency_us": "10", "latency_p99_us": "100"},
		}
		if len(rows) != len(want) {
			t.Fatalf("summary rows = %v, want three phase totals", rows)
		}
		for i, expected := range want {
			for key, value := range expected {
				if got, ok := rows[i][key]; !ok || got != value {
					t.Errorf("summary row %d %s = %q (present: %v), want %q", i, key, got, ok, value)
				}
			}
		}
	}
	rows := decodeCSVExport(t, requestCSVExport(a, csvExportRunID, "?kind=samples"), "spt-"+csvExportRunID+"-samples.csv")
	if len(rows) != 6 {
		t.Fatalf("sample rows = %v, want six interval measurements", rows)
	}
	for i, want := range []string{"8", "9", "10", "12", "13", "14"} {
		if rows[i]["interval_iops"] != want {
			t.Errorf("sample %d IOPS = %q, want %q", i, rows[i]["interval_iops"], want)
		}
	}
	for key, want := range map[string]string{"timestamp": "2026-10-22T12:00:02Z", "op_type": "CREATE", "cumulative_ops": "10", "bandwidth_mib_s": "2.5", "p50_latency_us": "", "latency_p99_us": "", "count_failed": "0"} {
		if got, ok := rows[0][key]; !ok || got != want {
			t.Errorf("first sample %s = %q (present: %v), want %q", key, got, ok, want)
		}
	}
}

func TestCSVExportSummaryFallsBackToSamples(t *testing.T) {
	// Older or incomplete result folders may have intervals without phase totals.
	a := newCSVExportApp(t, false)
	summary := requestCSVExport(a, csvExportRunID, "?kind=summary")
	samples := requestCSVExport(a, csvExportRunID, "?kind=samples")
	rows := decodeCSVExport(t, summary, "spt-"+csvExportRunID+"-summary.csv")
	if len(rows) != 6 || !reflect.DeepEqual(summary.Body.Bytes(), samples.Body.Bytes()) {
		t.Fatalf("summary fallback must preserve the interval measurements: %s", summary.Body.String())
	}
}
