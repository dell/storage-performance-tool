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

func requireCSVExportError(t *testing.T, rec *httptest.ResponseRecorder, status int, message string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("response = %d %s, want %d", rec.Code, rec.Body.String(), status)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"] != message {
		t.Errorf("error body = %s, decode error = %v, want %q", rec.Body.String(), err, message)
	}
	if !strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") || rec.Header().Get("Content-Disposition") != "" {
		t.Errorf("error advertised a CSV download: %v", rec.Header())
	}
}

func TestCSVExportRejectsInvalidOptions(t *testing.T) {
	a := newCSVExportApp(t, true)
	for _, tt := range []struct{ query, message string }{
		{"?unknown=1", "Unsupported CSV export options."},
		{"?kind=raw", "Choose summary, samples, phase, or window CSV."},
		{"?kind=phase", "Choose a supported run phase."},
		{"?kind=phase&phase=WRITE", "Choose a supported run phase."},
		{"?kind=window&phase=CLEANUP&start=0&end=10", "Choose a supported run phase."},
	} {
		t.Run(tt.query, func(t *testing.T) {
			requireCSVExportError(t, requestCSVExport(a, csvExportRunID, tt.query), http.StatusBadRequest, tt.message)
		})
	}
	for _, bounds := range []string{
		"", "&start=2", "&end=4", "&start=word&end=4", "&start=2&end=word",
		"&start=NaN&end=4", "&start=2&end=NaN", "&start=-Inf&end=4", "&start=2&end=Inf",
		"&start=-1&end=4", "&start=4&end=2",
	} {
		t.Run("window"+bounds, func(t *testing.T) {
			requireCSVExportError(t, requestCSVExport(a, csvExportRunID, "?kind=window&phase=CREATE"+bounds), http.StatusBadRequest, "Choose a valid visible time window.")
		})
	}
}

func TestCSVExportMissingRunsAndMeasurements(t *testing.T) {
	a := newCSVExportApp(t, true)
	for _, runID := range []string{"invalid", "mt-cli-run", "mt-dashboard-missing", "../" + csvExportRunID, csvExportRunID + "/../outside"} {
		t.Run(runID, func(t *testing.T) {
			requireCSVExportError(t, requestCSVExport(a, runID, ""), http.StatusNotFound, msgSavedRunNotFound)
		})
	}
	for _, query := range []string{
		"?kind=window&phase=CREATE&start=20&end=30",
		"?kind=window&phase=VERIFY&start=0&end=6",
	} {
		requireCSVExportError(t, requestCSVExport(a, csvExportRunID, query), http.StatusNotFound, "No measurements are available for this CSV export.")
	}
	files, err := filepath.Glob(filepath.Join(a.results, csvExportRunID, "*", "*.csv"))
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range files {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
	}
	for _, query := range []string{"", "?kind=samples", "?kind=phase&phase=CREATE", "?kind=window&phase=CREATE&start=0&end=10"} {
		t.Run("no metrics"+query, func(t *testing.T) {
			requireCSVExportError(t, requestCSVExport(a, csvExportRunID, query), http.StatusNotFound, "No measurements are available for this CSV export.")
		})
	}
	params := filepath.Join(a.results, csvExportRunID, "dashboard-20261022.120000.000", "spt_run_params.json")
	if err := os.WriteFile(params, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	requireCSVExportError(t, requestCSVExport(a, csvExportRunID, ""), http.StatusNotFound, msgSavedRunNotFound)
}
