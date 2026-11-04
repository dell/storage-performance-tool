package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const fixtureDashboardRunID = "mt-dashboard-20261021T170103-518aaa08"

// The existing saved-run endpoints read the same spt_run_params.json files as the catalog.
// These checks pin that they keep exposing settings only, never the stored S3 credentials.
func TestSavedRunEndpointsNeverExposeCredentials(t *testing.T) {
	a := newCatalogApp(t)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/runs", a.handleRunHistory)
	mux.HandleFunc("GET /api/runs/{runID}", a.handleSavedRun)
	mux.HandleFunc("GET /api/runs/{runID}/csv", a.handleRunCSV)
	get := func(target string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Host = "127.0.0.1:8000"
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if strings.Contains(rec.Body.String(), "CANARY") {
			t.Fatalf("%s exposed a credential value", target)
		}
		return rec
	}
	if rec := get("/api/runs"); rec.Code != 200 || !strings.Contains(rec.Body.String(), fixtureDashboardRunID) {
		t.Errorf("history: %d %s", rec.Code, rec.Body.String())
	}
	if rec := get("/api/runs/" + fixtureDashboardRunID); rec.Code != 200 || !strings.Contains(rec.Body.String(), `"object_count"`) {
		t.Errorf("saved run: %d %s", rec.Code, rec.Body.String())
	}
	for _, kind := range []string{"summary", "samples"} {
		if rec := get("/api/runs/" + fixtureDashboardRunID + "/csv?kind=" + kind); rec.Code != 200 {
			t.Errorf("csv %s: %d %s", kind, rec.Code, rec.Body.String())
		}
	}
	if rec := get("/api/runs/cli-baseline/csv"); rec.Code != 404 {
		t.Errorf("non-dashboard id should be 404, got %d", rec.Code)
	}
	if rec := get("/api/runs/" + fixtureDashboardRunID + "/csv?bogus=1"); rec.Code != 400 {
		t.Errorf("unsupported csv option should be 400, got %d", rec.Code)
	}
}
