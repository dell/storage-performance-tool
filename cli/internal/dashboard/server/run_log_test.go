package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCatalogLogLevelsAndRedaction(t *testing.T) {
	a := newCatalogApp(t)
	all := catalogGET(t, a, a.handleCatalogLog, "/api/catalog/log?id="+fixtureDashboard, 200)
	lines := all["lines"].([]any)
	if len(lines) != 5 {
		t.Fatalf("want 5 lines (errors.log duplicate dropped), got %d: %#v", len(lines), lines)
	}
	text := ""
	for _, l := range lines {
		text += l.(map[string]any)["text"].(string) + "\n"
	}
	if strings.Contains(text, "abc123") || strings.Contains(text, "AKIAEXAMPLE") || !strings.Contains(text, "secretKey=[redacted]") {
		t.Errorf("credential-like values were not redacted:\n%s", text)
	}
	warn := catalogGET(t, a, a.handleCatalogLog, "/api/catalog/log?id="+fixtureDashboard+"&level=warn", 200)["lines"].([]any)
	if len(warn) != 3 { // WARN, ERROR, and the ERROR's stack frame
		t.Errorf("warn+ lines: %#v", warn)
	}
	errs := catalogGET(t, a, a.handleCatalogLog, "/api/catalog/log?id="+fixtureDashboard+"&level=error&tail=1", 200)["lines"].([]any)
	if len(errs) != 1 || !strings.Contains(errs[0].(map[string]any)["text"].(string), "Frame.java") {
		t.Errorf("error tail: %#v", errs)
	}
	catalogGET(t, a, a.handleCatalogLog, "/api/catalog/log?id="+fixtureDashboard+"&level=debug", 400)
	catalogGET(t, a, a.handleCatalogLog, "/api/catalog/log?id="+fixtureDashboard+"&tail=0", 400)
	catalogGET(t, a, a.handleCatalogLog, "/api/catalog/log?id=nope", 404)
}

func TestDashboardRunLogConsoleAndResults(t *testing.T) {
	a := newCatalogApp(t)
	runID := "mt-dashboard-20261021T170103-518aaa08"
	console := "\x1b[32mStarting\x1b[0m\nWARNING: slow node\nError: request failed --secret-key hunter2\n"
	if err := os.WriteFile(filepath.Join(a.stateDir, runID+".log"), []byte(console), 0600); err != nil {
		t.Fatal(err)
	}
	get := func(q string, want int) map[string]any {
		req := "/api/runs/" + runID + "/log" + q
		return catalogGETPath(t, a, req, runID, want)
	}
	c := get("", 200)
	if c["interim"] != true || c["source"] != "console" {
		t.Errorf("console log must be labeled interim: %#v", c)
	}
	lines := c["lines"].([]any)
	if len(lines) != 3 || lines[0].(map[string]any)["text"] != "Starting" || lines[1].(map[string]any)["level"] != "warn" || lines[2].(map[string]any)["level"] != "error" {
		t.Errorf("console lines: %#v", lines)
	}
	if strings.Contains(lines[2].(map[string]any)["text"].(string), "hunter2") {
		t.Error("console secret not redacted")
	}
	r := get("?source=results&level=error", 200)
	if len(r["lines"].([]any)) != 2 || r["interim"] != false {
		t.Errorf("results errors: %#v", r)
	}
	get("?source=other", 400)
	catalogGETPath(t, a, "/api/runs/../log", "..", 404)
	catalogGETPath(t, a, "/api/runs/mt-dashboard-20990101T000000-00000000/log", "mt-dashboard-20990101T000000-00000000", 404)
}

func catalogGETPath(t *testing.T, a *app, target, runID string, want int) map[string]any {
	t.Helper()
	return catalogGET(t, a, func(w httpResponseWriter, r httpRequest) {
		r.SetPathValue("runID", runID)
		a.handleRunLog(w, r)
	}, target, want)
}

func TestReadTailCapsLargeLogs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "big.log")
	line := strings.Repeat("x", 99) + "\n"
	if err := os.WriteFile(path, []byte(strings.Repeat(line, (logReadMax/100)+500)), 0600); err != nil {
		t.Fatal(err)
	}
	lines, cut := readTail(path)
	if !cut || len(lines) == 0 || len(lines[0]) != 99 {
		t.Errorf("tail: cut=%v first=%d", cut, len(lines[0]))
	}
	if l, _ := readTail(filepath.Join(t.TempDir(), "missing")); l != nil {
		t.Error("missing file should return nil")
	}
}

type (
	httpResponseWriter = http.ResponseWriter
	httpRequest        = *http.Request
)

func TestLogEndpointsRejectForeignHost(t *testing.T) {
	a := newCatalogApp(t)
	for _, target := range []string{"/api/catalog/log?id=" + fixtureDashboard, "/api/runs/" + fixtureDashboardRunID + "/log"} {
		req := httptest.NewRequest(http.MethodGet, target, nil)
		req.Host = "attacker.example:8000"
		req.SetPathValue("runID", fixtureDashboardRunID)
		rec := httptest.NewRecorder()
		if strings.Contains(target, "catalog") {
			a.handleCatalogLog(rec, req)
		} else {
			a.handleRunLog(rec, req)
		}
		if rec.Code != 403 {
			t.Errorf("%s with a foreign Host header got %d, want 403", target, rec.Code)
		}
	}
}

func TestRedactLineCLIFlagAndBareSecret(t *testing.T) {
	cases := []struct {
		in, wantSub, mustGone string
	}{
		{"flag --storage-auth-secret=abc123 done", "--storage-auth-secret=[redacted]", "abc123"},
		{"cfg secret: abc123 trailing", "secret: [redacted]", "abc123"},
		{"cfg secret=abc123 trailing", "secret=[redacted]", "abc123"},
		{"still secretKey=hunter2", "secretKey=[redacted]", "hunter2"},
		{"Error: failed --secret-key hunter2", "--secret-key [redacted]", "hunter2"},
	}
	for _, tc := range cases {
		got := redactLine(tc.in)
		if !strings.Contains(got, tc.wantSub) || strings.Contains(got, tc.mustGone) {
			t.Fatalf("in=%q got=%q want substring %q without %q", tc.in, got, tc.wantSub, tc.mustGone)
		}
	}
}
