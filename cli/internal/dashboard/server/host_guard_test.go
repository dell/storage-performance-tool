package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// guardRunID is a well-formed saved-run id; the guard must answer before the
// handler looks it up.
const guardRunID = "mt-dashboard-20261020T100000-abcdef12"

// Every /api route must refuse a foreign Host (DNS rebinding) or a foreign
// Origin, before its handler runs. The pages themselves stay readable.
func TestAPIGuardRejectsForeignHostOnEveryRoute(t *testing.T) {
	a, marker := newRunHandlerTestApp(t)
	handler := a.httpServer().Handler
	routes := []struct{ method, path string }{
		{http.MethodGet, "/api/state"},
		{http.MethodGet, "/api/runs"},
		{http.MethodGet, "/api/runs/" + guardRunID},
		{http.MethodGet, "/api/runs/" + guardRunID + "/csv"},
		{http.MethodGet, "/api/runs/" + guardRunID + "/log"},
		{http.MethodGet, "/api/catalog"},
		{http.MethodGet, "/api/catalog/run?id=x"},
		{http.MethodGet, "/api/catalog/compare?baseline=x&candidate=y"},
		{http.MethodGet, "/api/catalog/log?id=x"},
		{http.MethodPost, "/api/runs"},
		{http.MethodPost, "/api/attach"},
		{http.MethodDelete, "/api/attach"},
		{http.MethodPost, "/api/storage/verify"},
		{http.MethodGet, "/api/unknown"},
	}
	for _, host := range []string{"evil.example", "evil.example:8000", "127.0.0.1:8001", "localhost", "10.0.0.5:8000"} {
		for _, route := range routes {
			req := httptest.NewRequest(route.method, route.path, strings.NewReader(validRunRequest))
			req.Host = host
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), msgLoopbackOnly) {
				t.Errorf("%s %s Host=%s: status %d body %s; want 403", route.method, route.path, host, rec.Code, rec.Body.String())
			}
		}
	}
	assertNotInvoked(t, marker)
}

func TestAPIGuardRejectsForeignOriginOnLoopbackHost(t *testing.T) {
	a, marker := newRunHandlerTestApp(t)
	handler := a.httpServer().Handler
	for _, origin := range []string{"http://evil.example", "null", "https://127.0.0.1:8000", "http://127.0.0.1:8001"} {
		for _, path := range []string{"/api/state", "/api/runs", "/api/catalog"} {
			req := httptest.NewRequest(http.MethodGet, path, nil)
			req.Host = "127.0.0.1:8000"
			req.Header.Set("Origin", origin)
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden {
				t.Errorf("GET %s Origin=%s: status %d; want 403", path, origin, rec.Code)
			}
		}
	}
	assertNotInvoked(t, marker)
}

// The SSH tunnel case: the browser sends Host 127.0.0.1:<port> or localhost:<port>,
// and no Origin on same-origin GETs, or a matching Origin.
func TestAPIGuardAllowsTunnelHosts(t *testing.T) {
	a, _ := newRunHandlerTestApp(t)
	handler := a.httpServer().Handler
	for _, host := range []string{"127.0.0.1:8000", "localhost:8000", "LOCALHOST:8000"} {
		for _, origin := range []string{"", "http://" + strings.ToLower(host)} {
			for _, path := range []string{"/api/state", "/api/runs", "/api/catalog"} {
				req := httptest.NewRequest(http.MethodGet, path, nil)
				req.Host = host
				if origin != "" {
					req.Header.Set("Origin", origin)
				}
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					t.Errorf("GET %s Host=%s Origin=%q: status %d body %s; want 200", path, host, origin, rec.Code, rec.Body.String())
				}
			}
		}
	}
	// The page shell is static and stays reachable; only /api is guarded.
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	req.Host = "evil.example"
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code == http.StatusForbidden {
		t.Errorf("GET / should not be Host-guarded, got 403")
	}
}

func assertNotInvoked(t *testing.T, marker string) {
	t.Helper()
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("a guarded request reached SPT or Docker")
	}
}
