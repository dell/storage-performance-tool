/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The testdata bodies were recorded from a v5.17-era engine on Jetty 9.4.58
// and on a Jetty 12.1.15 trial port (host, port, hash and timestamps
// normalised). They pin what a current engine actually answers to the port
// checker's probes.

func readFingerprintFixture(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatalf("read fixture %s: %v", name, err)
	}
	return string(data)
}

// dropLinesContaining removes every line of body that contains marker.
func dropLinesContaining(t *testing.T, body, marker string) string {
	t.Helper()
	if !strings.Contains(body, marker) {
		t.Fatalf("fixture has no line containing %q", marker)
	}
	lines := strings.Split(body, "\n")
	kept := lines[:0]
	for _, line := range lines {
		if !strings.Contains(line, marker) {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n")
}

// fingerprintServer serves one fixed response on path and 404 elsewhere.
func fingerprintServer(t *testing.T, path string, status int, contentType, body string) (*PortChecker, string) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != path {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	checker := NewPortCheckerWithOptions("0", "127.0.0.1", 200*time.Millisecond, 500*time.Millisecond, 1)
	return checker, server.URL
}

func probeRunBody(t *testing.T, body string) bool {
	t.Helper()
	checker, baseURL := fingerprintServer(t, endpointRun, http.StatusBadRequest, "text/html;charset=iso-8859-1", body)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return checker.probeRun(ctx, baseURL) != nil
}

func probeMetricsJSONBody(t *testing.T, body string) bool {
	t.Helper()
	checker, baseURL := fingerprintServer(t, endpointMetricsJSON, http.StatusOK, "application/json", body)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return checker.probeMetricsJSON(ctx, baseURL) != nil
}

func TestProbeRun_RecordedEngineBadRequest(t *testing.T) {
	jetty94 := readFingerprintFixture(t, "jetty94-run-ifmatch-star.html")
	jetty12 := readFingerprintFixture(t, "jetty12-run-ifmatch-star.html")

	tests := []struct {
		name string
		body string
	}{
		{"jetty 9.4 recorded body", jetty94},
		{"jetty 12.1 recorded body", jetty12},
		{"jetty 12.1 without the Jetty footer", dropLinesContaining(t, jetty12, "Powered by Jetty")},
		{"jetty 9.4 without the Jetty footer or servlet row",
			dropLinesContaining(t, dropLinesContaining(t, jetty94, "Powered by Jetty"), "com.dell.spt.base.control")},
		{"engine JSON error (Accept: application/json)",
			"{\n\"message\":\"Invalid run ID format: *\",\n\"url\":\"http://127.0.0.1:PORT/run\",\n\"status\":\"400\"\n}"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !probeRunBody(t, tt.body) {
				t.Errorf("probeRun did not recognise the engine's 400 body:\n%s", tt.body)
			}
		})
	}
}

func TestProbeRun_NonSptBadRequestNotDetected(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"nginx 400 page", "<html>\r\n<head><title>400 Bad Request</title></head>\r\n<body>\r\n<center><h1>400 Bad Request</h1></center>\r\n<hr><center>nginx</center>\r\n</body>\r\n</html>\r\n"},
		{"generic JSON 400", `{"error":"Bad Request","message":"Invalid parameter"}`},
		{"empty body", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if probeRunBody(t, tt.body) {
				t.Errorf("probeRun took a non-SPT 400 body for SPT:\n%s", tt.body)
			}
		})
	}
}

func TestProbeMetricsJSON_RecordedSchemaPayload(t *testing.T) {
	array := strings.TrimSpace(readFingerprintFixture(t, "metrics-json-schema5.json"))
	if !strings.HasPrefix(array, "[{") || !strings.HasSuffix(array, "}]") {
		t.Fatalf("fixture is not a one-element array: %.40s…", array)
	}
	object := strings.TrimSuffix(strings.TrimPrefix(array, "["), "]")

	tests := []struct {
		name string
		body string
	}{
		{"recorded one-element array", array},
		{"recorded element as an object", object},
		{"schema 2 minimal row", `[{"metrics_schema":2,"scope":"node","role":"entry","node_id":"n0","op_type":"CREATE","test_state":1}]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if !probeMetricsJSONBody(t, tt.body) {
				t.Errorf("probeMetricsJSON did not recognise the engine's metrics body: %.80s…", tt.body)
			}
		})
	}
}

func TestProbeMetricsJSON_NonSptJSONNotDetected(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{"array of unrelated objects", `[{"id":1,"name":"alpha"},{"id":2,"name":"beta"}]`},
		{"array of scalars", `[1,2,3]`},
		{"unrelated object", `{"status":"ok","uptime":12345}`},
		{"metrics_schema alone", `{"metrics_schema":5}`},
		{"metrics_schema as a string", `{"metrics_schema":"5","scope":"node","op_type":"none"}`},
		{"fractional metrics_schema", `{"metrics_schema":2.5,"scope":"node","op_type":"none"}`},
		{"zero metrics_schema", `{"metrics_schema":0,"scope":"node","op_type":"none"}`},
		{"non-string scope", `{"metrics_schema":5,"scope":1,"op_type":"none"}`},
		{"missing op_type", `[{"metrics_schema":5,"scope":"node","role":"entry"}]`},
		{"not JSON", "# HELP spt_up up\nspt_up 1\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if probeMetricsJSONBody(t, tt.body) {
				t.Errorf("probeMetricsJSON took a non-SPT body for SPT: %s", tt.body)
			}
		})
	}
}
