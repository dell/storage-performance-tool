package verification

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// httpNC is a simple NetworkChecker that delegates HTTPGet to net/http
type httpNC struct{}

func (httpNC) IsPortOpen(host string, port int, timeout time.Duration) bool  { return true }
func (httpNC) HTTPGet(ctx context.Context, u string) (*http.Response, error) { return http.Get(u) }

func getPort(t *testing.T, raw string) int {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("parse url: %v", err)
	}
	p, err := strconv.Atoi(u.Port())
	if err != nil {
		t.Fatalf("port: %v", err)
	}
	return p
}

func TestVerifier_checkMetricsEndpoint_SuccessAndParseError(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics/json", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"ok":true}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	v := &Verifier{netChecker: httpNC{}, timeProvider: &RealTimeProvider{}, config: Config{APIPort: getPort(t, srv.URL)}}
	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true}
	ok := v.checkMetricsEndpoint(host)
	if !ok.Passed || !strings.Contains(ok.Message, "Returns valid JSON") {
		t.Fatalf("expected metrics success, got: %+v", ok)
	}

	// now serve invalid JSON on a fresh server
	mux2 := http.NewServeMux()
	mux2.HandleFunc("/metrics/json", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("not-json"))
	})
	srv2 := httptest.NewServer(mux2)
	defer srv2.Close()
	v.config.APIPort = getPort(t, srv2.URL)
	bad := v.checkMetricsEndpoint(host)
	if bad.Passed || !strings.Contains(bad.Message, "Failed to parse metrics JSON") {
		t.Fatalf("expected parse failure, got: %+v", bad)
	}
}

func TestVerifier_checkControlEndpoint_SuccessAndFailure(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/run", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) })
	srv := httptest.NewServer(mux)
	defer srv.Close()

	v := &Verifier{netChecker: httpNC{}, timeProvider: &RealTimeProvider{}, config: Config{APIPort: getPort(t, srv.URL)}}
	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true}
	ok := v.checkControlEndpoint(host)
	if !ok.Passed {
		t.Fatalf("expected control endpoint pass, got: %+v", ok)
	}

	// failure case: point at an unreachable port
	v.config.APIPort = 1
	bad := v.checkControlEndpoint(host)
	if bad.Passed {
		t.Fatalf("expected failure on unreachable port, got: %+v", bad)
	}
}
