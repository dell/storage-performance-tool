package cmd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

func TestRequestShutdownAll_Succeeds(t *testing.T) {
	// Test server that accepts shutdown and returns IDLE status
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/shutdown":
			w.WriteHeader(http.StatusNoContent)
		case "/status":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"state":"IDLE"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	// host:port will be in u.Host
	// For localhost tests, the host portion can be 127.0.0.1 or [::1], treat entire as host
	apiHost := u.Hostname()
	apiPort := u.Port()

	hosts := []*hostparse.HostInfo{{Host: apiHost, Original: apiHost}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := requestShutdownAll(ctx, hosts, apiPort, 300*time.Millisecond, true); err != nil {
		t.Fatalf("requestShutdownAll returned error: %v", err)
	}
}
