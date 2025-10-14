package verification

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRealNetworkChecker_IsPortOpen_SuccessAndFail(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err != nil {
		t.Fatalf("parse port: %v", err)
	}

	nc := &RealNetworkChecker{}
	if !nc.IsPortOpen("127.0.0.1", port, 200*time.Millisecond) {
		t.Fatalf("expected open port to report true")
	}

	// close and test failure
	ln.Close()
	if nc.IsPortOpen("127.0.0.1", port, 200*time.Millisecond) {
		t.Fatalf("expected closed port to report false")
	}
}

func TestRealNetworkChecker_HTTPGet(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	}))
	defer srv.Close()

	nc := &RealNetworkChecker{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	resp, err := nc.HTTPGet(ctx, srv.URL)
	if err != nil {
		t.Fatalf("httpget: %v", err)
	}
	if resp.StatusCode != 200 {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestRealTimeProvider_NowSince(t *testing.T) {
	tp := &RealTimeProvider{}
	t0 := tp.Now()
	time.Sleep(5 * time.Millisecond)
	if tp.Since(t0) <= 0 {
		t.Fatalf("expected positive duration")
	}
}
