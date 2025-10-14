package portcheck

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSptAPIClient_ShutdownAndLinger_OK(t *testing.T) {
	// Simple server that accepts /shutdown and reports terminal /status
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/shutdown":
			w.WriteHeader(http.StatusAccepted)
		case "/status":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"state":"COMPLETED"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	c := NewSptAPIClient(srv.URL, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown returned error: %v", err)
	}

	if err := c.WaitForLinger(ctx, 300*time.Millisecond); err != nil {
		t.Fatalf("linger wait failed: %v", err)
	}
}

func TestSptAPIClient_Shutdown_ErrorStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/shutdown" {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte("boom"))
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()

	c := NewSptAPIClient(srv.URL, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	if err := c.Shutdown(ctx); err == nil {
		t.Fatal("expected shutdown error, got nil")
	}
}

func TestSptAPIClient_Linger_NonTerminalFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/shutdown":
			w.WriteHeader(http.StatusOK)
		case "/status":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"state":"RUNNING"}`))
		}
	}))
	defer srv.Close()

	c := NewSptAPIClient(srv.URL, 2*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	_ = c.Shutdown(ctx)
	if err := c.WaitForLinger(ctx, 200*time.Millisecond); err == nil {
		t.Fatal("expected linger error for non-terminal state, got nil")
	}
}
