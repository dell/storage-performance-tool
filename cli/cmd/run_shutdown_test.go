package cmd

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
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

	if err := requestShutdownAll(ctx, hosts, apiPort, 300*time.Millisecond, 77, true); err != nil {
		t.Fatalf("requestShutdownAll returned error: %v", err)
	}
}

func TestRequestShutdownAllWaitsForEntryTerminalBeforeWorkerShutdown(t *testing.T) {
	var shutdownCalls atomic.Int32
	var entryTerminal atomic.Bool
	var workerBeforeTerminal atomic.Bool
	entryShutdown := make(chan struct{})
	workerShutdown := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/shutdown":
			call := shutdownCalls.Add(1)
			if call == 1 {
				close(entryShutdown)
			} else if call == 2 {
				if !entryTerminal.Load() {
					workerBeforeTerminal.Store(true)
				}
				close(workerShutdown)
			}
			writer.WriteHeader(http.StatusAccepted)
		case "/status":
			state := "RUNNING"
			if entryTerminal.Load() {
				state = "STOPPED"
			}
			_, _ = writer.Write([]byte(`{"state":"` + state + `","run_id":77}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	hosts := []*hostparse.HostInfo{
		{Host: serverURL.Hostname(), Original: "entry"},
		{Host: serverURL.Hostname(), Original: "worker"},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- requestShutdownAll(ctx, hosts, serverURL.Port(), 300*time.Millisecond, 77, false)
	}()

	select {
	case <-entryShutdown:
	case <-time.After(time.Second):
		t.Fatal("entry shutdown was not requested")
	}
	select {
	case <-workerShutdown:
		t.Fatal("worker shutdown was requested before entry became terminal")
	case <-time.After(50 * time.Millisecond):
	}
	entryTerminal.Store(true)
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ordered fleet shutdown did not finish")
	}
	if workerBeforeTerminal.Load() {
		t.Fatal("worker shutdown raced ahead of the entry terminal barrier")
	}
}

func TestRequestShutdownAllRejectsTerminalStatusForDifferentRun(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/shutdown":
			writer.WriteHeader(http.StatusNoContent)
		case "/status":
			_, _ = writer.Write([]byte(`{"state":"STOPPED","run_id":78}`))
		default:
			http.NotFound(writer, request)
		}
	}))
	defer server.Close()

	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	hosts := []*hostparse.HostInfo{{Host: serverURL.Hostname(), Original: serverURL.Hostname()}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	err = requestShutdownAll(ctx, hosts, serverURL.Port(), 300*time.Millisecond, 77, false)
	if err == nil || !strings.Contains(err.Error(), "does not match the owned run") {
		t.Fatalf("requestShutdownAll() error = %v, want owned-run attribution failure", err)
	}
}
