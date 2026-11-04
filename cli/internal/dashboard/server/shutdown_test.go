package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

func prepareShutdownTest(t *testing.T) (*app, string, string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell fixture requires Unix")
	}
	a, marker := newRunHandlerTestApp(t)
	a.metricsURL = "http://127.0.0.1:1/metrics/json"
	a.storageFile = filepath.Join(a.stateDir, "storage-config.json")
	a.verifyFile = filepath.Join(a.stateDir, "storage-verification.json")
	args := filepath.Join(a.root, "arguments")
	ready := filepath.Join(a.root, "ready")
	t.Setenv("DASHBOARD_TEST_ARGS", args)
	t.Setenv("DASHBOARD_TEST_READY", ready)
	body := `#!/bin/sh
printf '%s\n' "$@" > "$DASHBOARD_TEST_ARGS"
trap 'printf stopped > "$DASHBOARD_TEST_MARKER"; exit 0' INT TERM
printf ready > "$DASHBOARD_TEST_READY"
while :; do /bin/sleep 0.05; done
`
	if err := os.WriteFile(a.sptBin, []byte(body), 0700); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(a.stopProcesses)
	return a, marker, args
}

func waitForChild(t *testing.T, root string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(filepath.Join(root, "ready")); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("child process did not start")
}

func TestDashboardShutdownInterruptsRun(t *testing.T) {
	a, marker, argsPath := prepareShutdownTest(t)
	request := httptest.NewRequest(http.MethodPost, "/api/runs", strings.NewReader(validRunRequest))
	request.Host = "localhost:8000"
	request.Header.Set("Origin", "http://localhost:8000")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	a.handleRun(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("start run: %d %s", response.Code, response.Body.String())
	}
	waitForChild(t, a.root)
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(args), "run\nwrite-verify\n--headless\n") {
		t.Fatalf("unexpected child arguments: %s", args)
	}
	a.stopProcesses()
	if data, err := os.ReadFile(marker); err != nil || string(data) != "stopped" {
		t.Fatalf("child was not gracefully interrupted: %q, %v", data, err)
	}
	a.mu.Lock()
	status := a.activeJob["status"]
	active := a.activeCmd != nil
	a.mu.Unlock()
	if active || status != jobInterrupted || a.lockBusy() { // the dashboard stopped SPT, so the run is interrupted even though the child exited 0
		t.Fatalf("child did not finish and release its lock: active=%v status=%v", active, status)
	}
}

func TestStopRunGracefully(t *testing.T) {
	a, marker, _ := prepareShutdownTest(t)
	stop := func(origin string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "/api/runs/stop", nil)
		request.Host = "localhost:8000"
		request.Header.Set("Origin", origin)
		response := httptest.NewRecorder()
		a.handleStop(response, request)
		return response
	}
	if response := stop("http://evil.example"); response.Code != http.StatusForbidden {
		t.Fatalf("cross-origin stop: %d", response.Code)
	}
	if response := stop("http://localhost:8000"); response.Code != http.StatusConflict {
		t.Fatalf("stop without a run: %d", response.Code)
	}
	request := httptest.NewRequest(http.MethodPost, "/api/runs", strings.NewReader(validRunRequest))
	request.Host = "localhost:8000"
	request.Header.Set("Origin", "http://localhost:8000")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	a.handleRun(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("start run: %d %s", response.Code, response.Body.String())
	}
	waitForChild(t, a.root)
	if response := stop("http://localhost:8000"); response.Code != http.StatusAccepted {
		t.Fatalf("stop run: %d %s", response.Code, response.Body.String())
	}
	if response := stop("http://localhost:8000"); response.Code != http.StatusAccepted && response.Code != http.StatusConflict {
		t.Fatalf("repeated stop: %d %s", response.Code, response.Body.String())
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		a.mu.Lock()
		status := a.activeJob[keyStatus]
		active := a.activeCmd != nil
		a.mu.Unlock()
		if !active && status == jobStopped && !a.lockBusy() {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("run did not finish as stopped: active=%v status=%v", active, status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "stopped" {
		t.Fatalf("child was not gracefully interrupted: %q, %v", data, err)
	}
	if response := stop("http://localhost:8000"); response.Code != http.StatusConflict {
		t.Fatalf("stop after the run ended: %d", response.Code)
	}
}

func TestStopRefusesAttachedRun(t *testing.T) {
	a, _ := newRunHandlerTestApp(t)
	a.mu.Lock()
	a.attachedHost = "127.0.0.1"
	a.attachedRunID = "42"
	a.mu.Unlock()
	request := httptest.NewRequest(http.MethodPost, "/api/runs/stop", nil)
	request.Host = "localhost:8000"
	request.Header.Set("Origin", "http://localhost:8000")
	response := httptest.NewRecorder()
	a.handleStop(response, request)
	if response.Code != http.StatusConflict || !strings.Contains(response.Body.String(), "only monitoring") {
		t.Fatalf("stop of an attached run: %d %s", response.Code, response.Body.String())
	}
}
