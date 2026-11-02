package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func submitRecoveryRun(a *app) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/api/runs", strings.NewReader(validRunRequest))
	r.Host = "localhost:8000"
	r.Header.Set("Origin", "http://localhost:8000")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	a.handleRun(w, r)
	return w
}

func waitForRecoveryRun(t *testing.T, a *app) {
	t.Helper()
	done := make(chan struct{})
	go func() { a.processes.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("run workers did not finish")
	}
}

func TestRunStartFailurePreservesStateAndAllowsRetry(t *testing.T) {
	for _, tt := range []struct {
		name, message string
		block         func(*testing.T, *app) func()
	}{
		{"results directory", "Could not create the SPT result directory", func(t *testing.T, a *app) func() {
			if err := os.WriteFile(a.results, []byte("occupied"), 0600); err != nil {
				t.Fatal(err)
			}
			return func() {
				if err := os.Remove(a.results); err != nil {
					t.Fatal(err)
				}
			}
		}},
		{"process start", "Could not start SPT", func(t *testing.T, a *app) func() {
			// Executable permission passes readiness, but a missing interpreter
			// makes exec fail before a child process starts.
			body := "#!" + filepath.Join(a.root, "missing-interpreter") + "\n"
			if err := os.WriteFile(a.sptBin, []byte(body), 0700); err != nil {
				t.Fatal(err)
			}
			return func() {}
		}},
		{"state persistence", "Could not save dashboard run state", func(t *testing.T, a *app) func() {
			// A directory at the temporary filename fails even when tests run
			// as root, while leaving the previous state file intact.
			path := a.stateFile + ".tmp"
			if err := os.Mkdir(path, 0700); err != nil {
				t.Fatal(err)
			}
			return func() {
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, _, _ := prepareShutdownTest(t)
			// A harmless child is sufficient; never invoke real SPT or storage.
			if err := os.WriteFile(a.sptBin, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
				t.Fatal(err)
			}
			previous := map[string]any{"run_id": "mt-dashboard-20261030T120000-abcd", "status": jobCompleted, "returncode": 0}
			a.activeJob = previous
			if err := a.writeState(previous); err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(a.stateFile)
			if err != nil {
				t.Fatal(err)
			}
			a.liveRunID, a.liveSignature = "previous-run", "previous-signature"
			a.liveSamples = []map[string]any{{"cumulative_ops": float64(17)}}
			repair := tt.block(t, a)
			w := submitRecoveryRun(a)
			if w.Code != http.StatusInternalServerError || !strings.Contains(w.Body.String(), tt.message) {
				t.Fatalf("failure response = %d %s", w.Code, w.Body.String())
			}
			if a.activeCmd != nil || a.activeLock != nil || a.lockBusy() {
				t.Fatal("failed start left an active process or held lock")
			}
			if !reflect.DeepEqual(a.activeJob, previous) {
				t.Fatalf("failed start replaced previous job: %#v", a.activeJob)
			}
			if a.liveRunID != "previous-run" || a.liveSignature != "previous-signature" || len(a.liveSamples) != 1 {
				t.Fatal("failed start discarded previous live history")
			}
			after, err := os.ReadFile(a.stateFile)
			if err != nil || string(after) != string(before) {
				t.Fatalf("previous state changed: %v", err)
			}
			if tt.name != "results directory" {
				entries, err := os.ReadDir(a.results)
				if err != nil || len(entries) != 0 {
					t.Fatalf("orphan results: %v, %v", entries, err)
				}
			}
			repair()
			if err := os.WriteFile(a.sptBin, []byte("#!/bin/sh\nexit 0\n"), 0700); err != nil {
				t.Fatal(err)
			}
			w = submitRecoveryRun(a)
			if w.Code != http.StatusAccepted {
				t.Fatalf("retry = %d %s", w.Code, w.Body.String())
			}
			waitForRecoveryRun(t, a)
			a.mu.Lock()
			job, active := cloneMap(a.activeJob), a.activeCmd != nil || a.activeLock != nil
			a.mu.Unlock()
			if job["status"] != jobCompleted || job["run_id"] == previous["run_id"] || active || a.lockBusy() {
				t.Fatalf("retry did not finish cleanly: %#v", job)
			}
		})
	}
}
