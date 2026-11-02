package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestVerifyRejectsRequestsWithoutChangingConfiguration(t *testing.T) {
	const good = `{"hosts":["127.0.0.1"],"endpoint":"http://127.0.0.1:9000"}`
	for _, tt := range []struct {
		name, body string
		status     int
		setup      func(*testing.T, *app, *http.Request)
	}{
		{name: "query", body: good, status: 404, setup: func(_ *testing.T, _ *app, r *http.Request) { r.URL.RawQuery = "extra=1" }},
		{name: "missing origin", body: good, status: 403, setup: func(_ *testing.T, _ *app, r *http.Request) { r.Header.Del("Origin") }},
		{name: "wrong content type", body: good, status: 415, setup: func(_ *testing.T, _ *app, r *http.Request) { r.Header.Set("Content-Type", "text/plain") }},
		{name: "empty JSON", status: 400},
		{name: "malformed JSON", body: "{", status: 400},
		{name: "unknown field", body: `{"hosts":["127.0.0.1"],"command":"ignored"}`, status: 400},
		{name: "second object", body: good + " {}", status: 400},
		{name: "oversized body", body: strings.Repeat(" ", maxRequestBytes+1) + good, status: 400},
		{name: "invalid host", body: `{"hosts":["node;id"]}`, status: 400},
		{name: "missing hosts", body: `{}`, status: 400},
		{name: "unsupported scheme", body: `{"hosts":["node-a"],"endpoint":"ftp://storage.invalid"}`, status: 400},
		{name: "embedded credentials", body: `{"hosts":["node-a"],"endpoint":"https://key:secret@storage.invalid"}`, status: 400},
		{name: "duplicate hosts", body: `{"hosts":["localhost","127.0.0.1"]}`, status: 400},
		{name: "missing executable", body: good, status: 503, setup: func(_ *testing.T, a *app, _ *http.Request) { a.sptBin = filepath.Join(a.root, "missing") }},
		{name: "stopping", body: good, status: 409, setup: func(_ *testing.T, a *app, _ *http.Request) { a.stopping = true }},
		{name: "already verifying", body: good, status: 409, setup: func(_ *testing.T, a *app, _ *http.Request) { a.verifying = true }},
		{name: "active run lock", body: good, status: 409, setup: func(t *testing.T, a *app, _ *http.Request) {
			lock, err := a.lockRun()
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { unlockClose(lock) })
		}},
		{name: "cannot save settings", body: good, status: 500, setup: func(t *testing.T, a *app, _ *http.Request) {
			if err := os.Mkdir(a.storageFile+".tmp", 0700); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, marker := newVerificationTestApp(t)
			a.verification = verification{Status: verifyPassed, Summary: "previous result", Nodes: []verifyNode{{Host: "127.0.0.1", Status: verifyPassed, Checks: []verifyCheck{}}}}
			beforeCfg, beforeVerify := a.storageSnapshot(), a.verification.clone()
			for path, value := range map[string]any{a.storageFile: beforeCfg, a.verifyFile: beforeVerify} {
				if err := writeFileAtomic(path, value); err != nil {
					t.Fatal(err)
				}
			}
			beforeBytes, err := os.ReadFile(a.storageFile)
			if err != nil {
				t.Fatal(err)
			}
			beforeVerification, err := os.ReadFile(a.verifyFile)
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest("POST", "/api/storage/verify", strings.NewReader(tt.body))
			r.Host = "localhost:8000"
			r.Header.Set("Origin", "http://localhost:8000")
			r.Header.Set("Content-Type", "application/json")
			if tt.setup != nil {
				tt.setup(t, a, r)
			}
			wasVerifying := a.verifying
			w := httptest.NewRecorder()
			a.httpServer().Handler.ServeHTTP(w, r)
			var response struct {
				Error string `json:"error"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || w.Code != tt.status || response.Error == "" {
				t.Fatalf("rejection = %d %s, %v; want %d", w.Code, w.Body.String(), err, tt.status)
			}
			waitVerification(t, a)
			if a.verifying != wasVerifying || !reflect.DeepEqual(a.storageSnapshot(), beforeCfg) || !reflect.DeepEqual(a.verification, beforeVerify) {
				t.Fatal("rejected request changed storage or verification")
			}
			afterBytes, err := os.ReadFile(a.storageFile)
			if err != nil || string(afterBytes) != string(beforeBytes) {
				t.Fatalf("rejection changed saved settings: %v", err)
			}
			afterVerification, err := os.ReadFile(a.verifyFile)
			if err != nil || string(afterVerification) != string(beforeVerification) {
				t.Fatal("rejection changed saved verification")
			}
			if a.activeCmd != nil || a.activeJob != nil || a.activeLock != nil {
				t.Fatal("verification rejection created a run")
			}
			assertNotInvoked(t, marker)
			if busy := a.lockBusy(); busy != (tt.name == "active run lock") {
				t.Fatal("rejection changed run lock ownership")
			}
		})
	}
}
