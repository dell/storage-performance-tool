package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	verifyapi "github.com/dell/storage-performance-tool/cli/internal/verification"
)

func TestVerificationWorkflow(t *testing.T) {
	for _, tt := range []struct {
		name, wantStatus, wantEndpoint                                                 string
		tls, missingEndpoint, missingNode, failedNode, reportNotReady, endpointFailure bool
	}{
		{name: "HTTP success", wantStatus: verifyPassed, wantEndpoint: verifyPassed},
		{name: "HTTPS success", tls: true, wantStatus: verifyPassed, wantEndpoint: verifyPassed},
		{name: "failed node", failedNode: true, wantStatus: verifyFailed, wantEndpoint: verifyPassed},
		{name: "missing node result", missingNode: true, wantStatus: verifyFailed, wantEndpoint: verifyPassed},
		{name: "report not ready", reportNotReady: true, wantStatus: verifyFailed, wantEndpoint: verifyPassed},
		{name: "missing endpoint", missingEndpoint: true, wantStatus: verifyFailed, wantEndpoint: verifyFailed},
		{name: "unreachable endpoint", endpointFailure: true, wantStatus: verifyFailed, wantEndpoint: verifyFailed},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, marker := newVerificationTestApp(t)
			endpointHandler := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) })
			var endpoint *httptest.Server
			if tt.tls {
				endpoint = httptest.NewTLSServer(endpointHandler)
			} else {
				endpoint = httptest.NewServer(endpointHandler)
			}
			defer endpoint.Close()
			a.endpointClient = endpoint.Client()
			if tt.endpointFailure {
				a.endpointClient = &http.Client{Transport: verificationOfflineTransport{}}
			}
			cfg := storageConfig{Hosts: []string{"root@node-a", "node-b"}, Endpoint: endpoint.URL}
			if tt.missingEndpoint {
				cfg.Endpoint = ""
				t.Setenv("S3_ENDPOINTS", "")
				t.Setenv("S3_ENDPOINT", "")
			}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(func() { unblock(); waitVerification(t, a) })
			a.verifyNodes = func([]*hostparse.HostInfo, verifyapi.Config) *verifyapi.Report {
				close(entered)
				<-release
				report := &verifyapi.Report{Ready: !tt.reportNotReady, Results: map[string]*verifyapi.NodeResult{
					"node-a": {Overall: verifyapi.Check{Passed: true, Message: "ready"}},
					"node-b": {Overall: verifyapi.Check{Passed: !tt.failedNode, Message: "node result"}},
				}}
				if tt.missingNode {
					delete(report.Results, "node-b")
				}
				return report
			}
			body, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			w := verifyRequest(a, string(body))
			if w.Code != http.StatusAccepted {
				t.Fatalf("verify = %d %s", w.Code, w.Body.String())
			}
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("verifier not called")
			}
			a.mu.Lock()
			busy := a.verifying
			a.mu.Unlock()
			if !busy {
				t.Fatal("verification not marked busy")
			}
			var initial struct {
				Storage struct {
					Verification verification `json:"verification"`
				} `json:"storage"`
			}
			if err := json.Unmarshal(w.Body.Bytes(), &initial); err != nil {
				t.Fatal(err)
			}
			if initial.Storage.Verification.Status != verifyRunning || len(initial.Storage.Verification.Nodes) != 2 {
				t.Fatalf("wrong initial state: %#v", initial)
			}
			if second := verifyRequest(a, string(body)); second.Code != http.StatusConflict {
				t.Fatalf("overlapping verify = %d %s", second.Code, second.Body.String())
			}
			// Verification invalidates the previous approval, so a run cannot
			// start while the new node checks are still blocked.
			r := httptest.NewRequest("POST", "/api/runs", strings.NewReader(validRunRequest))
			r.Host = "localhost:8000"
			r.Header.Set("Origin", "http://localhost:8000")
			r.Header.Set("Content-Type", "application/json")
			runResponse := httptest.NewRecorder()
			a.handleRun(runResponse, r)
			wantRunStatus := http.StatusConflict
			if tt.missingEndpoint {
				wantRunStatus = http.StatusServiceUnavailable
			}
			if runResponse.Code != wantRunStatus {
				t.Fatalf("run during verification = %d", runResponse.Code)
			}
			unblock()
			waitVerification(t, a)
			target := a.targetConfiguration()
			v := a.currentVerification(target)
			if v.Status != tt.wantStatus || v.Endpoint.Status != tt.wantEndpoint || v.StartedAt == "" || v.FinishedAt == "" || v.Summary == "" {
				t.Fatalf("wrong final verification: %+v", v)
			}
			for _, stamp := range []string{v.StartedAt, v.FinishedAt} {
				if _, err := time.Parse(time.RFC3339Nano, stamp); err != nil {
					t.Fatal(err)
				}
			}
			if tt.missingNode && (v.Nodes[1].Status != verifyFailed || !strings.Contains(v.Nodes[1].Message, "did not report")) {
				t.Fatalf("missing result left pending: %+v", v.Nodes[1])
			}
			if tt.failedNode && v.Nodes[1].Status != verifyFailed {
				t.Fatal("failed node accepted")
			}
			if a.verified(target) != (tt.wantStatus == verifyPassed) {
				t.Fatal("run readiness disagrees with verification")
			}
			a.mu.Lock()
			busy = a.verifying
			a.mu.Unlock()
			if busy || a.lockBusy() {
				t.Fatal("verification did not release busy state")
			}
			var savedCfg storageConfig
			data, err := os.ReadFile(a.storageFile)
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(data, &savedCfg); err != nil || !reflect.DeepEqual(savedCfg, cfg) {
				t.Fatalf("saved configuration = %+v, %v", savedCfg, err)
			}
			restarted := &app{storageFile: a.storageFile, verifyFile: a.verifyFile}
			restarted.loadStorage()
			if !reflect.DeepEqual(restarted.storage, cfg) || !reflect.DeepEqual(restarted.verification, v) {
				t.Fatalf("reload lost configuration or result: %+v", restarted.verification)
			}
			assertNotInvoked(t, marker)
		})
	}
}

type verificationOfflineTransport struct{}

func (verificationOfflineTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("simulated unreachable endpoint")
}
