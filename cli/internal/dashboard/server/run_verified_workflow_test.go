package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	verifyapi "github.com/dell/storage-performance-tool/cli/internal/verification"
)

func reopenLaunchApp(a *app) *app {
	b := &app{
		root: a.root, home: a.home, results: a.results, stateDir: a.stateDir,
		stateFile: a.stateFile, lockFile: a.lockFile, port: a.port, sptBin: a.sptBin,
		storageFile: a.storageFile, verifyFile: a.verifyFile, settings: a.settings,
		metricsURL: a.metricsURL, endpointClient: a.endpointClient,
		verifyNodes: a.verifyNodes,
	}
	b.loadStorage()
	return b
}

func TestVerifiedStorageLaunchWorkflow(t *testing.T) {
	for _, tt := range []struct {
		name                      string
		tls, environment, restart bool
	}{
		{name: "HTTP saved endpoint"},
		{name: "HTTPS approval survives restart", tls: true, restart: true},
		{name: "HTTP environment endpoint", environment: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			a, marker, argsPath, _ := newLaunchContractApp(t)
			a.verification = verification{}
			var endpointCalls, verifierCalls atomic.Int32
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				endpointCalls.Add(1)
				if r.Method != http.MethodGet {
					t.Errorf("endpoint method = %s; want GET", r.Method)
				}
				w.WriteHeader(http.StatusForbidden) // Reachability does not require S3 credentials.
			})
			var endpoint *httptest.Server
			if tt.tls {
				endpoint = httptest.NewTLSServer(handler)
			} else {
				endpoint = httptest.NewServer(handler)
			}
			defer endpoint.Close()
			a.endpointClient = endpoint.Client()
			a.verifyNodes = func(hosts []*hostparse.HostInfo, _ verifyapi.Config) *verifyapi.Report {
				verifierCalls.Add(1)
				if len(hosts) != 2 || hosts[0].Host != "node-a" || hosts[0].User != "root" || hosts[1].Host != "node-b" {
					t.Errorf("wrong verifier targets: %+v", hosts)
				}
				return &verifyapi.Report{Ready: true, Results: map[string]*verifyapi.NodeResult{
					"node-a": {Overall: verifyapi.Check{Passed: true}},
					"node-b": {Overall: verifyapi.Check{Passed: true}},
				}}
			}
			opts := options{ObjectCount: 10, ObjectSize: "64KiB", Threads: 1, MaxRuntimeSeconds: 60}
			assertRejectedRun(t, a, marker, launchContractRequest(t, a, opts), http.StatusConflict)
			cfg := storageConfig{Hosts: []string{"root@node-a", "node-b"}, Endpoint: endpoint.URL}
			if tt.environment {
				t.Setenv("S3_ENDPOINTS", endpoint.URL)
				cfg.Endpoint = ""
			}
			body, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if w := verifyRequest(a, string(body)); w.Code != http.StatusAccepted {
				t.Fatalf("verify = %d %s", w.Code, w.Body.String())
			}
			waitVerification(t, a)
			if endpointCalls.Load() != 1 || verifierCalls.Load() != 1 || !a.verified(a.targetConfiguration()) {
				t.Fatal("verification did not approve exactly the requested nodes and endpoint")
			}
			if tt.restart {
				a = reopenLaunchApp(a)
				t.Cleanup(a.stopProcesses)
			}
			w := launchContractRequest(t, a, opts)
			if w.Code != http.StatusAccepted {
				t.Fatalf("verified launch = %d %s", w.Code, w.Body.String())
			}
			waitForRecoveryRun(t, a)
			flags := launchFlags(t, readLaunchFields(t, argsPath))
			if flags["--endpoints"] != endpoint.URL || flags["--test-hosts"] != strings.Join(cfg.Hosts, ",") {
				t.Fatalf("launch used a different target from verification: %#v", flags)
			}
			if endpointCalls.Load() != 1 || verifierCalls.Load() != 1 {
				t.Fatal("launch unexpectedly repeated verification or contacted S3")
			}
			// A changed target must lose the old approval, including one restored
			// from disk. Neither rejection may replace the completed run.
			before := a.readStateFile()
			beforeBytes, err := os.ReadFile(a.stateFile)
			if err != nil {
				t.Fatal(err)
			}
			for _, change := range []string{"endpoint", "hosts"} {
				if err := os.Remove(argsPath); err != nil && !os.IsNotExist(err) {
					t.Fatal(err)
				}
				a.storageMu.Lock()
				a.storage = storageConfig{Hosts: append([]string(nil), cfg.Hosts...), Endpoint: cfg.Endpoint}
				if change == "endpoint" {
					a.storage.Endpoint = "http://changed.example.invalid:8333"
				} else {
					a.storage.Hosts = []string{"node-c"}
				}
				a.storageMu.Unlock()
				w := launchContractRequest(t, a, opts)
				waitForRecoveryRun(t, a)
				assertLaunchRejection(t, w, http.StatusConflict)
				if !reflect.DeepEqual(a.readStateFile(), before) {
					t.Errorf("changed %s replaced completed job state", change)
				}
				afterBytes, err := os.ReadFile(a.stateFile)
				if err != nil || string(afterBytes) != string(beforeBytes) {
					t.Errorf("changed %s rewrote saved state", change)
				}
				if _, err := os.Stat(argsPath); !os.IsNotExist(err) {
					t.Errorf("changed %s started another child", change)
				}
			}
			assertNotInvoked(t, marker)
		})
	}
}
