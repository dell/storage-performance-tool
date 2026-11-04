package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	verifyapi "github.com/dell/storage-performance-tool/cli/internal/verification"
)

func TestVerificationExcludesLaunchAcrossDashboardInstances(t *testing.T) {
	a, marker, argsPath, _ := newLaunchContractApp(t)
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) }))
	defer endpoint.Close()
	a.storage.Endpoint = endpoint.URL
	a.verification.Signature = configSignature(a.storage.Hosts, endpoint.URL)
	for path, value := range map[string]any{a.storageFile: a.storageSnapshot(), a.verifyFile: a.verification} {
		if err := writeFileAtomic(path, value); err != nil {
			t.Fatal(err)
		}
	}
	b := reopenLaunchApp(a)
	t.Cleanup(b.stopProcesses)
	entered, released := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(released) }) }
	t.Cleanup(func() { release(); waitVerification(t, a) })
	a.endpointClient = endpoint.Client()
	a.verifyNodes = func([]*hostparse.HostInfo, verifyapi.Config) *verifyapi.Report {
		close(entered)
		<-released
		return &verifyapi.Report{Ready: true, Results: map[string]*verifyapi.NodeResult{
			"127.0.0.1": {Overall: verifyapi.Check{Passed: true}},
		}}
	}
	var unexpectedNodes atomic.Int32
	transport := &storageBoundaryTransport{}
	b.endpointClient = &http.Client{Transport: transport}
	b.verifyNodes = func([]*hostparse.HostInfo, verifyapi.Config) *verifyapi.Report {
		unexpectedNodes.Add(1)
		return &verifyapi.Report{}
	}
	body, err := json.Marshal(a.storageSnapshot())
	if err != nil {
		t.Fatal(err)
	}
	if w := verifyRequest(a, string(body)); w.Code != http.StatusAccepted {
		t.Fatalf("initial verification = %d %s", w.Code, w.Body.String())
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("node verifier did not start")
	}
	// The operation guard must not impersonate a running job: state polling
	// uses the separate job lock to distinguish a live run from a stopped one.
	if a.lockBusy() {
		t.Error("verification acquired the running-job lock")
	}
	w := launchContractRequest(t, b, options{ObjectCount: 10, ObjectSize: "64KiB", Threads: 1, MaxRuntimeSeconds: 60})
	waitForRecoveryRun(t, b)
	if w.Code != http.StatusConflict {
		t.Errorf("run during another instance's verification = %d %s; want 409", w.Code, w.Body.String())
	}
	w = verifyRequest(b, string(body))
	waitVerification(t, b)
	if w.Code != http.StatusConflict {
		t.Errorf("overlapping verification = %d %s; want 409", w.Code, w.Body.String())
	}
	if unexpectedNodes.Load() != 0 || transport.calls.Load() != 0 {
		t.Error("overlapping verification reached a node or endpoint")
	}
	for _, path := range []string{argsPath, a.stateFile, a.results} {
		if _, err := os.Stat(path); !os.IsNotExist(err) {
			t.Errorf("overlapping run created %s: %v", path, err)
		}
	}
	release()
	waitVerification(t, a)
	b.loadStorage()
	if !b.verified(b.targetConfiguration()) {
		t.Fatal("finished verification did not save a usable approval")
	}
	w = launchContractRequest(t, b, options{ObjectCount: 10, ObjectSize: "64KiB", Threads: 1, MaxRuntimeSeconds: 60})
	if w.Code != http.StatusAccepted {
		t.Fatalf("launch after verification = %d %s", w.Code, w.Body.String())
	}
	waitForRecoveryRun(t, b)
	if b.activeJob[keyStatus] != jobCompleted || b.lockBusy() {
		t.Fatal("post-verification run did not finish cleanly")
	}
	assertNotInvoked(t, marker)
}

func TestVerificationSaveFailureReleasesLaunchGuard(t *testing.T) {
	a, marker, _, _ := newLaunchContractApp(t)
	for path, value := range map[string]any{a.storageFile: a.storageSnapshot(), a.verifyFile: a.verification} {
		if err := writeFileAtomic(path, value); err != nil {
			t.Fatal(err)
		}
	}
	b := reopenLaunchApp(a)
	t.Cleanup(b.stopProcesses)
	if err := os.Mkdir(a.storageFile+".tmp", 0700); err != nil {
		t.Fatal(err)
	}
	assertLaunchRejection(t, verifyRequest(a, `{"hosts":["127.0.0.1"]}`), http.StatusInternalServerError)
	// Even though settings could not be saved, another instance must still be
	// able to use the previously approved configuration and acquire both locks.
	w := launchContractRequest(t, b, options{ObjectCount: 10, ObjectSize: "64KiB", Threads: 1, MaxRuntimeSeconds: 60})
	if w.Code != http.StatusAccepted {
		t.Fatalf("launch after verification save failure = %d %s", w.Code, w.Body.String())
	}
	waitForRecoveryRun(t, b)
	if b.activeJob[keyStatus] != jobCompleted || b.lockBusy() {
		t.Fatal("run after save failure did not finish cleanly")
	}
	assertNotInvoked(t, marker)
}

func TestFailedVerificationReleasesLaunchGuard(t *testing.T) {
	for _, failNode := range []bool{true, false} {
		name := "endpoint failure"
		if failNode {
			name = "node failure"
		}
		t.Run(name, func(t *testing.T) {
			a, marker, _, _ := newLaunchContractApp(t)
			endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusForbidden) }))
			defer endpoint.Close()
			if failNode {
				a.endpointClient = endpoint.Client()
			}
			a.verifyNodes = func([]*hostparse.HostInfo, verifyapi.Config) *verifyapi.Report {
				return &verifyapi.Report{Ready: !failNode, Results: map[string]*verifyapi.NodeResult{
					"127.0.0.1": {Overall: verifyapi.Check{Passed: !failNode}},
				}}
			}
			body, err := json.Marshal(storageConfig{Hosts: []string{"127.0.0.1"}, Endpoint: endpoint.URL})
			if err != nil {
				t.Fatal(err)
			}
			if w := verifyRequest(a, string(body)); w.Code != http.StatusAccepted {
				t.Fatalf("initial verification = %d %s", w.Code, w.Body.String())
			}
			waitVerification(t, a)
			if a.currentVerification(a.targetConfiguration()).Status != verifyFailed {
				t.Fatal("fixture did not produce a failed verification")
			}
			b := reopenLaunchApp(a)
			t.Cleanup(b.stopProcesses)
			if w := verifyRequest(b, string(body)); w.Code != http.StatusAccepted {
				t.Fatalf("retry after failed verification = %d %s", w.Code, w.Body.String())
			}
			waitVerification(t, b)
			if b.verifying || b.lockBusy() {
				t.Fatal("failed verification retry retained busy state")
			}
			assertNotInvoked(t, marker)
		})
	}
}
