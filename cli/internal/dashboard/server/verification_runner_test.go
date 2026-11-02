package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	verifyapi "github.com/dell/storage-performance-tool/cli/internal/verification"
)

func newVerificationTestApp(t *testing.T) (*app, string) {
	t.Helper()
	a, marker := newRunHandlerTestApp(t)
	a.storageFile = filepath.Join(a.stateDir, "storage.json")
	a.verifyFile = filepath.Join(a.stateDir, "verification.json")
	a.verification = verification{}
	// Every test supplies reports explicitly; accidental calls must never use
	// the real verifier, Docker, SSH, or an external storage endpoint.
	a.verifyNodes = func([]*hostparse.HostInfo, verifyapi.Config) *verifyapi.Report {
		t.Error("unexpected node verification")
		return &verifyapi.Report{}
	}
	return a, marker
}

func verifyRequest(a *app, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(http.MethodPost, "/api/storage/verify", strings.NewReader(body))
	r.Host = "localhost:8000"
	r.Header.Set("Origin", "http://localhost:8000")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	a.httpServer().Handler.ServeHTTP(w, r)
	return w
}

func waitVerification(t *testing.T, a *app) {
	t.Helper()
	done := make(chan struct{})
	go func() { a.processes.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("verification did not finish")
	}
}

func TestVerificationReportMapping(t *testing.T) {
	a := &app{verification: verification{Nodes: []verifyNode{{Host: "root@node-a"}, {Host: "node-b"}}}}
	a.verifyNodes = func(hosts []*hostparse.HostInfo, cfg verifyapi.Config) *verifyapi.Report {
		if len(hosts) != 2 || hosts[0].Host != "node-a" || hosts[0].User != "root" || hosts[1].Host != "node-b" {
			t.Fatalf("incorrect parsed hosts: %#v", hosts)
		}
		if cfg.MinHosts != 2 || cfg.APIPort != 9999 || cfg.ShowProgress || cfg.ForceCleanup || cfg.NetworkMode == "" || cfg.RMIPortCount == 0 {
			t.Fatalf("incorrect verifier configuration: %+v", cfg)
		}
		return &verifyapi.Report{Ready: true, Summary: "all nodes ready", Results: map[string]*verifyapi.NodeResult{
			"NODE-A": {Overall: verifyapi.Check{Passed: true, Message: "ready"},
				SSHConnectivity: verifyapi.Check{Passed: true, Message: "ssh ok"},
				ClockSkew:       verifyapi.Check{Warning: true, Message: "clock warning"},
				MetricsEndpoint: verifyapi.Check{Message: "metrics failed"}},
			"node-b":      {Overall: verifyapi.Check{Message: "unavailable"}},
			"unrequested": {Overall: verifyapi.Check{Passed: true}},
		}}
	}
	summary, err := a.runVerificationAPI([]string{"root@node-a", "node-b"})
	if err != nil || summary != "all nodes ready" {
		t.Fatalf("report = %q, %v", summary, err)
	}
	want := []verifyNode{
		{Host: "root@node-a", Status: verifyPassed, Message: "ready", Checks: []verifyCheck{
			{Name: "SSH connectivity", Status: "pass", Message: "ssh ok"},
			{Name: "Clock skew", Status: "warn", Message: "clock warning"},
			{Name: "Metrics endpoint", Status: "fail", Message: "metrics failed"},
		}},
		{Host: "node-b", Status: verifyFailed, Message: "unavailable", Checks: []verifyCheck{}},
	}
	if !reflect.DeepEqual(a.verification.Nodes, want) {
		t.Fatalf("mapped nodes = %#v; want %#v", a.verification.Nodes, want)
	}
	a.verifyNodes = func([]*hostparse.HostInfo, verifyapi.Config) *verifyapi.Report {
		return &verifyapi.Report{Summary: "not ready"}
	}
	if summary, err = a.runVerificationAPI([]string{"node-a"}); err == nil || summary != "not ready" {
		t.Fatalf("failure report = %q, %v", summary, err)
	}
}

func TestVerificationStopsBeforeRunningNodeChecks(t *testing.T) {
	a := &app{verifyNodes: func([]*hostparse.HostInfo, verifyapi.Config) *verifyapi.Report {
		t.Fatal("node checks should not run")
		return nil
	}}
	if _, err := a.runVerificationAPI([]string{"node;invalid"}); err == nil {
		t.Fatal("invalid host accepted")
	}
	a.stopping = true
	if _, err := a.runVerificationAPI([]string{"127.0.0.1"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown error = %v", err)
	}
}
