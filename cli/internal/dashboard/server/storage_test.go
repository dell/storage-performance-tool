package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestValidateStorage(t *testing.T) {
	good := []storageConfig{
		{Hosts: []string{"127.0.0.1"}},
		{Hosts: []string{" root@node1 ", "", "admin@10.0.0.7", "worker-3.lab.example"}},
		{Hosts: []string{"localhost"}, Endpoint: " https://s3.example:9021 "},
		{Hosts: []string{"localhost"}, Endpoint: " http://127.0.0.1:9000 "},
	}
	for _, cfg := range good {
		if _, err := validateStorage(cfg); err != nil {
			t.Errorf("rejected valid settings %+v: %v", cfg, err)
		}
	}
	cleaned, _ := validateStorage(good[1])
	if strings.Join(cleaned.Hosts, ",") != "root@node1,admin@10.0.0.7,worker-3.lab.example" {
		t.Errorf("hosts were not trimmed and compacted: %#v", cleaned.Hosts)
	}

	bad := []storageConfig{
		{},
		{Hosts: []string{" ", ""}},
		{Hosts: []string{"-oProxyCommand=id"}},
		{Hosts: []string{"-u@node1"}},
		{Hosts: []string{"root@"}},
		{Hosts: []string{"@node1"}},
		{Hosts: []string{"node one"}},
		{Hosts: []string{"node1;id"}},
		{Hosts: []string{"node1,node2"}},
		{Hosts: []string{"node1", "root@NODE1"}},
		{Hosts: []string{"localhost", "127.0.0.1"}},
		{Hosts: []string{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9"}},
		{Hosts: []string{"node1"}, Endpoint: "https://key:secret@s3.example"},
		{Hosts: []string{"node1"}, Endpoint: "https://s3.example/?list"},
	}
	for _, cfg := range bad {
		if _, err := validateStorage(cfg); err == nil {
			t.Errorf("accepted unsafe settings: %+v", cfg)
		}
	}
}

func TestRunsRequireMatchingVerification(t *testing.T) {
	a := &app{}
	target := map[string]any{"hosts": []string{"127.0.0.1", "root@node2"}, "endpoint": "https://s3.example"}
	if a.verified(target) {
		t.Fatal("an unverified configuration was accepted")
	}
	signature := configSignature([]string{"127.0.0.1", "root@node2"}, "https://s3.example")
	for status, want := range map[string]bool{"passed": true, "failed": false, "running": false} {
		a.verification = verification{Status: status, Signature: signature}
		if got := a.verified(target); got != want {
			t.Errorf("verified()=%v for status %q; want %v", got, status, want)
		}
	}
	a.verification = verification{Status: "passed", Signature: signature}
	changed := map[string]any{"hosts": []string{"127.0.0.1"}, "endpoint": "https://s3.example"}
	if a.verified(changed) {
		t.Error("a verification for different client machines was accepted")
	}
	if got := a.currentVerification(changed).Status; got != "idle" {
		t.Errorf("stale verification reported as %q; want idle", got)
	}
}

func TestRunUsesConfiguredHosts(t *testing.T) {
	a := &app{sptBin: "spt"}
	target := map[string]any{"hosts": []string{"127.0.0.1", "root@node2"}, "endpoint": "https://s3.example", "bucket": "bench"}
	argv := a.buildArgv("/results/run", "mt-dashboard-test", options{"", 10, "64KiB", 1, 60, ""}, target)
	found := false
	for i, arg := range argv {
		if arg == "--test-hosts" && i+1 < len(argv) {
			found = argv[i+1] == "127.0.0.1,root@node2"
		}
	}
	if !found {
		t.Fatalf("run does not target the configured machines: %v", argv)
	}
	if got := targetHosts(map[string]any{}); got != "127.0.0.1" {
		t.Errorf("default host is %q; want the dashboard host", got)
	}

	a.storage = storageConfig{Hosts: []string{"root@node2", "127.0.0.1"}}
	if got := a.metricsEndpoint(); got != "http://node2:9999/metrics/json" {
		t.Errorf("live metrics read from %q; want the entry node", got)
	}
}

func TestCheckEndpoint(t *testing.T) {
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
	}))
	status, message := checkEndpoint(server.Client(), server.URL)
	if status != "passed" || !strings.Contains(message, "403") {
		t.Errorf("reachable endpoint reported as %q: %s", status, message)
	}
	// A default client does not trust the test server's self-signed certificate.
	status, message = checkEndpoint(nil, server.URL)
	if status != "passed" || !strings.Contains(message, "certificate") {
		t.Errorf("endpoint with an untrusted certificate reported as %q: %s", status, message)
	}
	server.Close()
	if status, _ := checkEndpoint(server.Client(), server.URL); status != "failed" {
		t.Errorf("unreachable endpoint reported as %q", status)
	}
}
