package verification

import (
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"testing"
)

func TestVerifier_assessNode_FailureOnCritical(t *testing.T) {
	v := &Verifier{}
	r := &NodeResult{SSHConnectivity: Check{Passed: false, Message: "no ssh"}}
	out := v.assessNode(r)
	if out.Passed {
		t.Fatalf("expected failure, got %+v", out)
	}
	if out.Message != "no ssh" {
		t.Fatalf("expected message propagated, got %q", out.Message)
	}
}

func TestVerifier_assessNode_PassWithEndpointIssues(t *testing.T) {
	v := &Verifier{}
	r := &NodeResult{
		SSHConnectivity:  Check{Passed: true},
		DockerAvailable:  Check{Passed: true},
		ContainerStart:   Check{Passed: true},
		PortsAccessible:  Check{Passed: true},
		ContainerCleanup: Check{Passed: true},
		MetricsEndpoint:  Check{Passed: false},
		ControlEndpoint:  Check{Passed: true},
	}
	out := v.assessNode(r)
	if !out.Passed {
		t.Fatalf("expected pass with warnings, got %+v", out)
	}
}

func TestVerifier_stopContainer_NoID(t *testing.T) {
	v := &Verifier{}
	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true}
	out := v.stopContainer(host, "")
	if !out.Passed || out.Message != "No container to clean up" {
		t.Fatalf("unexpected stopContainer result: %+v", out)
	}
}
