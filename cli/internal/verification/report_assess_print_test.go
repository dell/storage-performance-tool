package verification

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"
)

func capOut(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	old := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = old }()
	fn()
	_ = w.Close()
	b, _ := io.ReadAll(r)
	return string(b)
}

func TestReportAssess_AllPass(t *testing.T) {
	r := &Report{
		Results: map[string]*NodeResult{
			"hostA": {Overall: Check{Passed: true, Message: "ok"}},
			"hostB": {Overall: Check{Passed: true, Message: "ok"}},
		},
		Config:   Config{MinHosts: 2},
		Duration: 1 * time.Second,
	}
	r.assess()
	if !r.Ready {
		t.Fatalf("expected Ready=true")
	}
	if r.Summary != "All nodes passed verification" {
		t.Fatalf("unexpected summary: %q", r.Summary)
	}

	out := capOut(t, func() { r.Print() })
	if !strings.Contains(out, "READY") {
		t.Fatalf("expected READY in output, got: %s", out)
	}
}

func TestReportAssess_ThresholdMet(t *testing.T) {
	r := &Report{
		Results: map[string]*NodeResult{
			"hostA": {Overall: Check{Passed: true, Message: "ok"}},
			"hostB": {Overall: Check{Passed: false, Message: "oops"}},
			"hostC": {Overall: Check{Passed: true, Message: "ok"}},
		},
		Config:   Config{MinHosts: 2},
		Duration: 500 * time.Millisecond,
	}
	r.assess()
	if !r.Ready {
		t.Fatalf("expected Ready=true")
	}
	if !strings.Contains(r.Summary, "2/3 nodes passed (minimum threshold met)") {
		t.Fatalf("unexpected summary: %q", r.Summary)
	}
}

func TestReportPrint_WarningCheck(t *testing.T) {
	r := &Report{
		Results: map[string]*NodeResult{
			"hostA": {
				SSHConnectivity:  Check{Passed: true, Message: "ok"},
				ClockSkew:        Check{Passed: false, Warning: true, Message: "Clock skew: 2h behind entry node"},
				DockerAvailable:  Check{Passed: true, Message: "ok"},
				DockerImagePull:  Check{Passed: true, Message: "ok"},
				ContainerStart:   Check{Passed: true, Message: "ok"},
				PortsAccessible:  Check{Passed: true, Message: "ok"},
				MetricsEndpoint:  Check{Passed: true, Message: "ok"},
				ControlEndpoint:  Check{Passed: true, Message: "ok"},
				ContainerCleanup: Check{Passed: true, Message: "ok"},
				Overall:          Check{Passed: true, Message: "READY"},
			},
		},
		Config:   Config{MinHosts: 1},
		Duration: 1 * time.Second,
	}

	// capOut uses a pipe (not TTY), so no ANSI codes — just check text
	out := capOut(t, func() { r.Print() })
	if !strings.Contains(out, "WARN") {
		t.Fatalf("expected WARN in output for warning check, got: %s", out)
	}
	if !strings.Contains(out, "Clock skew") {
		t.Fatalf("expected clock skew message in output, got: %s", out)
	}
	// Warnings should NOT show as FAIL
	if strings.Contains(out, "Clock Sync:") && strings.Contains(out, "FAIL") {
		t.Fatalf("warning check should show WARN not FAIL, got: %s", out)
	}
}

func TestReportAssess_NotReady_WithFailuresListed(t *testing.T) {
	r := &Report{
		Results: map[string]*NodeResult{
			"hostA": {Overall: Check{Passed: false, Message: "bad A"}},
			"hostB": {Overall: Check{Passed: true, Message: "ok"}},
			"hostC": {Overall: Check{Passed: false, Message: "bad C"}},
		},
		Config:   Config{MinHosts: 2},
		Duration: 1 * time.Second,
	}
	r.assess()
	if r.Ready {
		t.Fatalf("expected Ready=false")
	}
	if !strings.Contains(r.Summary, "1/3 nodes passed (required: 2)") {
		t.Fatalf("unexpected summary prefix: %q", r.Summary)
	}
	if !strings.Contains(r.Summary, "hostA (bad A)") || !strings.Contains(r.Summary, "hostC (bad C)") {
		t.Fatalf("expected failed nodes listed, got: %q", r.Summary)
	}

	out := capOut(t, func() { r.Print() })
	if !strings.Contains(out, "NOT READY") {
		t.Fatalf("expected NOT READY in output, got: %s", out)
	}
	if !strings.Contains(out, "Node: hostA") || !strings.Contains(out, "Node: hostC") {
		t.Fatalf("expected node sections in output, got: %s", out)
	}
}
