package verification

import (
	"context"
	"testing"

	dockerconf "github.com/dell/storage-performance-tool/cli/internal/docker"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// mockPreflight implements preflight.Checker for verifier conflict tests
type mockPreflight struct {
	info *dockerconf.ConflictInfo
	err  error
}

func (m mockPreflight) CheckDocker(context.Context, *hostparse.HostInfo) (string, error) {
	return "24.0.0", nil
}
func (m mockPreflight) EnsureImage(context.Context, *hostparse.HostInfo, string) error { return nil }
func (m mockPreflight) CheckPorts(context.Context, *hostparse.HostInfo, int, int, int) (*dockerconf.ConflictInfo, error) {
	return m.info, m.err
}

func newHost() *hostparse.HostInfo {
	return &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"}
}

func TestCheckForPortConflicts_NoConflicts(t *testing.T) {
	host := newHost()
	info := &dockerconf.ConflictInfo{Host: host}
	exec := command.NewMockCommandExecutor()
	v := NewVerifierWithDepsAndPreflight([]*hostparse.HostInfo{host}, Config{APIPort: 9999}, exec, &RealNetworkChecker{}, &RealTimeProvider{}, mockPreflight{info: info})

	out := v.checkForPortConflicts(host)
	if !out.Passed || out.Message != "No port conflicts detected" {
		t.Fatalf("unexpected check result: %+v", out)
	}
}

func TestCheckForPortConflicts_ConflictsNoForce(t *testing.T) {
	host := newHost()
	info := &dockerconf.ConflictInfo{Host: host, ConflictPorts: []int{9999}}
	exec := command.NewMockCommandExecutor()
	v := NewVerifierWithDepsAndPreflight([]*hostparse.HostInfo{host}, Config{APIPort: 9999, ForceCleanup: false}, exec, &RealNetworkChecker{}, &RealTimeProvider{}, mockPreflight{info: info})

	out := v.checkForPortConflicts(host)
	if out.Passed || out.Error == nil {
		t.Fatalf("expected failure for conflicts without force, got: %+v", out)
	}
}

func TestCheckForPortConflicts_ForceCleanupResolved(t *testing.T) {
	host := newHost()
	info := &dockerconf.ConflictInfo{Host: host, ConflictPorts: []int{9999}, Containers: []*dockerconf.ContainerInfo{{ID: "abcdef1234567890", Name: "spt-verify-1", Image: "spt:latest", State: "running"}}}
	exec := command.NewMockCommandExecutor()
	// Expect rm -f c1 to succeed
	exec.SetCommandSuccess("docker rm -f abcdef1234567890", "abcdef1234567890")
	v := NewVerifierWithDepsAndPreflight([]*hostparse.HostInfo{host}, Config{APIPort: 9999, ForceCleanup: true}, exec, &RealNetworkChecker{}, &RealTimeProvider{}, mockPreflight{info: info})

	out := v.checkForPortConflicts(host)
	if !out.Passed {
		t.Fatalf("expected success after force cleanup, got: %+v", out)
	}
}

func TestCheckForPortConflicts_ForceCleanupNoMatches(t *testing.T) {
	host := newHost()
	// Non-spt container so GetSptContainers() yields none
	info := &dockerconf.ConflictInfo{Host: host, ConflictPorts: []int{9999}, Containers: []*dockerconf.ContainerInfo{{ID: "x", Name: "random", Image: "nginx", State: "running"}}}
	exec := command.NewMockCommandExecutor()
	v := NewVerifierWithDepsAndPreflight([]*hostparse.HostInfo{host}, Config{APIPort: 9999, ForceCleanup: true}, exec, &RealNetworkChecker{}, &RealTimeProvider{}, mockPreflight{info: info})

	out := v.checkForPortConflicts(host)
	if out.Passed {
		t.Fatalf("expected failure when no matching containers for force cleanup")
	}
}
