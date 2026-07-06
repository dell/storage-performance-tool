/*
Copyright © 2025 Dell Technologies
*/

package verification

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

func getDockerPSCommand() string {
	return fmt.Sprintf("%s %s %s %s %s",
		constants.DockerCommand,
		constants.DockerCmdPS,
		constants.DockerFlagAll,
		constants.DockerFlagFormat,
		constants.DockerConflictFormat)
}

// Minimal command executor mock for verification tests
type miniExec struct {
	responses map[string]struct {
		out, errStr string
		err         error
	}
	cmds [][]string
}

func newMiniExec() *miniExec {
	return &miniExec{responses: map[string]struct {
		out, errStr string
		err         error
	}{}}
}
func (m *miniExec) set(cmd string, out, errStr string, err error) {
	m.responses[cmd] = struct {
		out, errStr string
		err         error
	}{out, errStr, err}
}
func (m *miniExec) CopyFile(context.Context, *hostparse.HostInfo, string, string) error {
	return nil
}

func (m *miniExec) CopyFromHost(context.Context, *hostparse.HostInfo, string, string) error {
	return nil
}

func (m *miniExec) ExecuteCommand(ctx context.Context, host *hostparse.HostInfo, command []string) (string, string, error) {
	m.cmds = append(m.cmds, command)
	key := strings.Join(command, " ")
	if r, ok := m.responses[key]; ok {
		return r.out, r.errStr, r.err
	}
	// defaults
	if strings.HasPrefix(key, "sh -c ") && strings.Contains(key, "ss -ltnp") {
		return "", "", nil
	}
	if strings.HasPrefix(key, "sh -c ") && strings.Contains(key, "netstat -lnp") {
		return "", "", nil
	}
	if strings.HasPrefix(key, "docker ") {
		return "", "", nil
	}
	return "", "", fmt.Errorf("unexpected: %s", key)
}

func TestRemotePortChecker_NoConflicts_Snapshot(t *testing.T) {
	exec := newMiniExec()
	host := &hostparse.HostInfo{Host: "h", IsLocal: false}
	checker := NewRemotePortChecker(host, exec, []int{1099, 9999})
	info, err := checker.CheckForConflicts(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if info.HasConflicts() {
		t.Fatalf("unexpected conflicts: %v", info.ConflictPorts)
	}
	if len(exec.cmds) == 0 || strings.Join(exec.cmds[0], " ") != "sh -c "+constants.PortSnapshotCommand {
		t.Fatalf("expected snapshot command, got %v", exec.cmds)
	}
}

func TestRemotePortChecker_WithConflicts_Snapshot(t *testing.T) {
	exec := newMiniExec()
	snapshot := "tcp LISTEN 0 128 0.0.0.0:1099 0.0.0.0:* users:(\"java\",pid=1,fd=3)\n"
	exec.set("sh -c "+constants.PortSnapshotCommand, snapshot, "", nil)
	dockerOut := "abc\tspt-verify\t" + constants.DefaultSptImage + "\trunning\t0.0.0.0:1099->1099/tcp\t2024-01-01 12:00:00 +0000 UTC"
	exec.set(getDockerPSCommand(), dockerOut, "", nil)
	host := &hostparse.HostInfo{Host: "h", IsLocal: false}
	checker := NewRemotePortChecker(host, exec, []int{1099, 9999})
	info, err := checker.CheckForConflicts(context.Background())
	if err != nil {
		t.Fatalf("err: %v", err)
	}
	if !info.HasConflicts() || len(info.ConflictPorts) != 1 || info.ConflictPorts[0] != 1099 {
		t.Fatalf("bad conflicts: %v", info.ConflictPorts)
	}
	if len(info.AvailablePorts) != 1 || info.AvailablePorts[0] != 9999 {
		t.Fatalf("bad available: %v", info.AvailablePorts)
	}
}

func TestVerificationConflictResolver_CleanupFlow(t *testing.T) {
	exec := newMiniExec()
	// initial snapshot shows conflict
	exec.set("sh -c "+constants.PortSnapshotCommand, "tcp LISTEN 0 128 0.0.0.0:1099 0.0.0.0:*\n", "", nil)
	// docker ps shows spt container; cleanup removes it
	exec.set(getDockerPSCommand(), "abc\tspt-verify\t"+constants.DefaultSptImage+"\trunning\t0.0.0.0:1099->1099/tcp\t2024-01-01 12:00:00 +0000 UTC", "", nil)
	exec.set("docker rm -f abc", "abc", "", nil)
	// after cleanup snapshot returns no listeners
	exec.set("sh -c "+constants.PortSnapshotCommand, "", "", nil)

	resolver := NewVerificationConflictResolver(exec, true)
	host := &hostparse.HostInfo{Host: "h", IsLocal: false}
	res, err := resolver.CheckAndResolveConflicts(context.Background(), host, []int{1099})
	if err != nil {
		t.Fatalf("resolver err: %v", err)
	}
	if !res.Success {
		t.Fatalf("expected success: %s", res.Message)
	}
}
