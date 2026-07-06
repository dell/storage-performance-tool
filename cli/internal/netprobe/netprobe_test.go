/*
Copyright © 2025 Dell Technologies
*/

package netprobe

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// mockExec implements command.CommandExecutor for tests
type mockExec struct {
	responses map[string]struct {
		out, errStr string
		err         error
	}
	calls []string
}

func newMockExec() *mockExec {
	return &mockExec{responses: map[string]struct {
		out, errStr string
		err         error
	}{}}
}
func (m *mockExec) set(cmd string, out, errStr string, err error) {
	m.responses[cmd] = struct {
		out, errStr string
		err         error
	}{out, errStr, err}
}

func (m *mockExec) CopyFile(context.Context, *hostparse.HostInfo, string, string) error {
	return nil
}

func (m *mockExec) CopyFromHost(context.Context, *hostparse.HostInfo, string, string) error {
	return nil
}

func (m *mockExec) ExecuteCommand(ctx context.Context, host *hostparse.HostInfo, cmd []string) (string, string, error) {
	key := strings.Join(cmd, " ")
	m.calls = append(m.calls, key)
	if r, ok := m.responses[key]; ok {
		return r.out, r.errStr, r.err
	}
	return "", "", fmt.Errorf("unexpected command: %s", key)
}

func makeHost() *hostparse.HostInfo { return &hostparse.HostInfo{Host: "test-host", IsLocal: false} }

func TestSnapshotListeners_SS_ParseMultiple(t *testing.T) {
	m := newMockExec()
	// Simulate ss -ltnp output (subset)
	ss := strings.Join([]string{
		"State Recv-Q Send-Q Local Address:Port  Peer Address:Port  Process",
		"LISTEN 0 128 0.0.0.0:9999 0.0.0.0:* users:(\"java\",pid=4321,fd=51)",
		"tcp LISTEN 0 128 [::]:1099 [::]:* users:(\"docker-proxy\",pid=2345,fd=8)",
	}, "\n")
	m.set("sh -c "+constants.PortSnapshotCommand, ss, "", nil)

	lst, err := SnapshotListeners(context.Background(), m, makeHost())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(lst) != 2 {
		t.Fatalf("expected 2 listeners, got %d", len(lst))
	}

	// Verify first
	if lst[0].Port != 9999 || lst[0].PID != 4321 || lst[0].Process != "java" {
		t.Errorf("bad parse for #0: %+v", lst[0])
	}
	if !strings.Contains(lst[0].Address, ":9999") {
		t.Errorf("address missing port: %s", lst[0].Address)
	}
	// Verify second
	if lst[1].Port != 1099 || lst[1].PID != 2345 || lst[1].Process != "docker-proxy" {
		t.Errorf("bad parse for #1: %+v", lst[1])
	}
}

func TestSnapshotListeners_Netstat_ParsePIDProc(t *testing.T) {
	m := newMockExec()
	// Simulate netstat -lnp output (subset)
	netstat := strings.Join([]string{
		"Active Internet connections (only servers)",
		"Proto Recv-Q Send-Q Local Address           Foreign Address         State       PID/Program name",
		"tcp        0      0 0.0.0.0:80              0.0.0.0:*               LISTEN      1234/nginx",
		"tcp6       0      0 :::443                   :::*                    LISTEN      9876/httpd",
	}, "\n")
	m.set("sh -c "+constants.PortSnapshotCommand, netstat, "", nil)

	lst, err := SnapshotListeners(context.Background(), m, makeHost())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(lst) != 2 {
		t.Fatalf("expected 2 listeners, got %d", len(lst))
	}
	// nginx
	if lst[0].Port != 80 || lst[0].PID != 1234 || lst[0].Process != "nginx" {
		t.Errorf("bad parse for nginx: %+v", lst[0])
	}
	// httpd (note: process is httpd)
	if lst[1].Port != 443 || lst[1].PID != 9876 || lst[1].Process != "httpd" {
		t.Errorf("bad parse for httpd: %+v", lst[1])
	}
}

func TestSnapshotListeners_ErrorPropagation(t *testing.T) {
	m := newMockExec()
	m.set("sh -c "+constants.PortSnapshotCommand, "", "boom", fmt.Errorf("failed"))
	if _, err := SnapshotListeners(context.Background(), m, makeHost()); err == nil {
		t.Fatalf("expected error, got nil")
	}
}

func TestFilterByPort(t *testing.T) {
	listeners := []Listener{
		{Port: 80, Process: "nginx", PID: 1},
		{Port: 443, Process: "httpd", PID: 2},
		{Port: 80, Process: "docker-proxy", PID: 3},
	}
	out := FilterByPort(listeners, 80)
	if len(out) != 2 {
		t.Fatalf("expected 2 entries for port 80, got %d", len(out))
	}
	for _, l := range out {
		if l.Port != 80 {
			t.Errorf("unexpected port in filter: %d", l.Port)
		}
	}
}

func TestSnapshotListeners_SS_ExoticFormats(t *testing.T) {
	m := newMockExec()
	// Exotic ss lines: nested users, IPv6 address, high recv-q
	ss := strings.Join([]string{
		"tcp LISTEN 0 4096 127.0.0.1:5432 0.0.0.0:* users:(\"postgres\",pid=101,fd=7)",
		"tcp LISTEN 0 50 [fe80::1]:8080 [::]:* users:(\"java\",pid=9090,fd=123)",
		// A line without users section should be ignored for PID/Process, but still parsed for port
		"tcp LISTEN 0 128 0.0.0.0:7000 0.0.0.0:*",
	}, "\n")
	m.set("sh -c "+constants.PortSnapshotCommand, ss, "", nil)

	lst, err := SnapshotListeners(context.Background(), m, makeHost())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(lst) != 3 {
		t.Fatalf("expected 3 listeners, got %d", len(lst))
	}

	// postgres
	if lst[0].Port != 5432 || lst[0].PID != 101 || lst[0].Process != "postgres" {
		t.Errorf("bad postgres parse: %+v", lst[0])
	}
	if !strings.Contains(lst[1].Address, ":8080") || lst[1].PID != 9090 || lst[1].Process != "java" {
		t.Errorf("bad java parse: %+v", lst[1])
	}
	if lst[2].Port != 7000 {
		t.Errorf("expected port 7000 parsed, got %d", lst[2].Port)
	}
}
