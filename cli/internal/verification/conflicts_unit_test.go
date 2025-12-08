package verification

import (
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"strings"
	"testing"
)

func TestFilterLinesForPort(t *testing.T) {
	lines := []string{
		"tcp LISTEN 0 128 127.0.0.1:1099 0.0.0.0:*",
		"tcp LISTEN 0 128 127.0.0.1:9999 0.0.0.0:*",
	}
	out := filterLinesForPort(lines, 9999)
	if len(out) != 1 || out[0] != lines[1] {
		t.Fatalf("unexpected filter result: %#v", out)
	}
}

func TestParseContainerLine_OK(t *testing.T) {
	rpc := &RemotePortChecker{}
	line := "abcdef123456\tspt-verify-1\tspt:latest\trunning\t1099->1099/tcp,9999->9999/tcp\t2025-01-02 03:04:05 +0000 UTC"
	c, err := rpc.parseContainerLine(line)
	if err != nil {
		t.Fatalf("parse err: %v", err)
	}
	if c.ID != "abcdef123456" || c.Name != "spt-verify-1" || c.Image != "spt:latest" || c.State != "running" {
		t.Fatalf("unexpected fields: %+v", c)
	}
	if len(c.Ports) != 2 {
		t.Fatalf("expected 2 ports, got %d", len(c.Ports))
	}
	if c.Created.IsZero() {
		t.Fatalf("expected created parsed")
	}
}

func TestParseContainerLine_Invalid(t *testing.T) {
	rpc := &RemotePortChecker{}
	if _, err := rpc.parseContainerLine("too\tfew\tparts"); err == nil {
		t.Fatalf("expected error for invalid format")
	}
}

func TestVerificationConflictInfo_SptHelpers(t *testing.T) {
	info := &VerificationConflictInfo{
		Host:          &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
		ConflictPorts: []int{1099, 9999},
		Containers: []*VerificationContainer{
			{ID: "1", Name: "random", Image: "nginx", State: "running"},
			{ID: "2", Name: "spt-verify-abc", Image: "spt:latest", State: "exited"},
		},
	}
	if !info.HasConflicts() {
		t.Fatalf("expected HasConflicts = true")
	}
	desc := info.GetConflictDescription()
	if desc == "" || desc[0:4] != "Port" {
		t.Fatalf("unexpected description: %q", desc)
	}
	m := info.GetSptContainers()
	if len(m) != 1 || m[0].ID != "2" {
		t.Fatalf("expected one spt container, got %#v", m)
	}
}

func TestGenerateSSHStopCommands_LocalAndRemote(t *testing.T) {
	local := &VerificationConflictInfo{Host: &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true}, Containers: []*VerificationContainer{{ID: "1234567890abcdef"}}}
	cmds := local.GenerateSSHStopCommands("")
	if len(cmds) != 1 || cmds[0] != "docker stop 1234567890ab" {
		t.Fatalf("unexpected local command: %#v", cmds)
	}
	remote := &VerificationConflictInfo{Host: &hostparse.HostInfo{User: "root", Host: "10.0.0.1"}, Containers: []*VerificationContainer{{ID: "abcdef0123456789"}}}
	cmds = remote.GenerateSSHStopCommands("")
	if len(cmds) != 1 || cmds[0] != "ssh root@10.0.0.1 'docker stop abcdef012345'" {
		t.Fatalf("unexpected remote command: %#v", cmds)
	}
}

func TestFilterContainersByImage(t *testing.T) {
	vcr := NewVerificationConflictResolver(nil, true)
	in := []*VerificationContainer{
		{ID: "1", Image: "spt:5"},
		{ID: "2", Image: "nginx:latest"},
	}
	match, non := vcr.filterContainersByImage(in, "ghcr.io/dell/storage-performance-tool")
	if len(match) != 1 || len(non) != 1 || match[0].ID != "1" {
		t.Fatalf("unexpected split: match=%#v non=%#v", match, non)
	}
}

func TestGetContainerSummary(t *testing.T) {
	info := &VerificationConflictInfo{
		Containers: []*VerificationContainer{
			{ID: "1", State: "running", Image: "spt"},
			{ID: "2", State: "exited", Image: "busybox"},
		},
	}
	s := info.GetContainerSummary()
	if s == "" || !strings.Contains(s, "2 containers") {
		t.Fatalf("unexpected summary: %q", s)
	}
}
