/*
Copyright © 2025 Dell Technologies
*/

package verification

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

func Test_extractPIDFromLines(t *testing.T) {
	cases := []struct {
		in   []string
		want string
	}{
		{[]string{"LISTEN ... users:(\"java\",pid=4321,fd=51)"}, "4321"},
		{[]string{"tcp 0 0 0.0.0.0:80 ... LISTEN  1234/nginx"}, "1234"},
		{[]string{"garbage line"}, ""},
	}
	for _, c := range cases {
		if got := extractPIDFromLines(c.in); got != c.want {
			t.Errorf("extractPIDFromLines(%v)=%q, want %q", c.in, got, c.want)
		}
	}
}

func Test_formatSuggestedActions_ContainersAndPID(t *testing.T) {
	info := &VerificationConflictInfo{
		Host:          &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true},
		ConflictPorts: []int{1099, 9999},
		PortDetails: map[int][]string{
			9999: {"LISTEN 0 128 0.0.0.0:9999 0.0.0.0:* users:(\"java\",pid=5555,fd=1)"},
		},
		Containers: []*VerificationContainer{
			{ID: "abc123def456", Name: "spt-verify-x", Ports: []string{"0.0.0.0:1099->1099/tcp"}},
		},
	}
	out := formatSuggestedActions(info)
	if out == "" {
		t.Fatalf("expected suggestions, got empty")
	}
	if !containsAll(out, []string{"docker stop", "Port 1099"}) {
		t.Errorf("missing docker stop suggestion: %s", out)
	}
	if !containsAll(out, []string{"kill -TERM", "Port 9999"}) {
		t.Errorf("missing PID kill suggestion: %s", out)
	}
}

func containsAll(s string, parts []string) bool {
	for _, p := range parts {
		if !strings.Contains(s, p) {
			return false
		}
	}
	return true
}

func Test_formatSuggestedActions_RemoteHost(t *testing.T) {
	info := &VerificationConflictInfo{
		Host:          &hostparse.HostInfo{User: "root", Host: "remote.example.com", IsLocal: false},
		ConflictPorts: []int{1099, 9999},
		PortDetails: map[int][]string{
			9999: {"tcp LISTEN 0 128 0.0.0.0:9999 0.0.0.0:* users:(\"java\",pid=7777,fd=3)"},
		},
		Containers: []*VerificationContainer{
			{ID: "abcdef0123456789", Name: "spt-verify-y", Ports: []string{"0.0.0.0:1099->1099/tcp"}},
		},
	}
	out := formatSuggestedActions(info)
	if out == "" {
		t.Fatalf("expected suggestions for remote host, got empty")
	}
	// Should include ssh docker stop for port 1099
	if !containsAll(out, []string{"Port 1099", "ssh root@remote.example.com", "docker stop", "spt-verify-y"}) {
		t.Errorf("missing remote docker stop suggestion: %s", out)
	}
	// Should include ssh kill -TERM for PID 7777 on port 9999
	if !containsAll(out, []string{"Port 9999", "ssh root@remote.example.com", "kill -TERM", "7777"}) {
		t.Errorf("missing remote PID kill suggestion: %s", out)
	}
}

func TestDisplayConflictInfo_ShowsForceCleanupTip(t *testing.T) {
	// Build a conflict info with a Spt container present
	info := &VerificationConflictInfo{
		Host:          &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true},
		ConflictPorts: []int{1099},
		Containers: []*VerificationContainer{
			{ID: "abcdef0123456789", Name: "spt-verify-z", Image: "dcr.octo.dell.com/spt/spt", Ports: []string{"0.0.0.0:1099->1099/tcp"}},
		},
		PortDetails: map[int][]string{
			1099: {"tcp LISTEN 0 128 0.0.0.0:1099 0.0.0.0:* users:(\"docker-proxy\",pid=2222,fd=2)"},
		},
	}
	// Create resolver (executor not used by display)
	vcr := NewVerificationConflictResolver(nil, false)

	// Capture stdout
	old := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w
	vcr.displayConflictInfo(info)
	_ = w.Close()
	os.Stdout = old
	outBytes, _ := io.ReadAll(r)
	out := string(outBytes)

	if !strings.Contains(out, "Tip: re-run verify with --force-cleanup") {
		t.Fatalf("expected force-cleanup tip in output, got:\n%s", out)
	}
}
