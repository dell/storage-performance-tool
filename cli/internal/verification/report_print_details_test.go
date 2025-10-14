package verification

import (
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
)

func captureOut(t *testing.T, fn func()) string {
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

// Ensures the detailed failure block with command, stdout and stderr prints
func TestPrintCheck_FailureWithCommandResult(t *testing.T) {
	r := &Report{
		Results: map[string]*NodeResult{
			"n1": {
				SSHConnectivity: Check{Passed: false, Message: "cannot connect", CommandResult: &command.CommandResult{
					Command: "docker ps",
					Stdout:  "daemon info line 1\ndaemon info line 2",
					Stderr:  "permission denied\nother error",
				}},
				Overall: Check{Passed: false, Message: "overall failed"},
			},
		},
		Config:   Config{MinHosts: 1},
		Duration: 123 * time.Millisecond,
	}

	// Force assess so summary is set (though not strictly needed for this branch)
	r.assess()

	out := captureOut(t, func() { r.Print() })
	// verify sections emitted
	if !strings.Contains(out, "Failed Command: docker ps") {
		t.Fatalf("expected command line in output, got: %s", out)
	}
	if !strings.Contains(out, "Standard Output:") || !strings.Contains(out, "daemon info line 1") {
		t.Fatalf("expected stdout lines in output, got: %s", out)
	}
	if !strings.Contains(out, "Error Output:") || !strings.Contains(out, "permission denied") {
		t.Fatalf("expected stderr lines in output, got: %s", out)
	}
}
