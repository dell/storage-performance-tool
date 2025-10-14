package cmd

import (
	"io"
	"os"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/spf13/cobra"
)

// helper to build a minimal Cobra command wired to runVerify with the same flags
func newVerifyTestCmd() *cobra.Command {
	c := &cobra.Command{Use: "verify", RunE: runVerify}
	c.Flags().String("test-hosts", "", "")
	c.Flags().Int("min-hosts", 0, "")
	c.Flags().String("network-mode", "bridge", "")
	c.Flags().Int("api-port", 9999, "")
	c.Flags().Int("rmi-port-start", 40000, "")
	c.Flags().Int("rmi-port-count", 10, "")
	c.Flags().Bool("force-cleanup", false, "")
	return c
}

func TestRunVerify_InvalidNetworkMode(t *testing.T) {
	cmd := newVerifyTestCmd()
	cmd.Flags().Set("network-mode", "invalid-mode")
	// keep defaults for everything else
	if err := runVerify(cmd, nil); err == nil {
		t.Fatalf("expected error for invalid network-mode, got nil")
	} else if !strings.Contains(err.Error(), "network-mode must be 'bridge' or 'host'") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunVerify_MinHostsGreaterThanHosts(t *testing.T) {
	cmd := newVerifyTestCmd()
	// Default hosts => localhost only; set min-hosts to 2 to trigger validation error
	cmd.Flags().Set("min-hosts", "2")
	if err := runVerify(cmd, nil); err == nil {
		t.Fatalf("expected error for min-hosts > hosts, got nil")
	} else if !strings.Contains(err.Error(), "min-hosts (2) cannot be greater") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunVerify_InvalidHostSpec(t *testing.T) {
	cmd := newVerifyTestCmd()
	cmd.Flags().Set("test-hosts", "user@") // invalid user@host format
	if err := runVerify(cmd, nil); err == nil {
		t.Fatalf("expected error for invalid host specification, got nil")
	} else if !strings.Contains(err.Error(), "invalid host specification") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe error: %v", err)
	}
	old := os.Stdout
	os.Stdout = w
	defer func() { os.Stdout = old }()

	fn()

	_ = w.Close()
	out, _ := io.ReadAll(r)
	return string(out)
}

func TestPrintStartupMessage(t *testing.T) {
	// single local host
	hosts, err := hostparse.ParseTestHosts("")
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	out := captureStdout(t, func() { printStartupMessage(hosts) })
	if !strings.Contains(out, "Starting verification of localhost") {
		t.Fatalf("expected startup message for localhost, got: %s", out)
	}

	// multi-host
	hosts, err = hostparse.ParseTestHosts("user@10.0.0.1,10.0.0.2")
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	out = captureStdout(t, func() { printStartupMessage(hosts) })
	if !strings.Contains(out, "Starting parallel verification of 2 hosts") {
		t.Fatalf("expected parallel startup message, got: %s", out)
	}
}
