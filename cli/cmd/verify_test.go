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
	c.Flags().String("network-mode", "host", "")
	c.Flags().Int("api-port", 9999, "")
	c.Flags().Int("rmi-port-start", 40000, "")
	c.Flags().Int("rmi-port-count", 10, "")
	c.Flags().Bool("force-cleanup", false, "")
	c.Flags().String(flagInterfaces, "", "")
	c.Flags().StringSliceP("endpoints", "e", []string{}, "")
	c.Flags().Bool("slice-endpoints", false, "")
	c.Flags().IntP("threads", "t", 1, "")
	return c
}

func TestRunVerify_InterfacesRejected(t *testing.T) {
	tests := []struct {
		name  string
		flags map[string]string
		want  string
	}{
		{"bridge network", map[string]string{"network-mode": "bridge", "endpoints": "http://192.0.2.101:9000"},
			"--interfaces needs --network-mode host: a bridge-network container sees only its own interface"},
		{"no endpoints", map[string]string{},
			"--interfaces needs the S3 endpoints: pass --endpoints or set S3_ENDPOINTS"},
		{"bad entry", map[string]string{flagInterfaces: "ens2np0;reboot", "endpoints": "http://192.0.2.101:9000"},
			`--interfaces entry "ens2np0;reboot" is not an interface name`},
		{"bad endpoint", map[string]string{"endpoints": "ftp://192.0.2.101"},
			`invalid endpoint URL: unsupported scheme "ftp"`},
		{"mixed schemes", map[string]string{"endpoints": "http://192.0.2.101:9000,https://192.0.2.102:9021"},
			"all endpoints must use the same scheme"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newVerifyTestCmd()
			if err := cmd.Flags().Set("test-hosts", "127.0.0.1"); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Flags().Set(flagInterfaces, "auto"); err != nil {
				t.Fatal(err)
			}
			for flag, value := range tt.flags {
				if err := cmd.Flags().Set(flag, value); err != nil {
					t.Fatal(err)
				}
			}
			err := runVerify(cmd, nil)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("runVerify error = %v, want %q", err, tt.want)
			}
		})
	}
}

func TestVerifyNicPlanInputs(t *testing.T) {
	tests := []struct {
		name, interfaces, endpoints string
		wantInterfaces, wantEnds    []string
	}{
		{"no interfaces reads no endpoints", "", "ftp://not-read", nil, nil},
		{"uniform ports", "auto", "http://192.0.2.101:9000,http://192.0.2.102:9000,http://192.0.2.101:9000",
			[]string{"auto"}, []string{"192.0.2.101:9000", "192.0.2.102:9000"}},
		{"default port", "ens2np0,mlx5_1", "https://192.0.2.101",
			[]string{"ens2np0", "mlx5_1"}, []string{"192.0.2.101:443"}},
		{"mixed ports", "auto", "http://192.0.2.101:9000,http://192.0.2.102:9020",
			[]string{"auto"}, []string{"192.0.2.101:9000", "192.0.2.102:9020"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newVerifyTestCmd()
			if err := cmd.Flags().Set("endpoints", tt.endpoints); err != nil {
				t.Fatal(err)
			}
			interfaces, endpoints, err := verifyNicPlanInputs(cmd, tt.interfaces, "host")
			if err != nil {
				t.Fatalf("verifyNicPlanInputs: %v", err)
			}
			if strings.Join(interfaces, ",") != strings.Join(tt.wantInterfaces, ",") || strings.Join(endpoints, ",") != strings.Join(tt.wantEnds, ",") {
				t.Fatalf("got %v %v, want %v %v", interfaces, endpoints, tt.wantInterfaces, tt.wantEnds)
			}
		})
	}
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
	// Set localhost explicitly so a developer HOSTS environment variable cannot
	// turn this validation-only test into a real distributed verification.
	if err := cmd.Flags().Set("test-hosts", "127.0.0.1"); err != nil {
		t.Fatalf("set test-hosts: %v", err)
	}
	if err := cmd.Flags().Set("min-hosts", "2"); err != nil {
		t.Fatalf("set min-hosts: %v", err)
	}
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
