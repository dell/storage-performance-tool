package cmd

import (
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/verification"
	"github.com/spf13/cobra"
)

// helper to build a minimal Cobra command wired to runVerify with the same flags
func newVerifyTestCmd() *cobra.Command {
	c := &cobra.Command{Use: "verify", RunE: runVerify}
	registerVerifyFlags(c.Flags())
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
			interfaces, endpoints, err := verifyNicPlanInputs(cmd, tt.interfaces, "host", false)
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

// stubVerifyHosts replaces verification for one test and records the configurations runVerify
// passes to it.
func stubVerifyHosts(t *testing.T) *[]verification.Config {
	t.Helper()
	var configs []verification.Config
	previous := verifyHostsFunc
	verifyHostsFunc = func(_ []*hostparse.HostInfo, config verification.Config) *verification.Report {
		configs = append(configs, config)
		return &verification.Report{Results: map[string]*verification.NodeResult{}, Ready: true}
	}
	t.Cleanup(func() { verifyHostsFunc = previous })
	return &configs
}

func TestRunVerify_EnginePortsReachVerifier(t *testing.T) {
	tests := []struct {
		name                  string
		flags                 map[string]string
		wantAPI, wantRegistry int
	}{
		{"defaults", nil, constants.DefaultSptAPIPort, constants.DefaultRMIRegistryPort},
		{"api port", map[string]string{flagAPIPort: "19100"}, 19100, constants.DefaultRMIRegistryPort},
		{"api and registry ports", map[string]string{flagAPIPort: "19100", flagRMIRegistryPort: "11099"}, 19100, 11099},
		{"highest ports", map[string]string{flagAPIPort: "65535", flagRMIRegistryPort: "65534"}, 65535, 65534},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configs := stubVerifyHosts(t)
			cmd := newVerifyTestCmd()
			if err := cmd.Flags().Set("test-hosts", "127.0.0.1"); err != nil {
				t.Fatal(err)
			}
			for flag, value := range tt.flags {
				if err := cmd.Flags().Set(flag, value); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			captureStdout(t, func() { err = runVerify(cmd, nil) })
			if err != nil {
				t.Fatalf("runVerify: %v", err)
			}
			if len(*configs) != 1 {
				t.Fatalf("verified %d times, want 1", len(*configs))
			}
			got := (*configs)[0]
			if got.APIPort != tt.wantAPI || got.RMIRegistryPort != tt.wantRegistry {
				t.Fatalf("APIPort %d, RMIRegistryPort %d; want %d, %d", got.APIPort, got.RMIRegistryPort, tt.wantAPI, tt.wantRegistry)
			}
		})
	}
}

func TestRunVerify_RejectsOutOfRangePorts(t *testing.T) {
	tests := []struct {
		flag, value, want string
	}{
		{flagAPIPort, "0", "--api-port must be between 1 and 65535, got 0"},
		{flagAPIPort, "-1", "--api-port must be between 1 and 65535, got -1"},
		{flagAPIPort, "65536", "--api-port must be between 1 and 65535, got 65536"},
		{flagRMIRegistryPort, "0", "--rmi-registry-port must be between 1 and 65535, got 0"},
		{flagRMIRegistryPort, "65536", "--rmi-registry-port must be between 1 and 65535, got 65536"},
	}
	for _, tt := range tests {
		t.Run(tt.flag+"="+tt.value, func(t *testing.T) {
			configs := stubVerifyHosts(t)
			cmd := newVerifyTestCmd()
			if err := cmd.Flags().Set("test-hosts", "127.0.0.1"); err != nil {
				t.Fatal(err)
			}
			if err := cmd.Flags().Set(tt.flag, tt.value); err != nil {
				t.Fatal(err)
			}
			err := runVerify(cmd, nil)
			if err == nil || err.Error() != tt.want {
				t.Fatalf("runVerify error = %v, want %q", err, tt.want)
			}
			if len(*configs) != 0 {
				t.Fatal("verified a host with an out-of-range port")
			}
		})
	}
}

// Verify takes a non-default --rmi-registry-port on any hosts, remote and several included: its
// own test engine serves the registry there. spt run refuses one on remote hosts until
// .scratch/distributed-run-api-port 02 lifts that.
func TestRunVerify_RegistryPortReachesVerifierOnAnyHosts(t *testing.T) {
	for _, hosts := range []string{"127.0.0.1", "entry.example", "entry.example,worker.example"} {
		t.Run(hosts, func(t *testing.T) {
			configs := stubVerifyHosts(t)
			cmd := newVerifyTestCmd()
			for flag, value := range map[string]string{"test-hosts": hosts, flagRMIRegistryPort: "11099"} {
				if err := cmd.Flags().Set(flag, value); err != nil {
					t.Fatal(err)
				}
			}
			var err error
			captureStdout(t, func() { err = runVerify(cmd, nil) })
			if err != nil {
				t.Fatalf("runVerify: %v", err)
			}
			if len(*configs) != 1 || (*configs)[0].RMIRegistryPort != 11099 {
				t.Fatalf("verifier configs = %+v, want one with registry port 11099", *configs)
			}
		})
	}
}

// Verify's --rmi-registry-port is spt run's flag: same name, default and help text.
func TestVerifyRMIRegistryPortFlagMatchesRun(t *testing.T) {
	verifyFlag := verifyCmd.Flags().Lookup(flagRMIRegistryPort)
	runFlag := runCmd.Flags().Lookup(flagRMIRegistryPort)
	if verifyFlag == nil || runFlag == nil {
		t.Fatalf("--%s registered on verify %t, run %t", flagRMIRegistryPort, verifyFlag != nil, runFlag != nil)
	}
	if verifyFlag.DefValue != strconv.Itoa(constants.DefaultRMIRegistryPort) || verifyFlag.DefValue != runFlag.DefValue {
		t.Errorf("default: verify %s, run %s, want %d", verifyFlag.DefValue, runFlag.DefValue, constants.DefaultRMIRegistryPort)
	}
	if verifyFlag.Usage != runFlag.Usage {
		t.Errorf("help text differs: verify %q, run %q", verifyFlag.Usage, runFlag.Usage)
	}
	if apiFlag := verifyCmd.Flags().Lookup(flagAPIPort); apiFlag == nil || apiFlag.DefValue != strconv.Itoa(constants.DefaultSptAPIPort) {
		t.Errorf("--%s = %+v, want default %d", flagAPIPort, apiFlag, constants.DefaultSptAPIPort)
	}
}
