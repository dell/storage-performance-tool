/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
)

// Every engine serves its API and RMI registry ports, so equal ports fail the second bind in
// each one, and the run used to end on a readiness timeout. spt run refuses them on every route,
// before any port check, connection or launch.
func TestRunCmdRefusesEqualAPIAndRegistryPortsOnEveryRoute(t *testing.T) {
	routes := []struct{ name, hosts, minHosts string }{
		{"local", "127.0.0.1", "1"},
		{"lone remote", "entry.example", "1"},
		{"distributed", "entry.example,worker.example", "2"},
	}
	ports := []struct{ name, api, registry, want string }{
		{"both set", "11099", "11099", "--api-port and --rmi-registry-port are both 11099; every engine serves both ports, so they must differ"},
		{"API port on the default registry port", "1099", "1099", "--api-port and --rmi-registry-port are both 1099"},
		{"registry port on the default API port", "", "9999", "--api-port and --rmi-registry-port are both 9999"},
	}
	for _, route := range routes {
		for _, port := range ports {
			t.Run(route.name+", "+port.name, func(t *testing.T) {
				t.Chdir(t.TempDir())
				setDeleteRouteFlags(t, route.hosts, route.minHosts)
				setGlobalRunFlagForTest(t, "api-port", port.api)
				setGlobalRunFlagForTest(t, flagRMIRegistryPort, port.registry)

				previousPort := resolvePortConflictFunc
				previousConnect := connectMultiHostOrchestratorFunc
				previousLocal := startLocalHeadlessRunFunc
				previousRemote := startMultiHostHeadlessRunFunc
				t.Cleanup(func() {
					resolvePortConflictFunc = previousPort
					connectMultiHostOrchestratorFunc = previousConnect
					startLocalHeadlessRunFunc = previousLocal
					startMultiHostHeadlessRunFunc = previousRemote
				})
				var calls int
				errReached := errors.New("the run reached its route")
				resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
					calls++
					return nil, errReached
				}
				connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error {
					calls++
					return errReached
				}
				startLocalHeadlessRunFunc = func(string, string, scenario.Params, headless.HeadlessOptions) error {
					calls++
					return errReached
				}
				startMultiHostHeadlessRunFunc = func(
					*tui.MultiHostOrchestrator, string, string, scenario.Params, headless.HeadlessOptions,
				) error {
					calls++
					return errReached
				}

				err := runCmd.RunE(runCmd, []string{WorkloadTypeDelete})
				if err == nil || !strings.Contains(err.Error(), port.want) {
					t.Fatalf("RunE() error = %v, want %q", err, port.want)
				}
				if calls != 0 {
					t.Fatalf("the run checked ports, connected or launched %d time(s) before refusing", calls)
				}
			})
		}
	}
}

// Replay has no --rmi-registry-port: its engines serve the registry on 1099, so an --api-port of
// 1099 is refused before replay reads its source.
func TestReplayCommandRefusesTheRegistryPortAsItsAPIPort(t *testing.T) {
	var requests atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer source.Close()

	for _, hosts := range []string{"127.0.0.1", "entry.example", "entry.example,worker.example"} {
		t.Run(hosts, func(t *testing.T) {
			var out bytes.Buffer
			cmd := newReplayCommandForTest(t)
			cmd.SetOut(&out)
			cmd.SetArgs([]string{"--from", source.URL, "--endpoints", "http://192.0.2.1:9020", "--test-hosts", hosts,
				"--api-port", "1099", "--results-dir", t.TempDir()})
			err := cmd.Execute()
			want := "--api-port 1099 is the engines' RMI registry port; every engine serves both ports, so choose another API port"
			if err == nil || err.Error() != want {
				t.Fatalf("Execute() error = %v, want %q", err, want)
			}
		})
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("replay read its source %d time(s) before refusing the port", got)
	}
}

// Verify's test engine serves both ports too, so verify refuses equal ones before it verifies.
func TestRunVerify_RejectsEqualAPIAndRegistryPorts(t *testing.T) {
	tests := []struct {
		name  string
		flags map[string]string
		port  string
	}{
		{"both set", map[string]string{flagAPIPort: "11099", flagRMIRegistryPort: "11099"}, "11099"},
		{"API port on the default registry port", map[string]string{flagAPIPort: "1099"}, "1099"},
		{"registry port on the default API port", map[string]string{flagRMIRegistryPort: "9999"}, "9999"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			configs := stubVerifyHosts(t)
			cmd := newVerifyTestCmd()
			for flag, value := range tt.flags {
				if err := cmd.Flags().Set(flag, value); err != nil {
					t.Fatal(err)
				}
			}
			want := "--api-port and --rmi-registry-port are both " + tt.port + "; every engine serves both ports, so they must differ"
			if err := runVerify(cmd, nil); err == nil || err.Error() != want {
				t.Fatalf("runVerify error = %v, want %q", err, want)
			}
			if len(*configs) != 0 {
				t.Fatal("verified a host with equal API and registry ports")
			}
		})
	}
}
