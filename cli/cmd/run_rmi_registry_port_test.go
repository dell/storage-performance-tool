/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
)

func TestRunRMIRegistryPortFlagDefaultsToEngineRegistryPort(t *testing.T) {
	flag := runCmd.Flags().Lookup(flagRMIRegistryPort)
	if flag == nil {
		t.Fatalf("run flag %q not registered", flagRMIRegistryPort)
	}
	if flag.DefValue != strconv.Itoa(constants.DefaultRMIRegistryPort) {
		t.Fatalf("--%s default = %s, want %d", flagRMIRegistryPort, flag.DefValue, constants.DefaultRMIRegistryPort)
	}
}

func TestBuildScenarioParamsRMIRegistryPort(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    int
		wantErr string
	}{
		{name: "default leaves the engine default", want: 0},
		{name: "explicit default leaves the engine default", args: []string{"--rmi-registry-port=1099"}, want: 0},
		{name: "custom port", args: []string{"--rmi-registry-port=41099"}, want: 41099},
		{name: "highest port", args: []string{"--rmi-registry-port=65535"}, want: 65535},
		{name: "zero", args: []string{"--rmi-registry-port=0"}, wantErr: "between 1 and 65535, got 0"},
		{name: "too large", args: []string{"--rmi-registry-port=65536"}, wantErr: "between 1 and 65535, got 65536"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := newRunLikeCmd()
			cmd.Flags().Int(flagRMIRegistryPort, constants.DefaultRMIRegistryPort, "")
			if err := cmd.ParseFlags(tt.args); err != nil {
				t.Fatal(err)
			}
			params, err := buildScenarioParams("write", cmd)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("buildScenarioParams() error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildScenarioParams() error = %v", err)
			}
			if params.RMIRegistryPort != tt.want {
				t.Fatalf("RMIRegistryPort = %d, want %d", params.RMIRegistryPort, tt.want)
			}
		})
	}
}

func TestRunCmdRMIRegistryPortReachesEveryRoute(t *testing.T) {
	tests := []struct {
		name, hosts, minHosts, port string
		wantLocal, wantRemote       int
		wantConnect, wantPort       int
		wantParamPort               int
		wantOrchestratorPort        int
	}{
		{name: "local default", hosts: "127.0.0.1", minHosts: "1", port: "1099", wantLocal: 1, wantPort: 1},
		{name: "local custom", hosts: "127.0.0.1", minHosts: "1", port: "41099", wantLocal: 1, wantPort: 1, wantParamPort: 41099},
		{name: "lone remote default", hosts: "entry.example", minHosts: "1", port: "1099",
			wantRemote: 1, wantConnect: 1, wantOrchestratorPort: 1099},
		{name: "lone remote custom", hosts: "entry.example", minHosts: "1", port: "11099",
			wantRemote: 1, wantConnect: 1, wantParamPort: 11099, wantOrchestratorPort: 11099},
		{name: "distributed custom", hosts: "entry.example,worker.example", minHosts: "2", port: "11099",
			wantRemote: 1, wantConnect: 1, wantParamPort: 11099, wantOrchestratorPort: 11099},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			setDeleteRouteFlags(t, test.hosts, test.minHosts)
			setGlobalRunFlagForTest(t, flagRMIRegistryPort, test.port)

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
			var portCalls, connectCalls, localCalls, remoteCalls int
			resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
				portCalls++
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			connectMultiHostOrchestratorFunc = func(_ context.Context, orchestrator *tui.MultiHostOrchestrator) error {
				connectCalls++
				// The hosts connect, and their ports are checked, on the run's registry port.
				if got := orchestrator.RMIRegistryPort(); got != test.wantOrchestratorPort {
					t.Fatalf("orchestrator RMI registry port at connect = %d, want %d", got, test.wantOrchestratorPort)
				}
				return nil
			}
			errRouteComplete := errors.New("route inspected")
			startLocalHeadlessRunFunc = func(
				_ string, _ string, params scenario.Params, options headless.HeadlessOptions,
			) error {
				localCalls++
				if params.RMIRegistryPort != test.wantParamPort {
					t.Fatalf("local RMIRegistryPort = %d, want %d", params.RMIRegistryPort, test.wantParamPort)
				}
				options.LaunchHooks.NotifySubmitted()
				return errRouteComplete
			}
			startMultiHostHeadlessRunFunc = func(
				orchestrator *tui.MultiHostOrchestrator, _ string, _ string, params scenario.Params,
				options headless.HeadlessOptions,
			) error {
				remoteCalls++
				if params.RMIRegistryPort != test.wantParamPort {
					t.Fatalf("remote RMIRegistryPort = %d, want %d", params.RMIRegistryPort, test.wantParamPort)
				}
				if got := orchestrator.RMIRegistryPort(); got != test.wantOrchestratorPort {
					t.Fatalf("orchestrator RMI registry port = %d, want %d", got, test.wantOrchestratorPort)
				}
				options.LaunchHooks.NotifySubmitted()
				return errRouteComplete
			}

			err := runCmd.RunE(runCmd, []string{WorkloadTypeDelete})
			if !errors.Is(err, errRouteComplete) {
				t.Fatalf("RunE() error = %v, want route sentinel", err)
			}
			if localCalls != test.wantLocal || remoteCalls != test.wantRemote ||
				connectCalls != test.wantConnect || portCalls != test.wantPort {
				t.Fatalf("route calls local=%d remote=%d connect=%d port=%d, want %d/%d/%d/%d",
					localCalls, remoteCalls, connectCalls, portCalls,
					test.wantLocal, test.wantRemote, test.wantConnect, test.wantPort)
			}
		})
	}
}

// Ticket 01: a distributed run whose engines never serve the requested API port fails its start
// with the API wait error (TestDistributedRunFailsWhenTheAPIWaitFails), and spt run exits 1.
func TestRunCmdExitsOneWhenTheDistributedAPIWaitFails(t *testing.T) {
	t.Chdir(t.TempDir())
	setDeleteRouteFlags(t, "entry.example,worker.example", "2")
	setGlobalRunFlagForTest(t, "api-port", "19300")

	previousConnect := connectMultiHostOrchestratorFunc
	previousRemote := startMultiHostHeadlessRunFunc
	t.Cleanup(func() {
		connectMultiHostOrchestratorFunc = previousConnect
		startMultiHostHeadlessRunFunc = previousRemote
	})
	connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error { return nil }
	waitErr := fmt.Errorf("failed waiting for APIs after distributed start: %w",
		errors.New("insufficient APIs ready: 0/2 (need 2)"))
	var launches int
	startMultiHostHeadlessRunFunc = func(
		*tui.MultiHostOrchestrator, string, string, scenario.Params, headless.HeadlessOptions,
	) error {
		launches++
		// The headless runner returns the start error as it is.
		return waitErr
	}

	command := &cobra.Command{
		Use:           "run",
		SilenceErrors: true,
		SilenceUsage:  true,
		RunE: func(*cobra.Command, []string) error {
			return runCmd.RunE(runCmd, []string{WorkloadTypeDelete})
		},
	}
	if code := executeCommandCode(command); code != 1 {
		t.Fatalf("exit status = %d, want 1", code)
	}
	if launches != 1 {
		t.Fatalf("launches = %d, want 1", launches)
	}
	if err := runCmd.RunE(runCmd, []string{WorkloadTypeDelete}); !errors.Is(err, waitErr) {
		t.Fatalf("RunE() error = %v, want the API wait failure", err)
	}
}
