/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
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

func TestRunCmdRMIRegistryPortReachesOnlyTheLocalRoute(t *testing.T) {
	tests := []struct {
		name, hosts, minHosts, port string
		wantLocal, wantPort         int
		wantParamPort               int
		wantErr                     string
	}{
		{name: "local default", hosts: "127.0.0.1", minHosts: "1", port: "1099", wantLocal: 1, wantPort: 1},
		{name: "local custom", hosts: "127.0.0.1", minHosts: "1", port: "41099", wantLocal: 1, wantPort: 1, wantParamPort: 41099},
		{name: "lone remote rejected", hosts: "entry.example", minHosts: "1", port: "41099", wantErr: "applies only to local single-host runs"},
		{name: "distributed rejected", hosts: "entry.example,worker.example", minHosts: "2", port: "41099", wantErr: "applies only to local single-host runs"},
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
			connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error {
				connectCalls++
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
				*tui.MultiHostOrchestrator, string, string, scenario.Params, headless.HeadlessOptions,
			) error {
				remoteCalls++
				return errors.New("remote launch must remain unreachable")
			}

			err := runCmd.RunE(runCmd, []string{WorkloadTypeDelete})
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("RunE() error = %v, want %q", err, test.wantErr)
				}
			} else if !errors.Is(err, errRouteComplete) {
				t.Fatalf("RunE() error = %v, want route sentinel", err)
			}
			if localCalls != test.wantLocal || remoteCalls != 0 || connectCalls != 0 || portCalls != test.wantPort {
				t.Fatalf("route calls local=%d remote=%d connect=%d port=%d, want %d/0/0/%d",
					localCalls, remoteCalls, connectCalls, portCalls, test.wantLocal, test.wantPort)
			}
		})
	}
}
