/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"errors"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
)

// A two-host spt run, and a multi-host LIST, with --network-mode bridge hand the orchestrator a
// bridge network, and never take the local route. That orchestrator starts the remote entry with
// its API port published (tui.TestABridgeDistributedRunPublishesTheRemoteEntrysAPIPort and
// tui.TestABridgeListRunPublishesTheRemotePrimarysAPIPort).
func TestRunCmdStartsABridgeRemoteEntryThroughTheOrchestrator(t *testing.T) {
	for _, workload := range []string{WorkloadTypeDelete, WorkloadTypeList} {
		t.Run(workload, func(t *testing.T) {
			t.Chdir(t.TempDir())
			setDeleteRouteFlags(t, "entry.example,worker.example", "2")
			setGlobalRunFlagForTest(t, "network-mode", constants.BridgeNetworkMode)
			setGlobalRunFlagForTest(t, "api-port", "19412")

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
			var localCalls, remoteCalls int
			resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error { return nil }
			errRouteComplete := errors.New("route inspected")
			startLocalHeadlessRunFunc = func(_ string, _ string, _ scenario.Params, options headless.HeadlessOptions) error {
				localCalls++
				options.LaunchHooks.NotifySubmitted()
				return errRouteComplete
			}
			var networkMode string
			var hostCount int
			startMultiHostHeadlessRunFunc = func(
				orchestrator *tui.MultiHostOrchestrator, _ string, _ string, params scenario.Params,
				options headless.HeadlessOptions,
			) error {
				remoteCalls++
				networkMode = orchestrator.NetworkMode()
				hostCount = orchestrator.GetHostCount()
				if params.WorkloadType != workload {
					t.Errorf("orchestrated workload = %q, want %q", params.WorkloadType, workload)
				}
				options.LaunchHooks.NotifySubmitted()
				return errRouteComplete
			}

			if err := runCmd.RunE(runCmd, []string{workload}); !errors.Is(err, errRouteComplete) {
				t.Fatalf("RunE() error = %v, want route sentinel", err)
			}
			if localCalls != 0 || remoteCalls != 1 {
				t.Fatalf("route calls local=%d orchestrated=%d, want 0/1", localCalls, remoteCalls)
			}
			if networkMode != constants.BridgeNetworkMode || hostCount != 2 {
				t.Fatalf("orchestrator network mode %q over %d hosts, want %q over 2",
					networkMode, hostCount, constants.BridgeNetworkMode)
			}
		})
	}
}

// A distributed spt replay with --network-mode bridge hands its orchestrator a bridge network; the
// replay starts through the content route that publishes the remote entry's API port
// (tui.TestABridgeDistributedRunPublishesTheRemoteEntrysAPIPort).
func TestReplayCommandStartsABridgeRemoteEntryThroughTheOrchestrator(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()

	got := runReplayToLaunch(t, server, "entry.example,worker.example", true,
		"--network-mode", constants.BridgeNetworkMode, "--api-port", "19412")
	if got.orchestrator == nil {
		t.Fatal("a distributed replay launched without the orchestrator")
	}
	if mode := got.orchestrator.NetworkMode(); mode != constants.BridgeNetworkMode {
		t.Fatalf("orchestrator network mode = %q, want %q", mode, constants.BridgeNetworkMode)
	}
	if hosts := got.orchestrator.GetHostCount(); hosts != 2 {
		t.Fatalf("orchestrator hosts = %d, want 2", hosts)
	}
}
