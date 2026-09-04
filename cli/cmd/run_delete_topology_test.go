/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
)

func TestRunCmdDeleteRoutesEveryHostTopologyAndSkipsTheOtherPath(t *testing.T) {
	tests := []struct {
		name, hosts, minHosts string
		wantLocal, wantRemote int
		wantConnect, wantPort int
	}{
		{name: "local Docker controller", hosts: "127.0.0.1", minHosts: "1", wantLocal: 1, wantPort: 1},
		{name: "lone remote orchestrator", hosts: "entry.example", minHosts: "1", wantRemote: 1, wantConnect: 1},
		{name: "distributed entry workers", hosts: "entry.example,worker.example", minHosts: "2", wantRemote: 1, wantConnect: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			setDeleteRouteFlags(t, test.hosts, test.minHosts)

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
			errRouteComplete := errors.New("DELETE route inspected")
			inspectLaunch := func(params scenario.Params, scenarioContent []byte, hooks tui.LaunchHooks) error {
				t.Helper()
				if params.WorkloadType != WorkloadTypeDelete || params.ObjectCount != 2 {
					t.Fatalf("DELETE route params = %+v", params)
				}
				content := string(scenarioContent)
				freeze := strings.Index(content, "StandaloneDeleteSelection.fromManifest")
				dispatch := strings.Index(content, "DeleteLoad.config")
				if freeze < 0 || dispatch < 0 || freeze >= dispatch {
					t.Fatalf("DELETE route did not freeze its inventory before timed dispatch:\n%s", content)
				}
				if !strings.Contains(content[dispatch:], `"standalone": true`) {
					t.Fatalf("DELETE route omitted the positive standalone timed phase:\n%s", content)
				}
				hooks.NotifySubmitted()
				return errRouteComplete
			}
			startLocalHeadlessRunFunc = func(
				_ string, _ string, params scenario.Params, options headless.HeadlessOptions,
			) error {
				localCalls++
				return inspectLaunch(params, options.ScenarioContent, options.LaunchHooks)
			}
			startMultiHostHeadlessRunFunc = func(
				_ *tui.MultiHostOrchestrator, _ string, _ string, params scenario.Params,
				options headless.HeadlessOptions,
			) error {
				remoteCalls++
				return inspectLaunch(params, options.ScenarioContent, options.LaunchHooks)
			}

			err := runCmd.RunE(runCmd, []string{WorkloadTypeDelete})
			if !errors.Is(err, errRouteComplete) {
				t.Fatalf("RunE() error = %v, want route sentinel", err)
			}
			if localCalls != test.wantLocal || remoteCalls != test.wantRemote ||
				connectCalls != test.wantConnect || portCalls != test.wantPort {
				t.Fatalf(
					"DELETE route calls local=%d remote=%d connect=%d port=%d, want %d/%d/%d/%d",
					localCalls, remoteCalls, connectCalls, portCalls,
					test.wantLocal, test.wantRemote, test.wantConnect, test.wantPort,
				)
			}
		})
	}
}

func TestRunCmdExistingPrefixDeleteRejectsDistributedIdentityBeforeLaunch(t *testing.T) {
	tests := []struct {
		name, hosts, minHosts string
	}{
		{name: "lone remote", hosts: "entry.example", minHosts: "1"},
		{name: "multi host", hosts: "entry.example,worker.example", minHosts: "2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			setDeleteRouteFlags(t, test.hosts, test.minHosts)
			setGlobalRunFlagForTest(t, flagDeleteExisting, "true")
			setGlobalRunFlagForTest(t, "prefix", "guarded/")

			previousPort := resolvePortConflictFunc
			previousConnect := connectMultiHostOrchestratorFunc
			previousPrepare := prepareDistributedIntegrityRuntimeIdentityFunc
			previousRemote := startMultiHostHeadlessRunFunc
			t.Cleanup(func() {
				resolvePortConflictFunc = previousPort
				connectMultiHostOrchestratorFunc = previousConnect
				prepareDistributedIntegrityRuntimeIdentityFunc = previousPrepare
				startMultiHostHeadlessRunFunc = previousRemote
			})
			var portCalls, connectCalls, identityCalls, launchCalls int
			resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
				portCalls++
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error {
				connectCalls++
				return nil
			}
			errIdentity := errors.New("distributed DELETE image identity mismatch")
			prepareDistributedIntegrityRuntimeIdentityFunc = func(
				context.Context, *tui.MultiHostOrchestrator, string,
			) (tui.DistributedRuntimeIdentityEvidence, error) {
				identityCalls++
				return tui.DistributedRuntimeIdentityEvidence{}, errIdentity
			}
			startMultiHostHeadlessRunFunc = func(
				*tui.MultiHostOrchestrator, string, string, scenario.Params, headless.HeadlessOptions,
			) error {
				launchCalls++
				return errors.New("launch must remain unreachable")
			}

			err := runCmd.RunE(runCmd, []string{WorkloadTypeDelete})
			if !errors.Is(err, errIdentity) {
				t.Fatalf("RunE() error = %v, want identity mismatch", err)
			}
			if portCalls != 0 || connectCalls != 1 || identityCalls != 1 || launchCalls != 0 {
				t.Fatalf(
					"guarded DELETE calls port=%d connect=%d identity=%d launch=%d, want 0/1/1/0",
					portCalls, connectCalls, identityCalls, launchCalls,
				)
			}
		})
	}
}

func setDeleteRouteFlags(t *testing.T, hosts, minHosts string) {
	t.Helper()
	for name, value := range map[string]string{
		"test-hosts": hosts, "min-hosts": minHosts,
		"endpoints": "http://s3.example", "access-key": "access", "secret-key": "secret",
		"bucket": "qualification", "object-size": "1KiB", "object-count": "2",
		"duration": "", "threads": "1", "headless": "true", "auto-results": "false",
		"shutdown-on-complete": "false", "generate-only": "false", "items-file": "",
		"delete-batch-size": "2", flagDeleteExisting: "false", "prefix": "",
		flagAllowEmptyPrefix: "false", "cleanup": "false", flagVerifyDelete: "false",
		flagValidateInventory: "false",
	} {
		setGlobalRunFlagForTest(t, name, value)
	}
}
