package cmd

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
)

// newInterfacesCmd builds a run's scenario params from args and then the environment, as spt
// run does.
func newInterfacesCmd(t *testing.T, workloadType string, env map[string]string, args ...string) (scenario.Params, error) {
	t.Helper()
	clearEnvDefaultsTestEnv(t)
	for key, value := range env {
		t.Setenv(key, value)
	}
	cmd := newRunLikeCmd()
	cmd.Flags().Bool("slice-endpoints", false, "")
	cmd.Flags().String("region", "", "")
	cmd.Flags().String("range-size", "", "")
	cmd.Flags().String("range-offset", "", "")
	cmd.Flags().String("range-align", "", "")
	cmd.Flags().String("network-mode", constants.HostNetworkMode, "")
	cmd.Flags().String(flagInterfaces, "", "")
	registerEndpointSelectionFlags(cmd.Flags())
	if workloadType == WorkloadTypeTables {
		for _, flag := range []string{"target-file-size", "ingest-file-size", "total-ingest"} {
			cmd.Flags().String(flag, "1MiB", "")
		}
		cmd.Flags().String("compaction-timeout", "10m", "")
	}
	if err := cmd.ParseFlags(append([]string{"--endpoints", "http://192.0.2.10:9000"}, args...)); err != nil {
		t.Fatal(err)
	}
	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		t.Fatal(err)
	}
	return buildScenarioParams(workloadType, cmd)
}

func TestInterfacesFlagIsRegisteredOnRunAndReplay(t *testing.T) {
	for name, cmd := range map[string]*cobra.Command{"run": runCmd, "replay": replayCmd} {
		if f := cmd.Flags().Lookup(flagInterfaces); f == nil || f.DefValue != "" || !strings.Contains(f.Usage, "SPT_INTERFACES") {
			t.Fatalf("%s must register --%s with an empty default naming SPT_INTERFACES, got %v", name, flagInterfaces, f)
		}
	}
}

func TestInterfacesReachTheParamsFromTheFlagOrTheEnvironment(t *testing.T) {
	params, err := newInterfacesCmd(t, WorkloadTypeRead, nil, "--interfaces", " ens1f0, 192.0.2.21 ")
	if err != nil || !reflect.DeepEqual(params.Interfaces, []string{"ens1f0", "192.0.2.21"}) {
		t.Fatalf("flag: interfaces = %q, err = %v", params.Interfaces, err)
	}
	params, err = newInterfacesCmd(t, WorkloadTypeRead, map[string]string{constants.EnvInterfaces: "auto"})
	if err != nil || !reflect.DeepEqual(params.Interfaces, []string{"auto"}) {
		t.Fatalf("environment: interfaces = %q, err = %v", params.Interfaces, err)
	}
	params, err = newInterfacesCmd(t, WorkloadTypeRead, map[string]string{constants.EnvInterfaces: "auto"}, "--interfaces", "ens2f0")
	if err != nil || !reflect.DeepEqual(params.Interfaces, []string{"ens2f0"}) {
		t.Fatalf("the flag wins over the environment: interfaces = %q, err = %v", params.Interfaces, err)
	}
	params, err = newInterfacesCmd(t, WorkloadTypeRead, nil)
	if err != nil || params.Interfaces != nil {
		t.Fatalf("no NIC set: interfaces = %q, err = %v", params.Interfaces, err)
	}
}

func TestInterfacesRejectionsNameTheValuesSource(t *testing.T) {
	const fromEnv = "SPT_INTERFACES (environment or .env)"
	tests := map[string]struct {
		workload string
		env      map[string]string
		args     []string
		want     string
	}{
		"a bad entry": {WorkloadTypeRead, nil, []string{"--interfaces", "ens1f0;id"},
			`--interfaces entry "ens1f0;id" is not an interface name`},
		"a bad entry from the environment": {WorkloadTypeRead, map[string]string{constants.EnvInterfaces: "auto,ens1f0"}, nil,
			fromEnv + `: --interfaces "auto,ens1f0": auto must be the only entry`},
		"bridge networking": {WorkloadTypeRead, nil, []string{"--interfaces", "auto", "--network-mode", "bridge"},
			"--interfaces needs --network-mode host: a bridge-network container sees only its own interface"},
		"the aws driver": {WorkloadTypeWrite, map[string]string{constants.EnvInterfaces: "auto"}, []string{"--s3-driver", "aws"},
			fromEnv + " requires the default Netty S3 driver, not --s3-driver aws"},
		"the rdma driver": {WorkloadTypeWrite, nil, []string{"--interfaces", "mlx5_0", "--s3-driver", "rdma"},
			"--interfaces requires the default Netty S3 driver, not --s3-driver rdma"},
		"the tables workload": {WorkloadTypeTables, nil, []string{"--interfaces", "auto"},
			"--interfaces does not support the tables workload"},
		"the mock workload": {WorkloadTypeMock, nil, []string{"--interfaces", "auto"},
			"--interfaces does not support the mock workload"},
		"round-robin endpoints": {WorkloadTypeRead, nil, []string{"--interfaces", "auto", "--endpoint-selection", "round-robin"},
			"--interfaces supports only --endpoint-selection default, not round-robin"},
		"--rdma-device": {WorkloadTypeRead, nil, []string{"--interfaces", "auto", "--rdma-device", "mlx5_0"},
			"--interfaces cannot be combined with --rdma-device mlx5_0: name each NIC's RDMA device or address in --interfaces"},
		"RDMA_DEVICE": {WorkloadTypeRead, map[string]string{constants.EnvRdmaDevice: "mlx5_1"}, []string{"--interfaces", "auto"},
			"--interfaces cannot be combined with RDMA_DEVICE (environment or .env) mlx5_1"},
		"--rdma-local-ip": {WorkloadTypeRead, map[string]string{constants.EnvInterfaces: "ens1f0"}, []string{"--rdma-local-ip", "192.0.2.31"},
			fromEnv + " cannot be combined with --rdma-local-ip 192.0.2.31"},
		"an engine override": {WorkloadTypeRead, nil, []string{"--engine-override", "storage.net.interfaces=auto"},
			`engine override "storage.net.interfaces=auto" sets storage.net.interfaces, which is not supported; use --interfaces`},
		"a nodeInterfaces override": {WorkloadTypeRead, nil, []string{"--interfaces", "auto", "--engine-override", "storage-net-nodeInterfaces=[auto]"},
			"sets storage.net.nodeInterfaces, which is not supported; use --interfaces"},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := newInterfacesCmd(t, tc.workload, tc.env, tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestInterfacesAcceptTheRDMADefaultsAndOtherSettingsWithoutANicSet(t *testing.T) {
	if _, err := newInterfacesCmd(t, WorkloadTypeRead, nil, "--interfaces", "auto", "--rdma-device", "auto"); err != nil {
		t.Fatalf("--rdma-device auto leaves each NIC's device to the NIC set: %v", err)
	}
	for name, args := range map[string][]string{
		"aws":         {"--s3-driver", "aws"},
		"bridge":      {"--network-mode", "bridge"},
		"rdma device": {"--rdma-device", "mlx5_0", "--rdma-local-ip", "192.0.2.31"},
		"round-robin": {"--endpoint-selection", "round-robin"},
	} {
		if _, err := newInterfacesCmd(t, WorkloadTypeRead, nil, args...); err != nil {
			t.Fatalf("%s without a NIC set: %v", name, err)
		}
	}
	if _, err := newInterfacesCmd(t, WorkloadTypeTables, nil); err != nil {
		t.Fatalf("tables without a NIC set: %v", err)
	}
}

func TestRunMetadataGivesEverySliceTheRunsNicSet(t *testing.T) {
	hosts := []*hostparse.HostInfo{{Host: "worker-a", IsLocal: true}, nil, {Original: "worker-b.example"}, {Host: " worker-c "}}
	metadata := buildRunMetadata(runMetadataInput{
		HostInfos: hosts,
		Params:    scenario.Params{Interfaces: []string{"ens1f0", "192.0.2.21"}},
	})
	want := []results.NicNode{
		{Node: 0, Host: "worker-a", Interfaces: "ens1f0,192.0.2.21"},
		{Node: 1, Host: "worker-b.example", Interfaces: "ens1f0,192.0.2.21"},
		{Node: 2, Host: "worker-c", Interfaces: "ens1f0,192.0.2.21"},
	}
	if !reflect.DeepEqual(metadata.NicNodes, want) {
		t.Fatalf("nicNodes = %+v, want %+v", metadata.NicNodes, want)
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"nicNodes":[{"node":0,"host":"worker-a","interfaces":"ens1f0,192.0.2.21"}`,
		`"Interfaces":["ens1f0","192.0.2.21"]`} {
		if !strings.Contains(string(encoded), field) {
			t.Fatalf("metadata lacks %s:\n%s", field, encoded)
		}
	}

	without, err := json.Marshal(buildRunMetadata(runMetadataInput{HostInfos: hosts}))
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"nicNodes", "nicEngines"} {
		if strings.Contains(string(without), field) {
			t.Fatalf("a run without a NIC set records %s:\n%s", field, without)
		}
	}
}

func TestCaptureNicEnginesStoresTheFetchedReports(t *testing.T) {
	root := t.TempDir()
	for name, content := range map[string]string{
		"s1." + constants.ResultsArtifactSuffixMetricsTotal: "OpType,NodeCount\nREAD,1\n",
		"s1.nic.set.json": `{"schema_version":1,"step_id":"s1","hardware_threads":64,"driver_threads":0,
			"event_loops_per_nic":8,"nics":["ens1f0","ens1f1"],"event_loops":16}`,
		"s2." + constants.ResultsArtifactSuffixMetricsTotal: "OpType,NodeCount\nREAD,1\n",
		"s2.nic.set.json": `{"schema_version":9}`,
	} {
		if err := os.WriteFile(filepath.Join(root, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	metadata := buildRunMetadata(runMetadataInput{
		HostInfos: []*hostparse.HostInfo{{Host: "worker-a"}},
		Params:    scenario.Params{Interfaces: []string{"auto"}},
	})
	captureNicEngines(root, []string{"s1", "s2"}, metadata)
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	want := `"nicEngines":{"s1":[{"node":0,"host":"worker-a","drivers":1,"driverThreads":0,"hardwareThreads":64,` +
		`"eventLoopsPerNic":8,"nics":["ens1f0","ens1f1"],"eventLoops":16}]},` +
		`"nicEnginesError":"s2.nic.set.json: schema_version 9, want 1"`
	if !strings.Contains(string(encoded), want) {
		t.Fatalf("metadata lacks %s:\n%s", want, encoded)
	}

	none := buildRunMetadata(runMetadataInput{HostInfos: []*hostparse.HostInfo{{Host: "worker-a"}}})
	captureNicEngines(root, []string{"s1"}, none)
	if none.NicEngines != nil || none.NicEnginesError != "" {
		t.Fatalf("a run without a NIC set reads no report, got %+v %q", none.NicEngines, none.NicEnginesError)
	}
}

func TestRunCmdInterfacesReachEveryRouteAndEveryNode(t *testing.T) {
	tests := []struct {
		name, hosts, minHosts string
		wantLocal, wantRemote int
		wantNodes             []string
	}{
		{name: "local", hosts: "127.0.0.1", minHosts: "1", wantLocal: 1, wantNodes: []string{"127.0.0.1"}},
		{name: "lone remote", hosts: "entry.example", minHosts: "1", wantRemote: 1, wantNodes: []string{"entry.example"}},
		{name: "distributed", hosts: "entry.example,worker.example", minHosts: "2", wantRemote: 1,
			wantNodes: []string{"entry.example", "worker.example"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			setDeleteRouteFlags(t, test.hosts, test.minHosts)
			setGlobalRunFlagForTest(t, "auto-results", "true")
			setGlobalRunFlagForTest(t, flagInterfaces, "auto")

			previousPort := resolvePortConflictFunc
			previousConnect := connectMultiHostOrchestratorFunc
			previousLocal := startLocalHeadlessRunFunc
			previousRemote := startMultiHostHeadlessRunFunc
			previousAutoResults := startAutoResultsFunc
			t.Cleanup(func() {
				resolvePortConflictFunc = previousPort
				connectMultiHostOrchestratorFunc = previousConnect
				startLocalHeadlessRunFunc = previousLocal
				startMultiHostHeadlessRunFunc = previousRemote
				startAutoResultsFunc = previousAutoResults
			})
			resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error { return nil }
			var localCalls, remoteCalls int
			var launched []scenario.Params
			startLocalHeadlessRunFunc = func(_ string, _ string, params scenario.Params, options headless.HeadlessOptions) error {
				localCalls++
				launched = append(launched, params)
				options.LaunchHooks.NotifySubmitted()
				return nil
			}
			startMultiHostHeadlessRunFunc = func(
				_ *tui.MultiHostOrchestrator, _ string, _ string, params scenario.Params, options headless.HeadlessOptions,
			) error {
				remoteCalls++
				launched = append(launched, params)
				options.LaunchHooks.NotifySubmitted()
				return nil
			}
			var nicNodes []results.NicNode
			startAutoResultsFunc = func(
				_ context.Context, _ string, _ string, _ string, _ []string, _ int64, _ bool, _ []*hostparse.HostInfo,
				_ string, _ bool, _ int, _ string, metadata *runMetadata, _ io.Writer, _ io.Writer, _ string,
				_ func(context.Context), _ ...*integrity.FinalizeOptions,
			) *autoResultsMonitor {
				nicNodes = metadata.NicNodes
				monitor := &autoResultsMonitor{done: make(chan autoResultsOutcome, 1), armed: make(chan struct{})}
				go func() {
					<-monitor.armed
					outcome := autoResultsOutcome{
						Tracker:      &portcheck.RunResult{FinalState: constants.StateCompleted},
						Finalization: &integrity.FinalizeOutcome{Complete: true},
					}
					outcome.Lifecycle.PreparedInputs = cleanupPreparedForMonitorTest(metadata)
					monitor.done <- outcome
				}()
				return monitor
			}

			if err := runCmd.RunE(runCmd, []string{WorkloadTypeWrite}); err != nil {
				t.Fatalf("RunE() error = %v", err)
			}
			if localCalls != test.wantLocal || remoteCalls != test.wantRemote || len(launched) != 1 {
				t.Fatalf("route calls local=%d remote=%d, want %d/%d", localCalls, remoteCalls, test.wantLocal, test.wantRemote)
			}
			if !reflect.DeepEqual(launched[0].Interfaces, []string{"auto"}) {
				t.Fatalf("launched interfaces = %q, want [auto]", launched[0].Interfaces)
			}
			defaults, err := scenario.GenerateDefaults(launched[0])
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(defaults), "interfaces:\n") || strings.Contains(string(defaults), "nodeInterfaces") {
				t.Fatalf("the generated defaults must carry the run's NIC set for every slice and no node map:\n%s", defaults)
			}
			var hosts []string
			for i, node := range nicNodes {
				if node.Node != i || node.Interfaces != "auto" {
					t.Fatalf("nicNodes[%d] = %+v, want node %d with auto", i, node, i)
				}
				hosts = append(hosts, node.Host)
			}
			if !reflect.DeepEqual(hosts, test.wantNodes) {
				t.Fatalf("nicNodes hosts = %q, want %q", hosts, test.wantNodes)
			}
		})
	}
}
