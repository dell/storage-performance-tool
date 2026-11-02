package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/engineinfo"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
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
	cmd.Flags().StringArray(flagHostInterfaces, nil, "")
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
			"sets storage.net.nodeInterfaces, which is not supported; use --host-interfaces"},
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
	hosts := []*hostparse.HostInfo{{Host: "worker-a.example", IsLocal: true}, nil, {Original: "worker-b.example.example"}, {Host: " worker-c.example "}}
	metadata := buildRunMetadata(runMetadataInput{
		HostInfos: hosts,
		Params:    scenario.Params{Interfaces: []string{"ens1f0", "192.0.2.21"}},
	})
	want := []results.NicNode{
		{Node: 0, Host: "worker-a.example", Interfaces: "ens1f0,192.0.2.21", Source: "run"},
		{Node: 1, Host: "worker-b.example.example", Interfaces: "ens1f0,192.0.2.21", Source: "run"},
		{Node: 2, Host: "worker-c.example", Interfaces: "ens1f0,192.0.2.21", Source: "run"},
	}
	if !reflect.DeepEqual(metadata.NicNodes, want) {
		t.Fatalf("nicNodes = %+v, want %+v", metadata.NicNodes, want)
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{`"nicNodes":[{"node":0,"host":"worker-a.example","interfaces":"ens1f0,192.0.2.21","source":"run"}`,
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

func TestRunMetadataNicNodesFollowTheLockedExecutionParticipants(t *testing.T) {
	configured, err := hostparse.ParseTestHosts("entry.example,b.example,c.example")
	if err != nil {
		t.Fatal(err)
	}
	build := func(interfaces []string, hosts func() ([]*hostparse.HostInfo, bool, error)) *runMetadata {
		metadata := buildRunMetadata(runMetadataInput{HostInfos: configured, Params: scenario.Params{Interfaces: interfaces}})
		metadata.executionHosts = hosts
		return metadata
	}
	participants := []*hostparse.HostInfo{configured[0], configured[2]}
	tests := map[string]struct {
		hosts func() ([]*hostparse.HostInfo, bool, error)
		want  []string
	}{
		"before the topology is locked": {func() ([]*hostparse.HostInfo, bool, error) { return nil, false, nil }, []string{"entry.example", "b.example", "c.example"}},
		"an unreadable topology":        {func() ([]*hostparse.HostInfo, bool, error) { return nil, true, errors.New("duplicate host") }, []string{"entry.example", "b.example", "c.example"}},
		"a locked topology":             {func() ([]*hostparse.HostInfo, bool, error) { return participants, true, nil }, []string{"entry.example", "c.example"}},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			metadata := build([]string{"auto"}, test.hosts)
			if err := writeRunMetadata(metadata, t.TempDir()); err != nil {
				t.Fatal(err)
			}
			if got := nicNodeHosts(metadata.NicNodes); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("nicNodes hosts = %q, want %q", got, test.want)
			}
			for i, node := range metadata.NicNodes {
				if node.Node != i || node.Interfaces != "auto" {
					t.Fatalf("nicNodes[%d] = %+v, want slice %d with auto", i, node, i)
				}
			}
		})
	}

	without := build(nil, func() ([]*hostparse.HostInfo, bool, error) {
		t.Fatal("a run without a NIC set must not read the execution topology")
		return nil, false, nil
	})
	if err := writeRunMetadata(without, t.TempDir()); err != nil || without.NicNodes != nil {
		t.Fatalf("a run without a NIC set has no nicNodes, got %+v (err %v)", without.NicNodes, err)
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
		HostInfos: []*hostparse.HostInfo{{Host: "worker-a.example"}},
		Params:    scenario.Params{Interfaces: []string{"auto"}},
	})
	captureNicEngines(root, []string{"s1", "s2"}, metadata)
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	want := `"nicEngines":{"s1":[{"node":0,"host":"worker-a.example","drivers":1,"driverThreads":0,"hardwareThreads":64,` +
		`"eventLoopsPerNic":8,"nics":["ens1f0","ens1f1"],"eventLoops":16}]},` +
		`"nicEnginesError":"s2.nic.set.json: schema_version 9, want 1"`
	if !strings.Contains(string(encoded), want) {
		t.Fatalf("metadata lacks %s:\n%s", want, encoded)
	}

	none := buildRunMetadata(runMetadataInput{HostInfos: []*hostparse.HostInfo{{Host: "worker-a.example"}}})
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

// TestRunCmdNicNodesFollowTheWorkersThatStartedWhenAMiddleWorkerFails runs a distributed write
// on three hosts with --min-hosts 2 whose middle worker does not start: the Entry Node gets only
// c.example, so slice 1 is c.example. The stored nicNodes, the NIC-set report of slice 1 and the
// fetch's missing-file message must all name c.example, never b.example.
func TestRunCmdNicNodesFollowTheWorkersThatStartedWhenAMiddleWorkerFails(t *testing.T) {
	t.Chdir(t.TempDir())
	setDeleteRouteFlags(t, "entry.example,b.example,c.example", "2")
	setGlobalRunFlagForTest(t, "auto-results", "true")
	setGlobalRunFlagForTest(t, "results-dir", t.TempDir())
	setGlobalRunFlagForTest(t, flagInterfaces, "auto")

	// The engine API of the Entry Node: one step with two participants. Slice 1 wrote its
	// NIC-set report and no per-path CSV.
	report := func(step string, nics string) string {
		return `{"schema_version":1,"step_id":"` + step + `","hardware_threads":64,"driver_threads":0,` +
			`"event_loops_per_nic":8,"nics":` + nics + `,"event_loops":8}`
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rest, ok := strings.CutPrefix(r.URL.Path, "/logs/")
		step, logger, found := strings.Cut(rest, "/")
		if !ok || !found {
			http.NotFound(w, r)
			return
		}
		files := map[string]string{
			"metrics.FileTotal":      "OpType,NodeCount,CountSucc\nCREATE,2,10\n",
			"nic.paths.node-000.csv": "rows",
			"nic.set.node-000.json":  report(step, `["ens1f0"]`),
			"nic.set.node-001.json":  report(step, `["ens2f0"]`),
		}
		if logger == "index.json" {
			items := make([]map[string]any, 0, len(files))
			for name, content := range files {
				items = append(items, map[string]any{"logger": name, "size": len(content)})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": items})
			return
		}
		content, ok := files[logger]
		if !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(content))
	}))
	defer server.Close()

	previousPort := resolvePortConflictFunc
	previousConnect := connectMultiHostOrchestratorFunc
	previousParticipants := executionParticipantHostsFunc
	previousLocal := startLocalHeadlessRunFunc
	previousRemote := startMultiHostHeadlessRunFunc
	previousAutoResults := startAutoResultsFunc
	t.Cleanup(func() {
		resolvePortConflictFunc = previousPort
		connectMultiHostOrchestratorFunc = previousConnect
		executionParticipantHostsFunc = previousParticipants
		startLocalHeadlessRunFunc = previousLocal
		startMultiHostHeadlessRunFunc = previousRemote
		startAutoResultsFunc = previousAutoResults
	})
	resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
		return &portcheck.ResolutionResult{Success: true}, nil
	}
	connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error { return nil }

	// b.example fails to start, so the execution topology locks the entry node and c.example.
	started, err := hostparse.ParseTestHosts("entry.example,c.example")
	if err != nil {
		t.Fatal(err)
	}
	var locked atomic.Bool
	executionParticipantHostsFunc = func(*tui.MultiHostOrchestrator) ([]*tui.HostConnection, bool, error) {
		if !locked.Load() {
			return nil, false, nil
		}
		return []*tui.HostConnection{{Info: started[0]}, {Info: started[1]}}, true, nil
	}
	startLocalHeadlessRunFunc = func(string, string, scenario.Params, headless.HeadlessOptions) error {
		t.Error("a distributed run must not take the local route")
		return nil
	}
	remoteCalls := 0
	startMultiHostHeadlessRunFunc = func(
		_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions,
	) error {
		remoteCalls++
		locked.Store(true)
		options.LaunchHooks.NotifySubmitted()
		return nil
	}
	var stepID string
	var stored *runMetadata
	startAutoResultsFunc = func(
		parentCtx context.Context, baseURL, label, resultsDir string, plannedStepIDs []string,
		runID int64, debug bool, hosts []*hostparse.HostInfo, port string, shutdownOn bool,
		lingerSec int, scenarioPath string, metadata *runMetadata, progressOut, summaryOut io.Writer,
		traceFile string, preSummaryHook func(context.Context), options ...*integrity.FinalizeOptions,
	) *autoResultsMonitor {
		if len(plannedStepIDs) != 1 {
			t.Fatalf("planned steps = %q, want one", plannedStepIDs)
		}
		stepID, stored = plannedStepIDs[0], metadata
		if got := nicNodeHosts(metadata.NicNodes); !reflect.DeepEqual(got, []string{"entry.example", "b.example", "c.example"}) {
			t.Fatalf("before the launch nicNodes follow the configured hosts, got %q", got)
		}
		installAutoResultsFakes(t, plannedStepIDs)
		newResultsFetcherFunc = func(_, output string) autoResultsFetcher {
			fetcher := results.NewFetcher(server.URL, output)
			fetcher.Artifacts = []results.ArtifactSpec{
				{Loggers: []string{"metrics.FileTotal"}, Suffix: constants.ResultsArtifactSuffixMetricsTotal, Required: true},
				{Loggers: []string{"NicPaths"}, Suffix: constants.ResultsArtifactSuffixNicPaths},
				{Loggers: []string{"NicSet"}, Suffix: constants.ResultsArtifactSuffixNicSet},
			}
			return &fetcherAdapter{fetcher}
		}
		return startAutoResultsMonitor(
			parentCtx, baseURL, label, resultsDir, plannedStepIDs, runID, debug, hosts, port,
			shutdownOn, lingerSec, scenarioPath, metadata, progressOut, summaryOut, traceFile,
			preSummaryHook, options...,
		)
	}

	if err := runCmd.RunE(runCmd, []string{WorkloadTypeWrite}); err != nil {
		t.Fatalf("RunE() error = %v", err)
	}
	if remoteCalls != 1 || stored == nil {
		t.Fatalf("remote route calls = %d, metadata captured = %t; want one distributed launch", remoteCalls, stored != nil)
	}

	data, err := os.ReadFile(filepath.Join(stored.ResultsRoot, constants.ResultsMetadataFileName))
	if err != nil {
		t.Fatal(err)
	}
	var params struct {
		NicNodes   []results.NicNode              `json:"nicNodes"`
		NicEngines map[string][]results.NicEngine `json:"nicEngines"`
	}
	if err := json.Unmarshal(data, &params); err != nil {
		t.Fatal(err)
	}
	wantNodes := []results.NicNode{
		{Node: 0, Host: "entry.example", Interfaces: "auto", Source: "run"},
		{Node: 1, Host: "c.example", Interfaces: "auto", Source: "run"},
	}
	if !reflect.DeepEqual(params.NicNodes, wantNodes) {
		t.Fatalf("stored nicNodes = %+v, want the execution participants %+v", params.NicNodes, wantNodes)
	}
	engines := params.NicEngines[stepID]
	if len(engines) != 2 || engines[1].Node != 1 || engines[1].Host != "c.example" ||
		!reflect.DeepEqual(engines[1].Nics, []string{"ens2f0"}) {
		t.Fatalf("nicEngines[%s] = %+v, want slice 1's report on c.example", stepID, engines)
	}

	indexData, err := os.ReadFile(filepath.Join(stored.ResultsRoot, constants.ResultsManifestFileName))
	if err != nil {
		t.Fatal(err)
	}
	var index struct {
		Steps []results.StepManifest `json:"steps"`
	}
	if err := json.Unmarshal(indexData, &index); err != nil {
		t.Fatal(err)
	}
	var missing *results.FileStatus
	for i := range index.Steps {
		for j := range index.Steps[i].Files {
			if file := &index.Steps[i].Files[j]; file.Name == results.NicPathsNodeFileName(stepID, 1) {
				missing = file
			}
		}
	}
	if missing == nil || missing.Status != "missing" || !strings.Contains(missing.Error, "node-001 (c.example)") {
		t.Fatalf("slice 1's absent per-path CSV = %+v, want missing and attributed to c.example", missing)
	}
	// The configured hosts in spt_run_params.json still list b.example; the NIC evidence does not.
	if strings.Contains(string(indexData), "b.example") {
		t.Fatalf("index.json names the worker that did not start:\n%s", indexData)
	}
}

func nicNodeHosts(nodes []results.NicNode) []string {
	hosts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		hosts = append(hosts, node.Host)
	}
	return hosts
}

// TestReplayCmdNicNodesFollowTheWorkersThatStartedWhenAMiddleWorkerFails is the replay route of
// the same topology: the stored nicNodes are the execution participants.
func TestReplayCmdNicNodesFollowTheWorkersThatStartedWhenAMiddleWorkerFails(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()
	restore := installReplayEngineInfoSeams(t)
	defer restore()
	previousParticipants := executionParticipantHostsFunc
	t.Cleanup(func() { executionParticipantHostsFunc = previousParticipants })

	started, err := hostparse.ParseTestHosts("entry.example,c.example")
	if err != nil {
		t.Fatal(err)
	}
	var locked atomic.Bool
	executionParticipantHostsFunc = func(*tui.MultiHostOrchestrator) ([]*tui.HostConnection, bool, error) {
		if !locked.Load() {
			return nil, false, nil
		}
		return []*tui.HostConnection{{Info: started[0]}, {Info: started[1]}}, true, nil
	}
	descriptors := []engineinfo.ParticipantDescriptor{
		replayDescriptor(t, "entry.example", "9999", engineinfo.RoleEntry),
		replayDescriptor(t, "c.example", "9999", engineinfo.RoleWorker),
	}
	replayDistributedEnginePlan = func(*tui.MultiHostOrchestrator, string, bool) ([]engineinfo.ParticipantDescriptor, error) {
		return descriptors, nil
	}
	newReplayEngineIdentityCollector = func() engineinfo.FleetCollector {
		return &replayGateCollector{t: t, wantDescriptors: descriptors,
			fleet: runGateFleet(engineinfo.ConsistencyConsistent, engineinfo.StatusCollected)}
	}
	connectReplayOrchestrator = func(context.Context, *tui.MultiHostOrchestrator) error { return nil }
	shouldReplayRunHeadless = func(*cobra.Command) bool { return true }
	startReplayLocalHeadless = func(string, string, scenario.Params, headless.HeadlessOptions, []byte, []byte) error {
		t.Error("a distributed replay must not take the local route")
		return nil
	}
	var submissions atomic.Int32
	var out bytes.Buffer
	startReplayRemoteHeadless = func(_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params,
		options headless.HeadlessOptions, _ []byte, _ []byte,
	) error {
		locked.Store(true)
		return executeReplayHookForTest(options.Context, options.LaunchHooks, &out, &submissions)
	}
	var stored *runMetadata
	startReplayAutoResultsMonitor = func(
		_ context.Context, _, _, _ string, _ []string, _ int64, _ bool, _ []*hostparse.HostInfo,
		_ string, _ bool, _ int, _ string, metadata *runMetadata, _, _ io.Writer, _ string,
		_ func(context.Context), _ ...*integrity.FinalizeOptions,
	) *autoResultsMonitor {
		stored = metadata
		monitor := &autoResultsMonitor{done: make(chan autoResultsOutcome, 1), armed: make(chan struct{})}
		go func() {
			<-monitor.armed
			monitor.done <- autoResultsOutcome{ArtifactErr: writeRunMetadata(metadata, metadata.ResultsRoot)}
		}()
		return monitor
	}

	cmd := newReplayCommandForTest(t)
	cmd.Flags().String(flagInterfaces, "", "")
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append(replayIdentityArgs(server.URL, "entry.example,b.example,c.example", t.TempDir(), true, true, false),
		"--min-hosts", "2", "--interfaces", "auto"))
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v\n%s", err, out.String())
	}
	if submissions.Load() != 1 || stored == nil {
		t.Fatalf("submissions = %d, metadata captured = %t; want one distributed replay", submissions.Load(), stored != nil)
	}
	data, err := os.ReadFile(filepath.Join(stored.ResultsRoot, constants.ResultsMetadataFileName))
	if err != nil {
		t.Fatal(err)
	}
	var params struct {
		NicNodes []results.NicNode `json:"nicNodes"`
	}
	if err := json.Unmarshal(data, &params); err != nil {
		t.Fatal(err)
	}
	want := []results.NicNode{
		{Node: 0, Host: "entry.example", Interfaces: "auto", Source: "run"},
		{Node: 1, Host: "c.example", Interfaces: "auto", Source: "run"},
	}
	if !reflect.DeepEqual(params.NicNodes, want) {
		t.Fatalf("stored nicNodes = %+v, want the execution participants %+v", params.NicNodes, want)
	}
}

func TestHostInterfacesReachTheParamsFromTheFlagOrTheEnvironment(t *testing.T) {
	want := []nicplan.HostInterfaces{
		{Host: "127.0.0.1", Value: "none"},
		{Host: "wrk2", Value: "ens1f0,ens1f1", Entries: []string{"ens1f0", "ens1f1"}},
	}
	hosts := []string{"--test-hosts", "127.0.0.1,wrk2"}
	params, err := newInterfacesCmd(t, WorkloadTypeRead, nil, append(hosts,
		"--host-interfaces", "127.0.0.1=none", "--host-interfaces", "root@wrk2=ens1f0,ens1f1")...)
	if err != nil {
		t.Fatal(err)
	}
	want[1].Value = "ens1f0,ens1f1"
	if !reflect.DeepEqual(params.HostInterfaces, want) {
		t.Fatalf("flag: %+v, want %+v", params.HostInterfaces, want)
	}
	params, err = newInterfacesCmd(t, WorkloadTypeRead,
		map[string]string{constants.EnvHostInterfaces: "127.0.0.1=none; wrk2=ens1f0,ens1f1"}, hosts...)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(params.HostInterfaces, want) {
		t.Fatalf("environment: %+v, want %+v", params.HostInterfaces, want)
	}
	params, err = newInterfacesCmd(t, WorkloadTypeRead,
		map[string]string{constants.EnvHostInterfaces: "127.0.0.1=auto"},
		append(hosts, "--host-interfaces", "wrk2=none")...)
	if err != nil {
		t.Fatal(err)
	}
	if len(params.HostInterfaces) != 1 || params.HostInterfaces[0].Host != "wrk2" {
		t.Fatalf("the flag replaces the environment: %+v", params.HostInterfaces)
	}
}

func TestHostInterfacesRejectionsNameTheValuesSource(t *testing.T) {
	const fromEnv = "SPT_HOST_INTERFACES (environment or .env)"
	hosts := []string{"--test-hosts", "wrk1,wrk2"}
	tests := map[string]struct {
		env  map[string]string
		args []string
		want string
	}{
		"not in --test-hosts": {nil, []string{"--host-interfaces", "wrk9=auto"},
			"--host-interfaces wrk9=auto: wrk9 is not in --test-hosts"},
		"not in --test-hosts, from the environment": {map[string]string{constants.EnvHostInterfaces: "wrk9=auto"}, nil,
			fromEnv + ": --host-interfaces wrk9=auto: wrk9 is not in --test-hosts"},
		"a bridge network": {nil, []string{"--host-interfaces", "wrk1=auto", "--network-mode", "bridge"},
			"--host-interfaces needs --network-mode host: a bridge-network container sees only its own interface"},
		"another driver": {nil, []string{"--host-interfaces", "wrk1=auto", "--s3-driver", "aws"},
			"--host-interfaces requires the default Netty S3 driver, not --s3-driver aws"},
		"an RDMA device": {nil, []string{"--host-interfaces", "wrk1=auto", "--rdma-device", "mlx5_0"},
			"--host-interfaces cannot be combined with --rdma-device mlx5_0: name each NIC's RDMA device or address in --interfaces"},
		"an engine override of a key": {nil, []string{"--engine-override", "storage.net.nodeInterfaces.entry=none"},
			`engine override "storage.net.nodeInterfaces.entry=none" sets storage.net.nodeInterfaces, which is not supported; use --host-interfaces`},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			_, err := newInterfacesCmd(t, WorkloadTypeRead, tc.env, append(append([]string{}, hosts...), tc.args...)...)
			if err == nil || err.Error() != tc.want {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestHostInterfacesNoneAsksForNothingOfADriver(t *testing.T) {
	params, err := newInterfacesCmd(t, WorkloadTypeRead, nil,
		"--test-hosts", "wrk1,wrk2", "--host-interfaces", "wrk1=none", "--s3-driver", "aws", "--network-mode", "bridge")
	if err != nil {
		t.Fatal(err)
	}
	if len(params.HostInterfaces) != 1 || params.HostInterfaces[0].Value != nicplan.None {
		t.Fatalf("HostInterfaces = %+v", params.HostInterfaces)
	}
}

func TestRunMetadataRecordsHostInterfacesAndEachSlicesValue(t *testing.T) {
	hosts := []*hostparse.HostInfo{{Host: "wrk1"}, {Host: "wrk2"}, {Host: "wrk3"}}
	metadata := buildRunMetadata(runMetadataInput{
		HostInfos: hosts,
		Params: scenario.Params{Interfaces: []string{"auto"}, HostInterfaces: []nicplan.HostInterfaces{
			{Host: "wrk1", Value: "none"},
			{Host: "wrk2", Value: "mlx5_0,mlx5_1", Entries: []string{"mlx5_0", "mlx5_1"}},
		}},
	})
	want := []results.NicNode{
		{Node: 0, Host: "wrk1", Interfaces: "none", Source: "host"},
		{Node: 1, Host: "wrk2", Interfaces: "mlx5_0,mlx5_1", Source: "host"},
		{Node: 2, Host: "wrk3", Interfaces: "auto", Source: "run"},
	}
	if !reflect.DeepEqual(metadata.NicNodes, want) {
		t.Fatalf("nicNodes = %+v, want %+v", metadata.NicNodes, want)
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"hostInterfaces":{"wrk1":"none","wrk2":"mlx5_0,mlx5_1"}`) {
		t.Fatalf("metadata lacks hostInterfaces:\n%s", encoded)
	}
	if strings.Contains(string(encoded), `"HostInterfaces"`) {
		t.Fatalf("the scenario params must not repeat them:\n%s", encoded)
	}
}

func TestRunMetadataWithOnlyHostInterfacesGivesTheOtherSlicesNone(t *testing.T) {
	metadata := buildRunMetadata(runMetadataInput{
		HostInfos: []*hostparse.HostInfo{{Host: "wrk1"}, {Host: "wrk2"}},
		Params:    scenario.Params{HostInterfaces: []nicplan.HostInterfaces{{Host: "wrk2", Value: "auto", Entries: []string{"auto"}}}},
	})
	want := []results.NicNode{
		{Node: 0, Host: "wrk1", Interfaces: "none", Source: "run"},
		{Node: 1, Host: "wrk2", Interfaces: "auto", Source: "host"},
	}
	if !reflect.DeepEqual(metadata.NicNodes, want) {
		t.Fatalf("nicNodes = %+v, want %+v", metadata.NicNodes, want)
	}
}
