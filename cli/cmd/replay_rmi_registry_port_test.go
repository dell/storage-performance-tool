/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
)

// Replay's flag has spt run's name, default and help text.
func TestReplayRMIRegistryPortFlagMatchesRun(t *testing.T) {
	replayFlag := replayCmd.Flags().Lookup(flagRMIRegistryPort)
	if replayFlag == nil {
		t.Fatalf("replay flag %q not registered", flagRMIRegistryPort)
	}
	runFlag := runCmd.Flags().Lookup(flagRMIRegistryPort)
	if replayFlag.DefValue != strconv.Itoa(constants.DefaultRMIRegistryPort) || replayFlag.DefValue != runFlag.DefValue {
		t.Fatalf("replay --%s default = %s, run's = %s, want %d",
			flagRMIRegistryPort, replayFlag.DefValue, runFlag.DefValue, constants.DefaultRMIRegistryPort)
	}
	if replayFlag.Usage != rmiRegistryPortUsage || replayFlag.Usage != runFlag.Usage {
		t.Fatalf("replay --%s help = %q, run's = %q", flagRMIRegistryPort, replayFlag.Usage, runFlag.Usage)
	}
	if replayFlag.Value.Type() != runFlag.Value.Type() {
		t.Fatalf("replay --%s type = %s, run's = %s", flagRMIRegistryPort, replayFlag.Value.Type(), runFlag.Value.Type())
	}
}

// A registry port outside the TCP range is refused before replay reads its source.
func TestReplayCommandRefusesAnRMIRegistryPortOutsideTheTCPRange(t *testing.T) {
	var requests atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer source.Close()

	for _, port := range []string{"0", "65536"} {
		t.Run(port, func(t *testing.T) {
			cmd := newReplayCommandForTest(t)
			cmd.SetOut(io.Discard)
			cmd.SetArgs([]string{"--from", source.URL, "--endpoints", "http://192.0.2.1:9020",
				"--" + flagRMIRegistryPort, port, "--results-dir", t.TempDir()})
			want := "--rmi-registry-port must be between 1 and 65535, got " + port
			if err := cmd.Execute(); err == nil || err.Error() != want {
				t.Fatalf("Execute() error = %v, want %q", err, want)
			}
		})
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("replay read its source %d time(s) before refusing the port", got)
	}
}

// replayEnginePorts is what decides the registry port of a replay's engines: the local engine's
// params, and the orchestrator that starts every remote and distributed engine.
type replayEnginePorts struct {
	launches           int
	params             scenario.Params
	orchestrator       *tui.MultiHostOrchestrator
	connectedPort      int
	connectedPortValid bool
}

// runReplayToLaunch runs replay with stubbed engines up to its launch and reports the ports it
// launched with.
func runReplayToLaunch(t *testing.T, server *httptest.Server, hosts string, headlessMode bool, extra ...string) replayEnginePorts {
	t.Helper()
	t.Cleanup(installReplayEngineInfoSeams(t))
	var got replayEnginePorts
	resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
		return &portcheck.ResolutionResult{Success: true}, nil
	}
	connectReplayOrchestrator = func(_ context.Context, orchestrator *tui.MultiHostOrchestrator) error {
		// The hosts connect, and their ports are checked, on the replay's registry port.
		got.connectedPort = orchestrator.RMIRegistryPort()
		got.connectedPortValid = true
		return nil
	}
	confirmReplayLaunchCommand = func(io.Writer) error { return nil }
	shouldReplayRunHeadless = func(*cobra.Command) bool { return headlessMode }
	launch := func(params scenario.Params, orchestrator *tui.MultiHostOrchestrator, hooks tui.LaunchHooks) error {
		got.launches++
		got.params = params
		got.orchestrator = orchestrator
		hooks.NotifySubmitted()
		return nil
	}
	startReplayLocalHeadless = func(
		_ string, _ string, params scenario.Params, options headless.HeadlessOptions, _ []byte, _ []byte,
	) error {
		return launch(params, nil, options.LaunchHooks)
	}
	startReplayLocalTUI = func(_ string, _ string, params scenario.Params, options tui.RunOptions) error {
		return launch(params, nil, options.LaunchHooks)
	}
	startReplayRemoteHeadless = func(
		orchestrator *tui.MultiHostOrchestrator, _ string, _ string, params scenario.Params,
		options headless.HeadlessOptions, _ []byte, _ []byte,
	) error {
		return launch(params, orchestrator, options.LaunchHooks)
	}
	startReplayRemoteTUI = func(
		orchestrator *tui.MultiHostOrchestrator, _ string, _ string, params scenario.Params, options tui.RunOptions,
	) error {
		return launch(params, orchestrator, options.LaunchHooks)
	}
	startReplayAutoResultsMonitor = func(
		context.Context, string, string, string, []string, int64,
		bool, []*hostparse.HostInfo, string, bool, int, string,
		*runMetadata, io.Writer, io.Writer, string, func(context.Context),
		...*integrity.FinalizeOptions,
	) *autoResultsMonitor {
		done := make(chan autoResultsOutcome, 1)
		done <- autoResultsOutcome{SummaryRead: true, StepsWithResults: 1}
		return &autoResultsMonitor{done: done, armed: make(chan struct{})}
	}

	cmd := newReplayCommandForTest(t)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs(append([]string{
		"--from", server.URL,
		"--endpoints", "http://s3.example",
		"--test-hosts", hosts,
		"--results-dir", t.TempDir(),
	}, extra...))
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if got.launches != 1 {
		t.Fatalf("launches = %d, want 1", got.launches)
	}
	return got
}

type replayPortRoute struct {
	name, hosts string
	headless    bool
	remote      bool
	extra       []string
}

var replayPortRoutes = []replayPortRoute{
	{name: "local headless", hosts: "127.0.0.1", headless: true},
	{name: "local TUI", hosts: "127.0.0.1"},
	{name: "lone remote headless", hosts: "entry.example", headless: true, remote: true},
	{name: "lone remote TUI", hosts: "entry.example", remote: true},
	{name: "distributed headless", hosts: "entry.example,worker.example", headless: true, remote: true},
	{name: "distributed TUI", hosts: "entry.example,worker.example", remote: true},
	{name: "distributed with attached workers", hosts: "entry.example,worker.example", headless: true, remote: true,
		extra: []string{"--" + flagAttachExistingWorkers}},
}

// --rmi-registry-port reaches every engine of a replay: the local engine through its params, and
// the lone remote engine, the entry and the workers through the orchestrator that starts them, as
// spt run's does.
func TestReplayCommandRMIRegistryPortReachesEveryEngine(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()
	const port = 11099

	for _, route := range replayPortRoutes {
		t.Run(route.name, func(t *testing.T) {
			args := append([]string{"--" + flagRMIRegistryPort, strconv.Itoa(port)}, route.extra...)
			got := runReplayToLaunch(t, server, route.hosts, route.headless, args...)
			if got.params.RMIRegistryPort != port {
				t.Fatalf("params RMI registry port = %d, want %d", got.params.RMIRegistryPort, port)
			}
			if !route.remote {
				if got.orchestrator != nil || got.connectedPortValid {
					t.Fatal("a local replay used the orchestrator")
				}
				return
			}
			if !got.connectedPortValid || got.connectedPort != port {
				t.Fatalf("orchestrator RMI registry port at connect = %d (connected %v), want %d",
					got.connectedPort, got.connectedPortValid, port)
			}
			if got.orchestrator == nil || got.orchestrator.RMIRegistryPort() != port {
				t.Fatalf("orchestrator at launch = %v, want RMI registry port %d", got.orchestrator, port)
			}
		})
	}
}

// At the default registry port, with or without the flag, a replay launches its engines as it did
// before replay had the flag: the params carry no registry port, so the local engine adds no
// node-port argument and its startup arguments are unchanged, and the orchestrator holds no port,
// so it starts the remote and distributed engines on 1099 with their commands unchanged
// (tui.TestDefaultEnginePortsKeepEveryEngineCommand pins those commands). Run on the base, the
// no-flag case gives these same values.
func TestReplayDefaultRegistryPortKeepsEveryEngineCommand(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()

	for _, route := range replayPortRoutes {
		t.Run(route.name, func(t *testing.T) {
			withoutFlag := runReplayToLaunch(t, server, route.hosts, route.headless, route.extra...)
			withDefault := runReplayToLaunch(t, server, route.hosts, route.headless,
				append([]string{"--" + flagRMIRegistryPort, strconv.Itoa(constants.DefaultRMIRegistryPort)}, route.extra...)...)
			for name, got := range map[string]replayEnginePorts{"no flag": withoutFlag, "--rmi-registry-port=1099": withDefault} {
				if got.params.RMIRegistryPort != 0 {
					t.Fatalf("%s: params RMI registry port = %d, want 0 (the engine default)", name, got.params.RMIRegistryPort)
				}
				args, err := scenario.BuildEngineStartupArgs(got.params)
				if err != nil || len(args) != 0 {
					t.Fatalf("%s: engine startup args = %q, %v; want none", name, args, err)
				}
				if !route.remote {
					continue
				}
				if got.connectedPort != constants.DefaultRMIRegistryPort ||
					got.orchestrator.RMIRegistryPort() != constants.DefaultRMIRegistryPort {
					t.Fatalf("%s: orchestrator RMI registry port at connect %d, at launch %d; want %d", name,
						got.connectedPort, got.orchestrator.RMIRegistryPort(), constants.DefaultRMIRegistryPort)
				}
			}
			// The run ID is a timestamp, so it differs between any two replays.
			withoutFlag.params.RunID, withDefault.params.RunID = 0, 0
			if !reflect.DeepEqual(withoutFlag.params, withDefault.params) {
				t.Fatalf("the default flag changed the params:\n no flag: %+v\n 1099:    %+v", withoutFlag.params, withDefault.params)
			}
		})
	}
}
