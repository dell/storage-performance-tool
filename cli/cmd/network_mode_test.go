/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
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

const runBridgeWarning = "WARNING: Bridge networking may not work for distributed testing."
const replayBridgeWarning = "WARNING: Bridge networking may not work for distributed replay"

func wantNetworkModeError(value string) string {
	return "network-mode must be 'bridge' or 'host', got: " + value
}

// networkModeSpellings are the accepted spt run and replay --network-mode spellings and the mode
// each selects.
var networkModeSpellings = []struct{ value, want string }{
	{"host", constants.HostNetworkMode},
	{"Host", constants.HostNetworkMode},
	{"HOST", constants.HostNetworkMode},
	{" host", constants.HostNetworkMode},
	{"host ", constants.HostNetworkMode},
	{"bridge", constants.BridgeNetworkMode},
	{"Bridge", constants.BridgeNetworkMode},
	{" BRIDGE", constants.BridgeNetworkMode},
}

// refusedNetworkModes are spt run and replay --network-mode values refused before any start. An
// explicit empty value stays refused, as #313 made it.
var refusedNetworkModes = []string{"hots", "hostx", "bridge mode", ""}

func TestNormalizeNetworkModeIgnoresCaseAndSurroundingSpaces(t *testing.T) {
	for _, spelling := range networkModeSpellings {
		got, err := normalizeNetworkMode(spelling.value)
		if err != nil || got != spelling.want {
			t.Fatalf("normalizeNetworkMode(%q) = %q, %v; want %q", spelling.value, got, err, spelling.want)
		}
	}
	for _, value := range refusedNetworkModes {
		got, err := normalizeNetworkMode(value)
		if err == nil || err.Error() != wantNetworkModeError(value) || got != "" {
			t.Fatalf("normalizeNetworkMode(%q) = %q, %v; want %q", value, got, err, wantNetworkModeError(value))
		}
	}
}

// spt verify keeps its exact check: only the run and replay normaliser accepts other spellings.
func TestValidateNetworkModeAcceptsOnlyHostAndBridge(t *testing.T) {
	for _, value := range []string{constants.HostNetworkMode, constants.BridgeNetworkMode} {
		if err := validateNetworkMode(value); err != nil {
			t.Fatalf("validateNetworkMode(%q) = %v, want nil", value, err)
		}
	}
	for _, value := range []string{"hots", "Host", " host", "", "none"} {
		err := validateNetworkMode(value)
		if err == nil || err.Error() != wantNetworkModeError(value) {
			t.Fatalf("validateNetworkMode(%q) = %v, want %q", value, err, wantNetworkModeError(value))
		}
	}
}

// runNetworkModeRouteCalls counts every Docker, port and start call spt run makes on its routes.
// mode is the network mode the local launch options or the remote orchestrator carried.
type runNetworkModeRouteCalls struct {
	port, connect, local, remote int
	mode                         string
}

var errNetworkModeRouteReached = errors.New("network mode route reached")

func stubRunNetworkModeRoutes(t *testing.T) *runNetworkModeRouteCalls {
	t.Helper()
	calls := &runNetworkModeRouteCalls{}
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
	resolvePortConflictFunc = func(context.Context, string, portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
		calls.port++
		return &portcheck.ResolutionResult{Success: true}, nil
	}
	connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error {
		calls.connect++
		return nil
	}
	startLocalHeadlessRunFunc = func(_ string, _ string, _ scenario.Params, options headless.HeadlessOptions) error {
		calls.local++
		calls.mode = options.NetworkMode
		options.LaunchHooks.NotifySubmitted()
		return errNetworkModeRouteReached
	}
	startMultiHostHeadlessRunFunc = func(
		orchestrator *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions,
	) error {
		calls.remote++
		calls.mode = orchestrator.NetworkMode()
		options.LaunchHooks.NotifySubmitted()
		return errNetworkModeRouteReached
	}
	return calls
}

func TestRunCmdRefusesAnUnknownNetworkModeBeforeAnyStart(t *testing.T) {
	for _, route := range []struct{ name, hosts, minHosts string }{
		{"local", "127.0.0.1", "1"},
		{"lone remote", "entry.example", "1"},
		{"distributed", "entry.example,worker.example", "2"},
	} {
		for _, value := range refusedNetworkModes {
			t.Run(fmt.Sprintf("%s %q", route.name, value), func(t *testing.T) {
				dir := t.TempDir()
				t.Chdir(dir)
				setDeleteRouteFlags(t, route.hosts, route.minHosts)
				setGlobalRunFlagForTest(t, "network-mode", value)
				calls := stubRunNetworkModeRoutes(t)

				var runErr error
				stdout := captureStdout(t, func() {
					runErr = runCmd.RunE(runCmd, []string{WorkloadTypeDelete})
				})
				if runErr == nil || runErr.Error() != wantNetworkModeError(value) {
					t.Fatalf("RunE() error = %v, want %q", runErr, wantNetworkModeError(value))
				}
				if *calls != (runNetworkModeRouteCalls{}) {
					t.Fatalf("route calls = %+v, want none", *calls)
				}
				if strings.Contains(stdout, "WARNING") {
					t.Fatalf("a refused run printed a warning:\n%s", stdout)
				}
				if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
					t.Fatalf("a refused run wrote files: %v (err %v)", entries, err)
				}

				command := &cobra.Command{
					Use: "run", SilenceErrors: true, SilenceUsage: true,
					RunE: func(*cobra.Command, []string) error {
						return runCmd.RunE(runCmd, []string{WorkloadTypeDelete})
					},
				}
				if code := executeCommandCode(command); code != 1 {
					t.Fatalf("exit status = %d, want 1", code)
				}
			})
		}
	}
}

// acceptedNetworkModes are the unset flag ("") and every accepted spelling, with the mode each must
// select on every route.
func acceptedNetworkModes() []struct{ value, want string } {
	return append([]struct{ value, want string }{{"", constants.HostNetworkMode}}, networkModeSpellings...)
}

func TestRunCmdAcceptsHostBridgeAndTheDefaultNetworkMode(t *testing.T) {
	for _, route := range []struct {
		name, hosts, minHosts string
		wantLocal, wantRemote int
		wantPort, wantConnect int
		bridgeWarns           bool
	}{
		{name: "local", hosts: "127.0.0.1", minHosts: "1", wantLocal: 1, wantPort: 1},
		{name: "lone remote", hosts: "entry.example", minHosts: "1", wantRemote: 1, wantConnect: 1, bridgeWarns: true},
		{name: "distributed", hosts: "entry.example,worker.example", minHosts: "2", wantRemote: 1, wantConnect: 1, bridgeWarns: true},
	} {
		for _, accepted := range acceptedNetworkModes() {
			value := accepted.value
			label := fmt.Sprintf("%q", value)
			if value == "" {
				label = "unset"
			}
			t.Run(route.name+" "+label, func(t *testing.T) {
				t.Chdir(t.TempDir())
				setDeleteRouteFlags(t, route.hosts, route.minHosts)
				if value != "" {
					setGlobalRunFlagForTest(t, "network-mode", value)
				} else if flag := runCmd.Flags().Lookup("network-mode"); flag.Changed || flag.Value.String() != constants.HostNetworkMode {
					t.Fatalf("--network-mode is %q (changed %t), want the unset host default", flag.Value.String(), flag.Changed)
				}
				calls := stubRunNetworkModeRoutes(t)

				var runErr error
				stdout := captureStdout(t, func() {
					runErr = runCmd.RunE(runCmd, []string{WorkloadTypeDelete})
				})
				if !errors.Is(runErr, errNetworkModeRouteReached) {
					t.Fatalf("RunE() error = %v, want the route sentinel", runErr)
				}
				want := runNetworkModeRouteCalls{
					port: route.wantPort, connect: route.wantConnect, local: route.wantLocal, remote: route.wantRemote,
					mode: accepted.want,
				}
				if *calls != want {
					t.Fatalf("route calls = %+v, want %+v", *calls, want)
				}
				if got, _ := runCmd.Flags().GetString("network-mode"); got != accepted.want {
					t.Fatalf("--network-mode after the run = %q, want the normalised %q", got, accepted.want)
				}
				wantWarning := route.bridgeWarns && accepted.want == constants.BridgeNetworkMode
				if got := strings.Contains(stdout, runBridgeWarning); got != wantWarning {
					t.Fatalf("bridge warning printed = %t, want %t:\n%s", got, wantWarning, stdout)
				}
			})
		}
	}
}

// newNetworkModeReplayArchive serves a one-step archive and counts its requests.
func newNetworkModeReplayArchive(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	var requests atomic.Int64
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="max.s3.sanity.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	mux.HandleFunc("/max.s3.sanity.json", func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		_, _ = fmt.Fprint(w, `{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [
    {"type": "load", "config": {"item": {"data": {"size": "10KB"}}, "test": {"step": {"id": "MAX-W10KB", "limit": {"count": 1}}}, "load": {"limit": {"concurrency": 1}}}}
  ]
}`)
	})
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server, &requests
}

// replayNetworkModeCalls counts every port check, orchestrator and connect call spt replay makes.
// mode is the network mode the remote orchestrator carried when it connected; the local route stops
// at its port check, so TestReplayCommandPassesTheNormalisedNetworkModeToTheLocalLaunch covers it.
type replayNetworkModeCalls struct {
	port, multi, single, connect int
	mode                         string
}

var errReplayNetworkModeConnect = errors.New("stop at replay connect")

func stubReplayNetworkModeRoutes(t *testing.T) *replayNetworkModeCalls {
	t.Helper()
	calls := &replayNetworkModeCalls{}
	previousPort := resolvePortConflictFunc
	previousMulti := newReplayMultiHostOrchestrator
	previousSingle := newReplaySingleHostOrchestrator
	previousConnect := connectReplayOrchestrator
	t.Cleanup(func() {
		resolvePortConflictFunc = previousPort
		newReplayMultiHostOrchestrator = previousMulti
		newReplaySingleHostOrchestrator = previousSingle
		connectReplayOrchestrator = previousConnect
	})
	resolvePortConflictFunc = func(context.Context, string, portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
		calls.port++
		return nil, errReplayNetworkModeConnect
	}
	newReplayMultiHostOrchestrator = func(hostInfos []*hostparse.HostInfo, minHosts int, rmiConfig tui.RMIConfig) *tui.MultiHostOrchestrator {
		calls.multi++
		return previousMulti(hostInfos, minHosts, rmiConfig)
	}
	newReplaySingleHostOrchestrator = func(hostInfos []*hostparse.HostInfo, minHosts int, rmiConfig tui.RMIConfig) *tui.MultiHostOrchestrator {
		calls.single++
		return previousSingle(hostInfos, minHosts, rmiConfig)
	}
	connectReplayOrchestrator = func(_ context.Context, orchestrator *tui.MultiHostOrchestrator) error {
		calls.connect++
		calls.mode = orchestrator.NetworkMode()
		return errReplayNetworkModeConnect
	}
	return calls
}

func TestReplayCommandRefusesAnUnknownNetworkModeBeforeAnyFetchOrStart(t *testing.T) {
	for _, hosts := range []string{"127.0.0.1", "qa-client-01", "qa-entry-01,qa-worker-01"} {
		for _, value := range refusedNetworkModes {
			t.Run(fmt.Sprintf("%s %q", hosts, value), func(t *testing.T) {
				server, requests := newNetworkModeReplayArchive(t)
				calls := stubReplayNetworkModeRoutes(t)
				resultsDir := t.TempDir()

				args := []string{
					"--from", server.URL,
					"--endpoints", "http://10.0.0.1:9020",
					"--test-hosts", hosts,
					"--network-mode", value,
					"--results-dir", resultsDir,
				}
				var out bytes.Buffer
				cmd := newReplayCommandForTest(t)
				cmd.SetOut(&out)
				cmd.SilenceErrors = true
				cmd.SetArgs(args)
				if err := cmd.Execute(); err == nil || err.Error() != wantNetworkModeError(value) {
					t.Fatalf("Execute() error = %v, want %q", err, wantNetworkModeError(value))
				}
				exitCmd := newReplayCommandForTest(t)
				exitCmd.SetOut(&out)
				exitCmd.SilenceErrors = true
				exitCmd.SetArgs(args)
				if code := executeCommandCode(exitCmd); code != 1 {
					t.Fatalf("exit status = %d, want 1", code)
				}
				if got := requests.Load(); got != 0 {
					t.Fatalf("archive requests = %d, want 0", got)
				}
				if *calls != (replayNetworkModeCalls{}) {
					t.Fatalf("replay calls = %+v, want none", *calls)
				}
				if out.Len() != 0 {
					t.Fatalf("a refused replay printed output:\n%s", out.String())
				}
				if entries, err := os.ReadDir(resultsDir); err != nil || len(entries) != 0 {
					t.Fatalf("a refused replay wrote results: %v (err %v)", entries, err)
				}
			})
		}
	}
}

func TestReplayCommandAcceptsHostBridgeAndTheDefaultNetworkMode(t *testing.T) {
	for _, route := range []struct {
		name, hosts string
		want        replayNetworkModeCalls
		bridgeWarns bool
	}{
		{name: "local", hosts: "127.0.0.1", want: replayNetworkModeCalls{port: 1}},
		{name: "lone remote", hosts: "qa-client-01", want: replayNetworkModeCalls{single: 1, connect: 1}},
		{name: "distributed", hosts: "qa-entry-01,qa-worker-01", want: replayNetworkModeCalls{multi: 1, connect: 1}, bridgeWarns: true},
	} {
		for _, accepted := range acceptedNetworkModes() {
			value := accepted.value
			label := fmt.Sprintf("%q", value)
			if value == "" {
				label = "unset"
			}
			t.Run(route.name+" "+label, func(t *testing.T) {
				server, requests := newNetworkModeReplayArchive(t)
				calls := stubReplayNetworkModeRoutes(t)

				var out bytes.Buffer
				cmd := newReplayCommandForTest(t)
				cmd.SetOut(&out)
				args := []string{
					"--from", server.URL,
					"--endpoints", "http://10.0.0.1:9020",
					"--test-hosts", route.hosts,
					"--results-dir", t.TempDir(),
				}
				if value != "" {
					args = append(args, "--network-mode", value)
				}
				cmd.SetArgs(args)
				if err := cmd.Execute(); !errors.Is(err, errReplayNetworkModeConnect) {
					t.Fatalf("Execute() error = %v, want the route sentinel", err)
				}
				if requests.Load() == 0 {
					t.Fatal("a valid replay did not fetch its archive")
				}
				want := route.want
				if want.connect > 0 {
					want.mode = accepted.want
				}
				if *calls != want {
					t.Fatalf("replay calls = %+v, want %+v", *calls, want)
				}
				wantWarning := route.bridgeWarns && accepted.want == constants.BridgeNetworkMode
				if got := strings.Contains(out.String(), replayBridgeWarning); got != wantWarning {
					t.Fatalf("bridge warning printed = %t, want %t:\n%s", got, wantWarning, out.String())
				}
			})
		}
	}
}

func TestReplayCommandPassesTheNormalisedNetworkModeToTheLocalLaunch(t *testing.T) {
	for _, accepted := range acceptedNetworkModes() {
		t.Run(fmt.Sprintf("%q", accepted.value), func(t *testing.T) {
			server, _ := newNetworkModeReplayArchive(t)
			t.Cleanup(installReplayEngineInfoSeams(t))
			resolvePortConflictFunc = func(context.Context, string, portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			shouldReplayRunHeadless = func(*cobra.Command) bool { return true }
			var launched []string
			startReplayLocalHeadless = func(
				_ string, _ string, _ scenario.Params, options headless.HeadlessOptions, _ []byte, _ []byte,
			) error {
				launched = append(launched, options.NetworkMode)
				return errNetworkModeRouteReached
			}
			startReplayAutoResultsMonitor = func(
				context.Context, string, string, string, []string, int64,
				bool, []*hostparse.HostInfo, string, bool, int, string,
				*runMetadata, io.Writer, io.Writer, string, func(context.Context),
				...*integrity.FinalizeOptions,
			) *autoResultsMonitor {
				done := make(chan autoResultsOutcome, 1)
				done <- autoResultsOutcome{}
				return &autoResultsMonitor{done: done, armed: make(chan struct{})}
			}

			cmd := newReplayCommandForTest(t)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			args := []string{
				"--from", server.URL,
				"--endpoints", "http://10.0.0.1:9020",
				"--test-hosts", "127.0.0.1",
				"--results-dir", t.TempDir(),
			}
			if accepted.value != "" {
				args = append(args, "--network-mode", accepted.value)
			}
			cmd.SetArgs(args)
			if err := cmd.Execute(); !errors.Is(err, errNetworkModeRouteReached) {
				t.Fatalf("Execute() error = %v, want the launch sentinel", err)
			}
			if len(launched) != 1 || launched[0] != accepted.want {
				t.Fatalf("local launch network modes = %q, want [%q]", launched, accepted.want)
			}
		})
	}
}

// bridgeNICSetCases are --interfaces and --host-interfaces values that ask for NICs, each given with a
// bridge spelling other than the exact one, with the error requireHostNetwork must still return.
func bridgeNICSetCases(host string) []struct{ name, flag, value, mode, want string } {
	const needsHost = " needs --network-mode host: a bridge-network container sees only its own interface"
	var cases []struct{ name, flag, value, mode, want string }
	for _, mode := range []string{"Bridge", " BRIDGE"} {
		cases = append(cases,
			struct{ name, flag, value, mode, want string }{
				fmt.Sprintf("--interfaces %q", mode), flagInterfaces, "auto", mode, "--" + flagInterfaces + needsHost},
			struct{ name, flag, value, mode, want string }{
				fmt.Sprintf("--host-interfaces %q", mode), flagHostInterfaces, host + "=auto", mode, "--" + flagHostInterfaces + needsHost},
		)
	}
	return cases
}

// The --interfaces checks compare the flag with exactly bridge, so they rely on the flag being
// normalised before they read it.
func TestRunCmdRefusesANICSetWithAnyBridgeSpelling(t *testing.T) {
	for _, route := range []struct{ name, hosts, minHosts string }{
		{"local", "127.0.0.1", "1"},
		{"distributed", "entry.example,worker.example", "2"},
	} {
		for _, tc := range bridgeNICSetCases(strings.Split(route.hosts, ",")[0]) {
			t.Run(route.name+" "+tc.name, func(t *testing.T) {
				dir := t.TempDir()
				t.Chdir(dir)
				setDeleteRouteFlags(t, route.hosts, route.minHosts)
				setGlobalRunFlagForTest(t, "network-mode", tc.mode)
				setGlobalRunFlagForTest(t, tc.flag, tc.value)
				calls := stubRunNetworkModeRoutes(t)

				var runErr error
				captureStdout(t, func() {
					runErr = runCmd.RunE(runCmd, []string{WorkloadTypeDelete})
				})
				if runErr == nil || runErr.Error() != tc.want {
					t.Fatalf("RunE() error = %v, want %q", runErr, tc.want)
				}
				if *calls != (runNetworkModeRouteCalls{}) {
					t.Fatalf("route calls = %+v, want none", *calls)
				}
				if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
					t.Fatalf("a refused run wrote files: %v (err %v)", entries, err)
				}
				command := &cobra.Command{
					Use: "run", SilenceErrors: true, SilenceUsage: true,
					RunE: func(*cobra.Command, []string) error {
						return runCmd.RunE(runCmd, []string{WorkloadTypeDelete})
					},
				}
				if code := executeCommandCode(command); code != 1 {
					t.Fatalf("exit status = %d, want 1", code)
				}
			})
		}
	}
}

func TestReplayCommandRefusesANICSetWithAnyBridgeSpelling(t *testing.T) {
	for _, hosts := range []string{"127.0.0.1", "qa-entry-01,qa-worker-01"} {
		for _, tc := range bridgeNICSetCases(strings.Split(hosts, ",")[0]) {
			t.Run(hosts+" "+tc.name, func(t *testing.T) {
				server, requests := newNetworkModeReplayArchive(t)
				calls := stubReplayNetworkModeRoutes(t)
				resultsDir := t.TempDir()
				args := []string{
					"--from", server.URL,
					"--endpoints", "http://10.0.0.1:9020",
					"--test-hosts", hosts,
					"--network-mode", tc.mode,
					"--" + tc.flag, tc.value,
					"--results-dir", resultsDir,
				}
				newCmd := func() *cobra.Command {
					cmd := newReplayCommandForTest(t)
					cmd.Flags().String(flagInterfaces, "", "")
					cmd.Flags().StringArray(flagHostInterfaces, nil, "")
					cmd.SetOut(io.Discard)
					cmd.SilenceErrors = true
					cmd.SetArgs(args)
					return cmd
				}
				if err := newCmd().Execute(); err == nil || err.Error() != tc.want {
					t.Fatalf("Execute() error = %v, want %q", err, tc.want)
				}
				if code := executeCommandCode(newCmd()); code != 1 {
					t.Fatalf("exit status = %d, want 1", code)
				}
				if got := requests.Load(); got != 0 {
					t.Fatalf("archive requests = %d, want 0", got)
				}
				if *calls != (replayNetworkModeCalls{}) {
					t.Fatalf("replay calls = %+v, want none", *calls)
				}
				if entries, err := os.ReadDir(resultsDir); err != nil || len(entries) != 0 {
					t.Fatalf("a refused replay wrote results: %v (err %v)", entries, err)
				}
			})
		}
	}
}
