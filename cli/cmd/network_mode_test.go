/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
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
type runNetworkModeRouteCalls struct {
	port, connect, local, remote int
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
	resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
		calls.port++
		return &portcheck.ResolutionResult{Success: true}, nil
	}
	connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error {
		calls.connect++
		return nil
	}
	startLocalHeadlessRunFunc = func(_ string, _ string, _ scenario.Params, options headless.HeadlessOptions) error {
		calls.local++
		options.LaunchHooks.NotifySubmitted()
		return errNetworkModeRouteReached
	}
	startMultiHostHeadlessRunFunc = func(
		_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions,
	) error {
		calls.remote++
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
		for _, value := range []string{"hots", "Host", ""} {
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
		for _, value := range []string{"", constants.HostNetworkMode, constants.BridgeNetworkMode} {
			label := value
			if label == "" {
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
				want := runNetworkModeRouteCalls{port: route.wantPort, connect: route.wantConnect, local: route.wantLocal, remote: route.wantRemote}
				if *calls != want {
					t.Fatalf("route calls = %+v, want %+v", *calls, want)
				}
				wantWarning := route.bridgeWarns && value == constants.BridgeNetworkMode
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
type replayNetworkModeCalls struct {
	port, multi, single, connect int
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
	resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
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
	connectReplayOrchestrator = func(context.Context, *tui.MultiHostOrchestrator) error {
		calls.connect++
		return errReplayNetworkModeConnect
	}
	return calls
}

func TestReplayCommandRefusesAnUnknownNetworkModeBeforeAnyFetchOrStart(t *testing.T) {
	for _, hosts := range []string{"127.0.0.1", "qa-client-01", "qa-entry-01,qa-worker-01"} {
		for _, value := range []string{"hots", "Host", ""} {
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
		for _, value := range []string{"", constants.HostNetworkMode, constants.BridgeNetworkMode} {
			label := value
			if label == "" {
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
				if *calls != route.want {
					t.Fatalf("replay calls = %+v, want %+v", *calls, route.want)
				}
				wantWarning := route.bridgeWarns && value == constants.BridgeNetworkMode
				if got := strings.Contains(out.String(), replayBridgeWarning); got != wantWarning {
					t.Fatalf("bridge warning printed = %t, want %t:\n%s", got, wantWarning, out.String())
				}
			})
		}
	}
}
