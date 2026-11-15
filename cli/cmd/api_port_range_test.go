/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
)

// apiPortRefusals are --api-port values that can never be an engine's API port. Each is refused
// with the value it was given; a value that is not plain decimal digits is quoted.
var apiPortRefusals = []struct{ value, want string }{
	{"0", "--api-port must be between 1 and 65535, got 0"},
	{"-1", `--api-port must be between 1 and 65535, got "-1"`},
	{"65536", "--api-port must be between 1 and 65535, got 65536"},
	{"70000", "--api-port must be between 1 and 65535, got 70000"},
	{"070000", "--api-port must be between 1 and 65535, got 070000"},
	{"99999999999999999999", "--api-port must be between 1 and 65535, got 99999999999999999999"},
	{"abc", `--api-port must be between 1 and 65535, got "abc"`},
	{" ", `--api-port must be between 1 and 65535, got " "`},
	{"+20041", `--api-port must be between 1 and 65535, got "+20041"`},
	{"+9999", `--api-port must be between 1 and 65535, got "+9999"`},
	{"99 99", `--api-port must be between 1 and 65535, got "99 99"`},
	{"0x270f", `--api-port must be between 1 and 65535, got "0x270f"`},
}

// apiPortAcceptances are --api-port values a run accepts, with the canonical decimal that every
// later reader sees. An empty value is the unset flag.
var apiPortAcceptances = []struct{ value, want string }{
	{"", "9999"},
	{"1", "1"},
	{"9999", "9999"},
	{"65535", "65535"},
	{"43234", "43234"},
	{"09999", "9999"},
	{" 20041 ", "20041"},
}

var apiPortRunRoutes = []struct{ name, hosts, minHosts string }{
	{"local", "127.0.0.1", "1"},
	{"lone remote", "entry.example", "1"},
	{"distributed", "entry.example,worker.example", "2"},
}

// apiPortRunCalls stubs every route's first port check, connection and launch of spt run, and
// counts the calls. Each stub ends the run with errReached; the port check records its port.
type apiPortRunCalls struct {
	calls       int
	checkedPort string
	errReached  error
}

func stubAPIPortRunRoutes(t *testing.T) *apiPortRunCalls {
	t.Helper()
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
	got := &apiPortRunCalls{errReached: errors.New("the run reached its route")}
	resolvePortConflictFunc = func(_ context.Context, port string, _ bool) (*portcheck.ResolutionResult, error) {
		got.calls++
		got.checkedPort = port
		return nil, got.errReached
	}
	connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error {
		got.calls++
		return got.errReached
	}
	startLocalHeadlessRunFunc = func(string, string, scenario.Params, headless.HeadlessOptions) error {
		got.calls++
		return got.errReached
	}
	startMultiHostHeadlessRunFunc = func(
		*tui.MultiHostOrchestrator, string, string, scenario.Params, headless.HeadlessOptions,
	) error {
		got.calls++
		return got.errReached
	}
	return got
}

// spt run refuses an --api-port that is not plain decimal digits from 1 to 65535 on every route,
// before any port check, connection or launch.
func TestRunCmdRefusesAnAPIPortOutsideTheTCPRangeOnEveryRoute(t *testing.T) {
	for _, route := range apiPortRunRoutes {
		for _, tc := range apiPortRefusals {
			t.Run(route.name+"/"+tc.value, func(t *testing.T) {
				t.Chdir(t.TempDir())
				setDeleteRouteFlags(t, route.hosts, route.minHosts)
				setGlobalRunFlagForTest(t, flagAPIPort, tc.value)
				got := stubAPIPortRunRoutes(t)

				err := runCmd.RunE(runCmd, []string{WorkloadTypeDelete})
				if err == nil || err.Error() != tc.want {
					t.Fatalf("RunE() error = %v, want %q", err, tc.want)
				}
				if got.calls != 0 {
					t.Fatalf("the run checked ports, connected or launched %d time(s) before refusing", got.calls)
				}
			})
		}
	}
}

// --generate-only checks --api-port too, as it checks --rmi-registry-port, and writes nothing.
func TestRunCmdGenerateOnlyRefusesAnAPIPortOutsideTheTCPRange(t *testing.T) {
	dir := t.TempDir()
	t.Chdir(dir)
	setDeleteRouteFlags(t, "127.0.0.1", "1")
	setGlobalRunFlagForTest(t, "generate-only", "true")
	setGlobalRunFlagForTest(t, flagAPIPort, "70000")
	got := stubAPIPortRunRoutes(t)

	err := runCmd.RunE(runCmd, []string{WorkloadTypeDelete})
	if want := "--api-port must be between 1 and 65535, got 70000"; err == nil || err.Error() != want {
		t.Fatalf("RunE() error = %v, want %q", err, want)
	}
	if got.calls != 0 {
		t.Fatalf("the run checked ports, connected or launched %d time(s)", got.calls)
	}
	entries, readErr := os.ReadDir(dir)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if len(entries) != 0 {
		t.Fatalf("generate-only wrote %d file(s) before refusing the port", len(entries))
	}
}

// An accepted --api-port reaches the route, and every later reader of the flag sees its canonical
// decimal: the local port check gets it, and getAPIPort gives it to the remote and distributed
// orchestrator, the base URL and the run metadata.
func TestRunCmdUsesTheCanonicalAPIPortOnEveryRoute(t *testing.T) {
	for _, route := range apiPortRunRoutes {
		for _, tc := range apiPortAcceptances {
			t.Run(route.name+"/"+tc.value, func(t *testing.T) {
				t.Chdir(t.TempDir())
				setDeleteRouteFlags(t, route.hosts, route.minHosts)
				setGlobalRunFlagForTest(t, flagAPIPort, tc.value)
				got := stubAPIPortRunRoutes(t)

				err := runCmd.RunE(runCmd, []string{WorkloadTypeDelete})
				if !errors.Is(err, got.errReached) {
					t.Fatalf("RunE() error = %v, want the route to be reached", err)
				}
				if route.name == "local" && got.checkedPort != tc.want {
					t.Fatalf("port check port = %q, want %q", got.checkedPort, tc.want)
				}
				if port := getAPIPort(runCmd); port != tc.want {
					t.Fatalf("getAPIPort() = %q, want %q", port, tc.want)
				}
			})
		}
	}
}

// spt replay refuses an --api-port that is not plain decimal digits from 1 to 65535 on every
// route, before it reads its source, checks a port or starts an engine.
func TestReplayCommandRefusesAnAPIPortOutsideTheTCPRange(t *testing.T) {
	var requests atomic.Int32
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer source.Close()
	t.Cleanup(installReplayEngineInfoSeams(t))
	var calls atomic.Int32
	resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
		calls.Add(1)
		return nil, errors.New("the replay checked its port")
	}
	connectReplayOrchestrator = func(context.Context, *tui.MultiHostOrchestrator) error {
		calls.Add(1)
		return errors.New("the replay connected")
	}

	for _, hosts := range []string{"127.0.0.1", "entry.example", "entry.example,worker.example"} {
		for _, generateOnly := range []bool{false, true} {
			for _, tc := range apiPortRefusals {
				name := hosts + "/" + tc.value
				if generateOnly {
					name += "/generate-only"
				}
				t.Run(name, func(t *testing.T) {
					cmd := newReplayCommandForTest(t)
					cmd.SetOut(io.Discard)
					cmd.SetErr(io.Discard)
					args := []string{"--from", source.URL, "--endpoints", "http://192.0.2.1:9020",
						"--test-hosts", hosts, "--results-dir", t.TempDir(), "--" + flagAPIPort, tc.value}
					if generateOnly {
						args = append(args, "--generate-only", "--output-dir", t.TempDir())
					}
					cmd.SetArgs(args)
					if err := cmd.Execute(); err == nil || err.Error() != tc.want {
						t.Fatalf("Execute() error = %v, want %q", err, tc.want)
					}
				})
			}
		}
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("replay read its source %d time(s) before refusing the port", got)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("replay checked a port or connected %d time(s) before refusing the port", got)
	}
}

// replayAPIPorts is the API port each reader of a replay saw.
type replayAPIPorts struct {
	launches    int
	checkedPort string
	checked     bool
	launchPort  string
	monitorPort string
	baseURL     string
}

// An accepted --api-port reaches every engine of a replay as its canonical decimal: the local port
// check, the headless launch, the auto-results monitor and its base URL.
func TestReplayCommandUsesTheCanonicalAPIPortOnEveryRoute(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()
	routes := []struct {
		name, hosts string
		remote      bool
	}{
		{"local", "127.0.0.1", false},
		{"lone remote", "entry.example", true},
		{"distributed", "entry.example,worker.example", true},
	}
	for _, route := range routes {
		for _, tc := range apiPortAcceptances {
			t.Run(route.name+"/"+tc.value, func(t *testing.T) {
				got := runReplayToLaunchWithAPIPort(t, server, route.hosts, tc.value)
				if got.launches != 1 {
					t.Fatalf("launches = %d, want 1", got.launches)
				}
				if route.remote == got.checked {
					t.Fatalf("local port check ran = %v on the %s route", got.checked, route.name)
				}
				if !route.remote && got.checkedPort != tc.want {
					t.Fatalf("port check port = %q, want %q", got.checkedPort, tc.want)
				}
				if got.launchPort != tc.want || got.monitorPort != tc.want {
					t.Fatalf("launch API port = %q, monitor API port = %q, want %q", got.launchPort, got.monitorPort, tc.want)
				}
				if !strings.HasSuffix(got.baseURL, ":"+tc.want) {
					t.Fatalf("base URL = %q, want port %s", got.baseURL, tc.want)
				}
			})
		}
	}
}

// runReplayToLaunchWithAPIPort runs a headless replay with stubbed engines up to its launch and
// reports the API port each reader saw.
func runReplayToLaunchWithAPIPort(t *testing.T, server *httptest.Server, hosts, apiPort string) replayAPIPorts {
	t.Helper()
	t.Cleanup(installReplayEngineInfoSeams(t))
	var got replayAPIPorts
	resolvePortConflictFunc = func(_ context.Context, port string, _ bool) (*portcheck.ResolutionResult, error) {
		got.checked = true
		got.checkedPort = port
		return &portcheck.ResolutionResult{Success: true}, nil
	}
	connectReplayOrchestrator = func(context.Context, *tui.MultiHostOrchestrator) error { return nil }
	confirmReplayLaunchCommand = func(io.Writer) error { return nil }
	shouldReplayRunHeadless = func(*cobra.Command) bool { return true }
	launch := func(options headless.HeadlessOptions) error {
		got.launches++
		got.launchPort = options.APIPort
		options.LaunchHooks.NotifySubmitted()
		return nil
	}
	startReplayLocalHeadless = func(
		_ string, _ string, _ scenario.Params, options headless.HeadlessOptions, _ []byte, _ []byte,
	) error {
		return launch(options)
	}
	startReplayRemoteHeadless = func(
		_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params,
		options headless.HeadlessOptions, _ []byte, _ []byte,
	) error {
		return launch(options)
	}
	startReplayAutoResultsMonitor = func(
		_ context.Context, baseURL string, _ string, _ string, _ []string, _ int64,
		_ bool, _ []*hostparse.HostInfo, monitorPort string, _ bool, _ int, _ string,
		_ *runMetadata, _ io.Writer, _ io.Writer, _ string, _ func(context.Context),
		_ ...*integrity.FinalizeOptions,
	) *autoResultsMonitor {
		got.baseURL = baseURL
		got.monitorPort = monitorPort
		done := make(chan autoResultsOutcome, 1)
		done <- autoResultsOutcome{SummaryRead: true, StepsWithResults: 1}
		return &autoResultsMonitor{done: done, armed: make(chan struct{})}
	}

	cmd := newReplayCommandForTest(t)
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)
	cmd.SetArgs([]string{
		"--from", server.URL,
		"--endpoints", "http://s3.example",
		"--test-hosts", hosts,
		"--results-dir", t.TempDir(),
		"--" + flagAPIPort, apiPort,
	})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	return got
}
