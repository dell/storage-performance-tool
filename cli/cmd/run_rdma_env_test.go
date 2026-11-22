package cmd

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
)

// Engine containers get RDMA device passthrough when SPT_RDMA is truthy, so a run reaches its route
// with SPT_RDMA set only when it uses the s3-rdma driver, whatever the environment held.
func TestRunCmdKeepsSptRdmaOnlyForTheRdmaDriverOnEveryRoute(t *testing.T) {
	for _, route := range []struct {
		name, hosts, minHosts, reached string
	}{
		{"local", "127.0.0.1", "1", "local launch"},
		{"lone remote", "entry.example", "1", "connect"},
		{"distributed", "entry.example,worker.example", "2", "connect"},
	} {
		for _, tc := range []struct {
			name, env, useRdma, driver string
			want                       bool
		}{
			{"SPT_RDMA=true with --use-rdma=false", "true", "false", "", false},
			{"SPT_RDMA=yes, which use-rdma does not parse", "yes", "", "", false},
			{"SPT_RDMA=true with --s3-driver aws", "true", "", scenario.S3DriverAws, false},
			{"SPT_RDMA unset with --s3-driver rdma", "", "", scenario.S3DriverRdma, true},
		} {
			t.Run(route.name+"/"+tc.name, func(t *testing.T) {
				t.Chdir(t.TempDir())
				t.Setenv(constants.EnvRdmaEnabled, tc.env)
				setDeleteRouteFlags(t, route.hosts, route.minHosts)
				if tc.useRdma != "" {
					setGlobalRunFlagForTest(t, "use-rdma", tc.useRdma)
				}
				if tc.driver != "" {
					setGlobalRunFlagForTest(t, "s3-driver", tc.driver)
				}

				var reachedRoute string
				var rdmaAtRoute bool
				previousConnect := connectMultiHostOrchestratorFunc
				previousPort := resolvePortConflictFunc
				previousLocal := startLocalHeadlessRunFunc
				t.Cleanup(func() {
					connectMultiHostOrchestratorFunc = previousConnect
					resolvePortConflictFunc = previousPort
					startLocalHeadlessRunFunc = previousLocal
				})
				reached := errors.New("the run reached its route")
				connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error {
					reachedRoute, rdmaAtRoute = "connect", constants.IsRdmaEnabled()
					return reached
				}
				resolvePortConflictFunc = func(context.Context, string, portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
					return &portcheck.ResolutionResult{Success: true}, nil
				}
				startLocalHeadlessRunFunc = func(string, string, scenario.Params, headless.HeadlessOptions) error {
					reachedRoute, rdmaAtRoute = "local launch", constants.IsRdmaEnabled()
					return reached
				}

				if err := runCmd.RunE(runCmd, []string{WorkloadTypeWrite}); !errors.Is(err, reached) {
					t.Fatalf("RunE() error = %v, want the route to be reached", err)
				}
				if reachedRoute != route.reached {
					t.Fatalf("reached %q, want %q", reachedRoute, route.reached)
				}
				if rdmaAtRoute != tc.want {
					t.Fatalf("SPT_RDMA truthy at the route = %v, want %v", rdmaAtRoute, tc.want)
				}
			})
		}
	}
}

// A replay never runs s3-rdma, so an SPT_RDMA left in the environment must not reach its engines.
func TestReplayCommandClearsSptRdmaBeforeItsRoute(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="max.s3.sanity.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `export BUCKET=archive-bucket
java -jar ${MONGOOSE_DIR}/mongoose.jar --item-output-path=${BUCKET} --test-scenario-file=/tmp/perf/max.s3.sanity.json`)
	})
	mux.HandleFunc("/max.s3.sanity.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{
  "type": "sequential",
  "config": {"storage": {"driver": {"type": "s3"}}},
  "steps": [
    {"type": "load", "config": {"item": {"data": {"size": "10KB"}}, "test": {"step": {"id": "MAX-W10KB", "limit": {"count": 1}}}, "load": {"limit": {"concurrency": 1}}}}
  ]
}`)
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	t.Setenv(constants.EnvRdmaEnabled, "true")
	connectErr := errors.New("stop at the replay route")
	var rdmaAtRoute, connectCalled bool
	origConnect := connectReplayOrchestrator
	t.Cleanup(func() { connectReplayOrchestrator = origConnect })
	connectReplayOrchestrator = func(context.Context, *tui.MultiHostOrchestrator) error {
		connectCalled, rdmaAtRoute = true, constants.IsRdmaEnabled()
		return connectErr
	}

	var out bytes.Buffer
	cmd := newReplayCommandForTest(t)
	cmd.SetOut(&out)
	cmd.SetArgs([]string{
		"--from", server.URL,
		"--endpoints", "http://10.0.0.1:9020",
		"--test-hosts", "qa-client-01",
		"--results-dir", t.TempDir(),
	})
	if err := cmd.Execute(); !errors.Is(err, connectErr) {
		t.Fatalf("Execute() error = %v, want the connect sentinel", err)
	}
	if !connectCalled || rdmaAtRoute {
		t.Fatalf("connect called %v, SPT_RDMA truthy at the route %v; want true, false", connectCalled, rdmaAtRoute)
	}
}
