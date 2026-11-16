package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"slices"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
	"github.com/dell/storage-performance-tool/cli/internal/poolsizing"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
)

const (
	poolProbeReleaseImage = constants.DefaultSptImage + ":v9.9.9"
	poolProbeDevImage     = constants.DefaultSptImage + ":" + constants.DevImageTag
)

// capturePoolPlanInput replaces the plan with one that records its input.
func capturePoolPlanInput(t *testing.T) *[]poolsizing.Input {
	t.Helper()
	var inputs []poolsizing.Input
	previous := buildPoolPlanFunc
	buildPoolPlanFunc = func(_ context.Context, in poolsizing.Input) (poolsizing.Plan, error) {
		inputs = append(inputs, in)
		return poolsizing.Plan{}, nil
	}
	t.Cleanup(func() {
		buildPoolPlanFunc = previous
		poolsizing.ClearLaunchOptions()
	})
	return &inputs
}

// capturePoolLog points the shared logger, spt.log's, at a buffer for one test.
func capturePoolLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var out bytes.Buffer
	previous := logging.GetLogger()
	logging.SetLogger(slog.New(slog.NewTextHandler(&out, nil)))
	t.Cleanup(func() { logging.SetLogger(previous) })
	return &out
}

func probeHasPullNever(in poolsizing.Input) bool {
	command := poolsizing.ProbeCommand(in.Image, in.NeverPull, true, []string{"--rdma"})
	return strings.Contains(strings.Join(command, " "), "--pull never")
}

func probeHasPull(in poolsizing.Input) bool {
	command := poolsizing.ProbeCommand(in.Image, in.NeverPull, true, []string{"--rdma"})
	return slices.Contains(command, "--pull")
}

// The probe follows the documented skip: only a dev image is never pulled. A missing non-dev image
// is pulled even under the skip, since sizing runs before any route ensures the image.
func TestPoolSizingProbeNeverPullsOnlyADevImage(t *testing.T) {
	for _, tc := range []struct {
		name, skip, image string
		neverPull         bool
	}{
		{"skip with a release image", "true", poolProbeReleaseImage, false},
		{"skip as 1 with a release image", "1", poolProbeReleaseImage, false},
		{"no skip with a release image", "", poolProbeReleaseImage, false},
		{"skip false with a release image", "false", poolProbeReleaseImage, false},
		{"skip with a dev image", "true", poolProbeDevImage, true},
		{"no skip with a dev image", "", poolProbeDevImage, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(constants.EnvSkipImagePull, tc.skip)
			inputs := capturePoolPlanInput(t)
			hosts := []*hostparse.HostInfo{{Host: "h", IsLocal: true}}
			if _, err := planPoolSizing(context.Background(), rdmaRunParams(), nil, hosts, tc.image, false); err != nil {
				t.Fatal(err)
			}
			if len(*inputs) != 1 {
				t.Fatalf("plan calls = %d", len(*inputs))
			}
			in := (*inputs)[0]
			if in.Image != tc.image || in.NeverPull != tc.neverPull {
				t.Fatalf("image %q never-pull %v, want %q %v", in.Image, in.NeverPull, tc.image, tc.neverPull)
			}
			if got := probeHasPullNever(in); got != tc.neverPull {
				t.Fatalf("--pull never in the probe = %v, want %v", got, tc.neverPull)
			}
			if !tc.neverPull && probeHasPull(in) {
				t.Fatal("a non-dev image's probe has no --pull flag, so Docker pulls only a missing image")
			}
		})
	}
}

// missingImageHost is a host without the image: --pull never fails as Docker does, and any other
// probe pulls it and answers with a plan and 1 TiB.
type missingImageHost struct {
	command.CommandExecutor
	t     *testing.T
	pulls int
}

func (h *missingImageHost) ExecuteCommand(_ context.Context, _ *hostparse.HostInfo, cmd []string) (string, string, error) {
	if len(cmd) < 2 || cmd[1] != "run" {
		return "MemTotal:       1073741824 kB", "", nil
	}
	if strings.Contains(strings.Join(cmd, " "), "--pull never") {
		return "", "docker: Error response from daemon: No such image\n", errors.New("exit status 125")
	}
	h.pulls++
	output := nicplan.Output{Format: nicplan.Format, Version: nicplan.Version}
	if slices.Contains(cmd, "--capacity") {
		output.Capacity = &nicplan.Capacity{Bytes: 1024 * gib, Source: "MemTotal"}
	} else {
		output.Plan = &nicplan.Plan{OK: true, NicSet: []string{"ens1"}}
		output.PoolDemand = &nicplan.PoolDemand{Steps: []nicplan.PoolStep{{ID: "create", LimitBytes: 192 * gib, PoolsNeedBytes: 192 * gib, Known: true}}}
	}
	raw, err := json.Marshal(output)
	if err != nil {
		h.t.Fatal(err)
	}
	return string(raw), "", nil
}

// Under the skip, a host lacking a release image is sized from the run's own plan input: its
// probe pulls the image instead of failing on --pull never.
func TestAHostMissingAReleaseImageIsSizedUnderTheSkip(t *testing.T) {
	t.Setenv(constants.EnvSkipImagePull, "true")
	// An inherited explicit direct-memory limit would make the host "explicit", not sized.
	t.Setenv(constants.EnvSptJavaOpts, "")
	params := rdmaRunParams()
	params.Endpoints = []string{"http://192.0.2.1:9000"}
	in := poolPlanInput(params, nil, []*hostparse.HostInfo{{Host: "wrk1"}}, poolProbeReleaseImage, false)
	host := &missingImageHost{t: t}
	in.Executor = host
	plan, err := poolsizing.BuildPlan(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Engines) != 1 || plan.Engines[0].Mode != poolsizing.ModeSized || plan.Engines[0].Value() != "192g" {
		t.Fatalf("%+v", plan.Engines)
	}
	if host.pulls == 0 {
		t.Fatal("the probe pulled the image")
	}
}

// Under the skip, spt.log says the probe may pull a non-dev image; nothing else logs that line.
func TestPoolSizingLogsAProbePullDespiteTheSkipOnlyForANonDevImage(t *testing.T) {
	for _, tc := range []struct {
		name, skip, image string
		logged            bool
	}{
		{"skip with a release image", "true", poolProbeReleaseImage, true},
		{"no skip with a release image", "", poolProbeReleaseImage, false},
		{"skip false with a release image", "false", poolProbeReleaseImage, false},
		{"skip with a dev image", "true", poolProbeDevImage, false},
		{"no skip with a dev image", "", poolProbeDevImage, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(constants.EnvSkipImagePull, tc.skip)
			capturePoolPlanInput(t)
			logged := capturePoolLog(t)
			hosts := []*hostparse.HostInfo{{Host: "h", IsLocal: true}}
			if _, err := planPoolSizing(context.Background(), rdmaRunParams(), nil, hosts, tc.image, false); err != nil {
				t.Fatal(err)
			}
			if got := strings.Count(logged.String(), poolProbePullDespiteSkipLine); got != btoi(tc.logged) {
				t.Fatalf("line logged %d time(s), want %d:\n%s", got, btoi(tc.logged), logged)
			}
			if tc.logged && !strings.Contains(logged.String(), "image="+tc.image) {
				t.Fatalf("the line names the image:\n%s", logged)
			}
		})
	}
}

// Sizing that does not apply probes nothing, so it logs no probe pull either.
func TestPoolSizingLogsNoProbePullWhenSizingDoesNotApply(t *testing.T) {
	t.Setenv(constants.EnvSkipImagePull, "true")
	inputs := capturePoolPlanInput(t)
	logged := capturePoolLog(t)
	params := rdmaRunParams()
	params.Interfaces = nil
	if _, err := planPoolSizing(context.Background(), params, nil, []*hostparse.HostInfo{{Host: "h", IsLocal: true}}, poolProbeReleaseImage, false); err != nil {
		t.Fatal(err)
	}
	if len(*inputs) != 0 || strings.Contains(logged.String(), poolProbePullDespiteSkipLine) {
		t.Fatalf("plan calls %d, log:\n%s", len(*inputs), logged)
	}
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// spt run --s3-driver s3-rdma --interfaces auto --skip-image-pull sizes with the release image and
// no --pull never on every route, then reaches that route's connect or launch as before, where the
// image is ensured.
func TestRunCmdSizesRdmaPoolsWithAPullableProbeUnderTheSkipOnEveryRoute(t *testing.T) {
	for _, route := range []struct {
		name, hosts, minHosts, reached string
	}{
		{"local", "127.0.0.1", "1", "local launch"},
		{"lone remote", "entry.example", "1", "connect"},
		{"distributed", "entry.example,worker.example", "2", "connect"},
	} {
		t.Run(route.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			t.Setenv(constants.EnvSkipImagePull, "")
			t.Setenv(constants.EnvRdmaEnabled, "")
			setDeleteRouteFlags(t, route.hosts, route.minHosts)
			setGlobalRunFlagForTest(t, "s3-driver", scenario.S3DriverRdma)
			setGlobalRunFlagForTest(t, flagInterfaces, "auto")
			setGlobalRunFlagForTest(t, flagSkipImagePull, "true")
			setGlobalRunFlagForTest(t, flagSptImage, poolProbeReleaseImage)
			logged := capturePoolLog(t)

			var order []string
			var sized poolsizing.Input
			previousPlan := buildPoolPlanFunc
			previousConnect := connectMultiHostOrchestratorFunc
			previousPort := resolvePortConflictFunc
			previousLocal := startLocalHeadlessRunFunc
			t.Cleanup(func() {
				buildPoolPlanFunc = previousPlan
				connectMultiHostOrchestratorFunc = previousConnect
				resolvePortConflictFunc = previousPort
				startLocalHeadlessRunFunc = previousLocal
				poolsizing.ClearLaunchOptions()
			})
			buildPoolPlanFunc = func(_ context.Context, in poolsizing.Input) (poolsizing.Plan, error) {
				order = append(order, "sizing")
				sized = in
				return poolsizing.Plan{}, nil
			}
			reached := errors.New("the run reached its route")
			connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error {
				order = append(order, "connect")
				return reached
			}
			resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			startLocalHeadlessRunFunc = func(string, string, scenario.Params, headless.HeadlessOptions) error {
				order = append(order, "local launch")
				return reached
			}

			err := runCmd.RunE(runCmd, []string{WorkloadTypeWrite})
			if !errors.Is(err, reached) {
				t.Fatalf("RunE() error = %v, want the route to be reached", err)
			}
			if want := []string{"sizing", route.reached}; !slices.Equal(order, want) {
				t.Fatalf("order = %v, want %v", order, want)
			}
			if sized.Image != poolProbeReleaseImage || sized.NeverPull || probeHasPull(sized) {
				t.Fatalf("sizing image %q never-pull %v, probe %v", sized.Image, sized.NeverPull,
					poolsizing.ProbeCommand(sized.Image, sized.NeverPull, true, []string{"--rdma"}))
			}
			if !strings.Contains(logged.String(), poolProbePullDespiteSkipLine) {
				t.Fatalf("spt.log explains the probe's pull under the skip:\n%s", logged)
			}
		})
	}
}
