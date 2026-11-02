package cmd

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
	"github.com/dell/storage-performance-tool/cli/internal/poolsizing"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/internal/verification"
)

const gib = int64(1) << 30

// shortfallParams builds a run's params with the shortfall flag registered, as spt run does.
func shortfallParams(t *testing.T, env map[string]string, args ...string) (scenario.Params, error) {
	t.Helper()
	clearEnvDefaultsTestEnv(t)
	t.Setenv(constants.EnvRdmaPoolShortfall, "")
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
	cmd.Flags().String(flagRdmaPoolShortfall, poolsizing.ShortfallFail, "")
	registerEndpointSelectionFlags(cmd.Flags())
	if err := cmd.ParseFlags(append([]string{"--endpoints", "http://192.0.2.10:9000"}, args...)); err != nil {
		t.Fatal(err)
	}
	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		t.Fatal(err)
	}
	return buildScenarioParams(WorkloadTypeWrite, cmd)
}

func TestRdmaPoolShortfallComesFromTheFlagOrTheEnvironment(t *testing.T) {
	params, err := shortfallParams(t, nil, "--s3-driver", "rdma", "--interfaces", "auto", "--rdma-pool-shortfall", "warn")
	if err != nil || params.RdmaPoolShortfall != "warn" {
		t.Fatalf("flag: %q, %v", params.RdmaPoolShortfall, err)
	}
	params, err = shortfallParams(t, nil, "--s3-driver", "rdma", "--interfaces", "auto", "--rdma-pool-shortfall", "fail")
	if err != nil || params.RdmaPoolShortfall != "fail" {
		t.Fatalf("flag: %q, %v", params.RdmaPoolShortfall, err)
	}
	params, err = shortfallParams(t, map[string]string{constants.EnvRdmaPoolShortfall: "warn"}, "--s3-driver", "rdma", "--interfaces", "auto")
	if err != nil || params.RdmaPoolShortfall != "warn" {
		t.Fatalf("environment: %q, %v", params.RdmaPoolShortfall, err)
	}
	params, err = shortfallParams(t, nil, "--s3-driver", "rdma", "--interfaces", "auto")
	if err != nil || params.RdmaPoolShortfall != "" {
		t.Fatalf("not given: %q, %v", params.RdmaPoolShortfall, err)
	}
}

func TestAnInvalidRdmaPoolShortfallNamesItsSource(t *testing.T) {
	_, err := shortfallParams(t, nil, "--s3-driver", "rdma", "--interfaces", "auto", "--rdma-pool-shortfall", "maybe")
	if err == nil || !strings.Contains(err.Error(), `--rdma-pool-shortfall must be fail or warn, not "maybe"`) {
		t.Fatalf("err = %v", err)
	}
	_, err = shortfallParams(t, map[string]string{constants.EnvRdmaPoolShortfall: "maybe"}, "--s3-driver", "rdma", "--interfaces", "auto")
	if err == nil || !strings.Contains(err.Error(), "RDMA_POOL_SHORTFALL (environment or .env) must be fail or warn") {
		t.Fatalf("err = %v", err)
	}
}

func TestANicSetIsAcceptedOnTheRdmaDriverAndItsRejectionsStay(t *testing.T) {
	if _, err := shortfallParams(t, nil, "--s3-driver", "rdma", "--interfaces", "mlx5_0,mlx5_1"); err != nil {
		t.Fatalf("a list on s3-rdma: %v", err)
	}
	if _, err := shortfallParams(t, map[string]string{constants.EnvInterfaces: "auto"}, "--s3-driver", "rdma"); err != nil {
		t.Fatalf("auto from the environment on s3-rdma: %v", err)
	}
	for name, tc := range map[string]struct {
		env  map[string]string
		args []string
		want string
	}{
		"a device from a flag": {nil, []string{"--s3-driver", "rdma", "--interfaces", "auto", "--rdma-device", "mlx5_0"},
			"--interfaces cannot be combined with --rdma-device mlx5_0"},
		"a device from the environment": {map[string]string{constants.EnvRdmaDevice: "mlx5_0"}, []string{"--s3-driver", "rdma", "--interfaces", "auto"},
			"--interfaces cannot be combined with RDMA_DEVICE (environment or .env) mlx5_0"},
		"a local IP from a flag": {nil, []string{"--s3-driver", "rdma", "--interfaces", "auto", "--rdma-local-ip", "192.0.2.31"},
			"--interfaces cannot be combined with --rdma-local-ip 192.0.2.31"},
		"a local IP from the environment": {map[string]string{constants.EnvRdmaLocalIP: "192.0.2.31"}, []string{"--s3-driver", "rdma", "--interfaces", "auto"},
			"--interfaces cannot be combined with RDMA_LOCAL_IP (environment or .env) 192.0.2.31"},
		"a device with a host override": {nil, []string{"--s3-driver", "rdma", "--test-hosts", "wrk1", "--host-interfaces", "wrk1=auto", "--rdma-device", "mlx5_0"},
			"--host-interfaces cannot be combined with --rdma-device mlx5_0"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := shortfallParams(t, tc.env, tc.args...)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestTheShortfallModeReachesTheEngineDefaultsOnlyWhenGiven(t *testing.T) {
	params := scenario.Params{WorkloadType: WorkloadTypeWrite, S3Driver: scenario.S3DriverRdma, Endpoint: "http://192.0.2.10:9000", Bucket: "b", Threads: 1, ObjectSize: "1MiB"}
	without, err := scenario.GenerateDefaults(params)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(without), "poolShortfall") {
		t.Fatalf("without the flag the defaults are unchanged:\n%s", without)
	}
	params.RdmaPoolShortfall = "warn"
	with, err := scenario.GenerateDefaults(params)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(with), "poolShortfall: warn") {
		t.Fatalf("%s", with)
	}
}

func stubPoolPlan(t *testing.T, plan poolsizing.Plan, planErr error) *int {
	t.Helper()
	calls := 0
	previous := buildPoolPlanFunc
	buildPoolPlanFunc = func(context.Context, poolsizing.Input) (poolsizing.Plan, error) {
		calls++
		return plan, planErr
	}
	t.Cleanup(func() {
		buildPoolPlanFunc = previous
		poolsizing.ClearLaunchOptions()
	})
	return &calls
}

func rdmaRunParams() scenario.Params {
	return scenario.Params{WorkloadType: WorkloadTypeWrite, S3Driver: scenario.S3DriverRdma, Interfaces: []string{"auto"}, Threads: 96, ObjectSize: "1GiB"}
}

func TestSizingIsNotAppliedWithoutANicSetForS3OrWithThePoolOff(t *testing.T) {
	calls := stubPoolPlan(t, poolsizing.Plan{}, nil)
	for name, change := range map[string]func(*scenario.Params){
		"no NIC set": func(p *scenario.Params) { p.Interfaces = nil },
		"s3":         func(p *scenario.Params) { p.S3Driver = scenario.S3DriverDefault },
		"pool off":   func(p *scenario.Params) { p.RdmaDisableBufferPool = true },
	} {
		params := rdmaRunParams()
		change(&params)
		engines, err := planPoolSizing(context.Background(), params, nil, []*hostparse.HostInfo{{Host: "h", IsLocal: true}}, "image", false)
		if err != nil || engines != nil {
			t.Fatalf("%s: %v, %v", name, engines, err)
		}
	}
	if *calls != 0 {
		t.Fatalf("no host was asked: %d", *calls)
	}
	if poolsizing.LaunchOptionsFor(poolsizing.LocalHostKey).JavaOpts != "" {
		t.Fatal("the JVM options stay as they are")
	}
}

func sizedPlan(host string, limit int64) poolsizing.Plan {
	return poolsizing.Plan{Engines: []poolsizing.Engine{poolsizing.Decide(poolsizing.DecideInput{
		Host: host,
		Output: &nicplan.Output{
			Plan:       &nicplan.Plan{OK: true},
			Capacity:   &nicplan.Capacity{Bytes: 256 * gib, Source: "MemTotal"},
			PoolDemand: &nicplan.PoolDemand{Steps: []nicplan.PoolStep{{ID: "step", LimitBytes: limit, Known: true}}},
		},
	})}}
}

func TestASizedRunRecordsAndPassesItsValue(t *testing.T) {
	stubPoolPlan(t, sizedPlan("127.0.0.1", 192*gib), nil)
	hosts := []*hostparse.HostInfo{{Host: "127.0.0.1", IsLocal: true}}
	engines, err := planPoolSizing(context.Background(), rdmaRunParams(), nil, hosts, "image", false)
	if err != nil {
		t.Fatal(err)
	}
	if len(engines) != 1 || engines[0].PassedBytes != 192*gib || engines[0].CalculatedBytes != 192*gib {
		t.Fatalf("%+v", engines)
	}
	if got := poolsizing.LaunchOptionsFor(poolsizing.LocalHostKey).JavaOpts; got != "-XX:MaxDirectMemorySize=192g" {
		t.Fatalf("%q", got)
	}
}

func TestANeedAboveCapacityStopsTheRunBeforeLaunch(t *testing.T) {
	stubPoolPlan(t, sizedPlan("127.0.0.1", 512*gib), nil)
	hosts := []*hostparse.HostInfo{{Host: "127.0.0.1", IsLocal: true}}
	_, err := planPoolSizing(context.Background(), rdmaRunParams(), nil, hosts, "image", false)
	if err == nil || !strings.Contains(err.Error(), "needs 512GiB") || !strings.Contains(err.Error(), "but has 248GiB") {
		t.Fatalf("err = %v", err)
	}
	if poolsizing.LaunchOptionsFor(poolsizing.LocalHostKey).JavaOpts != "" {
		t.Fatal("nothing launches")
	}
}

func TestAPlanThatCannotBeBuiltSaysSo(t *testing.T) {
	stubPoolPlan(t, poolsizing.Plan{}, errors.New("no request"))
	_, err := planPoolSizing(context.Background(), rdmaRunParams(), nil, nil, "image", false)
	if err == nil || !strings.Contains(err.Error(), "size the RDMA pools: no request") {
		t.Fatalf("err = %v", err)
	}
}

func TestVerifyPreviewFailsAShortPoolUnderFailAndWarnsUnderWarn(t *testing.T) {
	for _, tc := range []struct {
		name      string
		shortfall string
		ready     bool
	}{{"fail", poolsizing.ShortfallFail, false}, {"warn", poolsizing.ShortfallWarn, true}} {
		t.Run(tc.name, func(t *testing.T) {
			engine := poolsizing.Decide(poolsizing.DecideInput{
				Host: "wrk1", Shortfall: tc.shortfall,
				Output: &nicplan.Output{
					Capacity:   &nicplan.Capacity{Bytes: 256 * gib, Source: "cgroup", Cgroup: "/lxc/9"},
					PoolDemand: &nicplan.PoolDemand{Steps: []nicplan.PoolStep{{ID: "create", LimitBytes: 512 * gib, LimitForPoolsBytes: 512 * gib, LimitForProcessBytes: 264 * gib, Known: true}}},
				},
			})
			stubPoolPlan(t, poolsizing.Plan{Engines: []poolsizing.Engine{engine}}, nil)
			cmd := newVerifyTestCmd()
			args := []string{"--use-rdma", "--object-size", "1GiB", "--rdma-pool-shortfall", tc.shortfall}
			if err := cmd.ParseFlags(args); err != nil {
				t.Fatal(err)
			}
			report := &verification.Report{Ready: true, Summary: "2 of 2"}
			hosts := []*hostparse.HostInfo{{Host: "wrk1"}}
			if err := verifyPoolPreview(context.Background(), cmd, verifyPoolInput{hosts: hosts, interfaces: []string{"auto"}, threads: 96}, report); err != nil {
				t.Fatal(err)
			}
			if report.Ready != tc.ready {
				t.Fatalf("ready = %v: %s", report.Ready, report.PoolPreview)
			}
			if !strings.Contains(report.PoolPreview, "RDMA pools on wrk1") || !strings.Contains(report.PoolPreview, "needs 512GiB") {
				t.Fatalf("%s", report.PoolPreview)
			}
		})
	}
}

func TestVerifyShowsNoPoolLinesWithoutTheWorkloadOrWithoutANicSet(t *testing.T) {
	calls := stubPoolPlan(t, poolsizing.Plan{}, nil)
	cmd := newVerifyTestCmd()
	if err := cmd.ParseFlags([]string{"--use-rdma"}); err != nil {
		t.Fatal(err)
	}
	report := &verification.Report{Ready: true}
	if err := verifyPoolPreview(context.Background(), cmd, verifyPoolInput{interfaces: []string{"auto"}, threads: 4}, report); err != nil {
		t.Fatal(err)
	}
	noSet := newVerifyTestCmd()
	if err := noSet.ParseFlags([]string{"--use-rdma", "--object-size", "1GiB"}); err != nil {
		t.Fatal(err)
	}
	if err := verifyPoolPreview(context.Background(), noSet, verifyPoolInput{threads: 4}, report); err != nil {
		t.Fatal(err)
	}
	if *calls != 0 || report.PoolPreview != "" || !report.Ready {
		t.Fatalf("calls %d, preview %q", *calls, report.PoolPreview)
	}
}

func TestVerifyStepsFollowTheRunsWorkload(t *testing.T) {
	ops := func(params scenario.Params) string {
		names := make([]string, 0, 2)
		for _, step := range poolSizingSteps(params) {
			names = append(names, step.Op)
		}
		return strings.Join(names, ",")
	}
	for _, tc := range []struct {
		params scenario.Params
		want   string
	}{
		{scenario.Params{WorkloadType: "write"}, "create"},
		{scenario.Params{WorkloadType: "read"}, "seed,read"},
		{scenario.Params{WorkloadType: "read", ItemsFile: "x.csv"}, "read"},
		{scenario.Params{WorkloadType: "mixed"}, "seed,mixed"},
		{scenario.Params{WorkloadType: "mixed", ReadItemsFile: "x.csv"}, "mixed"},
		{scenario.Params{WorkloadType: "delete"}, "seed,delete"},
		{scenario.Params{WorkloadType: "delete", ItemsFile: "x.csv"}, "delete"},
		{scenario.Params{WorkloadType: "delete", DeleteExisting: true}, "delete"},
		{scenario.Params{WorkloadType: "write-verify"}, "create,read"},
		{scenario.Params{WorkloadType: "read-verify"}, "read"},
	} {
		if got := ops(tc.params); got != tc.want {
			t.Errorf("%+v: %q, want %q", tc.params, got, tc.want)
		}
	}
}
