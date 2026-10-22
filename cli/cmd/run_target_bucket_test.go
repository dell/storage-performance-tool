package cmd

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"gopkg.in/yaml.v3"
)

func generatedTargetBucket(t *testing.T, params scenario.Params) (map[string]interface{}, bool) {
	t.Helper()
	data, err := scenario.GenerateDefaults(params)
	if err != nil {
		t.Fatal(err)
	}
	var config map[string]interface{}
	if err := yaml.Unmarshal(data, &config); err != nil {
		t.Fatal(err)
	}
	targetBucket, ok := config["storage"].(map[string]interface{})["targetBucket"]
	if !ok {
		return nil, false
	}
	return targetBucket.(map[string]interface{}), true
}

func TestProvisionTargetBucketFlagToEngineYAML(t *testing.T) {
	flag := runCmd.Flags().Lookup(flagProvisionTargetBucket)
	if flag == nil || flag.DefValue != "true" {
		t.Fatalf("run must register --%s defaulting to true", flagProvisionTargetBucket)
	}
	for _, driver := range []string{"default", "aws", "rdma"} {
		for _, args := range [][]string{nil, {"--provision-target-bucket"}, {"--provision-target-bucket=true"}, {"--provision-target-bucket=false"}} {
			t.Run(driver+"/"+strings.Join(args, ","), func(t *testing.T) {
				cmd := newRunLikeCmd()
				cmd.Flags().String("region", "", "")
				cmd.Flags().Bool(flagProvisionTargetBucket, true, flag.Usage)
				if err := cmd.ParseFlags(append([]string{"--endpoints", "https://s3.example.com", "--s3-driver", driver}, args...)); err != nil {
					t.Fatal(err)
				}
				params, err := buildScenarioParams("write", cmd)
				if err != nil {
					t.Fatal(err)
				}
				targetBucket, emitted := generatedTargetBucket(t, params)
				disabled := len(args) == 1 && args[0] == "--provision-target-bucket=false"
				if !disabled {
					if emitted {
						t.Fatalf("default provisioning must leave the engine default, got %v", targetBucket)
					}
					return
				}
				if provision, ok := targetBucket["provision"].(bool); !ok || provision {
					t.Fatalf("storage.targetBucket = %v, want an explicit provision: false", targetBucket)
				}
			})
		}
	}
}

func TestProvisionTargetBucketMissingFlagKeepsProvisioning(t *testing.T) {
	cmd := newRunLikeCmd()
	if err := cmd.ParseFlags([]string{"--endpoints", "https://s3.example.com"}); err != nil {
		t.Fatal(err)
	}
	params, err := buildScenarioParams("write", cmd)
	if err != nil {
		t.Fatal(err)
	}
	if params.TargetBucketProvisionDisabled {
		t.Fatal("a command without the flag must not disable provisioning")
	}
}

func TestEngineOverrideOfTargetBucketPolicyIsAuthoritative(t *testing.T) {
	for _, tc := range []struct {
		disabled bool
		override string
		want     bool
	}{
		{disabled: false, override: "storage.targetBucket.provision=false", want: false},
		{disabled: true, override: "storage.targetBucket.provision=true", want: true},
	} {
		params := scenario.Params{
			WorkloadType: WorkloadTypeWrite, Endpoints: []string{"https://s3.example.com"},
			TargetBucketProvisionDisabled: tc.disabled, EngineOverrides: []string{tc.override},
		}
		targetBucket, emitted := generatedTargetBucket(t, params)
		if !emitted || targetBucket["provision"] != tc.want {
			t.Fatalf("override %q gave storage.targetBucket = %v, want provision %v", tc.override, targetBucket, tc.want)
		}
	}
}

func TestRegionFromS3RegionEnvAndFlagPrecedence(t *testing.T) {
	clearEnvDefaultsTestEnv(t)
	t.Setenv(constants.EnvS3Region, "eu-west-3")

	cmd := newRunLikeCmd()
	cmd.Flags().String("region", "", "")
	if err := applyEnvDefaultsToRunFlags(cmd); err != nil {
		t.Fatal(err)
	}
	if got, _ := cmd.Flags().GetString("region"); got != "eu-west-3" {
		t.Fatalf("region from S3_REGION = %q", got)
	}

	explicit := newRunLikeCmd()
	explicit.Flags().String("region", "", "")
	if err := explicit.ParseFlags([]string{"--region", "us-west-2"}); err != nil {
		t.Fatal(err)
	}
	if err := applyEnvDefaultsToRunFlags(explicit); err != nil {
		t.Fatal(err)
	}
	if got, _ := explicit.Flags().GetString("region"); got != "us-west-2" {
		t.Fatalf("explicit --region must win over S3_REGION, got %q", got)
	}
}

func TestCreatePrefixIsADeprecatedNoOp(t *testing.T) {
	flag := runCmd.Flags().Lookup(flagCreatePrefix)
	if flag == nil {
		t.Fatal("--create-prefix must stay accepted in this release")
	}
	if flag.Deprecated == "" || !flag.Hidden {
		t.Fatalf("--create-prefix must be deprecated and hidden, got deprecated=%q hidden=%v", flag.Deprecated, flag.Hidden)
	}
	if !strings.Contains(flag.Deprecated, "--provision-target-bucket") ||
		!strings.Contains(flag.Deprecated, "next minor release") {
		t.Fatalf("deprecation message %q must point at --provision-target-bucket and announce removal", flag.Deprecated)
	}
}

// The defaults bytes the CLI generates reach every topology unchanged, opt-out included.
func TestRunCmdProvisioningPolicyReachesEveryTopology(t *testing.T) {
	previousValidate := validateRunWorkloadTypeFunc
	previousPort := resolvePortConflictFunc
	previousConnect := connectMultiHostOrchestratorFunc
	previousLocal := startLocalHeadlessRunFunc
	previousMulti := startMultiHostHeadlessRunFunc
	previousAutoResults := startAutoResultsFunc
	t.Cleanup(func() {
		validateRunWorkloadTypeFunc = previousValidate
		resolvePortConflictFunc = previousPort
		connectMultiHostOrchestratorFunc = previousConnect
		startLocalHeadlessRunFunc = previousLocal
		startMultiHostHeadlessRunFunc = previousMulti
		startAutoResultsFunc = previousAutoResults
	})
	validateRunWorkloadTypeFunc = func(string) error { return nil }
	resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
		return &portcheck.ResolutionResult{Success: true}, nil
	}
	connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error { return nil }
	var defaults, scenarioJS []byte
	var route string
	startLocalHeadlessRunFunc = func(_ string, _ string, _ scenario.Params, options headless.HeadlessOptions) error {
		route, defaults, scenarioJS = "local", options.DefaultsContent, options.ScenarioContent
		options.LaunchHooks.NotifySubmitted()
		return nil
	}
	startMultiHostHeadlessRunFunc = func(
		_ *tui.MultiHostOrchestrator, _ string, _ string, _ scenario.Params, options headless.HeadlessOptions,
	) error {
		route, defaults, scenarioJS = "hosts", options.DefaultsContent, options.ScenarioContent
		options.LaunchHooks.NotifySubmitted()
		return nil
	}
	startAutoResultsFunc = func(
		_ context.Context, _ string, _ string, _ string, _ []string, _ int64, _ bool,
		_ []*hostparse.HostInfo, _ string, _ bool, _ int, _ string, metadata *runMetadata,
		_ io.Writer, _ io.Writer, _ string, _ func(context.Context), _ ...*integrity.FinalizeOptions,
	) *autoResultsMonitor {
		monitor := &autoResultsMonitor{done: make(chan autoResultsOutcome, 1), armed: make(chan struct{})}
		go func() {
			<-monitor.armed
			outcome := autoResultsOutcome{Tracker: &portcheck.RunResult{FinalState: constants.StateCompleted}}
			outcome.Lifecycle.PreparedInputs = cleanupPreparedForMonitorTest(metadata)
			monitor.done <- outcome
		}()
		return monitor
	}

	for _, topology := range []struct{ name, hosts, minHosts, route string }{
		{name: "local", hosts: "127.0.0.1", minHosts: "1", route: "local"},
		{name: "one-remote-host", hosts: "remote.example", minHosts: "1", route: "hosts"},
		{name: "distributed", hosts: "entry.example,worker.example", minHosts: "2", route: "hosts"},
	} {
		for _, provision := range []string{"true", "false"} {
			t.Run(topology.name+"/provision="+provision, func(t *testing.T) {
				t.Chdir(t.TempDir())
				for name, value := range map[string]string{
					"test-hosts": topology.hosts, "min-hosts": topology.minHosts,
					"endpoints": "http://s3.example", "access-key": "access", "secret-key": "secret",
					"bucket": "bench", "object-size": "1KiB", "object-count": "1", "duration": "",
					"threads": "1", "headless": "true", "auto-results": "true", "cleanup": "true",
					"shutdown-on-complete": "false", "generate-only": "false", "items-file": "",
					flagProvisionTargetBucket: provision,
				} {
					setGlobalRunFlagForTest(t, name, value)
				}
				route, defaults, scenarioJS = "", nil, nil

				if err := runCmd.RunE(runCmd, []string{WorkloadTypeWrite}); err != nil {
					var exitErr *ExitCodeError
					if errors.As(err, &exitErr) {
						t.Fatalf("RunE() = %v", err)
					}
				}
				if route != topology.route {
					t.Fatalf("route = %q, want %q", route, topology.route)
				}
				hasOptOut := strings.Contains(string(defaults), "targetBucket:\n        provision: false") ||
					strings.Contains(string(defaults), "provision: false")
				if (provision == "false") != hasOptOut {
					t.Fatalf("defaults for provision=%s:\n%s", provision, defaults)
				}
				if !strings.Contains(string(scenarioJS), "DeleteLoad\n    .config(sharedConfig)\n    .config(existingBucketsOnly)") {
					t.Fatalf("cleanup phase must use existing buckets only:\n%s", scenarioJS)
				}
			})
		}
	}
}
