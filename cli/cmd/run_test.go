package cmd

import (
	"context"
	"errors"
	"io"
	"path/filepath"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
)

func TestDeriveBaseURLSingleHost(t *testing.T) {
	got := deriveBaseURL("8080", nil)
	want := "http://localhost:8080"
	if got != want {
		t.Fatalf("deriveBaseURL() = %q, want %q", got, want)
	}
}

func TestDeriveBaseURLMultiHost(t *testing.T) {
	hosts := []*hostparse.HostInfo{
		{Host: "entry.example", Original: "entry.example"},
		{Host: "worker", Original: "worker"},
	}
	got := deriveBaseURL("9000", hosts)
	want := "http://entry.example:9000"
	if got != want {
		t.Fatalf("deriveBaseURL() = %q, want %q", got, want)
	}
}

func TestDeriveBaseURLSingleRemoteHost(t *testing.T) {
	hosts := []*hostparse.HostInfo{
		{Host: "worker.example", IsLocal: false, Original: "root@worker.example"},
	}
	got := deriveBaseURL("9000", hosts)
	want := "http://worker.example:9000"
	if got != want {
		t.Fatalf("deriveBaseURL() = %q, want %q", got, want)
	}
}

func TestShouldUseMultiHostOrchestrator(t *testing.T) {
	tests := []struct {
		name  string
		hosts []*hostparse.HostInfo
		want  bool
	}{
		{
			name: "no hosts",
		},
		{
			name: "single local host",
			hosts: []*hostparse.HostInfo{
				{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
			},
		},
		{
			name: "single remote host",
			hosts: []*hostparse.HostInfo{
				{Host: "worker.example", IsLocal: false, Original: "root@worker.example"},
			},
			want: true,
		},
		{
			name: "multiple hosts",
			hosts: []*hostparse.HostInfo{
				{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
				{Host: "worker.example", IsLocal: false, Original: "root@worker.example"},
			},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldUseMultiHostOrchestrator(tt.hosts); got != tt.want {
				t.Fatalf("shouldUseMultiHostOrchestrator() = %t, want %t", got, tt.want)
			}
		})
	}
}

func TestRunCmdSingleRemoteHostUsesOrchestratorAndSkipsControllerPortCheck(t *testing.T) {
	t.Chdir(t.TempDir())

	setRunFlag := func(name, value string) {
		t.Helper()
		flag := runCmd.Flags().Lookup(name)
		if flag == nil {
			t.Fatalf("run flag %q not found", name)
		}
		previousValue := flag.Value.String()
		previousChanged := flag.Changed
		if err := flag.Value.Set(value); err != nil {
			t.Fatalf("set %s=%q: %v", name, value, err)
		}
		flag.Changed = true
		t.Cleanup(func() {
			if err := flag.Value.Set(previousValue); err != nil {
				t.Errorf("restore %s=%q: %v", name, previousValue, err)
			}
			flag.Changed = previousChanged
		})
	}
	setRunFlag("test-hosts", "root@worker.example")
	setRunFlag("min-hosts", "1")
	setRunFlag("duration", "1s")
	setRunFlag("object-size", "1K")
	setRunFlag("threads", "1")
	setRunFlag("headless", "true")
	setRunFlag("auto-results", "false")

	previousPortResolver := resolvePortConflictFunc
	controllerPortChecks := 0
	resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
		controllerPortChecks++
		return nil, errors.New("controller port preflight must not run for a remote host")
	}
	t.Cleanup(func() { resolvePortConflictFunc = previousPortResolver })

	previousConnect := connectMultiHostOrchestratorFunc
	remoteOrchestratorCalls := 0
	errRemoteOrchestratorReached := errors.New("remote orchestrator reached")
	connectMultiHostOrchestratorFunc = func(_ context.Context, orchestrator *tui.MultiHostOrchestrator) error {
		if orchestrator == nil {
			t.Fatal("remote path did not construct an orchestrator")
		}
		remoteOrchestratorCalls++
		return errRemoteOrchestratorReached
	}
	t.Cleanup(func() { connectMultiHostOrchestratorFunc = previousConnect })

	err := runCmd.RunE(runCmd, []string{WorkloadTypeMock})
	if !errors.Is(err, errRemoteOrchestratorReached) {
		t.Fatalf("RunE() error = %v, want remote orchestrator sentinel", err)
	}
	if controllerPortChecks != 0 {
		t.Fatalf("controller port preflight calls = %d, want 0", controllerPortChecks)
	}
	if remoteOrchestratorCalls != 1 {
		t.Fatalf("remote orchestrator calls = %d, want 1", remoteOrchestratorCalls)
	}
	if scenarioFiles, globErr := filepath.Glob("spt-scenario-*.js"); globErr != nil || len(scenarioFiles) != 0 {
		t.Fatalf("scenario cleanup files = %v, glob error = %v", scenarioFiles, globErr)
	}
}

func TestRunCmdWorkloadRoutesEveryHostTopology(t *testing.T) {
	type routeContextKey struct{}
	tests := []struct {
		name        string
		workload    string
		hosts       string
		minHosts    string
		shutdown    string
		wantLocal   int
		wantMulti   int
		wantConnect int
		wantPrepare int
		wantPort    int
	}{
		{name: "local verification", workload: WorkloadTypeWriteVerify, hosts: "127.0.0.1", minHosts: "1", shutdown: "false", wantLocal: 1, wantPort: 1},
		{name: "one remote verification", workload: WorkloadTypeWriteVerify, hosts: "entry.example", minHosts: "1", shutdown: "false", wantMulti: 1, wantConnect: 1, wantPrepare: 1},
		{name: "distributed verification", workload: WorkloadTypeWriteVerify, hosts: "entry.example,worker.example", minHosts: "2", shutdown: "false", wantMulti: 1, wantConnect: 1, wantPrepare: 1},
		{name: "ordinary remote delegated shutdown", workload: WorkloadTypeWrite, hosts: "entry.example", minHosts: "1", shutdown: "true", wantMulti: 1, wantConnect: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			for name, value := range map[string]string{
				"test-hosts": test.hosts, "min-hosts": test.minHosts,
				"endpoints": "http://s3.example", "access-key": "access", "secret-key": "secret",
				"bucket": "qualification", "object-size": "1KiB", "object-count": "1",
				"duration": "", "threads": "1", "headless": "true", "auto-results": "true",
				"shutdown-on-complete": test.shutdown, "generate-only": "false", "items-file": "",
			} {
				setGlobalRunFlagForTest(t, name, value)
			}
			runContext := context.WithValue(context.Background(), routeContextKey{}, test.name)
			previousContext := runCmd.Context()
			runCmd.SetContext(runContext)
			t.Cleanup(func() { runCmd.SetContext(previousContext) })

			previousPort := resolvePortConflictFunc
			previousConnect := connectMultiHostOrchestratorFunc
			previousPrepare := prepareDistributedIntegrityRuntimeIdentityFunc
			previousLocal := startLocalHeadlessRunFunc
			previousMulti := startMultiHostHeadlessRunFunc
			previousAutoResults := startAutoResultsFunc
			t.Cleanup(func() {
				resolvePortConflictFunc = previousPort
				connectMultiHostOrchestratorFunc = previousConnect
				prepareDistributedIntegrityRuntimeIdentityFunc = previousPrepare
				startLocalHeadlessRunFunc = previousLocal
				startMultiHostHeadlessRunFunc = previousMulti
				startAutoResultsFunc = previousAutoResults
			})

			var portCalls, connectCalls, prepareCalls, localCalls, multiCalls, autoResultsCalls int
			var autoResultsContext context.Context
			resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
				portCalls++
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			connectMultiHostOrchestratorFunc = func(context.Context, *tui.MultiHostOrchestrator) error {
				connectCalls++
				return nil
			}
			prepareDistributedIntegrityRuntimeIdentityFunc = func(
				context.Context, *tui.MultiHostOrchestrator, string,
			) (tui.DistributedRuntimeIdentityEvidence, error) {
				prepareCalls++
				return tui.DistributedRuntimeIdentityEvidence{ImageID: "sha256:test"}, nil
			}
			startLocalHeadlessRunFunc = func(_ string, _ string, params scenario.Params, _ headless.HeadlessOptions) error {
				localCalls++
				params.NotifyLaunchSubmitted()
				return nil
			}
			startMultiHostHeadlessRunFunc = func(
				_ *tui.MultiHostOrchestrator, _ string, _ string, params scenario.Params, _ headless.HeadlessOptions,
			) error {
				multiCalls++
				params.NotifyLaunchSubmitted()
				return nil
			}
			startAutoResultsFunc = func(
				ctx context.Context, _ string, _ string, _ string, _ []string, _ int64, _ bool, _ []*hostparse.HostInfo,
				_ string, _ bool, _ int, _ string, _ *runMetadata, _ io.Writer, _ io.Writer, _ string,
				_ func(context.Context), _ ...*integrity.FinalizeOptions,
			) *autoResultsMonitor {
				autoResultsCalls++
				autoResultsContext = ctx
				outcomes := make(chan autoResultsOutcome, 1)
				outcomes <- autoResultsOutcome{
					Tracker:      &portcheck.RunResult{FinalState: constants.StateCompleted},
					Finalization: &integrity.FinalizeOutcome{Complete: true},
				}
				return &autoResultsMonitor{done: outcomes, armed: make(chan struct{})}
			}

			if err := runCmd.RunE(runCmd, []string{test.workload}); err != nil {
				t.Fatalf("RunE() error = %v", err)
			}
			if localCalls != test.wantLocal || multiCalls != test.wantMulti ||
				connectCalls != test.wantConnect || prepareCalls != test.wantPrepare ||
				portCalls != test.wantPort || autoResultsCalls != 1 {
				t.Fatalf(
					"route calls local=%d multi=%d connect=%d prepare=%d port=%d auto-results=%d",
					localCalls, multiCalls, connectCalls, prepareCalls, portCalls, autoResultsCalls,
				)
			}
			if autoResultsContext == nil || autoResultsContext.Value(routeContextKey{}) != test.name {
				t.Fatalf("auto-results context = %#v, want command context marker %q", autoResultsContext, test.name)
			}
			if scenarioFiles, globErr := filepath.Glob("spt-scenario-*.js"); globErr != nil || len(scenarioFiles) != 0 {
				t.Fatalf("scenario cleanup files = %v, glob error = %v", scenarioFiles, globErr)
			}
		})
	}
}

func setGlobalRunFlagForTest(t *testing.T, name, value string) {
	t.Helper()
	flag := runCmd.Flags().Lookup(name)
	if flag == nil {
		t.Fatalf("run flag %q not found", name)
	}
	previousValue := flag.Value.String()
	previousChanged := flag.Changed
	if err := flag.Value.Set(value); err != nil {
		t.Fatalf("set %s=%q: %v", name, value, err)
	}
	flag.Changed = true
	t.Cleanup(func() {
		if err := flag.Value.Set(previousValue); err != nil {
			t.Errorf("restore %s=%q: %v", name, previousValue, err)
		}
		flag.Changed = previousChanged
	})
}
