package cmd

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/tui"
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
