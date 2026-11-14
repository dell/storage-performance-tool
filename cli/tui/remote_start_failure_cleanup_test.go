/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// startFailureContainerID is what the fake docker run prints; the start sites
// log its first 12 characters.
const startFailureContainerID = "c0ffee0123456789"

type startFailureCall struct {
	command string
	ctxErr  error
}

// startFailureExecutor plays a remote host for a whole engine start: docker
// run prints a container ID and every other command succeeds unless a hook
// for its prefix says otherwise. It records each command with the state of
// the context it ran on.
type startFailureExecutor struct {
	mu    sync.Mutex
	calls []startFailureCall
	// hooks maps a command prefix to its outcome; the first match wins.
	hooks map[string]func(ctx context.Context) error
	copy  func(ctx context.Context) error
}

func (e *startFailureExecutor) record(ctx context.Context, command string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, startFailureCall{command: command, ctxErr: ctx.Err()})
}

func (e *startFailureExecutor) ExecuteCommand(ctx context.Context, _ *hostparse.HostInfo, command []string) (string, string, error) {
	joined := strings.Join(command, " ")
	e.record(ctx, joined)
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	for prefix, hook := range e.hooks {
		if strings.HasPrefix(joined, prefix) {
			if err := hook(ctx); err != nil {
				return "", "failed", err
			}
			break
		}
	}
	if strings.HasPrefix(joined, "docker run") {
		return startFailureContainerID + "\n", "", nil
	}
	return "", "", nil
}

func (e *startFailureExecutor) CopyFile(ctx context.Context, _ *hostparse.HostInfo, localPath, remotePath string) error {
	e.record(ctx, "copy "+localPath+" "+remotePath)
	if e.copy != nil {
		return e.copy(ctx)
	}
	return ctx.Err()
}

func (e *startFailureExecutor) CopyFromHost(context.Context, *hostparse.HostInfo, string, string) error {
	return errors.New("a failed start never copies from the host")
}

// liveCommands lists the recorded commands with the given prefix that ran on
// a live context.
func (e *startFailureExecutor) liveCommands(prefix string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []string
	for _, call := range e.calls {
		if call.ctxErr == nil && strings.HasPrefix(call.command, prefix) {
			out = append(out, call.command)
		}
	}
	return out
}

func (e *startFailureExecutor) commands(prefix string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	var out []string
	for _, call := range e.calls {
		if strings.HasPrefix(call.command, prefix) {
			out = append(out, call.command)
		}
	}
	return out
}

// newStartFailureOrchestrator builds an orchestrator whose hosts run on the
// given fake executors, entry first, with a fixed advertised IP.
func newStartFailureOrchestrator(
	t *testing.T, execs []*startFailureExecutor,
) (*MultiHostTestOrchestrator, *MultiHostOrchestrator, []*RemoteDockerManager) {
	t.Helper()
	infos := make([]*hostparse.HostInfo, len(execs))
	for i := range execs {
		name := fmt.Sprintf("host%d.example.com", i)
		infos[i] = &hostparse.HostInfo{Host: name, Original: name}
	}
	orchestrator := NewMultiHostOrchestrator(infos, len(infos))
	orchestrator.SetNotifier(func(string) {})
	orchestrator.rollbackTimeout = 5 * time.Second
	orchestrator.detectAdvIP = func(context.Context, *hostparse.HostInfo) (string, error) {
		return "192.0.2.10", nil
	}
	managers := make([]*RemoteDockerManager, len(execs))
	for i, exec := range execs {
		mgr, err := newRemoteDockerManagerWithExecutor(infos[i], exec)
		if err != nil {
			t.Fatal(err)
		}
		managers[i] = mgr
		orchestrator.hosts[i].DockerManager = mgr
		orchestrator.hosts[i].SetStatus(HostStatusReady)
	}
	return NewMultiHostTestOrchestrator(orchestrator), orchestrator, managers
}

func failReadiness(context.Context, string, time.Duration) error {
	return errors.New("wait for /run readiness: context deadline exceeded")
}

// assertNothingLeft checks that the manager owns nothing and its host is no
// longer managed after the rollback or stop.
func assertNothingLeft(t *testing.T, host *HostConnection, mgr *RemoteDockerManager) {
	t.Helper()
	if mgr.HasManagedResources() {
		t.Fatalf("manager still owns container=%q staging=%q diagnostics=%q",
			mgr.containerID, mgr.stagingDir, mgr.diagnosticsDir)
	}
	if host.IsManaged() {
		t.Fatal("host is still managed after its resources were removed")
	}
}

// A worker that starts and then fails its API readiness check returns its
// container ID with the error. The distributed start rolls back, and the
// rollback removes that container on a live context.
func TestWorkerReadinessFailureRollbackRemovesContainer(t *testing.T) {
	entryExec, workerExec := &startFailureExecutor{}, &startFailureExecutor{}
	wrapper, orchestrator, managers := newStartFailureOrchestrator(
		t, []*startFailureExecutor{entryExec, workerExec})
	managers[1].proberRun = failReadiness

	params := scenario.ScenarioParams{WorkloadType: "mock"}
	err := wrapper.StartTestWithContentAndLaunchHooks(
		context.Background(), "test-image", params, []byte(`Load.run({})`), nil, LaunchHooks{})
	if err == nil || !strings.Contains(err.Error(), "worker API readiness") {
		t.Fatalf("start error = %v, want the worker's readiness failure", err)
	}
	if got := workerExec.liveCommands("docker rm -f " + startFailureContainerID); len(got) != 1 {
		t.Fatalf("worker container removals on a live context = %q, want one", got)
	}
	assertNothingLeft(t, orchestrator.hosts[1], managers[1])
	if got := entryExec.commands("docker run"); len(got) != 0 {
		t.Fatalf("entry started after too few workers: %q", got)
	}
}

// A worker that fails readiness in a run that goes on is managed, so
// StopAllContainers removes it at the end of the run.
func TestWorkerReadinessFailureStopAllRemovesContainer(t *testing.T) {
	workerExec := &startFailureExecutor{}
	_, orchestrator, managers := newStartFailureOrchestrator(
		t, []*startFailureExecutor{{}, workerExec})
	managers[1].proberRun = failReadiness
	worker := orchestrator.hosts[1]

	if err := orchestrator.startWorkerNode(context.Background(), worker, nil); err == nil {
		t.Fatal("worker start succeeded, want its readiness failure")
	}
	if !worker.IsManaged() {
		t.Fatal("a worker whose container runs is not managed after its readiness failure")
	}
	if err := orchestrator.StopAllContainers(context.Background()); err != nil {
		t.Fatalf("StopAllContainers() error = %v", err)
	}
	if got := workerExec.liveCommands("docker rm -f " + startFailureContainerID); len(got) != 1 {
		t.Fatalf("worker container removals = %q, want one", got)
	}
	assertNothingLeft(t, worker, managers[1])
}

// The entry node does the same as a worker: its container ID comes back with
// the readiness error, the host is managed, and the rollback removes it.
func TestEntryReadinessFailureRollbackRemovesContainer(t *testing.T) {
	entryExec := &startFailureExecutor{}
	_, orchestrator, managers := newStartFailureOrchestrator(t, []*startFailureExecutor{entryExec, {}})
	managers[0].proberRun = failReadiness
	entry := orchestrator.hosts[0]

	err := orchestrator.startEntryNodeContext(
		context.Background(), entry, orchestrator.hosts[1:], scenario.ScenarioParams{WorkloadType: "mock"}, nil)
	if err == nil || !strings.Contains(err.Error(), "entry API readiness") {
		t.Fatalf("entry start error = %v, want its readiness failure", err)
	}
	if !entry.IsManaged() {
		t.Fatal("an entry whose container runs is not managed after its readiness failure")
	}
	if err := orchestrator.cleanupManagedContainersAfterStartFailure(context.Background()); err != nil {
		t.Fatalf("rollback error = %v", err)
	}
	if got := entryExec.liveCommands("docker rm -f " + startFailureContainerID); len(got) != 1 {
		t.Fatalf("entry container removals on a live context = %q, want one", got)
	}
	assertNothingLeft(t, entry, managers[0])
}

// startLoneRemote runs the lone remote start (StartContainers on one host,
// rollback in the caller) and returns its error.
func startLoneRemote(ctx context.Context, wrapper *MultiHostTestOrchestrator) error {
	return wrapper.StartTestWithContentAndLaunchHooks(
		ctx, "test-image", scenario.ScenarioParams{WorkloadType: "mock"}, []byte(`Load.run({})`), nil, LaunchHooks{})
}

// With SPT_JAVA_OPTS set and no item files, a cancel during the env-file copy
// comes after the staging mkdir. The start's own cleanup runs on the ended
// context, so the rollback must remove the staging directory on a live one.
func TestDiagnosticsEnvFileCopyCancelRollbackRemovesStaging(t *testing.T) {
	t.Setenv(constants.EnvSptJavaOpts, "-Xlog:gc")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exec := &startFailureExecutor{copy: func(copyCtx context.Context) error {
		cancel()
		<-copyCtx.Done()
		return copyCtx.Err()
	}}
	wrapper, orchestrator, managers := newStartFailureOrchestrator(t, []*startFailureExecutor{exec})

	if err := startLoneRemote(ctx, wrapper); !errors.Is(err, context.Canceled) {
		t.Fatalf("start error = %v, want the copy's cancellation", err)
	}
	mkdirs := exec.liveCommands("mkdir -p /tmp/spt-items-")
	if len(mkdirs) != 1 {
		t.Fatalf("staging mkdirs = %q, want one", mkdirs)
	}
	stagingDir := strings.TrimPrefix(mkdirs[0], "mkdir -p ")
	if got := exec.liveCommands("rm -rf " + stagingDir); len(got) != 1 {
		t.Fatalf("staging removals on a live context = %q, want one", got)
	}
	if got := exec.commands("mkdir -p " + remoteDiagnosticsBaseDir); len(got) != 0 {
		t.Fatalf("diagnostics mkdir ran after the cancel: %q", got)
	}
	assertNothingLeft(t, orchestrator.hosts[0], managers[0])
}

// The diagnostics directory is owned before its mkdir runs. A mkdir cut off
// by a cancel, or a chmod that fails while the start's own removal also
// fails, leaves it owned, and the rollback removes it on a live context
// without trying to collect from it.
func TestDiagnosticsDirectoryFailureRollbackRemovesIt(t *testing.T) {
	for _, mode := range []string{"mkdir cut off", "chmod failed"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv(constants.EnvSptJavaOpts, "-Xlog:gc")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			exec := &startFailureExecutor{copy: func(context.Context) error { return nil }}
			ownRemovalFailed := false
			switch mode {
			case "mkdir cut off":
				exec.hooks = map[string]func(context.Context) error{
					"mkdir -p " + remoteDiagnosticsBaseDir: func(context.Context) error {
						cancel()
						return context.Canceled
					},
				}
			case "chmod failed":
				exec.hooks = map[string]func(context.Context) error{
					remoteChmodCommand + " 0777 " + remoteDiagnosticsBaseDir: func(context.Context) error {
						return errors.New("exit status 1")
					},
					"rm -rf " + remoteDiagnosticsBaseDir: func(context.Context) error {
						if !ownRemovalFailed {
							ownRemovalFailed = true
							return errors.New("exit status 1")
						}
						return nil
					},
				}
			}
			wrapper, orchestrator, managers := newStartFailureOrchestrator(t, []*startFailureExecutor{exec})

			if err := startLoneRemote(ctx, wrapper); err == nil {
				t.Fatal("start succeeded, want the diagnostics directory failure")
			}
			mkdirs := exec.commands("mkdir -p " + remoteDiagnosticsBaseDir)
			if len(mkdirs) != 1 {
				t.Fatalf("diagnostics mkdirs = %q, want one", mkdirs)
			}
			diagnosticsDir := strings.TrimPrefix(mkdirs[0], "mkdir -p ")
			removals := exec.liveCommands("rm -rf " + diagnosticsDir)
			wantRemovals := 1
			if mode == "chmod failed" {
				wantRemovals = 2 // the start's own failed removal, then the rollback's
			}
			if len(removals) != wantRemovals {
				t.Fatalf("diagnostics removals on a live context = %q, want %d", removals, wantRemovals)
			}
			if got := exec.commands("find "); len(got) != 0 {
				t.Fatalf("rollback collected from a directory no container used: %q", got)
			}
			if got := exec.commands("docker run"); len(got) != 0 {
				t.Fatalf("container started after the diagnostics failure: %q", got)
			}
			assertNothingLeft(t, orchestrator.hosts[0], managers[0])
		})
	}
}

// A mkdir of the diagnostics directory that fails while the context is live
// is definite: the path is not owned, so nothing removes it.
func TestDiagnosticsDefiniteMkdirFailureOwnsNothing(t *testing.T) {
	t.Setenv(constants.EnvSptJavaOpts, "-Xlog:gc")
	exec := &startFailureExecutor{
		copy: func(context.Context) error { return nil },
		hooks: map[string]func(context.Context) error{
			"mkdir -p " + remoteDiagnosticsBaseDir: func(context.Context) error { return errors.New("exit status 1") },
		},
	}
	wrapper, orchestrator, managers := newStartFailureOrchestrator(t, []*startFailureExecutor{exec})

	if err := startLoneRemote(context.Background(), wrapper); err == nil {
		t.Fatal("start succeeded, want the diagnostics mkdir failure")
	}
	if got := exec.commands("rm -rf " + remoteDiagnosticsBaseDir); len(got) != 0 {
		t.Fatalf("removed a diagnostics path the start never created: %q", got)
	}
	if got := exec.liveCommands("rm -rf /tmp/spt-items-"); len(got) != 1 {
		t.Fatalf("staging removals = %q, want the start's own one", got)
	}
	assertNothingLeft(t, orchestrator.hosts[0], managers[0])
}

// A start that fails before it creates anything leaves its host unmanaged,
// and the rollback runs nothing on it.
func TestFailedStartThatCreatedNothingLeavesHostUnmanaged(t *testing.T) {
	routes := []struct {
		name  string
		hosts int
		start func(*MultiHostTestOrchestrator) error
	}{
		{name: "lone remote start", hosts: 1, start: func(w *MultiHostTestOrchestrator) error {
			return startLoneRemote(context.Background(), w)
		}},
		{name: "distributed start", hosts: 2, start: func(w *MultiHostTestOrchestrator) error {
			return w.StartTestWithContentAndLaunchHooks(context.Background(), "test-image",
				scenario.ScenarioParams{WorkloadType: "mock"}, []byte(`Load.run({})`), nil, LaunchHooks{})
		}},
	}
	for _, route := range routes {
		t.Run(route.name, func(t *testing.T) {
			execs := make([]*startFailureExecutor, route.hosts)
			for i := range execs {
				execs[i] = &startFailureExecutor{hooks: map[string]func(context.Context) error{
					"docker run": func(context.Context) error { return errors.New("manifest unknown") },
				}}
			}
			wrapper, orchestrator, managers := newStartFailureOrchestrator(t, execs)
			if err := route.start(wrapper); err == nil {
				t.Fatal("start succeeded, want the docker run failure")
			}
			for i, exec := range execs {
				if got := exec.commands("docker rm"); len(got) != 0 {
					t.Fatalf("host %d: removed a container that never started: %q", i, got)
				}
				if got := exec.commands("rm "); len(got) != 0 {
					t.Fatalf("host %d: removed a path that was never created: %q", i, got)
				}
				assertNothingLeft(t, orchestrator.hosts[i], managers[i])
			}
		})
	}
}
