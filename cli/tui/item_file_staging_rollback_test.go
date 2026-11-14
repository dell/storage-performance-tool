/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// itemFileStartRoute is one production caller chain that stages item files
// and owns the start rollback after a staging failure.
type itemFileStartRoute struct {
	name  string
	hosts int
	start func(ctx context.Context, wrapper *MultiHostTestOrchestrator, mounts []scenario.FileMount) error
}

var itemFileStartRoutes = []itemFileStartRoute{
	{
		// StartTestWithLaunchHooks -> StartDistributedTest, rollback in the caller.
		name: "distributed start from params", hosts: 2,
		start: func(ctx context.Context, wrapper *MultiHostTestOrchestrator, mounts []scenario.FileMount) error {
			params := scenario.ScenarioParams{WorkloadType: "mock", Threads: 1, ObjectSize: "1MB", ItemFileMounts: mounts}
			return wrapper.StartTestWithLaunchHooks(ctx, "test-image", params, LaunchHooks{})
		},
	},
	{
		// StartTestWithContentAndLaunchHooks -> StartDistributedTestWithContent.
		name: "distributed start with content", hosts: 2,
		start: func(ctx context.Context, wrapper *MultiHostTestOrchestrator, mounts []scenario.FileMount) error {
			params := scenario.ScenarioParams{WorkloadType: "mock", ItemFileMounts: mounts}
			return wrapper.StartTestWithContentAndLaunchHooks(
				ctx, "test-image", params, []byte(`Load.run({})`), nil, LaunchHooks{})
		},
	},
	{
		// StartTestWithContentAndLaunchHooks -> StartContainers on one remote host.
		name: "lone remote start", hosts: 1,
		start: func(ctx context.Context, wrapper *MultiHostTestOrchestrator, mounts []scenario.FileMount) error {
			params := scenario.ScenarioParams{WorkloadType: "mock", ItemFileMounts: mounts}
			return wrapper.StartTestWithContentAndLaunchHooks(
				ctx, "test-image", params, []byte(`Load.run({})`), nil, LaunchHooks{})
		},
	},
}

// newItemFileStartWrapper builds a start orchestrator whose hosts stage
// through the given fake executors, one per host, entry first.
func newItemFileStartWrapper(
	t *testing.T, execs []*stagingExecutor, rollbackTimeout time.Duration,
) (*MultiHostTestOrchestrator, *MultiHostOrchestrator, []*RemoteDockerManager) {
	t.Helper()
	infos := make([]*hostparse.HostInfo, len(execs))
	for i := range execs {
		name := fmt.Sprintf("host%d.example.com", i)
		infos[i] = &hostparse.HostInfo{Host: name, Original: name}
	}
	orchestrator := NewMultiHostOrchestrator(infos, len(infos))
	orchestrator.SetNotifier(func(string) {})
	if rollbackTimeout > 0 {
		orchestrator.rollbackTimeout = rollbackTimeout
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

// A start whose item-file staging fails rolls back once, from its caller:
// one rm -rf per staging directory, each cleanup error once, and an elapsed
// time within one rollback budget. The last host's copy is cancelled, so
// staging adds no removal of its own; every earlier host staged and owns a
// directory. Every rm fails, so a second rollback pass would repeat each one.
// The last host's rm hangs until the rollback budget ends, so a second pass
// would also double the time.
func TestItemFileStagingFailureRollsBackOnce(t *testing.T) {
	const budget = time.Second
	for _, route := range itemFileStartRoutes {
		t.Run(route.name, func(t *testing.T) {
			execs := make([]*stagingExecutor, route.hosts)
			for i := range execs {
				execs[i] = &stagingExecutor{rmErr: errors.New("exit status 1")}
			}
			failing := execs[len(execs)-1]
			failing.rmErr = nil
			failing.rm = func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			}
			failing.copyStarted = make(chan struct{}, 1)
			failing.copy = func(ctx context.Context) error {
				<-ctx.Done()
				return fmt.Errorf("copy stopped: %w", context.Cause(ctx))
			}
			wrapper, orchestrator, managers := newItemFileStartWrapper(t, execs, budget)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- route.start(ctx, wrapper, deleteStagingMounts[:1]) }()
			select {
			case <-failing.copyStarted:
			case <-time.After(5 * time.Second):
				t.Fatal("the last host's copy did not start")
			}
			dirs := make([]string, len(managers))
			for i, mgr := range managers {
				dirs[i] = mgr.stagingDir
				if dirs[i] == "" {
					t.Fatalf("host %d owns no staging directory before the cancel", i)
				}
			}
			cancelledAt := time.Now()
			cancel()
			var err error
			select {
			case err = <-done:
			case <-time.After(10 * budget):
				t.Fatal("the start did not return after the cancel")
			}
			elapsed := time.Since(cancelledAt)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("start error = %v, want the copy's cancellation", err)
			}
			for i, exec := range execs {
				if rms := exec.commands("rm "); !reflect.DeepEqual(rms, []string{"rm -rf " + dirs[i]}) {
					t.Fatalf("host %d removals = %q, want one rollback rm of %s", i, rms, dirs[i])
				}
				for _, call := range exec.recorded() {
					if strings.HasPrefix(call.command, "rm ") && call.ctxErr != nil {
						t.Fatalf("host %d rm started on an ended context: %v", i, call.ctxErr)
					}
				}
				name := orchestrator.hosts[i].Info.Original
				owned := "cleanup " + name + " returned with managed resources still owned"
				if n := strings.Count(err.Error(), owned); n != 1 {
					t.Fatalf("%q appears %d times in %v; want once", owned, n, err)
				}
				removal := "remove remote staging directory " + dirs[i]
				if n := strings.Count(err.Error(), removal); n != 1 {
					t.Fatalf("%q appears %d times in %v; want once", removal, n, err)
				}
				if !orchestrator.hosts[i].IsManaged() || !managers[i].HasManagedResources() {
					t.Fatalf("host %d lost ownership of %s after a failed removal", i, dirs[i])
				}
			}
			// A second pass cannot end before 2x the budget: the failing host's
			// rm blocks for the whole budget in each pass.
			if elapsed >= budget*2 {
				t.Fatalf("the start returned %v after the cancel; want one %v rollback pass", elapsed, budget)
			}
			t.Logf("the start returned %v after the cancel", elapsed.Round(time.Millisecond))
		})
	}
}

// A mkdir cut off by its own deadline or a cancel may have run on the host,
// so the staging path stays owned and cleanup removes it; a mkdir that fails
// with its context live forgets the path and cleanup runs no rm.
func TestRemoteStagingMkdirFailureOwnership(t *testing.T) {
	cutOff := func(ctx context.Context) error {
		<-ctx.Done()
		return fmt.Errorf("ssh mkdir: %w", ctx.Err())
	}
	t.Run("deadline", func(t *testing.T) {
		exec := &stagingExecutor{mkdir: cutOff}
		mgr := newStagingManager(t, exec)
		mgr.stagingCommandTimeout = 20 * time.Millisecond
		err := mgr.SetFileMountsContext(context.Background(), deleteStagingMounts[:1])
		if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "create remote staging directory") {
			t.Fatalf("staging error = %v, want the cut-off mkdir", err)
		}
		assertCutOffMkdirKeptAndRemoved(t, exec, mgr)
	})
	t.Run("cancel", func(t *testing.T) {
		started := make(chan struct{})
		exec := &stagingExecutor{mkdir: func(ctx context.Context) error {
			close(started)
			return cutOff(ctx)
		}}
		mgr := newStagingManager(t, exec)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- mgr.SetFileMountsContext(ctx, deleteStagingMounts[:1]) }()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("the mkdir did not start")
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("staging error = %v, want the cancelled mkdir", err)
		}
		assertCutOffMkdirKeptAndRemoved(t, exec, mgr)
	})
	t.Run("definite failure", func(t *testing.T) {
		mkdirErr := errors.New("exit status 1")
		exec := &stagingExecutor{mkdir: func(context.Context) error { return mkdirErr }}
		mgr := newStagingManager(t, exec)
		if err := mgr.SetFileMountsContext(context.Background(), deleteStagingMounts[:1]); !errors.Is(err, mkdirErr) {
			t.Fatalf("staging error = %v, want the mkdir failure", err)
		}
		if mgr.HasManagedResources() || mgr.stagingDir != "" {
			t.Fatalf("a definite mkdir failure kept %q owned", mgr.stagingDir)
		}
		if err := mgr.CleanupContext(context.Background()); err != nil {
			t.Fatalf("cleanup = %v", err)
		}
		if rms := exec.commands("rm "); len(rms) != 0 {
			t.Fatalf("removals = %q, want none after a definite mkdir failure", rms)
		}
	})
}

func assertCutOffMkdirKeptAndRemoved(t *testing.T, exec *stagingExecutor, mgr *RemoteDockerManager) {
	t.Helper()
	dir := cutOffMkdirDir(t, exec)
	if !mgr.HasManagedResources() || mgr.stagingDir != dir {
		t.Fatalf("after a cut-off mkdir: owned=%t dir=%q; want %s kept owned", mgr.HasManagedResources(), mgr.stagingDir, dir)
	}
	if copies := exec.commands("copy "); len(copies) != 0 {
		t.Fatalf("staging copied %q after a failed mkdir", copies)
	}
	if err := mgr.CleanupContext(context.Background()); err != nil || mgr.HasManagedResources() {
		t.Fatalf("cleanup = %v, owned = %t; want %s removed", err, mgr.HasManagedResources(), dir)
	}
	if rms := exec.commands("rm "); !reflect.DeepEqual(rms, []string{"rm -rf " + dir}) {
		t.Fatalf("removals = %q, want one rm of %s", rms, dir)
	}
}

// cutOffMkdirDir returns the directory of the one mkdir the fake ran.
func cutOffMkdirDir(t *testing.T, exec *stagingExecutor) string {
	t.Helper()
	mkdirs := exec.commands("mkdir ")
	if len(mkdirs) != 1 {
		t.Fatalf("mkdirs = %q, want one", mkdirs)
	}
	dir, ok := strings.CutPrefix(mkdirs[0], "mkdir -p ")
	if !ok || dir == "" {
		t.Fatalf("mkdir = %q, want mkdir -p <dir>", mkdirs[0])
	}
	return dir
}

// Through each start route, a mkdir cut off by its deadline is followed by
// the caller's start rollback, which issues exactly one rm -rf of that path.
// The host then ends unmanaged with nothing owned.
func TestItemFileStagingCutOffMkdirRemovedByStartRollback(t *testing.T) {
	for _, route := range itemFileStartRoutes {
		t.Run(route.name, func(t *testing.T) {
			execs := make([]*stagingExecutor, route.hosts)
			for i := range execs {
				execs[i] = &stagingExecutor{}
			}
			execs[0].mkdir = func(ctx context.Context) error {
				<-ctx.Done()
				return fmt.Errorf("ssh mkdir: %w", ctx.Err())
			}
			wrapper, orchestrator, managers := newItemFileStartWrapper(t, execs, 0)
			managers[0].stagingCommandTimeout = 20 * time.Millisecond

			err := route.start(context.Background(), wrapper, deleteStagingMounts[:1])
			if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "create remote staging directory") {
				t.Fatalf("start error = %v, want the cut-off mkdir", err)
			}
			dir := cutOffMkdirDir(t, execs[0])
			if rms := execs[0].commands("rm "); !reflect.DeepEqual(rms, []string{"rm -rf " + dir}) {
				t.Fatalf("removals = %q, want one rollback rm of %s", rms, dir)
			}
			if orchestrator.hosts[0].IsManaged() || managers[0].HasManagedResources() {
				t.Fatalf("after the rollback: managed=%t owned=%t; want neither",
					orchestrator.hosts[0].IsManaged(), managers[0].HasManagedResources())
			}
			for i := 1; i < len(execs); i++ {
				if cmds := execs[i].recorded(); len(cmds) != 0 {
					t.Fatalf("host %d ran %v after the entry's staging failed", i, cmds)
				}
			}
		})
	}
}
