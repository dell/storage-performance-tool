/*
Copyright © 2026 Dell Technologies
*/

package headless

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
)

const (
	autoTerminateTestReachedLine = "Auto-terminate deadline (1 s) reached; stopping the run"
	autoTerminateTestStoppedLine = "Auto-terminate stop request finished; RunSession confirms the stop and owns evidence and resource finalization"
	autoTerminateTestStartError  = "auto-terminate deadline (1 s) reached during start-up, before the workload was submitted"
)

// autoTerminateRoute runs one headless route with a one-second deadline. start
// plays the orchestrator's start; it receives the launch context.
type autoTerminateRoute struct {
	name string
	run  func(t *testing.T, ctx context.Context, hooks tui.LaunchHooks, tracePath string,
		start func(context.Context) error) error
}

var autoTerminateRoutes = []autoTerminateRoute{
	{
		name: "local",
		run: func(t *testing.T, ctx context.Context, hooks tui.LaunchHooks, tracePath string,
			start func(context.Context) error) error {
			t.Helper()
			runner, err := NewHeadlessRunner(tui.NewMockDockerManager(), HeadlessOptions{
				LaunchHooks: hooks, AutoTerminateSeconds: 1, TraceFile: tracePath, ShutdownLingerSec: 1,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = runner.Close() }()
			return runner.runBenchmark(ctx, "", func(launchCtx context.Context, _ *tui.TestOrchestrator) error {
				return start(launchCtx)
			})
		},
	},
	{
		name: "multi-host",
		run: func(t *testing.T, ctx context.Context, hooks tui.LaunchHooks, tracePath string,
			start func(context.Context) error) error {
			t.Helper()
			original := startMultiHostTest
			t.Cleanup(func() { startMultiHostTest = original })
			startMultiHostTest = func(
				launchCtx context.Context, _ *tui.MultiHostTestOrchestrator, _ string,
				_ scenario.Params, _, _ []byte, _ tui.LaunchHooks,
			) error {
				return start(launchCtx)
			}
			orchestrator := tui.NewMultiHostOrchestrator([]*hostparse.HostInfo{
				{Host: "entry.example", Original: "entry.example"},
				{Host: "worker.example", Original: "worker.example"},
			}, 2)
			runner, err := NewMultiHostHeadlessRunner(orchestrator, HeadlessOptions{
				LaunchHooks: hooks, AutoTerminateSeconds: 1, TraceFile: tracePath, ShutdownLingerSec: 1,
				DelegateNormalShutdown: true,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = runner.Close() }()
			return runner.RunWithParams(ctx, "test-image", "", scenario.Params{WorkloadType: "mock"})
		},
	},
}

func readTrace(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path) // #nosec G304 -- test temp directory
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}

// At the deadline a session-managed run is stopped by its session, and the runner keeps its launch
// context, log reading and trace file open until the session reports the run terminal. Before the
// fix the runner returned the bare deadline at once and the step ran to its natural end.
func TestSessionManagedAutoTerminateStopsRunBeforeReturning(t *testing.T) {
	for _, route := range autoTerminateRoutes {
		t.Run(route.name, func(t *testing.T) {
			session := runcontrol.NewSession()
			hooks := tui.NewSessionLaunchHooks(session, nil)
			tracePath := filepath.Join(t.TempDir(), "trace.log")
			launched := make(chan context.Context, 1)
			done := make(chan error, 1)
			go func() {
				done <- route.run(t, context.Background(), hooks, tracePath, func(launchCtx context.Context) error {
					hooks.NotifySubmitted()
					launched <- launchCtx
					return nil
				})
			}()
			launchCtx := <-launched

			select {
			case <-session.WorkloadStopRequested():
			case err := <-done:
				t.Fatalf("runner returned %v at the deadline without asking the session to stop the run", err)
			case <-time.After(5 * time.Second):
				t.Fatal("the deadline did not ask the session to stop the run")
			}
			// The run is being stopped: presentation stays open and the runner keeps waiting.
			select {
			case err := <-done:
				t.Fatalf("runner returned %v before the session reported the run stopped", err)
			case <-time.After(200 * time.Millisecond):
			}
			if err := launchCtx.Err(); err != nil {
				t.Fatalf("launch context ended at the deadline (%v); log reading would stop before the run", err)
			}

			session.MarkWorkloadTerminal()
			var err error
			select {
			case err = <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("runner did not return after the session reported the run stopped")
			}
			autoTerminated, cleanupComplete := AutoTerminateState(err)
			if !autoTerminated || !cleanupComplete {
				t.Fatalf("runner error = %v, want AutoTerminateError with cleanup left to the session", err)
			}
			trace := readTrace(t, tracePath)
			for _, want := range []string{autoTerminateTestReachedLine, autoTerminateTestStoppedLine} {
				if !strings.Contains(trace, want) {
					t.Errorf("trace lacks %q:\n%s", want, trace)
				}
			}
		})
	}
}

// Ctrl-C while the session stops the run keeps its interrupt result.
func TestSessionManagedAutoTerminateInterruptedDuringStopReturnsCancellation(t *testing.T) {
	for _, route := range autoTerminateRoutes {
		t.Run(route.name, func(t *testing.T) {
			session := runcontrol.NewSession()
			hooks := tui.NewSessionLaunchHooks(session, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				done <- route.run(t, ctx, hooks, filepath.Join(t.TempDir(), "trace.log"), func(context.Context) error {
					hooks.NotifySubmitted()
					return nil
				})
			}()
			select {
			case <-session.WorkloadStopRequested():
			case <-time.After(5 * time.Second):
				t.Fatal("the deadline did not ask the session to stop the run")
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("runner error = %v, want the cancellation", err)
				}
				if autoTerminated, _ := AutoTerminateState(err); autoTerminated {
					t.Fatalf("an interrupted stop was reported as auto-terminate: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("runner did not return after the interrupt")
			}
		})
	}
}

// A deadline that fires before the workload is submitted ends start-up with an error that names
// auto-terminate, instead of the start step's own timeout text.
func TestAutoTerminateBeforeSubmissionNamesAutoTerminate(t *testing.T) {
	for _, route := range autoTerminateRoutes {
		t.Run(route.name, func(t *testing.T) {
			session := runcontrol.NewSession()
			hooks := tui.NewSessionLaunchHooks(session, nil)
			err := route.run(t, context.Background(), hooks, filepath.Join(t.TempDir(), "trace.log"),
				func(launchCtx context.Context) error {
					<-launchCtx.Done()
					return fmt.Errorf("spt API did not become ready after 1m0s: %w", launchCtx.Err())
				})
			if err == nil || !strings.Contains(err.Error(), autoTerminateTestStartError) {
				t.Fatalf("start error = %v, want it to contain %q", err, autoTerminateTestStartError)
			}
			if autoTerminated, _ := AutoTerminateState(err); autoTerminated {
				t.Fatalf("a start that never submitted was reported as a stopped run: %v", err)
			}
		})
	}
}

// A session that never reports the run terminal after the stop request fails the run with an
// error that names auto-terminate: the stop was not confirmed. Before, the runner warned and
// returned the auto-terminate result, which exits 0.
func TestSessionManagedAutoTerminateUnconfirmedStopFailsTheRun(t *testing.T) {
	original := autoTerminateStopBudget
	autoTerminateStopBudget = 50 * time.Millisecond
	t.Cleanup(func() { autoTerminateStopBudget = original })
	for _, route := range autoTerminateRoutes {
		t.Run(route.name, func(t *testing.T) {
			session := runcontrol.NewSession()
			hooks := tui.NewSessionLaunchHooks(session, nil)
			err := route.run(t, context.Background(), hooks, filepath.Join(t.TempDir(), "trace.log"),
				func(context.Context) error {
					hooks.NotifySubmitted()
					return nil
				})
			const want = "auto-terminate: the stop at the 1 s deadline was not confirmed"
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("runner error = %v, want it to contain %q", err, want)
			}
			if autoTerminated, _ := AutoTerminateState(err); autoTerminated {
				t.Fatalf("an unconfirmed stop was reported as a stopped run: %v", err)
			}
		})
	}
}

// The runner waits for the monitor's stop and its shutdown fallback, each with its own budget,
// plus the shutdown linger. A stop the fallback confirms after one and a half budgets, the old
// wait, still ends as a stopped run. The budgets are scaled down from 30 s to 100 ms; the linger
// is 1 s.
func TestSessionManagedAutoTerminateWaitsForTheShutdownFallback(t *testing.T) {
	original := autoTerminateStopBudget
	autoTerminateStopBudget = 100 * time.Millisecond
	t.Cleanup(func() { autoTerminateStopBudget = original })
	for _, route := range autoTerminateRoutes {
		t.Run(route.name, func(t *testing.T) {
			session := runcontrol.NewSession()
			hooks := tui.NewSessionLaunchHooks(session, nil)
			go func() {
				<-session.WorkloadStopRequested()
				// The stop's budget ran out; the shutdown fallback confirms the stop.
				time.Sleep(4 * autoTerminateStopBudget)
				session.MarkWorkloadTerminal()
			}()
			err := route.run(t, context.Background(), hooks, filepath.Join(t.TempDir(), "trace.log"),
				func(context.Context) error {
					hooks.NotifySubmitted()
					return nil
				})
			if autoTerminated, cleanupComplete := AutoTerminateState(err); !autoTerminated || !cleanupComplete {
				t.Fatalf("runner error = %v, want the auto-terminate result of a confirmed stop", err)
			}
		})
	}
}
