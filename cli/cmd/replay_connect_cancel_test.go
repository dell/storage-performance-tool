/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
)

// errReplayConnectNotCancelled is what the stubbed connect returns when the replay's cancel never
// reaches it.
var errReplayConnectNotCancelled = errors.New("replay connect outlived the cancelled replay context")

// Cancelling the replay's command context, as Ctrl-C or SIGTERM does, ends its host connection
// phase, and the replay then returns the cancellation without prompting, starting an engine or
// arming result collection. Like spt run, it does no host cleanup after a failed connect: the
// connection phase only reads host state, pulls an image or stops conflicting containers.
func TestReplayCommandCancelEndsTheHostConnection(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()

	for _, route := range []struct{ name, hosts string }{
		{name: "lone remote", hosts: "entry.example"},
		{name: "distributed", hosts: "entry.example,worker.example"},
	} {
		t.Run(route.name, func(t *testing.T) {
			t.Cleanup(installReplayEngineInfoSeams(t))
			resolvePortConflictFunc = func(context.Context, string, portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			connecting := make(chan struct{})
			var connectErr error
			connectReplayOrchestrator = func(ctx context.Context, _ *tui.MultiHostOrchestrator) error {
				close(connecting)
				select {
				case <-ctx.Done():
					connectErr = ctx.Err()
				case <-time.After(5 * time.Second):
					connectErr = errReplayConnectNotCancelled
				}
				return connectErr
			}
			var afterConnect []string
			confirmReplayLaunchCommand = func(io.Writer) error {
				afterConnect = append(afterConnect, "confirm")
				return nil
			}
			shouldReplayRunHeadless = func(*cobra.Command) bool { return true }
			startReplayLocalHeadless = func(
				string, string, scenario.Params, headless.HeadlessOptions, []byte, []byte,
			) error {
				afterConnect = append(afterConnect, "local headless")
				return nil
			}
			startReplayLocalTUI = func(string, string, scenario.Params, tui.RunOptions) error {
				afterConnect = append(afterConnect, "local TUI")
				return nil
			}
			startReplayRemoteHeadless = func(
				*tui.MultiHostOrchestrator, string, string, scenario.Params, headless.HeadlessOptions, []byte, []byte,
			) error {
				afterConnect = append(afterConnect, "remote headless")
				return nil
			}
			startReplayRemoteTUI = func(*tui.MultiHostOrchestrator, string, string, scenario.Params, tui.RunOptions) error {
				afterConnect = append(afterConnect, "remote TUI")
				return nil
			}
			startReplayAutoResultsMonitor = func(
				context.Context, string, string, string, []string, int64,
				bool, []*hostparse.HostInfo, string, bool, int, string,
				*runMetadata, io.Writer, io.Writer, string, func(context.Context),
				...*integrity.FinalizeOptions,
			) *autoResultsMonitor {
				afterConnect = append(afterConnect, "auto-results monitor")
				done := make(chan autoResultsOutcome, 1)
				done <- autoResultsOutcome{SummaryRead: true, StepsWithResults: 1}
				return &autoResultsMonitor{done: done, armed: make(chan struct{})}
			}

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go func() {
				select {
				case <-connecting:
					cancel()
				case <-ctx.Done():
				}
			}()

			cmd := newReplayCommandForTest(t)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{
				"--from", server.URL,
				"--endpoints", "http://s3.example",
				"--test-hosts", route.hosts,
				"--results-dir", t.TempDir(),
			})
			err := cmd.ExecuteContext(ctx)

			if !errors.Is(connectErr, context.Canceled) {
				t.Fatalf("connect ended with %v, want the replay's cancellation", connectErr)
			}
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("Execute() error = %v, want the replay's cancellation", err)
			}
			if len(afterConnect) != 0 {
				t.Fatalf("after the cancelled connect the replay ran %v, want nothing", afterConnect)
			}
		})
	}
}
