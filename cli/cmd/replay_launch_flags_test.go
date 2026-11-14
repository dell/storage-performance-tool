/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/integrity"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
)

const replayLaunchFlagSecret = "archived-launch-secret"

// newReplayArchiveServerWithLaunchFlags serves an archive whose launch script passes engine flags
// replay does not keep, one of them holding a credential.
func newReplayArchiveServerWithLaunchFlags(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `<a href="run.sh">run</a><a href="scenario.json">scenario</a>`)
	})
	mux.HandleFunc("/run.sh", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `export BUCKET=archive-bucket
export KEY=%s
java -jar ${MONGOOSE_DIR}/mongoose.jar --storage-auth-secret=${KEY} --storage-targetBucket-provision=false --storage-targetBucket-count=4 --item-output-path=${BUCKET} --test-scenario-file=/tmp/scenario.json`,
			replayLaunchFlagSecret)
	})
	mux.HandleFunc("/scenario.json", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"type":"sequential","config":{"storage":{"driver":{"type":"s3"}}},"steps":[{"type":"load","config":{"item":{"data":{"size":"1KB"}},"test":{"step":{"id":"W","limit":{"count":1}}},"load":{"limit":{"concurrency":1}}}}]}`)
	})
	return httptest.NewServer(mux)
}

// The warning names the dropped launch-script flags, never their values, on one line of the
// preflight that replay prints before it launches, so it shows before the full-screen UI starts as
// well as in headless output and in generate-only mode.
func TestReplayCommandWarnsOfTheLaunchScriptFlagsItDoesNotKeep(t *testing.T) {
	server := newReplayArchiveServerWithLaunchFlags(t)
	defer server.Close()
	const want = "  - replay does not keep these launch-script flags: --storage-auth-secret, " +
		"--storage-targetBucket-provision, --storage-targetBucket-count\n"

	for _, mode := range []string{"generate-only", "headless", "TUI"} {
		t.Run(mode, func(t *testing.T) {
			t.Cleanup(installReplayEngineInfoSeams(t))
			resolvePortConflictFunc = func(context.Context, string, bool) (*portcheck.ResolutionResult, error) {
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			shouldReplayRunHeadless = func(*cobra.Command) bool { return mode == "headless" }
			var out bytes.Buffer
			var printedBeforeLaunch string
			confirmReplayLaunchCommand = func(io.Writer) error {
				printedBeforeLaunch = out.String()
				return nil
			}
			startReplayLocalHeadless = func(
				_ string, _ string, _ scenario.Params, options headless.HeadlessOptions, _ []byte, _ []byte,
			) error {
				printedBeforeLaunch = out.String()
				options.LaunchHooks.NotifySubmitted()
				return nil
			}
			startReplayLocalTUI = func(_ string, _ string, _ scenario.Params, options tui.RunOptions) error {
				options.LaunchHooks.NotifySubmitted()
				return nil
			}
			startReplayAutoResultsMonitor = func(
				context.Context, string, string, string, []string, int64,
				bool, []*hostparse.HostInfo, string, bool, int, string,
				*runMetadata, io.Writer, io.Writer, string, func(context.Context),
				...*integrity.FinalizeOptions,
			) *autoResultsMonitor {
				done := make(chan autoResultsOutcome, 1)
				done <- autoResultsOutcome{SummaryRead: true, StepsWithResults: 1}
				return &autoResultsMonitor{done: done, armed: make(chan struct{})}
			}

			cmd := newReplayCommandForTest(t)
			cmd.SetOut(&out)
			cmd.SetErr(io.Discard)
			args := []string{"--from", server.URL, "--endpoints", "http://s3.example", "--results-dir", t.TempDir()}
			if mode == "generate-only" {
				args = append(args, "--generate-only", "--output-dir", t.TempDir())
			}
			cmd.SetArgs(args)
			if err := cmd.Execute(); err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if mode == "generate-only" {
				printedBeforeLaunch = out.String()
			}
			if strings.Count(printedBeforeLaunch, want) != 1 {
				t.Fatalf("output before launch has %d copies of %q:\n%s", strings.Count(printedBeforeLaunch, want), want, printedBeforeLaunch)
			}
			if strings.Contains(out.String(), replayLaunchFlagSecret) || strings.Contains(out.String(), "provision=false") {
				t.Fatalf("output shows a flag value:\n%s", out.String())
			}
		})
	}
}
