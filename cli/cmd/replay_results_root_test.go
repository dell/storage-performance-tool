package cmd

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/tui"
)

// TestReplayCommandGivesTheOrchestratorItsResultsRoot covers every route by which spt replay reaches
// the host orchestrator. Without the results root, the orchestrator's Docker managers get no
// diagnostics directory and a replay that fails at start leaves no Docker diagnostics behind.
func TestReplayCommandGivesTheOrchestratorItsResultsRoot(t *testing.T) {
	cases := []struct {
		name  string
		hosts string
		extra []string
	}{
		{name: "lone remote engine", hosts: "qa-client-01"},
		{name: "remote entry with distributed workers", hosts: "qa-entry-01,qa-worker-01"},
		{name: "attach mode", hosts: "qa-entry-01,qa-worker-01", extra: []string{"--attach-existing"}},
		{name: "remote engine without auto-results", hosts: "qa-client-01", extra: []string{"--auto-results=false"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newReplayArchiveServer(t)
			resultsDir := t.TempDir()

			connectErr := errors.New("stop after replay orchestrator wiring")
			var capturedRoot string
			var connectCalled bool
			origConnect := connectReplayOrchestrator
			t.Cleanup(func() { connectReplayOrchestrator = origConnect })
			connectReplayOrchestrator = func(_ context.Context, orchestrator *tui.MultiHostOrchestrator) error {
				connectCalled = true
				capturedRoot = orchestrator.ResultsRoot()
				return connectErr
			}

			cmd := newReplayCommandForTest(t)
			var out bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetArgs(append([]string{
				"--from", server.URL,
				"--endpoints", "http://10.0.0.1:9020",
				"--test-hosts", tc.hosts,
				"--min-hosts", "1",
				"--label", "rr",
				"--results-dir", resultsDir,
			}, tc.extra...))
			if err := cmd.Execute(); !errors.Is(err, connectErr) {
				t.Fatalf("Execute() error = %v, want wrapped connect sentinel\n%s", err, out.String())
			}
			if !connectCalled {
				t.Fatal("replay did not reach the orchestrator connect")
			}
			if capturedRoot == "" {
				t.Fatal("orchestrator results root is empty; Docker diagnostics would not be written")
			}
			if filepath.Dir(capturedRoot) != resultsDir || !strings.HasPrefix(filepath.Base(capturedRoot), "rr-") {
				t.Fatalf("orchestrator results root = %q, want rr-<token> under %q", capturedRoot, resultsDir)
			}
			// The generated replay artifacts land in the same directory, so the diagnostics sit
			// beside them.
			if _, err := os.Stat(filepath.Join(capturedRoot, "replay-scenario.js")); err != nil {
				entries, _ := os.ReadDir(capturedRoot)
				t.Fatalf("results root %q holds no generated scenario (%v); entries: %v", capturedRoot, err, entries)
			}
		})
	}
}
