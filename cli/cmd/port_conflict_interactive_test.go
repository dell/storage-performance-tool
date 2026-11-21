/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/spf13/cobra"
)

var errTestPortConflictStop = errors.New("stop after the port check")

// recordPortConflictOptions makes the resolver record the options it gets and
// end the command there.
func recordPortConflictOptions(t *testing.T, stdinIsTerminal bool) *[]portcheck.ResolveOptions {
	t.Helper()
	var got []portcheck.ResolveOptions
	prevResolver := resolvePortConflictFunc
	prevStdin := portConflictStdinIsTerminal
	resolvePortConflictFunc = func(_ context.Context, _ string, opts portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
		got = append(got, opts)
		return nil, errTestPortConflictStop
	}
	portConflictStdinIsTerminal = func() bool { return stdinIsTerminal }
	t.Cleanup(func() {
		resolvePortConflictFunc = prevResolver
		portConflictStdinIsTerminal = prevStdin
	})
	return &got
}

// The port check asks an operator only outside headless mode and with a
// terminal on stdin; otherwise the resolver is told nobody can answer.
func TestCheckPortConflictsPassesInteractivity(t *testing.T) {
	for _, tc := range []struct {
		name            string
		headless        bool
		stdinIsTerminal bool
		force           bool
		want            portcheck.ResolveOptions
	}{
		{"headless with a terminal", true, true, false, portcheck.ResolveOptions{}},
		{"headless without a terminal", true, false, false, portcheck.ResolveOptions{}},
		{"no terminal on stdin", false, false, false, portcheck.ResolveOptions{}},
		{"terminal", false, true, false, portcheck.ResolveOptions{Interactive: true}},
		{"force, headless", true, true, true, portcheck.ResolveOptions{Force: true}},
		{"force, terminal", false, true, true, portcheck.ResolveOptions{Force: true, Interactive: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := recordPortConflictOptions(t, tc.stdinIsTerminal)
			cmd := &cobra.Command{Use: "test"}
			cmd.Flags().Bool("force", tc.force, "")
			cmd.Flags().Int(flagAPIPort, 23998, "")
			if err := checkPortConflicts(cmd, tc.headless); !errors.Is(err, errTestPortConflictStop) {
				t.Fatalf("checkPortConflicts() error = %v, want the resolver's error", err)
			}
			if len(*got) != 1 || (*got)[0] != tc.want {
				t.Fatalf("resolver options = %+v, want [%+v]", *got, tc.want)
			}
		})
	}
}

// spt run settles its effective headless mode (the flag, or no /dev/tty)
// before the port check and passes it on.
func TestRunPortCheckUsesEffectiveHeadlessMode(t *testing.T) {
	for _, tc := range []struct {
		name     string
		headless bool
		want     bool
	}{
		{"headless", true, false},
		{"terminal", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Chdir(t.TempDir())
			isolateLoggingGlobals(t)
			debug = false
			logLevel = "info"
			logAppend = false
			clearEnvDefaultsTestEnv(t)
			setGlobalRunFlagForTest(t, "test-hosts", "127.0.0.1")
			prevTesting := isTesting
			isTesting = true
			prevHeadless := shouldRunHeadlessForRun
			shouldRunHeadlessForRun = func(*cobra.Command) bool { return tc.headless }
			t.Cleanup(func() {
				isTesting = prevTesting
				shouldRunHeadlessForRun = prevHeadless
			})
			got := recordPortConflictOptions(t, true)

			buf := new(bytes.Buffer)
			rootCmd.SetOut(buf)
			rootCmd.SetErr(buf)
			rootCmd.SetArgs([]string{"run", "mock"})
			t.Cleanup(func() {
				rootCmd.SetOut(nil)
				rootCmd.SetErr(nil)
				rootCmd.SetArgs(nil)
			})
			if _, err := rootCmd.ExecuteC(); !errors.Is(err, errTestPortConflictStop) {
				t.Fatalf("ExecuteC() error = %v, want the resolver's error", err)
			}
			if len(*got) != 1 || (*got)[0].Interactive != tc.want {
				t.Fatalf("resolver options = %+v, want one call with Interactive %t", *got, tc.want)
			}
		})
	}
}

// spt replay settles its effective headless mode before the port check and
// passes it on.
func TestReplayPortCheckUsesEffectiveHeadlessMode(t *testing.T) {
	server := newReplayArchiveServerWithLaunchFlags(t)
	defer server.Close()
	for _, tc := range []struct {
		name     string
		headless bool
		want     bool
	}{
		{"headless", true, false},
		{"terminal", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Cleanup(installReplayEngineInfoSeams(t))
			shouldReplayRunHeadless = func(*cobra.Command) bool { return tc.headless }
			confirmReplayLaunchCommand = func(io.Writer) error {
				t.Error("replay asked to confirm the launch before the port check")
				return nil
			}
			got := recordPortConflictOptions(t, true)

			cmd := newReplayCommandForTest(t)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{"--from", server.URL, "--endpoints", "http://s3.example", "--results-dir", t.TempDir()})
			if err := cmd.Execute(); !errors.Is(err, errTestPortConflictStop) {
				t.Fatalf("Execute() error = %v, want the resolver's error", err)
			}
			if len(*got) != 1 || (*got)[0].Interactive != tc.want {
				t.Fatalf("resolver options = %+v, want one call with Interactive %t", *got, tc.want)
			}
		})
	}
}
