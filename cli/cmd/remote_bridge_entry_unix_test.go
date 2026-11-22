//go:build unix

/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"context"
	"io"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/portcheck"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/dell/storage-performance-tool/cli/tui/headless"
	"github.com/spf13/cobra"
)

// remoteLoopbackWorker is a loopback address the host parser classifies as remote, so the
// orchestrator's RMI readiness wait on the worker reaches a listener in the test.
const remoteLoopbackWorker = "127.0.0.2"

// listenOnRemoteLoopbackWorker serves TCP on a port the kernel picks at remoteLoopbackWorker,
// standing in for an attached worker's RMI registry, and returns the port. The test is skipped
// where that address is not configured.
func listenOnRemoteLoopbackWorker(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", net.JoinHostPort(remoteLoopbackWorker, "0"))
	if err != nil {
		t.Skipf("cannot listen on %s: %v", remoteLoopbackWorker, err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	return listener.Addr().(*net.TCPAddr).Port
}

// remoteEntryDockerRun returns the entry docker run the fake ssh received, split into words.
func remoteEntryDockerRun(t *testing.T, sshCalls []string) []string {
	t.Helper()
	for _, call := range sshCalls {
		if strings.Contains(call, "docker run") && strings.Contains(call, "spt.role=entry") {
			return strings.Fields(call)
		}
	}
	t.Fatalf("no entry docker run among the ssh calls:\n%s", strings.Join(sshCalls, "\n"))
	return nil
}

// publishedPortsOf returns the -p values of a docker run.
func publishedPortsOf(tokens []string) []string {
	var ports []string
	for i, token := range tokens {
		if token == "-p" && i+1 < len(tokens) {
			ports = append(ports, tokens[i+1])
		}
	}
	return ports
}

// A two-host spt run starts its remote entry, through the real orchestrator and remote Docker
// manager, with its API port published on a bridge network and with host networking and no
// published port in host mode. The workers are attached, and the fake ssh refuses the entry's
// docker run, so no engine starts.
func TestRunCmdBridgeRemoteEntryPublishesTheRunsAPIPort(t *testing.T) {
	tests := []struct {
		networkMode   string
		wantPublished []string
		wantHostNet   bool
	}{
		{networkMode: constants.BridgeNetworkMode, wantPublished: []string{"19412:19412/tcp"}},
		{networkMode: constants.HostNetworkMode, wantHostNet: true},
	}
	for _, test := range tests {
		t.Run(test.networkMode, func(t *testing.T) {
			registryPort := listenOnRemoteLoopbackWorker(t)
			t.Chdir(t.TempDir())
			sshLog, _ := installFakeSSHAndSCP(t)
			t.Setenv("ADVERTISED_IPS", remoteLoopbackWorker+"="+remoteLoopbackWorker)
			setDeleteRouteFlags(t, "entry.example,"+remoteLoopbackWorker, "2")
			setGlobalRunFlagForTest(t, "network-mode", test.networkMode)
			setGlobalRunFlagForTest(t, "api-port", "19412")
			setGlobalRunFlagForTest(t, flagRMIRegistryPort, strconv.Itoa(registryPort))
			setGlobalRunFlagForTest(t, flagAttachExistingWorkers, "true")
			setGlobalRunFlagForTest(t, flagSkipImagePull, "true")

			previousPort := resolvePortConflictFunc
			previousLocal := startLocalHeadlessRunFunc
			t.Cleanup(func() {
				resolvePortConflictFunc = previousPort
				startLocalHeadlessRunFunc = previousLocal
			})
			resolvePortConflictFunc = func(context.Context, string, portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			var localCalls int
			startLocalHeadlessRunFunc = func(string, string, scenario.Params, headless.HeadlessOptions) error {
				localCalls++
				return nil
			}

			if err := runCmd.RunE(runCmd, []string{WorkloadTypeDelete}); err == nil {
				t.Fatal("RunE() succeeded although the fake ssh refuses docker run")
			}
			if localCalls != 0 {
				t.Fatalf("a distributed run took the local route %d time(s)", localCalls)
			}
			entry := remoteEntryDockerRun(t, readLogLines(t, sshLog))
			line := strings.Join(entry, " ")
			if got := publishedPortsOf(entry); !slices.Equal(got, test.wantPublished) {
				t.Errorf("entry publishes %q, want %q: %s", got, test.wantPublished, line)
			}
			if !slices.Contains(entry, "--run-port=19412") {
				t.Errorf("entry lacks --run-port=19412: %s", line)
			}
			if hostNet := strings.Contains(line, "--network host"); hostNet != test.wantHostNet {
				t.Errorf("entry host networking = %t, want %t: %s", hostNet, test.wantHostNet, line)
			}
		})
	}
}

// A distributed spt replay starts its remote entry the same way: with its API port published on
// a bridge network, and with host networking and no published port in host mode.
func TestReplayCommandBridgeRemoteEntryPublishesTheReplaysAPIPort(t *testing.T) {
	server := newReplayArchiveServer(t)
	defer server.Close()
	tests := []struct {
		networkMode   string
		wantPublished []string
		wantHostNet   bool
	}{
		{networkMode: constants.BridgeNetworkMode, wantPublished: []string{"19412:19412/tcp"}},
		{networkMode: constants.HostNetworkMode, wantHostNet: true},
	}
	for _, test := range tests {
		t.Run(test.networkMode, func(t *testing.T) {
			registryPort := listenOnRemoteLoopbackWorker(t)
			sshLog, _ := installFakeSSHAndSCP(t)
			t.Setenv("ADVERTISED_IPS", remoteLoopbackWorker+"="+remoteLoopbackWorker)
			// The connect and remote launch stay real; only the local port check, the launch
			// confirmation and the headless choice are stubbed.
			t.Cleanup(installReplayEngineInfoSeams(t))
			resolvePortConflictFunc = func(context.Context, string, portcheck.ResolveOptions) (*portcheck.ResolutionResult, error) {
				return &portcheck.ResolutionResult{Success: true}, nil
			}
			confirmReplayLaunchCommand = func(io.Writer) error { return nil }
			shouldReplayRunHeadless = func(*cobra.Command) bool { return true }
			var localCalls int
			startReplayLocalHeadless = func(
				string, string, scenario.Params, headless.HeadlessOptions, []byte, []byte,
			) error {
				localCalls++
				return nil
			}

			cmd := newReplayCommandForTest(t)
			cmd.SetOut(io.Discard)
			cmd.SetErr(io.Discard)
			cmd.SetArgs([]string{
				"--from", server.URL,
				"--endpoints", "http://s3.example",
				"--test-hosts", "entry.example," + remoteLoopbackWorker,
				"--min-hosts", "2",
				"--network-mode", test.networkMode,
				"--api-port", "19412",
				"--" + flagRMIRegistryPort, strconv.Itoa(registryPort),
				"--" + flagAttachExistingWorkers,
				"--" + flagSkipImagePull,
				"--auto-results=false",
				"--results-dir", t.TempDir(),
			})
			if err := cmd.Execute(); err == nil {
				t.Fatal("Execute() succeeded although the fake ssh refuses docker run")
			}
			if localCalls != 0 {
				t.Fatalf("a distributed replay took the local route %d time(s)", localCalls)
			}
			entry := remoteEntryDockerRun(t, readLogLines(t, sshLog))
			line := strings.Join(entry, " ")
			if got := publishedPortsOf(entry); !slices.Equal(got, test.wantPublished) {
				t.Errorf("entry publishes %q, want %q: %s", got, test.wantPublished, line)
			}
			if !slices.Contains(entry, "--run-port=19412") {
				t.Errorf("entry lacks --run-port=19412: %s", line)
			}
			if hostNet := strings.Contains(line, "--network host"); hostNet != test.wantHostNet {
				t.Errorf("entry host networking = %t, want %t: %s", hostNet, test.wantHostNet, line)
			}
		})
	}
}
