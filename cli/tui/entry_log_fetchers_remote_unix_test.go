//go:build unix

package tui

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// TestRemoteLogFetcher_CleanExitWithHeldPipeEndsWithoutWarning runs the real
// fetcher, Docker operations and executor against a fake docker CLI that exits
// 0 when its container stops, leaving a descendant that holds stderr open, as
// an ssh ProxyJump child does. The relay must end there instead of warning
// about a dropped stream and reconnecting.
func TestRemoteLogFetcher_CleanExitWithHeldPipeEndsWithoutWarning(t *testing.T) {
	dir := t.TempDir()
	calls := filepath.Join(dir, "calls")
	holderPID := filepath.Join(dir, "holder.pid")
	t.Cleanup(func() {
		if data, err := os.ReadFile(holderPID); err == nil {
			for _, field := range strings.Fields(string(data)) {
				if pid, err := strconv.Atoi(field); err == nil {
					_ = syscall.Kill(pid, syscall.SIGKILL)
				}
			}
		}
	})
	script := "#!/bin/sh\necho call >> '" + calls + "'\n" +
		"echo '2025-09-14T12:00:01.000000000Z first'\n" +
		"echo '2025-09-14T12:00:02.000000000Z second'\n" +
		"sleep 10 >/dev/null & echo $! >> '" + holderPID + "'\n" +
		"exit 0\n"
	if err := os.WriteFile(filepath.Join(dir, constants.DockerCommand), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	var logs bytes.Buffer
	previous := logging.GetLogger()
	logging.SetLogger(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { logging.SetLogger(previous) })

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	host := &hostparse.HostInfo{Host: "localhost", IsLocal: true}
	ops := command.NewDockerOperations(command.NewCommandExecutor(), host)
	var relayed []string
	started := time.Now()
	if err := newRemoteLogFetcher(ops, "abc").Stream(ctx, func(s string) { relayed = append(relayed, s) }); err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if strings.Contains(logs.String(), "remote log stream dropped") {
		t.Fatalf("a clean exit must not report a dropped stream:\n%s", logs.String())
	}
	if strings.Join(relayed, "|") != "first|second" {
		t.Fatalf("relayed lines = %q", relayed)
	}
	data, err := os.ReadFile(calls)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(data), "call"); n != 1 {
		t.Fatalf("docker logs ran %d times; a clean exit must not reconnect", n)
	}
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("Stream returned after %v, waiting on the held pipe", elapsed)
	}
}

// TestRemoteLogFetcher_SlowRelayAtExitReplaysUnreadOutput blocks the relay on
// the first line past the stream's wait delay while the fake docker CLI writes
// its last line and exits 0, with no descendant holding the pipe (a blocked
// terminal or headless sink). The cut can leave that line unread, so the relay
// must reconnect and replay it rather than end as if the stream completed.
func TestRemoteLogFetcher_SlowRelayAtExitReplaysUnreadOutput(t *testing.T) {
	dir := t.TempDir()
	ack := filepath.Join(dir, "ack")
	script := "#!/bin/sh\n" +
		"if [ ! -f '" + ack + "' ]; then\n" +
		"echo '2025-09-14T12:00:01.000000000Z first'\n" +
		"while [ ! -f '" + ack + "' ]; do sleep 0.01; done\n" +
		"fi\n" +
		"echo '2025-09-14T12:00:02.000000000Z FINAL-DIAGNOSTIC'\n"
	if err := os.WriteFile(filepath.Join(dir, constants.DockerCommand), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	host := &hostparse.HostInfo{Host: "localhost", IsLocal: true}
	ops := command.NewDockerOperations(command.NewCommandExecutor(), host)
	var relayed []string
	err := newRemoteLogFetcher(ops, "abc").Stream(ctx, func(s string) {
		relayed = append(relayed, s)
		if s == "first" {
			if err := os.WriteFile(ack, nil, 0o600); err != nil {
				t.Error(err)
			}
			time.Sleep(constants.StreamCommandWaitDelay + 1500*time.Millisecond)
		}
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if strings.Join(relayed, "|") != "first|FINAL-DIAGNOSTIC" {
		t.Fatalf("relayed lines = %q; the line written before exit was lost", relayed)
	}
}
