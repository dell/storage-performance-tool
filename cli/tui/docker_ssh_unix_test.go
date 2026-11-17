//go:build unix

/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// installSleepingSSH puts a fake ssh on PATH that never answers, as a host
// whose connect hangs.
func installSleepingSSH(t *testing.T) {
	t.Helper()
	dir := t.TempDir()
	script := "#!/bin/sh\nexec sleep 30\n"
	if err := os.WriteFile(filepath.Join(dir, constants.SSHCommand), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

func remoteTestHost() *hostparse.HostInfo {
	return &hostparse.HostInfo{Host: "remote.invalid", User: "spt", Original: "spt@remote.invalid"}
}

func TestNewDockerManagerForHost_CallerCancelIsNotAnSSHFailure(t *testing.T) {
	installSleepingSSH(t)
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	time.AfterFunc(200*time.Millisecond, func() { cancel(errInterruptCause) })

	dm, err := NewDockerManagerForHost(ctx, remoteTestHost())
	if dm != nil {
		dm.Close()
	}
	if err == nil {
		t.Fatal("NewDockerManagerForHost() error = nil, want the cancel")
	}
	if !errors.Is(err, context.Canceled) || !errors.Is(err, errInterruptCause) {
		t.Fatalf("NewDockerManagerForHost() error = %v, want the caller's cancel and its cause", err)
	}
	if strings.Contains(err.Error(), "Ensure:") {
		t.Fatalf("NewDockerManagerForHost() error after a cancel carries the SSH advice:\n%v", err)
	}
}

func TestNewDockerManagerForHost_PingDeadlineKeepsSSHAdvice(t *testing.T) {
	installSleepingSSH(t)
	previous := remoteDockerPingTimeout
	remoteDockerPingTimeout = 200 * time.Millisecond
	t.Cleanup(func() { remoteDockerPingTimeout = previous })

	dm, err := NewDockerManagerForHost(context.Background(), remoteTestHost())
	if dm != nil {
		dm.Close()
	}
	if err == nil {
		t.Fatal("NewDockerManagerForHost() error = nil, want a connection failure")
	}
	if errors.Is(err, context.Canceled) {
		t.Fatalf("NewDockerManagerForHost() error = %v, a ping deadline is not a cancel", err)
	}
	if !strings.Contains(err.Error(), "failed to connect to Docker on spt@remote.invalid") ||
		!strings.Contains(err.Error(), "Ensure:") {
		t.Fatalf("NewDockerManagerForHost() error after the ping deadline lacks the SSH advice:\n%v", err)
	}
}
