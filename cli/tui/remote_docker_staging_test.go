/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// stagingCall is one command the staging fake ran, with the state of its
// context when the command started.
type stagingCall struct {
	command     string
	ctxErr      error
	hasDeadline bool
	timeLeft    time.Duration
	value       any
}

type stagingValueKey struct{}

// stagingExecutor records every staging command. copy decides how each
// CopyFile behaves; rmErr fails every rm.
type stagingExecutor struct {
	mu          sync.Mutex
	calls       []stagingCall
	copy        func(ctx context.Context) error
	copyStarted chan struct{}
	rmErr       error
}

func (e *stagingExecutor) record(ctx context.Context, command string) {
	deadline, hasDeadline := ctx.Deadline()
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, stagingCall{
		command:     command,
		ctxErr:      ctx.Err(),
		hasDeadline: hasDeadline,
		timeLeft:    time.Until(deadline),
		value:       ctx.Value(stagingValueKey{}),
	})
}

func (e *stagingExecutor) recorded() []stagingCall {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]stagingCall(nil), e.calls...)
}

func (e *stagingExecutor) ExecuteCommand(ctx context.Context, _ *hostparse.HostInfo, command []string) (string, string, error) {
	e.record(ctx, strings.Join(command, " "))
	if err := ctx.Err(); err != nil {
		return "", "", err
	}
	if command[0] == "rm" && e.rmErr != nil {
		return "", "rm: cannot remove", e.rmErr
	}
	return "", "", nil
}

func (e *stagingExecutor) CopyFile(ctx context.Context, _ *hostparse.HostInfo, localPath, remotePath string) error {
	e.record(ctx, "copy "+localPath+" "+remotePath)
	if e.copyStarted != nil {
		select {
		case e.copyStarted <- struct{}{}:
		default:
		}
	}
	if e.copy == nil {
		return nil
	}
	return e.copy(ctx)
}

func (e *stagingExecutor) CopyFromHost(context.Context, *hostparse.HostInfo, string, string) error {
	return errors.New("staging never copies from the host")
}

func newStagingManager(t *testing.T, exec *stagingExecutor) *RemoteDockerManager {
	t.Helper()
	host := &hostparse.HostInfo{User: "root", Host: "worker.example.com", Original: "root@worker.example.com"}
	mgr, err := newRemoteDockerManagerWithExecutor(host, exec)
	if err != nil {
		t.Fatalf("new remote manager: %v", err)
	}
	return mgr
}

// deleteStagingMounts are the two files a DELETE from --items-file stages.
var deleteStagingMounts = []scenario.FileMount{
	{HostPath: "/host/delete-manifest.csv", ContainerPath: "/spt-input/items/delete-manifest.csv"},
	{HostPath: "/host/delete-manifest.complete", ContainerPath: "/spt-input/items/delete-manifest.complete"},
}

func TestRemoteStagingSmallCommandsKeepThirtySecondsEach(t *testing.T) {
	mgr := newStagingManager(t, &stagingExecutor{})
	if want := time.Duration(constants.ContainerStartTimeoutSecs) * time.Second; mgr.stagingCommandTimeout != want || want != 30*time.Second {
		t.Fatalf("staging command timeout = %v, want %v (30 s)", mgr.stagingCommandTimeout, want)
	}
}

// A copy that outlasts the small-command timeout succeeds: the copy has no cap
// of its own, and each mkdir and chmod gets a fresh timeout instead of sharing
// one with the copies.
func TestRemoteStagingCopyOutlastsTheSmallCommandTimeout(t *testing.T) {
	const commandTimeout = 50 * time.Millisecond
	exec := &stagingExecutor{copy: func(ctx context.Context) error {
		select {
		case <-time.After(4 * commandTimeout):
			return nil
		case <-ctx.Done():
			return fmt.Errorf("copy cut off: %w", ctx.Err())
		}
	}}
	mgr := newStagingManager(t, exec)
	mgr.stagingCommandTimeout = commandTimeout

	if err := mgr.SetFileMountsContext(context.Background(), deleteStagingMounts); err != nil {
		t.Fatalf("staging error = %v, want a copy longer than the command timeout to succeed", err)
	}
	calls := exec.recorded()
	var kinds []string
	for _, call := range calls {
		kind, _, _ := strings.Cut(call.command, " ")
		kinds = append(kinds, kind)
		if call.ctxErr != nil {
			t.Fatalf("%q started on an ended context: %v", call.command, call.ctxErr)
		}
		if kind == "copy" {
			if call.hasDeadline {
				t.Fatalf("%q ran with a deadline %v away; a copy has no fixed cap", call.command, call.timeLeft)
			}
			continue
		}
		if !call.hasDeadline || call.timeLeft <= 0 || call.timeLeft > commandTimeout {
			t.Fatalf("%q ran with deadline=%t, %v left; want its own %v", call.command, call.hasDeadline, call.timeLeft, commandTimeout)
		}
	}
	if want := []string{"mkdir", "copy", "chmod", "copy", "chmod"}; !reflect.DeepEqual(kinds, want) {
		t.Fatalf("staging commands = %q, want %q", kinds, want)
	}
	if len(mgr.stagedBindMounts) != 2 || !mgr.HasManagedResources() {
		t.Fatalf("staged binds = %q, owned = %t; want both files staged and owned", mgr.stagedBindMounts, mgr.HasManagedResources())
	}
}

// A cancel during a copy returns at once, still reads as a cancellation, and
// removes the staging directory on a fresh context bounded by
// remoteCleanupTimeout, so nothing is left owned for the rollback.
func TestRemoteStagingCancelDuringCopyRemovesTheDirectory(t *testing.T) {
	exec := &stagingExecutor{
		copyStarted: make(chan struct{}, 1),
		copy: func(ctx context.Context) error {
			<-ctx.Done()
			return fmt.Errorf("copy /host/items.csv to worker:/tmp/x stopped: %w", context.Cause(ctx))
		},
	}
	mgr := newStagingManager(t, exec)
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), stagingValueKey{}, "run"))
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- mgr.SetFileMountsContext(ctx, deleteStagingMounts)
	}()
	select {
	case <-exec.copyStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("the copy did not start")
	}
	stagingDir := mgr.stagingDir
	cancel()
	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("staging did not return after the cancel")
	}
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "stopped") {
		t.Fatalf("staging error = %v, want the copy's cancellation", err)
	}
	calls := exec.recorded()
	last := calls[len(calls)-1]
	if last.command != "rm -rf "+stagingDir {
		t.Fatalf("last staging command = %q, want rm -rf %s", last.command, stagingDir)
	}
	if last.ctxErr != nil || !last.hasDeadline || last.timeLeft <= 0 || last.timeLeft > remoteCleanupTimeout {
		t.Fatalf("rm ran with err=%v deadline=%t, %v left; want a live context bounded by %v",
			last.ctxErr, last.hasDeadline, last.timeLeft, remoteCleanupTimeout)
	}
	if last.value != "run" {
		t.Fatalf("rm context value = %v, want the run's values kept", last.value)
	}
	if mgr.HasManagedResources() || mgr.stagedBindMounts != nil {
		t.Fatalf("after the cleanup: owned=%t binds=%q; want nothing left", mgr.HasManagedResources(), mgr.stagedBindMounts)
	}
}

// A copy that fails on its own also cleans up; when that removal fails too,
// the directory stays owned so the orchestrator's rollback retries it.
func TestRemoteStagingFailedRemovalKeepsTheDirectoryOwned(t *testing.T) {
	copyErr := errors.New("copy /host/items.csv to worker:/tmp/x failed: exit status 1: lost connection")
	exec := &stagingExecutor{
		copy:  func(context.Context) error { return copyErr },
		rmErr: errors.New("exit status 1"),
	}
	mgr := newStagingManager(t, exec)
	err := mgr.SetFileMountsContext(context.Background(), deleteStagingMounts[:1])
	if !errors.Is(err, copyErr) || !strings.Contains(err.Error(), "remove remote staging directory") {
		t.Fatalf("staging error = %v, want the copy failure and the failed removal", err)
	}
	if strings.Contains(err.Error(), "context deadline exceeded") {
		t.Fatalf("the removal must not run on an expired context: %v", err)
	}
	if !mgr.HasManagedResources() {
		t.Fatal("a staging directory whose removal failed must stay owned for the rollback")
	}
	exec.rmErr = nil
	if err := mgr.CleanupContext(context.Background()); err != nil || mgr.HasManagedResources() {
		t.Fatalf("rollback cleanup = %v, owned = %t; want the directory removed", err, mgr.HasManagedResources())
	}
}

// Each host prints one notice with the item files' total size before its
// copy, so a long serial staging does not look like a hang.
func TestItemFileStagingNoticePrecedesEachHostCopy(t *testing.T) {
	dir := t.TempDir()
	manifest, marker := filepath.Join(dir, "manifest.csv"), filepath.Join(dir, "manifest.complete")
	if err := os.WriteFile(manifest, make([]byte, 1024), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, make([]byte, 512), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mounts []scenario.FileMount
		size   string
	}{
		{"with size", []scenario.FileMount{
			{HostPath: manifest, ContainerPath: "/spt-input/items/delete-manifest.csv"},
			{HostPath: marker, ContainerPath: "/spt-input/items/delete-manifest.complete"},
		}, "1.5KiB of "},
		{"unreadable file", []scenario.FileMount{{HostPath: filepath.Join(dir, "missing.csv"), ContainerPath: "/spt-input/items/read-items.csv"}}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{
				{Host: "entry", Original: "entry"}, {Host: "worker", Original: "worker"},
			}, 2)
			var events []string
			orchestrator.SetNotifier(func(msg string) { events = append(events, msg) })
			for _, host := range orchestrator.hosts {
				host.DockerManager = &orderedMountManager{MockDockerManager: NewMockDockerManager(), host: host.Info.Original, events: &events}
				host.SetStatus(HostStatusReady)
			}
			if err := orchestrator.configureItemFileMounts(context.Background(), orchestrator.hosts, tc.mounts); err != nil {
				t.Fatal(err)
			}
			want := []string{
				"📦 Copying " + tc.size + "item files to entry...", "stage entry",
				"📦 Copying " + tc.size + "item files to worker...", "stage worker",
			}
			if !reflect.DeepEqual(events, want) {
				t.Fatalf("events = %q, want %q", events, want)
			}
		})
	}
}

type orderedMountManager struct {
	*MockDockerManager
	host   string
	events *[]string
}

func (m *orderedMountManager) SetFileMounts(mounts []scenario.FileMount) error {
	*m.events = append(*m.events, "stage "+m.host)
	return m.MockDockerManager.SetFileMounts(mounts)
}
