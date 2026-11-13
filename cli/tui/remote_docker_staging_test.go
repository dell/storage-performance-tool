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
// CopyFile behaves; rmErr fails every rm, and rm, when set, decides how each
// rm on a live context behaves instead.
type stagingExecutor struct {
	mu          sync.Mutex
	calls       []stagingCall
	copy        func(ctx context.Context) error
	copyStarted chan struct{}
	rmErr       error
	rm          func(ctx context.Context) error
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
	if command[0] == "rm" && e.rm != nil {
		return "", "", e.rm(ctx)
	}
	return "", "", nil
}

// commands lists the recorded commands that start with prefix.
func (e *stagingExecutor) commands(prefix string) []string {
	var out []string
	for _, call := range e.recorded() {
		if strings.HasPrefix(call.command, prefix) {
			out = append(out, call.command)
		}
	}
	return out
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

// A cancel during a copy returns at once, still reads as a cancellation, adds
// no error of its own and runs nothing more: the staging directory stays owned
// for the orchestrator's rollback, which removes it.
func TestRemoteStagingCancelDuringCopyLeavesTheDirectoryForTheRollback(t *testing.T) {
	exec := &stagingExecutor{
		copyStarted: make(chan struct{}, 1),
		copy: func(ctx context.Context) error {
			<-ctx.Done()
			return fmt.Errorf("copy /host/items.csv to worker:/tmp/x stopped: %w", context.Cause(ctx))
		},
	}
	mgr := newStagingManager(t, exec)
	ctx, cancel := context.WithCancel(context.Background())
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
	if strings.Contains(err.Error(), "remove remote staging directory") {
		t.Fatalf("a cancelled staging must not add a removal error: %v", err)
	}
	calls := exec.recorded()
	if last := calls[len(calls)-1].command; !strings.HasPrefix(last, "copy ") {
		t.Fatalf("staging ran %q after the cancel; want nothing after the copy", last)
	}
	if !mgr.HasManagedResources() || mgr.stagingDir != stagingDir {
		t.Fatalf("after the cancel: owned=%t dir=%q; want %s left owned for the rollback",
			mgr.HasManagedResources(), mgr.stagingDir, stagingDir)
	}
	if err := mgr.CleanupContext(context.Background()); err != nil || mgr.HasManagedResources() {
		t.Fatalf("rollback cleanup = %v, owned = %t; want the directory removed", err, mgr.HasManagedResources())
	}
	if rms := exec.commands("rm "); !reflect.DeepEqual(rms, []string{"rm -rf " + stagingDir}) {
		t.Fatalf("removals = %q, want the rollback's alone", rms)
	}
}

// A copy that fails on its own while the run goes on removes the staging
// directory on a context derived from the run's and bounded by
// remoteCleanupTimeout. A cancel during that removal ends it at once, adds no
// error, and leaves the directory owned for the rollback.
func TestRemoteStagingFailedCopyRemovesTheDirectoryWithinTheRun(t *testing.T) {
	copyErr := errors.New("copy /host/items.csv to worker:/tmp/x failed: exit status 1: lost connection")
	t.Run("removed", func(t *testing.T) {
		exec := &stagingExecutor{copy: func(context.Context) error { return copyErr }}
		mgr := newStagingManager(t, exec)
		ctx := context.WithValue(context.Background(), stagingValueKey{}, "run")
		if err := mgr.SetFileMountsContext(ctx, deleteStagingMounts[:1]); !errors.Is(err, copyErr) || err.Error() != copyErr.Error() {
			t.Fatalf("staging error = %v, want the copy failure alone", err)
		}
		calls := exec.recorded()
		last := calls[len(calls)-1]
		if !strings.HasPrefix(last.command, "rm -rf ") {
			t.Fatalf("last staging command = %q, want the removal", last.command)
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
	})
	t.Run("a cancel ends the removal", func(t *testing.T) {
		removing := make(chan struct{})
		exec := &stagingExecutor{
			copy: func(context.Context) error { return copyErr },
			rm: func(ctx context.Context) error {
				close(removing)
				<-ctx.Done()
				return ctx.Err()
			},
		}
		mgr := newStagingManager(t, exec)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() { done <- mgr.SetFileMountsContext(ctx, deleteStagingMounts[:1]) }()
		select {
		case <-removing:
		case <-time.After(5 * time.Second):
			t.Fatal("the removal did not start")
		}
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, copyErr) || err.Error() != copyErr.Error() {
				t.Fatalf("staging error = %v, want the copy failure alone", err)
			}
		case <-time.After(time.Second):
			t.Fatal("the cancel did not end the removal")
		}
		if !mgr.HasManagedResources() {
			t.Fatal("a staging directory whose removal a cancel cut short must stay owned for the rollback")
		}
	})
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

// After a cancel during staging, the full-screen UI's join sees staging and
// the startup rollback end within TUILaunchJoinTimeout, and the rollback
// removes the directory. Each rm answers after removalTime, as on a slow but
// live host, so a removal of up to remoteCleanupTimeout before the rollback
// would push the whole past the join.
func TestItemFileStagingCancelAndRollbackEndWithinTUIJoin(t *testing.T) {
	removalTime := constants.TUILaunchJoinTimeout - remoteCleanupTimeout + 2*time.Second
	if removalTime >= constants.StartupRollbackTimeout {
		t.Fatalf("a %v removal cannot finish within the %v rollback", removalTime, constants.StartupRollbackTimeout)
	}
	exec := &stagingExecutor{
		copyStarted: make(chan struct{}, 1),
		copy: func(ctx context.Context) error {
			<-ctx.Done()
			return fmt.Errorf("copy /host/items.csv to worker:/tmp/x stopped: %w", context.Cause(ctx))
		},
		rm: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(removalTime):
				return nil
			}
		},
	}
	host := &hostparse.HostInfo{Host: "worker.example.com", Original: "worker.example.com"}
	mgr, err := newRemoteDockerManagerWithExecutor(host, exec)
	if err != nil {
		t.Fatal(err)
	}
	orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{host}, 1)
	orchestrator.SetNotifier(func(string) {})
	orchestrator.hosts[0].DockerManager = mgr

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan tuiLaunchResult, 1)
	staged := make(chan struct{})
	go func() {
		err := orchestrator.configureItemFileMounts(ctx, orchestrator.hosts, deleteStagingMounts[:1])
		// Close staged before the join can see the result, so the check
		// below never races the close.
		close(staged)
		result <- tuiLaunchResult{Submission: SubmissionNotSubmitted, Err: err}
	}()
	t.Cleanup(func() { <-staged })
	select {
	case <-exec.copyStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("the copy did not start")
	}
	stagingDir := mgr.stagingDir
	cancelledAt := time.Now()
	cancel()
	joinErr := finishTUILaunch(context.Background(), nil, result, LaunchHooks{})
	elapsed := time.Since(cancelledAt)
	if strings.Contains(joinErr.Error(), "timed out waiting for TUI startup cancellation and rollback") {
		t.Fatalf("the join gave up %v after the cancel, with staging and its rollback still running: %v", elapsed, joinErr)
	}
	select {
	case <-staged:
	default:
		t.Fatal("staging and its rollback were still running after the join returned")
	}
	if !errors.Is(joinErr, context.Canceled) || strings.Contains(joinErr.Error(), "remove remote staging directory") {
		t.Fatalf("join error = %v, want the copy's cancellation alone", joinErr)
	}
	if rms := exec.commands("rm "); !reflect.DeepEqual(rms, []string{"rm -rf " + stagingDir}) {
		t.Fatalf("removals = %q, want the rollback's alone", rms)
	}
	if mgr.HasManagedResources() {
		t.Fatalf("the rollback left %s owned", mgr.stagingDir)
	}
	t.Logf("the join returned %v after the cancel: %v", elapsed.Round(time.Millisecond), joinErr)
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
