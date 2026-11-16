/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// signalCause mirrors signal.NotifyContext's cause, which matches
// context.Canceled.
type signalCause string

func (s signalCause) Error() string { return string(s) }

func (s signalCause) Is(target error) bool { return target == context.Canceled }

var errInterruptCause error = signalCause("interrupt signal received")

type connectCancelFixture struct {
	orchestrator *MultiHostOrchestrator
	mu           sync.Mutex
	notes        []string
}

// newConnectCancelFixture builds three hosts (entry, worker-1, worker-2)
// whose Docker connect is the given function.
func newConnectCancelFixture(
	t *testing.T,
	minHosts int,
	connect func(ctx context.Context, host *hostparse.HostInfo) error,
) *connectCancelFixture {
	t.Helper()
	hostInfos := []*hostparse.HostInfo{
		{Host: "entry", Original: "entry"},
		{Host: "worker-1", Original: "worker-1"},
		{Host: "worker-2", Original: "worker-2"},
	}
	f := &connectCancelFixture{orchestrator: NewMultiHostOrchestrator(hostInfos, minHosts)}
	f.orchestrator.preflight = identityPreflight{}
	f.orchestrator.SetNotifier(func(msg string) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.notes = append(f.notes, msg)
	})
	f.orchestrator.newDockerManagerForHost = func(ctx context.Context, host *hostparse.HostInfo) (*DockerManager, error) {
		if err := connect(ctx, host); err != nil {
			return nil, err
		}
		managerCtx, cancel := context.WithCancel(ctx)
		return &DockerManager{ctx: managerCtx, cancel: cancel, hostInfo: host}, nil
	}
	t.Cleanup(func() {
		for _, host := range f.orchestrator.hosts {
			if host.DockerManager != nil {
				host.DockerManager.Close()
			}
		}
	})
	return f
}

func (f *connectCancelFixture) allNotes() string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return strings.Join(f.notes, "\n")
}

// requireCancelError checks that err is the one-line connect cancel error and
// that no host-count or failure-list output was printed.
func (f *connectCancelFixture) requireCancelError(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("ConnectHosts() error = nil, want the cancel\nnotes:\n%s", f.allNotes())
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ConnectHosts() error = %v, want errors.Is(err, context.Canceled)", err)
	}
	const want = "host connection cancelled: interrupt signal received"
	if err.Error() != want {
		t.Fatalf("ConnectHosts() error = %q, want %q", err, want)
	}
	for _, bad := range []string{"insufficient hosts connected", "designated entry host", "Ensure:", "signal:", "\n"} {
		if strings.Contains(err.Error(), bad) {
			t.Fatalf("ConnectHosts() error = %q, must not contain %q", err, bad)
		}
	}
	notes := f.allNotes()
	for _, bad := range []string{"Continuing with", "Failed connections:", "Connection Summary"} {
		if strings.Contains(notes, bad) {
			t.Fatalf("notes contain %q after a cancel:\n%s", bad, notes)
		}
	}
}

// connectedThenBlocked connects the entry and worker-1 and blocks worker-2 on
// its context, cancelling once both others have connected. worker-2 then
// fails as a killed ssh does, without naming the cancel.
func connectedThenBlocked(cancel context.CancelCauseFunc) func(context.Context, *hostparse.HostInfo) error {
	var connected sync.WaitGroup
	connected.Add(2)
	go func() {
		connected.Wait()
		cancel(errInterruptCause)
	}()
	return func(ctx context.Context, host *hostparse.HostInfo) error {
		if host.Original == "worker-2" {
			<-ctx.Done()
			return errors.New("signal: killed")
		}
		connected.Done()
		return nil
	}
}

func TestConnectHosts_CancelWithMinHostsMetReportsCancel(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	f := newConnectCancelFixture(t, 2, connectedThenBlocked(cancel))

	f.requireCancelError(t, f.orchestrator.ConnectHosts(ctx))
}

func TestConnectHosts_CancelWithDefaultMinHostsReportsCancel(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	f := newConnectCancelFixture(t, 3, connectedThenBlocked(cancel))

	f.requireCancelError(t, f.orchestrator.ConnectHosts(ctx))
}

func TestConnectHosts_CancelWithEveryHostBlockedReportsCancel(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	var started sync.WaitGroup
	started.Add(3)
	go func() {
		started.Wait()
		cancel(errInterruptCause)
	}()
	f := newConnectCancelFixture(t, 2, func(ctx context.Context, _ *hostparse.HostInfo) error {
		started.Done()
		<-ctx.Done()
		return errors.New("signal: killed")
	})

	f.requireCancelError(t, f.orchestrator.ConnectHosts(ctx))
}

// TestConnectHosts_CancelBeforeHostsReturnReportsCancel: worker-2's ssh dies
// with a non-context error and the context ends before the host attempts
// have all returned.
func TestConnectHosts_CancelBeforeHostsReturnReportsCancel(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	f := newConnectCancelFixture(t, 2, func(_ context.Context, host *hostparse.HostInfo) error {
		if host.Original == "worker-2" {
			cancel(errInterruptCause)
			return errors.New("signal: interrupt")
		}
		return nil
	})

	f.requireCancelError(t, f.orchestrator.ConnectHosts(ctx))
}

// TestConnectHosts_CancelAfterHostsReturnReportsCancel: a terminal Ctrl-C
// signals ssh, so a host attempt can fail with "signal: interrupt" and every
// host goroutine can return before the CLI's handler cancels the context. The
// cancel lands a few milliseconds later, inside the bounded wait; a bare
// ctx.Err() check after the host attempts would miss it.
func TestConnectHosts_CancelAfterHostsReturnReportsCancel(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	defer cancel(nil)
	f := newConnectCancelFixture(t, 2, func(_ context.Context, host *hostparse.HostInfo) error {
		if host.Original == "worker-2" {
			time.AfterFunc(10*time.Millisecond, func() { cancel(errInterruptCause) })
			return errors.New("signal: interrupt")
		}
		return nil
	})
	f.orchestrator.connectCancelWait = 2 * time.Second // the cancel must land in the window under load

	f.requireCancelError(t, f.orchestrator.ConnectHosts(ctx))
}

// TestConnectHosts_AllConnectedDoesNotWait sets a long cancel window: when
// every host connects, ConnectHosts must not spend any of it.
func TestConnectHosts_AllConnectedDoesNotWait(t *testing.T) {
	f := newConnectCancelFixture(t, 3, func(context.Context, *hostparse.HostInfo) error { return nil })
	f.orchestrator.connectCancelWait = time.Minute

	started := time.Now()
	if err := f.orchestrator.ConnectHosts(context.Background()); err != nil {
		t.Fatalf("ConnectHosts() error = %v, want nil", err)
	}
	if elapsed := time.Since(started); elapsed > 30*time.Second {
		t.Fatalf("ConnectHosts() took %v with every host connected, want no cancel wait", elapsed)
	}
	if notes := f.allNotes(); !strings.Contains(notes, "Connection Summary: 3/3 hosts connected") {
		t.Fatalf("notes lack the connection summary:\n%s", notes)
	}
}

// TestConnectHosts_UncancelledWorkerFailureWaitsOnlyTheWindow: a real worker
// failure, with no cancel, keeps today's output after at most the window.
func TestConnectHosts_UncancelledWorkerFailureWaitsOnlyTheWindow(t *testing.T) {
	f := newConnectCancelFixture(t, 2, func(_ context.Context, host *hostparse.HostInfo) error {
		if host.Original == "worker-2" {
			return errors.New("connection unavailable")
		}
		return nil
	})
	f.orchestrator.connectCancelWait = 50 * time.Millisecond

	started := time.Now()
	if err := f.orchestrator.ConnectHosts(context.Background()); err != nil {
		t.Fatalf("ConnectHosts() error = %v, want the worker failure tolerated", err)
	}
	if elapsed := time.Since(started); elapsed < 50*time.Millisecond {
		t.Fatalf("ConnectHosts() returned after %v, want it to wait the cancel window", elapsed)
	}
	notes := f.allNotes()
	for _, want := range []string{"Failed connections:", "worker-2: connection unavailable", "Continuing with 2 hosts (minimum threshold met)"} {
		if !strings.Contains(notes, want) {
			t.Fatalf("notes lack %q:\n%s", want, notes)
		}
	}
}
