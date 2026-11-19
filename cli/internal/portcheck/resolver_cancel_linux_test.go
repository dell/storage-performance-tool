/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"context"
	"errors"
	"net"
	"strings"
	"syscall"
	"testing"
	"time"
)

// fullQueueListener is a loopback listener whose accept queue is full, so a
// connection to it waits for its own timeout: only a dial that ctx can end
// returns sooner.
func fullQueueListener(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	raw, err := listener.(*net.TCPListener).SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Control(func(fd uintptr) { _ = syscall.Listen(int(fd), 0) }); err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	for range 8 {
		conn, err := net.DialTimeout("tcp", address, 300*time.Millisecond)
		if err != nil {
			_, port, _ := net.SplitHostPort(address)
			return port
		}
		t.Cleanup(func() { _ = conn.Close() })
	}
	t.Skip("the accept queue never filled on this host")
	return ""
}

// slowDialChecker probes port with a connection timeout far longer than the test.
func slowDialChecker(port string) *PortChecker {
	return NewPortCheckerWithOptions(port, "127.0.0.1", time.Minute, time.Minute, 1)
}

// cancelSoon cancels a context shortly, as Ctrl-C would.
func cancelSoon(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	timer := time.AfterFunc(100*time.Millisecond, cancel)
	t.Cleanup(func() {
		timer.Stop()
		cancel()
	})
	return ctx
}

func assertPrompt(t *testing.T, start time.Time) {
	t.Helper()
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("took %s after a cancellation at 100ms", elapsed)
	}
}

// The connection test that detection uses ends with the context.
func TestDetectionDialEndsWithTheContext(t *testing.T) {
	checker := slowDialChecker(fullQueueListener(t))
	ctx := cancelSoon(t)
	start := time.Now()
	within(t, func() bool { return checker.isListening(ctx) })
	assertPrompt(t, start)
}

// The connection in IsPortHeld, which follows a bind that settles nothing (as on
// Windows, or a port this user may not bind), ends with the context, and the
// unfinished probe counts as held.
func TestIsPortHeldDialEndsWithTheContext(t *testing.T) {
	checker := slowDialChecker(fullQueueListener(t))
	checker.bind = func(string) (net.Listener, error) { return nil, syscall.EACCES }
	ctx := cancelSoon(t)
	start := time.Now()
	if !within(t, func() bool { return checker.IsPortHeld(ctx) }) {
		t.Error("IsPortHeld() = false for a probe the context ended")
	}
	assertPrompt(t, start)
}

// Ctrl-C during the check after a force stop ends it at once, as an interruption.
func TestForceVerificationEndsWithTheContext(t *testing.T) {
	port := fullQueueListener(t)
	api := &fakeDockerAPI{}
	checker := slowDialChecker(port)
	checker.newFinder = func() (*DockerContainerFinder, error) { return newFakeFinder(api), nil }
	resolver := &ConflictResolver{Port: port, checker: checker, dockerFinder: newFakeFinder(api), clock: &pollClock{}}
	ctx := cancelSoon(t)

	start := time.Now()
	result := within(t, func() *ResolutionResult { return resolver.verifyResolution(ctx, StrategyForceStop) })
	if result.Success || !strings.Contains(result.Message, "Interrupted while checking port") ||
		!errors.Is(result.Error, context.Canceled) {
		t.Fatalf("result = %+v, want the interruption reported", result)
	}
	assertPrompt(t, start)
}
