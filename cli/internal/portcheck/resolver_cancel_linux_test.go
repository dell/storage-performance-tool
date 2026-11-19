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
	var listenErr error
	if err := raw.Control(func(fd uintptr) { listenErr = syscall.Listen(int(fd), 0) }); err != nil {
		t.Fatal(err)
	}
	if listenErr != nil {
		t.Fatalf("shrink the accept queue: %v", listenErr)
	}
	address := listener.Addr().String()
	const attempts = 8
	for range attempts {
		conn, err := net.DialTimeout("tcp", address, 300*time.Millisecond)
		if err == nil {
			t.Cleanup(func() { _ = conn.Close() })
			continue
		}
		// Only a dial that waited out its timeout shows a full queue; a refusal
		// or any other error would let the probe under test end at once.
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("filling the accept queue: %v", err)
		}
		_, port, _ := net.SplitHostPort(address)
		return port
	}
	t.Logf("the accept queue of %s took %d connections without filling", address, attempts)
	t.Skip("the accept queue never filled on this host")
	return ""
}

// slowDialChecker probes port with a connection timeout far longer than the test.
func slowDialChecker(port string) *PortChecker {
	return NewPortCheckerWithOptions(port, "127.0.0.1", time.Minute, time.Minute, 1)
}

// cancelSoon cancels a context shortly, as Ctrl-C would. It returns the time
// taken before the timer started, so the cancellation is never earlier than
// 100ms after it, however late the caller runs.
func cancelSoon(t *testing.T) (context.Context, time.Time) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	start := time.Now()
	timer := time.AfterFunc(100*time.Millisecond, cancel)
	t.Cleanup(func() {
		timer.Stop()
		cancel()
	})
	return ctx, start
}

// assertEndedByCancellation checks that the probe waited for the cancellation at
// 100ms and ended soon after it. The upper bound only separates a dial that the
// context ended from one that waited out its minute-long timeout.
func assertEndedByCancellation(t *testing.T, ctx context.Context, start time.Time) {
	t.Helper()
	elapsed := time.Since(start)
	if ctx.Err() == nil || elapsed < 100*time.Millisecond {
		t.Errorf("ended after %s with context error %v, before the cancellation at 100ms", elapsed, ctx.Err())
	}
	if elapsed > 15*time.Second {
		t.Errorf("took %s after a cancellation at 100ms", elapsed)
	}
}

// The connection test that detection uses ends with the context.
func TestDetectionDialEndsWithTheContext(t *testing.T) {
	checker := slowDialChecker(fullQueueListener(t))
	ctx, start := cancelSoon(t)
	if within(t, func() bool { return checker.isListening(ctx) }) {
		t.Error("isListening() = true; the dial should have been waiting on the full queue")
	}
	assertEndedByCancellation(t, ctx, start)
}

// The connection in IsPortHeld, which follows a bind that settles nothing (as on
// Windows, or a port this user may not bind), ends with the context, and the
// unfinished probe counts as held.
func TestIsPortHeldDialEndsWithTheContext(t *testing.T) {
	checker := slowDialChecker(fullQueueListener(t))
	checker.bind = func(string) (net.Listener, error) { return nil, syscall.EACCES }
	ctx, start := cancelSoon(t)
	if !within(t, func() bool { return checker.IsPortHeld(ctx) }) {
		t.Error("IsPortHeld() = false for a probe the context ended")
	}
	assertEndedByCancellation(t, ctx, start)
}

// Ctrl-C during the check after a force stop ends it at once, as an interruption.
func TestForceVerificationEndsWithTheContext(t *testing.T) {
	port := fullQueueListener(t)
	api := &fakeDockerAPI{}
	checker := slowDialChecker(port)
	checker.newFinder = func() (*DockerContainerFinder, error) { return newFakeFinder(api), nil }
	resolver := &ConflictResolver{Port: port, checker: checker, dockerFinder: newFakeFinder(api), clock: &pollClock{}}
	ctx, start := cancelSoon(t)

	result := within(t, func() *ResolutionResult { return resolver.verifyResolution(ctx, StrategyForceStop) })
	if result.Success || !strings.Contains(result.Message, "Interrupted while checking port") ||
		!errors.Is(result.Error, context.Canceled) {
		t.Fatalf("result = %+v, want the interruption reported", result)
	}
	assertEndedByCancellation(t, ctx, start)
}
