/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"context"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// pollClock advances on every wait and reports each one, so a test can change
// the world between two port checks without sleeping.
type pollClock struct {
	now    time.Time
	polls  int
	onPoll func(poll int)
}

func (c *pollClock) Now() time.Time        { return c.now }
func (c *pollClock) Sleep(d time.Duration) { c.now = c.now.Add(d) }
func (c *pollClock) After(d time.Duration) <-chan time.Time {
	c.now = c.now.Add(d)
	c.polls++
	if c.onPoll != nil {
		c.onPoll(c.polls)
	}
	fired := make(chan time.Time, 1)
	fired <- c.now
	return fired
}

// confirmedEngine is detection's result for an engine running run 41.
func confirmedEngine() *ServiceInfo {
	return &ServiceInfo{
		IsListening: true, IsSpt: true, APIConfirmed: true,
		Status: &SptStatus{RunID: "41", State: "RUNNING", RunActive: true},
	}
}

// closeAfterPolls makes the engine's port close at the given poll.
func closeAfterPolls(engine *fakeEngine, polls int) *pollClock {
	return &pollClock{onPoll: func(poll int) {
		if poll == polls {
			engine.server.Close()
		}
	}}
}

// --force and the interactive g answer both stop the run, shut the engine down
// and succeed once the port has closed.
func TestGracefulResolutionFreesThePort(t *testing.T) {
	tests := []struct {
		name        string
		force       bool
		interactive bool
		input       string
	}{
		{name: "--force without a terminal", force: true},
		{name: "g at the prompt", interactive: true, input: "g\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := newFakeEngine(t, 41)
			resolver := engine.resolver(tt.force, tt.interactive)
			resolver.input = strings.NewReader(tt.input)
			clock := closeAfterPolls(engine, 3)
			resolver.clock = clock
			defer resolver.Close()

			result, err := resolver.CheckAndResolveConflicts(context.Background())
			if err != nil {
				t.Fatalf("CheckAndResolveConflicts() error = %v", err)
			}
			if !result.Success || result.Strategy != StrategyGracefulStop {
				t.Fatalf("result = %+v, want a successful graceful stop", result)
			}
			if clock.polls != 3 {
				t.Errorf("polls = %d, want 3: success must wait for the port to close", clock.polls)
			}
			var mutations []engineRequest
			for _, request := range engine.received() {
				if request.method != http.MethodGet && request.method != http.MethodHead {
					mutations = append(mutations, request)
				}
			}
			want := []engineRequest{
				{http.MethodDelete, "/run", "41"},
				{http.MethodPost, "/shutdown", ""},
			}
			if !slices.Equal(mutations, want) {
				t.Errorf("engine changes = %+v, want %+v", mutations, want)
			}
		})
	}
}

// An idle or orphaned engine still holds the port: it gets no stop, but it is
// shut down.
func TestGracefulResolutionShutsDownAnIdleEngine(t *testing.T) {
	engine := newFakeEngine(t, 0)
	resolver := engine.resolver(true, false)
	resolver.clock = closeAfterPolls(engine, 1)
	defer resolver.Close()

	result, err := resolver.CheckAndResolveConflicts(context.Background())
	if err != nil || !result.Success {
		t.Fatalf("CheckAndResolveConflicts() = %+v, %v, want success", result, err)
	}
	requests := engine.received()
	if slices.ContainsFunc(requests, func(r engineRequest) bool { return r.method == http.MethodDelete }) {
		t.Errorf("idle engine received DELETE: %+v", requests)
	}
	if !slices.Contains(requests, engineRequest{http.MethodPost, "/shutdown", ""}) {
		t.Errorf("idle engine was not shut down: %+v", requests)
	}
}

func TestGracefulStopReportsTheDeadline(t *testing.T) {
	engine := newFakeEngine(t, 41)
	resolver := engine.resolver(false, true)
	clock := &pollClock{}
	resolver.clock = clock
	defer resolver.Close()

	result := gracefulStopWithin(t, context.Background(), resolver, confirmedEngine())
	if result.Success {
		t.Fatal("Success = true while the port stays open")
	}
	for _, want := range []string{"still in use", constants.PortConflictGracefulStopTimeout.String(), "api.linger.sec", "--force"} {
		if !strings.Contains(result.Message, want) {
			t.Errorf("Message = %q, want it to contain %q", result.Message, want)
		}
	}
	wantPolls := int(constants.PortConflictGracefulStopTimeout / constants.PortConflictReleasePollInterval)
	if clock.polls != wantPolls {
		t.Errorf("polls = %d, want %d", clock.polls, wantPolls)
	}
}

// Under --force the message does not advise --force: the container is next.
func TestForcedGracefulStopDeadlineDoesNotAdviseForce(t *testing.T) {
	engine := newFakeEngine(t, 41)
	resolver := engine.resolver(true, false)
	resolver.clock = &pollClock{}
	defer resolver.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result := gracefulStopWithin(t, ctx, resolver, confirmedEngine())
	if result.Success || !strings.Contains(result.Message, "still in use") || strings.Contains(result.Message, "--force") {
		t.Fatalf("result = %+v, want the deadline without --force advice", result)
	}
}

func TestGracefulStopWaitEndsWithTheContext(t *testing.T) {
	engine := newFakeEngine(t, 41)
	resolver := engine.resolver(false, true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resolver.clock = cancelOnWaitClock{cancel: cancel}
	defer resolver.Close()

	result := gracefulStopWithin(t, ctx, resolver, confirmedEngine())
	if result.Success || !strings.Contains(result.Message, "Stopped waiting for port") {
		t.Fatalf("result = %+v, want the cancelled wait reported", result)
	}
}

// blockedClock is a clock at a fixed time whose waits never end.
type blockedClock struct{}

func (blockedClock) Now() time.Time                       { return time.Unix(0, 0) }
func (blockedClock) Sleep(time.Duration)                  {}
func (blockedClock) After(time.Duration) <-chan time.Time { return nil }

// cancelOnWaitClock cancels when a wait begins, and the wait itself never fires,
// so only the context can end it.
type cancelOnWaitClock struct {
	blockedClock
	cancel context.CancelFunc
}

func (c cancelOnWaitClock) After(time.Duration) <-chan time.Time {
	c.cancel()
	return nil
}

// testBound turns a wait that never ends into a named failure.
const testBound = 20 * time.Second

func within[T any](t *testing.T, call func() T) T {
	t.Helper()
	done := make(chan T, 1)
	go func() { done <- call() }()
	select {
	case value := <-done:
		return value
	case <-time.After(testBound):
		t.Fatalf("no result within %s", testBound)
		panic("unreachable")
	}
}

func gracefulStopWithin(t *testing.T, ctx context.Context, resolver *ConflictResolver, info *ServiceInfo) *ResolutionResult {
	t.Helper()
	return within(t, func() *ResolutionResult { return resolver.executeGracefulStop(ctx, info) })
}

// A refused shutdown is a failure while the port is open, and no wait follows.
func TestGracefulStopReportsARefusedShutdown(t *testing.T) {
	engine := newFakeEngine(t, 41)
	engine.shutdownStatus = http.StatusInternalServerError
	resolver := engine.resolver(false, true)
	clock := &pollClock{}
	resolver.clock = clock
	defer resolver.Close()

	result := resolver.executeGracefulStop(context.Background(), confirmedEngine())
	if result.Success || !strings.Contains(result.Message, "The request to shut down the SPT engine failed") {
		t.Fatalf("result = %+v, want the refused shutdown reported", result)
	}
	if clock.polls != 0 {
		t.Errorf("polls = %d, want 0", clock.polls)
	}
}

// A run that replaces the reported one is neither stopped nor shut down.
func TestGracefulStopLeavesAReplacementRunAlone(t *testing.T) {
	engine := newFakeEngine(t, 41)
	engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
		e.runID = 42
		w.WriteHeader(http.StatusNotFound)
	}
	resolver := engine.resolver(false, true)
	defer resolver.Close()

	result := gracefulStopWithin(t, context.Background(), resolver, confirmedEngine())
	if result.Success || !strings.Contains(result.Message, "replaced by run 42") {
		t.Fatalf("result = %+v, want the replacement reported", result)
	}
	if slices.Contains(engine.received(), engineRequest{http.MethodPost, "/shutdown", ""}) {
		t.Error("engine running the replacement run was shut down")
	}
}
