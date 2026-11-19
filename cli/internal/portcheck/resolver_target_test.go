/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// answerAfter is prompt input that runs change, as if the world moved while the
// prompt was open, and then gives the answer.
type answerAfter struct {
	change func()
	answer *strings.Reader
}

func newAnswerAfter(answer string, change func()) *answerAfter {
	return &answerAfter{change: change, answer: strings.NewReader(answer)}
}

func (a *answerAfter) Read(p []byte) (int, error) {
	if a.change != nil {
		a.change()
		a.change = nil
	}
	return a.answer.Read(p)
}

func (e *fakeEngine) setRun(runID int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.runID = runID
}

func changesTo(requests []engineRequest) []engineRequest {
	var changes []engineRequest
	for _, request := range requests {
		if request.method != http.MethodGet && request.method != http.MethodHead {
			changes = append(changes, request)
		}
	}
	return changes
}

var shutdownRequest = engineRequest{http.MethodPost, "/shutdown", ""}

// g covers the run the prompt showed. A different run found afterwards is not
// stopped, and its engine is not shut down.
func TestPromptAnswerStopsOnlyTheRunShown(t *testing.T) {
	tests := []struct {
		name    string
		shown   int64
		current int64
		want    string
	}{
		{"another run replaced the one shown", 41, 42, "run 42 replaced the run shown, 41"},
		{"a run started on the idle engine shown", 0, 42, "run 42 started after"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := newFakeEngine(t, tt.shown)
			resolver := engine.resolver(false, true)
			resolver.input = newAnswerAfter("g\n", func() { engine.setRun(tt.current) })
			defer resolver.Close()

			result, err := resolver.CheckAndResolveConflicts(context.Background())
			if err != nil {
				t.Fatalf("CheckAndResolveConflicts() error = %v", err)
			}
			if result.Success || !strings.Contains(result.Message, tt.want) {
				t.Fatalf("result = %+v, want %q", result, tt.want)
			}
			assertReadOnly(t, engine.received())
		})
	}
}

// The run shown may end by itself while the prompt is open; the operator still
// asked for the engine to go.
func TestPromptAnswerShutsDownWhenTheShownRunEnded(t *testing.T) {
	engine := newFakeEngine(t, 41)
	resolver := engine.resolver(false, true)
	resolver.input = newAnswerAfter("g\n", func() { engine.setRun(0) })
	resolver.clock = closeAfterPolls(engine, 1)
	defer resolver.Close()

	result, err := resolver.CheckAndResolveConflicts(context.Background())
	if err != nil || !result.Success {
		t.Fatalf("CheckAndResolveConflicts() = %+v, %v, want success", result, err)
	}
	if got, want := changesTo(engine.received()), []engineRequest{shutdownRequest}; !slices.Equal(got, want) {
		t.Errorf("engine changes = %+v, want %+v", got, want)
	}
}

// A run that becomes active after the stop keeps its engine, by either route.
func TestNoShutdownWhenAnotherRunBecameActive(t *testing.T) {
	tests := []struct {
		name    string
		force   bool
		startID int64
		prepare func(*fakeEngine)
	}{
		{
			name: "g: after the accepted stop", startID: 41,
			prepare: func(e *fakeEngine) {
				e.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
					e.runID = 42
					w.WriteHeader(http.StatusOK)
				}
			},
		},
		{
			name: "--force: after the accepted stop", force: true, startID: 41,
			prepare: func(e *fakeEngine) {
				e.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
					e.runID = 42
					w.WriteHeader(http.StatusOK)
				}
			},
		},
		{
			name: "--force: after the engine was found idle", force: true,
			prepare: func(e *fakeEngine) {
				heads := 0
				e.onHead = func(e *fakeEngine) {
					if heads++; heads == 2 {
						e.runID = 42
					}
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := newFakeEngine(t, tt.startID)
			tt.prepare(engine)
			resolver := engine.resolver(tt.force, !tt.force)
			defer resolver.Close()
			info := confirmedEngine()
			info.Status.RunID = fmt.Sprint(tt.startID)

			result := resolver.executeGracefulStop(context.Background(), info)
			if result.Success || !strings.Contains(result.Message, "run 42 is active") {
				t.Fatalf("result = %+v, want the other run reported", result)
			}
			if slices.Contains(engine.received(), shutdownRequest) {
				t.Error("the engine running the other run was shut down")
			}
		})
	}
}

// A stopped run stays active until its cleanup returns; that is not another run.
func TestShutdownFollowsAStoppedRunStillCleaningUp(t *testing.T) {
	engine := newFakeEngine(t, 41)
	engine.onDelete = func(_ *fakeEngine, w http.ResponseWriter) { w.WriteHeader(http.StatusOK) }
	resolver := engine.resolver(false, true)
	resolver.clock = closeAfterPolls(engine, 1)
	defer resolver.Close()

	result := resolver.executeGracefulStop(context.Background(), confirmedEngine())
	if !result.Success {
		t.Fatalf("result = %+v, want success", result)
	}
	if !slices.Contains(engine.received(), shutdownRequest) {
		t.Error("the engine was not shut down")
	}
}

// A daemon that is not reached through a local socket has another host network,
// or may have: its host-network containers are not this port's holder.
func TestForceIgnoresHostNetworkEnginesOfARemoteDaemon(t *testing.T) {
	for _, host := range []string{"tcp://10.0.0.5:2375", "tcp://127.0.0.1:2375", "ssh://user@host", "npipe:////server/pipe/docker_engine"} {
		t.Run(host, func(t *testing.T) {
			engine := failingEngine(t)
			api := fakeDockerWith(hostEngine("same-port-elsewhere", engine.port()))
			api.daemonHost = host
			resolver := engine.resolver(true, false)
			resolver.dockerFinder = newFakeFinder(api)
			defer resolver.Close()

			result, err := resolver.CheckAndResolveConflicts(context.Background())
			if err != nil {
				t.Fatalf("CheckAndResolveConflicts() error = %v", err)
			}
			if result.Success || !strings.Contains(result.Message, "not reached through a local socket") {
				t.Fatalf("result = %+v, want the remote daemon reported", result)
			}
			if stopped, removed := api.mutations(); len(stopped) != 0 || len(removed) != 0 {
				t.Errorf("stopped %v, removed %v; want none", stopped, removed)
			}
		})
	}
}

// listenOnLoopbackAlias holds port on 127.0.0.2, where a connection to 127.0.0.1
// is refused although the port cannot be bound.
func listenOnLoopbackAlias(t *testing.T, port string) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", net.JoinHostPort("127.0.0.2", port))
	if err != nil {
		t.Skipf("no 127.0.0.2 listener on this host: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

func TestIsPortHeldSeesAListenerThatRefusesTheProbe(t *testing.T) {
	listener := listenOnLoopbackAlias(t, "0")
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	checker := NewPortCheckerWithOptions(port, "127.0.0.1", time.Second, time.Second, 1)
	if checker.IsPortInUse() {
		t.Fatal("IsPortInUse() = true; the test needs a refused connection")
	}
	if !checker.IsPortHeld(context.Background()) {
		t.Error("IsPortHeld() = false while the port is bound")
	}
	_ = listener.Close()
	if checker.IsPortHeld(context.Background()) {
		t.Error("IsPortHeld() = true after the listener closed")
	}
}

func TestDialFailureMeansHeld(t *testing.T) {
	timedOut := &net.OpError{Op: "dial", Err: os.ErrDeadlineExceeded}
	refused := &net.OpError{Op: "dial", Err: syscall.ECONNREFUSED}
	if !dialFailureMeansHeld(timedOut) {
		t.Error("an unanswered connection counted as no listener")
	}
	if dialFailureMeansHeld(refused) {
		t.Error("a refused connection counted as a listener")
	}
}

// Success waits until nothing holds the port, not until connections fail.
func TestGracefulStopWaitsForEveryListener(t *testing.T) {
	engine := newFakeEngine(t, 41)
	other := listenOnLoopbackAlias(t, engine.port())
	resolver := engine.resolver(false, true)
	clock := &pollClock{onPoll: func(poll int) {
		switch poll {
		case 1:
			engine.server.Close()
		case 3:
			_ = other.Close()
		}
	}}
	resolver.clock = clock
	defer resolver.Close()

	result := resolver.executeGracefulStop(context.Background(), confirmedEngine())
	if !result.Success {
		t.Fatalf("result = %+v, want success", result)
	}
	if clock.polls != 3 {
		t.Errorf("polls = %d, want 3", clock.polls)
	}
}

// blockingInput is a terminal nobody answers.
type blockingInput struct {
	reading chan struct{}
	release chan struct{}
}

func newBlockingInput(t *testing.T) *blockingInput {
	t.Helper()
	input := &blockingInput{reading: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(func() { close(input.release) })
	return input
}

func (b *blockingInput) Read([]byte) (int, error) {
	close(b.reading)
	<-b.release
	return 0, io.EOF
}

// Ctrl-C or SIGTERM ends an unanswered prompt and leaves the engine alone.
func TestPromptEndsWithTheContext(t *testing.T) {
	engine := newFakeEngine(t, 41)
	resolver := engine.resolver(false, true)
	input := newBlockingInput(t)
	resolver.input = input
	defer resolver.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		<-input.reading
		cancel()
	}()

	err := within(t, func() error {
		_, err := resolver.CheckAndResolveConflicts(ctx)
		return err
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	assertReadOnly(t, engine.received())
}

// The wait after a force stop ends with the context, without the verification.
func TestForceVerificationWaitEndsWithTheContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	engine := newFakeEngine(t, 41)
	api := fakeDockerWith(publishedEngine(t, "holder", "127.0.0.1", engine.port()))
	api.onRemove = func(string) { cancel() }
	resolver := engine.resolver(true, false)
	resolver.dockerFinder = newFakeFinder(api)
	resolver.clock = blockedClock{}
	defer resolver.Close()

	result := within(t, func() *ResolutionResult {
		return resolver.executeForceStop(ctx, &ServiceInfo{IsListening: true, IsSpt: true, APIConfirmed: true})
	})
	if result.Success || !errors.Is(result.Error, context.Canceled) || !strings.Contains(result.Message, "Stopped waiting") {
		t.Fatalf("result = %+v, want the cancelled wait", result)
	}
}

// After Ctrl-C during the graceful part, --force touches no container.
func TestForceRemovesNothingAfterCancellation(t *testing.T) {
	engine := newFakeEngine(t, 41)
	api := fakeDockerWith(hostEngine("holder", engine.port()))
	resolver := engine.resolver(true, false)
	resolver.dockerFinder = newFakeFinder(api)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resolver.clock = cancelOnWaitClock{cancel: cancel}
	defer resolver.Close()

	result := within(t, func() *ResolutionResult {
		result, _ := resolver.executeForceResolution(ctx, confirmedEngine())
		return result
	})
	if result.Success {
		t.Fatalf("result = %+v, want failure", result)
	}
	if stopped, removed := api.mutations(); len(stopped) != 0 || len(removed) != 0 {
		t.Errorf("stopped %v, removed %v; want none", stopped, removed)
	}
}
