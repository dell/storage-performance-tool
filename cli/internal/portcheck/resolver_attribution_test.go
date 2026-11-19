/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	containertypes "github.com/docker/docker/api/types/container"
)

// captureStdout returns what call prints.
func captureStdout(t *testing.T, call func()) string {
	t.Helper()
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	saved := os.Stdout
	os.Stdout = writer
	printed := make(chan string, 1)
	go func() {
		var out bytes.Buffer
		_, _ = io.Copy(&out, reader)
		printed <- out.String()
	}()
	defer func() {
		os.Stdout = saved
	}()
	call()
	_ = writer.Close()
	os.Stdout = saved
	return <-printed
}

// unrelatedPublisher is a running container with no SPT labels that publishes port.
func unrelatedPublisher(t *testing.T, port string) engineContainer {
	return engineContainer{
		id: "unrelated", state: containertypes.StateRunning,
		ports: []containertypes.Port{{IP: "0.0.0.0", PublicPort: listenerPort(t, port), PrivatePort: 80, Type: "tcp"}},
	}
}

func withDocker(resolver *ConflictResolver, api *fakeDockerAPI) {
	resolver.dockerFinder = newFakeFinder(api)
	resolver.checker.newFinder = func() (*DockerContainerFinder, error) { return newFakeFinder(api), nil }
}

// When a replacement run keeps the engine up, --force does not remove a container
// only because it publishes the port: it is not an SPT engine container.
func TestForceLeavesAnUnrelatedPublisherAfterAReplacementRun(t *testing.T) {
	engine := newFakeEngine(t, 41)
	engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
		e.runID = 42
		w.WriteHeader(http.StatusNotFound)
	}
	api := fakeDockerWith(unrelatedPublisher(t, engine.port()))
	resolver := engine.resolver(true, false)
	withDocker(resolver, api)
	defer resolver.Close()

	result := within(t, func() *ResolutionResult {
		result, _ := resolver.CheckAndResolveConflicts(context.Background())
		return result
	})
	if result.Success || !strings.Contains(result.Message, "not an SPT engine container") {
		t.Fatalf("result = %+v, want the publisher left alone", result)
	}
	if stopped, removed := api.mutations(); len(stopped) != 0 || len(removed) != 0 {
		t.Errorf("stopped %v, removed %v; want none", stopped, removed)
	}
}

// After the engine shut down, a port that only cannot be bound yet names no
// holder: --force removes nothing, not even the engine's own container.
func TestForceRemovesNothingWhenNothingListens(t *testing.T) {
	engine := newFakeEngine(t, 41)
	// Something the probe cannot reach keeps the port from being bound.
	listenOnLoopbackAlias(t, engine.port())
	api := fakeDockerWith(hostEngine("holder", engine.port()))
	resolver := engine.resolver(true, false)
	withDocker(resolver, api)
	resolver.clock = closeAfterPolls(engine, 1)
	defer resolver.Close()

	result := within(t, func() *ResolutionResult {
		result, _ := resolver.executeForceResolution(context.Background(), confirmedEngine())
		return result
	})
	if result.Success || !strings.Contains(result.Message, "no container was removed") {
		t.Fatalf("result = %+v, want nothing removed", result)
	}
	if !strings.Contains(result.Message, "cannot be bound yet") {
		t.Errorf("Message = %q, want the unbindable port explained", result.Message)
	}
	if stopped, removed := api.mutations(); len(stopped) != 0 || len(removed) != 0 {
		t.Errorf("stopped %v, removed %v; want none", stopped, removed)
	}
}

// --force still removes the labelled engine container that publishes the port
// when its run cannot be stopped.
func TestForceRemovesThePublishingEngineContainer(t *testing.T) {
	engine := failingEngine(t)
	holder := unrelatedPublisher(t, engine.port())
	holder.id, holder.labels = "engine", nodeLabels()
	api := fakeDockerWith(holder)
	api.onRemove = func(string) { engine.server.Close() }
	resolver := engine.resolver(true, false)
	withDocker(resolver, api)
	defer resolver.Close()

	result := within(t, func() *ResolutionResult {
		result, _ := resolver.CheckAndResolveConflicts(context.Background())
		return result
	})
	if !result.Success {
		t.Fatalf("result = %+v, want success", result)
	}
	if _, removed := api.mutations(); !slices.Equal(removed, []string{"engine"}) {
		t.Errorf("removed = %v, want the engine container", removed)
	}
}

func TestDaemonIsLocal(t *testing.T) {
	tests := []struct {
		host  string
		local bool
	}{
		{"unix:///var/run/docker.sock", true},
		{"UNIX:///var/run/docker.sock", true},
		{"npipe:////./pipe/docker_engine", true},
		{"npipe:////server/pipe/docker_engine", false},
		{"npipe://server/pipe/docker_engine", false},
		{"tcp://localhost:2375", false},
		{"tcp://127.0.0.1:2375", false},
		{"ssh://user@host", false},
		{"fd://", false},
		{"/var/run/docker.sock", false},
	}
	for _, tt := range tests {
		t.Run(tt.host, func(t *testing.T) {
			finder := newFakeFinder(&fakeDockerAPI{daemonHost: tt.host})
			if got := finder.DaemonIsLocal(); got != tt.local {
				t.Errorf("DaemonIsLocal() = %v, want %v", got, tt.local)
			}
		})
	}
}

// A container detection found is not removed through a daemon that may be remote.
func TestForceKeepsADetectedContainerOfARemoteDaemon(t *testing.T) {
	holder := newLookalikeHolder(t)
	holder.docker.daemonHost = "npipe:////server/pipe/docker_engine"
	resolver := holder.resolver(true, false)
	defer resolver.Close()

	result, err := resolver.CheckAndResolveConflicts(context.Background())
	if err != nil {
		t.Fatalf("CheckAndResolveConflicts() error = %v", err)
	}
	if result.Success || !strings.Contains(result.Message, "may not be local") {
		t.Fatalf("result = %+v, want the remote daemon reported", result)
	}
	holder.assertUntouched(t)
}

const (
	firstStart = "2026-11-19T10:00:00Z"
	laterStart = "2026-11-19T10:05:00Z"
)

// Detection keeps the run's start time, which tells a later run with the same ID apart.
func TestDetectionKeepsTheRunStart(t *testing.T) {
	engine := newFakeEngine(t, 41)
	engine.since = firstStart
	resolver := engine.resolver(false, false)
	defer resolver.Close()

	info, err := resolver.checkService(context.Background())
	if err != nil {
		t.Fatalf("checkService() error = %v", err)
	}
	if info.Status == nil || info.Status.RunningSince != firstStart {
		t.Fatalf("status = %+v, want the run start %s", info.Status, firstStart)
	}
	if note := runIdentityNote(info.Status); note != "" {
		t.Errorf("note = %q, want none with a start time", note)
	}
}

// Without a start time, the prompt says a reused ID cannot be told apart.
func TestPromptNotesARunWithoutAStartTime(t *testing.T) {
	note := runIdentityNote(&SptStatus{RunID: "41", RunActive: true})
	if !strings.Contains(note, "cannot tell it apart") || !strings.Contains(note, "reuses ID 41") {
		t.Errorf("note = %q, want the limit stated", note)
	}
	if note := runIdentityNote(&SptStatus{RunID: "41"}); note != "" {
		t.Errorf("note = %q for no active run, want none", note)
	}
}

// A later run that reuses the shown run's ID is not stopped.
func TestPromptAnswerLeavesALaterRunWithTheSameID(t *testing.T) {
	engine := newFakeEngine(t, 41)
	engine.since = firstStart
	resolver := engine.resolver(false, true)
	resolver.input = newAnswerAfter("g\n", func() {
		engine.mu.Lock()
		engine.since = laterStart
		engine.mu.Unlock()
	})
	defer resolver.Close()

	result, err := resolver.CheckAndResolveConflicts(context.Background())
	if err != nil {
		t.Fatalf("CheckAndResolveConflicts() error = %v", err)
	}
	if result.Success || !strings.Contains(result.Message, "reuses the ID of the run shown") {
		t.Fatalf("result = %+v, want the later run reported", result)
	}
	assertReadOnly(t, engine.received())
}

// A later run that takes the stopped run's ID keeps its engine, by either route.
func TestNoShutdownWhenALaterRunReusesTheStoppedID(t *testing.T) {
	for _, force := range []bool{false, true} {
		name := "prompt"
		if force {
			name = "force"
		}
		t.Run(name, func(t *testing.T) {
			engine := newFakeEngine(t, 41)
			engine.since = firstStart
			engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
				e.since = laterStart
				w.WriteHeader(http.StatusOK)
			}
			resolver := engine.resolver(force, !force)
			defer resolver.Close()
			info := confirmedEngine()
			info.Status.RunningSince = firstStart

			result := gracefulStopWithin(t, context.Background(), resolver, info)
			if result.Success || !strings.Contains(result.Message, "later run that reuses its ID") {
				t.Fatalf("result = %+v, want the later run reported", result)
			}
			if slices.Contains(engine.received(), shutdownRequest) {
				t.Error("the engine running the later run was shut down")
			}
			// Under --force the container may be next, so it is not called left running.
			if left := strings.Contains(result.Message, "left running"); left == force {
				t.Errorf("Message = %q; left running stated = %v under force %v", result.Message, left, force)
			}
		})
	}
}

// A worker answers the stop without ending the run; the message says so.
func TestWorkerStopIsReportedAsARequest(t *testing.T) {
	engine := newFakeEngine(t, 41)
	engine.worker = true
	engine.onDelete = func(_ *fakeEngine, w http.ResponseWriter) { w.WriteHeader(http.StatusOK) }
	resolver := engine.resolver(false, true)
	resolver.clock = closeAfterPolls(engine, 1)
	defer resolver.Close()
	info := confirmedEngine()
	info.Status.Worker = true

	var result *ResolutionResult
	printed := captureStdout(t, func() { result = gracefulStopWithin(t, context.Background(), resolver, info) })
	if !result.Success {
		t.Fatalf("result = %+v, want success", result)
	}
	if !strings.Contains(printed, "Asked the engine to stop run 41; this node is a worker of that run") {
		t.Errorf("printed %q, want the worker named", printed)
	}
	if strings.Contains(printed, "Stopped SPT run") {
		t.Errorf("printed %q, which claims the run stopped", printed)
	}
}

// unansweredAPI accepts connections and never answers.
func unansweredAPI(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			t.Cleanup(func() { _ = conn.Close() })
		}
	}()
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	return port
}

// Ctrl-C during the check after a force stop ends it at once.
func TestForceVerificationEndsWithTheContext(t *testing.T) {
	port := unansweredAPI(t)
	checker := NewPortCheckerWithOptions(port, "127.0.0.1", time.Second, time.Minute, 1)
	api := &fakeDockerAPI{}
	checker.newFinder = func() (*DockerContainerFinder, error) { return newFakeFinder(api), nil }
	resolver := &ConflictResolver{Port: port, checker: checker, dockerFinder: newFakeFinder(api), clock: &pollClock{}}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	result := within(t, func() *ResolutionResult { return resolver.verifyResolution(ctx, StrategyForceStop) })
	if result.Success || !strings.Contains(result.Message, "Interrupted") || !errors.Is(result.Error, context.DeadlineExceeded) {
		t.Fatalf("result = %+v, want the interruption reported", result)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("verification took %s after a 200ms deadline", elapsed)
	}
}

// g is offered only where the SPT API answered.
func TestMenuOffersGracefulOnlyWithAnAPI(t *testing.T) {
	if menu := resolutionOptions(confirmedEngine()); !strings.Contains(menu, "[g] Graceful stop") {
		t.Errorf("menu for an engine = %q, want g", menu)
	}
	lookalike := &ServiceInfo{IsListening: true, IsSpt: true, Container: &ContainerInfo{ID: "dashboard"}}
	menu := resolutionOptions(lookalike)
	if strings.Contains(menu, "[g]") || !strings.Contains(menu, "1) [f] Force stop") {
		t.Errorf("menu for a lookalike = %q, want option 1 to be force", menu)
	}
}

// For a lookalike, --force goes to the container without trying the API.
func TestForceTriesNoGracefulStopOnALookalike(t *testing.T) {
	holder := newLookalikeHolder(t)
	resolver := holder.resolver(true, false)
	defer resolver.Close()

	printed := captureStdout(t, func() {
		if _, err := resolver.CheckAndResolveConflicts(context.Background()); err != nil {
			t.Errorf("CheckAndResolveConflicts() error = %v", err)
		}
	})
	if strings.Contains(printed, "graceful") || strings.Contains(printed, "Graceful") {
		t.Errorf("printed %q, want no graceful attempt", printed)
	}
}

// Option 1 on a lookalike is the force stop the menu shows.
func TestOptionOneForceStopsALookalike(t *testing.T) {
	holder := newLookalikeHolder(t)
	resolver := holder.resolver(false, true)
	resolver.input = strings.NewReader("1\n")
	defer resolver.Close()

	printed := captureStdout(t, func() {
		if _, err := resolver.CheckAndResolveConflicts(context.Background()); err != nil {
			t.Errorf("CheckAndResolveConflicts() error = %v", err)
		}
	})
	if strings.Contains(printed, "Spt Test Details") {
		t.Errorf("printed %q, want no SPT test details without an SPT API", printed)
	}
	holder.assertNoAPIChange(t)
	if _, removed := holder.docker.mutations(); !slices.Equal(removed, []string{"dashboard"}) {
		t.Errorf("removed = %v, want the publishing container", removed)
	}
}

// When the re-check before shutdown fails because the engine went away, the port
// is free and the stop succeeds without a shutdown.
func TestFailedRecheckWithAFreePortSucceeds(t *testing.T) {
	engine := newFakeEngine(t, 41)
	heads := 0
	engine.onHead = func(e *fakeEngine) {
		heads++
		if heads == 2 {
			_ = e.server.Listener.Close()
			e.headStatus = http.StatusInternalServerError
		}
	}
	resolver := engine.resolver(false, true)
	defer resolver.Close()

	result := gracefulStopWithin(t, context.Background(), resolver, confirmedEngine())
	if !result.Success {
		t.Fatalf("result = %+v, want success", result)
	}
	if slices.Contains(engine.received(), shutdownRequest) {
		t.Error("shutdown sent after the re-check failed")
	}
}

// A port held only where the probe cannot connect is not free after a force stop.
func TestVerificationSeesAListenerThatRefusesTheProbe(t *testing.T) {
	listener := listenOnLoopbackAlias(t, "0")
	_, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	checker := NewPortCheckerWithOptions(port, "127.0.0.1", time.Second, time.Second, 1)
	resolver := &ConflictResolver{Port: port, checker: checker, clock: &pollClock{}}

	if result := resolver.verifyResolution(context.Background(), StrategyForceStop); result.Success {
		t.Fatalf("result = %+v, want the held port reported", result)
	}
}

// A refused shutdown fails at once while anything holds the port, even where the
// probe can no longer connect.
func TestRefusedShutdownSeesAListenerThatRefusesTheProbe(t *testing.T) {
	engine := newFakeEngine(t, 41)
	listenOnLoopbackAlias(t, engine.port())
	engine.shutdownStatus = http.StatusInternalServerError
	engine.onShutdown = func(e *fakeEngine) { _ = e.server.Listener.Close() }
	resolver := engine.resolver(false, true)
	clock := &pollClock{}
	resolver.clock = clock
	defer resolver.Close()

	result := gracefulStopWithin(t, context.Background(), resolver, confirmedEngine())
	if result.Success || !strings.Contains(result.Message, "Failed to shut down the SPT engine") {
		t.Fatalf("result = %+v, want the refused shutdown reported", result)
	}
	if clock.polls != 0 {
		t.Errorf("polls = %d, want 0", clock.polls)
	}
}

// An active run counts as a running test whatever /status says of its state.
func TestActiveRunIsARunningTest(t *testing.T) {
	info := &ServiceInfo{IsSpt: true, Status: &SptStatus{State: "STOPPED", RunActive: true}}
	if !info.IsTestActive() {
		t.Error("IsTestActive() = false for an active run in cleanup")
	}
}

// answeredAtCancel is a context whose cancellation is seen only through Err, as
// when the answer and Ctrl-C arrive together and the answer is read first.
type answeredAtCancel struct {
	context.Context
	cancelled chan struct{}
}

func (c *answeredAtCancel) Done() <-chan struct{} { return nil }
func (c *answeredAtCancel) Err() error {
	select {
	case <-c.cancelled:
		return context.Canceled
	default:
		return nil
	}
}

// An answer read as the command is cancelled is not acted on.
func TestAnswerReadAtCancellationAborts(t *testing.T) {
	engine := newFakeEngine(t, 41)
	resolver := engine.resolver(false, true)
	ctx := &answeredAtCancel{Context: context.Background(), cancelled: make(chan struct{})}
	resolver.input = newAnswerAfter("g\n", func() { close(ctx.cancelled) })
	defer resolver.Close()

	err := within(t, func() error {
		_, err := resolver.CheckAndResolveConflicts(ctx)
		return err
	})
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
	if got := changesTo(engine.received()); len(got) != 0 {
		t.Errorf("engine changes = %+v, want none", got)
	}
}
