/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	containertypes "github.com/docker/docker/api/types/container"
)

// setContainers replaces the containers the fake daemon lists.
func (f *fakeDockerAPI) setContainers(containers ...containertypes.Summary) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.containers = containers
}

// publisher is a running container with no SPT labels that publishes port on ip.
func publisher(t *testing.T, id, image, ip, port string) containertypes.Summary {
	return containertypes.Summary{
		ID: id, Names: []string{"/" + id}, Image: image, State: containertypes.StateRunning,
		Ports: []containertypes.Port{{IP: ip, PublicPort: listenerPort(t, port), PrivatePort: 80, Type: "tcp"}},
	}
}

func assertNoDockerChange(t *testing.T, api *fakeDockerAPI) {
	t.Helper()
	if stopped, removed := api.mutations(); len(stopped) != 0 || len(removed) != 0 {
		t.Errorf("stopped %v, removed %v; want none", stopped, removed)
	}
}

// Fable P6: two containers publish the port number, one on another address. The
// container shown is the one on the address that answered, and f and --force
// remove that one and no other.
func TestShownContainerIsTheOneRemoved(t *testing.T) {
	for _, mode := range []string{"prompt f", "no terminal, then --force"} {
		t.Run(mode, func(t *testing.T) {
			holder := newLookalikeHolder(t)
			holder.docker.setContainers(
				publisher(t, "shown-scratch", "spt-scratch", "127.0.0.2", holder.port),
				publisher(t, "database", "postgres:17", "127.0.0.1", holder.port),
			)
			var printed string
			if mode == "prompt f" {
				resolver := holder.resolver(false, true)
				resolver.input = strings.NewReader("f\n")
				printed = captureStdout(t, func() { _, _ = resolver.CheckAndResolveConflicts(context.Background()) })
				_ = resolver.Close()
			} else {
				resolver := holder.resolver(false, false)
				printed = captureStdout(t, func() { _, _ = resolver.CheckAndResolveConflicts(context.Background()) })
				_ = resolver.Close()
				if _, removed := holder.docker.mutations(); len(removed) != 0 {
					t.Fatalf("removed %v without --force", removed)
				}
				resolver = holder.resolver(true, false)
				printed += captureStdout(t, func() { _, _ = resolver.CheckAndResolveConflicts(context.Background()) })
				_ = resolver.Close()
			}
			if strings.Contains(printed, "shown-scratch") || !strings.Contains(printed, "database") {
				t.Errorf("printed %q, want database shown and shown-scratch not", printed)
			}
			if _, removed := holder.docker.mutations(); !slices.Equal(removed, []string{"database"}) {
				t.Errorf("removed %v, want only the container shown, database", removed)
			}
		})
	}
}

// Sol's prompt-swap: while the prompt is open, the container shown is replaced by
// another on the same endpoint, or a container appears where none was shown. f
// removes neither.
func TestPromptForceLeavesAContainerThatWasNotShown(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{"replaced", "changed since it was shown (shown: shown-container; now: replacement-container)"},
		{"appeared", "changed since it was shown (shown: none; now: late-container)"},
		{"gone", "changed since it was shown (shown: shown-container; now: no container publishes 127.0.0.1:"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			holder := newLookalikeHolder(t)
			later := "late-container"
			holder.docker.setContainers()
			if tt.name != "appeared" {
				holder.docker.setContainers(publisher(t, "shown-container", "web", "0.0.0.0", holder.port))
				later = "replacement-container"
			}
			resolver := holder.resolver(false, true)
			defer resolver.Close()
			resolver.input = newAnswerAfter("f\n", func() {
				if tt.name == "gone" {
					holder.docker.setContainers()
					return
				}
				holder.docker.setContainers(publisher(t, later, "web", "0.0.0.0", holder.port))
			})

			var result *ResolutionResult
			captureStdout(t, func() { result, _ = resolver.CheckAndResolveConflicts(context.Background()) })
			if result.Success || !strings.Contains(result.Message, tt.want) ||
				!strings.Contains(result.Message, "nothing was removed. Run the command again") {
				t.Fatalf("result = %+v, want %q", result, tt.want)
			}
			assertNoDockerChange(t, holder.docker)
		})
	}
}

// Two containers whose bindings both cover the address that answered: the prompt
// shows neither as the holder, and f removes neither.
func TestPromptShowsNoneOfTwoCoveringPublishers(t *testing.T) {
	holder := newLookalikeHolder(t)
	holder.docker.setContainers(
		publisher(t, "first", "web", "0.0.0.0", holder.port),
		publisher(t, "second", "web", "127.0.0.1", holder.port),
	)
	resolver := holder.resolver(false, true)
	defer resolver.Close()
	resolver.input = strings.NewReader("f\n")

	var result *ResolutionResult
	printed := captureStdout(t, func() { result, _ = resolver.CheckAndResolveConflicts(context.Background()) })
	if strings.Contains(printed, "Container Details") || !strings.Contains(printed, "Containers first, second all publish") {
		t.Errorf("printed %q, want both named and neither shown as the holder", printed)
	}
	if result.Success || !strings.Contains(result.Message, "Containers first, second all publish 127.0.0.1:"+holder.port) {
		t.Errorf("result = %+v, want the two publishers refused", result)
	}
	assertNoDockerChange(t, holder.docker)
}

// Sol's lost-after-stop and recovered cases, Fable P2, and the other refusals of
// the check before the shutdown: when the graceful step refuses because the run
// cannot be identified, --force sends no Docker request either. The engine's
// container was shown, so the message names it.
func TestForceRemovesNothingAfterAnIdentityRefusal(t *testing.T) {
	tests := []struct {
		name     string
		onDelete func(*fakeEngine, http.ResponseWriter)
		want     string
	}{
		{"status lost after the stop", func(e *fakeEngine, w http.ResponseWriter) {
			e.statusCode = http.StatusServiceUnavailable
			w.WriteHeader(http.StatusOK)
		}, "could not be read"},
		{"later run with the same ID", func(e *fakeEngine, w http.ResponseWriter) {
			e.since = laterStart
			w.WriteHeader(http.StatusOK)
		}, "is a later run that reuses the ID"},
		{"run replaced at the stop", func(e *fakeEngine, w http.ResponseWriter) {
			e.runID = 42
			w.WriteHeader(http.StatusNotFound)
		}, "replaced by run 42"},
		{"another run after the stop", func(e *fakeEngine, w http.ResponseWriter) {
			e.runID = 42
			w.WriteHeader(http.StatusOK)
		}, "run 42 is active on the engine"},
		{"active run unreadable after the stop", func(e *fakeEngine, w http.ResponseWriter) {
			e.headStatus = http.StatusInternalServerError
			w.WriteHeader(http.StatusOK)
		}, "could not be read again"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := newFakeEngine(t, 41)
			engine.since = firstStart
			engine.onDelete = tt.onDelete
			api := fakeDockerWith(publishedEngine(t, "bridge-engine", "0.0.0.0", engine.port()))
			api.onRemove = func(string) { engine.server.Close() }
			resolver := engine.resolver(true, false)
			withDocker(resolver, api)
			defer resolver.Close()

			var result *ResolutionResult
			printed := captureStdout(t, func() {
				result = within(t, func() *ResolutionResult {
					result, _ := resolver.CheckAndResolveConflicts(context.Background())
					return result
				})
			})
			if result.Success || !strings.Contains(result.Message, tt.want) ||
				!strings.Contains(result.Message, "No container was removed") ||
				!strings.Contains(result.Message, "docker rm -f bridge-engine") {
				t.Fatalf("result = %+v, want %q and nothing removed", result, tt.want)
			}
			if strings.Contains(printed, "Graceful stop failed, trying force stop") {
				t.Errorf("printed %q, want no force stop", printed)
			}
			assertNoDockerChange(t, api)
			if slices.Contains(engine.received(), shutdownRequest) {
				t.Error("the engine was shut down")
			}
		})
	}
}

// What still falls back: the identified run was stopped and its engine asked to
// shut down, and the port is still held when the wait ends. --force then removes
// the engine container shown.
func TestForceRemovesTheShownEngineAfterALingeringShutdown(t *testing.T) {
	engine := newFakeEngine(t, 41)
	engine.since = firstStart
	api := fakeDockerWith(publishedEngine(t, "engine", "127.0.0.1", engine.port()))
	api.onRemove = func(string) { engine.server.Close() }
	resolver := engine.resolver(true, false)
	withDocker(resolver, api)
	defer resolver.Close()

	var result *ResolutionResult
	printed := captureStdout(t, func() {
		result = within(t, func() *ResolutionResult {
			result, _ := resolver.CheckAndResolveConflicts(context.Background())
			return result
		})
	})
	if !result.Success || !strings.Contains(printed, "api.linger.sec may be longer") {
		t.Fatalf("result = %+v, printed %q; want the lingering engine's container removed", result, printed)
	}
	if !slices.Contains(engine.received(), shutdownRequest) {
		t.Error("no shutdown was sent before the removal")
	}
	if _, removed := api.mutations(); !slices.Equal(removed, []string{"engine"}) {
		t.Errorf("removed %v, want the engine container", removed)
	}
}

// Sol's recovered-after-stop and Fable P1: under --force, /status fails once, at
// the stop, and then reads a later run with the same ID. The start time detection
// saw still tells it apart, so the engine is not shut down. A run stopped with
// nothing but its ID known is not shut down while it is still active either.
func TestForceShutdownKeepsWhatDetectionSaw(t *testing.T) {
	for _, tt := range []struct{ name, shownSince, want string }{
		{"start time shown", firstStart, "is a later run that reuses the ID"},
		{"no start time shown", "", "nothing but its ID was known when it was stopped"},
	} {
		shownSince := tt.shownSince
		t.Run(tt.name, func(t *testing.T) {
			engine := newFakeEngine(t, 41)
			engine.since = firstStart
			engine.statusCode = http.StatusServiceUnavailable
			engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
				e.since, e.statusCode = laterStart, 0
				w.WriteHeader(http.StatusOK)
			}
			resolver := engine.resolver(true, false)
			defer resolver.Close()

			var result *ResolutionResult
			captureStdout(t, func() {
				result = gracefulStopWithin(t, context.Background(), resolver, shownAt(shownSince))
			})
			if result.Success || !IsRunIdentityRefusal(result.Error) || !strings.Contains(result.Message, tt.want) {
				t.Fatalf("result = %+v, want an identity refusal saying %q", result, tt.want)
			}
			if slices.Contains(engine.received(), shutdownRequest) {
				t.Error("the engine running the later run was shut down")
			}
			if !slices.Contains(engine.received(), deleteRequest) {
				t.Error("the run was not stopped, as --force may")
			}
		})
	}
}

// failingDialAfter is a probe dial that connects for the first n calls, and on
// the next one cancels and returns only when ctx ends, as a probe of a listener
// whose accept queue is full does when Ctrl-C comes.
func failingDialAfter(n int, cancel context.CancelFunc) func(context.Context, string) (net.Conn, error) {
	calls := 0
	return func(ctx context.Context, address string) (net.Conn, error) {
		calls++
		if calls <= n {
			var dialer net.Dialer
			return dialer.DialContext(ctx, "tcp", address)
		}
		cancel()
		<-ctx.Done()
		return nil, ctx.Err()
	}
}

// Sol's cancel-fallback, on a fallback that remains (the stop failed): Ctrl-C
// during the dial before the fallback prints no force stop and removes nothing.
func TestForceFallbackDialEndsWithTheContext(t *testing.T) {
	engine := failingEngine(t)
	api := fakeDockerWith(publishedEngine(t, "engine", "127.0.0.1", engine.port()))
	resolver := engine.resolver(true, false)
	withDocker(resolver, api)
	defer resolver.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resolver.checker.dial = failingDialAfter(1, cancel)

	var result *ResolutionResult
	printed := captureStdout(t, func() {
		result = within(t, func() *ResolutionResult {
			result, _ := resolver.CheckAndResolveConflicts(ctx)
			return result
		})
	})
	if result.Success || !errors.Is(result.Error, context.Canceled) || !strings.Contains(result.Message, "Interrupted while checking port") {
		t.Fatalf("result = %+v, want the interruption", result)
	}
	if strings.Contains(printed, "Graceful stop failed") || strings.Contains(printed, "[FORCE] Force stopping") {
		t.Errorf("printed %q, want no force stop announced", printed)
	}
	assertNoDockerChange(t, api)
}

// A dial interrupted at the end of the release wait reports the interruption, not
// a longer api.linger.sec.
func TestReleaseWaitFinalDialEndsWithTheContext(t *testing.T) {
	engine := newFakeEngine(t, 41)
	resolver := engine.resolver(false, true)
	defer resolver.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resolver.checker.dial = failingDialAfter(0, cancel)

	var result *ResolutionResult
	captureStdout(t, func() { result = gracefulStopWithin(t, ctx, resolver, confirmedEngine()) })
	if result.Success || !errors.Is(result.Error, context.Canceled) ||
		!strings.Contains(result.Message, "Stopped waiting for port") || strings.Contains(result.Message, "linger") {
		t.Fatalf("result = %+v, want the interruption", result)
	}
}

// blockingListAPI is a daemon whose container listing answers only when ctx ends.
type blockingListAPI struct{ *fakeDockerAPI }

func (b blockingListAPI) ContainerList(ctx context.Context, _ containertypes.ListOptions) ([]containertypes.Summary, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// The force stop's time limit running out inside the Docker listing is reported
// as the time limit, as in the verification.
func TestForceListingReportsItsTimeLimit(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	defer server.Close()
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	api := blockingListAPI{&fakeDockerAPI{}}
	resolver := &ConflictResolver{
		Port: port, checker: NewPortCheckerWithOptions(port, "127.0.0.1", time.Second, time.Second, 1),
		dockerFinder: &DockerContainerFinder{client: api, clock: NewFakeClock()}, clock: &pollClock{},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	result := within(t, func() *ResolutionResult {
		return resolver.executeForceStop(ctx, &ServiceInfo{IsListening: true, Address: net.ParseIP("127.0.0.1")})
	})
	if result.Success || !errors.Is(result.Error, context.DeadlineExceeded) ||
		result.Message != "Gave up finding the container on port "+port+": the time limit for it ran out" {
		t.Fatalf("result = %+v, want the time limit reported", result)
	}
}

// Sol's bad-stop-report: a /status that names the run in a state that is neither
// RUNNING nor terminal, and an engine that still reports the run after the stop,
// do not show that the run stopped.
func TestStopWordingNeedsTheRunToEnd(t *testing.T) {
	engine := newFakeEngine(t, 41)
	engine.statusState = "IDLE"
	engine.onDelete = func(_ *fakeEngine, w http.ResponseWriter) { w.WriteHeader(http.StatusOK) }
	resolver := engine.resolver(false, true)
	defer resolver.Close()

	printed := captureStdout(t, func() { gracefulStopWithin(t, context.Background(), resolver, confirmedEngine()) })
	if strings.Contains(printed, "Stopped SPT run 41") || !strings.Contains(printed, "does not show that the run ended") {
		t.Errorf("printed %q, want the unconfirmed wording", printed)
	}
}

// After a worker was shown, a /status lost at the stop is not described as the
// current status not naming the run.
func TestLostStatusOfAShownWorkerIsSaidToBeLost(t *testing.T) {
	engine := newFakeEngine(t, 41)
	engine.worker = true
	engine.statusCode = http.StatusServiceUnavailable
	engine.onDelete = func(_ *fakeEngine, w http.ResponseWriter) { w.WriteHeader(http.StatusOK) }
	resolver := engine.resolver(true, false)
	defer resolver.Close()
	info := confirmedEngine()
	info.Status.Worker = true

	printed := captureStdout(t, func() { gracefulStopWithin(t, context.Background(), resolver, info) })
	if strings.Contains(printed, "its status does not name that run") ||
		!strings.Contains(printed, "its status could not be read, and when it was shown it did not name that run") {
		t.Errorf("printed %q, want the lost status and the earlier worker reading", printed)
	}
}
