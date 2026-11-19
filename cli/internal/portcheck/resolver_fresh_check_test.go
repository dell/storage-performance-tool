/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"context"
	"errors"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	containertypes "github.com/docker/docker/api/types/container"
)

// Every write, an API request that changes the engine or a Docker stop or
// removal, follows a fresh check on the address that answered detection. These
// tests change the world just before a write and check that nothing is written
// after the check that refuses.

// resolve runs the whole resolution and returns its result and what it printed.
func resolve(t *testing.T, ctx context.Context, resolver *ConflictResolver) (*ResolutionResult, string) {
	t.Helper()
	var result *ResolutionResult
	printed := captureStdout(t, func() {
		result = within(t, func() *ResolutionResult {
			result, _ := resolver.CheckAndResolveConflicts(ctx)
			return result
		})
	})
	return result, printed
}

// shownEngine is a --force resolver for an engine running run 41 since
// firstStart, in the engine container shown-engine that publishes 127.0.0.1.
func shownEngine(t *testing.T) (*fakeEngine, *fakeDockerAPI, *ConflictResolver) {
	t.Helper()
	engine := newFakeEngine(t, 41)
	engine.since = firstStart
	api := fakeDockerWith(publishedEngine(t, "shown-engine", "127.0.0.1", engine.port()))
	api.onRemove = func(string) { engine.server.Close() }
	resolver := engine.resolver(true, false)
	withDocker(resolver, api)
	t.Cleanup(func() { _ = resolver.Close() })
	return engine, api, resolver
}

// assertChanges checks the requests that change the engine, in order.
func assertChanges(t *testing.T, engine *fakeEngine, want ...engineRequest) {
	t.Helper()
	if got := changesTo(engine.received()); !slices.Equal(got, want) {
		t.Errorf("engine changes = %+v, want %+v", got, want)
	}
}

func assertRefused(t *testing.T, result *ResolutionResult, want string) {
	t.Helper()
	if result.Success || !strings.Contains(result.Message, want) {
		t.Fatalf("result = %+v, want a refusal saying %q", result, want)
	}
}

// headCounter runs change at the nth HEAD /run. Detection sends the first.
func headCounter(n int, change func(*fakeEngine)) func(*fakeEngine) {
	heads := 0
	return func(e *fakeEngine) {
		heads++
		if heads == n {
			change(e)
		}
	}
}

// Sol reused-after-404 and Fable F1: the stop finds no run 41, and then run 41 is
// active: on the shipped engine, a later run that reuses the ID. It is left
// alone, and so is its container.
func TestForceRefusesASameIDRunAfterAMissedStop(t *testing.T) {
	for _, code := range []int{http.StatusNotFound, http.StatusNoContent} {
		t.Run(http.StatusText(code), func(t *testing.T) {
			engine, api, resolver := shownEngine(t)
			engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
				e.since = laterStart
				w.WriteHeader(code)
			}

			result, _ := resolve(t, context.Background(), resolver)
			assertRefused(t, result, "it is a later run that reuses the ID")
			if !IsRunIdentityRefusal(result.Error) {
				t.Errorf("error = %v, want an identity refusal", result.Error)
			}
			assertChanges(t, engine, deleteRequest)
			assertNoDockerChange(t, api)
		})
	}
}

// Sol unreadable-head and Fable F4: HEAD /run fails at the stop, before any
// DELETE. The check before the removal reads it again: another run, or the
// same error status, leaves the container running.
func TestForceRefusesWhenTheRunCannotBeReadAtTheStop(t *testing.T) {
	tests := []struct {
		name   string
		change func(*fakeEngine)
		want   string
	}{
		{"503, then run 42", func(e *fakeEngine) {
			e.headStatus, e.runID = http.StatusServiceUnavailable, 42
			e.onHead = headCounter(1, func(e *fakeEngine) { e.headStatus = 0 })
		}, "run 42 is active on the engine, not run 41"},
		{"503 throughout", func(e *fakeEngine) { e.headStatus = http.StatusServiceUnavailable },
			"its active run could not be read (run status 503)"},
		{"500 throughout", func(e *fakeEngine) { e.headStatus = http.StatusInternalServerError },
			"its active run could not be read (run status 500)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine, api, resolver := shownEngine(t)
			engine.onHead = headCounter(2, tt.change)

			result, _ := resolve(t, context.Background(), resolver)
			assertRefused(t, result, tt.want)
			assertChanges(t, engine)
			assertNoDockerChange(t, api)
		})
	}
}

// Sol delete-reset: the stop's connection breaks, and run 42 is active by the
// check before the removal. With the run shown still active instead, --force
// removes its container, as it may.
func TestForceAfterAStopWithNoAnswer(t *testing.T) {
	t.Run("another run", func(t *testing.T) {
		engine, api, resolver := shownEngine(t)
		engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
			e.runID = 42
			hangUp(e.t, w)
		}

		result, _ := resolve(t, context.Background(), resolver)
		assertRefused(t, result, "run 42 is active on the engine, not run 41")
		assertChanges(t, engine, deleteRequest)
		assertNoDockerChange(t, api)
	})
	t.Run("the run shown, control", func(t *testing.T) {
		engine, api, resolver := shownEngine(t)
		engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) { hangUp(e.t, w) }

		result, _ := resolve(t, context.Background(), resolver)
		if _, removed := api.mutations(); !result.Success || !slices.Equal(removed, []string{"shown-engine"}) {
			t.Fatalf("result = %+v, removed %v; want the container shown removed", result, removed)
		}
		assertChanges(t, engine, deleteRequest)
	})
}

// Sol shutdown-reset and idle-shutdown-reset: the run was stopped and the
// shutdown request's connection breaks. If run 42 is active by the check before
// the removal, the container is left running; if the engine is still idle, it
// is removed, which ends no run.
func TestForceAfterAShutdownWithNoAnswer(t *testing.T) {
	for _, tt := range []struct {
		name     string
		laterRun int64
	}{{"run 42 active", 42}, {"still idle, control", 0}} {
		t.Run(tt.name, func(t *testing.T) {
			engine, api, resolver := shownEngine(t)
			engine.hangUp = func(e *fakeEngine, r *http.Request) bool {
				if r.URL.Path != "/shutdown" {
					return false
				}
				e.runID = tt.laterRun
				return true
			}

			result, printed := resolve(t, context.Background(), resolver)
			assertChanges(t, engine, deleteRequest, shutdownRequest)
			if !strings.Contains(printed, "The request to shut down the SPT engine failed") {
				t.Errorf("printed %q, want the failed shutdown request named", printed)
			}
			stopped, removed := api.mutations()
			if tt.laterRun != 0 {
				assertRefused(t, result, "run 42 is active on the engine, not run 41")
				if len(stopped) != 0 || len(removed) != 0 {
					t.Errorf("stopped %v, removed %v; want none", stopped, removed)
				}
				return
			}
			if !result.Success || !slices.Equal(removed, []string{"shown-engine"}) {
				t.Fatalf("result = %+v, removed %v; want the idle engine's container removed", result, removed)
			}
		})
	}
}

// listHook is a daemon that runs onList before each container listing.
type listHook struct {
	*fakeDockerAPI
	onList func()
}

func (h listHook) ContainerList(ctx context.Context, options containertypes.ListOptions) ([]containertypes.Summary, error) {
	h.onList()
	return h.fakeDockerAPI.ContainerList(ctx, options)
}

// Sol late-api: no HTTP request answers during detection, so the engine is not
// confirmed, and then its API answers with run 42 active by the time of the
// removal: the container is left running. An engine whose API never answers is
// the fallback, and its container is removed.
func TestForceChecksAnAPIThatAnswersLate(t *testing.T) {
	for _, recovers := range []bool{true, false} {
		name := "recovers with run 42"
		if !recovers {
			name = "never answers, control"
		}
		t.Run(name, func(t *testing.T) {
			engine := newFakeEngine(t, 42)
			engine.since = laterStart
			down := true
			engine.hangUp = func(*fakeEngine, *http.Request) bool { return down }
			api := fakeDockerWith(publishedEngine(t, "shown-engine", "127.0.0.1", engine.port()))
			api.onRemove = func(string) { engine.server.Close() }
			hook := listHook{fakeDockerAPI: api, onList: func() {
				if recovers {
					engine.mu.Lock()
					down = false
					engine.mu.Unlock()
				}
			}}
			resolver := engine.resolver(true, false)
			resolver.dockerFinder = &DockerContainerFinder{client: hook, clock: NewFakeClock()}
			resolver.checker.newFinder = func() (*DockerContainerFinder, error) {
				return &DockerContainerFinder{client: hook, clock: NewFakeClock()}, nil
			}
			defer resolver.Close()

			result, _ := resolve(t, context.Background(), resolver)
			assertChanges(t, engine)
			stopped, removed := api.mutations()
			if recovers {
				assertRefused(t, result, "run 42 is active on the engine, and no run was shown")
				if len(stopped) != 0 || len(removed) != 0 {
					t.Errorf("stopped %v, removed %v; want none", stopped, removed)
				}
				return
			}
			if !result.Success || !slices.Equal(removed, []string{"shown-engine"}) {
				t.Fatalf("result = %+v, removed %v; want the container of the engine with no API removed", result, removed)
			}
		})
	}
}

// Sol remote-advice: a daemon reached over TCP may list another host's
// containers, so none is shown at the prompt or named in a refusal's advice.
func TestNoContainerOfARemoteDaemonIsNamed(t *testing.T) {
	for _, mode := range []string{"--force", "prompt"} {
		t.Run(mode, func(t *testing.T) {
			engine := newFakeEngine(t, 41)
			engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
				e.runID = 42
				w.WriteHeader(http.StatusNotFound)
			}
			api := fakeDockerWith(publishedEngine(t, "remote-engine", "0.0.0.0", engine.port()))
			api.daemonHost = "tcp://127.0.0.1:2375"
			resolver := engine.resolver(mode == "--force", mode == "prompt")
			withDocker(resolver, api)
			resolver.input = strings.NewReader("a\n")
			defer resolver.Close()

			result, printed := resolve(t, context.Background(), resolver)
			if strings.Contains(printed+result.Message, "remote-engine") {
				t.Errorf("printed %q, message %q; want the remote daemon's container not named", printed, result.Message)
			}
			assertNoDockerChange(t, api)
		})
	}
}

// Sol Medium 6, decided as option A: --force acts only on the run detection
// showed. A run that replaced it before the stop, with another ID or a later
// start time, is not stopped, and its container is left running.
func TestForceStopsOnlyTheRunShown(t *testing.T) {
	for _, tt := range []struct {
		name  string
		runID int64
		want  string
	}{{"another ID", 42, "run 42 replaced the run shown, 41"}, {"a later start time", 41, "is a later run that reuses the ID"}} {
		t.Run(tt.name, func(t *testing.T) {
			engine, api, resolver := shownEngine(t)
			engine.onHead = headCounter(2, func(e *fakeEngine) { e.runID, e.since = tt.runID, laterStart })

			result, _ := resolve(t, context.Background(), resolver)
			assertRefused(t, result, tt.want)
			if !IsRunIdentityRefusal(result.Error) {
				t.Errorf("error = %v, want an identity refusal", result.Error)
			}
			assertChanges(t, engine)
			assertNoDockerChange(t, api)
		})
	}
}

// sameNameReplacement is a container named name, with another ID, that
// publishes port on 127.0.0.1.
func sameNameReplacement(t *testing.T, name, port string) []containertypes.Summary {
	replacement := fakeDockerWith(publishedEngine(t, "replacement-id", "127.0.0.1", port)).containers
	replacement[0].Names = []string{"/" + name}
	return replacement
}

// Sol High 3: the container shown at the prompt is replaced, while the prompt
// is open, by another engine container on the same address, idle or with a run
// that has the ID shown. g sends the new engine nothing. A replacement with the
// same name is told apart by its ID.
func TestPromptGracefulRefusesAReplacedContainer(t *testing.T) {
	for _, tt := range []struct {
		name     string
		runID    int64
		sameName bool
	}{{"idle", 0, false}, {"run 41", 41, false}, {"same name, run 41", 41, true}} {
		t.Run(tt.name, func(t *testing.T) {
			engine := newFakeEngine(t, tt.runID)
			api := fakeDockerWith(publishedEngine(t, "shown-engine", "127.0.0.1", engine.port()))
			replacement := fakeDockerWith(publishedEngine(t, "replacement-engine", "127.0.0.1", engine.port())).containers
			want := "(shown: shown-engine; now: replacement-engine)"
			if tt.sameName {
				replacement = sameNameReplacement(t, "shown-engine", engine.port())
				want = "(shown: shown-engine; now: shown-engine)"
			}
			resolver := engine.resolver(false, true)
			withDocker(resolver, api)
			resolver.input = newAnswerAfter("g\n", func() { api.setContainers(replacement...) })
			defer resolver.Close()

			result, printed := resolve(t, context.Background(), resolver)
			if !strings.Contains(printed, "Name: shown-engine") {
				t.Errorf("printed %q, want the container shown", printed)
			}
			assertRefused(t, result, want)
			if !errors.Is(result.Error, errHolderChanged) {
				t.Errorf("error = %v, want the holder change", result.Error)
			}
			assertReadOnly(t, engine.received())
			assertNoDockerChange(t, api)
		})
	}
}

// The same check before the shutdown: the container changes after the stop, and
// the engine is not shut down, by either route. --force ends there and does not
// go on to the Docker route.
func TestNoShutdownWhenTheContainerChangesAfterTheStop(t *testing.T) {
	for _, mode := range []string{"--force", "prompt g"} {
		t.Run(mode, func(t *testing.T) {
			engine, api, resolver := shownEngine(t)
			if mode != "--force" {
				resolver.forceMode, resolver.interactiveMode = false, true
				resolver.input = strings.NewReader("g\n")
			}
			replacement := fakeDockerWith(publishedEngine(t, "replacement-engine", "127.0.0.1", engine.port())).containers
			engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
				e.runID = 0
				api.setContainers(replacement...)
			}

			result, printed := resolve(t, context.Background(), resolver)
			assertRefused(t, result, "now: replacement-engine")
			if !errors.Is(result.Error, errHolderChanged) || strings.Contains(printed, "trying force stop") {
				t.Errorf("result = %+v, printed %q; want the holder change, with no force stop", result, printed)
			}
			assertChanges(t, engine, deleteRequest)
			assertNoDockerChange(t, api)
		})
	}
}

// Fable F3: /status reads the run as ended at the stop, so it gives no start
// time, and a later run with the ID is running after it. Detection's start time
// still tells it apart, and the engine is not shut down.
func TestForceKeepsTheShownStartTimeWhenTheStopReadsAnEndedRun(t *testing.T) {
	engine, api, resolver := shownEngine(t)
	engine.onHead = headCounter(2, func(e *fakeEngine) { e.statusState = "STOPPED" })
	engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
		e.statusState, e.since = "", laterStart
		w.WriteHeader(http.StatusOK)
	}

	result, _ := resolve(t, context.Background(), resolver)
	assertRefused(t, result, "is a later run that reuses the ID")
	assertChanges(t, engine, deleteRequest)
	assertNoDockerChange(t, api)
}

// Fable F5: a daemon that never answers the listing holds detection no longer
// than the listing's bound, and no container is shown.
func TestDetectionDoesNotWaitOnAnUnansweringDaemon(t *testing.T) {
	engine := newFakeEngine(t, 41)
	resolver := engine.resolver(false, false)
	resolver.dockerFinder = &DockerContainerFinder{client: blockingListAPI{&fakeDockerAPI{}}, clock: NewFakeClock()}
	defer resolver.Close()

	start := time.Now()
	result, printed := resolve(t, context.Background(), resolver)
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Errorf("detection took %s", elapsed)
	}
	if !errors.Is(result.Error, ErrPortConflictUnattended) || strings.Contains(printed, "Container Details") {
		t.Errorf("result = %+v, printed %q; want the unattended abort with no container shown", result, printed)
	}
	assertReadOnly(t, engine.received())
}

// failingLists is a daemon whose first n container listings fail.
type failingLists struct {
	*fakeDockerAPI
	mu    *sync.Mutex
	fails *int
}

func (f failingLists) ContainerList(ctx context.Context, options containertypes.ListOptions) ([]containertypes.Summary, error) {
	f.mu.Lock()
	if *f.fails > 0 {
		*f.fails--
		f.mu.Unlock()
		return nil, errors.New("daemon busy")
	}
	f.mu.Unlock()
	return f.fakeDockerAPI.ContainerList(ctx, options)
}

// Fable F5: a listing that failed at detection and works later is not called a
// changed container: the refusal says that the containers could not be listed.
func TestForceSaysTheListingFailedAtDetection(t *testing.T) {
	engine := failingEngine(t)
	api := fakeDockerWith(publishedEngine(t, "bridge-engine", "0.0.0.0", engine.port()))
	fails := 1
	lists := failingLists{fakeDockerAPI: api, mu: &sync.Mutex{}, fails: &fails}
	resolver := engine.resolver(true, false)
	resolver.dockerFinder = &DockerContainerFinder{client: lists, clock: NewFakeClock()}
	defer resolver.Close()

	result, _ := resolve(t, context.Background(), resolver)
	assertRefused(t, result, "The containers could not be listed when port "+engine.port()+" was shown")
	if strings.Contains(result.Message, "shown: none") {
		t.Errorf("Message = %q, want no claim that the container changed", result.Message)
	}
	assertNoDockerChange(t, api)
}

// Fable F6: Ctrl-C during an API read is reported as an interruption, and
// nothing is written after it.
func TestCancellationDuringAnAPIReadIsAnInterruption(t *testing.T) {
	for _, tt := range []struct {
		name string
		head int
		want []engineRequest
	}{
		{"at the stop", 2, nil},
		{"at the check before the shutdown", 3, []engineRequest{deleteRequest}},
		{"at the shutdown request", 0, []engineRequest{deleteRequest, shutdownRequest}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			engine, api, resolver := shownEngine(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			block := make(chan struct{})
			defer close(block)
			wait := func(e *fakeEngine) {
				cancel()
				e.mu.Unlock()
				<-block
				e.mu.Lock()
			}
			if tt.head == 0 {
				engine.onShutdown = wait
			} else {
				engine.onDelete = func(_ *fakeEngine, w http.ResponseWriter) { w.WriteHeader(http.StatusOK) }
				engine.onHead = headCounter(tt.head, wait)
			}

			result, _ := resolve(t, ctx, resolver)
			assertRefused(t, result, "Interrupted while")
			if !errors.Is(result.Error, context.Canceled) {
				t.Errorf("error = %v, want the cancellation", result.Error)
			}
			assertChanges(t, engine, tt.want...)
			assertNoDockerChange(t, api)
		})
	}
}

// Fable F7: a worker shown at detection whose /status fails only at the stop is
// not said to have been known by its ID alone.
func TestWorkerWithAStatusLostAtTheStop(t *testing.T) {
	engine, api, resolver := shownEngine(t)
	engine.worker = true
	engine.onHead = headCounter(2, func(e *fakeEngine) { e.statusCode = http.StatusServiceUnavailable })
	engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
		e.statusCode = 0
		w.WriteHeader(http.StatusOK)
	}

	result, _ := resolve(t, context.Background(), resolver)
	assertRefused(t, result, "its /status could not be read when it was stopped, and no start time was shown for it")
	assertChanges(t, engine, deleteRequest)
	assertNoDockerChange(t, api)
}

// Control: a service with no HTTP answer at all, here one that closes every
// connection, is the fallback, and the container publishing its address is
// removed.
func TestForceRemovesAPublisherThatAnswersNoHTTP(t *testing.T) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	api := fakeDockerWith(unrelatedPublisher(t, port))
	api.onRemove = func(string) { _ = listener.Close() }
	resolver := &ConflictResolver{
		Port: port, checker: NewPortCheckerWithOptions(port, "127.0.0.1", time.Second, 2*time.Second, 1),
		forceMode: true, clock: &pollClock{},
	}
	withDocker(resolver, api)
	defer resolver.Close()

	result, _ := resolve(t, context.Background(), resolver)
	if _, removed := api.mutations(); !result.Success || !slices.Equal(removed, []string{"unrelated"}) {
		t.Fatalf("result = %+v, removed %v; want the publisher removed", result, removed)
	}
}

// Controls: with the container shown still in place, --force and g stop the run
// shown and shut the engine down, and an idle engine is only shut down. Nothing
// is removed.
func TestShownEngineIsStoppedAndShutDown(t *testing.T) {
	for _, mode := range []string{"--force", "prompt g"} {
		for _, runID := range []int64{41, 0} {
			t.Run(mode+map[bool]string{true: ", idle", false: ", run 41"}[runID == 0], func(t *testing.T) {
				engine, api, resolver := shownEngine(t)
				engine.setRun(runID)
				if mode != "--force" {
					resolver.forceMode, resolver.interactiveMode = false, true
					resolver.input = strings.NewReader("g\n")
				}
				resolver.clock = closeAfterPolls(engine, 1)

				result, _ := resolve(t, context.Background(), resolver)
				if !result.Success {
					t.Fatalf("result = %+v, want success", result)
				}
				if runID == 0 {
					assertChanges(t, engine, shutdownRequest)
				} else {
					assertChanges(t, engine, deleteRequest, shutdownRequest)
				}
				assertNoDockerChange(t, api)
			})
		}
	}
}

// Fable F8 (M16b): Ctrl-C just as the force stop's address probe connects ends
// it there, as an interruption while finding the container, with nothing removed.
func TestForceAddressProbeEndsWithTheContext(t *testing.T) {
	engine, api, resolver := shownEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	resolver.checker.dial = func(dialCtx context.Context, address string) (net.Conn, error) {
		var dialer net.Dialer
		conn, err := dialer.DialContext(dialCtx, "tcp", address)
		cancel()
		return conn, err
	}
	info := shownAt(firstStart)
	info.Address = net.ParseIP("127.0.0.1")
	info.Container = &ContainerInfo{ID: "shown-engine", Name: "shown-engine"}

	result := within(t, func() *ResolutionResult { return resolver.executeForceStop(ctx, info) })
	if result.Success || !errors.Is(result.Error, context.Canceled) ||
		!strings.Contains(result.Message, "Interrupted while finding the container on port") {
		t.Fatalf("result = %+v, want the interruption while finding the container", result)
	}
	assertChanges(t, engine)
	assertNoDockerChange(t, api)
}

// The check before a removal: detection saw run 41's start time, the stop
// failed, and /status cannot be read now, so the run cannot be shown to be the
// one shown, and its container is left running.
func TestForceRefusesARemovalWhenTheStartTimeCannotBeRead(t *testing.T) {
	engine, api, resolver := shownEngine(t)
	engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
		e.statusCode = http.StatusServiceUnavailable
		w.WriteHeader(http.StatusInternalServerError)
	}

	result, _ := resolve(t, context.Background(), resolver)
	assertRefused(t, result, "the engine's /status could not be read")
	assertChanges(t, engine, deleteRequest)
	assertNoDockerChange(t, api)
}
