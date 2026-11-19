/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"bufio"
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	containertypes "github.com/docker/docker/api/types/container"
)

// Re-review 5: the evidence each check before a write uses must be what it
// claims to be. A Docker stop is a write of its own, and so is the removal after
// it; a run that had ended is not the run shown; part of an answer is an answer;
// a redirect is not followed; a start time that cannot be read again is doubt.

// lockedChange runs change on the engine under its lock, as a request would.
func lockedChange(engine *fakeEngine, change func(*fakeEngine)) func() {
	return func() {
		engine.mu.Lock()
		defer engine.mu.Unlock()
		change(engine)
	}
}

// atThePrompt makes the resolver take the prompt's answer instead of --force;
// beforeAnswer runs as the answer is read.
func atThePrompt(resolver *ConflictResolver, answer string, beforeAnswer func()) {
	resolver.forceMode, resolver.interactiveMode = false, true
	resolver.input = newAnswerAfter(answer, beforeAnswer)
}

func assertDockerChanges(t *testing.T, api *fakeDockerAPI, wantStopped, wantRemoved []string) {
	t.Helper()
	stopped, removed := api.mutations()
	if !slices.Equal(stopped, wantStopped) || !slices.Equal(removed, wantRemoved) {
		t.Errorf("stopped %v, removed %v; want stopped %v, removed %v", stopped, removed, wantStopped, wantRemoved)
	}
}

// Fix 1, Sol failed-docker-stop and restart-between-writes: the Docker stop
// fails, or the container restarts after it, and run 42 is active by the
// removal. The check is made again before the removal, which is not sent.
func TestNoRemovalWhenARunStartsAfterTheDockerStop(t *testing.T) {
	for _, tt := range []struct {
		name    string
		route   string // "--force" after a failed DELETE, or the prompt's "f"
		shown   int64
		stopErr error
		want    string
	}{
		{"failed stop, f, idle engine shown", "f", 0, errors.New("daemon error 500"), "run 42 is active on the engine, and no run was shown"},
		{"restart after the stop, f, idle engine shown", "f", 0, nil, "run 42 is active on the engine, and no run was shown"},
		{"restart after the stop, f, run 41 shown", "f", 41, nil, "run 42 is active on the engine, not run 41"},
		{"restart after the stop, --force", "--force", 41, nil, "run 42 is active on the engine, not run 41"},
		{"failed stop, --force", "--force", 41, errors.New("daemon error 500"), "run 42 is active on the engine, not run 41"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			engine, api, resolver := shownEngine(t)
			engine.setRun(tt.shown)
			if tt.route == "f" {
				atThePrompt(resolver, "f\n", func() {})
			} else {
				engine.onDelete = func(_ *fakeEngine, w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) }
			}
			api.onStop = func(string) error {
				lockedChange(engine, func(e *fakeEngine) { e.runID, e.since = 42, laterStart })()
				return tt.stopErr
			}

			result, _ := resolve(t, context.Background(), resolver)
			assertRefused(t, result, "Asked Docker to stop container shown-engine, and then did not remove it: "+tt.want)
			assertDockerChanges(t, api, []string{"shown-engine"}, nil)
		})
	}
}

// Fix 1: after the stop, another container publishes the address; the one
// stopped is not removed. Control: the stop closes the engine, nothing answers,
// and the container is removed.
func TestTheRemovalChecksThePublisherAgain(t *testing.T) {
	t.Run("another publisher", func(t *testing.T) {
		engine, api, resolver := shownEngine(t)
		engine.setRun(0)
		atThePrompt(resolver, "f\n", func() {})
		replacement := fakeDockerWith(publishedEngine(t, "replacement-engine", "127.0.0.1", engine.port())).containers
		api.onStop = func(string) error { api.setContainers(replacement...); return nil }

		result, _ := resolve(t, context.Background(), resolver)
		assertRefused(t, result, "replacement-engine publishes 127.0.0.1:"+engine.port()+" now")
		assertDockerChanges(t, api, []string{"shown-engine"}, nil)
	})
	t.Run("the engine closed, control", func(t *testing.T) {
		engine, api, resolver := shownEngine(t)
		engine.setRun(0)
		atThePrompt(resolver, "f\n", func() {})
		api.onStop = func(string) error {
			api.setContainers()
			_ = engine.server.Listener.Close()
			return nil
		}

		result, _ := resolve(t, context.Background(), resolver)
		if !result.Success {
			t.Fatalf("result = %+v, want success", result)
		}
		assertDockerChanges(t, api, []string{"shown-engine"}, []string{"shown-engine"})
	})
}

// Fix 2, Sol terminal-id and Fable TestR5P2: detection reads STOPPED 41 at T1
// and no active run. By the write, a later run 41 (T2) is active. It is not the
// run shown, by any route.
func TestAnEndedRunIsNotTheRunShown(t *testing.T) {
	ended := func(e *fakeEngine) {
		e.runID, e.statusState, e.since, e.headStatus = 41, "STOPPED", firstStart, http.StatusNoContent
	}
	later := func(e *fakeEngine) { e.statusState, e.since, e.headStatus = "", laterStart, 0 }
	for _, tt := range []struct{ route, want string }{
		{"--force", "run 41 is active, and detection showed no active run on the engine, or could not read it"},
		{"g", "run 41 is active, and detection showed no active run on the engine, or could not read it"},
		{"f", "run 41 is active on the engine, and no run was shown"},
	} {
		t.Run(tt.route, func(t *testing.T) {
			engine, api, resolver := shownEngine(t)
			lockedChange(engine, ended)()
			if tt.route == "--force" {
				engine.onHead = headCounter(2, later)
			} else {
				atThePrompt(resolver, tt.route+"\n", lockedChange(engine, later))
			}

			result, printed := resolve(t, context.Background(), resolver)
			if tt.route != "--force" && !strings.Contains(printed, "Run ID: 41") {
				t.Errorf("printed %q, want the ended run's ID shown", printed)
			}
			assertRefused(t, result, tt.want)
			assertChanges(t, engine)
			assertNoDockerChange(t, api)
		})
	}
}

// Fix 5, Sol status-lost: RUNNING 41 at T1 is shown; at the stop, HEAD names 41
// and /status answers 503 while a later run 41 (T2) is active. --force does not
// stop it by its ID, and removes nothing.
func TestForceRefusesAStopWhenTheShownStartTimeCannotBeRead(t *testing.T) {
	engine, api, resolver := shownEngine(t)
	engine.onHead = headCounter(2, func(e *fakeEngine) { e.since, e.statusCode = laterStart, http.StatusServiceUnavailable })

	result, _ := resolve(t, context.Background(), resolver)
	assertRefused(t, result, "the engine's /status could not be read")
	if !IsRunIdentityRefusal(result.Error) {
		t.Errorf("error = %v, want an identity refusal", result.Error)
	}
	assertChanges(t, engine)
	assertNoDockerChange(t, api)
}

// rawHolder is a TCP service on 127.0.0.1 that answers each connection with
// serve, given the connection and a channel closed when the service ends: at
// the test's end, or at Docker's stop. Its port is published by the container
// unrelated.
func rawHolder(t *testing.T, serve func(conn net.Conn, done <-chan struct{})) (*fakeDockerAPI, *ConflictResolver) {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var once sync.Once
	stop := func() { once.Do(func() { close(done); _ = listener.Close() }) }
	t.Cleanup(stop)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				serve(conn, done)
			}()
		}
	}()
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	api := fakeDockerWith(unrelatedPublisher(t, port))
	// Docker's stop ends the service, as it ends a container's process.
	api.onStop = func(string) error { stop(); return nil }
	api.onRemove = func(string) { stop() }
	resolver := &ConflictResolver{
		Port: port, checker: NewPortCheckerWithOptions(port, "127.0.0.1", time.Second, 2*time.Second, 1),
		forceMode: true, clock: &pollClock{},
	}
	withDocker(resolver, api)
	t.Cleanup(func() { _ = resolver.Close() })
	return api, resolver
}

// readsRequest reads one request from conn, then writes answer (none if empty).
func readsRequest(conn net.Conn, answer string) bool {
	if _, err := http.ReadRequest(bufio.NewReader(conn)); err != nil {
		return false
	}
	if answer != "" {
		_, _ = conn.Write([]byte(answer))
	}
	return true
}

// Fix 3, Sol partial-head and Fable TestR5P12: part of an answer is an answer.
// The service sends a status line, with or without a header, and closes the
// connection: --force and f leave its container running.
func TestPartOfAnAnswerIsNotNoAnswer(t *testing.T) {
	for _, tt := range []struct{ name, answer string }{
		{"status line and a header, then close", "HTTP/1.1 200 OK\r\nETag: 42\r\n"},
		{"status line, then close", "HTTP/1.1 200 OK\r\n"},
		{"a few bytes, then close", "HTT"},
	} {
		for _, route := range []string{"--force", "f"} {
			t.Run(tt.name+", "+route, func(t *testing.T) {
				api, resolver := rawHolder(t, func(conn net.Conn, _ <-chan struct{}) { readsRequest(conn, tt.answer) })
				if route == "f" {
					atThePrompt(resolver, "f\n", func() {})
				}

				result, _ := resolve(t, context.Background(), resolver)
				assertRefused(t, result, "Did not stop or remove container unrelated: the service on port "+resolver.Port+" answered, but not with an HTTP answer the CLI can read")
				assertNoDockerChange(t, api)
			})
		}
	}
}

// Fix 3 and Fable M29: a request that times out is no answer only when not a
// byte came back. Sol slow-head (nothing sent) stays the fallback, and the
// container is removed; a service that sends a status line and stalls refuses.
func TestATimeoutIsNoAnswerOnlyWithNoBytes(t *testing.T) {
	for _, tt := range []struct {
		name, answer string
		removed      bool
	}{
		{"nothing sent, control", "", true},
		{"a status line, then nothing", "HTTP/1.1 200 OK\r\n", false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			api, resolver := rawHolder(t, func(conn net.Conn, done <-chan struct{}) {
				if readsRequest(conn, tt.answer) {
					<-done
				}
			})

			result, _ := resolve(t, context.Background(), resolver)
			if tt.removed {
				if _, removed := api.mutations(); !result.Success || !slices.Equal(removed, []string{"unrelated"}) {
					t.Fatalf("result = %+v, removed %v; want the container removed", result, removed)
				}
				return
			}
			assertRefused(t, result, "not with an HTTP answer the CLI can read")
			assertNoDockerChange(t, api)
		})
	}
}

// Fix 4, Sol redirect-write and Fable Low 3: a write answered with a redirect
// is not sent on to the address it names, and --force removes nothing after it.
func TestAWriteIsNotRedirected(t *testing.T) {
	for _, tt := range []struct {
		name, method, path string
		want               []engineRequest
	}{
		{"shutdown", http.MethodPost, "/shutdown", []engineRequest{deleteRequest, shutdownRequest}},
		{"stop", http.MethodDelete, "/run", []engineRequest{deleteRequest}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			other := newFakeEngine(t, 92)
			engine, api, resolver := shownEngine(t)
			engine.redirect = func(r *http.Request) string {
				if r.Method == tt.method && r.URL.Path == tt.path {
					return other.server.URL + tt.path
				}
				return ""
			}

			result, _ := resolve(t, context.Background(), resolver)
			assertRefused(t, result, "with a redirect (status 307")
			if !IsRunIdentityRefusal(result.Error) {
				t.Errorf("error = %v, want an identity refusal", result.Error)
			}
			assertChanges(t, engine, tt.want...)
			if got := other.received(); len(got) != 0 {
				t.Errorf("the address redirected to received %+v, want nothing", got)
			}
			assertNoDockerChange(t, api)
		})
	}
}

// Fix 4, Sol redirect-read and redirect-closed, Fable TestR5P3: a service that
// redirects every request is not an SPT API, and its redirect is the answer to
// the check before a removal: the container is left running, and the address
// redirected to, an idle engine or a closed port, gets nothing.
func TestAReadIsNotRedirected(t *testing.T) {
	closedPort := func(t *testing.T) string {
		listener, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		address := listener.Addr().String()
		_ = listener.Close()
		return "http://" + address
	}
	for _, tt := range []struct{ name string }{{"to an idle engine"}, {"to a closed port"}} {
		t.Run(tt.name, func(t *testing.T) {
			other := newFakeEngine(t, 0)
			target := other.server.URL
			if tt.name == "to a closed port" {
				target = closedPort(t)
			}
			origin := newFakeEngine(t, 0)
			origin.redirect = func(r *http.Request) string { return target + r.URL.Path }
			api := fakeDockerWith(unrelatedPublisher(t, origin.port()))
			resolver := origin.resolver(true, false)
			withDocker(resolver, api)
			defer resolver.Close()

			result, _ := resolve(t, context.Background(), resolver)
			assertRefused(t, result, "answers HTTP, but not as an SPT engine (run status 307)")
			if got := other.received(); len(got) != 0 {
				t.Errorf("the address redirected to received %+v, want nothing", got)
			}
			assertNoDockerChange(t, api)
		})
	}
}

// Fix 6, Sol closed-at-stop and Fable TestR5P7: the engine accepts the stop and
// closes, and its container is gone, before the check before the shutdown. The
// port is free, so the stop succeeded. Control: with the container gone but the
// port still held, the refusal stays.
func TestAPortFreedAfterTheStopIsSuccess(t *testing.T) {
	for _, route := range []string{"--force", "g"} {
		for _, closes := range []bool{true, false} {
			name := route + ", port freed"
			if !closes {
				name = route + ", port still held, control"
			}
			t.Run(name, func(t *testing.T) {
				engine, api, resolver := shownEngine(t)
				if route == "g" {
					atThePrompt(resolver, "g\n", func() {})
				}
				engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
					e.runID = 0
					api.setContainers()
					if closes {
						_ = e.server.Listener.Close()
					}
					w.WriteHeader(http.StatusOK)
				}

				result, printed := resolve(t, context.Background(), resolver)
				assertChanges(t, engine, deleteRequest)
				assertNoDockerChange(t, api)
				if !closes {
					assertRefused(t, result, "now: no container publishes 127.0.0.1:"+engine.port())
					return
				}
				if !result.Success || !strings.Contains(printed, "SPT engine stopped - port "+engine.port()+" is now available") {
					t.Fatalf("result = %+v, printed %q; want the stop's success", result, printed)
				}
			})
		}
	}
}

// Fix 7, Fable Medium 1 and Sol Low 8: a container whose service answers HTTP,
// but not as an SPT engine, is left running by --force and f, and the message
// says so and names the manual removal. It says nothing of an SPT API or run.
func TestANonEngineServiceIsRefusedWithATrueMessage(t *testing.T) {
	for _, tt := range []struct {
		name   string
		status int
	}{{"404", http.StatusNotFound}, {"405", http.StatusMethodNotAllowed}, {"200 without an ETag", http.StatusOK}} {
		for _, route := range []string{"--force", "f"} {
			t.Run(tt.name+", "+route, func(t *testing.T) {
				service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodHead && r.URL.Path == "/run" {
						w.WriteHeader(tt.status)
						return
					}
					w.WriteHeader(http.StatusNotFound)
				}))
				t.Cleanup(service.Close)
				_, port, _ := net.SplitHostPort(service.Listener.Addr().String())
				api := fakeDockerWith(unrelatedPublisher(t, port))
				resolver := &ConflictResolver{
					Port: port, checker: NewPortCheckerWithOptions(port, "127.0.0.1", time.Second, 2*time.Second, 1),
					forceMode: true, clock: &pollClock{},
				}
				withDocker(resolver, api)
				defer resolver.Close()
				if route == "f" {
					atThePrompt(resolver, "f\n", func() {})
				}

				result, _ := resolve(t, context.Background(), resolver)
				assertRefused(t, result, "the service on port "+port+" answers HTTP, but not as an SPT engine")
				for _, want := range []string{"can't tell whether it is safe to remove", "docker rm -f unrelated"} {
					if !strings.Contains(result.Message, want) {
						t.Errorf("Message = %q, want %q", result.Message, want)
					}
				}
				for _, untrue := range []string{"SPT API answered", "stop the run yourself", "SPT engine on port"} {
					if strings.Contains(result.Message, untrue) {
						t.Errorf("Message = %q, must not say %q", result.Message, untrue)
					}
				}
				assertNoDockerChange(t, api)
			})
		}
	}
	t.Run("a banner that is not HTTP", func(t *testing.T) {
		api, resolver := rawHolder(t, func(conn net.Conn, _ <-chan struct{}) {
			_, _ = conn.Write([]byte("SSH-2.0-OpenSSH_9.6\r\n"))
		})

		result, _ := resolve(t, context.Background(), resolver)
		assertRefused(t, result, "answered, but not with an HTTP answer the CLI can read")
		if strings.Contains(result.Message, "SPT API answered") || !strings.Contains(result.Message, "docker rm -f unrelated") {
			t.Errorf("Message = %q, want no SPT API named and the manual removal", result.Message)
		}
		assertNoDockerChange(t, api)
	})
}

// blockListingsAfter is a daemon whose listings after the first n never return
// until their context ends.
type blockListingsAfter struct {
	*fakeDockerAPI
	mu *sync.Mutex
	n  *int
}

func (b blockListingsAfter) ContainerList(ctx context.Context, options containertypes.ListOptions) ([]containertypes.Summary, error) {
	b.mu.Lock()
	*b.n--
	block := *b.n < 0
	b.mu.Unlock()
	if block {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return b.fakeDockerAPI.ContainerList(ctx, options)
}

// Fable M33: the listing before an API write is bounded too. A daemon that
// stops answering after detection ends the check within its bound, as a
// refusal, and nothing is sent to the engine.
func TestTheListingBeforeAnAPIWriteIsBounded(t *testing.T) {
	engine, api, resolver := shownEngine(t)
	n := 1
	blocking := blockListingsAfter{fakeDockerAPI: api, mu: &sync.Mutex{}, n: &n}
	resolver.dockerFinder = &DockerContainerFinder{client: blocking, clock: NewFakeClock()}

	start := time.Now()
	result, _ := resolve(t, context.Background(), resolver)
	if elapsed := time.Since(start); elapsed > 15*time.Second {
		t.Errorf("the check took %s", elapsed)
	}
	assertRefused(t, result, "now: the containers could not be listed")
	assertChanges(t, engine)
	assertNoDockerChange(t, api)
}

// Fable M22: two containers publish the address by the stop, so neither is
// the one shown, and nothing is sent.
func TestSeveralPublishersAtAnAPIWriteRefuse(t *testing.T) {
	engine, api, resolver := shownEngine(t)
	both := append(slices.Clone(api.containers),
		fakeDockerWith(publishedEngine(t, "second-engine", "0.0.0.0", engine.port())).containers...)
	atThePrompt(resolver, "g\n", func() { api.setContainers(both...) })

	result, _ := resolve(t, context.Background(), resolver)
	assertRefused(t, result, "shown-engine, second-engine all publish 127.0.0.1:"+engine.port())
	assertChanges(t, engine)
	assertNoDockerChange(t, api)
}

// Fable M31: Ctrl-C during the check before a removal, or during the check
// again after the stop, ends it there: nothing more goes to Docker.
func TestCancellationDuringTheRemovalCheckStopsBeforeDocker(t *testing.T) {
	for _, tt := range []struct {
		name        string
		head        int
		wantStopped []string
	}{
		{"before the stop", 2, nil},
		{"before the removal", 3, []string{"shown-engine"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			engine, api, resolver := shownEngine(t)
			engine.setRun(0)
			atThePrompt(resolver, "f\n", func() {})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			block := make(chan struct{})
			defer close(block)
			engine.onHead = headCounter(tt.head, func(e *fakeEngine) {
				cancel()
				e.mu.Unlock()
				<-block
				e.mu.Lock()
			})

			result, _ := resolve(t, ctx, resolver)
			assertRefused(t, result, "Interrupted while checking the run on port "+engine.port()+" before removing a container")
			assertDockerChanges(t, api, tt.wantStopped, nil)
		})
	}
}
