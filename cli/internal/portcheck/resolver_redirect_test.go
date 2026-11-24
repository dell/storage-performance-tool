/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// Re-review 6: a 3xx is an answer that is not the engine's, whatever its
// Location says and whichever request it answers. At a write, and at a read
// that guards one, it refuses: nothing more is sent and Docker is not changed.

// rawRedirect answers method and path with a 307 whose Location is location,
// set as it is, before the engine sees the request, which is still recorded.
func rawRedirect(engine *fakeEngine, method, path, location string) {
	inner := engine.server.Config.Handler
	engine.server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method || r.URL.Path != path {
			inner.ServeHTTP(w, r)
			return
		}
		engine.mu.Lock()
		engine.requests = append(engine.requests, engineRequest{r.Method, r.URL.Path, r.Header.Get("If-Match")})
		engine.mu.Unlock()
		w.Header().Set("Location", location)
		w.WriteHeader(http.StatusTemporaryRedirect)
	})
}

// Must fix 1, Opus L1 and Sol High 2: net/http parses Location before
// CheckRedirect, so a 3xx with a Location it cannot parse came back as a failed
// request, and --force went on to Docker. It is a redirect.
func TestAMalformedLocationIsARedirect(t *testing.T) {
	for _, tt := range []struct {
		name, method, path string
		shown              int64
		want               []engineRequest
	}{
		{"shutdown", http.MethodPost, "/shutdown", 0, []engineRequest{shutdownRequest}},
		{"stop", http.MethodDelete, "/run", 41, []engineRequest{deleteRequest}},
	} {
		for _, location := range []string{"http://[::1", "%zz", "http://a b/"} {
			for _, route := range []string{"--force", "g"} {
				t.Run(tt.name+", "+route+", "+location, func(t *testing.T) {
					engine, api, resolver := shownEngine(t)
					engine.setRun(tt.shown)
					if route == "g" {
						atThePrompt(resolver, "g\n", func() {})
					}
					rawRedirect(engine, tt.method, tt.path, location)

					result, _ := resolve(t, context.Background(), resolver)
					assertRefused(t, result, "with a redirect (status 307)")
					if !IsRunIdentityRefusal(result.Error) {
						t.Errorf("error = %v, want an identity refusal", result.Error)
					}
					assertChanges(t, engine, tt.want...)
					assertNoDockerChange(t, api)
				})
			}
		}
	}
}

// Must fix 1: every HTTP client of the port checker returns a 3xx as the
// answer, with a malformed Location too, and sends nothing to where it points.
func TestEveryClientReturnsARedirectAsTheAnswer(t *testing.T) {
	other := newFakeEngine(t, 0)
	for _, location := range []string{"http://[::1", other.server.URL + "/run"} {
		origin := newFakeEngine(t, 0)
		rawRedirect(origin, http.MethodGet, "/run", location)
		resolver := origin.resolver(true, false)
		clients := map[string]*http.Client{
			"NewSptAPIClient":           NewSptAPIClient(origin.server.URL, time.Second).HTTPClient,
			"NewRunTracker":             NewRunTracker(origin.server.URL).HTTPClient,
			"NewPortChecker":            NewPortChecker(origin.port()).httpClient,
			"NewPortCheckerWithOptions": resolver.checker.httpClient,
			"the resolver's apiClient": resolver.apiClient(&ServiceInfo{
				Address: net.ParseIP("127.0.0.1"),
			}).HTTPClient,
		}
		for name, client := range clients {
			t.Run(name+", "+location, func(t *testing.T) {
				request, err := http.NewRequest(http.MethodGet, origin.server.URL+"/run", nil)
				if err != nil {
					t.Fatal(err)
				}
				resp, err := client.Do(request)
				if err != nil {
					t.Fatalf("Do: %v, want the 307 itself", err)
				}
				_ = resp.Body.Close()
				if resp.StatusCode != http.StatusTemporaryRedirect {
					t.Errorf("status = %d, want 307", resp.StatusCode)
				}
			})
		}
	}
	if got := other.received(); len(got) != 0 {
		t.Errorf("the address redirected to received %+v, want nothing", got)
	}
}

// Must fix 2, Opus L2 and Sol High 1: a redirect on a read that guards a write
// is not a read that failed. On the worker route, detection saw run 41 with no
// start time, and the ID alone was matched when /status was unavailable; a 307
// from /status at action time refuses every route.
func TestARedirectedStatusRefusesTheWorkerRoute(t *testing.T) {
	for _, route := range []string{"--force", "g", "f"} {
		t.Run(route, func(t *testing.T) {
			other := newFakeEngine(t, 41)
			engine, api, resolver := shownEngine(t)
			engine.since, engine.worker = "", true
			statusReads := 0
			engine.redirect = func(r *http.Request) string {
				if r.Method != http.MethodGet || r.URL.Path != "/status" {
					return ""
				}
				if statusReads++; statusReads == 1 {
					return "" // detection's read
				}
				return other.server.URL + "/status"
			}
			if route != "--force" {
				atThePrompt(resolver, route+"\n", func() {})
			}

			result, _ := resolve(t, context.Background(), resolver)
			assertRefused(t, result, "answered the read of its status (GET /status) with a redirect (status 307)")
			assertChanges(t, engine)
			assertNoDockerChange(t, api)
			if got := other.received(); len(got) != 0 {
				t.Errorf("the address redirected to received %+v, want nothing", got)
			}
		})
	}
}

// Must fix 2, Sol High 1b: a one-off 307 on the HEAD right before the first
// write, after which the engine reports no run, sends no write and changes no
// container.
func TestAOneOffRedirectOnTheHeadBeforeAWriteRefuses(t *testing.T) {
	for _, route := range []string{"--force", "g", "f"} {
		t.Run(route, func(t *testing.T) {
			engine, api, resolver := shownEngine(t)
			heads := 0
			engine.redirect = func(r *http.Request) string {
				if r.Method != http.MethodHead {
					return ""
				}
				if heads++; heads != 2 {
					return ""
				}
				engine.runID = 0 // the serving goroutine holds the lock
				return "http://127.0.0.1:1/run"
			}
			if route != "--force" {
				atThePrompt(resolver, route+"\n", func() {})
			}

			result, _ := resolve(t, context.Background(), resolver)
			assertRefused(t, result, "answered the read of its active run (HEAD /run) with a redirect (status 307)")
			assertChanges(t, engine)
			assertNoDockerChange(t, api)
		})
	}
}

// Should fix 3, Sol Medium 4: a refused redirect names its status, never its
// Location, which can carry credentials.
func TestARedirectRefusalDoesNotEchoTheLocation(t *testing.T) {
	const marker = "secret-marker-6f1c"
	for _, path := range []string{"/run", "/status"} {
		t.Run(path, func(t *testing.T) {
			engine, api, resolver := shownEngine(t)
			atThePrompt(resolver, "g\n", func() {})
			reads := 0
			engine.redirect = func(r *http.Request) string {
				switch {
				case r.URL.Path != path:
					return ""
				case path == "/run" && r.Method != http.MethodDelete:
					return ""
				case path == "/status":
					if reads++; reads == 1 {
						return ""
					}
				}
				return "http://user:pass@127.0.0.1:1" + path + "?token=" + marker
			}

			result, printed := resolve(t, context.Background(), resolver)
			assertRefused(t, result, "with a redirect (status 307)")
			for name, text := range map[string]string{"message": result.Message, "error": result.Error.Error(), "output": printed} {
				if strings.Contains(text, marker) || strings.Contains(text, "user:pass") {
					t.Errorf("%s echoes the Location: %q", name, text)
				}
			}
			assertNoDockerChange(t, api)
		})
	}
}

// Should fix 4, Opus L4: f after a prompt that showed run 41, when detection's
// HEAD failed once, refuses the run without saying that no run was shown.
func TestFAfterAShownRunDoesNotSayNoneWasShown(t *testing.T) {
	engine, api, resolver := shownEngine(t)
	heads := 0
	engine.onHead = func(e *fakeEngine) {
		heads++
		e.headStatus = 0
		if heads == 1 {
			e.headStatus = http.StatusServiceUnavailable // detection's HEAD fails once
		}
	}
	atThePrompt(resolver, "f\n", func() {})

	result, printed := resolve(t, context.Background(), resolver)
	if !strings.Contains(printed, "41") {
		t.Fatalf("the prompt did not show run 41: %q", printed)
	}
	assertRefused(t, result, "run 41 is active on the engine, and detection showed no active run on it, or could not read it")
	if strings.Contains(result.Message, "no run was shown") {
		t.Errorf("message = %q, want no claim that no run was shown", result.Message)
	}
	assertNoDockerChange(t, api)
}

// Should fix 4, Opus L4 and Sol Medium 5: a service that is not an SPT engine
// and answers HEAD /run with 200 and a bare decimal ETag gets the container
// advice, not "stop the run yourself".
func TestANonEngineWithANumericETagGetsTheContainerAdvice(t *testing.T) {
	for _, route := range []string{"--force", "f"} {
		t.Run(route, func(t *testing.T) {
			service := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodHead && r.URL.Path == "/run" {
					w.Header().Set("ETag", "41")
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
			assertRefused(t, result, "did not answer detection as an SPT engine")
			for _, want := range []string{"docker rm -f unrelated", "--api-port with --rmi-registry-port"} {
				if !strings.Contains(result.Message, want) {
					t.Errorf("message = %q, want %q", result.Message, want)
				}
			}
			if strings.Contains(result.Message, "stop the run yourself") {
				t.Errorf("message = %q, want no engine-only advice", result.Message)
			}
			assertNoDockerChange(t, api)
		})
	}
}

// Should fix 4, Fable Low 4: a run refusal at the removal check names the
// container to remove by hand.
func TestARunRefusalAtTheRemovalNamesTheContainer(t *testing.T) {
	engine, api, resolver := shownEngine(t)
	atThePrompt(resolver, "f\n", func() {})
	engine.onHead = headCounter(2, func(e *fakeEngine) { e.runID, e.since = 42, laterStart })

	result, _ := resolve(t, context.Background(), resolver)
	assertRefused(t, result, "run 42 is active on the engine, not run 41")
	if !strings.Contains(result.Message, "docker rm -f shown-engine") {
		t.Errorf("message = %q, want the container named", result.Message)
	}
	assertNoDockerChange(t, api)
}

// Should fix 6, Fable Low 2 (M3, M4): after the Docker stop, several
// publishers of the address, or a listing that fails, refuse the removal.
func TestTheRemovalRefusesSeveralPublishersOrAFailedListingAfterTheStop(t *testing.T) {
	t.Run("several publishers", func(t *testing.T) {
		engine, api, resolver := shownEngine(t)
		engine.setRun(0)
		atThePrompt(resolver, "f\n", func() {})
		both := append(slices.Clone(api.containers),
			fakeDockerWith(publishedEngine(t, "second-engine", "0.0.0.0", engine.port())).containers...)
		api.onStop = func(string) error { api.setContainers(both...); return nil }

		result, _ := resolve(t, context.Background(), resolver)
		assertRefused(t, result, "shown-engine, second-engine all publish 127.0.0.1:"+engine.port()+" now")
		assertDockerChanges(t, api, []string{"shown-engine"}, nil)
	})
	t.Run("a failed listing", func(t *testing.T) {
		engine, api, resolver := shownEngine(t)
		engine.setRun(0)
		atThePrompt(resolver, "f\n", func() {})
		var mu sync.Mutex
		fails := 0
		finder := &DockerContainerFinder{client: failingLists{fakeDockerAPI: api, mu: &mu, fails: &fails}, clock: NewFakeClock()}
		resolver.dockerFinder = finder
		resolver.checker.newFinder = func() (*DockerContainerFinder, error) { return finder, nil }
		api.onStop = func(string) error {
			mu.Lock()
			defer mu.Unlock()
			fails = 1
			return nil
		}

		result, _ := resolve(t, context.Background(), resolver)
		assertRefused(t, result, "the containers could not be listed again (failed to list containers: daemon busy)")
		assertDockerChanges(t, api, []string{"shown-engine"}, nil)
	})
}
