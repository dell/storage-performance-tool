/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"context"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

// deleteRequest is a stop of run 41.
var deleteRequest = engineRequest{http.MethodDelete, "/run", "41"}

// A native engine answers on 127.0.0.1 while an SPT engine container publishes the
// same port on 127.0.0.2. When the stop does not free the port, neither --force
// nor f removes that container: it does not hold the address that answered.
func TestForceLeavesAContainerOnAnotherAddress(t *testing.T) {
	for _, force := range []bool{true, false} {
		name := "prompt f"
		if force {
			name = "force"
		}
		t.Run(name, func(t *testing.T) {
			engine := failingEngine(t)
			api := fakeDockerWith(publishedEngine(t, "different-spt-engine", "127.0.0.2", engine.port()))
			resolver := engine.resolver(force, !force)
			withDocker(resolver, api)
			resolver.input = strings.NewReader("f\n")
			defer resolver.Close()

			result := within(t, func() *ResolutionResult {
				result, _ := resolver.CheckAndResolveConflicts(context.Background())
				return result
			})
			if result.Success || !strings.Contains(result.Message, "No container publishes 127.0.0.1:"+engine.port()) ||
				!strings.Contains(result.Message, "published by different-spt-engine, but on another address") {
				t.Fatalf("result = %+v, want no container matched and the candidate named", result)
			}
			if stopped, removed := api.mutations(); len(stopped) != 0 || len(removed) != 0 {
				t.Errorf("stopped %v, removed %v; want none", stopped, removed)
			}
			if !force {
				assertReadOnly(t, engine.received())
			}
		})
	}
}

// The engine container that publishes the very address that answered is removed.
func TestForceRemovesTheEngineOnTheAnsweringAddress(t *testing.T) {
	engine := failingEngine(t)
	api := fakeDockerWith(publishedEngine(t, "engine", "127.0.0.1", engine.port()))
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

func TestBindingCovers(t *testing.T) {
	v4, v6 := net.ParseIP("127.0.0.1"), net.ParseIP("::1")
	tests := []struct {
		binding string
		ip      net.IP
		covers  bool
	}{
		{"127.0.0.1", v4, true},
		{"0.0.0.0", v4, true},
		{"127.0.0.2", v4, false},
		{"::", v4, false},
		{"::1", v4, false},
		{"", v4, false},
		{"::1", v6, true},
		{"::", v6, true},
		{"0.0.0.0", v6, false},
		{"127.0.0.1", v6, false},
		{"::ffff:127.0.0.1", v4, true},
	}
	for _, tt := range tests {
		if got := bindingCovers(tt.binding, tt.ip); got != tt.covers {
			t.Errorf("bindingCovers(%q, %s) = %v, want %v", tt.binding, tt.ip, got, tt.covers)
		}
	}
}

// An IPv6 binding does not accept the CLI's IPv4 connection, so its container is
// left alone.
func TestForceLeavesAContainerOfTheOtherFamily(t *testing.T) {
	engine := failingEngine(t)
	api := fakeDockerWith(publishedEngine(t, "ipv6-engine", "::", engine.port()))
	resolver := engine.resolver(true, false)
	withDocker(resolver, api)
	defer resolver.Close()

	result := within(t, func() *ResolutionResult {
		result, _ := resolver.CheckAndResolveConflicts(context.Background())
		return result
	})
	if result.Success || !strings.Contains(result.Message, "No container publishes") {
		t.Fatalf("result = %+v, want no container matched", result)
	}
	if stopped, removed := api.mutations(); len(stopped) != 0 || len(removed) != 0 {
		t.Errorf("stopped %v, removed %v; want none", stopped, removed)
	}
}

// Two containers that could hold the address: neither is removed. One container
// listed with two bindings that both cover it is one candidate.
func TestForceRefusesTwoCandidates(t *testing.T) {
	engine := failingEngine(t)
	dual := publishedEngine(t, "dual", "0.0.0.0", engine.port())
	dual.ports = append(dual.ports, publishedEngine(t, "", "127.0.0.1", engine.port()).ports...)
	api := fakeDockerWith(dual, publishedEngine(t, "second", "127.0.0.1", engine.port()))
	resolver := engine.resolver(true, false)
	withDocker(resolver, api)
	defer resolver.Close()

	result := within(t, func() *ResolutionResult {
		result, _ := resolver.CheckAndResolveConflicts(context.Background())
		return result
	})
	if result.Success || !strings.Contains(result.Message, "Containers dual, second all publish") {
		t.Fatalf("result = %+v, want both named", result)
	}
	if stopped, removed := api.mutations(); len(stopped) != 0 || len(removed) != 0 {
		t.Errorf("stopped %v, removed %v; want none", stopped, removed)
	}

	single := fakeDockerWith(dual)
	single.onRemove = func(string) { engine.server.Close() }
	withDocker(resolver, single)
	result = within(t, func() *ResolutionResult {
		result, _ := resolver.CheckAndResolveConflicts(context.Background())
		return result
	})
	if _, removed := single.mutations(); !result.Success || !slices.Equal(removed, []string{"dual"}) {
		t.Errorf("result = %+v, removed %v; want dual removed", result, removed)
	}
}

// An upper-case scheme is dialled over TCP by the Docker client, so the daemon
// behind it may be anywhere: nothing is removed, by either route.
func TestForceRemovesNothingThroughAnUpperCaseScheme(t *testing.T) {
	t.Run("detected container", func(t *testing.T) {
		holder := newLookalikeHolder(t)
		holder.docker.daemonHost = "UNIX://127.0.0.1:2375"
		resolver := holder.resolver(true, false)
		defer resolver.Close()

		result, err := resolver.CheckAndResolveConflicts(context.Background())
		if err != nil || result.Success || !strings.Contains(result.Message, "not reached through a local socket") {
			t.Fatalf("result = %+v, %v, want the daemon refused", result, err)
		}
		holder.assertUntouched(t)
	})
	t.Run("host-network engine", func(t *testing.T) {
		engine := failingEngine(t)
		api := fakeDockerWith(hostEngine("remote-engine", engine.port()))
		api.daemonHost = "Unix://127.0.0.1:2375"
		resolver := engine.resolver(true, false)
		withDocker(resolver, api)
		defer resolver.Close()

		result := within(t, func() *ResolutionResult {
			result, _ := resolver.CheckAndResolveConflicts(context.Background())
			return result
		})
		if result.Success || !strings.Contains(result.Message, "not reached through a local socket") {
			t.Fatalf("result = %+v, want the daemon refused", result)
		}
		if stopped, removed := api.mutations(); len(stopped) != 0 || len(removed) != 0 {
			t.Errorf("stopped %v, removed %v; want none", stopped, removed)
		}
	})
}

// shownAt is detection's result for run 41 started at since.
func shownAt(since string) *ServiceInfo {
	info := confirmedEngine()
	info.Status.RunningSince = since
	return info
}

// At g, a run whose start time was seen must show it again: a /status that fails,
// or that reads the run as ended, leaves it unknown and nothing is sent.
func TestPromptAnswerNeedsTheShownStartTime(t *testing.T) {
	tests := []struct {
		name   string
		change func(*fakeEngine)
		want   string
	}{
		{"status fails", func(e *fakeEngine) { e.statusCode = http.StatusServiceUnavailable }, "could not be read"},
		{"status fails with a later run", func(e *fakeEngine) {
			e.since, e.statusCode = laterStart, http.StatusServiceUnavailable
		}, "could not be read"},
		{"status 500", func(e *fakeEngine) { e.statusCode = http.StatusInternalServerError }, "could not be read"},
		{"status reads the run stopped", func(e *fakeEngine) { e.statusState = "STOPPED" }, "has ended and its cleanup is still running"},
		{"status without a start time", func(e *fakeEngine) { e.since = "" }, "gives no start time"},
		{"status names another run", func(e *fakeEngine) { e.statusRunID = 99 }, "names run 99"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			engine := newFakeEngine(t, 41)
			engine.since = firstStart
			tt.change(engine)
			resolver := engine.resolver(false, true)
			defer resolver.Close()

			result := gracefulStopWithin(t, context.Background(), resolver, shownAt(firstStart))
			if result.Success || !strings.Contains(result.Message, tt.want) || !strings.Contains(result.Message, "it was not stopped") {
				t.Fatalf("result = %+v, want %q and nothing stopped", result, tt.want)
			}
			assertReadOnly(t, engine.received())
		})
	}
}

// After the stop, a /status that fails leaves the run unknown: no shutdown, by
// either route.
func TestNoShutdownWhenTheStatusIsLostAfterTheStop(t *testing.T) {
	for _, force := range []bool{false, true} {
		engine := newFakeEngine(t, 41)
		engine.since = firstStart
		engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
			e.since, e.statusCode = laterStart, http.StatusServiceUnavailable
			w.WriteHeader(http.StatusOK)
		}
		resolver := engine.resolver(force, !force)
		defer resolver.Close()

		result := gracefulStopWithin(t, context.Background(), resolver, shownAt(firstStart))
		if result.Success || !strings.Contains(result.Message, "could not be read") {
			t.Errorf("force %v: result = %+v, want the unknown run reported", force, result)
		}
		if got, want := changesTo(engine.received()), []engineRequest{deleteRequest}; !slices.Equal(got, want) {
			t.Errorf("force %v: engine changes = %+v, want only the stop", force, got)
		}
	}
}

// A run stopped during its cleanup reads as ended with the same ID, and the
// engine is shut down.
func TestShutdownFollowsAStoppedRunWithAStartTime(t *testing.T) {
	for _, force := range []bool{false, true} {
		engine := newFakeEngine(t, 41)
		engine.since = firstStart
		engine.onDelete = func(e *fakeEngine, w http.ResponseWriter) {
			e.since, e.statusState = laterStart, "STOPPED"
			w.WriteHeader(http.StatusOK)
		}
		resolver := engine.resolver(force, !force)
		resolver.clock = closeAfterPolls(engine, 1)
		defer resolver.Close()

		var result *ResolutionResult
		printed := captureStdout(t, func() {
			result = gracefulStopWithin(t, context.Background(), resolver, shownAt(firstStart))
		})
		if !result.Success || !slices.Contains(engine.received(), shutdownRequest) {
			t.Errorf("force %v: result = %+v, want the engine shut down", force, result)
		}
		if !strings.Contains(printed, "Stopped SPT run 41") {
			t.Errorf("force %v: printed %q, want the stop reported", force, printed)
		}
	}
}

// A worker shown at the prompt whose /status then fails is not stopped: what it
// runs is unknown. Under --force, which stops it, it is not called stopped and
// not shut down.
func TestLostStatusOfAWorker(t *testing.T) {
	worker := func() *ServiceInfo {
		info := confirmedEngine()
		info.Status.Worker = true
		return info
	}
	t.Run("prompt", func(t *testing.T) {
		engine := newFakeEngine(t, 41)
		engine.worker = true
		engine.statusCode = http.StatusServiceUnavailable
		resolver := engine.resolver(false, true)
		defer resolver.Close()

		result := gracefulStopWithin(t, context.Background(), resolver, worker())
		if result.Success || !strings.Contains(result.Message, "could not be read") {
			t.Fatalf("result = %+v, want the unknown run reported", result)
		}
		assertReadOnly(t, engine.received())
	})
	t.Run("force", func(t *testing.T) {
		engine := newFakeEngine(t, 41)
		engine.worker = true
		engine.statusCode = http.StatusServiceUnavailable
		engine.onDelete = func(_ *fakeEngine, w http.ResponseWriter) { w.WriteHeader(http.StatusOK) }
		resolver := engine.resolver(true, false)
		defer resolver.Close()

		var result *ResolutionResult
		printed := captureStdout(t, func() {
			result = gracefulStopWithin(t, context.Background(), resolver, worker())
		})
		if result.Success || slices.Contains(engine.received(), shutdownRequest) {
			t.Errorf("result = %+v, want no shutdown", result)
		}
		if strings.Contains(printed, "Stopped SPT run") || !strings.Contains(printed, "may be a worker") {
			t.Errorf("printed %q, want the worker wording", printed)
		}
	})
}

// An entry engine whose /status a refused POST /run overwrote names another run,
// as a worker's does. g still stops the run shown, which had no start time, and
// the message holds for both.
func TestStatusNamingAnotherRunDoesNotRefuseG(t *testing.T) {
	engine := newFakeEngine(t, 41)
	engine.statusRunID = 99
	resolver := engine.resolver(false, true)
	resolver.clock = closeAfterPolls(engine, 1)
	defer resolver.Close()
	info, err := resolver.checkService(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !info.Status.Worker {
		t.Fatalf("status = %+v, want it marked as naming another run", info.Status)
	}

	var result *ResolutionResult
	printed := captureStdout(t, func() { result = gracefulStopWithin(t, context.Background(), resolver, info) })
	if !result.Success || !slices.Contains(engine.received(), deleteRequest) {
		t.Fatalf("result = %+v, want the run stopped", result)
	}
	if !strings.Contains(printed, "may be a worker of it") || strings.Contains(printed, "Stopped SPT run") {
		t.Errorf("printed %q, want wording true for a worker and an entry", printed)
	}
}

// A run shown without a start time, as a worker's, that has one at g is a later
// run that took the ID on this engine.
func TestWorkerRunThatGainsAStartTimeIsALaterRun(t *testing.T) {
	engine := newFakeEngine(t, 41)
	engine.since = firstStart
	resolver := engine.resolver(false, true)
	defer resolver.Close()
	info := confirmedEngine()
	info.Status.Worker = true

	result := gracefulStopWithin(t, context.Background(), resolver, info)
	if result.Success || !strings.Contains(result.Message, "had none before") {
		t.Fatalf("result = %+v, want the later run reported", result)
	}
	assertReadOnly(t, engine.received())
}

// Only a RUNNING /status gives the run's start time; an ended run's since is
// when it ended.
func TestDetectionTakesNoStartTimeFromAnEndedRun(t *testing.T) {
	engine := newFakeEngine(t, 41)
	engine.since, engine.statusState = firstStart, "STOPPED"
	status, err := NewSptAPIClient(engine.server.URL, 0).GetStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status.RunningSince != "" {
		t.Errorf("RunningSince = %q for a stopped run, want none", status.RunningSince)
	}
}

// The prompt prints the note on a run without a start time.
func TestPromptPrintsTheIdentityNote(t *testing.T) {
	engine := newFakeEngine(t, 41)
	resolver := engine.resolver(false, true)
	resolver.input = strings.NewReader("a\n")
	defer resolver.Close()

	printed := captureStdout(t, func() { _, _ = resolver.CheckAndResolveConflicts(context.Background()) })
	if !strings.Contains(printed, "Note: this engine reports no start time for run 41") {
		t.Errorf("printed %q, want the note", printed)
	}
}

// A lookalike is described by its container, not as an SPT service.
func TestLookalikeDescription(t *testing.T) {
	info := &ServiceInfo{IsListening: true, IsSpt: true, Container: &ContainerInfo{Name: "dashboard", Image: "spt-dashboard"}}
	if got := info.GetServiceDescription(); got != "Container that looks like SPT, with no SPT API: dashboard (spt-dashboard)" {
		t.Errorf("GetServiceDescription() = %q", got)
	}
}

// When the CLI's own limit ends the check, the message says so, not Ctrl-C.
func TestForceVerificationReportsItsTimeLimit(t *testing.T) {
	engine := newFakeEngine(t, 41)
	resolver := engine.resolver(true, false)
	defer resolver.Close()
	ctx, cancel := context.WithTimeout(context.Background(), -time.Second)
	defer cancel()

	result := within(t, func() *ResolutionResult { return resolver.verifyResolution(ctx, StrategyForceStop) })
	if result.Success || !strings.Contains(result.Message, "the time limit for it ran out") ||
		strings.Contains(result.Message, "Interrupted") {
		t.Fatalf("result = %+v, want the time limit reported", result)
	}
}

// --force on a context that ended prints no force stop and removes nothing.
func TestForceAfterCancellationPrintsNoForceStop(t *testing.T) {
	holder := newLookalikeHolder(t)
	resolver := holder.resolver(true, false)
	defer resolver.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	info := &ServiceInfo{IsListening: true, IsSpt: true}

	var result *ResolutionResult
	printed := captureStdout(t, func() { result, _ = resolver.executeForceResolution(ctx, info) })
	if result.Success || !strings.Contains(result.Message, "Interrupted") || strings.Contains(printed, "Force stopping") {
		t.Fatalf("result = %+v, printed %q; want the interruption alone", result, printed)
	}
	holder.assertUntouched(t)
}

// Under --force, a stop whose /status cannot be read is not called the run's end,
// and the engine is not shut down.
func TestForceStopWithAnUnreadableStatus(t *testing.T) {
	engine := newFakeEngine(t, 41)
	engine.statusCode = http.StatusServiceUnavailable
	engine.onDelete = func(_ *fakeEngine, w http.ResponseWriter) { w.WriteHeader(http.StatusOK) }
	resolver := engine.resolver(true, false)
	defer resolver.Close()

	var result *ResolutionResult
	printed := captureStdout(t, func() {
		result = gracefulStopWithin(t, context.Background(), resolver, confirmedEngine())
	})
	if result.Success || slices.Contains(engine.received(), shutdownRequest) {
		t.Errorf("result = %+v, want no shutdown", result)
	}
	if strings.Contains(printed, "Stopped SPT run") || !strings.Contains(printed, "its status could not be read") {
		t.Errorf("printed %q, want the stop reported as unconfirmed", printed)
	}
}
