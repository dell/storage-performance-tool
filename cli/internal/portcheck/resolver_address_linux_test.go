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
	"testing"
	"time"
)

// dialInTurn connects to each host in turn, as a dial of a host name that maps to
// all of them does (localhost on ::1 and 127.0.0.1, for one).
func dialInTurn(port string, hosts ...string) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, _ string) (net.Conn, error) {
		var lastErr error
		for _, host := range hosts {
			var dialer net.Dialer
			conn, err := dialer.DialContext(ctx, "tcp", net.JoinHostPort(host, port))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		return nil, lastErr
	}
}

// otherAddressHolder listens on 127.0.0.2:port, as another service would.
func otherAddressHolder(t *testing.T, port string) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.2:"+port)
	if err != nil {
		t.Skipf("cannot listen on 127.0.0.2:%s: %v", port, err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	return listener
}

var addressCaseNames = []string{"another user's container", "the shown container"}

// addressCases are the containers that could be removed once 127.0.0.2 answers
// in place of the engine shown on 127.0.0.1: another user's engine container on
// that address, and the engine container shown, which also publishes it.
func addressCases(t *testing.T, port string) map[string]engineContainer {
	shown := publishedEngine(t, "shown-engine", "127.0.0.1", port)
	shown.ports = append(shown.ports, publishedEngine(t, "", "127.0.0.2", port).ports...)
	return map[string]engineContainer{
		"another user's container": publishedEngine(t, "other-users-bridge-engine", "127.0.0.2", port),
		"the shown container":      shown,
	}
}

// Fable P3: --force stops and shuts down the engine shown on 127.0.0.1; it exits,
// another service still holds 127.0.0.2, and the wait ends. The address that now
// answers is not the one shown, so nothing is removed.
func TestForceRefusesWhenTheAnsweringAddressChanged(t *testing.T) {
	for _, name := range addressCaseNames {
		t.Run(name, func(t *testing.T) {
			engine := newFakeEngine(t, 41)
			port := engine.port()
			otherAddressHolder(t, port)
			container := addressCases(t, port)[name]
			engine.onShutdown = func(e *fakeEngine) { _ = e.server.Listener.Close() }
			api := fakeDockerWith(container)
			resolver := engine.resolver(true, false)
			resolver.checker.dial = dialInTurn(port, "127.0.0.1", "127.0.0.2")
			withDocker(resolver, api)
			defer resolver.Close()

			var result *ResolutionResult
			captureStdout(t, func() {
				result = within(t, func() *ResolutionResult {
					result, _ := resolver.CheckAndResolveConflicts(context.Background())
					return result
				})
			})
			if !slices.Contains(engine.received(), shutdownRequest) {
				t.Fatalf("result = %+v; the engine shown was not shut down", result)
			}
			want := "The address answering port " + port + " changed since it was shown (127.0.0.1, now 127.0.0.2)"
			if result.Success || !strings.Contains(result.Message, want) {
				t.Fatalf("result = %+v, want %q", result, want)
			}
			assertNoDockerChange(t, api)
		})
	}
}

// Fable P4: the engine shown on 127.0.0.1 ends while the prompt is open, and
// 127.0.0.2 answers when f is given. Nothing is removed.
func TestPromptForceRefusesWhenTheAnsweringAddressChanged(t *testing.T) {
	for _, name := range addressCaseNames {
		t.Run(name, func(t *testing.T) {
			engine := newFakeEngine(t, 41)
			port := engine.port()
			otherAddressHolder(t, port)
			container := addressCases(t, port)[name]
			api := fakeDockerWith(container)
			resolver := engine.resolver(false, true)
			resolver.checker.dial = dialInTurn(port, "127.0.0.1", "127.0.0.2")
			withDocker(resolver, api)
			resolver.input = newAnswerAfter("f\n", func() { _ = engine.server.Listener.Close() })
			defer resolver.Close()

			var result *ResolutionResult
			captureStdout(t, func() {
				result = within(t, func() *ResolutionResult {
					result, _ := resolver.CheckAndResolveConflicts(context.Background())
					return result
				})
			})
			if result.Success || !strings.Contains(result.Message, "changed since it was shown (127.0.0.1, now 127.0.0.2)") {
				t.Fatalf("result = %+v, want the changed address refused", result)
			}
			assertNoDockerChange(t, api)
			assertReadOnly(t, engine.received())
		})
	}
}

// Fable P7: with localhost on both families, an engine container published on
// both wildcards, on the IPv4 wildcard or on 127.0.0.1 is still removed when its
// API refuses the stop.
func TestForceRemovesAnEngineOnEachCoveringBinding(t *testing.T) {
	for _, bindings := range [][]string{{"0.0.0.0", "::"}, {"0.0.0.0"}, {"127.0.0.1"}} {
		t.Run(strings.Join(bindings, ","), func(t *testing.T) {
			network, address := "tcp", "[::]:0"
			if len(bindings) == 1 {
				network, address = "tcp4", bindings[0]+":0"
			}
			listener, err := net.Listen(network, address)
			if err != nil {
				t.Skipf("cannot listen on %s: %v", address, err)
			}
			engine := &fakeEngine{t: t, runID: 41}
			engine.onDelete = func(_ *fakeEngine, w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) }
			engine.server = httptest.NewUnstartedServer(http.HandlerFunc(engine.serve))
			engine.server.Listener = listener
			engine.server.Start()
			t.Cleanup(engine.server.Close)
			port := engine.port()

			container := publishedEngine(t, "engine", bindings[0], port)
			for _, ip := range bindings[1:] {
				container.ports = append(container.ports, publishedEngine(t, "", ip, port).ports...)
			}
			api := fakeDockerWith(container)
			api.onRemove = func(string) { engine.server.Close() }
			resolver := &ConflictResolver{
				Port:      port,
				checker:   NewPortCheckerWithOptions(port, "127.0.0.1", time.Second, 2*time.Second, 1),
				forceMode: true,
				clock:     &pollClock{},
			}
			resolver.checker.dial = dialInTurn(port, "::1", "127.0.0.1")
			withDocker(resolver, api)
			defer resolver.Close()

			var result *ResolutionResult
			captureStdout(t, func() {
				result = within(t, func() *ResolutionResult {
					result, _ := resolver.CheckAndResolveConflicts(context.Background())
					return result
				})
			})
			if _, removed := api.mutations(); !result.Success || !slices.Equal(removed, []string{"engine"}) {
				t.Fatalf("result = %+v, removed %v; want the engine container removed", result, removed)
			}
		})
	}
}
