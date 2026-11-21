/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	containertypes "github.com/docker/docker/api/types/container"
)

// engineContainer describes a container for the fake Docker API.
type engineContainer struct {
	id      string
	state   string
	labels  map[string]string
	network containertypes.NetworkMode
	args    []string
	ports   []containertypes.Port
}

func nodeLabels() map[string]string {
	return map[string]string{
		constants.DockerLabelManaged: "true",
		constants.DockerLabelRole:    constants.DockerRoleNode,
	}
}

func hostEngine(id, port string) engineContainer {
	return engineContainer{
		id: id, state: containertypes.StateRunning, labels: nodeLabels(), network: "host",
		args: []string{constants.SptNodeCommand, constants.EngineRunPortArgPrefix + port},
	}
}

func fakeDockerWith(containers ...engineContainer) *fakeDockerAPI {
	api := &fakeDockerAPI{inspects: map[string]containertypes.InspectResponse{}}
	for _, c := range containers {
		api.containers = append(api.containers, containertypes.Summary{
			ID: c.id, Names: []string{"/" + c.id}, State: c.state, Labels: c.labels, Ports: c.ports,
		})
		api.inspects[c.id] = containertypes.InspectResponse{ContainerJSONBase: &containertypes.ContainerJSONBase{
			Args:       c.args,
			HostConfig: &containertypes.HostConfig{NetworkMode: c.network},
		}}
	}
	return api
}

func TestFindHostNetworkEngine(t *testing.T) {
	const port = "23100"
	variant := func(id string, change func(*engineContainer)) engineContainer {
		c := hostEngine(id, port)
		change(&c)
		return c
	}
	tests := []struct {
		name       string
		containers []engineContainer
		wantID     string
		wantErr    string
	}{
		{name: "the engine serving the port", containers: []engineContainer{hostEngine("other", "23200"), hostEngine("engine", port)}, wantID: "engine"},
		{name: "bridge network", containers: []engineContainer{variant("c", func(c *engineContainer) { c.network = "bridge" })}},
		{name: "not managed by SPT", containers: []engineContainer{variant("c", func(c *engineContainer) { delete(c.labels, constants.DockerLabelManaged) })}},
		{name: "managed label false", containers: []engineContainer{variant("c", func(c *engineContainer) { c.labels[constants.DockerLabelManaged] = "false" })}},
		{name: "verify role", containers: []engineContainer{variant("c", func(c *engineContainer) { c.labels[constants.DockerLabelRole] = constants.DockerRoleVerify })}},
		{name: "exited", containers: []engineContainer{variant("c", func(c *engineContainer) { c.state = containertypes.StateExited })}},
		{name: "port is only a prefix of the argument", containers: []engineContainer{hostEngine("c", port+"1")}},
		{name: "argument is only a prefix of the port", containers: []engineContainer{hostEngine("c", port[:4])}},
		{name: "two candidates", containers: []engineContainer{hostEngine("a", port), hostEngine("b", port)}, wantErr: "more than one"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newFakeFinder(fakeDockerWith(tt.containers...)).FindHostNetworkEngine(context.Background(), port)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("error = %v", err)
			}
			switch {
			case tt.wantID == "" && got != nil:
				t.Fatalf("found %q, want none", got.ID)
			case tt.wantID != "" && (got == nil || got.ID != tt.wantID):
				t.Fatalf("found %+v, want %q", got, tt.wantID)
			}
		})
	}
}

func listenerPort(t *testing.T, port string) uint16 {
	t.Helper()
	number, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		t.Fatalf("port %q: %v", port, err)
	}
	return uint16(number)
}

// failingEngine is an engine whose run cannot be stopped, so --force goes on to
// the container.
func failingEngine(t *testing.T) *fakeEngine {
	t.Helper()
	engine := newFakeEngine(t, 41)
	engine.onDelete = func(_ *fakeEngine, w http.ResponseWriter) { w.WriteHeader(http.StatusInternalServerError) }
	return engine
}

// --force removes no host-network engine: nothing shows that the container, and
// not another process, holds the port. It names the one with that run port, and
// not a bridge engine that only exposes the port or an engine on another port.
func TestForceNamesButLeavesTheHostNetworkEngine(t *testing.T) {
	engine := failingEngine(t)
	port := engine.port()
	portNumber := listenerPort(t, port)
	bridge := engineContainer{
		id: "bridge-exposes", state: containertypes.StateRunning, labels: nodeLabels(), network: "bridge",
		args:  []string{constants.SptNodeCommand, constants.EngineRunPortArgPrefix + port},
		ports: []containertypes.Port{{PrivatePort: portNumber, Type: "tcp"}},
	}
	api := fakeDockerWith(bridge, hostEngine("other-port", "1"), hostEngine("holder", port))
	api.onRemove = func(string) { engine.server.Close() }
	resolver := engine.resolver(true, false)
	resolver.dockerFinder = newFakeFinder(api)
	defer resolver.Close()

	result, err := resolver.CheckAndResolveConflicts(context.Background())
	if err != nil {
		t.Fatalf("CheckAndResolveConflicts() error = %v", err)
	}
	if result.Success || !strings.Contains(result.Message, "Container holder runs on host networking") ||
		!strings.Contains(result.Message, "docker rm -f holder") {
		t.Fatalf("result = %+v, want holder named and left running", result)
	}
	if stopped, removed := api.mutations(); len(stopped) != 0 || len(removed) != 0 {
		t.Errorf("stopped %v, removed %v; want none", stopped, removed)
	}
}

// With only a bridge container that exposes the port, --force removes nothing.
func TestForceNeverRemovesAnExposingBridgeContainer(t *testing.T) {
	engine := failingEngine(t)
	port := engine.port()
	portNumber := listenerPort(t, port)
	api := fakeDockerWith(engineContainer{
		id: "bridge-exposes", state: containertypes.StateRunning, labels: nodeLabels(), network: "bridge",
		args:  []string{constants.SptNodeCommand, constants.EngineRunPortArgPrefix + port},
		ports: []containertypes.Port{{PrivatePort: portNumber, Type: "tcp"}},
	})
	resolver := engine.resolver(true, false)
	resolver.dockerFinder = newFakeFinder(api)
	defer resolver.Close()

	result, err := resolver.CheckAndResolveConflicts(context.Background())
	if err != nil {
		t.Fatalf("CheckAndResolveConflicts() error = %v", err)
	}
	if result.Success || !strings.Contains(result.Message, "No container publishes 127.0.0.1:"+port) {
		t.Fatalf("result = %+v, want no container found", result)
	}
	if stopped, removed := api.mutations(); len(stopped) != 0 || len(removed) != 0 {
		t.Errorf("stopped %v, removed %v; want none", stopped, removed)
	}
}

// When something other than an engine holds the port, the host-network lookup
// is not used: that engine cannot be what holds it.
func TestForceLeavesHostNetworkEngineWhenAnotherServiceHoldsThePort(t *testing.T) {
	other := httptest.NewServer(http.NotFoundHandler())
	defer other.Close()
	_, port, err := net.SplitHostPort(other.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	api := fakeDockerWith(hostEngine("engine", port))
	resolver := &ConflictResolver{
		Port: port, checker: NewPortCheckerWithOptions(port, "127.0.0.1", time.Second, time.Second, 1),
		dockerFinder: newFakeFinder(api), clock: NewFakeClock(),
	}

	result := resolver.executeForceStop(context.Background(), &ServiceInfo{IsListening: true, Address: net.ParseIP("127.0.0.1")})
	if result.Success || !strings.Contains(result.Message, "No container publishes") || strings.Contains(result.Message, "engine") {
		t.Fatalf("result = %+v, want no container found", result)
	}
	if _, removed := api.mutations(); len(removed) != 0 {
		t.Errorf("removed = %v, want none", removed)
	}
}
