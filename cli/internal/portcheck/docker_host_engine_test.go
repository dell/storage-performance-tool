/*
Copyright © 2026 Dell Technologies
*/

package portcheck

import (
	"context"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"

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

// --force removes the host-network engine that holds the port and nothing else:
// not a bridge engine that only exposes the port or was told the same run port,
// and not an engine on another port.
func TestForceRemovesOnlyTheHostNetworkEngine(t *testing.T) {
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
	if !result.Success || result.Strategy != StrategyForceStop {
		t.Fatalf("result = %+v, want a successful force stop", result)
	}
	if _, removed := api.mutations(); !slices.Equal(removed, []string{"holder"}) {
		t.Errorf("removed = %v, want only holder", removed)
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
	if result.Success || !strings.Contains(result.Message, "No containers found using port "+port) {
		t.Fatalf("result = %+v, want no container found", result)
	}
	if stopped, removed := api.mutations(); len(stopped) != 0 || len(removed) != 0 {
		t.Errorf("stopped %v, removed %v; want none", stopped, removed)
	}
}

// When something other than an engine holds the port, the host-network lookup
// is not used: that engine cannot be what holds it.
func TestForceLeavesHostNetworkEngineWhenAnotherServiceHoldsThePort(t *testing.T) {
	api := fakeDockerWith(hostEngine("engine", "23100"))
	resolver := &ConflictResolver{Port: "23100", dockerFinder: newFakeFinder(api), clock: NewFakeClock()}

	result := resolver.executeForceStop(context.Background(), &ServiceInfo{IsListening: true})
	if result.Success || !strings.Contains(result.Message, "No containers found") {
		t.Fatalf("result = %+v, want no container found", result)
	}
	if _, removed := api.mutations(); len(removed) != 0 {
		t.Errorf("removed = %v, want none", removed)
	}
}
