/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	containertypes "github.com/docker/docker/api/types/container"
)

// fakeDockerAPI is a dockerAPI that returns a fixed container list, whatever
// the list options, and records every call that would change a container.
type fakeDockerAPI struct {
	mu         sync.Mutex
	containers []containertypes.Summary
	listOpts   []containertypes.ListOptions
	stopped    []string
	removed    []string
}

func (f *fakeDockerAPI) ContainerList(_ context.Context, options containertypes.ListOptions) ([]containertypes.Summary, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listOpts = append(f.listOpts, options)
	return f.containers, nil
}

func (f *fakeDockerAPI) ContainerStop(_ context.Context, containerID string, _ containertypes.StopOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stopped = append(f.stopped, containerID)
	return nil
}

func (f *fakeDockerAPI) ContainerRemove(_ context.Context, containerID string, _ containertypes.RemoveOptions) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removed = append(f.removed, containerID)
	return nil
}

func (f *fakeDockerAPI) ContainerInspect(_ context.Context, _ string) (containertypes.InspectResponse, error) {
	return containertypes.InspectResponse{}, nil
}

func (f *fakeDockerAPI) Close() error { return nil }

func (f *fakeDockerAPI) mutations() (stopped, removed []string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.stopped...), append([]string(nil), f.removed...)
}

func newFakeFinder(api *fakeDockerAPI) *DockerContainerFinder {
	return &DockerContainerFinder{client: api, clock: NewFakeClock()}
}

func TestFindContainerUsingPort_OnlyRunningTCPPublisher(t *testing.T) {
	const port = 19512
	portStr := strconv.Itoa(port)

	tests := []struct {
		name       string
		containers []containertypes.Summary
		wantID     string
	}{
		{
			name: "stopped container that published the port is not returned",
			containers: []containertypes.Summary{
				{ID: "exited", Names: []string{"/exited"}, State: containertypes.StateExited,
					Ports: []containertypes.Port{{IP: "0.0.0.0", PublicPort: port, PrivatePort: port, Type: "tcp"}}},
				{ID: "created", Names: []string{"/created"}, State: containertypes.StateCreated,
					Ports: []containertypes.Port{{IP: "0.0.0.0", PublicPort: port, PrivatePort: port, Type: "tcp"}}},
			},
		},
		{
			name: "exposed-only and remapped containers are not returned",
			containers: []containertypes.Summary{
				{ID: "expose", Names: []string{"/expose"}, State: containertypes.StateRunning,
					Ports: []containertypes.Port{{PrivatePort: port, Type: "tcp"}}},
				{ID: "remap", Names: []string{"/remap"}, State: containertypes.StateRunning,
					Ports: []containertypes.Port{{IP: "127.0.0.1", PublicPort: port + 2, PrivatePort: port, Type: "tcp"}}},
				{ID: "udp", Names: []string{"/udp"}, State: containertypes.StateRunning,
					Ports: []containertypes.Port{{IP: "0.0.0.0", PublicPort: port, PrivatePort: port, Type: "udp"}}},
			},
		},
		{
			name: "running TCP publisher is returned after non-holders",
			containers: []containertypes.Summary{
				{ID: "expose", Names: []string{"/expose"}, State: containertypes.StateRunning,
					Ports: []containertypes.Port{{PrivatePort: port, Type: "tcp"}}},
				{ID: "holder", Names: []string{"/holder"}, State: containertypes.StateRunning,
					Ports: []containertypes.Port{{IP: "0.0.0.0", PublicPort: port, PrivatePort: 9999, Type: "tcp"}}},
			},
			wantID: "holder",
		},
		{
			name: "paused TCP publisher still holds the port",
			containers: []containertypes.Summary{
				{ID: "paused", Names: []string{"/paused"}, State: containertypes.StatePaused,
					Ports: []containertypes.Port{{IP: "0.0.0.0", PublicPort: port, PrivatePort: port, Type: "tcp"}}},
			},
			wantID: "paused",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			api := &fakeDockerAPI{containers: tt.containers}
			got, err := newFakeFinder(api).FindContainerUsingPort(context.Background(), portStr)
			if err != nil {
				t.Fatalf("FindContainerUsingPort() error = %v", err)
			}
			if tt.wantID == "" {
				if got != nil {
					t.Fatalf("FindContainerUsingPort() = %q, want no container", got.ID)
				}
			} else if got == nil || got.ID != tt.wantID {
				t.Fatalf("FindContainerUsingPort() = %+v, want container %q", got, tt.wantID)
			}
			for _, opts := range api.listOpts {
				if opts.All {
					t.Errorf("ContainerList called with All: true; stopped containers cannot hold a host port")
				}
			}
		})
	}
}

// TestForce_ExposedOnlySptContainerIsNotTouched covers a non-SPT listener on
// the port while an SPT-image container only exposes the port and another maps
// a different host port to it: detection must not mark the conflict as SPT,
// and --force must neither send the stop request nor remove a container.
func TestForce_ExposedOnlySptContainerIsNotTouched(t *testing.T) {
	var (
		mu       sync.Mutex
		requests []string
	)
	listener := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		requests = append(requests, r.Method+" "+r.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusNotImplemented)
	}))
	defer listener.Close()

	_, portStr, err := net.SplitHostPort(strings.TrimPrefix(listener.URL, "http://"))
	if err != nil {
		t.Fatalf("listener address: %v", err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("listener port: %v", err)
	}

	api := &fakeDockerAPI{containers: []containertypes.Summary{
		{ID: "spt-expose", Names: []string{"/spt-expose"}, Image: "ghcr.io/dell/storage-performance-tool:5.17.0",
			State: containertypes.StateRunning,
			Ports: []containertypes.Port{{PrivatePort: uint16(port), Type: "tcp"}}},
		{ID: "spt-remap", Names: []string{"/spt-remap"}, Image: "ghcr.io/dell/storage-performance-tool:5.17.0",
			State: containertypes.StateRunning,
			Ports: []containertypes.Port{{IP: "127.0.0.1", PublicPort: uint16(port%60000) + 1, PrivatePort: uint16(port), Type: "tcp"}}},
	}}

	checker := NewPortCheckerWithOptions(portStr, "127.0.0.1", time.Second, time.Second, 1)
	checker.newFinder = func() (*DockerContainerFinder, error) { return newFakeFinder(api), nil }

	info, err := checker.CheckService(context.Background())
	if err != nil {
		t.Fatalf("CheckService() error = %v", err)
	}
	if !info.IsListening {
		t.Fatalf("CheckService() IsListening = false, want true")
	}
	if info.IsSpt {
		t.Errorf("CheckService() IsSpt = true, want false for a non-SPT listener")
	}
	if info.Container != nil {
		t.Errorf("CheckService() Container = %q, want none", info.Container.Name)
	}

	resolver := &ConflictResolver{
		Port:            portStr,
		checker:         checker,
		dockerFinder:    newFakeFinder(api),
		forceMode:       true,
		interactiveMode: false,
		clock:           NewFakeClock(),
	}

	result, err := resolver.CheckAndResolveConflicts(context.Background())
	if err != nil {
		t.Fatalf("CheckAndResolveConflicts() error = %v", err)
	}
	if result.Success {
		t.Errorf("CheckAndResolveConflicts() Success = true, want false")
	}
	if !strings.Contains(result.Message, "No containers found using port "+portStr) {
		t.Errorf("CheckAndResolveConflicts() Message = %q, want the no-containers message", result.Message)
	}

	stopped, removed := api.mutations()
	if len(stopped) != 0 || len(removed) != 0 {
		t.Errorf("containers stopped %v, removed %v; want none", stopped, removed)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, req := range requests {
		if strings.HasPrefix(req, http.MethodDelete+" ") {
			t.Errorf("listener received the SPT stop request %q", req)
		}
	}
}
