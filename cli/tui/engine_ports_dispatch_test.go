/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// errLaunchOverride is what an extension's launch overrides return, so a test sees that they ran.
var errLaunchOverride = errors.New("extension launch override ran")

// errEmbeddedLaunch ends a launch that reached the embedded manager instead of the override.
var errEmbeddedLaunch = errors.New("the embedded manager launched the engine")

// extensionLaunch is one call to an extension's launch override.
type extensionLaunch struct {
	kind    string   // worker, entry or node
	apiPort string   // the API port a node-mode start is given
	args    []string // the arguments a node-mode start is given
}

// extensionRemoteManager is a wrapper an extension can assign to HostConnection.DockerManager: it
// embeds SPT's remote manager and overrides its exported launches. Embedding also gives it the
// manager's unexported port-aware starts, which must never run in place of the overrides.
type extensionRemoteManager struct {
	*RemoteDockerManager
	mu       sync.Mutex
	launched []extensionLaunch
}

func newExtensionRemoteManager(t *testing.T) (*extensionRemoteManager, *command.MockCommandExecutor) {
	t.Helper()
	mgr, executor, _ := newTestRemoteManager(t)
	mgr.proberRun = func(context.Context, string, time.Duration) error { return errEmbeddedLaunch }
	return &extensionRemoteManager{RemoteDockerManager: mgr}, executor
}

func (m *extensionRemoteManager) override(launch extensionLaunch) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.launched = append(m.launched, launch)
	return "", errLaunchOverride
}

func (m *extensionRemoteManager) launches() []extensionLaunch {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]extensionLaunch(nil), m.launched...)
}

func (m *extensionRemoteManager) StartWorkerNodeContainerContext(
	context.Context, string, string, int, int, []string,
) (string, error) {
	return m.override(extensionLaunch{kind: "worker"})
}

func (m *extensionRemoteManager) StartEntryNodeContainerContext(
	context.Context, string, []string, []string, string,
) (string, error) {
	return m.override(extensionLaunch{kind: "entry"})
}

func (m *extensionRemoteManager) StartContainerInNodeModeContext(
	_ context.Context, _ string, apiPort, _ string, args []string,
) (string, error) {
	return m.override(extensionLaunch{kind: "node", apiPort: apiPort, args: args})
}

// extensionLocalManager is the same kind of wrapper around SPT's local Docker manager.
type extensionLocalManager struct {
	*DockerManager
	launched []string
}

func (m *extensionLocalManager) StartWorkerNodeContainerContext(
	context.Context, string, string, int, int, []string,
) (string, error) {
	m.launched = append(m.launched, "worker")
	return "", errLaunchOverride
}

func (m *extensionLocalManager) StartEntryNodeContainerContext(
	context.Context, string, []string, []string, string,
) (string, error) {
	m.launched = append(m.launched, "entry")
	return "", errLaunchOverride
}

// dispatchPorts are the default engine ports and a run's other ports.
var dispatchPorts = []struct {
	name  string
	ports enginePorts
}{
	{"default ports", defaultEnginePorts()},
	{"other ports", testRunPorts},
}

func setEnginePorts(o *MultiHostOrchestrator, ports enginePorts) {
	o.SetAPIPort(strconv.Itoa(ports.api))
	o.SetRMIRegistryPort(ports.registry)
}

func requireNoDockerRun(t *testing.T, executor *command.MockCommandExecutor) {
	t.Helper()
	if runs := executor.GetExecutedCommandsMatching("docker run"); len(runs) != 0 {
		t.Errorf("the embedded manager sent %d docker run command(s), bypassing the extension's override", len(runs))
	}
}

// requireOverrideOrRefusal checks one RMI start of an extension's manager: on the default ports
// its override runs; on other ports the start is refused, and the override does not run.
func requireOverrideOrRefusal(t *testing.T, err error, ports enginePorts, role string, launched []string) {
	t.Helper()
	if ports.isDefault() {
		if err == nil || !strings.Contains(err.Error(), errLaunchOverride.Error()) {
			t.Errorf("%s start error = %v, want the override's", role, err)
		}
		if !slices.Contains(launched, role) {
			t.Errorf("overrides run = %v, want the %s override", launched, role)
		}
		return
	}
	want := "cannot start an RMI " + role + " on API port 19300 and RMI registry port 11099; it supports only 9999 and 1099"
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Errorf("%s start error = %v, want %q", role, err, want)
	}
	if slices.Contains(launched, role) {
		t.Errorf("the %s override ran on ports it was never asked to serve", role)
	}
}

func launchKinds(launches []extensionLaunch) []string {
	kinds := make([]string, 0, len(launches))
	for _, launch := range launches {
		kinds = append(kinds, launch.kind)
	}
	return kinds
}

// Review, fix round 1: dispatch tried the managers' unexported port-aware start first, so an
// extension's wrapper that embeds a built-in manager lost its exported launch overrides even on
// 9999 and 1099. On the default ports its overrides run, as before; on other ports its start is
// refused with the clear error, and nothing is launched.
func TestDistributedStartRunsAnExtensionsLaunchOverrides(t *testing.T) {
	for _, role := range []string{"worker", "entry"} {
		for _, tc := range dispatchPorts {
			t.Run(role+" on "+tc.name, func(t *testing.T) {
				clearEngineEnvironment(t)
				ext, executor := newExtensionRemoteManager(t)
				hosts := []*hostparse.HostInfo{{Host: "entry.example", Original: "entry.example"}}
				if role == "worker" {
					hosts = append(hosts, &hostparse.HostInfo{Host: "worker.example", Original: "worker.example"})
				}
				o := NewMultiHostOrchestrator(hosts, len(hosts))
				setEnginePorts(o, tc.ports)
				o.detectAdvIP = func(context.Context, *hostparse.HostInfo) (string, error) { return "198.51.100.1", nil }
				if role == "worker" {
					o.hosts[0].DockerManager, o.hosts[1].DockerManager = NewMockDockerManager(), ext
				} else {
					o.hosts[0].DockerManager = ext
				}

				err := o.StartDistributedTestWithContent(context.Background(), constants.DefaultSptImage,
					scenario.ScenarioParams{WorkloadType: "write", Threads: 1}, []byte("Load.config({}).run();"))
				requireOverrideOrRefusal(t, err, tc.ports, role, launchKinds(ext.launches()))
				requireNoDockerRun(t, executor)
			})
		}
	}
}

// A LIST run starts its primary as an entry, and its idle hosts in node mode: the primary follows
// the RMI entry rule, and each idle engine's override runs on any ports, given the run's.
func TestListRunRunsAnExtensionsLaunchOverrides(t *testing.T) {
	for _, tc := range dispatchPorts {
		t.Run(tc.name, func(t *testing.T) {
			clearEngineEnvironment(t)
			primary, primaryExecutor := newExtensionRemoteManager(t)
			idle, idleExecutor := newExtensionRemoteManager(t)
			o := NewMultiHostOrchestrator([]*hostparse.HostInfo{
				{Host: "primary.example", Original: "primary.example"},
				{Host: "idle.example", Original: "idle.example"},
			}, 2)
			setEnginePorts(o, tc.ports)
			o.hosts[0].DockerManager, o.hosts[1].DockerManager = primary, idle
			o.hosts[0].SetStatus(HostStatusReady)
			o.hosts[1].SetStatus(HostStatusReady)
			wrapper := NewMultiHostTestOrchestrator(o)
			wrapper.SetCallbacks(func(*TestStatus) {}, func(*MultiNodeMetricsUpdate) {}, func(string) {}, func(string) {})
			wrapper.waitForAPIs = func(context.Context, time.Duration) error { return nil }

			params := scenario.ScenarioParams{WorkloadType: scenario.WorkloadTypeList, Threads: 1, Bucket: "demo", Endpoint: "http://minio:9000"}
			err := wrapper.StartTestWithLaunchHooks(context.Background(), constants.DefaultSptImage, params, NewLaunchHooks(func() {}))
			requireOverrideOrRefusal(t, err, tc.ports, "entry", launchKinds(primary.launches()))
			requireNoDockerRun(t, primaryExecutor)

			want := []extensionLaunch{{kind: "node", apiPort: strconv.Itoa(tc.ports.api), args: o.nodeModePortArgs()}}
			if got := idle.launches(); !reflect.DeepEqual(got, want) {
				t.Errorf("idle engine launches = %+v, want %+v", got, want)
			}
			requireNoDockerRun(t, idleExecutor)
		})
	}
}

// The same rule holds for a wrapper around SPT's local Docker manager, at the start helpers.
func TestEngineStartsRunTheOverridesOfAWrappedLocalManager(t *testing.T) {
	for _, tc := range dispatchPorts {
		t.Run(tc.name, func(t *testing.T) {
			clearEngineEnvironment(t)
			docker := &fakeDockerClient{}
			ext := &extensionLocalManager{DockerManager: &DockerManager{client: docker, ctx: context.Background()}}
			t.Cleanup(ext.Close)
			image := constants.DefaultSptImage
			worker := "198.51.100.1:" + strconv.Itoa(tc.ports.registry)

			_, workerErr := startWorkerNodeContainerContext(
				context.Background(), ext, image, "198.51.100.1", tc.ports, constants.DefaultRMIPortStart, 1, nil)
			requireOverrideOrRefusal(t, workerErr, tc.ports, "worker", ext.launched)
			_, entryErr := startEntryNodeContainerContext(
				context.Background(), ext, image, tc.ports, []string{worker}, nil, constants.HostNetworkMode)
			requireOverrideOrRefusal(t, entryErr, tc.ports, "entry", ext.launched)
			if docker.createContainerConfig != nil {
				t.Errorf("the embedded manager created a container, bypassing the extension's override: %q",
					docker.createContainerConfig.Cmd)
			}
		})
	}
}

// A lone remote engine and the local engine start in node mode, which carries the run's ports in
// the exported launch's arguments, so an extension's override runs on any ports. These starts
// never took the port-aware path; the test guards them.
func TestNodeModeStartsRunAnExtensionsOverrideOnAnyPorts(t *testing.T) {
	image := constants.DefaultSptImage
	args := []string{"--storage-driver-type=s3"}
	for _, tc := range dispatchPorts {
		registryArgs := nonDefaultNodePortArgs(tc.ports)
		want := []extensionLaunch{{kind: "node", apiPort: strconv.Itoa(tc.ports.api), args: slices.Concat(registryArgs, args)}}

		t.Run("lone remote on "+tc.name, func(t *testing.T) {
			clearEngineEnvironment(t)
			ext, executor := newExtensionRemoteManager(t)
			o := NewMultiHostOrchestrator([]*hostparse.HostInfo{{Host: "lone.example", Original: "lone.example"}}, 1)
			o.SetNotifier(func(string) {})
			setEnginePorts(o, tc.ports)
			o.hosts[0].DockerManager = ext
			o.hosts[0].SetStatus(HostStatusReady)

			if err := o.StartContainers(context.Background(), image, args); err == nil {
				t.Fatal("StartContainers() error = nil, want the override's failure")
			}
			if err := o.hosts[0].GetError(); err == nil || !errors.Is(err, errLaunchOverride) {
				t.Errorf("host error = %v, want the override's", err)
			}
			if got := ext.launches(); !reflect.DeepEqual(got, want) {
				t.Errorf("launches = %+v, want %+v", got, want)
			}
			requireNoDockerRun(t, executor)
		})

		t.Run("local on "+tc.name, func(t *testing.T) {
			clearEngineEnvironment(t)
			ext, executor := newExtensionRemoteManager(t)
			o := NewTestOrchestrator(ext, strconv.Itoa(tc.ports.api), t.TempDir())
			params := scenario.Params{WorkloadType: "write", Threads: 1}
			if !tc.ports.isDefault() {
				params.RMIRegistryPort = tc.ports.registry
			}

			err := o.StartTestWithContent(context.Background(), image, params, []byte("Load.config({}).run();"), nil)
			if !errors.Is(err, errLaunchOverride) {
				t.Errorf("StartTestWithContent() error = %v, want the override's", err)
			}
			launches := ext.launches()
			if len(launches) != 1 || launches[0].kind != "node" || launches[0].apiPort != strconv.Itoa(tc.ports.api) {
				t.Fatalf("launches = %+v, want one node-mode start on API port %d", launches, tc.ports.api)
			}
			hasRegistryArg := slices.ContainsFunc(launches[0].args, func(arg string) bool {
				return strings.HasPrefix(arg, constants.EngineNodePortArgPrefix)
			})
			if hasRegistryArg != !tc.ports.isDefault() || (len(registryArgs) > 0 && !slices.Contains(launches[0].args, registryArgs[0])) {
				t.Errorf("local engine args = %q, want the registry argument %q", launches[0].args, registryArgs)
			}
			requireNoDockerRun(t, executor)
		})
	}
}
