/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	dockerconf "github.com/dell/storage-performance-tool/cli/internal/docker"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
	"github.com/dell/storage-performance-tool/cli/internal/poolsizing"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/docker/go-connections/nat"
)

var remoteContainerNameTime = regexp.MustCompile(`--name spt-(node|worker|entry)-[0-9]+-`)

// lastDockerRun returns the last docker run command a remote manager sent, with the container
// name's timestamp masked.
func lastDockerRun(t *testing.T, mock *command.MockCommandExecutor) string {
	t.Helper()
	executed := mock.GetExecutedCommandsMatching("docker run")
	if len(executed) == 0 {
		t.Fatal("no docker run command was sent")
	}
	return remoteContainerNameTime.ReplaceAllString(
		strings.Join(executed[len(executed)-1].Command, " "), "--name spt-$1-<time>-")
}

// localContainer describes what a local Docker manager asked the daemon for.
func localContainer(f *fakeDockerClient) string {
	exposed := make([]string, 0, len(f.createContainerConfig.ExposedPorts))
	for port := range f.createContainerConfig.ExposedPorts {
		exposed = append(exposed, string(port))
	}
	sort.Strings(exposed)
	bindings := make([]string, 0, len(f.createHostConfig.PortBindings))
	for port, binds := range f.createHostConfig.PortBindings {
		for _, bind := range binds {
			bindings = append(bindings, fmt.Sprintf("%s->%s:%s", port, bind.HostIP, bind.HostPort))
		}
	}
	sort.Slice(bindings, func(i, j int) bool {
		return nat.Port(strings.Split(bindings[i], "->")[0]).Int() < nat.Port(strings.Split(bindings[j], "->")[0]).Int()
	})
	return fmt.Sprintf("cmd=%q exposed=%v bindings=%v network=%q",
		f.createContainerConfig.Cmd, exposed, bindings, f.createHostConfig.NetworkMode)
}

type recordingPortPreflight struct {
	identityPreflight
	mu     sync.Mutex
	checks []string
}

func (p *recordingPortPreflight) CheckPorts(
	_ context.Context, host *hostparse.HostInfo, registryPort, apiPort, rmiPortStart, rmiPortCount int,
) (*dockerconf.ConflictInfo, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.checks = append(p.checks, fmt.Sprintf("%s registry=%d api=%d rmi=%d+%d",
		host.Original, registryPort, apiPort, rmiPortStart, rmiPortCount))
	return &dockerconf.ConflictInfo{}, nil
}

func (p *recordingPortPreflight) sortedChecks() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	checks := append([]string(nil), p.checks...)
	sort.Strings(checks)
	return checks
}

// clearEngineEnvironment keeps host settings that change the generated commands out of a test.
func clearEngineEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv(constants.EnvSptJavaOpts, "")
	t.Setenv(constants.EnvRdmaEnabled, "")
	poolsizing.ClearLaunchOptions()
	t.Cleanup(poolsizing.ClearLaunchOptions)
}

// defaultEngineCommands starts every kind of engine on the default ports, through the managers
// and through the orchestrator, and returns what each one asked Docker for.
func defaultEngineCommands(t *testing.T) []string {
	t.Helper()
	clearEngineEnvironment(t)
	image := constants.DefaultSptImage
	args := []string{"--storage-driver-type=s3"}
	var got []string

	for _, network := range []string{constants.HostNetworkMode, constants.BridgeNetworkMode} {
		mgr, mock, _ := newTestRemoteManager(t)
		if _, err := mgr.StartContainerInNodeMode(image, constants.SptAPIPort, network, args); err != nil {
			t.Fatal(err)
		}
		got = append(got, "remote node "+network+": "+lastDockerRun(t, mock))
	}
	{
		mgr, mock, _ := newTestRemoteManager(t)
		if _, err := mgr.StartWorkerNodeContainer(image, "10.0.0.10", 40000, 3, args); err != nil {
			t.Fatal(err)
		}
		got = append(got, "remote worker: "+lastDockerRun(t, mock))
	}
	for _, network := range []string{constants.HostNetworkMode, constants.BridgeNetworkMode} {
		mgr, mock, _ := newTestRemoteManager(t)
		workers := []string{"10.0.0.11:" + constants.RMIRegistryPort, "10.0.0.12:" + constants.RMIRegistryPort}
		if _, err := mgr.StartEntryNodeContainer(image, workers, args, network); err != nil {
			t.Fatal(err)
		}
		got = append(got, "remote entry "+network+": "+lastDockerRun(t, mock))
	}

	for _, network := range []string{constants.HostNetworkMode, constants.BridgeNetworkMode} {
		f := &fakeDockerClient{}
		dm := &DockerManager{client: f, ctx: context.Background()}
		if _, err := dm.StartContainerInNodeMode(image, constants.SptAPIPort, network, args); err != nil {
			t.Fatal(err)
		}
		got = append(got, "local node "+network+": "+localContainer(f))
		dm.Close()
	}
	{
		f := &fakeDockerClient{}
		dm := &DockerManager{client: f, ctx: context.Background()}
		if _, err := dm.StartWorkerNodeContainer(image, "10.0.0.10", 40000, 3, args); err != nil {
			t.Fatal(err)
		}
		got = append(got, "local worker: "+localContainer(f))
		dm.Close()
	}
	for _, network := range []string{constants.HostNetworkMode, constants.BridgeNetworkMode} {
		f := &fakeDockerClient{}
		dm := &DockerManager{client: f, ctx: context.Background()}
		workers := []string{"10.0.0.11:" + constants.RMIRegistryPort}
		if _, err := dm.StartEntryNodeContainer(image, workers, args, network); err != nil {
			t.Fatal(err)
		}
		got = append(got, "local entry "+network+": "+localContainer(f))
		dm.Close()
	}

	// A distributed run: a remote worker and the entry that reaches it.
	{
		o := NewMultiHostOrchestrator([]*hostparse.HostInfo{
			{Host: "entry.example", Original: "entry.example"},
			{Host: "wrk2.example", Original: "wrk2.example"},
		}, 2)
		o.image = image
		o.detectAdvIP = func(context.Context, *hostparse.HostInfo) (string, error) { return "198.51.100.12", nil }
		entryMgr, entryMock, _ := newTestRemoteManager(t)
		workerMgr, workerMock, _ := newTestRemoteManager(t)
		o.hosts[0].DockerManager = entryMgr
		o.hosts[1].DockerManager = workerMgr
		if err := o.startWorkerNode(context.Background(), o.hosts[1], args); err != nil {
			t.Fatal(err)
		}
		params := scenario.ScenarioParams{WorkloadType: "write", Threads: 1, ObjectSize: "1MB", ObjectCount: 1,
			HostInterfaces: []nicplan.HostInterfaces{
				{Host: "wrk2.example", Value: "mlx5_0", Entries: []string{"mlx5_0"}},
			}}
		if err := o.startEntryNodeContext(context.Background(), o.hosts[0], []*HostConnection{o.hosts[1]}, params, args); err != nil {
			t.Fatal(err)
		}
		got = append(got, "distributed worker: "+lastDockerRun(t, workerMock), "distributed entry: "+lastDockerRun(t, entryMock))
	}

	// A lone remote engine.
	{
		o := NewMultiHostOrchestrator([]*hostparse.HostInfo{{Host: "lone.example", Original: "lone.example"}}, 1)
		o.SetNotifier(func(string) {})
		mgr, mock, _ := newTestRemoteManager(t)
		o.hosts[0].DockerManager = mgr
		o.hosts[0].SetStatus(HostStatusReady)
		if err := o.StartContainers(context.Background(), image, args); err != nil {
			t.Fatal(err)
		}
		got = append(got, "lone remote: "+lastDockerRun(t, mock))
	}

	// The port check on every host.
	{
		o := NewMultiHostOrchestrator([]*hostparse.HostInfo{
			{Host: "entry.example", Original: "entry.example"},
			{Host: "wrk2.example", Original: "wrk2.example"},
		}, 2)
		preflight := &recordingPortPreflight{}
		o.preflight = preflight
		o.SetNotifier(func(string) {})
		o.newDockerManagerForHost = func(ctx context.Context, host *hostparse.HostInfo) (*DockerManager, error) {
			managerCtx, cancel := context.WithCancel(ctx)
			return &DockerManager{ctx: managerCtx, cancel: cancel, hostInfo: host}, nil
		}
		if err := o.ConnectHosts(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, host := range o.hosts {
			host.DockerManager.Close()
		}
		for _, check := range preflight.sortedChecks() {
			got = append(got, "port check: "+check)
		}
	}
	return got
}

// wantDefaultEngineCommands is what 9b60d6dc generated for each engine on the default ports.
var wantDefaultEngineCommands = []string{
	`remote node host: docker run -d --name spt-node-<time>-worker1-example-com -e SPT_LOG_DIR=/tmp/spt-node-home --label spt.host=worker1.example.com --label spt.managed=true --label spt.role=node --network host ghcr.io/dell/storage-performance-tool --run-node=true --run-port=9999 --storage-driver-type=s3`,
	`remote node bridge: docker run -d --name spt-node-<time>-worker1-example-com -e SPT_LOG_DIR=/tmp/spt-node-home --label spt.host=worker1.example.com --label spt.managed=true --label spt.role=node -p 9999:9999/tcp ghcr.io/dell/storage-performance-tool --run-node=true --run-port=9999 --storage-driver-type=s3`,
	`remote worker: docker run -d --name spt-worker-<time>-worker1-example-com -e JAVA_OPTS=-Djava.rmi.server.hostname=10.0.0.10 -e JAVA_TOOL_OPTIONS=-Djava.rmi.server.hostname=10.0.0.10 -e SPT_LOG_DIR=/tmp/spt-node-home --label spt.host=worker1.example.com --label spt.managed=true --label spt.role=worker --network host ghcr.io/dell/storage-performance-tool --run-node=true --run-port=9999 --load-step-node-port=1099 --storage-driver-type=s3`,
	`remote entry host: docker run -d --name spt-entry-<time>-worker1-example-com -e SPT_LOG_DIR=/tmp/spt-node-home --label spt.host=worker1.example.com --label spt.managed=true --label spt.role=entry --network host ghcr.io/dell/storage-performance-tool --load-step-node-addrs=10.0.0.11:1099,10.0.0.12:1099 --run-node=true --run-port=9999 --storage-driver-type=s3`,
	`remote entry bridge: docker run -d --name spt-entry-<time>-worker1-example-com -e SPT_LOG_DIR=/tmp/spt-node-home --label spt.host=worker1.example.com --label spt.managed=true --label spt.role=entry ghcr.io/dell/storage-performance-tool --load-step-node-addrs=10.0.0.11:1099,10.0.0.12:1099 --run-node=true --run-port=9999 --storage-driver-type=s3`,
	`local node host: cmd=["--run-node=true" "--run-port=9999" "--storage-driver-type=s3"] exposed=[] bindings=[] network="host"`,
	`local node bridge: cmd=["--run-node=true" "--run-port=9999" "--storage-driver-type=s3"] exposed=[9999/tcp] bindings=[9999/tcp->127.0.0.1:9999] network=""`,
	`local worker: cmd=["--run-node=true" "--run-port=9999" "--storage-driver-type=s3"] exposed=[1099/tcp 40000/tcp 40001/tcp 40002/tcp 9999/tcp] bindings=[1099/tcp->0.0.0.0:1099 9999/tcp->0.0.0.0:9999 40000/tcp->0.0.0.0:40000 40001/tcp->0.0.0.0:40001 40002/tcp->0.0.0.0:40002] network=""`,
	`local entry host: cmd=["--load-step-node-addrs=10.0.0.11:1099" "--run-node=true" "--run-port=9999" "--storage-driver-type=s3"] exposed=[] bindings=[] network="host"`,
	`local entry bridge: cmd=["--load-step-node-addrs=10.0.0.11:1099" "--run-node=true" "--run-port=9999" "--storage-driver-type=s3"] exposed=[9999/tcp] bindings=[9999/tcp->0.0.0.0:9999] network=""`,
	`distributed worker: docker run -d --name spt-worker-<time>-worker1-example-com -e JAVA_OPTS=-Djava.rmi.server.hostname=198.51.100.12 -e JAVA_TOOL_OPTIONS=-Djava.rmi.server.hostname=198.51.100.12 -e SPT_LOG_DIR=/tmp/spt-node-home --label spt.host=worker1.example.com --label spt.managed=true --label spt.role=worker --network host ghcr.io/dell/storage-performance-tool --run-node=true --run-port=9999 --load-step-node-port=1099 --storage-driver-type=s3`,
	`distributed entry: docker run -d --name spt-entry-<time>-worker1-example-com -e SPT_LOG_DIR=/tmp/spt-node-home --label spt.host=worker1.example.com --label spt.managed=true --label spt.role=entry --network host ghcr.io/dell/storage-performance-tool --load-step-node-addrs=198.51.100.12:1099 --run-node=true --run-port=9999 --storage-driver-type=s3 --storage-net-nodeInterfaces-198.51.100.12:1099=mlx5_0`,
	`lone remote: docker run -d --name spt-node-<time>-worker1-example-com -e SPT_LOG_DIR=/tmp/spt-node-home --label spt.host=worker1.example.com --label spt.managed=true --label spt.role=node --network host ghcr.io/dell/storage-performance-tool --run-node=true --run-port=9999 --storage-driver-type=s3`,
	`port check: entry.example registry=1099 api=9999 rmi=40000+10`,
	`port check: wrk2.example registry=1099 api=9999 rmi=40000+10`,
}

func TestDefaultEnginePortsKeepEveryEngineCommand(t *testing.T) {
	got := defaultEngineCommands(t)
	if len(got) != len(wantDefaultEngineCommands) {
		t.Fatalf("got %d commands, want %d:\n%s", len(got), len(wantDefaultEngineCommands), strings.Join(got, "\n"))
	}
	for i := range got {
		if got[i] != wantDefaultEngineCommands[i] {
			t.Errorf("command %d changed:\n got: %s\nwant: %s", i, got[i], wantDefaultEngineCommands[i])
		}
	}
}
