/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

const bridgeEntryAPIPort = 19412

// remoteEntryRunPrefix is everything a remote entry's docker run sends before its port
// publishing, image and engine arguments.
const remoteEntryRunPrefix = "docker run -d --name spt-entry-<time>-worker1-example-com " +
	"-e SPT_LOG_DIR=/tmp/spt-node-home --label spt.host=worker1.example.com " +
	"--label spt.managed=true --label spt.role=entry "

// A remote entry outside host networking publishes its API port, and only that port, through
// both its starts; in host mode it publishes nothing and sends the command it sent before.
func TestARemoteEntryPublishesItsAPIPortOutsideHostNetworking(t *testing.T) {
	clearEngineEnvironment(t)
	image := constants.DefaultSptImage
	runPorts := enginePorts{api: bridgeEntryAPIPort, registry: testRunRegistryPort}
	tests := []struct {
		name, networkMode string
		start             func(*RemoteDockerManager, string) error
		want              string
	}{
		{
			name: "run ports, bridge", networkMode: constants.BridgeNetworkMode,
			start: func(m *RemoteDockerManager, mode string) error {
				_, err := m.startEntryNodeOnPorts(context.Background(), image, runPorts,
					[]string{"10.0.0.10:11099"}, nil, mode)
				return err
			},
			want: remoteEntryRunPrefix + "-p 19412:19412/tcp " + image +
				" --load-step-node-addrs=10.0.0.10:11099 --run-node=true --run-port=19412 --load-step-node-port=11099",
		},
		{
			name: "default ports, bridge", networkMode: constants.BridgeNetworkMode,
			start: func(m *RemoteDockerManager, mode string) error {
				_, err := m.StartEntryNodeContainerContext(context.Background(), image,
					[]string{"10.0.0.10:1099"}, nil, mode)
				return err
			},
			want: remoteEntryRunPrefix + "-p 9999:9999/tcp " + image +
				" --load-step-node-addrs=10.0.0.10:1099 --run-node=true --run-port=9999",
		},
		// The host-mode commands are the ones dda2fd8e sends.
		{
			name: "run ports, host", networkMode: constants.HostNetworkMode,
			start: func(m *RemoteDockerManager, mode string) error {
				_, err := m.startEntryNodeOnPorts(context.Background(), image, runPorts,
					[]string{"10.0.0.10:11099"}, nil, mode)
				return err
			},
			want: remoteEntryRunPrefix + "--network host " + image +
				" --load-step-node-addrs=10.0.0.10:11099 --run-node=true --run-port=19412 --load-step-node-port=11099",
		},
		{
			name: "default ports, host", networkMode: constants.HostNetworkMode,
			start: func(m *RemoteDockerManager, mode string) error {
				_, err := m.StartEntryNodeContainerContext(context.Background(), image,
					[]string{"10.0.0.10:1099"}, nil, mode)
				return err
			},
			want: remoteEntryRunPrefix + "--network host " + image +
				" --load-step-node-addrs=10.0.0.10:1099 --run-node=true --run-port=9999",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mgr, mock, _ := newTestRemoteManager(t)
			if err := test.start(mgr, test.networkMode); err != nil {
				t.Fatalf("entry start error = %v", err)
			}
			if got := lastDockerRun(t, mock); got != test.want {
				t.Errorf("remote entry docker run:\n got: %s\nwant: %s", got, test.want)
			}
		})
	}
}

// remoteManagerOn returns a remote Docker manager for host whose docker commands go to a mock.
func remoteManagerOn(t *testing.T, host *hostparse.HostInfo) (*RemoteDockerManager, *command.MockCommandExecutor) {
	t.Helper()
	mock := command.NewMockCommandExecutor()
	mock.DefaultResponse = command.MockResponse{Stdout: "abc123def456"}
	mgr, err := newRemoteDockerManagerWithExecutor(host, mock)
	if err != nil {
		t.Fatalf("remote docker manager for %s: %v", host.Original, err)
	}
	return mgr, mock
}

// entryDockerRun returns the docker run a host's mock received for an entry, or fails.
func entryDockerRun(t *testing.T, mock *command.MockCommandExecutor) []string {
	t.Helper()
	for _, executed := range mock.GetExecutedCommandsMatching("docker run") {
		if strings.Contains(strings.Join(executed.Command, " "), "spt.role=entry") {
			return executed.Command
		}
	}
	t.Fatal("no entry docker run was sent")
	return nil
}

// publishedPorts returns the -p values of a docker run.
func publishedPorts(tokens []string) []string {
	var ports []string
	for i, token := range tokens {
		if token == "-p" && i+1 < len(tokens) {
			ports = append(ports, tokens[i+1])
		}
	}
	return ports
}

var errStopAfterEntryStart = errors.New("stop after the entry start")

// A two-host run (and so a distributed replay, which starts through the same content route) on a
// bridge network starts its remote entry with the run's API port published; the remote worker
// stays on host networking and publishes nothing.
func TestABridgeDistributedRunPublishesTheRemoteEntrysAPIPort(t *testing.T) {
	clearEngineEnvironment(t)
	// The orchestrator's RMI readiness wait dials each worker's registry port; serve it on a port
	// the kernel picks. Both hosts are remote managers; their docker commands go to mocks.
	registryPort := listenOnFreePort(t)
	o := NewMultiHostOrchestratorWithRMI([]*hostparse.HostInfo{
		{Host: "127.0.0.1", Original: "entry"},
		{Host: "127.0.0.1", Original: "worker1"},
	}, 2, RMIConfig{NetworkMode: constants.BridgeNetworkMode, PortStart: constants.DefaultRMIPortStart,
		PortCount: constants.DefaultRMIPortCount})
	o.SetNotifier(func(string) {})
	o.SetAPIPort("19412")
	o.SetRMIRegistryPort(registryPort)
	o.detectAdvIP = func(context.Context, *hostparse.HostInfo) (string, error) { return "127.0.0.1", nil }
	entryMgr, entryMock := remoteManagerOn(t, o.hosts[0].Info)
	workerMgr, workerMock := remoteManagerOn(t, o.hosts[1].Info)
	o.hosts[0].DockerManager, o.hosts[1].DockerManager = entryMgr, workerMgr
	o.hosts[0].SetStatus(HostStatusReady)
	o.hosts[1].SetStatus(HostStatusReady)
	wrapper := NewMultiHostTestOrchestrator(o)
	wrapper.SetCallbacks(func(*TestStatus) {}, func(*MultiNodeMetricsUpdate) {}, func(string) {}, func(string) {})
	wrapper.waitForAPIs = func(context.Context, time.Duration) error { return errStopAfterEntryStart }

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	params := scenario.ScenarioParams{WorkloadType: "write", Threads: 1}
	err := wrapper.StartTestWithContent(ctx, "test-image", params, []byte(`Load.config({}).run();`), []byte("storage: {}\n"))
	if !errors.Is(err, errStopAfterEntryStart) {
		t.Fatalf("StartTestWithContent() error = %v, want the stub API wait", err)
	}

	entry := entryDockerRun(t, entryMock)
	if got := publishedPorts(entry); len(got) != 1 || got[0] != "19412:19412/tcp" {
		t.Errorf("entry publishes %q, want only 19412:19412/tcp: %s", got, strings.Join(entry, " "))
	}
	requireOnlyRunPorts(t, "entry", entry, "--run-port=19412")
	worker := strings.Fields(lastDockerRun(t, workerMock))
	if got := publishedPorts(worker); len(got) != 0 || !strings.Contains(strings.Join(worker, " "), "--network host") {
		t.Errorf("worker publishes %q, want host networking and no -p: %s", got, strings.Join(worker, " "))
	}
}

// A multi-host LIST run on a bridge network starts its remote primary as an entry with the API
// port published, as it does its idle remote engines.
func TestABridgeListRunPublishesTheRemotePrimarysAPIPort(t *testing.T) {
	clearEngineEnvironment(t)
	o := NewMultiHostOrchestratorWithRMI([]*hostparse.HostInfo{
		{Host: "primary.example", Original: "primary.example"},
		{Host: "idle.example", Original: "idle.example"},
	}, 2, RMIConfig{NetworkMode: constants.BridgeNetworkMode, PortStart: constants.DefaultRMIPortStart,
		PortCount: constants.DefaultRMIPortCount})
	o.SetNotifier(func(string) {})
	primaryMgr, primaryMock := remoteManagerOn(t, o.hosts[0].Info)
	idleMgr, idleMock := remoteManagerOn(t, o.hosts[1].Info)
	o.hosts[0].DockerManager, o.hosts[1].DockerManager = primaryMgr, idleMgr
	o.hosts[0].SetStatus(HostStatusReady)
	o.hosts[1].SetStatus(HostStatusReady)
	wrapper := NewMultiHostTestOrchestrator(o)
	wrapper.SetCallbacks(func(*TestStatus) {}, func(*MultiNodeMetricsUpdate) {}, func(string) {}, func(string) {})
	// The first wait is on the idle engines and is not fatal; the second follows the entry start.
	var waits atomic.Int32
	wrapper.waitForAPIs = func(context.Context, time.Duration) error {
		if waits.Add(1) == 1 {
			return nil
		}
		return errStopAfterEntryStart
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	params := scenario.ScenarioParams{WorkloadType: scenario.WorkloadTypeList, Threads: 1, Bucket: "demo", Endpoint: "http://minio:9000"}
	err := wrapper.StartTestWithLaunchHooks(ctx, "test-image", params, NewLaunchHooks(func() {}))
	if !errors.Is(err, errStopAfterEntryStart) {
		t.Fatalf("StartTest(list) error = %v, want the stub API wait", err)
	}

	entry := entryDockerRun(t, primaryMock)
	if got := publishedPorts(entry); len(got) != 1 || got[0] != "9999:9999/tcp" {
		t.Errorf("LIST primary publishes %q, want only 9999:9999/tcp: %s", got, strings.Join(entry, " "))
	}
	if idle := strings.Fields(lastDockerRun(t, idleMock)); !strings.Contains(strings.Join(idle, " "), "-p 9999:9999/tcp") {
		t.Errorf("LIST idle engine does not publish its API port: %s", strings.Join(idle, " "))
	}
}
