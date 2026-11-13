/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

const testRunAPIPort = "19300"

// recordProbes makes a remote manager record the API URL it waits on instead of probing it.
func recordProbes(mgr *RemoteDockerManager) func() []string {
	var mu sync.Mutex
	var urls []string
	mgr.proberRun = func(_ context.Context, baseURL string, _ time.Duration) error {
		mu.Lock()
		defer mu.Unlock()
		urls = append(urls, baseURL)
		return nil
	}
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), urls...)
	}
}

// fullIDDockerClient answers a create with a container ID as long as Docker's, which the
// orchestrator shortens in its log.
type fullIDDockerClient struct {
	*fakeDockerClient
}

func (c fullIDDockerClient) ContainerCreate(
	ctx context.Context, config *container.Config, hostConfig *container.HostConfig,
	networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, name string,
) (container.CreateResponse, error) {
	if _, err := c.fakeDockerClient.ContainerCreate(ctx, config, hostConfig, networkingConfig, platform, name); err != nil {
		return container.CreateResponse{}, err
	}
	return container.CreateResponse{ID: "0123456789abcdef0123456789abcdef"}, nil
}

// requireNoDefaultAPIPort fails when a command still names the default API port.
func requireNoDefaultAPIPort(t *testing.T, role string, tokens []string) {
	t.Helper()
	for _, token := range tokens {
		if strings.Contains(token, constants.SptAPIPort) {
			t.Errorf("%s command still names port %s in %q: %s", role, constants.SptAPIPort, token, strings.Join(tokens, " "))
		}
	}
}

// Ticket 01: a distributed run on --api-port 19300 started every engine on 9999 and then waited
// on 19300. The entry and every worker, remote or local, must serve the run's API port.
func TestDistributedRunStartsEveryEngineOnTheRunsAPIPort(t *testing.T) {
	clearEngineEnvironment(t)
	o := NewMultiHostOrchestrator([]*hostparse.HostInfo{
		{Host: "entry.example", Original: "entry.example"},
		{Host: "wrk2.example", Original: "wrk2.example"},
		{Host: "127.0.0.1", Original: "127.0.0.1", IsLocal: true},
	}, 3)
	o.image = constants.DefaultSptImage
	o.SetAPIPort(testRunAPIPort)
	advertised := map[string]string{"wrk2.example": "198.51.100.12", "127.0.0.1": "198.51.100.13"}
	o.detectAdvIP = func(_ context.Context, host *hostparse.HostInfo) (string, error) {
		return advertised[host.Original], nil
	}
	entryMgr, entryMock, _ := newTestRemoteManager(t)
	workerMgr, workerMock, _ := newTestRemoteManager(t)
	entryProbes, workerProbes := recordProbes(entryMgr), recordProbes(workerMgr)
	local := &fakeDockerClient{}
	localDM := &DockerManager{client: fullIDDockerClient{local}, ctx: context.Background()}
	t.Cleanup(localDM.Close)
	o.hosts[0].DockerManager = entryMgr
	o.hosts[1].DockerManager = workerMgr
	o.hosts[2].DockerManager = localDM

	for _, worker := range o.hosts[1:] {
		if err := o.startWorkerNode(context.Background(), worker, nil); err != nil {
			t.Fatalf("startWorkerNode(%s) error = %v", worker.Info.Original, err)
		}
	}
	params := scenario.ScenarioParams{WorkloadType: "write", Threads: 1, ObjectSize: "1MB", ObjectCount: 1}
	if err := o.startEntryNodeContext(context.Background(), o.hosts[0], o.hosts[1:], params, nil); err != nil {
		t.Fatalf("startEntryNodeContext() error = %v", err)
	}

	wantRunPort := "--run-port=" + testRunAPIPort
	for role, mock := range map[string]*command.MockCommandExecutor{"remote worker": workerMock, "entry": entryMock} {
		tokens := strings.Fields(lastDockerRun(t, mock))
		if !slices.Contains(tokens, wantRunPort) {
			t.Errorf("%s command lacks %s: %s", role, wantRunPort, strings.Join(tokens, " "))
		}
		requireNoDefaultAPIPort(t, role, tokens)
	}
	for role, probes := range map[string][]string{"remote worker": workerProbes(), "entry": entryProbes()} {
		if len(probes) != 1 || !strings.HasSuffix(probes[0], ":"+testRunAPIPort) {
			t.Errorf("%s API readiness probes = %v, want one on port %s", role, probes, testRunAPIPort)
		}
	}

	if !slices.Contains(local.createContainerConfig.Cmd, wantRunPort) {
		t.Errorf("local worker command lacks %s: %q", wantRunPort, local.createContainerConfig.Cmd)
	}
	requireNoDefaultAPIPort(t, "local worker", local.createContainerConfig.Cmd)
	described := localContainer(local)
	if !strings.Contains(described, testRunAPIPort+"/tcp->0.0.0.0:"+testRunAPIPort) {
		t.Errorf("local worker does not publish port %s: %s", testRunAPIPort, described)
	}
	if strings.Contains(described, constants.SptAPIPort+"/tcp") {
		t.Errorf("local worker still publishes port %s: %s", constants.SptAPIPort, described)
	}
}

// A local entry in bridge mode publishes the run's API port, which the CLI waits on.
func TestALocalBridgeEntryPublishesTheRunsAPIPort(t *testing.T) {
	clearEngineEnvironment(t)
	o := NewMultiHostOrchestratorWithRMI([]*hostparse.HostInfo{
		{Host: "127.0.0.1", Original: "127.0.0.1", IsLocal: true},
	}, 1, RMIConfig{
		NetworkMode: constants.BridgeNetworkMode,
		PortStart:   constants.DefaultRMIPortStart,
		PortCount:   constants.DefaultRMIPortCount,
	})
	o.image = constants.DefaultSptImage
	o.SetAPIPort(testRunAPIPort)
	local := &fakeDockerClient{}
	localDM := &DockerManager{client: fullIDDockerClient{local}, ctx: context.Background()}
	t.Cleanup(localDM.Close)
	o.hosts[0].DockerManager = localDM

	params := scenario.ScenarioParams{WorkloadType: "write", Threads: 1, ObjectSize: "1MB", ObjectCount: 1}
	if err := o.startEntryNodeContext(context.Background(), o.hosts[0], nil, params, nil); err != nil {
		t.Fatalf("startEntryNodeContext() error = %v", err)
	}
	if !slices.Contains(local.createContainerConfig.Cmd, "--run-port="+testRunAPIPort) {
		t.Errorf("local entry command = %q, want --run-port=%s", local.createContainerConfig.Cmd, testRunAPIPort)
	}
	if got, want := localContainer(local), "exposed=["+testRunAPIPort+"/tcp] bindings=["+testRunAPIPort+"/tcp->0.0.0.0:"+testRunAPIPort+"]"; !strings.Contains(got, want) {
		t.Errorf("local bridge entry = %s, want %s", got, want)
	}
}
