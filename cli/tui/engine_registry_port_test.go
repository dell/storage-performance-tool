/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

const testRunRegistryPort = 11099

var testRunPorts = enginePorts{api: 19300, registry: testRunRegistryPort}

// requireOnlyRunPorts fails when a command lacks the run's port arguments or still names a
// default port as a port.
func requireOnlyRunPorts(t *testing.T, role string, tokens []string, want ...string) {
	t.Helper()
	for _, arg := range want {
		if !slices.Contains(tokens, arg) {
			t.Errorf("%s command lacks %s: %s", role, arg, strings.Join(tokens, " "))
		}
	}
	for _, token := range tokens {
		if strings.HasSuffix(token, "="+constants.RMIRegistryPort) || strings.Contains(token, ":"+constants.RMIRegistryPort) ||
			strings.Contains(token, constants.SptAPIPort) {
			t.Errorf("%s command still names a default port in %q: %s", role, token, strings.Join(tokens, " "))
		}
	}
}

// defaultPortsOnlyManager is a DockerInterface without the port-aware start, as an extension's
// manager is.
type defaultPortsOnlyManager struct {
	DockerInterface
}

// listenOnFreePort serves TCP on a port the kernel picks, never a default engine port.
func listenOnFreePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}()
	return listener.Addr().(*net.TCPAddr).Port
}

// closedPort returns a port that nothing listens on.
func closedPort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	_ = listener.Close()
	return strconv.Itoa(port)
}

func TestRemoteEnginesServeTheRunsRegistryPort(t *testing.T) {
	clearEngineEnvironment(t)
	image := constants.DefaultSptImage

	workerMgr, workerMock, _ := newTestRemoteManager(t)
	if _, err := workerMgr.startWorkerNodeOnPorts(
		context.Background(), image, "10.0.0.10", testRunPorts, 40000, 3, nil); err != nil {
		t.Fatalf("startWorkerNodeOnPorts() error = %v", err)
	}
	worker := strings.Fields(lastDockerRun(t, workerMock))
	requireOnlyRunPorts(t, "remote worker", worker, "--run-port=19300", "--load-step-node-port=11099")

	entryMgr, entryMock, _ := newTestRemoteManager(t)
	if _, err := entryMgr.startEntryNodeOnPorts(context.Background(), image, testRunPorts,
		[]string{"10.0.0.10:11099"}, nil, constants.HostNetworkMode); err != nil {
		t.Fatalf("startEntryNodeOnPorts() error = %v", err)
	}
	entry := strings.Fields(lastDockerRun(t, entryMock))
	requireOnlyRunPorts(t, "remote entry", entry,
		"--load-step-node-addrs=10.0.0.10:11099", "--run-port=19300", "--load-step-node-port=11099")
}

func TestLocalEnginesPublishTheRunsRegistryPort(t *testing.T) {
	clearEngineEnvironment(t)
	image := constants.DefaultSptImage

	worker := &fakeDockerClient{}
	workerDM := &DockerManager{client: worker, ctx: context.Background()}
	t.Cleanup(workerDM.Close)
	if _, err := workerDM.startWorkerNodeOnPorts(
		context.Background(), image, "10.0.0.10", testRunPorts, 40000, 2, nil); err != nil {
		t.Fatalf("startWorkerNodeOnPorts() error = %v", err)
	}
	want := `cmd=["--run-node=true" "--run-port=19300" "--load-step-node-port=11099"] ` +
		`exposed=[11099/tcp 19300/tcp 40000/tcp 40001/tcp] ` +
		`bindings=[11099/tcp->0.0.0.0:11099 19300/tcp->0.0.0.0:19300 40000/tcp->0.0.0.0:40000 40001/tcp->0.0.0.0:40001] network=""`
	if got := localContainer(worker); got != want {
		t.Errorf("local worker:\n got: %s\nwant: %s", got, want)
	}

	entry := &fakeDockerClient{}
	entryDM := &DockerManager{client: entry, ctx: context.Background()}
	t.Cleanup(entryDM.Close)
	if _, err := entryDM.startEntryNodeOnPorts(context.Background(), image, testRunPorts,
		[]string{"10.0.0.10:11099"}, nil, constants.BridgeNetworkMode); err != nil {
		t.Fatalf("startEntryNodeOnPorts() error = %v", err)
	}
	want = `cmd=["--load-step-node-addrs=10.0.0.10:11099" "--run-node=true" "--run-port=19300" "--load-step-node-port=11099"] ` +
		`exposed=[19300/tcp] bindings=[19300/tcp->0.0.0.0:19300] network=""`
	if got := localContainer(entry); got != want {
		t.Errorf("local bridge entry:\n got: %s\nwant: %s", got, want)
	}
}

// Ticket 02: the entry, every worker and the NIC-set keys follow --rmi-registry-port, and the
// entry reaches each worker on it.
func TestDistributedRunServesAndReachesTheRunsRegistryPort(t *testing.T) {
	clearEngineEnvironment(t)
	o := NewMultiHostOrchestrator([]*hostparse.HostInfo{
		{Host: "entry.example", Original: "entry.example"},
		{Host: "wrk2.example", Original: "wrk2.example"},
		{Host: "wrk3.example", Original: "wrk3.example"},
	}, 3)
	o.image = constants.DefaultSptImage
	o.SetAPIPort("19300")
	o.SetRMIRegistryPort(testRunRegistryPort)
	advertised := map[string]string{"wrk2.example": "198.51.100.12", "wrk3.example": "198.51.100.13"}
	o.detectAdvIP = func(_ context.Context, host *hostparse.HostInfo) (string, error) {
		return advertised[host.Original], nil
	}
	mocks := make([]*command.MockCommandExecutor, len(o.hosts))
	for i, host := range o.hosts {
		mgr, mock, _ := newTestRemoteManager(t)
		host.DockerManager, mocks[i] = mgr, mock
	}
	for _, worker := range o.hosts[1:] {
		if err := o.startWorkerNode(context.Background(), worker, nil); err != nil {
			t.Fatalf("startWorkerNode(%s) error = %v", worker.Info.Original, err)
		}
	}
	params := scenario.ScenarioParams{WorkloadType: "write", Threads: 1, ObjectSize: "1MB", ObjectCount: 1,
		HostInterfaces: []nicplan.HostInterfaces{
			{Host: "entry.example", Value: "none"},
			{Host: "wrk2.example", Value: "mlx5_0", Entries: []string{"mlx5_0"}},
		}}
	if err := o.startEntryNodeContext(context.Background(), o.hosts[0], o.hosts[1:], params, nil); err != nil {
		t.Fatalf("startEntryNodeContext() error = %v", err)
	}

	for i, worker := range o.hosts[1:] {
		requireOnlyRunPorts(t, "worker "+worker.Info.Original, strings.Fields(lastDockerRun(t, mocks[i+1])),
			"--run-port=19300", "--load-step-node-port=11099")
	}
	requireOnlyRunPorts(t, "entry", strings.Fields(lastDockerRun(t, mocks[0])),
		"--load-step-node-addrs=198.51.100.12:11099,198.51.100.13:11099",
		"--run-port=19300", "--load-step-node-port=11099",
		"--storage-net-nodeInterfaces-entry=none",
		"--storage-net-nodeInterfaces-198.51.100.12:11099=mlx5_0")

	wrapper := NewMultiHostTestOrchestrator(o)
	if got := wrapper.fleetContributorID(o.hosts[1]); got != "198.51.100.12:11099" {
		t.Errorf("worker metric contributor = %q, want the address the entry reaches it on", got)
	}
}

func TestALoneRemoteEngineServesTheRunsPorts(t *testing.T) {
	clearEngineEnvironment(t)
	o := NewMultiHostOrchestrator([]*hostparse.HostInfo{{Host: "lone.example", Original: "lone.example"}}, 1)
	o.SetNotifier(func(string) {})
	o.SetAPIPort("19300")
	o.SetRMIRegistryPort(testRunRegistryPort)
	mgr, mock, _ := newTestRemoteManager(t)
	o.hosts[0].DockerManager = mgr
	o.hosts[0].SetStatus(HostStatusReady)

	if err := o.StartContainers(context.Background(), constants.DefaultSptImage, []string{"--storage-driver-type=s3"}); err != nil {
		t.Fatalf("StartContainers() error = %v", err)
	}
	requireOnlyRunPorts(t, "lone remote", strings.Fields(lastDockerRun(t, mock)),
		"--run-port=19300", "--load-step-node-port=11099", "--storage-driver-type=s3")
}

func TestConnectHostsChecksTheRunsPortsOnEveryHost(t *testing.T) {
	o := NewMultiHostOrchestrator([]*hostparse.HostInfo{
		{Host: "entry.example", Original: "entry.example"},
		{Host: "wrk2.example", Original: "wrk2.example"},
		{Host: "wrk3.example", Original: "wrk3.example"},
	}, 3)
	preflight := &recordingPortPreflight{}
	o.preflight = preflight
	o.SetNotifier(func(string) {})
	o.SetAPIPort("19300")
	o.SetRMIRegistryPort(testRunRegistryPort)
	o.newDockerManagerForHost = func(ctx context.Context, host *hostparse.HostInfo) (*DockerManager, error) {
		managerCtx, cancel := context.WithCancel(ctx)
		return &DockerManager{ctx: managerCtx, cancel: cancel, hostInfo: host}, nil
	}
	if err := o.ConnectHosts(context.Background()); err != nil {
		t.Fatalf("ConnectHosts() error = %v", err)
	}
	for _, host := range o.hosts {
		host.DockerManager.Close()
	}
	want := []string{
		"entry.example registry=11099 api=19300 rmi=40000+10",
		"wrk2.example registry=11099 api=19300 rmi=40000+10",
		"wrk3.example registry=11099 api=19300 rmi=40000+10",
	}
	if got := preflight.sortedChecks(); !reflect.DeepEqual(got, want) {
		t.Fatalf("port checks = %q, want %q", got, want)
	}
}

func TestRMIReadinessWaitsOnTheRunsRegistryPort(t *testing.T) {
	registryPort := listenOnFreePort(t)
	o := NewMultiHostOrchestrator([]*hostparse.HostInfo{{Host: "127.0.0.1", Original: "worker", IsLocal: true}}, 1)
	o.SetRMIRegistryPort(registryPort)
	if got := o.RMIRegistryPort(); got != registryPort {
		t.Fatalf("RMIRegistryPort() = %d, want %d", got, registryPort)
	}
	if err := o.waitForRMIReadyContext(context.Background(), o.hosts[0], 2*time.Second); err != nil {
		t.Fatalf("waitForRMIReadyContext() error = %v, want the registry on port %d found", err, registryPort)
	}
}

func TestRMIRegistryPortDefaultsToTheEngineDefault(t *testing.T) {
	o := NewMultiHostOrchestrator([]*hostparse.HostInfo{{Host: "lone.example", Original: "lone.example"}}, 1)
	if got := o.RMIRegistryPort(); got != constants.DefaultRMIRegistryPort {
		t.Fatalf("RMIRegistryPort() = %d, want %d", got, constants.DefaultRMIRegistryPort)
	}
	if got := o.nodeModePortArgs(); got != nil {
		t.Fatalf("nodeModePortArgs() = %q, want none on the default port", got)
	}
	o.SetRMIRegistryPort(testRunRegistryPort)
	if got := o.nodeModePortArgs(); !reflect.DeepEqual(got, []string{"--load-step-node-port=11099"}) {
		t.Fatalf("nodeModePortArgs() = %q", got)
	}
}

func TestAManagerWithoutPortSupportStartsOnlyOnTheDefaultPorts(t *testing.T) {
	mock := NewMockDockerManager()
	dm := defaultPortsOnlyManager{mock}
	if _, err := startWorkerNodeContainerContext(
		context.Background(), dm, "image", "10.0.0.10", testRunPorts, 40000, 1, nil); err == nil ||
		!strings.Contains(err.Error(), "API port 19300 and RMI registry port 11099; it supports only 9999 and 1099") {
		t.Fatalf("worker on other ports error = %v", err)
	}
	if _, err := startEntryNodeContainerContext(
		context.Background(), dm, "image", testRunPorts, nil, nil, constants.HostNetworkMode); err == nil ||
		!strings.Contains(err.Error(), "supports only 9999 and 1099") {
		t.Fatalf("entry on other ports error = %v", err)
	}
	if len(mock.GetWorkerNodeCalls()) != 0 || len(mock.GetEntryNodeCalls()) != 0 {
		t.Fatal("a refused start reached the manager")
	}

	if _, err := startWorkerNodeContainerContext(
		context.Background(), dm, "image", "10.0.0.10", defaultEnginePorts(), 40000, 1, nil); err != nil {
		t.Fatalf("worker on the default ports error = %v", err)
	}
	if _, err := startEntryNodeContainerContext(
		context.Background(), dm, "image", defaultEnginePorts(), nil, nil, constants.HostNetworkMode); err != nil {
		t.Fatalf("entry on the default ports error = %v", err)
	}
	if len(mock.GetWorkerNodeCalls()) != 1 || len(mock.GetEntryNodeCalls()) != 1 {
		t.Fatal("a start on the default ports did not reach the manager")
	}
}

func newPortTestAPIServer(t *testing.T, runStarted *atomic.Bool) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ready":
			_, _ = w.Write([]byte(`{"ready":true,"status":"ready","scope":"node","role":"entry","node_id":"n0"}`))
		case "/health":
			_, _ = w.Write([]byte(`{"status":"ok","scope":"node","role":"entry","node_id":"n0"}`))
		case "/run":
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			runStarted.Store(true)
			w.Header().Set("ETag", "\"run-ports\"")
			_, _ = w.Write([]byte(`{"runId":"run-ports"}`))
		case "/status":
			_, _ = w.Write([]byte(`{"state":"RUNNING","message":"running","runId":"run-ports"}`))
		case "/metrics/json":
			_, _ = fmt.Fprintf(w, `[{"metrics_schema":2,"scope":"node","role":"entry","node_id":"n0","run_id":"run-ports","sample_ts":"%s","step_id":"s1","op_type":"LIST","timestamp":%d,"elapsed_time_seconds":0.1,"test_state":1,"completion_percent":0,"overall_completion_percent":0,"unbounded":true,"overall_unbounded":true,"operations":{"success_count":0,"failed_count":0,"success_rate_last":0,"failed_rate_last":0},"bandwidth":{"bytes_total":0,"bytes_rate_last":0},"timing":{"latency_mean_us":0,"duration_mean_us":0},"concurrency":{"current":0,"mean":0}}]`,
				time.Now().UTC().Format(time.RFC3339Nano), time.Now().UnixMilli())
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// A LIST run starts its primary as an entry and its idle hosts as API engines, all on the run's
// ports.
func TestListRunStartsEveryEngineOnTheRunsPorts(t *testing.T) {
	var runStarted atomic.Bool
	srv := newPortTestAPIServer(t, &runStarted)
	_, apiPort := splitServerHostPort(t, srv.URL)
	o := NewMultiHostOrchestrator([]*hostparse.HostInfo{
		{Host: "127.0.0.1", IsLocal: true, Original: "primary"},
		{Host: "127.0.0.1", IsLocal: true, Original: "worker1"},
	}, 2)
	o.SetAPIPort(apiPort)
	o.SetRMIRegistryPort(testRunRegistryPort)
	primaryDM, workerDM := NewMockDockerManager(), NewMockDockerManager()
	o.hosts[0].DockerManager, o.hosts[1].DockerManager = primaryDM, workerDM
	o.hosts[0].SetStatus(HostStatusReady)
	o.hosts[1].SetStatus(HostStatusReady)
	wrapper := NewMultiHostTestOrchestrator(o)
	wrapper.SetCallbacks(func(*TestStatus) {}, func(*MultiNodeMetricsUpdate) {}, func(string) {}, func(string) {})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	params := scenario.ScenarioParams{WorkloadType: scenario.WorkloadTypeList, Threads: 1, Bucket: "demo", Endpoint: "http://minio:9000"}
	if err := wrapper.StartTestWithLaunchHooks(ctx, "test-image", params, NewLaunchHooks(func() {})); err != nil {
		t.Fatalf("StartTest(list) error = %v", err)
	}
	defer func() { _ = wrapper.StopTest() }()

	port, _ := strconv.Atoi(apiPort)
	if got, want := primaryDM.startedOnPorts(), []enginePorts{{api: port, registry: testRunRegistryPort}}; !reflect.DeepEqual(got, want) {
		t.Errorf("LIST primary started on %+v, want %+v", got, want)
	}
	calls := workerDM.GetContainerCalls()
	if len(calls) != 1 || !slices.Contains(calls[0].Cmd, "--run-port="+apiPort) ||
		!slices.Contains(calls[0].Cmd, "--load-step-node-port=11099") {
		t.Errorf("LIST idle engine calls = %+v, want the run's API and registry ports", calls)
	}
	if !runStarted.Load() {
		t.Error("LIST run was not submitted")
	}
}

// A distributed run whose engines become ready on the run's ports starts, and reports its
// workers' metrics under the address the entry reaches them on.
func TestDistributedRunStartsOnTheRunsPorts(t *testing.T) {
	var runStarted atomic.Bool
	srv := newPortTestAPIServer(t, &runStarted)
	host, apiPort := splitServerHostPort(t, srv.URL)
	registryPort := listenOnFreePort(t)
	o := NewMultiHostOrchestrator([]*hostparse.HostInfo{
		{Host: host, IsLocal: true, Original: "entry"},
		{Host: host, IsLocal: true, Original: "worker1"},
	}, 2)
	o.SetAPIPort(apiPort)
	o.SetRMIRegistryPort(registryPort)
	o.detectAdvIP = func(context.Context, *hostparse.HostInfo) (string, error) { return host, nil }
	entryDM, workerDM := NewMockDockerManager(), NewMockDockerManager()
	o.hosts[0].DockerManager, o.hosts[1].DockerManager = entryDM, workerDM
	o.hosts[0].SetStatus(HostStatusReady)
	o.hosts[1].SetStatus(HostStatusReady)
	wrapper := NewMultiHostTestOrchestrator(o)
	wrapper.SetCallbacks(func(*TestStatus) {}, func(*MultiNodeMetricsUpdate) {}, func(string) {}, func(string) {})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	params := scenario.ScenarioParams{WorkloadType: "write", Threads: 1}
	if err := wrapper.StartTestWithContent(ctx, "test-image", params, []byte(`Load.config({}).run();`), []byte("storage: {}\n")); err != nil {
		t.Fatalf("StartTestWithContent() error = %v", err)
	}
	defer func() { _ = wrapper.StopTest() }()

	api, _ := strconv.Atoi(apiPort)
	want := []enginePorts{{api: api, registry: registryPort}}
	if got := workerDM.startedOnPorts(); !reflect.DeepEqual(got, want) {
		t.Errorf("worker started on %+v, want %+v", got, want)
	}
	if got := entryDM.startedOnPorts(); !reflect.DeepEqual(got, want) {
		t.Errorf("entry started on %+v, want %+v", got, want)
	}
	workerAddr := net.JoinHostPort(host, strconv.Itoa(registryPort))
	if calls := entryDM.GetEntryNodeCalls(); len(calls) != 1 || !reflect.DeepEqual(calls[0].WorkerAddresses, []string{workerAddr}) {
		t.Errorf("entry calls = %+v, want worker address %s", calls, workerAddr)
	}
	contributors, err := o.MetricContributorIDs()
	if err != nil || !reflect.DeepEqual(contributors, []string{constants.MetricsLocalContributorID, workerAddr}) {
		t.Errorf("metric contributors = %v, error %v; want local and %s", contributors, err, workerAddr)
	}
	if !runStarted.Load() {
		t.Error("distributed run was not submitted")
	}
}

// Ticket 01: when the engines never serve the API port the run waits on, the start fails with
// the API wait error and stops the engines. spt run returns that error, and exits 1
// (TestRunCmdExitsOneWhenTheDistributedAPIWaitFails).
func TestDistributedRunFailsWhenTheAPIWaitFails(t *testing.T) {
	registryPort := listenOnFreePort(t)
	apiPort := closedPort(t)
	o := NewMultiHostOrchestrator([]*hostparse.HostInfo{
		{Host: "127.0.0.1", IsLocal: true, Original: "entry"},
		{Host: "127.0.0.1", IsLocal: true, Original: "worker1"},
	}, 2)
	o.SetNotifier(func(string) {})
	o.SetAPIPort(apiPort)
	o.SetRMIRegistryPort(registryPort)
	o.detectAdvIP = func(context.Context, *hostparse.HostInfo) (string, error) { return "127.0.0.1", nil }
	entryDM, workerDM := NewMockDockerManager(), NewMockDockerManager()
	o.hosts[0].DockerManager, o.hosts[1].DockerManager = entryDM, workerDM
	o.hosts[0].SetStatus(HostStatusReady)
	o.hosts[1].SetStatus(HostStatusReady)
	wrapper := NewMultiHostTestOrchestrator(o)
	wrapper.SetCallbacks(func(*TestStatus) {}, func(*MultiNodeMetricsUpdate) {}, func(string) {}, func(string) {})
	// The real wait on the run's API port, with a short budget in place of the 30 s one.
	wrapper.waitForAPIs = func(ctx context.Context, _ time.Duration) error {
		return o.WaitForAPIs(ctx, 300*time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	params := scenario.ScenarioParams{WorkloadType: "write", Threads: 1}
	err := wrapper.StartTestWithContent(ctx, "test-image", params, []byte(`Load.config({}).run();`), []byte("storage: {}\n"))
	if err == nil {
		_ = wrapper.StopTest()
		t.Fatal("StartTestWithContent() error = nil, want the API wait failure")
	}
	if !strings.Contains(err.Error(), "failed waiting for APIs after distributed start") {
		t.Fatalf("StartTestWithContent() error = %v, want the API wait failure", err)
	}
	if entryDM.GetCleanupCallCount() == 0 || workerDM.GetCleanupCallCount() == 0 {
		t.Error("a failed API wait left an engine container running")
	}
}
