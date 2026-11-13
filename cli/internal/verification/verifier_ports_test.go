package verification

import (
	"context"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	dockerconf "github.com/dell/storage-performance-tool/cli/internal/docker"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// dockerRunCommand returns the first docker run command the executor saw.
func dockerRunCommand(t *testing.T, executions []MockCommandExecution) []string {
	t.Helper()
	for _, execution := range executions {
		if len(execution.Command) > 1 && execution.Command[0] == constants.DockerCommand && execution.Command[1] == constants.DockerCmdRun {
			return execution.Command
		}
	}
	t.Fatalf("no docker run command among %v", executions)
	return nil
}

func TestStartNodeContainer_EngineListensOnVerifiedPorts(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		want   []string
		absent []string
	}{
		{
			name:   "non-default API port",
			config: Config{NetworkMode: constants.HostNetworkMode, APIPort: 19100},
			want:   []string{"--network host", " --run-node --run-port=19100"},
			absent: []string{constants.EngineNodePortArgPrefix, " -p "},
		},
		{
			name:   "default API port",
			config: Config{NetworkMode: constants.HostNetworkMode, APIPort: constants.DefaultSptAPIPort},
			want:   []string{"--run-node --run-port=9999"},
			absent: []string{constants.EngineNodePortArgPrefix},
		},
		{
			name:   "unset ports take the defaults",
			config: Config{NetworkMode: constants.HostNetworkMode},
			want:   []string{"--run-node --run-port=9999"},
			absent: []string{constants.EngineNodePortArgPrefix},
		},
		{
			name:   "bridge publishes the API port it serves",
			config: Config{NetworkMode: constants.BridgeNetworkMode, APIPort: 19100, RMIPortStart: 40000, RMIPortCount: 2},
			want:   []string{"-p 1099:1099/tcp", "-p 19100:19100/tcp", "-p 40000:40000/tcp", "-p 40001:40001/tcp", "--run-port=19100"},
			absent: []string{"-p 9999:9999/tcp", "--run-port=9999", constants.EngineNodePortArgPrefix},
		},
		{
			name:   "RDMA keeps its device flags",
			config: Config{NetworkMode: constants.HostNetworkMode, APIPort: 19100, UseRdma: true},
			want: []string{
				"--device " + constants.RdmaDevicePath, "--cap-add " + constants.RdmaCapIpcLock,
				"--ulimit " + constants.RdmaUlimitMemlock, "--run-node --run-port=19100",
			},
		},
		{
			name:   "non-default registry port",
			config: Config{NetworkMode: constants.HostNetworkMode, APIPort: 19100, RMIRegistryPort: 11099},
			want:   []string{"--run-node --run-port=19100 --load-step-node-port=11099"},
		},
		{
			name:   "default registry port leaves the engine default",
			config: Config{NetworkMode: constants.HostNetworkMode, RMIRegistryPort: constants.DefaultRMIRegistryPort},
			absent: []string{constants.EngineNodePortArgPrefix},
		},
		{
			name:   "bridge publishes the registry port it serves",
			config: Config{NetworkMode: constants.BridgeNetworkMode, APIPort: 19100, RMIRegistryPort: 11099},
			want:   []string{"-p 11099:11099/tcp", "-p 19100:19100/tcp", "--run-port=19100 --load-step-node-port=11099"},
			absent: []string{"-p 1099:1099/tcp", "-p 9999:9999/tcp"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			executor := &FlexibleMockCommandExecutor{}
			host := &hostparse.HostInfo{Host: "node.example.com", Original: "node.example.com"}
			v := NewVerifierWithDeps([]*hostparse.HostInfo{host}, tt.config, executor, &MockNetworkChecker{}, &RealTimeProvider{})
			if check := v.startNodeContainer(host, &NodeResult{}); !check.Passed {
				t.Fatalf("start failed: %+v", check)
			}
			run := strings.Join(dockerRunCommand(t, executor.ExecutedCommands), " ")
			for _, want := range tt.want {
				if !strings.Contains(run, want) {
					t.Errorf("docker run lacks %q: %s", want, run)
				}
			}
			for _, absent := range tt.absent {
				if strings.Contains(run, absent) {
					t.Errorf("docker run has %q: %s", absent, run)
				}
			}
		})
	}
}

// recordingPreflight records the ports verify checks for conflicts.
type recordingPreflight struct {
	mockPreflight
	ports [][]int
}

func (p *recordingPreflight) CheckPorts(_ context.Context, host *hostparse.HostInfo, registryPort, apiPort, rmiPortStart, rmiPortCount int) (*dockerconf.ConflictInfo, error) {
	p.ports = append(p.ports, []int{registryPort, apiPort, rmiPortStart, rmiPortCount})
	return &dockerconf.ConflictInfo{Host: host}, nil
}

// TestVerifyNode_NonDefaultPortsAreConsistent stands in for a command-level test: verify's engine
// serves, and verify checks, the same non-default ports throughout.
func TestVerifyNode_NonDefaultPortsAreConsistent(t *testing.T) {
	const apiPort, registryPort = 19100, 11099
	host := &hostparse.HostInfo{Host: "node.example.com", User: "root", Original: "root@node.example.com"}
	executor := &FlexibleMockCommandExecutor{}
	network := &MockNetworkChecker{HTTPResponses: map[string]MockHTTPResponse{
		"http://node.example.com:19100/metrics/json": {Response: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}},
		"http://node.example.com:19100/run":          {Response: &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(""))}},
	}}
	network.SetupPortsOpen(host.Host, []int{registryPort, apiPort})
	pf := &recordingPreflight{}
	config := Config{NetworkMode: constants.HostNetworkMode, APIPort: apiPort, RMIRegistryPort: registryPort, RMIPortStart: 40000, RMIPortCount: 10}
	v := NewVerifierWithDepsAndPreflight([]*hostparse.HostInfo{host}, config, executor, network, &RealTimeProvider{}, pf)

	result := v.verifyNode(host)

	if !result.Overall.Passed || result.Overall.Message != "READY" {
		t.Fatalf("Overall = %+v; ports %+v, metrics %+v, control %+v", result.Overall, result.PortsAccessible, result.MetricsEndpoint, result.ControlEndpoint)
	}
	if want := [][]int{{registryPort, apiPort, 40000, 10}}; !slices.EqualFunc(pf.ports, want, slices.Equal[[]int]) {
		t.Errorf("preflight checked %v, want %v", pf.ports, want)
	}
	run := strings.Join(dockerRunCommand(t, executor.ExecutedCommands), " ")
	if !strings.HasSuffix(run, " --run-node --run-port=19100 --load-step-node-port=11099") {
		t.Errorf("docker run = %s", run)
	}
	var probed []int
	for _, check := range network.PortChecks {
		if check.Host != host.Host {
			t.Errorf("probed host %q", check.Host)
		}
		probed = append(probed, check.Port)
	}
	if !slices.Equal(probed, []int{registryPort, apiPort}) {
		t.Errorf("probed ports %v, want [%d %d]", probed, registryPort, apiPort)
	}
	if want := "Essential service ports accessible (11099 RMI registry, 19100 REST API)"; result.PortsAccessible.Message != want {
		t.Errorf("ports message = %q, want %q", result.PortsAccessible.Message, want)
	}
	if want := []string{"http://node.example.com:19100/metrics/json", "http://node.example.com:19100/run"}; !slices.Equal(network.HTTPRequests, want) {
		t.Errorf("HTTP requests %v, want %v", network.HTTPRequests, want)
	}
}

// TestWaitForPorts_DeadlineRoundKeepsLastObservation runs a wait that is an exact multiple of the
// poll interval, so its last tick fires at the deadline, with the registry always open and the API
// port always closed. The round at the deadline has no time to probe and must not make verify
// report the registry closed. A run in which a loaded machine kept both ports from being probed
// before the deadline shows nothing about that round, so it is skipped, not failed.
func TestWaitForPorts_DeadlineRoundKeepsLastObservation(t *testing.T) {
	const runs = 40
	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true}
	apiProbes := 0
	network := &startupNetworkChecker{probe: func(_ string, port int, _ time.Duration) bool {
		if port != constants.DefaultRMIRegistryPort {
			apiProbes++
		}
		return port == constants.DefaultRMIRegistryPort
	}}
	v := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{APIPort: constants.DefaultSptAPIPort}, &MockCommandExecutor{}, network, &RealTimeProvider{})
	const interval = 2 * time.Millisecond
	observed := 0
	for run := range runs {
		apiProbes = 0
		ctx, cancel := context.WithTimeout(context.Background(), 10*interval)
		check := v.waitForPorts(ctx, host, interval)
		cancel()
		if apiProbes == 0 {
			continue
		}
		observed++
		if check.Passed || !strings.Contains(check.Message, "9999 (REST API)") || strings.Contains(check.Message, "1099") {
			t.Fatalf("run %d: the message must name only the closed API port: %+v", run, check)
		}
	}
	if observed == 0 {
		t.Skip("no run probed both ports before its deadline; the machine is too loaded for this test")
	}
}

// TestWaitForPorts_ProbeCutShortAtDeadlineKeepsLastObservation covers a probe whose timeout the
// deadline cuts short, as on a slow network: it fails at the deadline although the port is open.
func TestWaitForPorts_ProbeCutShortAtDeadlineKeepsLastObservation(t *testing.T) {
	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true}
	registryProbes := 0
	network := &startupNetworkChecker{probe: func(_ string, port int, timeout time.Duration) bool {
		if port != constants.DefaultRMIRegistryPort {
			return false
		}
		registryProbes++
		if registryProbes == 1 {
			return true
		}
		time.Sleep(timeout) // Times out, as a dial whose budget is shorter than the round trip does.
		return false
	}}
	v := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{APIPort: constants.DefaultSptAPIPort}, &MockCommandExecutor{}, network, &RealTimeProvider{})
	// The second registry probe sleeps out its timeout, which the deadline cuts short below the
	// full per-port timeout; the deadline leaves a loaded machine ample time to start that round.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	check := v.waitForPorts(ctx, host, time.Millisecond)
	if registryProbes < 2 {
		t.Fatalf("the registry was probed %d times; the test needs a probe cut short", registryProbes)
	}
	if check.Passed || !strings.Contains(check.Message, "9999 (REST API)") || strings.Contains(check.Message, "1099") {
		t.Fatalf("the message must name only the closed API port: %+v", check)
	}
}

func TestWaitForPorts_CanceledBeforeAnyProbe(t *testing.T) {
	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true}
	network := &startupNetworkChecker{probe: func(string, int, time.Duration) bool {
		t.Fatal("canceled wait probed a port")
		return true
	}}
	v := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{APIPort: 19100}, &MockCommandExecutor{}, network, &RealTimeProvider{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	check := v.waitForPorts(ctx, host, time.Millisecond)
	want := "Service startup wait ended: context canceled; Ports with no result before the wait ended: [1099 (RMI registry) 19100 (REST API)]"
	if check.Passed || check.Message != want {
		t.Fatalf("message = %q, want %q", check.Message, want)
	}
}

func TestWaitForPorts_NonDefaultPorts(t *testing.T) {
	host := &hostparse.HostInfo{Host: "node.example.com"}
	var probed []int
	network := &startupNetworkChecker{probe: func(_ string, port int, _ time.Duration) bool {
		probed = append(probed, port)
		return port == 11099
	}}
	v := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{APIPort: 19100, RMIRegistryPort: 11099}, &MockCommandExecutor{}, network, &RealTimeProvider{})
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	check := v.waitForPorts(ctx, host, 5*time.Millisecond)
	if check.Passed || !strings.HasSuffix(check.Message, "; Ports not accessible: [19100 (REST API)]") {
		t.Fatalf("check = %+v", check)
	}
	for _, port := range probed {
		if port != 11099 && port != 19100 {
			t.Fatalf("probed port %d; probes %v", port, probed)
		}
	}
}

// A port whose only probe the deadline cut short has no result: it was probed, but the probe may
// have timed out on an open port, so verify names it neither open nor closed.
func TestWaitForPorts_FirstProbeCutShortHasNoResult(t *testing.T) {
	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true}
	network := &startupNetworkChecker{probe: func(_ string, _ int, timeout time.Duration) bool {
		time.Sleep(timeout)
		return false
	}}
	v := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{APIPort: 19100}, &MockCommandExecutor{}, network, &RealTimeProvider{})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	check := v.waitForPorts(ctx, host, time.Millisecond)
	want := "Service startup wait ended: context deadline exceeded; Ports with no result before the wait ended: [1099 (RMI registry) 19100 (REST API)]"
	if check.Passed || check.Message != want {
		t.Fatalf("message = %q, want %q", check.Message, want)
	}
}

// Ports seen open in different rounds but never in the same one fail the wait, and the message
// does not call them accessible.
func TestPortsMessage_OpenAcrossRoundsIsNotAccessible(t *testing.T) {
	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true}
	v := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{APIPort: 19100}, &MockCommandExecutor{}, &startupNetworkChecker{}, &RealTimeProvider{})
	ports := v.servicePorts()
	seen := []portState{portOpen, portOpen}
	if got, want := v.portsMessage(ports, seen, false), "Ports each seen open, but never all in one round: [1099 (RMI registry) 19100 (REST API)]"; got != want {
		t.Fatalf("failed round: %q, want %q", got, want)
	}
	if got, want := v.portsMessage(ports, seen, true), "Essential service ports accessible (1099 RMI registry, 19100 REST API)"; got != want {
		t.Fatalf("passed round: %q, want %q", got, want)
	}
}
