package tui

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/stdcopy"
)

const (
	waitReportSecret   = "RUNSECRETVALUE42"
	waitReportEndpoint = "http://10.9.8.7:9020"
	// waitReportRefusedPort is a port nothing listens on, so an API wait on it fails at once.
	waitReportRefusedPort = "1"
)

// waitForAPIsWithContainer runs spt run's multi-host API wait for one host whose container the
// mock describes, and returns the progress lines and the wait's error.
func waitForAPIsWithContainer(t *testing.T, apiPort string, mock *MockDockerManager) ([]string, error) {
	t.Helper()
	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: "127.0.0.1", Original: "worker-a"}}, 1)
	orchestrator.apiPort = apiPort
	var mu sync.Mutex
	var lines []string
	orchestrator.SetNotifier(func(line string) {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, line)
	})
	host := orchestrator.hosts[0]
	host.DockerManager = mock
	host.ContainerID = mock.ContainerID()
	host.SetStatus(HostStatusRunning)
	err := orchestrator.WaitForAPIs(context.Background(), 100*time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	return append([]string(nil), lines...), err
}

// requireAfterWaitFailure checks the progress lines report the host's wait failure before want.
func requireAfterWaitFailure(t *testing.T, lines []string, want ...string) {
	t.Helper()
	output := strings.Join(lines, "\n")
	failureAt := strings.Index(output, "❌ worker-a: API timeout:")
	if failureAt < 0 {
		t.Fatalf("progress lacks the wait failure:\n%s", output)
	}
	for _, text := range want {
		at := strings.Index(output, text)
		if at < 0 || at < failureAt {
			t.Fatalf("progress must report %q after the wait failure:\n%s", text, output)
		}
	}
}

func TestWaitForAPIsFailureReportsExitedContainer(t *testing.T) {
	t.Setenv(constants.EnvS3SecretKey, waitReportSecret)
	t.Setenv(constants.EnvS3Endpoint, waitReportEndpoint)
	mock := NewMockDockerManager()
	mock.SetAPIWaitContainer(command.ContainerState{Status: "exited", ExitCode: 1}, nil, strings.Join([]string{
		"starting engine against " + waitReportEndpoint,
		"--secret-key " + waitReportSecret,
		"java.net.BindException: Failed to bind to 0.0.0.0:9999",
	}, "\n"), nil)

	lines, err := waitForAPIsWithContainer(t, waitReportRefusedPort, mock)
	if err == nil || err.Error() != "no APIs became ready within timeout" {
		t.Fatalf("WaitForAPIs() error = %v, want the unchanged wait failure", err)
	}
	requireAfterWaitFailure(t, lines,
		"Container mock-contain: exited with exit code 1",
		"Last 3 container log lines:",
		"Failed to bind to 0.0.0.0:9999",
		"starting engine against http://***",
		"--secret-key ***",
		"API Summary: 0/1 APIs ready")
	output := strings.Join(lines, "\n")
	for _, leaked := range []string{waitReportSecret, "10.9.8.7"} {
		if strings.Contains(output, leaked) {
			t.Fatalf("progress leaked %q:\n%s", leaked, output)
		}
	}
	if calls := mock.APIWaitReportCalls(); len(calls) != 1 || calls[0] != mock.ContainerID() {
		t.Fatalf("report calls = %v, want one for the host's container", calls)
	}
}

func TestWaitForAPIsFailureReportsRunningContainer(t *testing.T) {
	mock := NewMockDockerManager()
	logs := make([]string, 25)
	for i := range logs {
		logs[i] = fmt.Sprintf("engine startup line %02d", i)
	}
	mock.SetAPIWaitContainer(command.ContainerState{Status: "running"}, nil, strings.Join(logs, "\n"), nil)

	lines, _ := waitForAPIsWithContainer(t, waitReportRefusedPort, mock)
	requireAfterWaitFailure(t, lines,
		"Container mock-contain: running", "Last 20 container log lines:", "engine startup line 24")
	if output := strings.Join(lines, "\n"); strings.Contains(output, "engine startup line 04") {
		t.Fatalf("progress must hold only the last 20 log lines:\n%s", output)
	}
}

func TestWaitForAPIsFailureStillReportsWaitWhenLogsUnreadable(t *testing.T) {
	mock := NewMockDockerManager()
	mock.SetAPIWaitContainer(command.ContainerState{}, errors.New("ssh: connection reset"), "",
		errors.New("docker logs: connection reset"))

	lines, err := waitForAPIsWithContainer(t, waitReportRefusedPort, mock)
	if err == nil || err.Error() != "no APIs became ready within timeout" {
		t.Fatalf("WaitForAPIs() error = %v, want the unchanged wait failure", err)
	}
	requireAfterWaitFailure(t, lines,
		"could not read container state: ssh: connection reset",
		"could not read container logs: docker logs: connection reset")
}

func TestWaitForAPIsSuccessPrintsNothingNew(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ready":true,"status":"ready"}`))
	}))
	defer server.Close()
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatal(err)
	}
	mock := NewMockDockerManager()
	mock.SetAPIWaitContainer(command.ContainerState{Status: "running"}, nil, "should not be read", nil)

	lines, err := waitForAPIsWithContainer(t, port, mock)
	if err != nil {
		t.Fatalf("WaitForAPIs() error = %v", err)
	}
	want := []string{
		"⏳ Waiting for APIs to be ready on 1 host(s)...",
		fmt.Sprintf("  ✅ API ready on worker-a (http://127.0.0.1:%s)", port),
		"API Summary: 1/1 APIs ready",
	}
	if strings.Join(lines, "\n") != strings.Join(want, "\n") {
		t.Fatalf("success progress changed:\n got %q\nwant %q", lines, want)
	}
	if calls := mock.APIWaitReportCalls(); len(calls) != 0 {
		t.Fatalf("a ready API must not read its container: %v", calls)
	}
}

func TestRemoteWorkerReadinessFailureReportsContainer(t *testing.T) {
	t.Setenv(constants.EnvS3AccessKey, "RUNACCESSKEY77")
	mgr, mock, _ := newTestRemoteManager(t)
	waitErr := errors.New("context deadline exceeded")
	mgr.proberRun = func(context.Context, string, time.Duration) error { return waitErr }
	mock.SetCommandSuccess(strings.Join([]string{
		constants.DockerCommand, constants.DockerCmdInspect, constants.DockerFlagFormat,
		constants.DockerContainerStateFormat, "abc123def456",
	}, " "), "exited:137:true")
	mock.SetCommandResponse("docker logs --tail 20 abc123def456 2>&1", command.MockResponse{
		Stdout: "loading key RUNACCESSKEY77", Stderr: "Failed to bind to 0.0.0.0:1099",
	})

	_, err := mgr.StartWorkerNodeContainerContext(context.Background(), constants.DefaultSptImage, "192.0.2.10", 1099, 1, nil)
	if !errors.Is(err, waitErr) {
		t.Fatalf("start error = %v, want the wrapped wait failure", err)
	}
	message := err.Error()
	if !strings.HasPrefix(message, "worker API readiness: context deadline exceeded\n") {
		t.Fatalf("the wait failure must come first:\n%s", message)
	}
	for _, want := range []string{
		"Container abc123def456: exited with exit code 137 (killed for running out of memory)",
		"Failed to bind to 0.0.0.0:1099",
		"loading key ***",
	} {
		if !strings.Contains(message, want) {
			t.Fatalf("start error lacks %q:\n%s", want, message)
		}
	}
	if strings.Contains(message, "RUNACCESSKEY77") {
		t.Fatalf("start error leaked the access key:\n%s", message)
	}
}

func TestRemoteWorkerReadinessSuccessReadsNoContainer(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)
	mgr.proberRun = func(context.Context, string, time.Duration) error { return nil }
	if _, err := mgr.StartWorkerNodeContainerContext(context.Background(), constants.DefaultSptImage, "192.0.2.10", 1099, 1, nil); err != nil {
		t.Fatalf("start error = %v", err)
	}
	for _, executed := range mock.GetExecutedCommands() {
		joined := strings.Join(executed.Command, " ")
		if strings.Contains(joined, "docker inspect --format {{.State") || strings.Contains(joined, "docker logs") {
			t.Fatalf("a ready worker must not read its container: %s", joined)
		}
	}
}

func TestSingleHostStartupDiagnosticsReportStateFirst(t *testing.T) {
	t.Setenv(constants.EnvS3Endpoints, waitReportEndpoint)
	var logs bytes.Buffer
	_, _ = stdcopy.NewStdWriter(&logs, stdcopy.Stdout).Write([]byte("connecting to " + waitReportEndpoint + "\nFailed to bind\n"))
	f := &fakeDockerClient{logBody: logs.Bytes(), containerState: &container.State{Status: "exited", ExitCode: 3}}
	orchestrator := NewTestOrchestrator(&DockerManager{client: f, ctx: context.Background()}, constants.SptAPIPort, "")
	orchestrator.containerID = "cid"

	got := orchestrator.containerStartupDiagnostics(context.Background())
	want := "Container state: exited with exit code 3\n\nDocker logs:\nconnecting to http://***\nFailed to bind"
	if got != want {
		t.Fatalf("diagnostics =\n%s\nwant\n%s", got, want)
	}
}

func TestSingleHostStartupDiagnosticsReportUnreadableState(t *testing.T) {
	f := &fakeDockerClient{containerInspectErr: errors.New("daemon unavailable")}
	orchestrator := NewTestOrchestrator(&DockerManager{client: f, ctx: context.Background()}, constants.SptAPIPort, "")
	orchestrator.containerID = "cid"

	got := orchestrator.containerStartupDiagnostics(context.Background())
	if !strings.HasPrefix(got, "could not read container state: daemon unavailable\n\nDocker logs:\nlog1") {
		t.Fatalf("diagnostics must say the state was unreadable and keep the logs:\n%s", got)
	}
}

func TestDockerManagerContainerStateReportsMissingContainer(t *testing.T) {
	f := &fakeDockerClient{containerInspectErr: errors.New("Error response from daemon: No such container: cid")}
	dm := &DockerManager{client: f, ctx: context.Background()}
	state, err := dm.containerState(context.Background(), "cid")
	if err != nil || !state.Missing || state.String() != "missing (no such container)" {
		t.Fatalf("containerState() = %+v, %v; want missing", state, err)
	}
}

// TestSingleHostAPIWaitFailurePrintsFailureOnceBeforeDiagnostics runs the single-host start
// through a failed API wait: the failure line appears once, followed by the diagnostics.
func TestSingleHostAPIWaitFailurePrintsFailureOnceBeforeDiagnostics(t *testing.T) {
	previous := apiReadyTimeout
	apiReadyTimeout = 200 * time.Millisecond
	t.Cleanup(func() { apiReadyTimeout = previous })
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()

	mock := NewMockDockerManager()
	mock.SetAPIWaitContainer(command.ContainerState{Status: "exited", ExitCode: 1}, nil, "", nil)
	orchestrator := NewTestOrchestrator(mock, constants.SptAPIPort, "")
	orchestrator.apiClient = NewSptAPIClient(server.URL)
	var errorsReported []string
	orchestrator.SetCallbacks(nil, nil, nil, func(message string) { errorsReported = append(errorsReported, message) })

	err := orchestrator.StartTestWithContent(context.Background(), "test-image",
		scenario.Params{WorkloadType: "write", Threads: 1, ObjectSize: "1MB"},
		[]byte("// scenario\n"), []byte("storage: {}\n"))
	if err == nil || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("StartTestWithContent() error = %v, want the wrapped wait failure", err)
	}
	output := strings.Join(append(errorsReported, err.Error()), "\n")
	const failure = "spt API failed to become ready: spt API did not become ready after 200ms"
	if count := strings.Count(output, failure); count != 1 {
		t.Fatalf("the failure line appears %d times, want once:\n%s", count, output)
	}
	failureAt := strings.Index(output, failure)
	headerAt := strings.Index(output, "Container startup diagnostics:\nContainer state: exited with exit code 1")
	if headerAt < failureAt {
		t.Fatalf("the diagnostics must follow the failure line:\n%s", output)
	}
}
