package verification

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

const (
	waitReportSecret   = "VERIFYSECRETVALUE42"
	waitReportEndpoint = "http://10.9.8.7:9020"
)

var (
	verifyInspectCommand = strings.Join([]string{
		constants.DockerCommand, constants.DockerCmdInspect, constants.DockerFlagFormat,
		"{{.State.Status}}:{{.State.ExitCode}}:{{.State.OOMKilled}}", mockContainerID,
	}, " ")
	verifyLogsCommand = strings.Join([]string{constants.DockerCommand, constants.DockerCmdLogs, mockContainerID}, " ")
)

// verifyPortsWithContainer runs verify's port wait against a started container whose engine
// ports open as open says, with the given docker inspect and docker logs responses.
func verifyPortsWithContainer(t *testing.T, open bool, inspect, logs MockCommandResponse) (*NodeResult, *MockCommandExecutor, string) {
	t.Helper()
	previous := portWaitTimeout
	portWaitTimeout = 50 * time.Millisecond
	t.Cleanup(func() { portWaitTimeout = previous })

	host := &hostparse.HostInfo{Host: "node.example.com", Original: "node.example.com"}
	executor := &MockCommandExecutor{Commands: map[string]MockCommandResponse{
		verifyInspectCommand: inspect,
		verifyLogsCommand:    logs,
	}}
	network := &MockNetworkChecker{OpenPorts: map[string]bool{
		fmt.Sprintf("%s:%d", host.Host, constants.DefaultRMIRegistryPort): open,
		fmt.Sprintf("%s:%d", host.Host, constants.DefaultSptAPIPort):      open,
	}}
	v := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{}, executor, network, &RealTimeProvider{})
	result := &NodeResult{Host: host.Host, HostInfo: host, containerID: mockContainerID}
	v.stepVerifyPorts(host, result)
	report := &Report{}
	printed := capOut(t, func() { report.printCheck("Ports Accessible", result.PortsAccessible, false) })
	return result, executor, printed
}

func executedDockerCommand(executor *MockCommandExecutor, command string) bool {
	for _, execution := range executor.ExecutedCommands {
		if strings.Join(execution.Command, " ") == command {
			return true
		}
	}
	return false
}

// assertFailureFirst checks the printed check names the wait failure before any detail.
func assertFailureFirst(t *testing.T, printed string, detail string) {
	t.Helper()
	failAt := strings.Index(printed, "FAIL")
	detailAt := strings.Index(printed, detail)
	if failAt < 0 || detailAt < 0 || failAt > detailAt {
		t.Fatalf("printed check must report the wait failure, then %q:\n%s", detail, printed)
	}
}

func TestVerifyPortsFailureReportsExitedContainer(t *testing.T) {
	t.Setenv(constants.EnvS3SecretKey, waitReportSecret)
	t.Setenv(constants.EnvS3Endpoints, waitReportEndpoint+",10.9.8.6:9020")
	logs := strings.Join([]string{
		"starting engine",
		"connecting to " + waitReportEndpoint + " and 10.9.8.6:9020",
		"secret " + waitReportSecret + " loaded",
		"java.net.BindException: Failed to bind to 0.0.0.0:9999",
	}, "\n")
	result, _, printed := verifyPortsWithContainer(t, false,
		MockCommandResponse{Stdout: "exited:1:false"}, MockCommandResponse{Stdout: logs})

	check := result.PortsAccessible
	if check.Passed || !strings.Contains(check.Message, "Service startup wait ended") {
		t.Fatalf("the wait failure must stay the check's message: %+v", check)
	}
	for _, want := range []string{
		"Container " + mockContainerID + ": exited with exit code 1",
		"Last 4 container log lines:",
		"Failed to bind to 0.0.0.0:9999",
		"connecting to *** and ***",
		"secret *** loaded",
	} {
		if !strings.Contains(check.Detail, want) || !strings.Contains(printed, want) {
			t.Fatalf("detail lacks %q:\ndetail:\n%s\nprinted:\n%s", want, check.Detail, printed)
		}
	}
	for _, leaked := range []string{waitReportSecret, "10.9.8.7", "10.9.8.6"} {
		if strings.Contains(printed, leaked) {
			t.Fatalf("printed check leaked %q:\n%s", leaked, printed)
		}
	}
	assertFailureFirst(t, printed, "exited with exit code 1")
}

func TestVerifyPortsFailureReportsRunningContainerLastLines(t *testing.T) {
	lines := make([]string, 30)
	for i := range lines {
		lines[i] = fmt.Sprintf("startup line %02d", i)
	}
	result, _, printed := verifyPortsWithContainer(t, false,
		MockCommandResponse{Stdout: "running:0:false"}, MockCommandResponse{Stdout: strings.Join(lines, "\n")})

	detail := result.PortsAccessible.Detail
	for _, want := range []string{"Container " + mockContainerID + ": running", "Last 20 container log lines:", "startup line 10", "startup line 29"} {
		if !strings.Contains(detail, want) {
			t.Fatalf("detail lacks %q:\n%s", want, detail)
		}
	}
	if strings.Contains(detail, "startup line 09") {
		t.Fatalf("detail must hold only the last 20 lines:\n%s", detail)
	}
	assertFailureFirst(t, printed, ": running")
}

func TestVerifyPortsFailureReportsMissingContainer(t *testing.T) {
	result, _, _ := verifyPortsWithContainer(t, false,
		MockCommandResponse{Stderr: "Error: No such object: " + mockContainerID, Error: errors.New("exit status 1")},
		MockCommandResponse{Error: errors.New("exit status 1")})
	if !strings.Contains(result.PortsAccessible.Detail, "missing (no such container)") {
		t.Fatalf("detail must name the container missing:\n%s", result.PortsAccessible.Detail)
	}
}

func TestVerifyPortsFailureStillReportsWaitWhenStateAndLogsUnreadable(t *testing.T) {
	result, _, printed := verifyPortsWithContainer(t, false,
		MockCommandResponse{Stderr: "Cannot connect to the Docker daemon", Error: errors.New("exit status 1")},
		MockCommandResponse{Error: errors.New("ssh: connection reset")})

	check := result.PortsAccessible
	if check.Passed || !strings.Contains(check.Message, "Service startup wait ended") {
		t.Fatalf("the wait failure must stay the check's message: %+v", check)
	}
	for _, want := range []string{
		"could not read container state: exit status 1 (stderr: Cannot connect to the Docker daemon)",
		"could not read container logs:",
		"ssh: connection reset",
	} {
		if !strings.Contains(check.Detail, want) {
			t.Fatalf("detail lacks %q:\n%s", want, check.Detail)
		}
	}
	assertFailureFirst(t, printed, "could not read container state")
}

func TestVerifyPortsSuccessReadsNoContainerDetail(t *testing.T) {
	result, executor, printed := verifyPortsWithContainer(t, true,
		MockCommandResponse{Stdout: "running:0:false"}, MockCommandResponse{Stdout: "ready"})
	if !result.PortsAccessible.Passed || result.PortsAccessible.Detail != "" {
		t.Fatalf("a passed wait must carry no detail: %+v", result.PortsAccessible)
	}
	if executedDockerCommand(executor, verifyInspectCommand) || executedDockerCommand(executor, verifyLogsCommand) {
		t.Fatalf("a passed wait must not read the container: %v", executor.ExecutedCommands)
	}
	if strings.Count(strings.TrimSpace(printed), "\n") != 0 || strings.Contains(printed, "Container ") {
		t.Fatalf("a passed check prints one line:\n%s", printed)
	}
}
