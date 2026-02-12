/*
Copyright © 2025 Dell Technologies
*/

package verification

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

func TestNewVerifier(t *testing.T) {
	hosts := []*hostparse.HostInfo{
		{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
	}
	config := Config{
		NetworkMode:  "bridge",
		RMIPortStart: 40000,
		RMIPortCount: 10,
		MinHosts:     1,
	}

	verifier := NewVerifier(hosts, config)

	if verifier == nil {
		t.Fatal("NewVerifier returned nil")
	}

	if len(verifier.hosts) != 1 {
		t.Errorf("Expected 1 host, got %d", len(verifier.hosts))
	}

	if verifier.config.NetworkMode != "bridge" {
		t.Errorf("Expected bridge mode, got %s", verifier.config.NetworkMode)
	}

	// Check that real implementations are set
	if verifier.cmdExecutor == nil {
		t.Error("cmdExecutor should not be nil")
	}
	if verifier.netChecker == nil {
		t.Error("netChecker should not be nil")
	}
	if verifier.timeProvider == nil {
		t.Error("timeProvider should not be nil")
	}
}

func TestCheckLocalDocker_Success(t *testing.T) {
	mockCmd := &MockCommandExecutor{}
	mockCmd.SetupDockerSuccess()

	mockTime := NewMockTimeProvider(time.Now())

	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{}, mockCmd, &MockNetworkChecker{}, mockTime)

	result := verifier.checkLocalDocker(host, mockTime.Now())

	if !result.Passed {
		t.Errorf("Expected check to pass, but it failed: %s", result.Message)
	}

	if result.Message != "Local Docker 28.3.3-ce accessible" {
		t.Errorf("Unexpected message: %s", result.Message)
	}

	// Verify command was executed
	if len(mockCmd.ExecutedCommands) != 1 {
		t.Errorf("Expected 1 command execution, got %d", len(mockCmd.ExecutedCommands))
	}

	expectedCmd := []string{"docker", "version", "--format", "{{.Server.Version}}"}
	actualCmd := mockCmd.ExecutedCommands[0].Command
	if !equalSlice(expectedCmd, actualCmd) {
		t.Errorf("Expected command %v, got %v", expectedCmd, actualCmd)
	}
}

func TestCheckRemoteDocker_Success(t *testing.T) {
	mockCmd := &MockCommandExecutor{}
	mockCmd.SetupDockerSuccess()

	mockTime := NewMockTimeProvider(time.Now())

	host := &hostparse.HostInfo{Host: "remote.example.com", User: "root", IsLocal: false, Original: "root@remote.example.com"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{}, mockCmd, &MockNetworkChecker{}, mockTime)

	result := verifier.checkRemoteDocker(host, mockTime.Now())

	if !result.Passed {
		t.Errorf("Expected check to pass, but it failed: %s", result.Message)
	}

	if result.Message != "Docker 28.3.3-ce accessible via SSH" {
		t.Errorf("Unexpected message: %s", result.Message)
	}
}

func TestCheckDocker_SSHConnectionFailure(t *testing.T) {
	mockCmd := &MockCommandExecutor{
		Commands: map[string]MockCommandResponse{},
	}
	// Add failing docker version command
	mockCmd.Commands[fmt.Sprintf("%s %s %s %s", constants.DockerCommand, constants.DockerCmdVersion, constants.DockerFlagFormat, constants.DockerVersionFormat)] = MockCommandResponse{
		Stdout: "",
		Stderr: "ssh: connect to host remote.example.com port 22: Connection refused",
		Error:  &mockError{msg: "ssh connection failed"},
	}

	mockTime := NewMockTimeProvider(time.Now())

	host := &hostparse.HostInfo{Host: "remote.example.com", User: "root", IsLocal: false, Original: "root@remote.example.com"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{}, mockCmd, &MockNetworkChecker{}, mockTime)

	result := verifier.checkRemoteDocker(host, mockTime.Now())

	if result.Passed {
		t.Error("Expected check to fail due to SSH connection failure")
	}

	if result.Message != "SSH connection failed" {
		t.Errorf("Expected SSH connection failure message, got: %s", result.Message)
	}
}

func TestCheckPorts_AllOpen(t *testing.T) {
	mockCmd := &MockCommandExecutor{}
	mockCmd.SetupDockerSuccess()

	mockNet := &MockNetworkChecker{}
	mockNet.SetupPortsOpen("test.example.com", []int{1099, 9999})

	mockTime := NewMockTimeProvider(time.Now())

	config := Config{
		RMIPortStart: 40000,
		RMIPortCount: 10,
		APIPort:      constants.DefaultSptAPIPort,
	}

	host := &hostparse.HostInfo{Host: "test.example.com", IsLocal: false, Original: "test.example.com"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, config, mockCmd, mockNet, mockTime)

	result := verifier.checkPorts(host)

	if !result.Passed {
		t.Errorf("Expected ports check to pass, but it failed: %s", result.Message)
	}

	// Verify correct ports were checked (only essential service ports)
	expectedChecks := 2 // 1099, 9999 only
	if len(mockNet.PortChecks) != expectedChecks {
		t.Errorf("Expected %d port checks, got %d", expectedChecks, len(mockNet.PortChecks))
	}

	// Verify the correct ports were checked
	expectedPorts := map[int]bool{1099: false, 9999: false}
	for _, check := range mockNet.PortChecks {
		if _, found := expectedPorts[check.Port]; found {
			expectedPorts[check.Port] = true
		} else {
			t.Errorf("Unexpected port check for port %d", check.Port)
		}
	}
	for port, checked := range expectedPorts {
		if !checked {
			t.Errorf("Expected port %d to be checked but it wasn't", port)
		}
	}
}

func TestCheckPorts_SomeClosed(t *testing.T) {
	mockCmd := &MockCommandExecutor{}
	mockCmd.SetupDockerSuccess()

	mockNet := &MockNetworkChecker{}
	mockNet.SetupPortsOpen("test.example.com", []int{1099})   // Only RMI registry open
	mockNet.SetupPortsClosed("test.example.com", []int{9999}) // REST API closed

	mockTime := NewMockTimeProvider(time.Now())

	config := Config{
		RMIPortStart: 40000,
		RMIPortCount: 10,
		APIPort:      constants.DefaultSptAPIPort,
	}

	host := &hostparse.HostInfo{Host: "test.example.com", IsLocal: false, Original: "test.example.com"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, config, mockCmd, mockNet, mockTime)

	result := verifier.checkPorts(host)

	if result.Passed {
		t.Error("Expected ports check to fail due to closed ports")
	}

	// Should mention the closed REST API port
	if !contains(result.Message, constants.SptAPIPort) {
		t.Errorf("Expected message to mention closed port %s, got: %s", constants.SptAPIPort, result.Message)
	}
}

func TestStartNodeContainer_Success(t *testing.T) {
	// Use flexible mock for container operations
	mockCmd := &FlexibleMockCommandExecutor{}
	mockTime := NewMockTimeProvider(time.Now())

	config := Config{
		NetworkMode:  "bridge",
		RMIPortStart: 40000,
		RMIPortCount: 10,
	}

	host := &hostparse.HostInfo{Host: "test.example.com", IsLocal: false, Original: "test.example.com"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, config, mockCmd, &MockNetworkChecker{}, mockTime)

	result := &NodeResult{}
	check := verifier.startNodeContainer(host, result)

	if !check.Passed {
		t.Errorf("Expected container start to succeed, but it failed: %s", check.Message)
	}

	if result.containerID != "container123456789" {
		t.Errorf("Expected containerID to be set to 'container123456789', got '%s'", result.containerID)
	}

	// Verify a docker run command was executed (auto-pull may add extra commands)
	foundRun := false
	for _, exec := range mockCmd.ExecutedCommands {
		if len(exec.Command) > 1 && exec.Command[0] == "docker" && exec.Command[1] == "run" {
			foundRun = true
			break
		}
	}
	if !foundRun {
		t.Errorf("Expected a docker run command to be executed")
	}
}

func TestParallelExecution_ResultCorrectness(t *testing.T) {
	// Test that all hosts are processed and results are correctly collected
	mockCmd := &MockCommandExecutor{}
	mockCmd.SetupDockerSuccess()
	mockCmd.SetupContainerSuccess()

	mockNet := &MockNetworkChecker{}
	mockNet.SetupPortsOpen("host1.example.com", []int{1099, 9999, 40000, 40009})
	mockNet.SetupPortsOpen("host2.example.com", []int{1099, 9999, 40000, 40009})
	mockNet.SetupPortsOpen("host3.example.com", []int{1099, 9999, 40000, 40009})

	mockTime := NewMockTimeProvider(time.Now())

	hosts := []*hostparse.HostInfo{
		{Host: "host1.example.com", User: "root", IsLocal: false, Original: "root@host1.example.com"},
		{Host: "host2.example.com", User: "root", IsLocal: false, Original: "root@host2.example.com"},
		{Host: "host3.example.com", User: "root", IsLocal: false, Original: "root@host3.example.com"},
	}

	config := Config{
		NetworkMode:  "bridge",
		RMIPortStart: 40000,
		RMIPortCount: 10,
		MinHosts:     1,
	}

	verifier := NewVerifierWithDeps(hosts, config, mockCmd, mockNet, mockTime)

	report := verifier.VerifyAll()

	if report == nil {
		t.Fatal("VerifyAll returned nil report")
	}

	// All hosts should have results
	if len(report.Results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(report.Results))
	}

	// Each host should have its correct result
	expectedHosts := []string{"host1.example.com", "host2.example.com", "host3.example.com"}
	for _, expectedHost := range expectedHosts {
		result, exists := report.Results[expectedHost]
		if !exists {
			t.Errorf("Missing result for host %s", expectedHost)
			continue
		}

		// Verify the result is associated with the correct host
		if result.Host != expectedHost {
			t.Errorf("Result for %s has wrong host field: %s", expectedHost, result.Host)
		}

		if result.HostInfo.Host != expectedHost {
			t.Errorf("Result for %s has wrong HostInfo: %s", expectedHost, result.HostInfo.Host)
		}

		// All checks should pass with our successful mocks
		if !result.DockerAvailable.Passed {
			t.Errorf("Expected Docker check to pass for host %s", expectedHost)
		}
	}
}

func TestParallelExecution_ConcurrencySafety(t *testing.T) {
	// Test with many hosts to catch potential race conditions
	mockCmd := &MockCommandExecutor{}
	mockCmd.SetupDockerSuccess()

	mockNet := &MockNetworkChecker{}
	mockTime := NewMockTimeProvider(time.Now())

	// Create many hosts to increase chance of catching race conditions
	var hosts []*hostparse.HostInfo
	for i := 0; i < 10; i++ {
		host := fmt.Sprintf("host%d.example.com", i)
		hosts = append(hosts, &hostparse.HostInfo{
			Host:     host,
			User:     "root",
			IsLocal:  false,
			Original: fmt.Sprintf("root@%s", host),
		})

		// Set up successful responses for each host
		mockNet.SetupPortsOpen(host, []int{1099, 9999, 40000, 40009})
	}

	config := Config{
		NetworkMode:  "bridge",
		RMIPortStart: 40000,
		RMIPortCount: 10,
		MinHosts:     1,
	}

	verifier := NewVerifierWithDeps(hosts, config, mockCmd, mockNet, mockTime)

	// Run multiple times to increase chance of catching race conditions
	for run := 0; run < 5; run++ {
		report := verifier.VerifyAll()

		if report == nil {
			t.Fatalf("VerifyAll returned nil report on run %d", run)
		}

		// All hosts should have results
		if len(report.Results) != 10 {
			t.Errorf("Run %d: Expected 10 results, got %d", run, len(report.Results))
		}

		// Verify no results were lost or corrupted
		for i := 0; i < 10; i++ {
			expectedHost := fmt.Sprintf("host%d.example.com", i)
			result, exists := report.Results[expectedHost]
			if !exists {
				t.Errorf("Run %d: Missing result for host %s", run, expectedHost)
				continue
			}

			if result.Host != expectedHost {
				t.Errorf("Run %d: Result for %s has wrong host field: %s", run, expectedHost, result.Host)
			}
		}

		// Clear results for next run
		verifier.results = make(map[string]*NodeResult)
	}
}

func TestErrorHandling_MixedSuccessFailure(t *testing.T) {
	// Test that some hosts can succeed while others fail
	mockCmd := &MockCommandExecutor{
		Commands: map[string]MockCommandResponse{},
	}
	mockCmd.SetupDockerSuccess()

	mockNet := &MockNetworkChecker{}
	mockTime := NewMockTimeProvider(time.Now())

	hosts := []*hostparse.HostInfo{
		{Host: "success-host.example.com", User: "root", IsLocal: false, Original: "root@success-host.example.com"},
		{Host: "failure-host.example.com", User: "root", IsLocal: false, Original: "root@failure-host.example.com"},
	}

	// Set up success for one host, failure for another
	mockNet.SetupPortsOpen("success-host.example.com", []int{1099, 9999, 40000, 40009})
	mockNet.SetupPortsClosed("failure-host.example.com", []int{1099, 9999, 40000, 40009})

	config := Config{
		NetworkMode:  "bridge",
		RMIPortStart: 40000,
		RMIPortCount: 10,
		MinHosts:     1,
	}

	verifier := NewVerifierWithDeps(hosts, config, mockCmd, mockNet, mockTime)

	report := verifier.VerifyAll()

	if report == nil {
		t.Fatal("VerifyAll returned nil report")
	}

	if len(report.Results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(report.Results))
	}

	// Success host should pass Docker check
	successResult := report.Results["success-host.example.com"]
	if successResult == nil {
		t.Fatal("Missing result for success-host.example.com")
	}
	if !successResult.DockerAvailable.Passed {
		t.Error("Expected Docker check to pass for success-host.example.com")
	}

	// Failure host should also pass Docker check but fail on ports
	failureResult := report.Results["failure-host.example.com"]
	if failureResult == nil {
		t.Fatal("Missing result for failure-host.example.com")
	}
	if !failureResult.DockerAvailable.Passed {
		t.Error("Expected Docker check to pass for failure-host.example.com")
	}
	if failureResult.PortsAccessible.Passed {
		t.Error("Expected ports check to fail for failure-host.example.com")
	}
}

func TestVerifyNode_SSHFailureStopsChecks(t *testing.T) {
	mockCmd := &MockCommandExecutor{
		Commands: map[string]MockCommandResponse{
			"true": {
				Stdout: "",
				Stderr: "ssh: connect to host remote.example.com port 22: Permission denied",
				Error:  fmt.Errorf("ssh authentication failed"),
			},
		},
	}
	mockTime := NewMockTimeProvider(time.Now())

	host := &hostparse.HostInfo{
		Host:     "remote.example.com",
		User:     "admin",
		IsLocal:  false,
		Original: "admin@remote.example.com",
	}

	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{}, mockCmd, &MockNetworkChecker{}, mockTime)

	result := verifier.verifyNode(host)

	if result == nil {
		t.Fatal("verifyNode returned nil result")
	}

	if result.SSHConnectivity.Passed {
		t.Fatal("Expected SSH connectivity check to fail")
	}
	if result.Overall.Passed {
		t.Fatal("Expected overall result to fail when SSH connectivity fails")
	}
	if !strings.Contains(result.SSHConnectivity.Message, "SSH connection failed") {
		t.Fatalf("Unexpected SSH failure message: %s", result.SSHConnectivity.Message)
	}

	// Docker checks should not have been attempted
	if len(mockCmd.ExecutedCommands) != 1 {
		t.Fatalf("Expected only SSH probe command to run, but saw %d commands", len(mockCmd.ExecutedCommands))
	}
	if got := strings.Join(mockCmd.ExecutedCommands[0].Command, " "); got != "true" {
		t.Fatalf("Expected SSH probe command 'true', got %q", got)
	}
}

func TestErrorHandling_CompleteFailures(t *testing.T) {
	// Test that even complete failures don't break parallel processing
	mockCmd := &MockCommandExecutor{
		Commands: map[string]MockCommandResponse{},
	}
	// Add failing docker version command
	mockCmd.Commands[fmt.Sprintf("%s %s %s %s", constants.DockerCommand, constants.DockerCmdVersion, constants.DockerFlagFormat, constants.DockerVersionFormat)] = MockCommandResponse{
		Stdout: "",
		Stderr: "Docker daemon not running",
		Error:  &mockError{msg: "docker daemon not accessible"},
	}

	mockTime := NewMockTimeProvider(time.Now())

	hosts := []*hostparse.HostInfo{
		{Host: "failing-host1.example.com", User: "root", IsLocal: false, Original: "root@failing-host1.example.com"},
		{Host: "failing-host2.example.com", User: "root", IsLocal: false, Original: "root@failing-host2.example.com"},
	}

	config := Config{
		NetworkMode:  "bridge",
		RMIPortStart: 40000,
		RMIPortCount: 10,
		MinHosts:     1,
	}

	verifier := NewVerifierWithDeps(hosts, config, mockCmd, &MockNetworkChecker{}, mockTime)

	report := verifier.VerifyAll()

	if report == nil {
		t.Fatal("VerifyAll returned nil report")
	}

	// Both hosts should have failed, but verification should complete
	if len(report.Results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(report.Results))
	}

	for host, result := range report.Results {
		if result.Overall.Passed {
			t.Errorf("Expected host %s to fail", host)
		}

		if result.DockerAvailable.Passed {
			t.Errorf("Expected Docker check to fail for host %s", host)
		}

		// Result should still be properly associated with correct host
		if result.Host != host {
			t.Errorf("Result for %s has wrong host field: %s", host, result.Host)
		}
	}
}

func TestTimeProviderIntegration(t *testing.T) {
	// Test that time provider is used consistently
	fixedTime := time.Date(2025, 1, 1, 12, 0, 0, 0, time.UTC)
	mockTime := &MockTimeProvider{
		CurrentTime: fixedTime,
		TimeAdvance: 100 * time.Millisecond,
	}

	mockCmd := &MockCommandExecutor{}
	mockCmd.SetupDockerSuccess()

	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{}, mockCmd, &MockNetworkChecker{}, mockTime)

	result := verifier.checkLocalDocker(host, mockTime.Now())

	// Check that duration is calculated using mock time
	expectedDuration := 100 * time.Millisecond // One advance
	if result.Duration != expectedDuration {
		t.Errorf("Expected duration %v, got %v", expectedDuration, result.Duration)
	}
}

// Helper functions and types

type mockError struct {
	msg string
}

func (e *mockError) Error() string {
	return e.msg
}

func equalSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHTTPEndpointCheck_Success(t *testing.T) {
	// Create a valid JSON response body
	jsonData := `{"timestamp": "2023-01-01T00:00:00Z", "status": "ok"}`
	responseBody := strings.NewReader(jsonData)

	mockNet := &MockNetworkChecker{
		HTTPResponses: map[string]MockHTTPResponse{
			"http://test.example.com:9999/metrics/json": {
				Response: &http.Response{
					StatusCode: http.StatusOK,
					Body:       io.NopCloser(responseBody),
				},
				Error: nil,
			},
		},
	}

	mockTime := NewMockTimeProvider(time.Now())

	host := &hostparse.HostInfo{Host: "test.example.com", IsLocal: false, Original: "test.example.com"}
	config := Config{APIPort: constants.DefaultSptAPIPort}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, config, &MockCommandExecutor{}, mockNet, mockTime)

	result := verifier.checkMetricsEndpoint(host)

	if !result.Passed {
		t.Errorf("Expected metrics endpoint check to pass, but it failed: %s", result.Message)
	}

	// Verify the correct URL was requested
	if len(mockNet.HTTPRequests) != 1 {
		t.Errorf("Expected 1 HTTP request, got %d", len(mockNet.HTTPRequests))
	} else {
		expectedURL := "http://test.example.com:9999/metrics/json"
		if mockNet.HTTPRequests[0] != expectedURL {
			t.Errorf("Expected request to %s, got %s", expectedURL, mockNet.HTTPRequests[0])
		}
	}
}

func TestHTTPEndpointCheck_Failure(t *testing.T) {
	mockNet := &MockNetworkChecker{
		HTTPResponses: map[string]MockHTTPResponse{
			"http://test.example.com:9999/metrics/json": {
				Response: nil,
				Error:    fmt.Errorf("connection refused"),
			},
		},
	}

	mockTime := NewMockTimeProvider(time.Now())

	host := &hostparse.HostInfo{Host: "test.example.com", IsLocal: false, Original: "test.example.com"}
	config := Config{APIPort: constants.DefaultSptAPIPort}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, config, &MockCommandExecutor{}, mockNet, mockTime)

	result := verifier.checkMetricsEndpoint(host)

	if result.Passed {
		t.Error("Expected metrics endpoint check to fail due to connection error")
	}

	if result.Message != "Failed to reach /metrics/json endpoint" {
		t.Errorf("Unexpected error message: %s", result.Message)
	}
}

func TestContainerOperations_Success(t *testing.T) {
	// Create a flexible mock command executor for container operations
	mockCmd := &FlexibleMockCommandExecutor{}
	mockTime := NewMockTimeProvider(time.Now())

	config := Config{
		NetworkMode:  "bridge",
		RMIPortStart: 40000,
		RMIPortCount: 10,
	}

	host := &hostparse.HostInfo{Host: "test.example.com", IsLocal: false, Original: "test.example.com"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, config, mockCmd, &MockNetworkChecker{}, mockTime)

	// Test container start
	result := &NodeResult{}
	startResult := verifier.startNodeContainer(host, result)

	if !startResult.Passed {
		t.Errorf("Expected container start to succeed, but it failed: %s", startResult.Message)
	}

	if result.containerID != "container123456789" {
		t.Errorf("Expected containerID to be set to 'container123456789', got '%s'", result.containerID)
	}

	// Test container cleanup
	cleanupResult := verifier.stopContainer(host, result.containerID)

	if !cleanupResult.Passed {
		t.Errorf("Expected container cleanup to succeed, but it failed: %s", cleanupResult.Message)
	}
}

func TestStartNodeContainer_FailureCleansUpDanglingContainer(t *testing.T) {
	mockCmd := &cleanupOnFailureExecutor{}
	mockTime := NewMockTimeProvider(time.Now())

	config := Config{
		NetworkMode:  "bridge",
		RMIPortStart: 40000,
		RMIPortCount: 2,
		APIPort:      constants.DefaultSptAPIPort,
	}

	host := &hostparse.HostInfo{Host: "test.example.com", IsLocal: false, Original: "admin@test.example.com"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, config, mockCmd, &MockNetworkChecker{}, mockTime)

	result := &NodeResult{}
	check := verifier.startNodeContainer(host, result)

	if check.Passed {
		t.Fatal("Expected startNodeContainer to fail due to simulated port conflict")
	}
	if mockCmd.cleanupCalls == 0 {
		t.Fatal("Expected cleanup command to be invoked after container start failure")
	}
	if mockCmd.cleanupTarget == "" || !strings.HasPrefix(mockCmd.cleanupTarget, "spt-verify-") {
		t.Fatalf("Unexpected cleanup target: %q", mockCmd.cleanupTarget)
	}
}

// FlexibleMockCommandExecutor for testing container operations
type FlexibleMockCommandExecutor struct {
	ExecutedCommands []MockCommandExecution
}

func (m *FlexibleMockCommandExecutor) ExecuteCommand(ctx context.Context, host *hostparse.HostInfo, command []string) (stdout, stderr string, err error) {
	// Record the execution
	m.ExecutedCommands = append(m.ExecutedCommands, MockCommandExecution{
		Host:    host,
		Command: command,
		Context: ctx,
	})

	cmdStr := strings.Join(command, " ")

	// Ignore verification cleanup queries
	if strings.Join(command, " ") == "docker ps -q --filter label=spt.verify=true" {
		return "", "", nil
	}

	// Handle docker run commands
	if strings.HasPrefix(cmdStr, "docker run") {
		return "container123456789", "", nil
	}

	// Handle docker rm commands
	if strings.HasPrefix(cmdStr, "docker rm -f") {
		return "container123456789", "", nil
	}

	// Default success response
	return "success", "", nil
}

type cleanupOnFailureExecutor struct {
	ExecutedCommands []MockCommandExecution
	cleanupCalls     int
	cleanupTarget    string
}

func (m *cleanupOnFailureExecutor) ExecuteCommand(ctx context.Context, host *hostparse.HostInfo, command []string) (stdout, stderr string, err error) {
	m.ExecutedCommands = append(m.ExecutedCommands, MockCommandExecution{
		Host:    host,
		Command: command,
		Context: ctx,
	})

	cmdStr := strings.Join(command, " ")

	switch {
	case cmdStr == "docker ps -q --filter label=spt.verify=true":
		return "", "", nil
	case strings.HasPrefix(cmdStr, "docker pull "):
		return "pulled", "", nil
	case strings.HasPrefix(cmdStr, "docker run "):
		return "deadbeef", "port in use", fmt.Errorf("docker: Error response from daemon: address already in use")
	case strings.HasPrefix(cmdStr, "docker rm -f "):
		m.cleanupCalls++
		m.cleanupTarget = command[len(command)-1]
		return "", "", nil
	default:
		return "", "", nil
	}
}

func TestEdgeCases_EmptyHosts(t *testing.T) {
	verifier := NewVerifier([]*hostparse.HostInfo{}, Config{})

	report := verifier.VerifyAll()

	if report == nil {
		t.Fatal("VerifyAll returned nil report")
	}

	if len(report.Results) != 0 {
		t.Errorf("Expected 0 results for empty hosts, got %d", len(report.Results))
	}
}

func TestEdgeCases_SingleHost(t *testing.T) {
	mockCmd := &MockCommandExecutor{}
	mockCmd.SetupDockerSuccess()

	mockNet := &MockNetworkChecker{}
	mockNet.SetupPortsOpen("single.example.com", []int{1099, 9999, 40000, 40009})

	mockTime := NewMockTimeProvider(time.Now())

	host := &hostparse.HostInfo{Host: "single.example.com", User: "root", IsLocal: false, Original: "root@single.example.com"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{MinHosts: 1}, mockCmd, mockNet, mockTime)

	report := verifier.VerifyAll()

	if report == nil {
		t.Fatal("VerifyAll returned nil report")
	}

	if len(report.Results) != 1 {
		t.Errorf("Expected 1 result, got %d", len(report.Results))
	}

	result := report.Results["single.example.com"]
	if result == nil {
		t.Fatal("Missing result for single.example.com")
	}

	if !result.DockerAvailable.Passed {
		t.Error("Expected Docker check to pass for single host")
	}
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}

func TestRdmaDeviceCheck_DevicePresent(t *testing.T) {
	mockCmd := &MockCommandExecutor{
		Commands: map[string]MockCommandResponse{
			"ls /dev/infiniband/": {
				Stdout: "uverbs0\nuverbs1\n",
				Stderr: "",
				Error:  nil,
			},
		},
	}
	mockTime := NewMockTimeProvider(time.Now())

	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{UseRdma: true}, mockCmd, &MockNetworkChecker{}, mockTime)

	result := verifier.checkRdmaDevice(host)

	if !result.Passed {
		t.Errorf("Expected RDMA device check to pass, but it failed: %s", result.Message)
	}
	if !strings.Contains(result.Message, "uverbs") {
		t.Errorf("Expected message to mention uverbs devices, got: %s", result.Message)
	}
}

func TestRdmaDeviceCheck_NoDevice(t *testing.T) {
	mockCmd := &MockCommandExecutor{
		Commands: map[string]MockCommandResponse{
			"ls /dev/infiniband/": {
				Stdout: "",
				Stderr: "ls: cannot access '/dev/infiniband/': No such file or directory",
				Error:  fmt.Errorf("exit status 2"),
			},
		},
	}
	mockTime := NewMockTimeProvider(time.Now())

	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{UseRdma: true}, mockCmd, &MockNetworkChecker{}, mockTime)

	result := verifier.checkRdmaDevice(host)

	if result.Passed {
		t.Error("Expected RDMA device check to fail when directory is missing")
	}
	if !strings.Contains(result.Message, "not found") {
		t.Errorf("Expected 'not found' message, got: %s", result.Message)
	}
}

func TestRdmaDeviceCheck_EmptyDirectory(t *testing.T) {
	mockCmd := &MockCommandExecutor{
		Commands: map[string]MockCommandResponse{
			"ls /dev/infiniband/": {
				Stdout: "",
				Stderr: "",
				Error:  nil,
			},
		},
	}
	mockTime := NewMockTimeProvider(time.Now())

	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{UseRdma: true}, mockCmd, &MockNetworkChecker{}, mockTime)

	result := verifier.checkRdmaDevice(host)

	if result.Passed {
		t.Error("Expected RDMA device check to fail when no uverbs devices found")
	}
	if !strings.Contains(result.Message, "No uverbs") {
		t.Errorf("Expected 'No uverbs' message, got: %s", result.Message)
	}
}

func TestRdmaDriverCheck_LibPresent(t *testing.T) {
	mockCmd := &MockCommandExecutor{
		Commands: map[string]MockCommandResponse{
			"docker exec abc123 ls /usr/lib64/libibverbs.so.1": {
				Stdout: "/usr/lib64/libibverbs.so.1",
				Stderr: "",
				Error:  nil,
			},
		},
	}
	mockTime := NewMockTimeProvider(time.Now())

	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{UseRdma: true}, mockCmd, &MockNetworkChecker{}, mockTime)

	result := verifier.checkRdmaDriver(host, "abc123")

	if !result.Passed {
		t.Errorf("Expected RDMA driver check to pass, but it failed: %s", result.Message)
	}
	if !strings.Contains(result.Message, "present") {
		t.Errorf("Expected 'present' in message, got: %s", result.Message)
	}
}

func TestRdmaDriverCheck_LibMissing(t *testing.T) {
	mockCmd := &MockCommandExecutor{
		Commands: map[string]MockCommandResponse{
			"docker exec abc123 ls /usr/lib64/libibverbs.so.1": {
				Stdout: "",
				Stderr: "ls: cannot access '/usr/lib64/libibverbs.so.1': No such file or directory",
				Error:  fmt.Errorf("exit status 2"),
			},
		},
	}
	mockTime := NewMockTimeProvider(time.Now())

	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{UseRdma: true}, mockCmd, &MockNetworkChecker{}, mockTime)

	result := verifier.checkRdmaDriver(host, "abc123")

	if result.Passed {
		t.Error("Expected RDMA driver check to fail when library is missing")
	}
	if !strings.Contains(result.Message, "not found") {
		t.Errorf("Expected 'not found' in message, got: %s", result.Message)
	}
}

func TestRdmaDriverCheck_EmptyContainerID(t *testing.T) {
	mockCmd := &MockCommandExecutor{}
	mockTime := NewMockTimeProvider(time.Now())

	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{UseRdma: true}, mockCmd, &MockNetworkChecker{}, mockTime)

	result := verifier.checkRdmaDriver(host, "")

	if result.Passed {
		t.Error("Expected RDMA driver check to fail with empty container ID")
	}
	if !strings.Contains(result.Message, "No container") {
		t.Errorf("Expected 'No container' in message, got: %s", result.Message)
	}
}

func TestStartNodeContainer_RdmaPassthrough(t *testing.T) {
	mockCmd := &FlexibleMockCommandExecutor{}
	mockTime := NewMockTimeProvider(time.Now())

	config := Config{
		NetworkMode:  "host",
		RMIPortStart: 40000,
		RMIPortCount: 2,
		UseRdma:      true,
	}

	host := &hostparse.HostInfo{Host: "test.example.com", IsLocal: false, Original: "test.example.com"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, config, mockCmd, &MockNetworkChecker{}, mockTime)

	result := &NodeResult{}
	check := verifier.startNodeContainer(host, result)

	if !check.Passed {
		t.Fatalf("Expected container start to succeed, got: %s", check.Message)
	}

	// Find the docker run command and verify RDMA flags
	foundRdma := false
	for _, exec := range mockCmd.ExecutedCommands {
		cmdStr := strings.Join(exec.Command, " ")
		if strings.HasPrefix(cmdStr, "docker run") {
			if strings.Contains(cmdStr, "--device") &&
				strings.Contains(cmdStr, constants.RdmaDevicePath) &&
				strings.Contains(cmdStr, "--cap-add") &&
				strings.Contains(cmdStr, constants.RdmaCapIpcLock) &&
				strings.Contains(cmdStr, "--ulimit") &&
				strings.Contains(cmdStr, constants.RdmaUlimitMemlock) {
				foundRdma = true
			}
			break
		}
	}
	if !foundRdma {
		t.Error("Expected RDMA device passthrough flags in docker run command")
		for _, exec := range mockCmd.ExecutedCommands {
			t.Logf("  cmd: %v", exec.Command)
		}
	}
}

func TestStartNodeContainer_NoRdmaByDefault(t *testing.T) {
	mockCmd := &FlexibleMockCommandExecutor{}
	mockTime := NewMockTimeProvider(time.Now())

	config := Config{
		NetworkMode:  "host",
		RMIPortStart: 40000,
		RMIPortCount: 2,
		UseRdma:      false, // explicitly disabled
	}

	host := &hostparse.HostInfo{Host: "test.example.com", IsLocal: false, Original: "test.example.com"}
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, config, mockCmd, &MockNetworkChecker{}, mockTime)

	result := &NodeResult{}
	check := verifier.startNodeContainer(host, result)

	if !check.Passed {
		t.Fatalf("Expected container start to succeed, got: %s", check.Message)
	}

	// Verify NO RDMA flags in docker run command
	for _, exec := range mockCmd.ExecutedCommands {
		cmdStr := strings.Join(exec.Command, " ")
		if strings.HasPrefix(cmdStr, "docker run") {
			if strings.Contains(cmdStr, "--device") || strings.Contains(cmdStr, "--cap-add") {
				t.Errorf("Expected NO RDMA flags when UseRdma=false, got: %v", exec.Command)
			}
			break
		}
	}
}

func TestRdmaChecksSkippedWhenDisabled(t *testing.T) {
	mockCmd := &FlexibleMockCommandExecutor{}
	mockTime := NewMockTimeProvider(time.Now())

	host := &hostparse.HostInfo{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"}
	// UseRdma is false (default)
	verifier := NewVerifierWithDeps([]*hostparse.HostInfo{host}, Config{}, mockCmd, &MockNetworkChecker{}, mockTime)

	result := verifier.verifyNode(host)

	// RDMA checks should not have been populated
	if result.RdmaDevice.Message != "" {
		t.Errorf("Expected empty RDMA device check when RDMA disabled, got: %s", result.RdmaDevice.Message)
	}
	if result.RdmaDriver.Message != "" {
		t.Errorf("Expected empty RDMA driver check when RDMA disabled, got: %s", result.RdmaDriver.Message)
	}
}
