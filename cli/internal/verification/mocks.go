/*
Copyright © 2025 Dell Technologies
*/

package verification

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// MockCommandExecutor implements CommandExecutor for testing
type MockCommandExecutor struct {
	// Commands maps command strings to expected responses
	Commands map[string]MockCommandResponse

	// ExecutedCommands tracks what commands were executed
	ExecutedCommands []MockCommandExecution

	mu sync.Mutex
}

// MockCommandResponse describes an expected stdout/stderr/error triple for a command.
type MockCommandResponse struct {
	Stdout string
	Stderr string
	Error  error
}

// MockCommandExecution records a command that was executed by the mock.
type MockCommandExecution struct {
	Host    *hostparse.HostInfo
	Command []string
	Context context.Context
}

// ExecuteCommand implements CommandExecutor for tests; it records and replays configured responses.
func (m *MockCommandExecutor) ExecuteCommand(ctx context.Context, host *hostparse.HostInfo, command []string) (stdout, stderr string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Record the execution
	m.ExecutedCommands = append(m.ExecutedCommands, MockCommandExecution{
		Host:    host,
		Command: command,
		Context: ctx,
	})

	// Find matching command response
	cmdStr := strings.Join(command, " ")
	if response, exists := m.Commands[cmdStr]; exists {
		return response.Stdout, response.Stderr, response.Error
	}

	// Allow SSH probe command to succeed by default so tests don't need explicit setup.
	if cmdStr == "true" {
		return "", "", nil
	}

	if len(command) >= 2 && command[0] == constants.DockerCommand && command[1] == constants.DockerCmdRun {
		return "abc123def456", "", nil
	}

	// Default response if not found
	return "", "", fmt.Errorf("mock: unexpected command %s", cmdStr)
}

// SetupDockerSuccess configures mock to return successful Docker responses
func (m *MockCommandExecutor) SetupDockerSuccess() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Commands == nil {
		m.Commands = make(map[string]MockCommandResponse)
	}

	m.Commands[fmt.Sprintf("%s %s %s %s", constants.DockerCommand, constants.DockerCmdVersion, constants.DockerFlagFormat, constants.DockerVersionFormat)] = MockCommandResponse{
		Stdout: "28.3.3-ce",
		Stderr: "",
		Error:  nil,
	}

	// Ensure verification cleanup queries succeed with empty output.
	m.Commands[fmt.Sprintf("%s %s %s %s %s", constants.DockerCommand, constants.DockerCmdPS, constants.DockerFlagQuiet, constants.DockerFlagFilter, "label=spt.verify=true")] = MockCommandResponse{
		Stdout: "",
		Stderr: "",
		Error:  nil,
	}

	// Add docker pull command
	m.Commands[fmt.Sprintf("%s %s %s", constants.DockerCommand, constants.DockerCmdPull, constants.DefaultSptImage)] = MockCommandResponse{
		Stdout: "Successfully pulled " + constants.DefaultSptImage,
		Stderr: "",
		Error:  nil,
	}

	// Add docker ps commands for container listing (unfiltered)
	m.Commands[fmt.Sprintf("%s %s %s %s %s", constants.DockerCommand, constants.DockerCmdPS, constants.DockerFlagAll, constants.DockerFlagFormat, constants.DockerConflictFormat)] = MockCommandResponse{
		Stdout: "",
		Stderr: "",
		Error:  nil,
	}

	// Add common netstat commands for port checking
	m.Commands["sh -c netstat -ln | grep ':1099 '"] = MockCommandResponse{
		Stdout: "",
		Stderr: "exit status 1",
		Error:  fmt.Errorf("grep exit 1"), // No ports found (success case)
	}

	m.Commands["sh -c netstat -ln | grep ':9999 '"] = MockCommandResponse{
		Stdout: "",
		Stderr: "exit status 1",
		Error:  fmt.Errorf("grep exit 1"), // No ports found (success case)
	}

	// Add netstat command for port 0 (used in some edge case tests)
	m.Commands["sh -c netstat -ln | grep ':0 '"] = MockCommandResponse{
		Stdout: "",
		Stderr: "exit status 1",
		Error:  fmt.Errorf("grep exit 1"), // No ports found (success case)
	}
}

// SetupContainerSuccess configures mock to return successful container responses
func (m *MockCommandExecutor) SetupContainerSuccess() {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.Commands == nil {
		m.Commands = make(map[string]MockCommandResponse)
	}

	// Mock container start - any docker run command returns a container ID
	for cmdStr := range m.Commands {
		if strings.HasPrefix(cmdStr, constants.DockerCommand+" "+constants.DockerCmdRun) {
			m.Commands[cmdStr] = MockCommandResponse{
				Stdout: "abc123def456",
				Stderr: "",
				Error:  nil,
			}
		}
	}

	// Add a generic docker run response
	m.Commands[fmt.Sprintf("%s %s %s %s spt-verify", constants.DockerCommand, constants.DockerCmdRun, constants.DockerFlagDetached, constants.DockerFlagName)] = MockCommandResponse{
		Stdout: "abc123def456",
		Stderr: "",
		Error:  nil,
	}

	// Mock container cleanup
	m.Commands[fmt.Sprintf("%s %s %s abc123def456", constants.DockerCommand, constants.DockerCmdRM, constants.DockerFlagForce)] = MockCommandResponse{
		Stdout: "abc123def456",
		Stderr: "",
		Error:  nil,
	}
}

// MockNetworkChecker implements NetworkChecker for testing
type MockNetworkChecker struct {
	// OpenPorts maps "host:port" to whether the port should appear open
	OpenPorts map[string]bool

	// HTTPResponses maps URLs to mock HTTP responses
	HTTPResponses map[string]MockHTTPResponse

	// PortChecks tracks what port checks were made
	PortChecks []MockPortCheck

	// HTTPRequests tracks what HTTP requests were made
	HTTPRequests []string

	mu sync.Mutex
}

// MockHTTPResponse holds an HTTP response or error for a mocked request.
type MockHTTPResponse struct {
	Response *http.Response
	Error    error
}

// MockPortCheck records a port probe made by the mock.
type MockPortCheck struct {
	Host    string
	Port    int
	Timeout time.Duration
}

// IsPortOpen records the probe and returns the configured open/closed state.
func (m *MockNetworkChecker) IsPortOpen(host string, port int, timeout time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Record the check
	m.PortChecks = append(m.PortChecks, MockPortCheck{
		Host:    host,
		Port:    port,
		Timeout: timeout,
	})

	// Check if port should be open
	key := fmt.Sprintf("%s:%d", host, port)
	if isOpen, exists := m.OpenPorts[key]; exists {
		return isOpen
	}

	// Default to closed
	return false
}

// HTTPGet records the request and returns the configured response/error.
func (m *MockNetworkChecker) HTTPGet(_ context.Context, url string) (*http.Response, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Record the request
	m.HTTPRequests = append(m.HTTPRequests, url)

	// Find matching response
	if response, exists := m.HTTPResponses[url]; exists {
		return response.Response, response.Error
	}

	// Default response
	return nil, fmt.Errorf("mock: unexpected URL %s", url)
}

// SetupPortsOpen configures mock to return all specified ports as open
func (m *MockNetworkChecker) SetupPortsOpen(host string, ports []int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.OpenPorts == nil {
		m.OpenPorts = make(map[string]bool)
	}

	for _, port := range ports {
		key := fmt.Sprintf("%s:%d", host, port)
		m.OpenPorts[key] = true
	}
}

// SetupPortsClosed configures mock to return all specified ports as closed
func (m *MockNetworkChecker) SetupPortsClosed(host string, ports []int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.OpenPorts == nil {
		m.OpenPorts = make(map[string]bool)
	}

	for _, port := range ports {
		key := fmt.Sprintf("%s:%d", host, port)
		m.OpenPorts[key] = false
	}
}

// MockTimeProvider implements TimeProvider for testing
type MockTimeProvider struct {
	// CurrentTime is the time that Now() should return
	CurrentTime time.Time

	// TimeAdvance is how much time advances with each call to Now()
	TimeAdvance time.Duration

	// CallCount tracks how many times Now() has been called
	CallCount int

	mu sync.Mutex
}

// Now returns the configured current time and advances if TimeAdvance is set.
func (m *MockTimeProvider) Now() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.CallCount++
	currentTime := m.CurrentTime
	m.CurrentTime = m.CurrentTime.Add(m.TimeAdvance)
	return currentTime
}

// Since returns the delta between the current mock time and t.
func (m *MockTimeProvider) Since(t time.Time) time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.CurrentTime.Sub(t)
}

// NewMockTimeProvider creates a MockTimeProvider starting at a specific time
func NewMockTimeProvider(startTime time.Time) *MockTimeProvider {
	return &MockTimeProvider{
		CurrentTime: startTime,
		TimeAdvance: time.Second, // Default 1 second advance per call
		CallCount:   0,
	}
}

// TimedMockCommandExecutor was removed - timing-based tests are brittle
