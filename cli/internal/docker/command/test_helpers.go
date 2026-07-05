/*
Copyright © 2025 Dell Technologies
*/

package command

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// MockCommandExecutor implements CommandExecutor for testing without system calls
type MockCommandExecutor struct {
	// Commands maps command strings to expected responses
	Commands map[string]MockResponse

	// ExecutedCommands tracks what commands were executed
	ExecutedCommands []ExecutedCommand
	CopiedFiles      []CopiedFile
	CopiedFromFiles  []CopiedFile

	// FailureMode simulates specific failures
	FailureMode string

	// DefaultResponse used when command not found in Commands map
	DefaultResponse MockResponse
}

// MockResponse simulates command execution results
type MockResponse struct {
	Stdout string
	Stderr string
	Error  error
	Delay  time.Duration // Simulate slow commands
}

// ExecutedCommand tracks commands that were executed
type ExecutedCommand struct {
	Host    *hostparse.HostInfo
	Command []string
	Context context.Context
}

// CopiedFile tracks file copy operations.
type CopiedFile struct {
	Host       *hostparse.HostInfo
	LocalPath  string
	RemotePath string
	Content    string
}

// NewMockCommandExecutor creates a new MockCommandExecutor
func NewMockCommandExecutor() *MockCommandExecutor {
	return &MockCommandExecutor{
		Commands:         make(map[string]MockResponse),
		ExecutedCommands: make([]ExecutedCommand, 0),
		FailureMode:      "",
		DefaultResponse: MockResponse{
			Stdout: "",
			Stderr: "",
			Error:  fmt.Errorf("mock: unexpected command"),
		},
	}
}

// ExecuteCommand simulates command execution
func (m *MockCommandExecutor) ExecuteCommand(ctx context.Context, host *hostparse.HostInfo, command []string) (stdout, stderr string, err error) {
	// Record the execution
	m.ExecutedCommands = append(m.ExecutedCommands, ExecutedCommand{
		Host:    host,
		Command: append([]string(nil), command...), // Deep copy
		Context: ctx,
	})

	// Handle specific failure modes
	switch m.FailureMode {
	case "context_cancelled":
		return "", "", context.Canceled
	case "docker_not_found":
		if len(command) > 0 && command[0] == constants.DockerCommand {
			return "", "docker: command not found", fmt.Errorf("command not found")
		}
	case "ssh_failure":
		if !host.IsLocal {
			return "", "ssh: connect to host failed", fmt.Errorf("ssh connection failed")
		}
	case "timeout":
		time.Sleep(100 * time.Millisecond) // Simulate timeout
		return "", "", context.DeadlineExceeded
	}

	// Find matching command response
	cmdStr := strings.Join(command, " ")
	if response, exists := m.Commands[cmdStr]; exists {
		// Simulate delay if specified
		if response.Delay > 0 {
			select {
			case <-time.After(response.Delay):
			case <-ctx.Done():
				return "", "", ctx.Err()
			}
		}
		return response.Stdout, response.Stderr, response.Error
	}

	// Return default response
	return m.DefaultResponse.Stdout, m.DefaultResponse.Stderr, m.DefaultResponse.Error
}

// CopyFile simulates copying a local file to a host.
func (m *MockCommandExecutor) CopyFile(_ context.Context, host *hostparse.HostInfo, localPath, remotePath string) error {
	copied := CopiedFile{Host: host, LocalPath: localPath, RemotePath: remotePath}
	if data, err := os.ReadFile(localPath); err == nil {
		copied.Content = string(data)
	}
	m.CopiedFiles = append(m.CopiedFiles, copied)
	if m.FailureMode == "copy_failure" {
		return fmt.Errorf("copy failed")
	}
	return nil
}

// CopyFromHost simulates copying a file from a host to the local filesystem.
func (m *MockCommandExecutor) CopyFromHost(_ context.Context, host *hostparse.HostInfo, remotePath, localPath string) error {
	m.CopiedFromFiles = append(m.CopiedFromFiles, CopiedFile{Host: host, LocalPath: localPath, RemotePath: remotePath})
	if m.FailureMode == "copy_from_failure" {
		return fmt.Errorf("copy from host failed")
	}
	if err := os.MkdirAll(filepath.Dir(localPath), 0o750); err != nil {
		return err
	}
	return os.WriteFile(localPath, []byte("mock remote file\n"), 0o600)
}

// SetCommandResponse sets the response for a specific command
func (m *MockCommandExecutor) SetCommandResponse(command string, response MockResponse) {
	m.Commands[command] = response
}

// SetCommandSuccess sets a successful response for a command
func (m *MockCommandExecutor) SetCommandSuccess(command, stdout string) {
	m.Commands[command] = MockResponse{
		Stdout: stdout,
		Stderr: "",
		Error:  nil,
	}
}

// SetCommandFailure sets a failure response for a command
func (m *MockCommandExecutor) SetCommandFailure(command, stderr string, err error) {
	m.Commands[command] = MockResponse{
		Stdout: "",
		Stderr: stderr,
		Error:  err,
	}
}

// GetExecutedCommands returns all executed commands
func (m *MockCommandExecutor) GetExecutedCommands() []ExecutedCommand {
	return m.ExecutedCommands
}

// GetLastExecutedCommand returns the most recently executed command
func (m *MockCommandExecutor) GetLastExecutedCommand() *ExecutedCommand {
	if len(m.ExecutedCommands) == 0 {
		return nil
	}
	return &m.ExecutedCommands[len(m.ExecutedCommands)-1]
}

// ClearExecutedCommands clears the execution history
func (m *MockCommandExecutor) ClearExecutedCommands() {
	m.ExecutedCommands = make([]ExecutedCommand, 0)
}

// HasExecutedCommand checks if a specific command was executed
func (m *MockCommandExecutor) HasExecutedCommand(cmdStr string) bool {
	for _, executed := range m.ExecutedCommands {
		if strings.Join(executed.Command, " ") == cmdStr {
			return true
		}
	}
	return false
}

// GetExecutedCommandsMatching returns commands matching a pattern
func (m *MockCommandExecutor) GetExecutedCommandsMatching(pattern string) []ExecutedCommand {
	var matching []ExecutedCommand
	for _, executed := range m.ExecutedCommands {
		cmdStr := strings.Join(executed.Command, " ")
		if strings.Contains(cmdStr, pattern) {
			matching = append(matching, executed)
		}
	}
	return matching
}

// Test Helper Functions

// CreateTestHostInfo creates a HostInfo for testing
func CreateTestHostInfo(host string, isLocal bool) *hostparse.HostInfo {
	return &hostparse.HostInfo{
		Host:     host,
		IsLocal:  isLocal,
		User:     "testuser",
		Original: host,
	}
}

// CreateLocalHost creates a local host for testing
func CreateLocalHost() *hostparse.HostInfo {
	return CreateTestHostInfo("localhost", true)
}

// CreateRemoteHost creates a remote host for testing
func CreateRemoteHost(hostname string) *hostparse.HostInfo {
	return CreateTestHostInfo(hostname, false)
}

// CreateTestContainerConfig creates a basic ContainerConfig for testing
func CreateTestContainerConfig() ContainerConfig {
	return ContainerConfig{
		Image:       constants.DefaultSptImage,
		Name:        "test-container",
		NetworkMode: NetworkModeBridge,
		PortMappings: []PortMapping{
			{HostPort: 8080, ContainerPort: 8080, Protocol: "tcp"},
		},
		Environment: map[string]string{
			"TEST_ENV": "value",
		},
		Command:  []string{"--help"},
		Detached: true,
	}
}

// CreateMinimalContainerConfig creates a minimal ContainerConfig for testing
func CreateMinimalContainerConfig() ContainerConfig {
	return ContainerConfig{
		Image: constants.DefaultSptImage,
	}
}

// SetupDockerMockResponses configures common Docker command responses
func SetupDockerMockResponses(mock *MockCommandExecutor) {
	// Docker run command returns container ID
	mock.SetCommandSuccess("docker run -d --name test-container -e TEST_ENV=value -p 8080:8080/tcp "+constants.DefaultSptImage+" --help", "abc123def456")

	// Docker ps command returns container info
	dockerPsCmd := fmt.Sprintf("%s %s %s %s %s", constants.DockerCommand, constants.DockerCmdPS, constants.DockerFlagQuiet, constants.DockerFlagFilter, "id=abc123def456")
	mock.SetCommandSuccess(dockerPsCmd, "abc123def456")

	// Docker rm command returns container ID
	dockerRmCmd := fmt.Sprintf("%s %s %s abc123def456", constants.DockerCommand, constants.DockerCmdRM, constants.DockerFlagForce)
	mock.SetCommandSuccess(dockerRmCmd, "abc123def456")

	// Docker logs command returns sample logs
	dockerLogsCmd := fmt.Sprintf("%s %s abc123def456", constants.DockerCommand, constants.DockerCmdLogs)
	mock.SetCommandSuccess(dockerLogsCmd, "Sample container log output\nLine 2 of logs")

	// Docker images command to check availability
	dockerImagesCmd := fmt.Sprintf("%s %s %s %s", constants.DockerCommand, constants.DockerCmdImages, constants.DockerFlagQuiet, constants.DefaultSptImage)
	mock.SetCommandSuccess(dockerImagesCmd, "sha256:abc123")

	// Docker pull command
	dockerPullCmd := fmt.Sprintf("%s %s %s", constants.DockerCommand, constants.DockerCmdPull, constants.DefaultSptImage)
	mock.SetCommandSuccess(dockerPullCmd, "Successfully pulled "+constants.DefaultSptImage)

	// Docker ps for listing containers
	dockerListCmd := fmt.Sprintf("%s %s %s %s %s", constants.DockerCommand, constants.DockerCmdPS, constants.DockerFlagAll, constants.DockerFlagFormat, constants.DockerContainerFormat)
	mock.SetCommandSuccess(dockerListCmd, "abc123def456|test-container|"+constants.DefaultSptImage+"|running|8080/tcp")
}

// SetupNetstatMockResponses configures common netstat command responses
func SetupNetstatMockResponses(mock *MockCommandExecutor) {
	// Port 8080 available (grep returns exit status 1)
	netstatCmd8080 := fmt.Sprintf("sh -c netstat -ln | grep ':%d '", 8080)
	mock.SetCommandFailure(netstatCmd8080, "exit status 1", fmt.Errorf("grep exit 1"))

	// Port 9999 in use (grep finds match)
	netstatCmd9999 := fmt.Sprintf("sh -c netstat -ln | grep ':%d '", 9999)
	mock.SetCommandSuccess(netstatCmd9999, "tcp        0      0 0.0.0.0:9999            0.0.0.0:*               LISTEN")

	// Port 1099 available
	netstatCmd1099 := fmt.Sprintf("sh -c netstat -ln | grep ':%d '", 1099)
	mock.SetCommandFailure(netstatCmd1099, "exit status 1", fmt.Errorf("grep exit 1"))
}

// AssertCommandExecuted checks that a specific command was executed
func AssertCommandExecuted(mock *MockCommandExecutor, expectedCmd string) error {
	if !mock.HasExecutedCommand(expectedCmd) {
		return fmt.Errorf("expected command not executed: %s", expectedCmd)
	}
	return nil
}

// AssertDockerCommand validates that a Docker command has correct structure
func AssertDockerCommand(command []string) error {
	if len(command) < 2 {
		return fmt.Errorf("docker command too short: %v", command)
	}
	if command[0] != constants.DockerCommand {
		return fmt.Errorf("expected Docker command to start with %s, got %s", constants.DockerCommand, command[0])
	}
	return nil
}

// AssertSSHCommand validates that an SSH command has correct structure
func AssertSSHCommand(command []string, expectedTarget string) error {
	if len(command) < 4 {
		return fmt.Errorf("SSH command too short: %v", command)
	}
	if command[0] != constants.SSHCommand {
		return fmt.Errorf("expected SSH command to start with %s, got %s", constants.SSHCommand, command[0])
	}

	// Find SSH target in command
	targetFound := false
	for _, arg := range command {
		if arg == expectedTarget {
			targetFound = true
			break
		}
	}
	if !targetFound {
		return fmt.Errorf("SSH target %s not found in command: %v", expectedTarget, command)
	}

	return nil
}

// CreateContainerInfoSample creates sample ContainerInfo for testing
func CreateContainerInfoSample() ContainerInfo {
	return ContainerInfo{
		ID:    "abc123def456",
		Name:  "test-container",
		Image: constants.DefaultSptImage,
		State: "running",
		Ports: []string{"0.0.0.0:8080->8080/tcp"},
	}
}
