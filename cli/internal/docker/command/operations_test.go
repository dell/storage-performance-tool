/*
Copyright © 2025 Dell Technologies
*/

package command

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func TestDockerOperationsImpl_StartContainer(t *testing.T) {
	t.Setenv(constants.EnvSkipImagePull, "true")
	tests := []struct {
		name            string
		config          ContainerConfig
		mockResponses   map[string]MockResponse
		wantContainerID string
		wantErr         bool
		wantStdout      string
		wantStderr      string
	}{
		{
			name:   "successful container start",
			config: CreateTestContainerConfig(),
			mockResponses: map[string]MockResponse{
				// Image exists locally
				"docker images -q " + constants.DefaultSptImage: {
					Stdout: "sha256:abc123",
				},
				"docker run -d --name test-container -e TEST_ENV=value -p 8080:8080/tcp " + constants.DefaultSptImage + " --help": {
					Stdout: "abc123def456",
					Stderr: "",
					Error:  nil,
				},
			},
			wantContainerID: "abc123def456",
			wantErr:         false,
			wantStdout:      "abc123def456",
			wantStderr:      "",
		},
		{
			name:   "container start failure",
			config: CreateTestContainerConfig(),
			mockResponses: map[string]MockResponse{
				// Image exists locally
				"docker images -q " + constants.DefaultSptImage: {
					Stdout: "sha256:abc123",
				},
				"docker run -d --name test-container -e TEST_ENV=value -p 8080:8080/tcp " + constants.DefaultSptImage + " --help": {
					Stdout: "",
					Stderr: "docker: Error response from daemon: port already in use",
					Error:  fmt.Errorf("exit status 1"),
				},
			},
			wantContainerID: "",
			wantErr:         true,
			wantStdout:      "",
			wantStderr:      "docker: Error response from daemon: port already in use",
		},
		{
			name:   "empty container ID returned",
			config: CreateTestContainerConfig(),
			mockResponses: map[string]MockResponse{
				// Image exists locally
				"docker images -q " + constants.DefaultSptImage: {
					Stdout: "sha256:abc123",
				},
				"docker run -d --name test-container -e TEST_ENV=value -p 8080:8080/tcp " + constants.DefaultSptImage + " --help": {
					Stdout: "",
					Stderr: "",
					Error:  nil,
				},
			},
			wantContainerID: "",
			wantErr:         true,
			wantStdout:      "",
			wantStderr:      "",
		},
		{
			name:   "minimal container configuration",
			config: CreateMinimalContainerConfig(),
			mockResponses: map[string]MockResponse{
				// Image exists locally
				"docker images -q " + constants.DefaultSptImage: {
					Stdout: "sha256:abc123",
				},
				"docker run " + constants.DefaultSptImage: {
					Stdout: "minimal123",
					Stderr: "",
					Error:  nil,
				},
			},
			wantContainerID: "minimal123",
			wantErr:         false,
			wantStdout:      "minimal123",
			wantStderr:      "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockExecutor := NewMockCommandExecutor()
			for cmd, response := range tt.mockResponses {
				mockExecutor.SetCommandResponse(cmd, response)
			}

			host := CreateLocalHost()
			ops := NewDockerOperations(mockExecutor, host)
			ctx := context.Background()

			containerID, result, err := ops.StartContainer(ctx, tt.config)

			// Check error expectation
			if (err != nil) != tt.wantErr {
				t.Errorf("StartContainer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Check container ID
			if containerID != tt.wantContainerID {
				t.Errorf("StartContainer() containerID = %q, want %q", containerID, tt.wantContainerID)
			}

			// Check command result
			if result.Stdout != tt.wantStdout {
				t.Errorf("StartContainer() result.Stdout = %q, want %q", result.Stdout, tt.wantStdout)
			}
			if result.Stderr != tt.wantStderr {
				t.Errorf("StartContainer() result.Stderr = %q, want %q", result.Stderr, tt.wantStderr)
			}

			// Verify command was executed
			if len(mockExecutor.GetExecutedCommandsMatching("docker run ")) == 0 {
				t.Errorf("Expected a docker run command to be executed")
			}

			if len(mockExecutor.GetExecutedCommandsMatching("docker pull ")) > 0 {
				t.Errorf("Did not expect docker pull to be executed when skip-image-pull is enabled")
			}
		})
	}
}

func TestDockerOperationsImpl_StartContainer_AutoPullWhenMissing(t *testing.T) {
	t.Setenv(constants.EnvSkipImagePull, "false")
	mockExecutor := NewMockCommandExecutor()
	// Pull succeeds
	mockExecutor.SetCommandSuccess("docker pull "+constants.DefaultSptImage, "Successfully pulled "+constants.DefaultSptImage)
	// Run succeeds
	runCmd := "docker run -d --name test-container -e TEST_ENV=value -p 8080:8080/tcp " + constants.DefaultSptImage + " --help"
	mockExecutor.SetCommandSuccess(runCmd, "abc123def456")

	host := CreateLocalHost()
	ops := NewDockerOperations(mockExecutor, host)
	ctx := context.Background()

	containerID, _, err := ops.StartContainer(ctx, CreateTestContainerConfig())
	if err != nil {
		t.Fatalf("StartContainer() unexpected error: %v", err)
	}
	if containerID != "abc123def456" {
		t.Fatalf("StartContainer() containerID = %s, want abc123def456", containerID)
	}

	// Verify pull and run were executed
	if !mockExecutor.HasExecutedCommand("docker pull " + constants.DefaultSptImage) {
		t.Errorf("Expected docker pull to be executed")
	}
	if len(mockExecutor.GetExecutedCommandsMatching("docker run ")) == 0 {
		t.Errorf("Expected docker run to be executed")
	}
}

func TestDockerOperationsImpl_StopContainer(t *testing.T) {
	tests := []struct {
		name          string
		containerID   string
		mockResponses map[string]MockResponse
		wantErr       bool
	}{
		{
			name:        "successful container stop",
			containerID: "abc123def456",
			mockResponses: map[string]MockResponse{
				"docker rm -f abc123def456": {
					Stdout: "abc123def456",
					Stderr: "",
					Error:  nil,
				},
			},
			wantErr: false,
		},
		{
			name:        "container stop failure",
			containerID: "abc123def456",
			mockResponses: map[string]MockResponse{
				"docker rm -f abc123def456": {
					Stdout: "",
					Stderr: "Error: No such container: abc123def456",
					Error:  fmt.Errorf("exit status 1"),
				},
			},
			wantErr: true,
		},
		{
			name:          "empty container ID",
			containerID:   "",
			mockResponses: map[string]MockResponse{},
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockExecutor := NewMockCommandExecutor()
			for cmd, response := range tt.mockResponses {
				mockExecutor.SetCommandResponse(cmd, response)
			}

			host := CreateLocalHost()
			ops := NewDockerOperations(mockExecutor, host)
			ctx := context.Background()

			err := ops.StopContainer(ctx, tt.containerID)

			if (err != nil) != tt.wantErr {
				t.Errorf("StopContainer() error = %v, wantErr %v", err, tt.wantErr)
			}

			// For empty container ID, no command should be executed
			if tt.containerID == "" {
				executedCmds := mockExecutor.GetExecutedCommands()
				if len(executedCmds) != 0 {
					t.Errorf("Expected 0 executed commands for empty container ID, got %d", len(executedCmds))
				}
			}
		})
	}
}

func TestDockerOperationsImpl_IsContainerRunning(t *testing.T) {
	tests := []struct {
		name          string
		containerID   string
		mockResponses map[string]MockResponse
		wantRunning   bool
		wantErr       bool
	}{
		{
			name:        "container is running",
			containerID: "abc123def456",
			mockResponses: map[string]MockResponse{
				"docker ps -q --filter id=abc123def456": {
					Stdout: "abc123def456",
					Stderr: "",
					Error:  nil,
				},
			},
			wantRunning: true,
			wantErr:     false,
		},
		{
			name:        "container is not running",
			containerID: "abc123def456",
			mockResponses: map[string]MockResponse{
				"docker ps -q --filter id=abc123def456": {
					Stdout: "",
					Stderr: "",
					Error:  nil,
				},
			},
			wantRunning: false,
			wantErr:     false,
		},
		{
			name:        "docker ps command fails",
			containerID: "abc123def456",
			mockResponses: map[string]MockResponse{
				"docker ps -q --filter id=abc123def456": {
					Stdout: "",
					Stderr: "Cannot connect to the Docker daemon",
					Error:  fmt.Errorf("exit status 1"),
				},
			},
			wantRunning: false,
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockExecutor := NewMockCommandExecutor()
			for cmd, response := range tt.mockResponses {
				mockExecutor.SetCommandResponse(cmd, response)
			}

			host := CreateLocalHost()
			ops := NewDockerOperations(mockExecutor, host)
			ctx := context.Background()

			running, err := ops.IsContainerRunning(ctx, tt.containerID)

			if (err != nil) != tt.wantErr {
				t.Errorf("IsContainerRunning() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if running != tt.wantRunning {
				t.Errorf("IsContainerRunning() running = %v, want %v", running, tt.wantRunning)
			}
		})
	}
}

func TestDockerOperationsImpl_GetContainerLogs(t *testing.T) {
	tests := []struct {
		name          string
		containerID   string
		follow        bool
		mockResponses map[string]MockResponse
		wantStdout    []string
		wantStderr    []string
		wantErr       bool
	}{
		{
			name:        "get logs without follow",
			containerID: "abc123def456",
			follow:      false,
			mockResponses: map[string]MockResponse{
				"docker logs abc123def456": {
					Stdout: "Log line 1\nLog line 2\nLog line 3",
					Stderr: "",
					Error:  nil,
				},
			},
			wantStdout: []string{"Log line 1", "Log line 2", "Log line 3"},
			wantStderr: []string{},
			wantErr:    false,
		},
		{
			name:        "get logs with follow",
			containerID: "abc123def456",
			follow:      true,
			mockResponses: map[string]MockResponse{
				"docker logs -f abc123def456": {
					Stdout: "Streaming log line",
					Stderr: "",
					Error:  nil,
				},
			},
			wantStdout: []string{"Streaming log line"},
			wantStderr: []string{},
			wantErr:    false,
		},
		{
			name:        "get logs with stderr",
			containerID: "abc123def456",
			follow:      false,
			mockResponses: map[string]MockResponse{
				"docker logs abc123def456": {
					Stdout: "Normal output",
					Stderr: "Error output\nWarning output",
					Error:  nil,
				},
			},
			wantStdout: []string{"Normal output"},
			wantStderr: []string{"Error output", "Warning output"},
			wantErr:    false,
		},
		{
			name:        "docker logs command fails",
			containerID: "abc123def456",
			follow:      false,
			mockResponses: map[string]MockResponse{
				"docker logs abc123def456": {
					Stdout: "",
					Stderr: "No such container: abc123def456",
					Error:  fmt.Errorf("exit status 1"),
				},
			},
			wantStdout: []string{},
			wantStderr: []string{},
			wantErr:    true,
		},
		{
			name:        "empty logs",
			containerID: "abc123def456",
			follow:      false,
			mockResponses: map[string]MockResponse{
				"docker logs abc123def456": {
					Stdout: "",
					Stderr: "",
					Error:  nil,
				},
			},
			wantStdout: []string{},
			wantStderr: []string{},
			wantErr:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockExecutor := NewMockCommandExecutor()
			for cmd, response := range tt.mockResponses {
				mockExecutor.SetCommandResponse(cmd, response)
			}

			host := CreateLocalHost()
			ops := NewDockerOperations(mockExecutor, host)
			ctx := context.Background()

			var capturedStdout []string
			var capturedStderr []string

			stdoutCallback := func(line string) {
				capturedStdout = append(capturedStdout, line)
			}
			stderrCallback := func(line string) {
				capturedStderr = append(capturedStderr, line)
			}

			err := ops.GetContainerLogs(ctx, tt.containerID, tt.follow, stdoutCallback, stderrCallback)

			if (err != nil) != tt.wantErr {
				t.Errorf("GetContainerLogs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !compareStringSlices(capturedStdout, tt.wantStdout) {
				t.Errorf("GetContainerLogs() stdout = %v, want %v", capturedStdout, tt.wantStdout)
			}

			if !compareStringSlices(capturedStderr, tt.wantStderr) {
				t.Errorf("GetContainerLogs() stderr = %v, want %v", capturedStderr, tt.wantStderr)
			}
		})
	}
}

func TestDockerOperationsImpl_GetContainerLogsSince_WithTimestamps(t *testing.T) {
	mockExecutor := NewMockCommandExecutor()
	host := CreateRemoteHost("remote-host")
	ops := NewDockerOperations(mockExecutor, host)

	since := time.Date(2025, 9, 14, 12, 0, 0, 123000000, time.UTC)
	cmd := fmt.Sprintf("%s %s %s %s %s abc123def456", constants.DockerCommand, constants.DockerCmdLogs, constants.DockerFlagTimestamps, constants.DockerFlagSince, since.Format(time.RFC3339Nano))
	mockExecutor.SetCommandSuccess(cmd, "2025-09-14T12:00:01.000000000Z first\n2025-09-14T12:00:02.100000000Z second")

	got := []string{}
	err := ops.GetContainerLogsSince(context.Background(), "abc123def456", since, true,
		func(line string) { got = append(got, line) },
		func(line string) { got = append(got, line) },
	)
	if err != nil {
		t.Fatalf("GetContainerLogsSince returned error: %v", err)
	}
	if len(got) != 2 || !strings.Contains(got[0], "first") || !strings.Contains(got[1], "second") {
		t.Fatalf("unexpected lines: %#v", got)
	}
}

func TestDockerOperationsImpl_ListContainers(t *testing.T) {
	tests := []struct {
		name           string
		filters        map[string]string
		mockResponses  map[string]MockResponse
		wantContainers []ContainerInfo
		wantErr        bool
	}{
		{
			name:    "list all containers",
			filters: map[string]string{},
			mockResponses: map[string]MockResponse{
				"docker ps -a --format " + constants.DockerContainerFormat: {
					Stdout: "abc123def456|test-container|" + constants.DefaultSptImage + "|running|8080/tcp",
					Stderr: "",
					Error:  nil,
				},
			},
			wantContainers: []ContainerInfo{
				{
					ID:    "abc123def456",
					Name:  "test-container",
					Image: constants.DefaultSptImage,
					State: "running",
					Ports: []string{"8080/tcp"},
				},
			},
			wantErr: false,
		},
		{
			name: "list containers with filter",
			filters: map[string]string{
				"name": "test",
			},
			mockResponses: map[string]MockResponse{
				"docker ps -a --format " + constants.DockerContainerFormat + " --filter name=test": {
					Stdout: "def456abc123|test-filtered|nginx:latest|exited|",
					Stderr: "",
					Error:  nil,
				},
			},
			wantContainers: []ContainerInfo{
				{
					ID:    "def456abc123",
					Name:  "test-filtered",
					Image: "nginx:latest",
					State: "exited",
					Ports: []string{},
				},
			},
			wantErr: false,
		},
		{
			name:    "multiple containers",
			filters: map[string]string{},
			mockResponses: map[string]MockResponse{
				"docker ps -a --format " + constants.DockerContainerFormat: {
					Stdout: "abc123|container1|image1|running|8080/tcp\ndef456|container2|image2|exited|9090/tcp,9091/tcp",
					Stderr: "",
					Error:  nil,
				},
			},
			wantContainers: []ContainerInfo{
				{
					ID:    "abc123",
					Name:  "container1",
					Image: "image1",
					State: "running",
					Ports: []string{"8080/tcp"},
				},
				{
					ID:    "def456",
					Name:  "container2",
					Image: "image2",
					State: "exited",
					Ports: []string{"9090/tcp,9091/tcp"},
				},
			},
			wantErr: false,
		},
		{
			name:    "no containers",
			filters: map[string]string{},
			mockResponses: map[string]MockResponse{
				"docker ps -a --format " + constants.DockerContainerFormat: {
					Stdout: "",
					Stderr: "",
					Error:  nil,
				},
			},
			wantContainers: []ContainerInfo{},
			wantErr:        false,
		},
		{
			name:    "docker ps command fails",
			filters: map[string]string{},
			mockResponses: map[string]MockResponse{
				"docker ps -a --format " + constants.DockerContainerFormat: {
					Stdout: "",
					Stderr: "Cannot connect to the Docker daemon",
					Error:  fmt.Errorf("exit status 1"),
				},
			},
			wantContainers: nil,
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockExecutor := NewMockCommandExecutor()
			for cmd, response := range tt.mockResponses {
				mockExecutor.SetCommandResponse(cmd, response)
			}

			host := CreateLocalHost()
			ops := NewDockerOperations(mockExecutor, host)
			ctx := context.Background()

			containers, err := ops.ListContainers(ctx, tt.filters)

			if (err != nil) != tt.wantErr {
				t.Errorf("ListContainers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(containers) != len(tt.wantContainers) {
				t.Errorf("ListContainers() returned %d containers, want %d", len(containers), len(tt.wantContainers))
				return
			}

			for i, got := range containers {
				want := tt.wantContainers[i]
				if got.ID != want.ID || got.Name != want.Name || got.Image != want.Image || got.State != want.State {
					t.Errorf("ListContainers() container[%d] = {ID:%s, Name:%s, Image:%s, State:%s}, want {ID:%s, Name:%s, Image:%s, State:%s}",
						i, got.ID, got.Name, got.Image, got.State,
						want.ID, want.Name, want.Image, want.State)
				}
				if !compareStringSlices(got.Ports, want.Ports) {
					t.Errorf("ListContainers() container[%d] ports = %v, want %v", i, got.Ports, want.Ports)
				}
			}
		})
	}
}

func TestDockerOperationsImpl_ImageOperations(t *testing.T) {
	t.Run("PullImage success", func(t *testing.T) {
		mockExecutor := NewMockCommandExecutor()
		mockExecutor.SetCommandSuccess("docker pull "+constants.DefaultSptImage, "Successfully pulled "+constants.DefaultSptImage)

		host := CreateLocalHost()
		ops := NewDockerOperations(mockExecutor, host)
		ctx := context.Background()

		result, err := ops.PullImage(ctx, constants.DefaultSptImage)

		if err != nil {
			t.Errorf("PullImage() error = %v", err)
		}
		if !strings.Contains(result.Stdout, "Successfully pulled") {
			t.Errorf("PullImage() stdout doesn't contain expected success message: %s", result.Stdout)
		}
	})

	t.Run("PullImage failure", func(t *testing.T) {
		mockExecutor := NewMockCommandExecutor()
		mockExecutor.SetCommandFailure("docker pull nonexistent:latest", "Error response from daemon: pull access denied", fmt.Errorf("exit status 1"))

		host := CreateLocalHost()
		ops := NewDockerOperations(mockExecutor, host)
		ctx := context.Background()

		_, err := ops.PullImage(ctx, "nonexistent:latest")

		if err == nil {
			t.Error("PullImage() expected error for nonexistent image")
		}
	})

	t.Run("IsImageAvailable available", func(t *testing.T) {
		mockExecutor := NewMockCommandExecutor()
		mockExecutor.SetCommandSuccess("docker images -q "+constants.DefaultSptImage, "sha256:abc123")

		host := CreateLocalHost()
		ops := NewDockerOperations(mockExecutor, host)
		ctx := context.Background()

		available, err := ops.IsImageAvailable(ctx, constants.DefaultSptImage)

		if err != nil {
			t.Errorf("IsImageAvailable() error = %v", err)
		}
		if !available {
			t.Error("IsImageAvailable() should return true for available image")
		}
	})

	t.Run("IsImageAvailable not available", func(t *testing.T) {
		mockExecutor := NewMockCommandExecutor()
		mockExecutor.SetCommandSuccess("docker images -q nonexistent:latest", "")

		host := CreateLocalHost()
		ops := NewDockerOperations(mockExecutor, host)
		ctx := context.Background()

		available, err := ops.IsImageAvailable(ctx, "nonexistent:latest")

		if err != nil {
			t.Errorf("IsImageAvailable() error = %v", err)
		}
		if available {
			t.Error("IsImageAvailable() should return false for nonexistent image")
		}
	})
}

func TestDockerOperationsImpl_SpecializedContainerMethods(t *testing.T) {
	t.Setenv(constants.EnvSkipImagePull, "true")
	// Test the implementation-specific methods by casting to the concrete type
	t.Run("StartWorkerNodeContainer", func(t *testing.T) {
		mockExecutor := NewMockCommandExecutor()
		// Image exists locally
		mockExecutor.SetCommandSuccess("docker images -q "+constants.DefaultSptImage, "sha256:abc123")
		expectedCmd := "docker run -d --name worker-1 -e JAVA_OPTS=-Djava.rmi.server.hostname=localhost -e JAVA_TOOL_OPTIONS=-Djava.rmi.server.hostname=localhost --network host " +
			constants.DefaultSptImage + " --run-node=true --run-port=9999 --load-step-node-port=1099"
		mockExecutor.SetCommandSuccess(expectedCmd, "worker123")

		host := CreateLocalHost()
		ops := NewDockerOperations(mockExecutor, host).(*DockerOperationsImpl)
		ctx := context.Background()

		containerID, err := ops.StartWorkerNodeContainer(ctx, constants.DefaultSptImage, "worker-1", "localhost", 40000, 3)

		if err != nil {
			t.Errorf("StartWorkerNodeContainer() error = %v", err)
		}
		if containerID != "worker123" {
			t.Errorf("StartWorkerNodeContainer() containerID = %s, want worker123", containerID)
		}
	})

	t.Run("StartEntryNodeContainer bridge mode", func(t *testing.T) {
		mockExecutor := NewMockCommandExecutor()
		// Image exists locally
		mockExecutor.SetCommandSuccess("docker images -q "+constants.DefaultSptImage, "sha256:abc123")
		expectedCmd := "docker run -d --name entry-1 -p 9999:9999/tcp " +
			constants.DefaultSptImage + " --load-step-node-addrs=worker1:9999,worker2:9999 --run-node=true"
		mockExecutor.SetCommandSuccess(expectedCmd, "entry123")

		host := CreateLocalHost()
		ops := NewDockerOperations(mockExecutor, host).(*DockerOperationsImpl)
		ctx := context.Background()

		containerID, err := ops.StartEntryNodeContainer(ctx, constants.DefaultSptImage, "entry-1", []string{"worker1:9999", "worker2:9999"}, NetworkModeBridge)

		if err != nil {
			t.Errorf("StartEntryNodeContainer() error = %v", err)
		}
		if containerID != "entry123" {
			t.Errorf("StartEntryNodeContainer() containerID = %s, want entry123", containerID)
		}
	})

	t.Run("StartEntryNodeContainer host mode", func(t *testing.T) {
		mockExecutor := NewMockCommandExecutor()
		expectedCmd := "docker run -d --name entry-host --network host " +
			constants.DefaultSptImage + " --load-step-node-addrs=worker1:9999 --run-node=true"
		mockExecutor.SetCommandSuccess(expectedCmd, "entryhost123")

		host := CreateLocalHost()
		ops := NewDockerOperations(mockExecutor, host).(*DockerOperationsImpl)
		ctx := context.Background()

		containerID, err := ops.StartEntryNodeContainer(ctx, constants.DefaultSptImage, "entry-host", []string{"worker1:9999"}, NetworkModeHost)

		if err != nil {
			t.Errorf("StartEntryNodeContainer() error = %v", err)
		}
		if containerID != "entryhost123" {
			t.Errorf("StartEntryNodeContainer() containerID = %s, want entryhost123", containerID)
		}
	})
}

func TestDockerOperationsImpl_WaitForContainerReady(t *testing.T) {
	t.Run("container becomes ready immediately", func(t *testing.T) {
		mockExecutor := NewMockCommandExecutor()
		mockExecutor.SetCommandSuccess("docker ps -q --filter id=abc123", "abc123")

		host := CreateLocalHost()
		ops := NewDockerOperations(mockExecutor, host).(*DockerOperationsImpl)
		ctx := context.Background()

		err := ops.WaitForContainerReady(ctx, "abc123", 5*time.Second)

		if err != nil {
			t.Errorf("WaitForContainerReady() error = %v", err)
		}
	})

	t.Run("container never becomes ready - timeout", func(t *testing.T) {
		mockExecutor := NewMockCommandExecutor()
		mockExecutor.SetCommandSuccess("docker ps -q --filter id=abc123", "") // Always empty (not running)

		host := CreateLocalHost()
		ops := NewDockerOperations(mockExecutor, host).(*DockerOperationsImpl)
		ctx := context.Background()

		start := time.Now()
		err := ops.WaitForContainerReady(ctx, "abc123", 100*time.Millisecond)
		duration := time.Since(start)

		if err == nil {
			t.Error("WaitForContainerReady() expected timeout error")
		}
		if !strings.Contains(err.Error(), "not ready after") {
			t.Errorf("WaitForContainerReady() error message doesn't indicate timeout: %v", err)
		}
		if duration < 100*time.Millisecond {
			t.Errorf("WaitForContainerReady() returned too quickly: %v", duration)
		}
	})

	t.Run("context cancellation during wait", func(t *testing.T) {
		mockExecutor := NewMockCommandExecutor()
		mockExecutor.SetCommandSuccess("docker ps -q --filter id=abc123", "") // Always empty

		host := CreateLocalHost()
		ops := NewDockerOperations(mockExecutor, host).(*DockerOperationsImpl)
		ctx, cancel := context.WithCancel(context.Background())

		// Cancel after short delay
		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		err := ops.WaitForContainerReady(ctx, "abc123", 5*time.Second)

		if err != context.Canceled {
			t.Errorf("WaitForContainerReady() error = %v, want context.Canceled", err)
		}
	})
}

func TestDockerOperationsImpl_GetContainerIP(t *testing.T) {
	tests := []struct {
		name          string
		containerID   string
		mockResponses map[string]MockResponse
		wantIP        string
		wantErr       bool
	}{
		{
			name:        "get IP successfully",
			containerID: "abc123",
			mockResponses: map[string]MockResponse{
				"docker inspect --format " + constants.DockerIPAddressFormat + " abc123": {
					Stdout: "172.17.0.2",
					Stderr: "",
					Error:  nil,
				},
			},
			wantIP:  "172.17.0.2",
			wantErr: false,
		},
		{
			name:        "container has no IP",
			containerID: "abc123",
			mockResponses: map[string]MockResponse{
				"docker inspect --format " + constants.DockerIPAddressFormat + " abc123": {
					Stdout: "",
					Stderr: "",
					Error:  nil,
				},
			},
			wantIP:  "",
			wantErr: true,
		},
		{
			name:        "docker inspect fails",
			containerID: "nonexistent",
			mockResponses: map[string]MockResponse{
				"docker inspect --format " + constants.DockerIPAddressFormat + " nonexistent": {
					Stdout: "",
					Stderr: "Error: No such container: nonexistent",
					Error:  fmt.Errorf("exit status 1"),
				},
			},
			wantIP:  "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockExecutor := NewMockCommandExecutor()
			for cmd, response := range tt.mockResponses {
				mockExecutor.SetCommandResponse(cmd, response)
			}

			host := CreateLocalHost()
			ops := NewDockerOperations(mockExecutor, host).(*DockerOperationsImpl)
			ctx := context.Background()

			ip, err := ops.GetContainerIP(ctx, tt.containerID)

			if (err != nil) != tt.wantErr {
				t.Errorf("GetContainerIP() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if ip != tt.wantIP {
				t.Errorf("GetContainerIP() ip = %q, want %q", ip, tt.wantIP)
			}
		})
	}
}

func TestDockerOperationsImpl_ContextHandling(t *testing.T) {
	t.Run("context cancellation during StartContainer", func(t *testing.T) {
		mockExecutor := NewMockCommandExecutor()
		mockExecutor.FailureMode = "context_cancelled"

		host := CreateLocalHost()
		ops := NewDockerOperations(mockExecutor, host)
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, _, err := ops.StartContainer(ctx, CreateTestContainerConfig())

		if err == nil || !strings.Contains(err.Error(), "context canceled") {
			t.Errorf("StartContainer() error = %v, want error containing 'context canceled'", err)
		}
	})

	t.Run("context timeout during operations", func(t *testing.T) {
		mockExecutor := NewMockCommandExecutor()
		mockExecutor.FailureMode = "timeout"

		host := CreateLocalHost()
		ops := NewDockerOperations(mockExecutor, host)
		ctx := context.Background()

		_, _, err := ops.StartContainer(ctx, CreateTestContainerConfig())

		if err == nil || !strings.Contains(err.Error(), "context deadline exceeded") {
			t.Errorf("StartContainer() error = %v, want error containing 'context deadline exceeded'", err)
		}
	})
}

func TestParsePortFromString(t *testing.T) {
	tests := []struct {
		name     string
		portStr  string
		expected int
	}{
		{
			name:     "valid port number",
			portStr:  "8080",
			expected: 8080,
		},
		{
			name:     "zero port",
			portStr:  "0",
			expected: 0,
		},
		{
			name:     "high port number",
			portStr:  "65535",
			expected: 65535,
		},
		{
			name:     "invalid port string",
			portStr:  "invalid",
			expected: -1,
		},
		{
			name:     "empty string",
			portStr:  "",
			expected: -1,
		},
		{
			name:     "negative number",
			portStr:  "-1",
			expected: -1,
		},
		{
			name:     "floating point number",
			portStr:  "8080.5",
			expected: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ParsePortFromString(tt.portStr)
			if result != tt.expected {
				t.Errorf("ParsePortFromString(%q) = %d, want %d", tt.portStr, result, tt.expected)
			}
		})
	}
}

// Helper functions

func compareStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}
