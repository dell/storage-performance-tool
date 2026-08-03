/*
Copyright © 2025 Dell Technologies
*/

package docker

import (
	"context"
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
)

// Start -> Logs -> Stop lifecycle with mocks
func TestDockerIntegration_E2E_ContainerLifecycle(t *testing.T) {
	t.Setenv(constants.EnvSkipImagePull, "true")
	mock := command.NewMockCommandExecutor()
	mock.SetCommandSuccess("docker images -q "+constants.DefaultSptImage, "sha256:abc123")
	startCmd := "docker run -d --name integration-test -e TEST_ENV=integration -p 8080:8080/tcp " + constants.DefaultSptImage + " --help"
	mock.SetCommandSuccess(startCmd, "integration123abc")
	mock.SetCommandSuccess("docker ps -q --filter id=integration123abc", "integration123abc")
	mock.SetCommandSuccess("docker logs integration123abc", "Container started successfully\nRunning integration test")
	mock.SetCommandSuccess("docker rm -f integration123abc", "integration123abc")

	host := command.CreateLocalHost()
	ops := command.NewDockerOperations(mock, host)
	ctx := context.Background()

	cfg := command.ContainerConfig{Image: constants.DefaultSptImage, Name: "integration-test", NetworkMode: command.NetworkModeBridge,
		PortMappings: []command.PortMapping{{HostPort: 8080, ContainerPort: 8080, Protocol: "tcp"}},
		Environment:  map[string]string{"TEST_ENV": "integration"}, Command: []string{"--help"}, Detached: true,
	}

	id, _, err := ops.StartContainer(ctx, cfg)
	if err != nil {
		t.Fatalf("start failed: %v", err)
	}
	if id != "integration123abc" {
		t.Errorf("unexpected id %s", id)
	}
	running, err := ops.IsContainerRunning(ctx, id)
	if err != nil || !running {
		t.Fatalf("expected running, got err=%v running=%v", err, running)
	}
	var logs []string
	_ = ops.GetContainerLogs(ctx, id, false, func(s string) { logs = append(logs, s) }, func(string) {})
	if len(logs) != 2 {
		t.Errorf("expected 2 logs, got %d", len(logs))
	}
	if err := ops.StopContainer(ctx, id); err != nil {
		t.Fatalf("stop failed: %v", err)
	}
}

// Port conflict detection using one-shot snapshot
func TestDockerIntegration_E2E_PortConflictDetection(t *testing.T) {
	mock := command.NewMockCommandExecutor()
	// Snapshot shows 8080 in use, 9090 free
	mock.SetCommandResponse("sh -c "+constants.PortSnapshotCommand, command.MockResponse{Stdout: "tcp LISTEN 0 128 0.0.0.0:8080 0.0.0.0:*\n", Stderr: "", Error: nil})
	mock.SetCommandSuccess("docker ps -a --format "+constants.DockerConflictFormat, "abc123def456\tconflicting-container\t"+constants.DefaultSptImage+"\trunning\t0.0.0.0:8080->8080/tcp\t2024-01-01 12:00:00 +0000 UTC")

	host := command.CreateLocalHost()
	checker := NewPortChecker(host, mock, []int{8080, 9090})
	info, err := checker.CheckForConflicts(context.Background())
	if err != nil {
		t.Fatalf("conflict check err: %v", err)
	}
	if !info.HasConflicts() {
		t.Fatal("expected conflicts")
	}
	if len(info.ConflictPorts) != 1 || info.ConflictPorts[0] != 8080 {
		t.Errorf("unexpected conflicts: %v", info.ConflictPorts)
	}
	if len(info.AvailablePorts) != 1 || info.AvailablePorts[0] != 9090 {
		t.Errorf("unexpected available: %v", info.AvailablePorts)
	}
	if len(info.Containers) != 1 {
		t.Errorf("expected 1 container, got %d", len(info.Containers))
	}
}

// Worker node deployment with snapshot-based preflight
func TestDockerIntegration_E2E_WorkerNodeDeployment(t *testing.T) {
	t.Setenv(constants.EnvSkipImagePull, "true")
	mock := command.NewMockCommandExecutor()
	mock.SetCommandSuccess("docker images -q "+constants.DefaultSptImage, "sha256:abc123")
	mock.SetCommandResponse("sh -c "+constants.PortSnapshotCommand, command.MockResponse{Stdout: "", Stderr: "", Error: nil})
	mock.SetCommandSuccess("docker ps -a --format "+constants.DockerConflictFormat, "")
	workerCmd := "docker run -d --name integration-worker -e JAVA_OPTS=-Djava.rmi.server.hostname=localhost -e JAVA_TOOL_OPTIONS=-Djava.rmi.server.hostname=localhost --network host " + constants.DefaultSptImage + " --run-node=true --run-port=9999 --load-step-node-port=1099"
	mock.SetCommandSuccess(workerCmd, "worker123def")
	host := command.CreateLocalHost()
	ports := []int{constants.DefaultRMIRegistryPort, constants.DefaultSptAPIPort, 40000, 40001, 40002}
	checker := NewPortChecker(host, mock, ports)
	info, err := checker.CheckForConflicts(context.Background())
	if err != nil {
		t.Fatalf("conflict check err: %v", err)
	}
	if info.HasConflicts() {
		t.Fatalf("unexpected conflicts: %v", info.ConflictPorts)
	}
	ops := command.NewDockerOperations(mock, host).(*command.DockerOperationsImpl)
	id, err := ops.StartWorkerNodeContainer(context.Background(), constants.DefaultSptImage, "integration-worker", "localhost", 40000, 3)
	if err != nil {
		t.Fatalf("start worker err: %v", err)
	}
	if id != "worker123def" {
		t.Errorf("unexpected id %s", id)
	}
}

// Error handling scenarios remain the same
func TestDockerIntegration_E2E_ErrorHandling(t *testing.T) {
	t.Run("Docker not available", func(t *testing.T) {
		mock := command.NewMockCommandExecutor()
		mock.FailureMode = "docker_not_found"
		host := command.CreateLocalHost()
		ops := command.NewDockerOperations(mock, host)
		ctx := context.Background()
		_, _, err := ops.StartContainer(ctx, command.CreateTestContainerConfig())
		if err == nil {
			t.Error("expected error when Docker not available")
		}
		checker := NewPortChecker(host, mock, []int{8080})
		info, checkErr := checker.CheckForConflicts(ctx)
		if checkErr != nil {
			t.Errorf("port checker error: %v", checkErr)
		}
		if len(info.Containers) != 0 {
			t.Error("unexpected containers when docker unavailable")
		}
	})
	t.Run("SSH connectivity failure", func(t *testing.T) {
		mock := command.NewMockCommandExecutor()
		mock.FailureMode = "ssh_failure"
		remote := command.CreateRemoteHost("unreachable.example.com")
		ops := command.NewDockerOperations(mock, remote)
		_, _, err := ops.StartContainer(context.Background(), command.CreateTestContainerConfig())
		if err == nil {
			t.Error("expected ssh error")
		}
	})
	t.Run("Context cancellation handling", func(t *testing.T) {
		mock := command.NewMockCommandExecutor()
		mock.FailureMode = "context_cancelled"
		host := command.CreateLocalHost()
		ops := command.NewDockerOperations(mock, host)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _, err := ops.StartContainer(ctx, command.CreateTestContainerConfig())
		if err == nil || !strings.Contains(err.Error(), "context canceled") {
			t.Errorf("expected context canceled, got %v", err)
		}
	})
}
