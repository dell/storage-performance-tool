/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"strings"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

func newTestRemoteManager(t *testing.T) (*RemoteDockerManager, *command.MockCommandExecutor, *hostparse.HostInfo) {
	t.Helper()
	host := &hostparse.HostInfo{User: "root", Host: "worker1.example.com", Original: "root@worker1.example.com", IsLocal: false}
	mock := command.NewMockCommandExecutor()
	// Make any docker command succeed and return a fake container ID
	mock.DefaultResponse = command.MockResponse{Stdout: "abc123def456", Stderr: "", Error: nil}
	mgr, err := newRemoteDockerManagerWithExecutor(host, mock)
	if err != nil {
		t.Fatalf("failed to create remote docker manager: %v", err)
	}
	return mgr, mock, host
}

func TestRemoteDocker_StartContainerInNodeMode_RespectsPort(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)

	id, err := mgr.StartContainerInNodeMode(constants.DefaultSptImage, "10080", constants.BridgeNetworkMode)
	if err != nil {
		t.Fatalf("StartContainerInNodeMode error: %v", err)
	}
	if id == "" {
		t.Fatal("container ID should not be empty")
	}

	// Verify a docker run with the expected port mapping and args was executed
	executed := mock.GetExecutedCommandsMatching("docker run")
	if len(executed) == 0 {
		t.Fatalf("expected a docker run command to be executed")
	}
	cmd := strings.Join(executed[len(executed)-1].Command, " ")
	mustContain := []string{
		"docker run",
		"-d",
		"-p 10080:10080/tcp",
		constants.DefaultSptImage,
		"--run-node=true",
		"--run-port=10080",
		"--label spt.host=worker1.example.com",
		"--label spt.managed=true",
		"--label spt.role=node",
	}
	for _, s := range mustContain {
		if !strings.Contains(cmd, s) {
			t.Errorf("expected docker run command to contain %q, got: %s", s, cmd)
		}
	}
	if !strings.Contains(cmd, "--name spt-node-") {
		t.Errorf("expected docker run command to include node name prefix, got: %s", cmd)
	}
}

func TestRemoteDocker_StartContainerInNodeMode_HostNetwork(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)

	id, err := mgr.StartContainerInNodeMode(constants.DefaultSptImage, "10080", constants.HostNetworkMode)
	if err != nil {
		t.Fatalf("StartContainerInNodeMode error: %v", err)
	}
	if id == "" {
		t.Fatal("container ID should not be empty")
	}

	executed := mock.GetExecutedCommandsMatching("docker run")
	if len(executed) == 0 {
		t.Fatalf("expected a docker run command to be executed")
	}
	cmd := strings.Join(executed[len(executed)-1].Command, " ")
	mustContain := []string{
		"docker run",
		"-d",
		"--network host",
		constants.DefaultSptImage,
		"--run-node=true",
		"--run-port=10080",
	}
	for _, s := range mustContain {
		if !strings.Contains(cmd, s) {
			t.Errorf("expected docker run command to contain %q, got: %s", s, cmd)
		}
	}
	if strings.Contains(cmd, "-p 10080:10080/tcp") {
		t.Fatalf("host-network node should not publish bridge ports: %s", cmd)
	}
}

func TestRemoteDocker_StartWorkerNodeContainer_BuildsExpectedCommand(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)

	id, err := mgr.StartWorkerNodeContainer(constants.DefaultSptImage, "10.0.0.10", 40000, 3)
	if err != nil {
		t.Fatalf("StartWorkerNodeContainer error: %v", err)
	}
	if id == "" {
		t.Fatal("container ID should not be empty")
	}

	executed := mock.GetExecutedCommandsMatching("docker run")
	if len(executed) == 0 {
		t.Fatalf("expected a docker run command to be executed")
	}
	cmd := strings.Join(executed[len(executed)-1].Command, " ")
	mustContain := []string{
		"docker run",
		"-d",
		"-e JAVA_OPTS=-Djava.rmi.server.hostname=10.0.0.10",
		"-e JAVA_TOOL_OPTIONS=-Djava.rmi.server.hostname=10.0.0.10",
		"--network host",
		constants.DefaultSptImage,
		"--run-node=true",
		"--run-port=9999",
		"--load-step-node-port=1099",
		"--label spt.host=worker1.example.com",
		"--label spt.managed=true",
		"--label spt.role=worker",
	}
	for _, s := range mustContain {
		if !strings.Contains(cmd, s) {
			t.Errorf("expected worker run command to contain %q, got: %s", s, cmd)
		}
	}
	if !strings.Contains(cmd, "--name spt-worker-") {
		t.Errorf("expected worker run command to include worker name prefix, got: %s", cmd)
	}
}

func TestRemoteDocker_StartEntryNodeContainer_IncludesWorkerAddrsAndArgs(t *testing.T) {
	mgr, mock, _ := newTestRemoteManager(t)

	workerAddrs := []string{"w1:1099", "w2:1099"}
	extra := []string{"--storage-driver-type=s3", "--storage-net-node-addrs", "minio:9000"}

	id, err := mgr.StartEntryNodeContainer(constants.DefaultSptImage, workerAddrs, extra, constants.DefaultNetworkMode)
	if err != nil {
		t.Fatalf("StartEntryNodeContainer error: %v", err)
	}
	if id == "" {
		t.Fatal("container ID should not be empty")
	}

	executed := mock.GetExecutedCommandsMatching("docker run")
	if len(executed) == 0 {
		t.Fatalf("expected a docker run command to be executed")
	}
	cmd := strings.Join(executed[len(executed)-1].Command, " ")
	mustContain := []string{
		"docker run",
		"-d",
		"--network host",
		constants.DefaultSptImage,
		"--load-step-node-addrs=w1:1099,w2:1099",
		"--run-node=true",
		"--run-port=9999",
		"--storage-driver-type=s3",
		"--storage-net-node-addrs minio:9000",
		"--label spt.host=worker1.example.com",
		"--label spt.managed=true",
		"--label spt.role=entry",
	}
	for _, s := range mustContain {
		if !strings.Contains(cmd, s) {
			t.Errorf("expected entry run command to contain %q, got: %s", s, cmd)
		}
	}
	if !strings.Contains(cmd, "--name spt-entry-") {
		t.Errorf("expected entry run command to include entry name prefix, got: %s", cmd)
	}
}
