/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// TestStartWorkerNodeContainer tests the worker node container startup functionality
func TestStartWorkerNodeContainer(t *testing.T) {
	mock := NewMockDockerManager()

	// Test successful worker node start
	containerID, err := mock.StartWorkerNodeContainer(
		"test-image",
		"192.168.1.100",
		40000,
		10,
	)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if containerID != "mock-container-123" {
		t.Errorf("Expected container ID 'mock-container-123', got %s", containerID)
	}

	// Verify the call was recorded
	calls := mock.GetWorkerNodeCalls()
	if len(calls) != 1 {
		t.Fatalf("Expected 1 worker node call, got %d", len(calls))
	}

	call := calls[0]
	if call.Image != "test-image" {
		t.Errorf("Expected image 'test-image', got %s", call.Image)
	}
	if call.RMIHostname != "192.168.1.100" {
		t.Errorf("Expected RMI hostname '192.168.1.100', got %s", call.RMIHostname)
	}
	if call.RMIPortStart != 40000 {
		t.Errorf("Expected RMI port start 40000, got %d", call.RMIPortStart)
	}
	if call.RMIPortCount != 10 {
		t.Errorf("Expected RMI port count 10, got %d", call.RMIPortCount)
	}
}

// TestStartEntryNodeContainer tests the entry node container startup functionality
func TestStartEntryNodeContainer(t *testing.T) {
	mock := NewMockDockerManager()

	// Test successful entry node start
	workerAddresses := []string{"192.168.1.100:1099", "192.168.1.101:1099"}
	additionalArgs := []string{"--storage-auth-uid=test", "--storage-auth-secret=secret"}

	containerID, err := mock.StartEntryNodeContainer(
		"test-image",
		workerAddresses,
		additionalArgs,
		"host",
	)

	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if containerID != "mock-container-123" {
		t.Errorf("Expected container ID 'mock-container-123', got %s", containerID)
	}

	// Verify the call was recorded
	calls := mock.GetEntryNodeCalls()
	if len(calls) != 1 {
		t.Fatalf("Expected 1 entry node call, got %d", len(calls))
	}

	call := calls[0]
	if call.Image != "test-image" {
		t.Errorf("Expected image 'test-image', got %s", call.Image)
	}
	if len(call.WorkerAddresses) != 2 {
		t.Errorf("Expected 2 worker addresses, got %d", len(call.WorkerAddresses))
	}
	if call.WorkerAddresses[0] != "192.168.1.100:1099" {
		t.Errorf("Expected first worker address '192.168.1.100:1099', got %s", call.WorkerAddresses[0])
	}
	if len(call.AdditionalArgs) != 2 {
		t.Errorf("Expected 2 additional args, got %d", len(call.AdditionalArgs))
	}
}

// TestStartWorkerNodeContainer_Failure tests worker node container failure handling
func TestStartWorkerNodeContainer_Failure(t *testing.T) {
	mock := NewMockDockerManager()
	mock.SetFailOnStart(true)

	_, err := mock.StartWorkerNodeContainer(
		"test-image",
		"192.168.1.100",
		40000,
		10,
	)

	if err == nil {
		t.Fatal("Expected error when failOnStart is true")
	}

	expectedError := "mock start worker node container failed"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}

// TestStartEntryNodeContainer_Failure tests entry node container failure handling
func TestStartEntryNodeContainer_Failure(t *testing.T) {
	mock := NewMockDockerManager()
	mock.SetFailOnStart(true)

	_, err := mock.StartEntryNodeContainer(
		"test-image",
		[]string{"192.168.1.100:1099"},
		[]string{},
		"host",
	)

	if err == nil {
		t.Fatal("Expected error when failOnStart is true")
	}

	expectedError := "mock start entry node container failed"
	if err.Error() != expectedError {
		t.Errorf("Expected error '%s', got '%s'", expectedError, err.Error())
	}
}

// TestMultiHostOrchestratorRMI_Constructor tests the RMI-enabled constructor
func TestMultiHostOrchestratorRMI_Constructor(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", User: "root", IsLocal: false, Original: "root@host1"},
		{Host: "host2", User: "root", IsLocal: false, Original: "root@host2"},
	}

	// Test default constructor
	orchestrator1 := NewMultiHostOrchestrator(hostInfos, 1)
	if orchestrator1.networkMode != "host" {
		t.Errorf("Expected default network mode 'host', got %s", orchestrator1.networkMode)
	}
	if orchestrator1.rmiPortStart != 40000 {
		t.Errorf("Expected default RMI port start 40000, got %d", orchestrator1.rmiPortStart)
	}
	if orchestrator1.rmiPortCount != 10 {
		t.Errorf("Expected default RMI port count 10, got %d", orchestrator1.rmiPortCount)
	}

	// Test RMI-enabled constructor
	rmiConfig := RMIConfig{
		NetworkMode: "host",
		PortStart:   50000,
		PortCount:   20,
	}

	orchestrator2 := NewMultiHostOrchestratorWithRMI(hostInfos, 2, rmiConfig)
	if orchestrator2.networkMode != "host" {
		t.Errorf("Expected network mode 'host', got %s", orchestrator2.networkMode)
	}
	if orchestrator2.rmiPortStart != 50000 {
		t.Errorf("Expected RMI port start 50000, got %d", orchestrator2.rmiPortStart)
	}
	if orchestrator2.rmiPortCount != 20 {
		t.Errorf("Expected RMI port count 20, got %d", orchestrator2.rmiPortCount)
	}
	if orchestrator2.minHosts != 2 {
		t.Errorf("Expected min hosts 2, got %d", orchestrator2.minHosts)
	}
}
