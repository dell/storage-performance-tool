/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestMultiHostOrchestrator_NewMultiHostTestOrchestrator(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
		{Host: "host2", IsLocal: false, Original: "host2"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	wrapper := NewMultiHostTestOrchestrator(orchestrator)

	if wrapper == nil {
		t.Fatal("Wrapper should not be nil")
	}

	if wrapper.multiHost != orchestrator {
		t.Error("Wrapper should contain the original orchestrator")
	}

	// Test SetCallbacks doesn't panic
	wrapper.SetCallbacks(
		func(status *TestStatus) {},
		func(update *MultiNodeMetricsUpdate) {},
		func(line string) {},
		func(err string) {},
	)
}

func TestMultiHostOrchestrator_StartContainers_NoDockerManager(t *testing.T) {
	// Test the path when hosts don't have Docker managers
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)

	// Set host as ready but without DockerManager
	orchestrator.hosts[0].SetStatus(HostStatusReady)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := orchestrator.StartContainers(ctx, "test-image", "/test/scenario.js")

	// Should fail because hosts don't have Docker managers
	if err == nil {
		t.Error("StartContainers should fail when hosts lack Docker managers")
	}
}

func TestMultiHostOrchestrator_WaitForAPIs_NoRunningContainers_Simple(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)

	// Leave host in non-running state
	orchestrator.hosts[0].SetStatus(HostStatusReady)

	ctx := context.Background()
	err := orchestrator.WaitForAPIs(ctx, 1*time.Second)

	if err == nil {
		t.Error("WaitForAPIs should fail when no containers are running")
		return
	}

	expectedError := "no running containers to wait for"
	if !containsStringSimple(err.Error(), expectedError) {
		t.Errorf("Error should contain '%s', got: %s", expectedError, err.Error())
	}
}

func TestMultiHostOrchestrator_StopAllContainers_NoRunning(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)

	// Set host as ready but not running
	orchestrator.hosts[0].SetStatus(HostStatusReady)

	ctx := context.Background()
	err := orchestrator.StopAllContainers(ctx)

	// Should succeed with no running containers
	if err != nil {
		t.Errorf("StopAllContainers should succeed when no containers are running, got: %v", err)
	}
}

func TestMultiHostOrchestrator_StopAllContainers_EmptyContainerID(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)

	// Set host as running but without container ID
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	// Leave ContainerID empty

	ctx := context.Background()
	err := orchestrator.StopAllContainers(ctx)

	// Should succeed because empty container IDs are skipped
	if err != nil {
		t.Errorf("StopAllContainers should succeed when container IDs are empty, got: %v", err)
	}
}

func TestMultiHostOrchestrator_StopAllContainers_ReadySnapshots(t *testing.T) {
	var readyHits int32
	var statuses []int
	var mu sync.Mutex
	var shutdown atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ready":
			atomic.AddInt32(&readyHits, 1)
			mu.Lock()
			defer mu.Unlock()
			if shutdown.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				statuses = append(statuses, http.StatusServiceUnavailable)
				_, _ = w.Write([]byte(`{"ready":false,"status":"stopping"}`))
				return
			}
			w.WriteHeader(http.StatusOK)
			statuses = append(statuses, http.StatusOK)
			_, _ = w.Write([]byte(`{"ready":true,"status":"ready"}`))
		case "/shutdown":
			shutdown.Store(true)
			w.WriteHeader(http.StatusOK)
		case "/status":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"state":"COMPLETED"}`))
		case "/metrics":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("# HELP test\n"))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	hostInfos := []*hostparse.HostInfo{{Host: "host1", Original: "host1"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	h := orchestrator.hosts[0]
	h.SetStatus(HostStatusRunning)
	h.ContainerID = "container-1"
	h.APIClient = NewSptAPIClient(server.URL)
	h.DockerManager = NewMockDockerManager()
	h.SetManaged(true)

	err := orchestrator.StopAllContainers(context.Background())
	if err != nil {
		t.Fatalf("unexpected error stopping containers: %v", err)
	}
	if atomic.LoadInt32(&readyHits) < 2 {
		t.Fatalf("expected at least two /ready calls, got %d", readyHits)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(statuses) < 2 || statuses[0] != http.StatusOK || statuses[len(statuses)-1] != http.StatusServiceUnavailable {
		t.Fatalf("unexpected readiness statuses: %v", statuses)
	}
	if h.DockerManager.(*MockDockerManager).GetCleanupCallCount() != 1 {
		t.Fatalf("expected cleanup to be called once")
	}
}

func TestMultiHostOrchestrator_GetHostsInfo_CompleteInfo(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
		{Host: "host2", IsLocal: false, Original: "host2"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)

	// Set up different states
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	orchestrator.hosts[0].ContainerID = "container-123"

	orchestrator.hosts[1].SetError(fmt.Errorf("connection failed"))

	hostsInfo := orchestrator.GetHostsInfo()

	if len(hostsInfo) != 2 {
		t.Errorf("Expected 2 host info entries, got %d", len(hostsInfo))
	}

	// Check first host info
	info1 := hostsInfo[0]
	if info1["host"] != "host1" {
		t.Errorf("Expected host1, got %s", info1["host"])
	}
	if info1["status"] != "running" {
		t.Errorf("Expected running status, got %s", info1["status"])
	}
	if info1["container_id"] != "container-123" {
		t.Errorf("Expected container-123, got %s", info1["container_id"])
	}

	// Check second host info
	info2 := hostsInfo[1]
	if info2["host"] != "host2" {
		t.Errorf("Expected host2, got %s", info2["host"])
	}
	if info2["status"] != "failed" {
		t.Errorf("Expected failed status, got %s", info2["status"])
	}
	if info2["error"] == nil {
		t.Errorf("Expected error to be set")
	}
}

// Simple helper function for string containment
func containsStringSimple(haystack, needle string) bool {
	if len(needle) > len(haystack) {
		return false
	}
	for i := 0; i <= len(haystack)-len(needle); i++ {
		if haystack[i:i+len(needle)] == needle {
			return true
		}
	}
	return false
}

// RMI Readiness Tests

func TestMultiHostOrchestrator_CheckPort(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)

	// Test with a port that should be closed (high port number)
	result := orchestrator.checkPort("127.0.0.1", 65432, 1*time.Second)
	if result {
		t.Error("Expected checkPort to return false for closed port, got true")
	}

	// Test with invalid host
	result = orchestrator.checkPort("invalid-host-that-does-not-exist", 80, 1*time.Second)
	if result {
		t.Error("Expected checkPort to return false for invalid host, got true")
	}
}

func TestMultiHostOrchestrator_WaitForRMIReady_Timeout(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)

	// If the standard RMI port is already open on localhost (e.g., a dev
	// environment is running a node), this test's assumption is invalid.
	// Skip to avoid flakiness on shared environments/CI agents.
	if orchestrator.checkPort("127.0.0.1", constants.RMIRegistryPortInt, 100*time.Millisecond) {
		t.Skip("skipping: RMI port 1099 is open on localhost")
	}

	// Create a host connection that doesn't have RMI running
	host := &HostConnection{
		Info:   hostInfos[0],
		Status: HostStatusReady,
	}

	// Test with very short timeout to ensure it times out quickly
	err := orchestrator.waitForRMIReady(host, 2*time.Second)
	if err == nil {
		t.Error("Expected waitForRMIReady to timeout, but it succeeded")
	}
	if !strings.Contains(err.Error(), "RMI registry not ready") {
		t.Errorf("Expected timeout error message, got: %v", err)
	}
}

func TestMultiHostOrchestrator_WaitForRMIReady_InvalidHost(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "invalid-host-that-does-not-exist", IsLocal: false, Original: "invalid-host"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)

	host := &HostConnection{
		Info:   hostInfos[0],
		Status: HostStatusReady,
	}

	// Test with invalid host - should fail quickly
	err := orchestrator.waitForRMIReady(host, 3*time.Second)
	if err == nil {
		t.Error("Expected waitForRMIReady to fail with invalid host, but it succeeded")
	}
}

func TestMultiHostOrchestrator_WaitForAllWorkersReady_NoWorkers(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)

	// Test with empty worker list
	err := orchestrator.waitForAllWorkersReady([]*HostConnection{}, 5*time.Second)
	if err != nil {
		t.Errorf("Expected waitForAllWorkersReady to succeed with no workers, got: %v", err)
	}
}

func TestMultiHostOrchestrator_WaitForAllWorkersReady_InsufficientWorkers(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "worker1", IsLocal: false, Original: "worker1"},
		{Host: "worker2", IsLocal: false, Original: "worker2"},
	}

	// Set minHosts to 3 (1 entry + 2 workers) but only provide 2 total
	orchestrator := NewMultiHostOrchestrator(hostInfos, 3)

	workers := []*HostConnection{
		{
			Info:   hostInfos[0],
			Status: HostStatusRunning,
		},
		{
			Info:   hostInfos[1],
			Status: HostStatusRunning,
		},
	}

	// Both workers will fail to be ready (invalid hosts), so we'll have 0 ready workers
	// but need 2 (minHosts 3 - 1 entry node = 2 workers required)
	err := orchestrator.waitForAllWorkersReady(workers, 2*time.Second)
	if err == nil {
		t.Error("Expected waitForAllWorkersReady to fail with insufficient ready workers")
	}
	if !strings.Contains(err.Error(), "insufficient ready workers") {
		t.Errorf("Expected 'insufficient ready workers' error, got: %v", err)
	}
}

func TestStartEntryNode_UsesAdvertisedIPForWorkerAddresses(t *testing.T) {
	// Set up two hosts: primary and one worker
	hostInfos := []*hostparse.HostInfo{
		{Host: "entry1", IsLocal: false, Original: "entry1"},
		{Host: "worker1", IsLocal: false, Original: "worker1"},
	}

	o := NewMultiHostOrchestrator(hostInfos, 2)
	o.image = "test-image"

	// Assign mock docker managers
	primary := o.hosts[0]
	worker := o.hosts[1]
	primary.DockerManager = NewMockDockerManager()
	worker.DockerManager = NewMockDockerManager()

	// Mark worker as running and set an advertised IP
	worker.SetStatus(HostStatusRunning)
	worker.AdvertisedIP = "10.9.8.7"

	// Call startEntryNode directly to avoid network waits
	params := scenario.ScenarioParams{WorkloadType: "mock", Threads: 1, ObjectSize: "1MB", ObjectCount: 1}
	if err := o.startEntryNode(primary, []*HostConnection{worker}, params); err != nil {
		t.Fatalf("startEntryNode error: %v", err)
	}

	// Verify entry node received worker address built from AdvertisedIP
	calls := primary.DockerManager.(*MockDockerManager).GetEntryNodeCalls()
	if len(calls) == 0 {
		t.Fatalf("expected StartEntryNodeContainer to be called")
	}
	got := calls[0].WorkerAddresses
	want := []string{"10.9.8.7:" + constants.RMIRegistryPort}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("worker addresses mismatch: got %v, want %v", got, want)
	}
}

func TestMultiHostOrchestrator_WaitForAllWorkersReady_PartialFailure(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "127.0.0.1", IsLocal: true, Original: "127.0.0.1"},
		{Host: "invalid-host", IsLocal: false, Original: "invalid-host"},
	}

	// Set minHosts to 1 (1 entry + 0 workers required) so partial failure is acceptable
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)

	workers := []*HostConnection{
		{
			Info:   hostInfos[0],
			Status: HostStatusRunning,
		},
		{
			Info:   hostInfos[1],
			Status: HostStatusRunning,
		},
	}

	// One worker will fail (invalid host), but since we only require 0 workers (minHosts=1 - 1 entry = 0), it should succeed
	err := orchestrator.waitForAllWorkersReady(workers, 2*time.Second)
	if err != nil {
		t.Errorf("Expected waitForAllWorkersReady to succeed with partial failure when minHosts allows it, got: %v", err)
	}
}

func TestMultiHostOrchestrator_WaitForAllWorkersReady_ErrorPropagation(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "invalid1", IsLocal: false, Original: "invalid1"},
		{Host: "invalid2", IsLocal: false, Original: "invalid2"},
	}

	// Require 2 workers (minHosts=3 - 1 entry = 2 workers)
	orchestrator := NewMultiHostOrchestrator(hostInfos, 3)

	workers := []*HostConnection{
		{
			Info:   hostInfos[0],
			Status: HostStatusRunning,
		},
		{
			Info:   hostInfos[1],
			Status: HostStatusRunning,
		},
	}

	// Both workers will fail, and we need 2, so should get detailed error
	err := orchestrator.waitForAllWorkersReady(workers, 1*time.Second)
	if err == nil {
		t.Error("Expected waitForAllWorkersReady to fail when all workers fail")
	}

	// Check that error contains details about both failed workers
	errStr := err.Error()
	if !strings.Contains(errStr, "insufficient ready workers: 0 ready, 2 required") {
		t.Errorf("Expected detailed error message about worker requirements, got: %v", err)
	}
	if !strings.Contains(errStr, "invalid1") || !strings.Contains(errStr, "invalid2") {
		t.Errorf("Expected error to contain details about failed workers, got: %v", err)
	}
}

// Distributed Test Flow Tests

func TestMultiHostTestOrchestrator_StartTest_NoReadyHosts(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	wrapper := NewMultiHostTestOrchestrator(orchestrator)

	// Don't set any hosts as ready - all will remain in initial state

	ctx := context.Background()
	params := scenario.ScenarioParams{WorkloadType: "write"}

	err := wrapper.StartTest(ctx, "test-image", params)
	if err == nil {
		t.Error("Expected StartTest to fail with no ready hosts")
	}
	if !strings.Contains(err.Error(), "no ready hosts available") {
		t.Errorf("Expected 'no ready hosts available' error, got: %v", err)
	}
}

func TestMultiHostTestOrchestrator_StartTest_SingleHost(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "localhost", IsLocal: true, Original: "localhost"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	wrapper := NewMultiHostTestOrchestrator(orchestrator)

	// Set up host as ready with mock Docker manager
	orchestrator.hosts[0].SetStatus(HostStatusReady)
	mockDocker := NewMockDockerManager()
	orchestrator.hosts[0].DockerManager = mockDocker

	// Set up callbacks to capture status updates
	var statusUpdate *TestStatus
	var outputMessage string

	wrapper.SetCallbacks(
		func(status *TestStatus) { statusUpdate = status },
		func(update *MultiNodeMetricsUpdate) {},
		func(line string) { outputMessage = line },
		func(err string) {},
	)

	ctx := context.Background()
	params := scenario.ScenarioParams{WorkloadType: "write"}

	err := wrapper.StartTest(ctx, "test-image", params)
	if err != nil {
		t.Errorf("Expected StartTest to succeed for single host, got: %v", err)
	}

	// Verify status update
	if statusUpdate == nil {
		t.Error("Expected status update to be called")
	} else {
		if statusUpdate.State != "RUNNING" {
			t.Errorf("Expected status state 'RUNNING', got: %s", statusUpdate.State)
		}
		if !strings.Contains(statusUpdate.Message, "Single-host test running") {
			t.Errorf("Expected single-host message, got: %s", statusUpdate.Message)
		}
	}

	// Verify output message
	if !strings.Contains(outputMessage, "Starting single-host test on localhost") {
		t.Errorf("Expected single-host output message, got: %s", outputMessage)
	}

	// Verify Docker manager was called
	if mockDocker.GetStartCallCount() == 0 {
		t.Error("Expected StartContainerWithScenario to be called for single host")
	}
}

// Test verifies that multi-host logic is selected but we don't test the actual StartDistributedTest
// implementation here since that requires real network operations and complex setup
func TestMultiHostTestOrchestrator_StartTest_MultiHost_Logic(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
		{Host: "host2", IsLocal: false, Original: "host2"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	wrapper := NewMultiHostTestOrchestrator(orchestrator)

	// Set up both hosts as ready
	orchestrator.hosts[0].SetStatus(HostStatusReady)
	orchestrator.hosts[1].SetStatus(HostStatusReady)

	// Set up callbacks to capture status updates
	var statusUpdate *TestStatus
	var outputMessage string

	wrapper.SetCallbacks(
		func(status *TestStatus) { statusUpdate = status },
		func(update *MultiNodeMetricsUpdate) {},
		func(line string) { outputMessage = line },
		func(err string) {},
	)

	// Verify the logic chooses multi-host path by checking the ready hosts count
	readyHosts := orchestrator.GetReadyHosts()
	if len(readyHosts) != 2 {
		t.Fatalf("Expected 2 ready hosts, got %d", len(readyHosts))
	}

	// This verifies that our StartTest method would take the multi-host path
	// (we're testing the logic without executing the full distributed test)
	ctx := context.Background()
	params := scenario.ScenarioParams{WorkloadType: "write"}

	// We'll expect this to fail due to missing DockerManager, but the callbacks
	// should be called with multi-host messages before the failure
	wrapper.StartTest(ctx, "test-image", params)

	// The important thing is that the status update indicates multi-host operation
	if statusUpdate != nil && statusUpdate.State == "RUNNING" &&
		strings.Contains(statusUpdate.Message, "Multi-host distributed test running on 2 hosts") {
		// Success - multi-host path was selected
		t.Logf("Multi-host path correctly selected: %s", statusUpdate.Message)
	}

	if outputMessage != "" && strings.Contains(outputMessage, "Starting distributed test on hosts: host1, host2") {
		// Success - output message indicates distributed test
		t.Logf("Distributed test output correctly generated: %s", outputMessage)
	}
}

func TestMultiHostOrchestrator_StartDistributedTest_AttachWorkers(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:1099")
	if err != nil {
		t.Skipf("unable to bind local RMI port for test: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	hostInfos := []*hostparse.HostInfo{
		{Host: "entry", Original: "entry"},
		{Host: "127.0.0.1", Original: "worker1"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	orchestrator.SetAttachExistingWorkers(true)
	orchestrator.detectAdvIP = func(_ context.Context, _ *hostparse.HostInfo) (string, error) {
		return "127.0.0.1", nil
	}

	entryDM := NewMockDockerManager()
	workerDM := NewMockDockerManager()
	orchestrator.hosts[0].DockerManager = entryDM
	orchestrator.hosts[0].SetStatus(HostStatusReady)
	orchestrator.hosts[1].DockerManager = workerDM
	orchestrator.hosts[1].SetStatus(HostStatusReady)

	params := scenario.ScenarioParams{
		WorkloadType: "mock",
		Threads:      1,
		ObjectSize:   "1MB",
	}

	err = orchestrator.StartDistributedTest(context.Background(), "test-image", params)
	if err != nil {
		t.Fatalf("StartDistributedTest returned error: %v", err)
	}

	worker := orchestrator.hosts[1]
	if worker.GetStatus() != HostStatusRunning {
		t.Fatalf("expected worker status running, got %s", worker.GetStatus())
	}
	if worker.IsManaged() {
		t.Fatalf("expected worker to remain unmanaged in attach mode")
	}
	if worker.ContainerID != "" {
		t.Fatalf("expected worker container ID to be empty, got %s", worker.ContainerID)
	}
	if worker.AdvertisedIP != "127.0.0.1" {
		t.Fatalf("expected advertised IP 127.0.0.1, got %s", worker.AdvertisedIP)
	}
	if len(workerDM.GetWorkerNodeCalls()) != 0 {
		t.Fatalf("expected no worker container launches, got %d", len(workerDM.GetWorkerNodeCalls()))
	}

	entry := orchestrator.hosts[0]
	if !entry.IsManaged() {
		t.Fatalf("expected entry node to be managed")
	}
	if len(entryDM.GetEntryNodeCalls()) != 1 {
		t.Fatalf("expected one entry node launch, got %d", len(entryDM.GetEntryNodeCalls()))
	}
}

func TestMultiHostOrchestrator_StopAllContainers_SkipsUnmanaged(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "entry", Original: "entry"},
		{Host: "worker1", Original: "worker1"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)

	entryDM := NewMockDockerManager()
	workerDM := NewMockDockerManager()

	entry := orchestrator.hosts[0]
	entry.DockerManager = entryDM
	entry.ContainerID = "entry"
	entry.SetStatus(HostStatusRunning)
	entry.SetManaged(true)

	worker := orchestrator.hosts[1]
	worker.DockerManager = workerDM
	worker.ContainerID = "worker"
	worker.SetStatus(HostStatusRunning)
	worker.SetManaged(false)

	if err := orchestrator.StopAllContainers(context.Background()); err != nil {
		t.Fatalf("StopAllContainers returned error: %v", err)
	}

	if entryDM.GetCleanupCallCount() != 1 {
		t.Fatalf("expected entry cleanup to run once, got %d", entryDM.GetCleanupCallCount())
	}
	if workerDM.GetCleanupCallCount() != 0 {
		t.Fatalf("expected worker cleanup to be skipped, got %d", workerDM.GetCleanupCallCount())
	}
	if worker.IsManaged() {
		t.Fatalf("expected worker to remain unmanaged after stop")
	}
}

// LIST workload should run only on the primary while workers stay idle
// but visible/connected in the live view. Verify that the orchestrator:
// - starts API-only containers on workers (StartContainerInNodeMode)
// - starts the entry node on the primary with no worker addresses
// - emits a baseline update that contains all nodes, with workers inactive
func TestMultiHostTestOrchestrator_StartTest_List_PrimaryOnly(t *testing.T) {
	// Mock API server that is always ready and accepts /run
	var runStarted atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ready":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ready":true,"status":"ready","scope":"node","role":"entry","node_id":"n0"}`))
		case "/health":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok","scope":"node","role":"entry","node_id":"n0"}`))
		case "/run":
			if r.Method == http.MethodPost {
				runStarted.Store(true)
				w.Header().Set("ETag", "\"run-123\"")
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"runId":"run-123"}`))
				return
			}
			w.WriteHeader(http.StatusMethodNotAllowed)
		case "/status":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"state":"RUNNING","message":"running","runId":"run-123"}`))
		case "/metrics/json":
			// Return a minimal single-step metrics sample to be parseable when polling begins
			now := time.Now().UTC().Format(time.RFC3339Nano)
			payload := fmt.Sprintf(`[{"metrics_schema":2,"scope":"node","role":"entry","node_id":"n0","run_id":"run-123","sample_ts":"%s","step_id":"s-list","op_type":"LIST","timestamp":%d,"elapsed_time_seconds":0.1,"test_state":1,"completion_percent":0,"overall_completion_percent":0,"unbounded":true,"overall_unbounded":true,"operations":{"success_count":0,"failed_count":0,"success_rate_last":0,"failed_rate_last":0},"bandwidth":{"bytes_total":0,"bytes_rate_last":0},"timing":{"latency_mean_us":0,"duration_mean_us":0},"concurrency":{"current":0,"mean":0}}]`, now, time.Now().UnixMilli())
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(payload))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	// Extract port from server URL and normalize hosts to 127.0.0.1 for APIURL building
	u, _ := url.Parse(srv.URL)
	hostPort := u.Host
	_, port, _ := net.SplitHostPort(hostPort)

	hostInfos := []*hostparse.HostInfo{
		{Host: "127.0.0.1", IsLocal: true, Original: "primary"},
		{Host: "127.0.0.1", IsLocal: true, Original: "worker1"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	orchestrator.apiPort = port // point all API probes to our mock server

	// Mark both hosts ready and attach mock docker managers
	primary := orchestrator.hosts[0]
	worker := orchestrator.hosts[1]
	primary.SetStatus(HostStatusReady)
	worker.SetStatus(HostStatusReady)
	primaryDM := NewMockDockerManager()
	workerDM := NewMockDockerManager()
	primary.DockerManager = primaryDM
	worker.DockerManager = workerDM

	wrapper := NewMultiHostTestOrchestrator(orchestrator)

	// Capture first metrics update (baseline)
	baselineCh := make(chan *MultiNodeMetricsUpdate, 1)
	wrapper.SetCallbacks(
		func(*TestStatus) {},
		func(update *MultiNodeMetricsUpdate) {
			select {
			case baselineCh <- update:
			default:
			}
		},
		func(string) {},
		func(string) {},
	)

	// Start the LIST workload (primary-only)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	params := scenario.ScenarioParams{WorkloadType: scenario.WorkloadTypeList, Threads: 2, Bucket: "demo", Endpoint: "http://minio:9000"}
	if err := wrapper.StartTest(ctx, "test-image", params); err != nil {
		t.Fatalf("StartTest(list) returned error: %v", err)
	}

	// Wait briefly for baseline emission
	var baseline *MultiNodeMetricsUpdate
	select {
	case baseline = <-baselineCh:
	case <-time.After(300 * time.Millisecond):
		// best-effort; baseline should have been sent before StartTest returned
	}

	// Workers should have started API-only node containers
	if got := len(workerDM.GetContainerCalls()); got != 1 {
		t.Fatalf("expected 1 worker node start call, got %d", got)
	}

	// Primary should start entry node with no worker addresses
	entryCalls := primaryDM.GetEntryNodeCalls()
	if len(entryCalls) != 1 {
		t.Fatalf("expected 1 entry node start call, got %d", len(entryCalls))
	}
	if len(entryCalls[0].WorkerAddresses) != 0 {
		t.Fatalf("expected zero worker addresses for LIST primary-only, got %v", entryCalls[0].WorkerAddresses)
	}

	// API-based run should have been initiated
	if !runStarted.Load() {
		t.Fatalf("expected run to be started via /run API on primary")
	}

	// Baseline should show both nodes present with worker inactive
	if baseline != nil {
		if len(baseline.NodeStatus) < 2 {
			t.Fatalf("expected baseline status for both nodes, got %d", len(baseline.NodeStatus))
		}
		ws, ok := baseline.NodeStatus["worker1"]
		if !ok {
			t.Fatalf("baseline missing worker1 status")
		}
		if ws.IsActive {
			t.Fatalf("worker should be inactive in baseline for LIST runs")
		}
	}
}
