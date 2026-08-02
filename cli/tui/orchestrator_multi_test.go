/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	dockerconf "github.com/dell/storage-performance-tool/cli/internal/docker"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/preflight"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

type identityPreflight struct {
	identities      map[string]preflight.ImageIdentity
	errors          map[string]error
	payloads        map[string]string
	payloadErrors   map[string]error
	payloadCalls    *atomic.Int64
	runningPayloads map[string]string
	runningErrors   map[string]error
	runningCalls    *atomic.Int64
}

func (f identityPreflight) InspectPayloadIdentity(
	_ context.Context,
	host *hostparse.HostInfo,
	_ string,
) (string, error) {
	if f.payloadCalls != nil {
		f.payloadCalls.Add(1)
	}
	if err := f.payloadErrors[host.Original]; err != nil {
		return "", err
	}
	return f.payloads[host.Original], nil
}

func (f identityPreflight) InspectRunningPayloadIdentity(
	_ context.Context,
	host *hostparse.HostInfo,
	_ string,
) (string, error) {
	if f.runningCalls != nil {
		f.runningCalls.Add(1)
	}
	if err := f.runningErrors[host.Original]; err != nil {
		return "", err
	}
	return f.runningPayloads[host.Original], nil
}

func (f identityPreflight) CheckDocker(context.Context, *hostparse.HostInfo) (string, error) {
	return "test", nil
}

func (f identityPreflight) EnsureImage(context.Context, *hostparse.HostInfo, string) error {
	return nil
}

func (f identityPreflight) CheckPorts(context.Context, *hostparse.HostInfo, int, int, int) (*dockerconf.ConflictInfo, error) {
	return &dockerconf.ConflictInfo{}, nil
}

func (f identityPreflight) InspectImageIdentity(
	_ context.Context,
	host *hostparse.HostInfo,
	_ string,
) (preflight.ImageIdentity, error) {
	if err := f.errors[host.Original]; err != nil {
		return preflight.ImageIdentity{}, err
	}
	return f.identities[host.Original], nil
}

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

	err := orchestrator.StartContainers(ctx, "test-image", nil)

	// Should fail because hosts don't have Docker managers
	if err == nil {
		t.Error("StartContainers should fail when hosts lack Docker managers")
	}
}

func TestMultiHostOrchestrator_StartContainers_PassesNetworkMode(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}
	orchestrator := NewMultiHostOrchestratorWithRMI(hostInfos, 1, RMIConfig{
		NetworkMode: constants.HostNetworkMode,
		PortStart:   constants.DefaultRMIPortStart,
		PortCount:   constants.DefaultRMIPortCount,
	})
	mockDocker := NewMockDockerManager()
	orchestrator.hosts[0].DockerManager = mockDocker
	orchestrator.hosts[0].SetStatus(HostStatusReady)

	if err := orchestrator.StartContainers(context.Background(), "test-image", nil); err != nil {
		t.Fatalf("StartContainers() error = %v", err)
	}
	calls := mockDocker.GetContainerCalls()
	if len(calls) != 1 {
		t.Fatalf("container calls = %d, want 1", len(calls))
	}
	if got := strings.Join(calls[0].Cmd, " "); !strings.Contains(got, "--network-mode=host") {
		t.Fatalf("node mode call did not receive host network mode: %s", got)
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

func TestMultiHostOrchestratorWaitForAPIsHonorsCallerCancellation(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: "127.0.0.1", Original: "host1"}}, 1)
	orchestrator.apiPort = "1"
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	started := time.Now()
	err := orchestrator.WaitForAPIs(ctx, 5*time.Second)
	if !errors.Is(err, context.Canceled) || time.Since(started) > 100*time.Millisecond {
		t.Fatalf("WaitForAPIs() = %v after %s, want prompt context cancellation", err, time.Since(started))
	}
}

func TestMultiHostOrchestratorWaitForAPIsPreservesPartialReadiness(t *testing.T) {
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
	orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{
		{Host: "127.0.0.1", Original: "ready"},
		{Host: "127.0.0.2", Original: "unavailable"},
	}, 1)
	orchestrator.apiPort = port
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusRunning)
	}
	if err = orchestrator.WaitForAPIs(context.Background(), 100*time.Millisecond); err != nil {
		t.Fatalf("partial readiness should preserve the ready participant: %v", err)
	}
	if orchestrator.hosts[0].APIClient == nil || orchestrator.hosts[1].APIClient != nil {
		t.Fatalf("partial readiness clients = ready:%p unavailable:%p",
			orchestrator.hosts[0].APIClient, orchestrator.hosts[1].APIClient)
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

func TestMultiHostOrchestratorStopFailureRetainsOwnershipAndRetryClearsIt(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: "host1", Original: "host1"}}, 1)
	host := orchestrator.hosts[0]
	manager := NewMockDockerManager()
	manager.SetContainerID("container-1")
	manager.SetFailOnCleanup(true)
	host.DockerManager = manager
	host.ContainerID = "container-1"
	host.SetStatus(HostStatusRunning)
	host.SetManaged(true)

	if err := orchestrator.StopAllContainers(context.Background()); err == nil {
		t.Fatal("StopAllContainers() error = nil, want cleanup failure")
	}
	if !host.IsManaged() || host.GetStatus() != HostStatusRunning ||
		host.ContainerID != "container-1" || manager.ContainerID() != "container-1" {
		t.Fatalf("failed cleanup disowned live resource: managed=%t status=%s hostID=%q managerID=%q",
			host.IsManaged(), host.GetStatus(), host.ContainerID, manager.ContainerID())
	}

	manager.SetFailOnCleanup(false)
	if err := orchestrator.StopAllContainers(context.Background()); err != nil {
		t.Fatalf("StopAllContainers() retry error = %v", err)
	}
	if host.IsManaged() || host.GetStatus() != HostStatusStopped || host.ContainerID != "" {
		t.Fatalf("successful retry retained ownership: managed=%t status=%s id=%q",
			host.IsManaged(), host.GetStatus(), host.ContainerID)
	}
}

func TestCleanupManagedContainersAfterStartFailureRetainsFailedHostForRetry(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: "host1", Original: "host1"}}, 1)
	host := orchestrator.hosts[0]
	manager := NewMockDockerManager()
	manager.SetContainerID("container-1")
	manager.SetFailOnCleanup(true)
	host.DockerManager = manager
	host.ContainerID = "container-1"
	host.SetStatus(HostStatusRunning)
	host.SetManaged(true)

	if err := orchestrator.cleanupManagedContainersAfterStartFailure(context.Background()); err == nil {
		t.Fatal("pre-submission cleanup error = nil, want failure")
	}
	if !host.IsManaged() || host.ContainerID == "" {
		t.Fatalf("failed pre-submission cleanup lost retry ownership: managed=%t id=%q", host.IsManaged(), host.ContainerID)
	}
	manager.SetFailOnCleanup(false)
	if err := orchestrator.cleanupManagedContainersAfterStartFailure(context.Background()); err != nil {
		t.Fatalf("pre-submission cleanup retry error = %v", err)
	}
	if host.IsManaged() || host.ContainerID != "" || host.GetStatus() != HostStatusStopped {
		t.Fatalf("successful pre-submission retry retained host: managed=%t id=%q status=%s",
			host.IsManaged(), host.ContainerID, host.GetStatus())
	}
}

type mountFailureDockerManager struct {
	*MockDockerManager
	mountErr     error
	stagingOwned bool
}

func (m *mountFailureDockerManager) SetFileMounts([]scenario.FileMount) error {
	return m.mountErr
}

func (m *mountFailureDockerManager) CleanupContext(ctx context.Context) error {
	if m.failOnCleanup {
		return errors.New("mock error: failed to cleanup remote staging")
	}
	m.stagingOwned = false
	return m.MockDockerManager.CleanupContext(ctx)
}

func (m *mountFailureDockerManager) HasManagedResources() bool {
	return m.stagingOwned || m.MockDockerManager.HasManagedResources()
}

func TestItemStagingFailureRetainsManagedOwnershipWithoutContainerID(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: "host1", Original: "host1"}}, 1)
	host := orchestrator.hosts[0]
	manager := &mountFailureDockerManager{
		MockDockerManager: NewMockDockerManager(),
		mountErr:          errors.New("remote staging and cleanup failed"),
		stagingOwned:      true,
	}
	manager.SetContainerID("")
	manager.SetFailOnCleanup(true)
	host.DockerManager = manager
	host.SetStatus(HostStatusReady)
	orchestrator.itemFileMounts = []scenario.FileMount{{
		HostPath: "/local/items.csv", ContainerPath: "/spt-input/items.csv",
	}}

	err := orchestrator.StartContainers(context.Background(), "test-image", nil)
	if err == nil || !strings.Contains(err.Error(), "remote staging and cleanup failed") {
		t.Fatalf("StartContainers() error = %v, want staging failure", err)
	}
	if !host.IsManaged() || host.ContainerID != "" || manager.ContainerID() != "" {
		t.Fatalf("staging-only cleanup failure lost ownership: managed=%t hostID=%q managerID=%q",
			host.IsManaged(), host.ContainerID, manager.ContainerID())
	}

	manager.SetFailOnCleanup(false)
	if err = orchestrator.StopAllContainers(context.Background()); err != nil {
		t.Fatalf("StopAllContainers() staging retry error = %v", err)
	}
	if host.IsManaged() || host.GetStatus() != HostStatusStopped {
		t.Fatalf("successful staging retry retained ownership: managed=%t status=%s",
			host.IsManaged(), host.GetStatus())
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

type orderedDiagnosticsDockerManager struct {
	*MockDockerManager

	mu     sync.Mutex
	events []string
	record *diagnosticsRecord
}

func newOrderedDiagnosticsDockerManager() *orderedDiagnosticsDockerManager {
	return &orderedDiagnosticsDockerManager{MockDockerManager: NewMockDockerManager()}
}

func (m *orderedDiagnosticsDockerManager) Cleanup() error {
	return m.CleanupContext(context.Background())
}

func (m *orderedDiagnosticsDockerManager) CleanupContext(ctx context.Context) error {
	m.appendEvent("cleanup")
	m.record = &diagnosticsRecord{
		Host:        "host1",
		Role:        constants.DockerRoleWorker,
		Collected:   true,
		ContainerID: "container-1",
	}
	return m.MockDockerManager.CleanupContext(ctx)
}

func (m *orderedDiagnosticsDockerManager) gracefulStopForDiagnostics(context.Context) error {
	m.appendEvent("stop")
	return nil
}

func (m *orderedDiagnosticsDockerManager) collectDiagnostics(context.Context) (*diagnosticsRecord, error) {
	m.appendEvent("collect")
	return m.record, nil
}

func (m *orderedDiagnosticsDockerManager) diagnosticsRecord() *diagnosticsRecord {
	m.appendEvent("record")
	return m.record
}

func (m *orderedDiagnosticsDockerManager) appendEvent(event string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
}

func (m *orderedDiagnosticsDockerManager) eventList() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.events...)
}

func TestMultiHostOrchestrator_StopAllContainers_DiagnosticsAfterCleanup(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{{Host: "host1", Original: "host1"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	h := orchestrator.hosts[0]
	h.SetStatus(HostStatusRunning)
	h.ContainerID = "container-1"
	h.DockerManager = newOrderedDiagnosticsDockerManager()
	h.SetManaged(true)

	if err := orchestrator.StopAllContainers(context.Background()); err != nil {
		t.Fatalf("StopAllContainers() error = %v", err)
	}

	got := strings.Join(h.DockerManager.(*orderedDiagnosticsDockerManager).eventList(), ",")
	if got != "cleanup,record" {
		t.Fatalf("diagnostics order = %q, want cleanup,record", got)
	}
}

type diagnosticsOnlyCleanupErrorManager struct {
	*MockDockerManager
}

func (m *diagnosticsOnlyCleanupErrorManager) CleanupContext(ctx context.Context) error {
	if err := m.MockDockerManager.CleanupContext(ctx); err != nil {
		return err
	}
	return errors.New("copy diagnostics: permission denied")
}

func TestStopAllContainersClearsOwnershipAfterDiagnosticsOnlyError(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: "host1", Original: "host1"}}, 1)
	host := orchestrator.hosts[0]
	manager := &diagnosticsOnlyCleanupErrorManager{MockDockerManager: NewMockDockerManager()}
	manager.SetContainerID("container-1")
	host.DockerManager = manager
	host.ContainerID = "container-1"
	host.SetStatus(HostStatusRunning)
	host.SetManaged(true)

	err := orchestrator.StopAllContainers(context.Background())
	if err == nil || !strings.Contains(err.Error(), "copy diagnostics") {
		t.Fatalf("StopAllContainers() error = %v, want diagnostics evidence error", err)
	}
	if host.IsManaged() || host.ContainerID != "" || host.GetStatus() != HostStatusStopped ||
		manager.HasManagedResources() {
		t.Fatalf("diagnostics-only error retained false ownership: managed=%t hostID=%q status=%s resources=%t",
			host.IsManaged(), host.ContainerID, host.GetStatus(), manager.HasManagedResources())
	}
}

type blockingDiagnosticsDockerManager struct {
	*MockDockerManager

	host    string
	started chan<- string
	release <-chan struct{}
}

func (m *blockingDiagnosticsDockerManager) gracefulStopForDiagnostics(context.Context) error {
	return nil
}

func (m *blockingDiagnosticsDockerManager) collectDiagnostics(ctx context.Context) (*diagnosticsRecord, error) {
	select {
	case m.started <- m.host:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case <-m.release:
		return &diagnosticsRecord{
			Host:      m.host,
			Role:      constants.DockerRoleWorker,
			Collected: true,
		}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (m *blockingDiagnosticsDockerManager) diagnosticsRecord() *diagnosticsRecord {
	return nil
}

func TestMultiHostOrchestrator_CollectDiagnosticsRunsHostsConcurrently(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", Original: "host1"},
		{Host: "host2", Original: "host2"},
	}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	orchestrator.SetResultsRoot(t.TempDir())

	started := make(chan string, len(hostInfos))
	release := make(chan struct{})
	var releaseOnce sync.Once
	for _, host := range orchestrator.hosts {
		host.SetManaged(true)
		host.DockerManager = &blockingDiagnosticsDockerManager{
			MockDockerManager: NewMockDockerManager(),
			host:              host.Info.Original,
			started:           started,
			release:           release,
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- orchestrator.CollectDiagnostics(ctx)
	}()

	for range hostInfos {
		select {
		case <-started:
		case <-time.After(500 * time.Millisecond):
			releaseOnce.Do(func() { close(release) })
			t.Fatal("CollectDiagnostics did not start all host collectors concurrently")
		}
	}
	releaseOnce.Do(func() { close(release) })

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("CollectDiagnostics() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("CollectDiagnostics did not complete after host collectors were released")
	}
}

type cancellationDrainDiagnosticsManager struct {
	*MockDockerManager
	host      string
	wait      bool
	started   chan<- struct{}
	startOnce sync.Once
}

func (*cancellationDrainDiagnosticsManager) gracefulStopForDiagnostics(context.Context) error {
	return nil
}

func (m *cancellationDrainDiagnosticsManager) collectDiagnostics(ctx context.Context) (*diagnosticsRecord, error) {
	if m.wait {
		m.startOnce.Do(func() { m.started <- struct{}{} })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &diagnosticsRecord{Host: m.host, Role: constants.DockerRoleWorker, Collected: true}, nil
}

func (*cancellationDrainDiagnosticsManager) diagnosticsRecord() *diagnosticsRecord { return nil }

func TestCollectDiagnosticsCancellationDrainsAvailableRecords(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{
		{Host: "ready", Original: "ready"},
		{Host: "canceled", Original: "canceled"},
	}, 2)
	root := t.TempDir()
	orchestrator.SetResultsRoot(root)
	started := make(chan struct{}, 1)
	for index, host := range orchestrator.hosts {
		host.SetManaged(true)
		host.DockerManager = &cancellationDrainDiagnosticsManager{
			MockDockerManager: NewMockDockerManager(),
			host:              host.Info.Original,
			wait:              index == 1,
			started:           started,
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- orchestrator.CollectDiagnostics(ctx) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("cancelable diagnostics worker did not start")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("CollectDiagnostics() error = %v, want context.Canceled", err)
	}
	data, err := os.ReadFile(filepath.Join(root, dockerDiagnosticsResultsDirName, dockerDiagnosticsManifestFileName))
	if err != nil {
		t.Fatalf("partial diagnostics manifest was not written: %v", err)
	}
	var manifest diagnosticsManifest
	if err = json.Unmarshal(data, &manifest); err != nil {
		t.Fatal(err)
	}
	if len(manifest.Records) != 1 || manifest.Records[0].Host != "ready" {
		t.Fatalf("partial diagnostics records = %+v, want completed host", manifest.Records)
	}
}

type collectDiagnosticsOrderDockerManager struct {
	*MockDockerManager

	mu      sync.Mutex
	events  []string
	stopErr error
}

func (m *collectDiagnosticsOrderDockerManager) gracefulStopForDiagnostics(context.Context) error {
	m.mu.Lock()
	m.events = append(m.events, "stop")
	m.mu.Unlock()
	return m.stopErr
}

func (m *collectDiagnosticsOrderDockerManager) collectDiagnostics(context.Context) (*diagnosticsRecord, error) {
	m.mu.Lock()
	m.events = append(m.events, "collect")
	m.mu.Unlock()
	return &diagnosticsRecord{Host: "host1", Collected: true}, nil
}

func (m *collectDiagnosticsOrderDockerManager) diagnosticsRecord() *diagnosticsRecord {
	return nil
}

func (m *collectDiagnosticsOrderDockerManager) eventList() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.events...)
}

// TestMultiHostOrchestrator_CollectDiagnosticsStopsBeforeCollecting guards against
// regressing to collecting JFR/GC artifacts (dumponexit) while the container
// (and therefore the JVM) may still be running: unlike StopAllContainers/Cleanup,
// this standalone diagnostics-only pass has nothing else in its call path that
// stops the container first, so CollectDiagnostics must do it itself.
func TestMultiHostOrchestrator_CollectDiagnosticsStopsBeforeCollecting(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{{Host: "host1", Original: "host1"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	orchestrator.SetResultsRoot(t.TempDir())
	fake := &collectDiagnosticsOrderDockerManager{MockDockerManager: NewMockDockerManager()}
	orchestrator.hosts[0].SetManaged(true)
	orchestrator.hosts[0].DockerManager = fake

	if err := orchestrator.CollectDiagnostics(context.Background()); err != nil {
		t.Fatalf("CollectDiagnostics() error = %v", err)
	}

	got := strings.Join(fake.eventList(), ",")
	if got != "stop,collect" {
		t.Fatalf("diagnostics order = %q, want stop,collect", got)
	}
}

// TestMultiHostOrchestrator_CollectDiagnosticsStillCollectsWhenStopFails covers
// the best-effort path: a stop failure (e.g. a transient SSH error) must not
// abandon collection outright, since the remote/local diagnostics files may
// still be readable even if we can't confirm the container fully stopped.
func TestMultiHostOrchestrator_CollectDiagnosticsStillCollectsWhenStopFails(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{{Host: "host1", Original: "host1"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	orchestrator.SetResultsRoot(t.TempDir())
	fake := &collectDiagnosticsOrderDockerManager{
		MockDockerManager: NewMockDockerManager(),
		stopErr:           fmt.Errorf("ssh timeout"),
	}
	orchestrator.hosts[0].SetManaged(true)
	orchestrator.hosts[0].DockerManager = fake

	if err := orchestrator.CollectDiagnostics(context.Background()); err != nil {
		t.Fatalf("CollectDiagnostics() error = %v", err)
	}

	got := strings.Join(fake.eventList(), ",")
	if got != "stop,collect" {
		t.Fatalf("diagnostics order = %q, want stop,collect even when stop failed (best-effort collection)", got)
	}
}

type finalizeOrderDockerManager struct {
	*MockDockerManager

	mu     sync.Mutex
	events []string
}

func (m *finalizeOrderDockerManager) gracefulStopForDiagnostics(context.Context) error {
	m.record("stop")
	return nil
}

func (m *finalizeOrderDockerManager) collectDiagnostics(context.Context) (*diagnosticsRecord, error) {
	m.record("collect")
	return &diagnosticsRecord{Host: "host1", Collected: true}, nil
}

func (m *finalizeOrderDockerManager) diagnosticsRecord() *diagnosticsRecord {
	return nil
}

func (m *finalizeOrderDockerManager) Cleanup() error {
	return m.CleanupContext(context.Background())
}

func (m *finalizeOrderDockerManager) CleanupContext(ctx context.Context) error {
	m.record("cleanup")
	return m.MockDockerManager.CleanupContext(ctx)
}

func (m *finalizeOrderDockerManager) record(event string) {
	m.mu.Lock()
	m.events = append(m.events, event)
	m.mu.Unlock()
}

func (m *finalizeOrderDockerManager) eventList() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.events...)
}

func newFinalizeTestOrchestrator(t *testing.T) (*MultiHostOrchestrator, *finalizeOrderDockerManager) {
	t.Helper()
	hostInfos := []*hostparse.HostInfo{{Host: "host1", Original: "host1"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	orchestrator.SetResultsRoot(t.TempDir())
	h := orchestrator.hosts[0]
	h.SetStatus(HostStatusRunning)
	h.ContainerID = "container-1"
	fake := &finalizeOrderDockerManager{MockDockerManager: NewMockDockerManager()}
	h.DockerManager = fake
	h.SetManaged(true)
	return orchestrator, fake
}

// TestMultiHostOrchestrator_FinalizeDiagnosticsAndCleanup_CollectsBeforeStopping
// covers the canonical end-of-run sequence: diagnostics must be collected
// (which itself stops the container first, see gracefulStopForDiagnostics)
// before the container is fully removed and staging cleaned up.
func TestMultiHostOrchestrator_FinalizeDiagnosticsAndCleanup_CollectsBeforeStopping(t *testing.T) {
	orchestrator, fake := newFinalizeTestOrchestrator(t)

	if err := orchestrator.FinalizeDiagnosticsAndCleanup(context.Background()); err != nil {
		t.Fatalf("FinalizeDiagnosticsAndCleanup() error = %v", err)
	}

	got := strings.Join(fake.eventList(), ",")
	if got != "stop,collect,cleanup" {
		t.Fatalf("finalize order = %q, want stop,collect,cleanup", got)
	}
}

// TestMultiHostOrchestrator_FinalizeDiagnosticsAndCleanup_RunsOnceConcurrently
// guards the concurrency hazard this method exists to prevent: the
// auto-results pre-summary hook and a caller-side timeout fallback could
// both try to finalize the same run at once (e.g. a slow diagnostics copy
// still running when the outer wait expires). Only one of them must
// actually do the work.
func TestMultiHostOrchestrator_FinalizeDiagnosticsAndCleanup_RunsOnceConcurrently(t *testing.T) {
	orchestrator, fake := newFinalizeTestOrchestrator(t)

	const callers = 5
	var wg sync.WaitGroup
	errs := make([]error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = orchestrator.FinalizeDiagnosticsAndCleanup(context.Background())
		}(i)
	}
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("call %d error = %v, want nil", i, err)
		}
	}

	cleanupCount := 0
	for _, event := range fake.eventList() {
		if event == "cleanup" {
			cleanupCount++
		}
	}
	if cleanupCount != 1 {
		t.Fatalf("cleanup ran %d times across %d concurrent callers, want exactly 1 (events: %v)",
			cleanupCount, callers, fake.eventList())
	}
}

func TestMultiHostOrchestratorFinalizeDiagnosticsRetriesFailedCleanup(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: "host1", Original: "host1"}}, 1)
	host := orchestrator.hosts[0]
	manager := NewMockDockerManager()
	manager.SetContainerID("container-1")
	manager.SetFailOnCleanup(true)
	host.DockerManager = manager
	host.ContainerID = "container-1"
	host.SetStatus(HostStatusRunning)
	host.SetManaged(true)

	if err := orchestrator.FinalizeDiagnosticsAndCleanup(context.Background()); err == nil {
		t.Fatal("first finalization error = nil, want cleanup failure")
	}
	if !host.IsManaged() || host.ContainerID == "" {
		t.Fatalf("failed finalization lost cleanup ownership: managed=%t id=%q", host.IsManaged(), host.ContainerID)
	}
	manager.SetFailOnCleanup(false)
	if err := orchestrator.FinalizeDiagnosticsAndCleanup(context.Background()); err != nil {
		t.Fatalf("finalization retry error = %v", err)
	}
	if manager.GetCleanupCallCount() != 2 || host.IsManaged() || host.ContainerID != "" {
		t.Fatalf("retry state: calls=%d managed=%t id=%q",
			manager.GetCleanupCallCount(), host.IsManaged(), host.ContainerID)
	}
	if err := orchestrator.FinalizeDiagnosticsAndCleanup(context.Background()); err != nil {
		t.Fatalf("successful finalization should remain sticky: %v", err)
	}
	if manager.GetCleanupCallCount() != 2 {
		t.Fatalf("successful finalization reran cleanup: %d calls", manager.GetCleanupCallCount())
	}
}

type slowFailedCleanupManager struct {
	*MockDockerManager
	started chan struct{}
	release chan struct{}

	mu        sync.Mutex
	calls     int
	active    int
	maxActive int
}

type phaseBudgetDockerManager struct {
	*MockDockerManager
	diagnosticsReturned chan struct{}
	cleanupStarted      chan struct{}
	normalCleanupCalls  atomic.Int32
	resourceOnlyCalls   atomic.Int32
	record              *diagnosticsRecord
}

func (m *phaseBudgetDockerManager) gracefulStopForDiagnostics(context.Context) error {
	return nil
}

func (m *phaseBudgetDockerManager) collectDiagnostics(ctx context.Context) (*diagnosticsRecord, error) {
	<-ctx.Done()
	m.record = &diagnosticsRecord{
		Host: "host1", Role: constants.DockerRoleWorker,
		ContainerID: "container-1", Error: ctx.Err().Error(),
	}
	close(m.diagnosticsReturned)
	return m.record, ctx.Err()
}

func (m *phaseBudgetDockerManager) diagnosticsRecord() *diagnosticsRecord {
	return m.record
}

func (m *phaseBudgetDockerManager) CleanupContext(ctx context.Context) error {
	m.normalCleanupCalls.Add(1)
	return m.MockDockerManager.CleanupContext(ctx)
}

func (m *phaseBudgetDockerManager) CleanupResourcesContext(ctx context.Context) error {
	m.resourceOnlyCalls.Add(1)
	close(m.cleanupStarted)
	return m.MockDockerManager.CleanupContext(ctx)
}

func TestFinalizeDiagnosticsTimeoutStillRunsMandatoryRemovalWithFreshBudget(t *testing.T) {
	resultsRoot := t.TempDir()
	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: "host1", Original: "host1"}}, 1)
	orchestrator.SetResultsRoot(resultsRoot)
	orchestrator.diagnosticsTimeout = 10 * time.Millisecond
	orchestrator.cleanupTimeout = time.Second
	manager := &phaseBudgetDockerManager{
		MockDockerManager:   NewMockDockerManager(),
		diagnosticsReturned: make(chan struct{}),
		cleanupStarted:      make(chan struct{}),
	}
	manager.SetContainerID("container-1")
	host := orchestrator.hosts[0]
	host.DockerManager = manager
	host.ContainerID = "container-1"
	host.SetStatus(HostStatusRunning)
	host.SetManaged(true)

	err := orchestrator.FinalizeDiagnosticsAndCleanup(context.Background())
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("FinalizeDiagnosticsAndCleanup() error = %v, want diagnostics deadline", err)
	}
	select {
	case <-manager.diagnosticsReturned:
	default:
		t.Fatal("timed-out diagnostics collector was not joined")
	}
	select {
	case <-manager.cleanupStarted:
	default:
		t.Fatal("mandatory removal did not start after diagnostics timeout")
	}
	if manager.normalCleanupCalls.Load() != 0 || manager.resourceOnlyCalls.Load() != 1 {
		t.Fatalf("cleanup calls normal=%d resource_only=%d, want 0/1",
			manager.normalCleanupCalls.Load(), manager.resourceOnlyCalls.Load())
	}
	if host.IsManaged() || manager.HasManagedResources() {
		t.Fatal("mandatory removal retained container ownership")
	}
	manifestPath := filepath.Join(resultsRoot, dockerDiagnosticsResultsDirName, dockerDiagnosticsManifestFileName)
	if _, statErr := os.Stat(manifestPath); statErr != nil {
		t.Fatalf("partial diagnostics evidence was not retained: %v", statErr)
	}
	if retryErr := orchestrator.FinalizeDiagnosticsAndCleanup(context.Background()); retryErr != nil {
		t.Fatalf("post-timeout retry error = %v", retryErr)
	}
	if manager.resourceOnlyCalls.Load() != 1 {
		t.Fatalf("cleanup retry overlapped or repeated removal: %d calls", manager.resourceOnlyCalls.Load())
	}
}

func (m *slowFailedCleanupManager) CleanupContext(ctx context.Context) error {
	m.mu.Lock()
	m.calls++
	call := m.calls
	m.active++
	if m.active > m.maxActive {
		m.maxActive = m.active
	}
	m.mu.Unlock()
	defer func() {
		m.mu.Lock()
		m.active--
		m.mu.Unlock()
	}()
	if call == 1 {
		close(m.started)
		<-m.release
		return errors.New("first cleanup failed after slow unwind")
	}
	return m.MockDockerManager.CleanupContext(ctx)
}

func (m *slowFailedCleanupManager) counts() (calls, maxActive int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls, m.maxActive
}

func TestFinalizeDiagnosticsCanceledWaiterDoesNotExposeOverlappingRetry(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: "host1", Original: "host1"}}, 1)
	host := orchestrator.hosts[0]
	manager := &slowFailedCleanupManager{
		MockDockerManager: NewMockDockerManager(),
		started:           make(chan struct{}),
		release:           make(chan struct{}),
	}
	manager.SetContainerID("container-1")
	host.DockerManager = manager
	host.ContainerID = "container-1"
	host.SetStatus(HostStatusRunning)
	host.SetManaged(true)

	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan error, 1)
	go func() { firstDone <- orchestrator.FinalizeDiagnosticsAndCleanup(firstCtx) }()
	select {
	case <-manager.started:
	case <-time.After(time.Second):
		t.Fatal("first cleanup did not start")
	}
	cancelFirst()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("first waiter error = %v, want context.Canceled", err)
	}

	joinedDone := make(chan error, 1)
	go func() { joinedDone <- orchestrator.FinalizeDiagnosticsAndCleanup(context.Background()) }()
	time.Sleep(25 * time.Millisecond)
	if calls, maxActive := manager.counts(); calls != 1 || maxActive != 1 {
		t.Fatalf("retry started before prior workers quiesced: calls=%d max_active=%d", calls, maxActive)
	}
	close(manager.release)
	if err := <-joinedDone; err == nil || !strings.Contains(err.Error(), "slow unwind") {
		t.Fatalf("joined attempt error = %v, want first cleanup failure", err)
	}
	if err := orchestrator.FinalizeDiagnosticsAndCleanup(context.Background()); err != nil {
		t.Fatalf("retry after quiescence failed: %v", err)
	}
	if calls, maxActive := manager.counts(); calls != 2 || maxActive != 1 {
		t.Fatalf("cleanup retry state: calls=%d max_active=%d, want 2/1", calls, maxActive)
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
	result := orchestrator.checkPortContext(context.Background(), "127.0.0.1", 65432, 1*time.Second)
	if result {
		t.Error("Expected checkPort to return false for closed port, got true")
	}

	// Test with invalid host
	result = orchestrator.checkPortContext(context.Background(), "invalid-host-that-does-not-exist", 80, 1*time.Second)
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
	if orchestrator.checkPortContext(context.Background(), "127.0.0.1", constants.RMIRegistryPortInt, 100*time.Millisecond) {
		t.Skip("skipping: RMI port 1099 is open on localhost")
	}

	// Create a host connection that doesn't have RMI running
	host := &HostConnection{
		Info:   hostInfos[0],
		Status: HostStatusReady,
	}

	// Test with very short timeout to ensure it times out quickly
	err := orchestrator.waitForRMIReadyContext(context.Background(), host, 2*time.Second)
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
	err := orchestrator.waitForRMIReadyContext(context.Background(), host, 3*time.Second)
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
	err := orchestrator.waitForAllWorkersReadyContext(context.Background(), []*HostConnection{}, 5*time.Second)
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
	err := orchestrator.waitForAllWorkersReadyContext(context.Background(), workers, 2*time.Second)
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
	if err := o.startEntryNode(primary, []*HostConnection{worker}, params, nil); err != nil {
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
	if calls[0].NetworkMode != constants.DefaultNetworkMode {
		t.Fatalf("entry node network mode = %q, want %q", calls[0].NetworkMode, constants.DefaultNetworkMode)
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
	err := orchestrator.waitForAllWorkersReadyContext(context.Background(), workers, 2*time.Second)
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
	err := orchestrator.waitForAllWorkersReadyContext(context.Background(), workers, 1*time.Second)
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
	var postedScenario string
	var postedDefaults string
	srv := newSingleHostReplayAPIServer(t, &postedScenario, &postedDefaults)
	defer srv.Close()
	host, port := splitServerHostPort(t, srv.URL)

	hostInfos := []*hostparse.HostInfo{
		{Host: host, IsLocal: false, Original: "qa-client-01"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	orchestrator.SetAPIPort(port)
	wrapper := NewMultiHostTestOrchestrator(orchestrator)

	// Set up host as ready with mock Docker manager
	orchestrator.hosts[0].SetStatus(HostStatusReady)
	mockDocker := NewMockDockerManager()
	orchestrator.hosts[0].DockerManager = mockDocker

	// Set up callbacks to capture status updates
	var statusUpdate *TestStatus
	var outputMessages []string

	wrapper.SetCallbacks(
		func(status *TestStatus) { statusUpdate = status },
		func(update *MultiNodeMetricsUpdate) {},
		func(line string) { outputMessages = append(outputMessages, line) },
		func(err string) {},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	params := scenario.ScenarioParams{WorkloadType: "mock", Threads: 1, ObjectSize: "1MB"}
	var launchSubmitted atomic.Bool
	hooks := LaunchHooks{OnSubmitted: func() { launchSubmitted.Store(true) }}

	err := wrapper.StartTestWithLaunchHooks(ctx, "test-image", params, hooks)
	if err != nil {
		t.Errorf("Expected StartTest to succeed for single host, got: %v", err)
	}
	defer func() { _ = wrapper.StopTest() }()

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
	if !containsStringSimple(strings.Join(outputMessages, "\n"), "Starting single-host test") {
		t.Errorf("Expected single-host output message, got: %v", outputMessages)
	}

	containerCalls := mockDocker.GetContainerCalls()
	if len(containerCalls) != 1 {
		t.Fatalf("expected one node-mode container start, got %d", len(containerCalls))
	}
	if !containsStringSimple(strings.Join(containerCalls[0].Cmd, " "), "--run-port="+port) {
		t.Fatalf("node-mode container did not use API port %s: %v", port, containerCalls[0].Cmd)
	}
	if !orchestrator.hosts[0].IsManaged() {
		t.Fatal("expected single-host container to be marked managed")
	}
	if postedScenario == "" {
		t.Fatal("expected generated scenario to be posted to /run")
	}
	if !launchSubmitted.Load() {
		t.Fatal("expected successful single-host POST to notify launch submission")
	}
	if !strings.Contains(postedDefaults, "dummy-mock") {
		t.Fatalf("expected generated defaults to include mock driver, got:\n%s", postedDefaults)
	}
}

func TestMultiHostTestOrchestrator_StartTestWithContent_SingleHostPostsProvidedArtifacts(t *testing.T) {
	var postedScenario string
	var postedDefaults string
	srv := newSingleHostReplayAPIServer(t, &postedScenario, &postedDefaults)
	defer srv.Close()
	host, port := splitServerHostPort(t, srv.URL)

	hostInfos := []*hostparse.HostInfo{
		{Host: host, IsLocal: false, Original: "qa-client-01"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	orchestrator.SetAPIPort(port)
	orchestrator.hosts[0].SetStatus(HostStatusReady)
	mockDocker := NewMockDockerManager()
	orchestrator.hosts[0].DockerManager = mockDocker

	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	wrapper.SetCallbacks(
		func(*TestStatus) {},
		func(*MultiNodeMetricsUpdate) {},
		func(string) {},
		func(string) {},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replayScenario := []byte(`var step = "replay-001-max-w10kb"; Load.config({}).run();`)
	replayDefaults := []byte("storage:\n  driver:\n    type: dummy-mock\n")
	params := scenario.ScenarioParams{WorkloadType: "replay", Threads: 1}
	if err := wrapper.StartTestWithContent(ctx, "test-image", params, replayScenario, replayDefaults); err != nil {
		t.Fatalf("StartTestWithContent() error = %v", err)
	}
	defer func() { _ = wrapper.StopTest() }()

	if postedScenario != string(replayScenario) {
		t.Fatalf("posted scenario mismatch:\n%s", postedScenario)
	}
	if postedDefaults != string(replayDefaults) {
		t.Fatalf("posted defaults mismatch:\n%s", postedDefaults)
	}
	containerCalls := mockDocker.GetContainerCalls()
	if len(containerCalls) != 1 {
		t.Fatalf("expected one node-mode container start, got %d", len(containerCalls))
	}
	if !containsStringSimple(strings.Join(containerCalls[0].Cmd, " "), "--run-port="+port) {
		t.Fatalf("node-mode container did not use API port %s: %v", port, containerCalls[0].Cmd)
	}
	if orchestrator.hosts[0].APIClient == nil {
		t.Fatal("expected API client to be attached after readiness")
	}
	if !orchestrator.hosts[0].IsManaged() {
		t.Fatal("expected single-host replay container to be marked managed")
	}
}

func TestDistributedIntegrityImageIdentityRecordsMatchingFleet(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "entry", Original: "entry"},
		{Host: "worker", Original: "worker"},
	}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusReady)
	}
	id := "sha256:" + strings.Repeat("a", 64)
	payloadCalls := &atomic.Int64{}
	orchestrator.preflight = identityPreflight{identities: map[string]preflight.ImageIdentity{
		"entry":  {ID: id, RepoDigests: []string{"repo/spt@sha256:one"}},
		"worker": {ID: id},
	}, payloadCalls: payloadCalls}
	var recorded *DistributedRuntimeIdentityEvidence
	orchestrator.SetRuntimeIdentityRecorder(func(evidence DistributedRuntimeIdentityEvidence) {
		recorded = &evidence
	})

	evidence, err := orchestrator.verifyDistributedIntegrityImageIdentity(context.Background(), "repo/spt:test")
	if err != nil {
		t.Fatal(err)
	}
	if evidence.ImageID != id || evidence.Tier != distributedRuntimeIdentityTierImmutableImage || len(evidence.Participants) != 2 {
		t.Fatalf("unexpected evidence: %+v", evidence)
	}
	if recorded == nil || recorded.ImageID != id {
		t.Fatalf("runtime identity recorder received %+v", recorded)
	}
	if payloadCalls.Load() != 0 {
		t.Fatalf("image tier made %d payload probes, want 0", payloadCalls.Load())
	}
}

func TestDistributedIntegrityImageIdentityRecordsSingleRemoteParticipant(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{{Host: "entry", Original: "entry"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
	orchestrator.hosts[0].SetStatus(HostStatusReady)
	id := "sha256:" + strings.Repeat("a", 64)
	orchestrator.preflight = identityPreflight{identities: map[string]preflight.ImageIdentity{
		"entry": {ID: id, RepoDigests: []string{"repo/spt@sha256:one"}},
	}}
	var recorded *DistributedRuntimeIdentityEvidence
	orchestrator.SetRuntimeIdentityRecorder(func(evidence DistributedRuntimeIdentityEvidence) {
		recorded = &evidence
	})

	evidence, err := orchestrator.PrepareDistributedIntegrityRuntimeIdentity(
		context.Background(), "repo/spt:test")
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Tier != distributedRuntimeIdentityTierImmutableImage || evidence.ImageID != id ||
		len(evidence.Participants) != 1 {
		t.Fatalf("unexpected evidence: %+v", evidence)
	}
	if recorded == nil || recorded.ImageID != id {
		t.Fatalf("runtime identity recorder received %+v", recorded)
	}
}

func TestDistributedIntegrityImageIdentityFailureStopsSingleRemoteParticipant(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: "entry", Original: "entry"}}, 1)
	orchestrator.hosts[0].SetStatus(HostStatusReady)
	orchestrator.preflight = identityPreflight{errors: map[string]error{
		"entry": errors.New("image identity unavailable"),
	}}

	_, err := orchestrator.PrepareDistributedIntegrityRuntimeIdentity(
		context.Background(), "repo/spt:test")
	if err == nil || !strings.Contains(err.Error(), "entry") ||
		!strings.Contains(err.Error(), "image identity unavailable") {
		t.Fatalf("single-remote identity error = %v", err)
	}
}

func TestDistributedIntegrityPayloadIdentityRecordsMatchingFleet(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{{Host: "entry", Original: "entry"}, {Host: "worker", Original: "worker"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusReady)
	}
	id := "sha256:" + strings.Repeat("a", 64)
	payload := strings.Repeat("b", 64)
	payloadCalls := &atomic.Int64{}
	orchestrator.preflight = identityPreflight{
		identities:   map[string]preflight.ImageIdentity{"entry": {ID: id}, "worker": {ID: id}},
		payloads:     map[string]string{"entry": payload, "worker": payload},
		payloadCalls: payloadCalls,
	}
	orchestrator.SetIntegrityRuntimeIdentityTier(constants.IntegrityRuntimeIdentityTierPayload)

	evidence, err := orchestrator.PrepareDistributedIntegrityRuntimeIdentity(context.Background(), "repo/spt:test")
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Tier != distributedRuntimeIdentityTierImageAndPayload || evidence.PayloadSHA256 != payload {
		t.Fatalf("unexpected payload evidence: %+v", evidence)
	}
	for _, participant := range evidence.Participants {
		if participant.PayloadSHA256 != payload {
			t.Fatalf("participant payload evidence = %+v", participant)
		}
	}
	if _, err := orchestrator.PrepareDistributedIntegrityRuntimeIdentity(context.Background(), "repo/spt:test"); err != nil {
		t.Fatal(err)
	}
	if payloadCalls.Load() != 2 {
		t.Fatalf("payload probes = %d after cached second prepare, want 2", payloadCalls.Load())
	}
}

func TestDistributedIntegrityPayloadMismatchStopsBeforeContainerStart(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{{Host: "entry", Original: "entry"}, {Host: "worker", Original: "worker"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	entryDM, workerDM := NewMockDockerManager(), NewMockDockerManager()
	orchestrator.hosts[0].DockerManager = entryDM
	orchestrator.hosts[1].DockerManager = workerDM
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusReady)
	}
	id := "sha256:" + strings.Repeat("a", 64)
	orchestrator.preflight = identityPreflight{
		identities: map[string]preflight.ImageIdentity{"entry": {ID: id}, "worker": {ID: id}},
		payloads:   map[string]string{"entry": strings.Repeat("b", 64), "worker": strings.Repeat("c", 64)},
	}
	orchestrator.SetIntegrityRuntimeIdentityTier(constants.IntegrityRuntimeIdentityTierPayload)

	err := orchestrator.StartDistributedTestWithContent(
		context.Background(), "repo/spt:test",
		scenario.ScenarioParams{WorkloadType: scenario.WorkloadTypeWriteVerify}, []byte(`Load.run({})`),
	)
	if err == nil || !strings.Contains(err.Error(), "payload identity mismatch") ||
		!strings.Contains(err.Error(), "entry") || !strings.Contains(err.Error(), "worker") {
		t.Fatalf("StartDistributedTestWithContent() error = %v", err)
	}
	if len(entryDM.GetEntryNodeCalls()) != 0 || len(workerDM.GetWorkerNodeCalls()) != 0 {
		t.Fatal("mixed payload fleet started containers before the identity gate")
	}
}

func TestDistributedIntegrityPayloadUnavailableNamesHost(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{{Host: "entry", Original: "entry"}, {Host: "worker", Original: "worker"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusReady)
	}
	id := "sha256:" + strings.Repeat("a", 64)
	orchestrator.preflight = identityPreflight{
		identities:    map[string]preflight.ImageIdentity{"entry": {ID: id}, "worker": {ID: id}},
		payloads:      map[string]string{"entry": strings.Repeat("b", 64)},
		payloadErrors: map[string]error{"worker": errors.New("sha256sum unavailable")},
	}
	orchestrator.SetIntegrityRuntimeIdentityTier(constants.IntegrityRuntimeIdentityTierPayload)

	_, err := orchestrator.verifyDistributedIntegrityImageIdentity(context.Background(), "repo/spt:test")
	if err == nil || !strings.Contains(err.Error(), "payload identity unavailable") || !strings.Contains(err.Error(), "worker") {
		t.Fatalf("payload identity error = %v", err)
	}
}

func TestRunningIntegrityPayloadIdentityUsesStartedContainers(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{{Host: "entry", Original: "entry"}, {Host: "worker", Original: "worker"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	for i, host := range orchestrator.hosts {
		host.SetStatus(HostStatusReady)
		host.ContainerID = fmt.Sprintf("container-%d", i)
	}
	id := "sha256:" + strings.Repeat("a", 64)
	payload := strings.Repeat("b", 64)
	runningCalls := &atomic.Int64{}
	orchestrator.preflight = identityPreflight{
		identities:      map[string]preflight.ImageIdentity{"entry": {ID: id}, "worker": {ID: id}},
		payloads:        map[string]string{"entry": payload, "worker": payload},
		runningPayloads: map[string]string{"entry": payload, "worker": payload},
		runningCalls:    runningCalls,
	}
	orchestrator.SetIntegrityRuntimeIdentityTier(constants.IntegrityRuntimeIdentityTierPayload)
	if _, err := orchestrator.PrepareDistributedIntegrityRuntimeIdentity(context.Background(), "repo/spt:test"); err != nil {
		t.Fatal(err)
	}
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusRunning)
	}
	if err := orchestrator.VerifyRunningIntegrityPayloadIdentity(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runningCalls.Load() != 2 {
		t.Fatalf("running payload probes = %d, want 2", runningCalls.Load())
	}
}

func TestRunningIntegrityPayloadIdentityRejectsDriftAndUnavailableEvidence(t *testing.T) {
	for _, tc := range []struct {
		name          string
		running       map[string]string
		runningErrors map[string]error
		want          string
	}{
		{name: "mutated running payload", running: map[string]string{"entry": strings.Repeat("b", 64), "worker": strings.Repeat("c", 64)}, want: "running payload identity mismatch on worker"},
		{name: "unavailable running payload", running: map[string]string{"entry": strings.Repeat("b", 64)}, runningErrors: map[string]error{"worker": errors.New("container exited")}, want: "worker: container exited"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hostInfos := []*hostparse.HostInfo{{Host: "entry", Original: "entry"}, {Host: "worker", Original: "worker"}}
			orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
			for i, host := range orchestrator.hosts {
				host.SetStatus(HostStatusReady)
				host.ContainerID = fmt.Sprintf("container-%d", i)
			}
			id := "sha256:" + strings.Repeat("a", 64)
			payload := strings.Repeat("b", 64)
			orchestrator.preflight = identityPreflight{
				identities:      map[string]preflight.ImageIdentity{"entry": {ID: id}, "worker": {ID: id}},
				payloads:        map[string]string{"entry": payload, "worker": payload},
				runningPayloads: tc.running,
				runningErrors:   tc.runningErrors,
			}
			orchestrator.SetIntegrityRuntimeIdentityTier(constants.IntegrityRuntimeIdentityTierPayload)
			if _, err := orchestrator.PrepareDistributedIntegrityRuntimeIdentity(context.Background(), "repo/spt:test"); err != nil {
				t.Fatal(err)
			}
			for _, host := range orchestrator.hosts {
				host.SetStatus(HostStatusRunning)
			}
			err := orchestrator.VerifyRunningIntegrityPayloadIdentity(context.Background())
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("VerifyRunningIntegrityPayloadIdentity() error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestDistributedIntegrityImageMismatchStopsBeforeContainerStart(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "entry", Original: "entry"},
		{Host: "worker", Original: "worker"},
	}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	entryDM, workerDM := NewMockDockerManager(), NewMockDockerManager()
	orchestrator.hosts[0].DockerManager = entryDM
	orchestrator.hosts[1].DockerManager = workerDM
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusReady)
	}
	orchestrator.preflight = identityPreflight{identities: map[string]preflight.ImageIdentity{
		"entry":  {ID: "sha256:" + strings.Repeat("a", 64)},
		"worker": {ID: "sha256:" + strings.Repeat("b", 64)},
	}}

	err := orchestrator.StartDistributedTestWithContent(
		context.Background(),
		"repo/spt:test",
		scenario.ScenarioParams{WorkloadType: scenario.WorkloadTypeWriteVerify},
		[]byte(`Load.run({})`),
	)
	if err == nil || !strings.Contains(err.Error(), "image identity mismatch") ||
		!strings.Contains(err.Error(), "entry") || !strings.Contains(err.Error(), "worker") {
		t.Fatalf("StartDistributedTestWithContent() error = %v", err)
	}
	if len(entryDM.GetEntryNodeCalls()) != 0 || len(workerDM.GetWorkerNodeCalls()) != 0 {
		t.Fatal("mixed image fleet started containers before the identity gate")
	}
}

func TestDistributedIntegrityRejectsAttachedWorkersBeforeContainerStart(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{{Host: "entry", Original: "entry"}, {Host: "worker", Original: "worker"}}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	orchestrator.SetAttachExistingWorkers(true)
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusReady)
	}
	err := orchestrator.StartDistributedTestWithContent(
		context.Background(),
		"repo/spt:test",
		scenario.ScenarioParams{WorkloadType: scenario.WorkloadTypeReadVerify},
		[]byte(`Load.run({})`),
	)
	if err == nil || !strings.Contains(err.Error(), "runtime image identity is not provable") {
		t.Fatalf("StartDistributedTestWithContent() error = %v", err)
	}
}

func TestMultiHostIntegrityCapabilityFailureStopsBeforeRunAndCleansUp(t *testing.T) {
	var runPosts atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ready", "/health":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ready":true,"status":"ready"}`))
		case constants.SptConfigSchemaEndpoint:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"storage":{"integrity":{"mode":"string"}}}`))
		case "/run":
			if r.Method == http.MethodPost {
				runPosts.Add(1)
			}
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()
	host, port := splitServerHostPort(t, srv.URL)
	orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{{Host: host, IsLocal: false, Original: "qa-client-01"}}, 1)
	orchestrator.SetAPIPort(port)
	orchestrator.hosts[0].SetStatus(HostStatusReady)
	orchestrator.preflight = identityPreflight{identities: map[string]preflight.ImageIdentity{
		"qa-client-01": {ID: "sha256:" + strings.Repeat("a", 64)},
	}}
	mockDocker := NewMockDockerManager()
	orchestrator.hosts[0].DockerManager = mockDocker
	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	wrapper.SetCallbacks(func(*TestStatus) {}, func(*MultiNodeMetricsUpdate) {}, func(string) {}, func(string) {})

	err := wrapper.StartTestWithContent(
		context.Background(),
		"test-image",
		scenario.ScenarioParams{WorkloadType: scenario.WorkloadTypeReadVerify},
		[]byte(`Load.run({})`),
		nil,
	)
	if !errors.Is(err, ErrEngineIncompatible) {
		t.Fatalf("StartTestWithContent() error = %v, want ErrEngineIncompatible", err)
	}
	if got := runPosts.Load(); got != 0 {
		t.Fatalf("/run POST count = %d, want 0", got)
	}
	if got := mockDocker.GetCleanupCallCount(); got != 1 {
		t.Fatalf("cleanup calls = %d, want 1", got)
	}
	if orchestrator.hosts[0].IsManaged() {
		t.Fatal("failed capability gate left the host marked managed")
	}
}

func TestMultiHostTestOrchestrator_StartTestWithContent_MultiHostStartsDistributedAndPostsProvidedArtifacts(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:"+constants.RMIRegistryPort)
	if err != nil {
		t.Skipf("unable to bind local RMI port for test: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	var postedScenario string
	var postedDefaults string
	srv := newSingleHostReplayAPIServer(t, &postedScenario, &postedDefaults)
	defer srv.Close()
	host, port := splitServerHostPort(t, srv.URL)

	hostInfos := []*hostparse.HostInfo{
		{Host: host, IsLocal: true, Original: "entry"},
		{Host: host, IsLocal: true, Original: "worker1"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	orchestrator.SetAPIPort(port)
	orchestrator.detectAdvIP = func(_ context.Context, _ *hostparse.HostInfo) (string, error) {
		return "127.0.0.1", nil
	}

	entryDM := NewMockDockerManager()
	workerDM := NewMockDockerManager()
	orchestrator.hosts[0].SetStatus(HostStatusReady)
	orchestrator.hosts[0].DockerManager = entryDM
	orchestrator.hosts[1].SetStatus(HostStatusReady)
	orchestrator.hosts[1].DockerManager = workerDM

	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	wrapper.SetCallbacks(
		func(*TestStatus) {},
		func(*MultiNodeMetricsUpdate) {},
		func(string) {},
		func(string) {},
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	replayScenario := []byte(`var step = "replay-001-max-w10kb"; Load.config({}).run();`)
	replayDefaults := []byte("storage:\n  driver:\n    type: s3\n")
	mounts := []scenario.FileMount{{HostPath: "/host/items.csv", ContainerPath: "/spt-input/items/read-items.csv"}}
	params := scenario.ScenarioParams{
		WorkloadType:   "write",
		Threads:        1,
		ItemFileMounts: mounts,
		EngineOverrides: []string{
			"load.service.threads=4",
		},
	}
	var launchSubmitted atomic.Bool
	hooks := LaunchHooks{OnSubmitted: func() { launchSubmitted.Store(true) }}
	if err := wrapper.StartTestWithContentAndLaunchHooks(
		ctx, "test-image", params, replayScenario, replayDefaults, hooks,
	); err != nil {
		t.Fatalf("StartTestWithContent() error = %v", err)
	}
	defer func() { _ = wrapper.StopTest() }()
	if !launchSubmitted.Load() {
		t.Fatal("successful distributed /run POST did not signal launch submission")
	}

	if postedScenario != string(replayScenario) {
		t.Fatalf("posted scenario mismatch:\n%s", postedScenario)
	}
	if postedDefaults != string(replayDefaults) {
		t.Fatalf("posted defaults mismatch:\n%s", postedDefaults)
	}

	workerCalls := workerDM.GetWorkerNodeCalls()
	if len(workerCalls) != 1 {
		t.Fatalf("expected one worker node start, got %d", len(workerCalls))
	}
	if workerCalls[0].RMIHostname != "127.0.0.1" {
		t.Fatalf("worker RMI hostname = %q, want 127.0.0.1", workerCalls[0].RMIHostname)
	}
	if !containsStringSimple(strings.Join(workerCalls[0].AdditionalArgs, " "), "--load-service-threads=4") {
		t.Fatalf("worker startup args = %v, want --load-service-threads=4", workerCalls[0].AdditionalArgs)
	}

	entryCalls := entryDM.GetEntryNodeCalls()
	if len(entryCalls) != 1 {
		t.Fatalf("expected one entry node start, got %d", len(entryCalls))
	}
	if !containsStringSimple(strings.Join(entryCalls[0].AdditionalArgs, " "), "--load-service-threads=4") {
		t.Fatalf("entry startup args = %v, want --load-service-threads=4", entryCalls[0].AdditionalArgs)
	}
	wantWorkerAddr := "127.0.0.1:" + constants.RMIRegistryPort
	if len(entryCalls[0].WorkerAddresses) != 1 || entryCalls[0].WorkerAddresses[0] != wantWorkerAddr {
		t.Fatalf("entry worker addresses = %v, want [%s]", entryCalls[0].WorkerAddresses, wantWorkerAddr)
	}
	if got := entryDM.GetFileMounts(); len(got) != 1 || got[0] != mounts[0] {
		t.Fatalf("entry item mounts = %#v, want %#v", got, mounts)
	}
	if got := workerDM.GetFileMounts(); len(got) != 1 || got[0] != mounts[0] {
		t.Fatalf("worker item mounts = %#v, want %#v", got, mounts)
	}
	if orchestrator.hosts[0].APIClient == nil {
		t.Fatal("expected entry API client to be attached after readiness")
	}
	if !orchestrator.hosts[0].IsManaged() || !orchestrator.hosts[1].IsManaged() {
		t.Fatal("expected entry and worker containers to be marked managed")
	}
}

func newSingleHostReplayAPIServer(t *testing.T, postedScenario, postedDefaults *string) *httptest.Server {
	t.Helper()
	var mu sync.Mutex
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ready":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ready":true,"status":"ready","scope":"node","role":"entry","node_id":"n0"}`))
		case "/health":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"ok","scope":"node","role":"entry","node_id":"n0"}`))
		case constants.SptConfigSchemaEndpoint:
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"storage":{"integrity":{"mode":"string","algorithm":"string",` +
				`"input":{"provenance":"string","expectedProducerId":"string"},` +
				`"selection":{"maxCount":"long"}}}}`))
		case "/run":
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if r.Method != http.MethodPost {
				w.WriteHeader(http.StatusMethodNotAllowed)
				return
			}
			if err := r.ParseMultipartForm(10 << 20); err != nil {
				t.Errorf("ParseMultipartForm() error = %v", err)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			scenarioBody := readMultipartFile(t, r, "scenario")
			defaultsBody := readMultipartFile(t, r, "defaults")
			mu.Lock()
			*postedScenario = scenarioBody
			*postedDefaults = defaultsBody
			mu.Unlock()
			w.Header().Set("ETag", "run-single-remote")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"runId":"run-single-remote"}`))
		case "/metrics/json":
			now := time.Now().UTC().Format(time.RFC3339Nano)
			payload := fmt.Sprintf(`[{"metrics_schema":2,"scope":"node","role":"entry","node_id":"n0","run_id":"run-single-remote","sample_ts":"%s","step_id":"replay-001","op_type":"CREATE","timestamp":%d,"elapsed_time_seconds":0.1,"test_state":1,"completion_percent":0,"overall_completion_percent":0,"unbounded":true,"overall_unbounded":true,"operations":{"success_count":0,"failed_count":0,"success_rate_last":0,"failed_rate_last":0},"bandwidth":{"bytes_total":0,"bytes_rate_last":0},"timing":{"latency_mean_us":0,"duration_mean_us":0},"concurrency":{"current":0,"mean":0}}]`, now, time.Now().UnixMilli())
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(payload))
		case "/status":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"state":"RUNNING","message":"running","runId":"run-single-remote"}`))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
}

func readMultipartFile(t *testing.T, r *http.Request, name string) string {
	t.Helper()
	file, _, err := r.FormFile(name)
	if err != nil {
		t.Fatalf("FormFile(%q) error = %v", name, err)
	}
	defer func() { _ = file.Close() }()
	body, err := io.ReadAll(file)
	if err != nil {
		t.Fatalf("ReadAll(%q) error = %v", name, err)
	}
	return string(body)
}

func splitServerHostPort(t *testing.T, rawURL string) (string, string) {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatalf("parse server URL: %v", err)
	}
	host, port, err := net.SplitHostPort(u.Host)
	if err != nil {
		t.Fatalf("split server host/port: %v", err)
	}
	return host, port
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

func TestMultiHostTestOrchestrator_CompletionCh_ClosesOnTerminalStatus(t *testing.T) {
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
			_, _ = w.Write([]byte(`{"state":"COMPLETED","message":"all steps done","runId":"run-123"}`))
		case "/metrics/json":
			now := time.Now().UTC().Format(time.RFC3339Nano)
			payload := fmt.Sprintf(`[{"metrics_schema":2,"scope":"node","role":"entry","node_id":"n0","run_id":"run-123","sample_ts":"%s","step_id":"s-create","op_type":"CREATE","timestamp":%d,"elapsed_time_seconds":0.1,"test_state":2,"completion_percent":100,"overall_completion_percent":100,"unbounded":false,"overall_unbounded":false,"operations":{"success_count":1,"failed_count":0,"success_rate_last":1,"failed_rate_last":0},"bandwidth":{"bytes_total":1024,"bytes_rate_last":1024},"timing":{"latency_mean_us":1000,"duration_mean_us":1000},"concurrency":{"current":0,"mean":0}}]`, now, time.Now().UnixMilli())
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(payload))
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	u, _ := url.Parse(srv.URL)
	_, port, _ := net.SplitHostPort(u.Host)

	hostInfos := []*hostparse.HostInfo{
		{Host: "127.0.0.1", IsLocal: true, Original: "primary"},
		{Host: "127.0.0.1", IsLocal: true, Original: "worker1"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	orchestrator.apiPort = port

	primary := orchestrator.hosts[0]
	worker := orchestrator.hosts[1]
	primary.SetStatus(HostStatusReady)
	worker.SetStatus(HostStatusReady)
	primary.DockerManager = NewMockDockerManager()
	worker.DockerManager = NewMockDockerManager()

	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	wrapper.SetCallbacks(
		func(*TestStatus) {},
		func(*MultiNodeMetricsUpdate) {},
		func(string) {},
		func(string) {},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	params := scenario.ScenarioParams{WorkloadType: scenario.WorkloadTypeList, Threads: 1, Bucket: "demo", Endpoint: "http://minio:9000"}
	if err := wrapper.StartTest(ctx, "test-image", params); err != nil {
		t.Fatalf("StartTest returned error: %v", err)
	}
	if !runStarted.Load() {
		t.Fatalf("expected run to be started via /run API")
	}

	select {
	case <-wrapper.CompletionCh():
	case <-time.After(6 * time.Second):
		t.Fatal("CompletionCh did not close after terminal status")
	}

	if err := wrapper.StopTest(); err != nil {
		t.Fatalf("StopTest returned error: %v", err)
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

func TestMultiHostOrchestrator_StartDistributedTestRejectsStartupSettingsWithAttachedWorkers(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "entry", Original: "entry"},
		{Host: "worker1", Original: "worker1"},
	}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	orchestrator.SetAttachExistingWorkers(true)

	err := orchestrator.StartDistributedTestWithContent(
		context.Background(),
		"test-image",
		scenario.ScenarioParams{
			EngineOverrides: []string{"load.service.threads=4"},
		},
		[]byte(`Load.run({});`),
	)
	if err == nil {
		t.Fatal("StartDistributedTestWithContent() error = nil, want attached-worker startup error")
	}
	if !strings.Contains(err.Error(), "cannot be applied with attached workers") ||
		!strings.Contains(err.Error(), "--load-service-threads=4") {
		t.Fatalf("StartDistributedTestWithContent() error = %v", err)
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
	var launchSubmitted atomic.Bool
	hooks := LaunchHooks{OnSubmitted: func() { launchSubmitted.Store(true) }}
	if err := wrapper.StartTestWithLaunchHooks(ctx, "test-image", params, hooks); err != nil {
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
	if !launchSubmitted.Load() {
		t.Fatal("expected successful LIST POST to notify launch submission")
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

func TestMultiHostListPreSubmissionFailuresCleanEveryManagedContainer(t *testing.T) {
	tests := []struct {
		name      string
		want      string
		failRun   bool
		configure func(*MultiHostTestOrchestrator, *MockDockerManager)
	}{
		{
			name: "scenario generation", want: "scenario failure",
			configure: func(wrapper *MultiHostTestOrchestrator, _ *MockDockerManager) {
				wrapper.generateScenario = func(scenario.Params) (string, error) {
					return "", errors.New("scenario failure")
				}
			},
		},
		{
			name: "endpoint arguments", want: "endpoint failure",
			configure: func(wrapper *MultiHostTestOrchestrator, _ *MockDockerManager) {
				wrapper.buildEndpointArgs = func(scenario.Params) ([]string, error) {
					return nil, errors.New("endpoint failure")
				}
			},
		},
		{
			name: "missing primary manager", want: "no Docker manager",
			configure: func(wrapper *MultiHostTestOrchestrator, _ *MockDockerManager) {
				wrapper.multiHost.hosts[0].DockerManager = nil
			},
		},
		{
			name: "primary start", want: "mock start entry node container failed",
			configure: func(_ *MultiHostTestOrchestrator, primary *MockDockerManager) {
				primary.SetFailOnStart(true)
			},
		},
		{
			name: "post-start readiness", want: "readiness failure",
			configure: func(wrapper *MultiHostTestOrchestrator, _ *MockDockerManager) {
				calls := 0
				baseWait := wrapper.waitForAPIs
				wrapper.waitForAPIs = func(ctx context.Context, timeout time.Duration) error {
					calls++
					if calls == 2 {
						return errors.New("readiness failure")
					}
					return baseWait(ctx, timeout)
				}
			},
		},
		{
			name: "defaults generation", want: "defaults failure",
			configure: func(wrapper *MultiHostTestOrchestrator, _ *MockDockerManager) {
				wrapper.generateDefaults = func(scenario.Params) ([]byte, error) {
					return nil, errors.New("defaults failure")
				}
			},
		},
		{name: "entry submission", want: "status 500", failRun: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/ready":
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"ready":true,"status":"ready"}`))
				case "/run":
					if test.failRun {
						w.WriteHeader(http.StatusInternalServerError)
						return
					}
					w.Header().Set("ETag", "run-1")
					w.WriteHeader(http.StatusOK)
				default:
					w.WriteHeader(http.StatusOK)
				}
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

			orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{
				{Host: "127.0.0.1", Original: "primary"},
				{Host: "127.0.0.1", Original: "worker"},
			}, 2)
			orchestrator.apiPort = port
			primary, worker := NewMockDockerManager(), NewMockDockerManager()
			orchestrator.hosts[0].DockerManager = primary
			orchestrator.hosts[1].DockerManager = worker
			for _, host := range orchestrator.hosts {
				host.SetStatus(HostStatusReady)
			}
			wrapper := NewMultiHostTestOrchestrator(orchestrator)
			if test.configure != nil {
				test.configure(wrapper, primary)
			}
			var submitted atomic.Bool
			err = wrapper.StartTestWithLaunchHooks(context.Background(), "image", scenario.Params{
				WorkloadType: scenario.WorkloadTypeList, Threads: 1, Bucket: "bucket",
				Endpoint: server.URL,
			}, LaunchHooks{OnSubmitted: func() { submitted.Store(true) }})
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("LIST failure = %v, want %q", err, test.want)
			}
			if submitted.Load() {
				t.Fatal("failed LIST launch notified submission")
			}
			for _, host := range orchestrator.hosts {
				if host.DockerManager == nil {
					continue
				}
				if host.IsManaged() || host.ContainerID != "" || host.GetStatus() == HostStatusRunning {
					t.Fatalf("%s retained cleanup ownership: managed=%t id=%q status=%s",
						host.Info.Original, host.IsManaged(), host.ContainerID, host.GetStatus())
				}
			}
		})
	}
}

func TestMultiHostListCleanupFailureRetainsWorkerForRetry(t *testing.T) {
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
	orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{
		{Host: "127.0.0.1", Original: "primary"},
		{Host: "127.0.0.1", Original: "worker"},
	}, 2)
	orchestrator.apiPort = port
	primary, worker := NewMockDockerManager(), NewMockDockerManager()
	orchestrator.hosts[0].DockerManager = primary
	orchestrator.hosts[1].DockerManager = worker
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusReady)
	}
	worker.SetFailOnCleanup(true)
	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	wrapper.generateScenario = func(scenario.Params) (string, error) {
		return "", errors.New("scenario failure")
	}
	err = wrapper.StartTest(context.Background(), "image", scenario.Params{
		WorkloadType: scenario.WorkloadTypeList, Threads: 1, Bucket: "bucket", Endpoint: server.URL,
	})
	if err == nil || !strings.Contains(err.Error(), "scenario failure") ||
		!strings.Contains(err.Error(), "failed to cleanup") {
		t.Fatalf("LIST cleanup failure = %v, want primary cause plus cleanup failure", err)
	}
	host := orchestrator.hosts[1]
	if !host.IsManaged() || host.ContainerID == "" {
		t.Fatalf("failed LIST cleanup lost worker retry ownership: managed=%t id=%q",
			host.IsManaged(), host.ContainerID)
	}
	worker.SetFailOnCleanup(false)
	if err = orchestrator.StopAllContainers(context.Background()); err != nil {
		t.Fatalf("LIST cleanup retry failed: %v", err)
	}
	if host.IsManaged() || host.ContainerID != "" {
		t.Fatalf("LIST cleanup retry retained worker: managed=%t id=%q", host.IsManaged(), host.ContainerID)
	}
}

func TestStartupRollbackUsesFreshBudgetAfterCallerCancellation(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: "host1", Original: "host1"}}, 1)
	manager := NewMockDockerManager()
	manager.SetContainerID("container-1")
	host := orchestrator.hosts[0]
	host.DockerManager = manager
	host.ContainerID = "container-1"
	host.SetStatus(HostStatusRunning)
	host.SetManaged(true)

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := orchestrator.cleanupManagedContainersAfterStartFailure(canceled); err != nil {
		t.Fatalf("cleanupManagedContainersAfterStartFailure() error = %v", err)
	}
	if host.IsManaged() || manager.HasManagedResources() || manager.GetCleanupCallCount() != 1 {
		t.Fatalf("canceled rollback state: managed=%t resources=%t cleanup_calls=%d",
			host.IsManaged(), manager.HasManagedResources(), manager.GetCleanupCallCount())
	}
}

func TestMultiHostListCancellationDuringWorkerReadinessStopsAndRollsBack(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{
		{Host: "primary", Original: "primary"}, {Host: "worker", Original: "worker"},
	}, 2)
	primary, worker := NewMockDockerManager(), NewMockDockerManager()
	orchestrator.hosts[0].DockerManager = primary
	orchestrator.hosts[1].DockerManager = worker
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusReady)
	}
	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	ctx, cancel := context.WithCancel(context.Background())
	wrapper.waitForAPIs = func(context.Context, time.Duration) error {
		cancel()
		return context.Canceled
	}

	err := wrapper.StartTestWithLaunchHooks(ctx, "image", scenario.Params{
		WorkloadType: scenario.WorkloadTypeList, Threads: 1, Bucket: "bucket",
	}, NewLaunchHooks(nil))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("LIST cancellation error = %v, want context.Canceled", err)
	}
	if worker.GetStartCallCount() != 1 || worker.GetCleanupCallCount() != 1 {
		t.Fatalf("worker calls start=%d cleanup=%d, want 1/1",
			worker.GetStartCallCount(), worker.GetCleanupCallCount())
	}
	if primary.GetStartCallCount() != 0 {
		t.Fatalf("primary started %d time(s) after cancellation", primary.GetStartCallCount())
	}
	for _, host := range orchestrator.hosts {
		if host.IsManaged() || host.ContainerID != "" {
			t.Fatalf("host %s retained resources after cancellation", host.Info.Original)
		}
	}
}

func TestSingleRemoteCancellationDuringReadinessRollsBackWithoutPost(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: "remote", Original: "remote"}}, 1)
	manager := NewMockDockerManager()
	host := orchestrator.hosts[0]
	host.DockerManager = manager
	host.SetStatus(HostStatusReady)
	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	ctx, cancel := context.WithCancel(context.Background())
	wrapper.waitForAPIs = func(context.Context, time.Duration) error {
		cancel()
		return context.Canceled
	}

	err := wrapper.StartTestWithContentAndLaunchHooks(
		ctx, "image", scenario.Params{WorkloadType: scenario.WorkloadTypeRead},
		[]byte("scenario"), []byte("defaults"), NewLaunchHooks(nil))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("single-remote cancellation error = %v, want context.Canceled", err)
	}
	if manager.GetStartCallCount() != 1 || manager.GetCleanupCallCount() != 1 {
		t.Fatalf("single-remote calls start=%d cleanup=%d, want 1/1",
			manager.GetStartCallCount(), manager.GetCleanupCallCount())
	}
	if host.IsManaged() || manager.HasManagedResources() {
		t.Fatal("single-remote cancellation retained managed resources")
	}
}

func TestDistributedAdvertisedIPCancellationPreventsWorkerAndEntryStarts(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{
		{Host: "entry", Original: "entry"}, {Host: "worker", Original: "worker"},
	}, 2)
	entry, worker := NewMockDockerManager(), NewMockDockerManager()
	orchestrator.hosts[0].DockerManager = entry
	orchestrator.hosts[1].DockerManager = worker
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusReady)
	}
	detectionStarted := make(chan struct{})
	orchestrator.detectAdvIP = func(ctx context.Context, _ *hostparse.HostInfo) (string, error) {
		close(detectionStarted)
		<-ctx.Done()
		return "", ctx.Err()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- orchestrator.StartDistributedTestWithContent(
			ctx, "image", scenario.Params{WorkloadType: scenario.WorkloadTypeRead}, []byte("scenario"))
	}()
	select {
	case <-detectionStarted:
	case <-time.After(time.Second):
		t.Fatal("advertised-IP detection did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("distributed cancellation error = %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("distributed startup ignored advertised-IP cancellation")
	}
	if worker.GetStartCallCount() != 0 || entry.GetStartCallCount() != 0 {
		t.Fatalf("container starts after canceled detection: worker=%d entry=%d",
			worker.GetStartCallCount(), entry.GetStartCallCount())
	}
}

func TestSingleRemoteIntegrityStartsVerifiedImmutableImageID(t *testing.T) {
	var postedScenario, postedDefaults string
	server := newSingleHostReplayAPIServer(t, &postedScenario, &postedDefaults)
	defer server.Close()
	host, port := splitServerHostPort(t, server.URL)

	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: host, IsLocal: false, Original: "qa-client-01"}}, 1)
	orchestrator.SetAPIPort(port)
	orchestrator.hosts[0].SetStatus(HostStatusReady)
	mockDocker := NewMockDockerManager()
	orchestrator.hosts[0].DockerManager = mockDocker
	verifiedID := "sha256:" + strings.Repeat("a", 64)
	orchestrator.preflight = identityPreflight{identities: map[string]preflight.ImageIdentity{
		"qa-client-01": {ID: verifiedID},
	}}
	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	wrapper.SetCallbacks(func(*TestStatus) {}, func(*MultiNodeMetricsUpdate) {}, func(string) {}, func(string) {})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const mutableTag = "repo/spt:mutable"
	if err := wrapper.StartTestWithContent(
		ctx,
		mutableTag,
		scenario.ScenarioParams{WorkloadType: scenario.WorkloadTypeReadVerify},
		[]byte(`Load.run({})`),
		nil,
	); err != nil {
		t.Fatal(err)
	}

	calls := mockDocker.GetContainerCalls()
	if len(calls) != 1 {
		t.Fatalf("container starts = %d, want 1", len(calls))
	}
	if calls[0].Image != verifiedID {
		t.Fatalf("container image = %q, want verified immutable ID %q (tag was %q)",
			calls[0].Image, verifiedID, mutableTag)
	}
	if postedScenario == "" {
		t.Fatal("verified single-remote scenario was not posted")
	}
}

func TestSingleRemoteIntegrityIdentityFailureStopsBeforeContainerAndScenario(t *testing.T) {
	var postedScenario, postedDefaults string
	server := newSingleHostReplayAPIServer(t, &postedScenario, &postedDefaults)
	defer server.Close()
	host, port := splitServerHostPort(t, server.URL)

	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: host, IsLocal: false, Original: "qa-client-01"}}, 1)
	orchestrator.SetAPIPort(port)
	orchestrator.hosts[0].SetStatus(HostStatusReady)
	mockDocker := NewMockDockerManager()
	orchestrator.hosts[0].DockerManager = mockDocker
	orchestrator.preflight = identityPreflight{errors: map[string]error{
		"qa-client-01": errors.New("mutable tag no longer resolves to prepared image"),
	}}
	wrapper := NewMultiHostTestOrchestrator(orchestrator)
	wrapper.SetCallbacks(func(*TestStatus) {}, func(*MultiNodeMetricsUpdate) {}, func(string) {}, func(string) {})

	err := wrapper.StartTestWithContent(
		context.Background(),
		"repo/spt:mutable",
		scenario.ScenarioParams{WorkloadType: scenario.WorkloadTypeReadVerify},
		[]byte(`Load.run({})`),
		nil,
	)
	if err == nil || !strings.Contains(err.Error(), "qa-client-01") {
		t.Fatalf("identity error = %v, want host-specific failure", err)
	}
	if len(mockDocker.GetContainerCalls()) != 0 {
		t.Fatal("identity failure started a container")
	}
	if postedScenario != "" {
		t.Fatal("identity failure posted the scenario")
	}
}

func TestRunningPayloadEvidenceRecordsExactStartedParticipantSetByHost(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "entry", Original: "entry"},
		{Host: "dropped", Original: "dropped"},
		{Host: "worker", Original: "worker"},
	}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusReady)
	}
	imageID := "sha256:" + strings.Repeat("a", 64)
	payload := strings.Repeat("b", 64)
	runningCalls := &atomic.Int64{}
	orchestrator.preflight = identityPreflight{
		identities: map[string]preflight.ImageIdentity{
			"entry": {ID: imageID}, "dropped": {ID: imageID}, "worker": {ID: imageID},
		},
		payloads: map[string]string{
			"entry": payload, "dropped": payload, "worker": payload,
		},
		runningPayloads: map[string]string{"entry": payload, "worker": payload},
		runningCalls:    runningCalls,
	}
	orchestrator.SetIntegrityRuntimeIdentityTier(constants.IntegrityRuntimeIdentityTierPayload)
	var recorded DistributedRuntimeIdentityEvidence
	orchestrator.SetRuntimeIdentityRecorder(func(evidence DistributedRuntimeIdentityEvidence) {
		recorded = evidence
	})
	if _, err := orchestrator.PrepareDistributedIntegrityRuntimeIdentity(
		context.Background(), "repo/spt:test"); err != nil {
		t.Fatal(err)
	}
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	orchestrator.hosts[0].ContainerID = "entry-container"
	orchestrator.hosts[1].SetStatus(HostStatusFailed)
	orchestrator.hosts[2].SetStatus(HostStatusRunning)
	orchestrator.hosts[2].ContainerID = "worker-container"

	if err := orchestrator.VerifyRunningIntegrityPayloadIdentity(context.Background()); err != nil {
		t.Fatal(err)
	}
	if runningCalls.Load() != 2 {
		t.Fatalf("running payload probes = %d, want 2", runningCalls.Load())
	}
	if len(recorded.Participants) != 2 || recorded.Participants[0].Host != "entry" ||
		recorded.Participants[1].Host != "worker" {
		t.Fatalf("running participant evidence = %+v, want exact entry/worker set", recorded.Participants)
	}
	for _, participant := range recorded.Participants {
		if participant.PayloadSHA256 != payload || participant.ImageID != imageID {
			t.Fatalf("incomplete running participant evidence: %+v", participant)
		}
	}
}

func TestRunningImageEvidenceRecordsExactStartedParticipantSetByHost(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "entry", Original: "entry"},
		{Host: "dropped", Original: "dropped"},
		{Host: "worker", Original: "worker"},
	}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusReady)
	}
	imageID := "sha256:" + strings.Repeat("a", 64)
	payloadCalls := &atomic.Int64{}
	orchestrator.preflight = identityPreflight{
		identities: map[string]preflight.ImageIdentity{
			"entry": {ID: imageID}, "dropped": {ID: imageID}, "worker": {ID: imageID},
		},
		payloadCalls: payloadCalls,
	}
	var recorded DistributedRuntimeIdentityEvidence
	orchestrator.SetRuntimeIdentityRecorder(func(evidence DistributedRuntimeIdentityEvidence) {
		recorded = evidence
	})
	if _, err := orchestrator.PrepareDistributedIntegrityRuntimeIdentity(
		context.Background(), "repo/spt:test"); err != nil {
		t.Fatal(err)
	}
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	orchestrator.hosts[0].ContainerID = "entry-container"
	orchestrator.hosts[1].SetStatus(HostStatusFailed)
	orchestrator.hosts[2].SetStatus(HostStatusRunning)
	orchestrator.hosts[2].ContainerID = "worker-container"

	if err := orchestrator.VerifyRunningIntegrityRuntimeIdentity(context.Background()); err != nil {
		t.Fatal(err)
	}
	if payloadCalls.Load() != 0 {
		t.Fatalf("image-tier reconciliation made %d payload probes, want 0", payloadCalls.Load())
	}
	if len(recorded.Participants) != 2 || recorded.Participants[0].Host != "entry" ||
		recorded.Participants[1].Host != "worker" {
		t.Fatalf("running image participant evidence = %+v, want exact entry/worker set", recorded.Participants)
	}
	for _, participant := range recorded.Participants {
		if participant.ImageID != imageID || participant.PayloadSHA256 != "" {
			t.Fatalf("invalid running image participant evidence: %+v", participant)
		}
	}
}

func TestRunningImageEvidenceRejectsAmbiguousOrUnexpectedCoverage(t *testing.T) {
	newPrepared := func(t *testing.T) *MultiHostOrchestrator {
		t.Helper()
		hostInfos := []*hostparse.HostInfo{
			{Host: "entry", Original: "entry"}, {Host: "worker", Original: "worker"},
		}
		orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
		for index, host := range orchestrator.hosts {
			host.SetStatus(HostStatusReady)
			host.ContainerID = fmt.Sprintf("container-%d", index)
		}
		imageID := "sha256:" + strings.Repeat("a", 64)
		orchestrator.preflight = identityPreflight{identities: map[string]preflight.ImageIdentity{
			"entry": {ID: imageID}, "worker": {ID: imageID},
		}}
		if _, err := orchestrator.PrepareDistributedIntegrityRuntimeIdentity(
			context.Background(), "repo/spt:test"); err != nil {
			t.Fatal(err)
		}
		for _, host := range orchestrator.hosts {
			host.SetStatus(HostStatusRunning)
		}
		return orchestrator
	}

	tests := []struct {
		name   string
		mutate func(*MultiHostOrchestrator)
		want   string
	}{
		{name: "no running participants", mutate: func(orchestrator *MultiHostOrchestrator) {
			for _, host := range orchestrator.hosts {
				host.SetStatus(HostStatusFailed)
			}
		}, want: "no running participants"},
		{name: "empty prepared host", mutate: func(orchestrator *MultiHostOrchestrator) {
			orchestrator.runtimeIdentityEvidence.Participants[0].Host = ""
		}, want: "empty participant host"},
		{name: "duplicate prepared host", mutate: func(orchestrator *MultiHostOrchestrator) {
			orchestrator.runtimeIdentityEvidence.Participants[1].Host =
				orchestrator.runtimeIdentityEvidence.Participants[0].Host
		}, want: "duplicate host"},
		{name: "missing prepared running host", mutate: func(orchestrator *MultiHostOrchestrator) {
			orchestrator.runtimeIdentityEvidence.Participants =
				orchestrator.runtimeIdentityEvidence.Participants[:1]
		}, want: "worker was not present in prepared"},
		{name: "unexpected running host", mutate: func(orchestrator *MultiHostOrchestrator) {
			orchestrator.hosts[1].Info.Host = "replacement"
		}, want: "not present in prepared"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			orchestrator := newPrepared(t)
			orchestrator.mu.Lock()
			test.mutate(orchestrator)
			orchestrator.mu.Unlock()
			err := orchestrator.VerifyRunningIntegrityRuntimeIdentity(context.Background())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("coverage error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestRunningPayloadEvidenceRejectsAmbiguousOrUnexpectedCoverage(t *testing.T) {
	newPrepared := func(t *testing.T) (*MultiHostOrchestrator, string) {
		t.Helper()
		hostInfos := []*hostparse.HostInfo{{Host: "entry", Original: "entry"}, {Host: "worker", Original: "worker"}}
		orchestrator := NewMultiHostOrchestrator(hostInfos, 1)
		for index, host := range orchestrator.hosts {
			host.SetStatus(HostStatusReady)
			host.ContainerID = fmt.Sprintf("container-%d", index)
		}
		imageID := "sha256:" + strings.Repeat("a", 64)
		payload := strings.Repeat("b", 64)
		orchestrator.preflight = identityPreflight{
			identities:      map[string]preflight.ImageIdentity{"entry": {ID: imageID}, "worker": {ID: imageID}},
			payloads:        map[string]string{"entry": payload, "worker": payload},
			runningPayloads: map[string]string{"entry": payload, "worker": payload},
		}
		orchestrator.SetIntegrityRuntimeIdentityTier(constants.IntegrityRuntimeIdentityTierPayload)
		if _, err := orchestrator.PrepareDistributedIntegrityRuntimeIdentity(
			context.Background(), "repo/spt:test"); err != nil {
			t.Fatal(err)
		}
		for _, host := range orchestrator.hosts {
			host.SetStatus(HostStatusRunning)
		}
		return orchestrator, payload
	}

	t.Run("no running participants", func(t *testing.T) {
		orchestrator, _ := newPrepared(t)
		for _, host := range orchestrator.hosts {
			host.SetStatus(HostStatusFailed)
		}
		err := orchestrator.VerifyRunningIntegrityPayloadIdentity(context.Background())
		if err == nil || !strings.Contains(err.Error(), "no running participants") {
			t.Fatalf("coverage error = %v", err)
		}
	})

	t.Run("unexpected running host", func(t *testing.T) {
		orchestrator, _ := newPrepared(t)
		orchestrator.hosts[1].Info.Host = "replacement"
		err := orchestrator.VerifyRunningIntegrityPayloadIdentity(context.Background())
		if err == nil || !strings.Contains(err.Error(), "not present in prepared") {
			t.Fatalf("coverage error = %v", err)
		}
	})

	t.Run("duplicate prepared host", func(t *testing.T) {
		orchestrator, _ := newPrepared(t)
		orchestrator.mu.Lock()
		orchestrator.runtimeIdentityEvidence.Participants[1].Host =
			orchestrator.runtimeIdentityEvidence.Participants[0].Host
		orchestrator.mu.Unlock()
		err := orchestrator.VerifyRunningIntegrityPayloadIdentity(context.Background())
		if err == nil || !strings.Contains(err.Error(), "duplicate host") {
			t.Fatalf("coverage error = %v", err)
		}
	})
}

func TestFinalizeDiagnosticsCanceledWaiterDoesNotBlockBehindFirstCaller(t *testing.T) {
	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: "host1", Original: "host1"}}, 1)
	orchestrator.SetResultsRoot(t.TempDir())
	started := make(chan string, 1)
	release := make(chan struct{})
	host := orchestrator.hosts[0]
	host.SetStatus(HostStatusRunning)
	host.ContainerID = "container-1"
	host.SetManaged(true)
	host.DockerManager = &blockingDiagnosticsDockerManager{
		MockDockerManager: NewMockDockerManager(),
		host:              "host1",
		started:           started,
		release:           release,
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- orchestrator.FinalizeDiagnosticsAndCleanup(context.Background()) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first finalizer did not enter diagnostics")
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	startedAt := time.Now()
	err := orchestrator.FinalizeDiagnosticsAndCleanup(canceled)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled waiter error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(startedAt); elapsed > 250*time.Millisecond {
		t.Fatalf("canceled waiter blocked for %s behind first caller", elapsed)
	}
	close(release)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first finalizer did not return after release")
	}
}

func TestRunningIntegrityIdentityRejectsParticipantCountBelowMinimum(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "entry", Original: "entry"},
		{Host: "worker-1", Original: "worker-1"},
		{Host: "worker-2", Original: "worker-2"},
	}
	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
	imageID := "sha256:" + strings.Repeat("a", 64)
	orchestrator.preflight = identityPreflight{identities: map[string]preflight.ImageIdentity{
		"entry": {ID: imageID}, "worker-1": {ID: imageID}, "worker-2": {ID: imageID},
	}}
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusReady)
	}
	if _, err := orchestrator.PrepareDistributedIntegrityRuntimeIdentity(
		context.Background(), "repo/spt:test"); err != nil {
		t.Fatal(err)
	}
	orchestrator.hosts[0].SetStatus(HostStatusRunning)
	orchestrator.hosts[1].SetStatus(HostStatusFailed)
	orchestrator.hosts[2].SetStatus(HostStatusFailed)

	err := orchestrator.VerifyRunningIntegrityRuntimeIdentity(context.Background())
	if err == nil || !strings.Contains(err.Error(), "below min-hosts 2") {
		t.Fatalf("quorum error = %v, want min-hosts rejection", err)
	}
	if got := len(orchestrator.runtimeIdentityEvidence.Participants); got != 3 {
		t.Fatalf("failed gate rewrote evidence to %d participants", got)
	}
}

func TestRunningIntegrityIdentityRejectsUnavailableLockedParticipantForEveryTier(t *testing.T) {
	for _, tier := range []string{
		constants.IntegrityRuntimeIdentityTierImage,
		constants.IntegrityRuntimeIdentityTierPayload,
	} {
		t.Run(tier, func(t *testing.T) {
			hostInfos := []*hostparse.HostInfo{
				{Host: "entry", Original: "entry"}, {Host: "worker", Original: "worker"},
			}
			orchestrator := NewMultiHostOrchestrator(hostInfos, 2)
			imageID := "sha256:" + strings.Repeat("a", 64)
			payload := strings.Repeat("b", 64)
			orchestrator.preflight = identityPreflight{
				identities: map[string]preflight.ImageIdentity{
					"entry": {ID: imageID}, "worker": {ID: imageID},
				},
				payloads: map[string]string{"entry": payload, "worker": payload},
			}
			orchestrator.SetIntegrityRuntimeIdentityTier(tier)
			for _, host := range orchestrator.hosts {
				host.SetStatus(HostStatusReady)
			}
			if _, err := orchestrator.PrepareDistributedIntegrityRuntimeIdentity(
				context.Background(), "repo/spt:test"); err != nil {
				t.Fatal(err)
			}
			orchestrator.executionParticipantKeys = []string{"entry", "worker"}
			orchestrator.hosts[0].SetStatus(HostStatusRunning)
			orchestrator.hosts[0].APIClient = &SptAPIClient{}
			orchestrator.hosts[1].SetStatus(HostStatusFailed)

			err := orchestrator.VerifyRunningIntegrityRuntimeIdentity(context.Background())
			if err == nil || !strings.Contains(err.Error(), "worker is not running") {
				t.Fatalf("locked participant error = %v", err)
			}
			if got := len(orchestrator.runtimeIdentityEvidence.Participants); got != 2 {
				t.Fatalf("failed gate rewrote evidence to %d participants", got)
			}
		})
	}
}

func TestEntryAPIRunRejectsAPIFailedLockedWorkerBeforeScenarioPostForEveryTier(t *testing.T) {
	for _, tier := range []string{
		constants.IntegrityRuntimeIdentityTierImage,
		constants.IntegrityRuntimeIdentityTierPayload,
	} {
		t.Run(tier, func(t *testing.T) {
			var runPosts atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/ready":
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write([]byte(`{"ready":true,"status":"ready"}`))
				case "/run":
					if r.Method == http.MethodPost {
						runPosts.Add(1)
					}
					w.WriteHeader(http.StatusNoContent)
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer server.Close()
			entryHost, apiPort := splitServerHostPort(t, server.URL)

			orchestrator := NewMultiHostOrchestrator([]*hostparse.HostInfo{
				{Host: entryHost, Original: "entry"},
				{Host: "worker.invalid", Original: "worker"},
			}, 2)
			orchestrator.SetAPIPort(apiPort)
			orchestrator.SetIntegrityRuntimeIdentityTier(tier)
			imageID := "sha256:" + strings.Repeat("a", 64)
			payload := strings.Repeat("b", 64)
			orchestrator.preflight = identityPreflight{
				identities: map[string]preflight.ImageIdentity{
					"entry": {ID: imageID}, "worker": {ID: imageID},
				},
				payloads:        map[string]string{"entry": payload, "worker": payload},
				runningPayloads: map[string]string{"entry": payload, "worker": payload},
			}
			for _, host := range orchestrator.hosts {
				host.SetStatus(HostStatusReady)
			}
			if _, err := orchestrator.PrepareDistributedIntegrityRuntimeIdentity(
				context.Background(), "repo/spt:test"); err != nil {
				t.Fatal(err)
			}

			// This is the exact set already supplied to the entry node's RMI topology.
			// Losing the worker's API proof cannot silently shrink that execution set.
			orchestrator.executionParticipantKeys = []string{entryHost, "worker.invalid"}
			orchestrator.hosts[0].ContainerID = "entry-container"
			orchestrator.hosts[0].SetStatus(HostStatusRunning)
			orchestrator.hosts[1].ContainerID = "worker-container"
			orchestrator.hosts[1].SetStatus(HostStatusFailed)

			wrapper := NewMultiHostTestOrchestrator(orchestrator)
			err := wrapper.startEntryAPIRun(
				context.Background(), "repo/spt:test",
				scenario.ScenarioParams{WorkloadType: scenario.WorkloadTypeWriteVerify},
				[]byte(`CreateLoad.config({}).run(); ReadLoad.config({}).run();`), nil,
				"Distributed verification", LaunchHooks{},
			)
			if err == nil || !strings.Contains(err.Error(), "worker is not running") {
				t.Fatalf("entry API gate error = %v", err)
			}
			if got := runPosts.Load(); got != 0 {
				t.Fatalf("/run POST count = %d, want 0", got)
			}
			if got := len(orchestrator.runtimeIdentityEvidence.Participants); got != 2 {
				t.Fatalf("failed gate rewrote evidence to %d participants", got)
			}
		})
	}
}
