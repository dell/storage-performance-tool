/*
Copyright © 2026 Dell Technologies
*/

package tui

import (
	"context"
	"reflect"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// newSingleRemoteHostTestOrchestrator returns an orchestrator for one ready remote host whose
// engine API is a fake that accepts the run.
func newSingleRemoteHostTestOrchestrator(t *testing.T) (*MultiHostOrchestrator, *MultiHostTestOrchestrator, *MockDockerManager) {
	t.Helper()
	var postedScenario, postedDefaults string
	srv := newSingleHostReplayAPIServer(t, &postedScenario, &postedDefaults)
	t.Cleanup(srv.Close)
	host, port := splitServerHostPort(t, srv.URL)

	orchestrator := NewMultiHostOrchestrator(
		[]*hostparse.HostInfo{{Host: host, IsLocal: false, Original: "root@wrk1"}}, 1)
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
	return orchestrator, wrapper, mockDocker
}

// One ready host runs a standalone entry engine, outside StartDistributedTestWithContent. Its
// run must still know its metric contributors, or the terminal DELETE check and the driver
// statistics capture refuse the run.
func TestSingleReadyHostStartRecordsTheEntryAsTheOnlyMetricContributor(t *testing.T) {
	params := scenario.ScenarioParams{WorkloadType: "mock", Threads: 1, ObjectSize: "1MB"}
	tests := map[string]func(context.Context, *MultiHostTestOrchestrator) error{
		"caller-provided content, as spt run starts it": func(ctx context.Context, wrapper *MultiHostTestOrchestrator) error {
			return wrapper.StartTestWithContentAndLaunchHooks(
				ctx, "test-image", params,
				[]byte(`var step = "mt-001-delete"; Load.config({}).run();`),
				[]byte("storage:\n  driver:\n    type: dummy-mock\n"),
				LaunchHooks{})
		},
		"generated content": func(ctx context.Context, wrapper *MultiHostTestOrchestrator) error {
			return wrapper.StartTestWithLaunchHooks(ctx, "test-image", params, LaunchHooks{})
		},
	}
	for name, start := range tests {
		t.Run(name, func(t *testing.T) {
			orchestrator, wrapper, mockDocker := newSingleRemoteHostTestOrchestrator(t)
			if _, err := orchestrator.MetricContributorIDs(); err == nil {
				t.Fatal("metric contributors were available before the engine started")
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			if err := start(ctx, wrapper); err != nil {
				t.Fatalf("single-host start error = %v", err)
			}
			defer func() { _ = wrapper.StopTest() }()

			if got := len(mockDocker.GetContainerCalls()); got != 1 {
				t.Fatalf("node-mode container starts = %d, want the single-host branch's one", got)
			}
			if got := len(mockDocker.GetEntryNodeCalls()); got != 0 {
				t.Fatalf("entry-node container starts = %d, want none outside the distributed branch", got)
			}
			contributors, err := orchestrator.MetricContributorIDs()
			if err != nil {
				t.Fatalf("MetricContributorIDs() error = %v", err)
			}
			if want := []string{constants.MetricsLocalContributorID}; !reflect.DeepEqual(contributors, want) {
				t.Fatalf("metric contributors = %v, want %v", contributors, want)
			}
		})
	}
}

// A single-host start that fails before its engine starts must not leave an earlier
// execution's contributors to bind the run's terminal captures.
func TestSingleReadyHostFailedStartForgetsEarlierMetricContributors(t *testing.T) {
	orchestrator, wrapper, mockDocker := newSingleRemoteHostTestOrchestrator(t)
	orchestrator.executionContributorIDs = []string{constants.MetricsLocalContributorID, "192.0.2.25:1099"}
	mockDocker.SetFailOnStart(true)

	err := wrapper.StartTestWithContentAndLaunchHooks(
		context.Background(), "test-image",
		scenario.ScenarioParams{WorkloadType: "mock", Threads: 1, ObjectSize: "1MB"},
		[]byte(`Load.config({}).run();`), []byte("storage: {}\n"), LaunchHooks{})
	if err == nil {
		t.Fatal("single-host start succeeded although its container failed to start")
	}
	if contributors, err := orchestrator.MetricContributorIDs(); err == nil {
		t.Fatalf("failed start kept metric contributors %v", contributors)
	}
}
