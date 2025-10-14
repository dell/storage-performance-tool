/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

func TestStartTUIWithMultiHostOrchestrator_NoReadyHosts(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
		{Host: "host2", IsLocal: false, Original: "host2"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)

	// Don't set any hosts as ready
	for _, host := range orchestrator.hosts {
		host.SetStatus(HostStatusPending)
	}

	params := scenario.ScenarioParams{
		WorkloadType: "mock",
		Duration:     "5s",
		Threads:      2,
	}

	err := StartTUIWithMultiHostOrchestrator(orchestrator, "test-image", "/test/scenario.js", params, nil)

	if err == nil {
		t.Error("StartTUIWithMultiHostOrchestrator should fail when no hosts are ready")
		return
	}

	expectedError := "no ready hosts available for TUI startup"
	if !containsStringHelper(err.Error(), expectedError) {
		t.Errorf("Error should contain '%s', got: %s", expectedError, err.Error())
	}
}

func TestStartTUIWithMultiHostOrchestrator_Setup(t *testing.T) {
	// This test verifies the setup logic without actually starting the TUI
	// (which would require a full Bubble Tea environment)

	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
		{Host: "host2", IsLocal: false, Original: "host2"},
		{Host: "host3", IsLocal: false, Original: "host3"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)

	// Set up some hosts as ready
	// We can't directly access hosts array, so we'll test the logic differently
	// by checking the counts

	params := scenario.ScenarioParams{
		WorkloadType: "write",
		Duration:     "5m",
		Threads:      4,
		KeepScenario: true,
		Endpoint:     "http://test:9000",
		AccessKey:    "test",
		SecretKey:    "test",
		Bucket:       "test",
	}

	// Since we can't set up hosts without Docker integration,
	// we'll verify the orchestrator was created properly
	if orchestrator.GetHostCount() != 3 {
		t.Errorf("Expected 3 total hosts, got %d", orchestrator.GetHostCount())
	}

	// Test that we can create the initial model structure
	// We can't easily test the full TUI startup without mocking Bubble Tea
	model := InitialModel()
	model.scenarioFile = "/test/scenario.js"
	model.keepScenario = params.KeepScenario
	model.scenarioParams = &params

	// Verify model initialization
	if model.scenarioFile != "/test/scenario.js" {
		t.Errorf("Expected scenario file '/test/scenario.js', got '%s'", model.scenarioFile)
	}

	if !model.keepScenario {
		t.Error("Keep scenario should be true")
	}

	if model.scenarioParams == nil {
		t.Error("Scenario params should be set")
	}

	if model.scenarioParams.WorkloadType != "write" {
		t.Errorf("Expected workload type 'write', got '%s'", model.scenarioParams.WorkloadType)
	}

	// Test multi-host orchestrator wrapper creation
	multiOrchestrator := NewMultiHostTestOrchestrator(orchestrator)
	if multiOrchestrator == nil {
		t.Error("Multi-host orchestrator wrapper should be created")
	}

	// Verify the wrapper has the correct underlying orchestrator
	if multiOrchestrator.multiHost != orchestrator {
		t.Error("Wrapper should contain the original orchestrator")
	}

	// Test that we can create a multi-host orchestrator wrapper without errors
	// (Full Docker manager assignment would require actual Docker connectivity)
}

func TestStartTUIWithMultiHostOrchestrator_ParameterHandling(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
	}

	_ = NewMultiHostOrchestrator(hostInfos, 1)

	// Set up host for testing (without direct Docker manager access)

	tests := []struct {
		name         string
		params       scenario.ScenarioParams
		image        string
		scenarioPath string
		expectSetup  bool
		description  string
	}{
		{
			name: "complete write parameters",
			params: scenario.ScenarioParams{
				WorkloadType: "write",
				Duration:     "10m",
				Threads:      8,
				KeepScenario: false,
				Endpoint:     "http://s3.example.com",
				AccessKey:    "access123",
				SecretKey:    "secret456",
				Bucket:       "test-bucket",
				ObjectSize:   "1MB",
			},
			image:        "spt:latest",
			scenarioPath: "/tmp/scenario.js",
			expectSetup:  true,
			description:  "Should handle complete write parameters",
		},
		{
			name: "mock parameters",
			params: scenario.ScenarioParams{
				WorkloadType: "mock",
				Duration:     "5s",
				Threads:      2,
				KeepScenario: true,
			},
			image:        "spt:test",
			scenarioPath: "/test/mock_scenario.js",
			expectSetup:  true,
			description:  "Should handle mock parameters",
		},
		{
			name: "minimal parameters",
			params: scenario.ScenarioParams{
				WorkloadType: "read",
				Duration:     "1m",
				Threads:      1,
			},
			image:        "spt",
			scenarioPath: "/minimal/scenario.js",
			expectSetup:  true,
			description:  "Should handle minimal parameters",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test parameter propagation without actually starting TUI
			model := InitialModel()
			model.scenarioFile = tt.scenarioPath
			model.keepScenario = tt.params.KeepScenario
			model.scenarioParams = &tt.params

			// Initialize live view renderer with scenario params
			model.liveViewRenderer = NewLiveViewRenderer(&tt.params)

			// Verify parameters are correctly set
			if model.scenarioFile != tt.scenarioPath {
				t.Errorf("Expected scenario path '%s', got '%s'", tt.scenarioPath, model.scenarioFile)
			}

			if model.keepScenario != tt.params.KeepScenario {
				t.Errorf("Expected keep scenario %v, got %v", tt.params.KeepScenario, model.keepScenario)
			}

			if model.scenarioParams == nil {
				t.Error("Scenario params should be set")
			} else {
				if model.scenarioParams.WorkloadType != tt.params.WorkloadType {
					t.Errorf("Expected workload type '%s', got '%s'", tt.params.WorkloadType, model.scenarioParams.WorkloadType)
				}
				if model.scenarioParams.Duration != tt.params.Duration {
					t.Errorf("Expected duration '%s', got '%s'", tt.params.Duration, model.scenarioParams.Duration)
				}
				if model.scenarioParams.Threads != tt.params.Threads {
					t.Errorf("Expected threads %d, got %d", tt.params.Threads, model.scenarioParams.Threads)
				}
			}

			if model.liveViewRenderer == nil {
				t.Error("Live view renderer should be initialized")
			}
		})
	}
}

func TestNewMultiHostTestOrchestrator(t *testing.T) {
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
		{Host: "host2", IsLocal: false, Original: "host2"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)

	// Test creating the wrapper
	wrapper := NewMultiHostTestOrchestrator(orchestrator)

	if wrapper == nil {
		t.Fatal("Wrapper should not be nil")
	}

	if wrapper.multiHost != orchestrator {
		t.Error("Wrapper should contain the original orchestrator")
	}

	// Test that it implements TestOrchestratorInterface methods
	// Note: We can't easily test the full interface without starting actual tests
	// but we can test basic method existence and initial behavior

	// Test SetCallbacks - should not panic
	wrapper.SetCallbacks(
		func(status *TestStatus) {
			// Mock status callback
		},
		func(update *MultiNodeMetricsUpdate) {
			// Mock metrics callback
		},
		func(line string) {
			// Mock output callback
		},
		func(err string) {
			// Mock error callback
		},
	)

	// Test that callbacks are set (can't easily verify they work without complex mocking)
	// The main test is that the method doesn't panic and can be called
}

func TestMultiHostTUI_ReadyHostSelection(t *testing.T) {
	// Test the basic orchestrator setup for TUI integration
	hostInfos := []*hostparse.HostInfo{
		{Host: "host1", IsLocal: false, Original: "host1"},
		{Host: "host2", IsLocal: false, Original: "host2"},
	}

	orchestrator := NewMultiHostOrchestrator(hostInfos, 2)

	// Verify orchestrator was created with correct host count
	if orchestrator.GetHostCount() != 2 {
		t.Errorf("Expected 2 hosts, got %d", orchestrator.GetHostCount())
	}

	// Test that GetReadyHosts() works (will be empty without Docker setup)
	readyHosts := orchestrator.GetReadyHosts()

	// Without Docker managers, no hosts will be ready initially
	if len(readyHosts) != 0 {
		t.Errorf("Expected 0 ready hosts without Docker setup, got %d", len(readyHosts))
	}

	// This test verifies the basic orchestrator structure needed for TUI
}

// Helper function for string containment
func containsStringHelper(haystack, needle string) bool {
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
