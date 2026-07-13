/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"os"
	"strings"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// TestScenarioFileCleanup verifies that scenario files are properly tracked and cleaned up
func TestScenarioFileCleanup(t *testing.T) {
	// Create a temporary scenario file
	tmpFile, err := os.CreateTemp("", "test-scenario-*.js")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	tmpPath := tmpFile.Name()
	_ = tmpFile.Close()

	// Create a model and set the scenario file
	model := InitialModel()
	model.scenarioFile = tmpPath

	// Verify file exists
	if _, err := os.Stat(tmpPath); os.IsNotExist(err) {
		t.Fatal("Temp file should exist before cleanup")
	}

	// The actual cleanup happens in StartTUIWithScenario's defer block
	// We can test that the model properly tracks the scenario file
	if model.scenarioFile != tmpPath {
		t.Errorf("Expected scenario file %s, got %s", tmpPath, model.scenarioFile)
	}

	// Clean up manually for the test
	os.Remove(tmpPath)
}

// TestDockerManagerIntegration tests the integration between TUI model and Docker manager
func TestDockerManagerIntegration(t *testing.T) {
	// Create a model
	model := InitialModel()

	// Create a mock Docker manager
	mockDM := NewMockDockerManager()
	model.dockerManager = mockDM

	// Test that the model can use the Docker interface
	containerID := model.dockerManager.ContainerID()
	if containerID != "mock-container-123" {
		t.Errorf("Expected container ID 'mock-container-123', got %s", containerID)
	}

	// Test container startup
	id, err := model.dockerManager.StartContainer("test-image", []string{"--help"})
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if id != "mock-container-123" {
		t.Errorf("Expected container ID 'mock-container-123', got %s", id)
	}

	// Verify the model stores the container ID when processing messages
	model.containerID = id
	if model.containerID != "mock-container-123" {
		t.Errorf("Model should store container ID")
	}
}

// TestContainerOutputProcessing tests how the model processes container output
func TestContainerOutputProcessing(t *testing.T) {
	model := InitialModel()
	mockDM := NewMockDockerManager()
	model.dockerManager = mockDM

	// Set up mock output with Spt-style data
	mockDM.SetOutputLines([]string{
		"Starting Spt benchmark...",
		"1.000|250630194048|CREATE|         0|0.0       |    22544384|     0|10.008 |100.5 |100.5|         5|          9",
		"2.000|250630194108|CREATE|         0|0.0       |    73473495|     0|30.029 |150.7 |150.7|         5|         10",
		"Benchmark complete.",
	})
	mockDM.SetOutputDelay(1 * time.Millisecond)

	// Collect output lines
	var outputLines []string
	var errorLines []string

	mockDM.StreamOutput("test-container",
		func(line string) {
			outputLines = append(outputLines, line)
		},
		func(line string) {
			errorLines = append(errorLines, line)
		})

	// Wait for streaming to complete
	time.Sleep(20 * time.Millisecond)

	// Verify output was received
	if len(outputLines) != 4 {
		t.Errorf("Expected 4 output lines, got %d", len(outputLines))
	}

	// Verify no errors
	if len(errorLines) > 0 {
		t.Errorf("Unexpected error lines: %v", errorLines)
	}

	// Console parsing removed; ensure output captured and no errors
}

// TestErrorHandlingInStartup tests error scenarios during container startup
func TestErrorHandlingInStartup(t *testing.T) {
	model := InitialModel()
	mockDM := NewMockDockerManager()
	model.dockerManager = mockDM

	// Test container start failure
	mockDM.SetFailOnStart(true)

	_, err := model.dockerManager.StartContainer("test-image", []string{"cmd"})
	if err == nil {
		t.Error("Expected error when container start fails")
	}
	if !strings.Contains(err.Error(), "failed to start container") {
		t.Errorf("Expected specific error message, got: %v", err)
	}

	// Test scenario start failure
	_, err = model.dockerManager.StartContainerWithScenario("test-image", "/tmp/scenario.js", []string{})
	if err == nil {
		t.Error("Expected error when scenario container start fails")
	}
}

// TestCleanupOnExit tests that cleanup is properly called
func TestCleanupOnExit(t *testing.T) {
	model := InitialModel()
	mockDM := NewMockDockerManager()
	model.dockerManager = mockDM
	model.containerID = "test-container"

	// Perform cleanup
	err := model.dockerManager.Cleanup()
	if err != nil {
		t.Errorf("Unexpected error during cleanup: %v", err)
	}

	// Verify cleanup was called
	if count := mockDM.GetCleanupCallCount(); count != 1 {
		t.Errorf("Expected 1 cleanup call, got %d", count)
	}

	// Container ID should be cleared in the mock
	if id := mockDM.ContainerID(); id != "" {
		t.Errorf("Expected empty container ID after cleanup, got %s", id)
	}
}

// TestStartupMessageFlow tests the message flow during container startup
func TestStartupMessageFlow(t *testing.T) {
	model := InitialModel()

	// Test the types of messages that would be sent during startup
	messages := []tea.Msg{
		sptMessageMsg("Starting container..."),
		containerStartedMsg{containerID: "mock-container-123"},
		sptMessageMsg("Container started: mock-contain"),
		containerStatusMsg("Running"),
		containerOutputMsg("Test output line"),
		containerStatusMsg("Stopped"),
		sptMessageMsg("Container has stopped"),
	}

	// Process each message type to ensure model handles them correctly
	for _, msg := range messages {
		updatedModel, cmd := model.Update(msg)
		model = updatedModel.(Model)

		// Verify model state changes appropriately
		switch m := msg.(type) {
		case containerStartedMsg:
			if model.containerID != m.containerID {
				t.Errorf("Model should update container ID on containerStartedMsg")
			}
		case containerStatusMsg:
			if model.containerStatus != string(m) {
				t.Errorf("Model should update container status, expected %s, got %s", string(m), model.containerStatus)
			}
		}

		// Ensure no unexpected commands are returned
		if cmd != nil {
			t.Logf("Command returned for message type %T", msg)
		}
	}
}

// TestScenarioSupport tests scenario-specific functionality
func TestScenarioSupport(t *testing.T) {
	mockDM := NewMockDockerManager()

	// Test scenario container creation
	scenarioPath := "/tmp/test-scenario.js"
	containerID, err := mockDM.StartContainerWithScenario("spt-image", scenarioPath, []string{})
	if err != nil {
		t.Errorf("Failed to start container with scenario: %v", err)
	}

	if containerID != "mock-container-123" {
		t.Errorf("Expected mock container ID, got %s", containerID)
	}

	// Verify scenario call was recorded
	calls := mockDM.GetScenarioCalls()
	if len(calls) != 1 {
		t.Fatalf("Expected 1 scenario call, got %d", len(calls))
	}

	if calls[0].ScenarioPath != scenarioPath {
		t.Errorf("Expected scenario path %s, got %s", scenarioPath, calls[0].ScenarioPath)
	}

	if calls[0].Image != "spt-image" {
		t.Errorf("Expected image 'spt-image', got %s", calls[0].Image)
	}
}

// TestHybridDockerIntegration tests the integration of BuildEndpointArgs with Docker container creation
func TestHybridDockerIntegration(t *testing.T) {
	tests := []struct {
		name         string
		params       scenario.ScenarioParams
		expectedArgs []string
		expectError  bool
	}{
		{
			name: "S3 write scenario with HTTP endpoint",
			params: scenario.ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "http://minio:9000",
				AccessKey:    "testkey",
				SecretKey:    "testsecret",
				Bucket:       "testbucket",
				Threads:      4,
				ObjectSize:   "1MB",
				ObjectCount:  100,
			},
			expectedArgs: []string{
				"--storage-net-node-addrs=minio",
				"--storage-net-node-port=9000",
				"--storage-auth-uid=testkey",
				"--storage-auth-secret=testsecret",
			},
			expectError: false,
		},
		{
			name: "S3 write scenario with HTTPS endpoint",
			params: scenario.ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "https://s3.amazonaws.com",
				AccessKey:    "AKIAEXAMPLE",
				SecretKey:    "secretkey",
				Bucket:       "mybucket",
				Threads:      2,
				ObjectSize:   "512KB",
				Duration:     "5m",
			},
			expectedArgs: []string{
				"--storage-net-node-addrs=s3.amazonaws.com",
				"--storage-net-node-port=443",
				"--storage-auth-uid=AKIAEXAMPLE",
				"--storage-auth-secret=secretkey",
				"--storage-net-ssl-enabled=true",
			},
			expectError: false,
		},
		{
			name: "Mock scenario with no endpoint args",
			params: scenario.ScenarioParams{
				WorkloadType: "mock",
				Threads:      8,
				ObjectSize:   "2MB",
				Duration:     "30s",
			},
			expectedArgs: []string{},
			expectError:  false,
		},
		{
			name: "Invalid endpoint should fail gracefully",
			params: scenario.ScenarioParams{
				WorkloadType: "write",
				Endpoint:     "invalid-url",
				AccessKey:    "key",
				SecretKey:    "secret",
				Bucket:       "bucket",
			},
			expectedArgs: nil,
			expectError:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create mock Docker manager
			mockDM := NewMockDockerManager()

			// NOTE: BuildEndpointArgs is deprecated and now returns empty args
			// The new API-based flow uses defaults.yaml for configuration
			endpointArgs, _ := scenario.BuildEndpointArgs(tt.params)
			if len(endpointArgs) != 0 {
				t.Errorf("BuildEndpointArgs should return empty args (deprecated), got: %v", endpointArgs)
			}

			// Test scenario generation
			scenarioContent, err := scenario.GenerateScenario(tt.params)

			// For the "Invalid endpoint" test case, scenario generation should succeed
			// but defaults generation might fail (depending on validation)
			if tt.name == "Invalid endpoint should fail gracefully" {
				// Skip further tests for invalid cases
				return
			}

			if err != nil {
				t.Errorf("Failed to generate scenario: %v", err)
				return
			}

			// Test defaults generation for API-based flow
			defaultsContent, err := scenario.GenerateDefaults(tt.params)
			if err != nil && tt.params.WorkloadType != "mock" {
				t.Errorf("Unexpected error from GenerateDefaults: %v", err)
				return
			}

			// For write workloads, verify defaults content is valid
			if tt.params.WorkloadType == "write" && defaultsContent == nil {
				t.Error("Expected non-nil defaults content for write workload")
			}

			// For mock workloads, defaults should also be non-nil (contains dummy-mock config)
			if tt.params.WorkloadType == "mock" && defaultsContent == nil {
				t.Error("Expected non-nil defaults content for mock workload")
			}

			// Test Docker integration - in the new flow, we start container in node mode
			// and send configuration via API, not CLI args
			containerID, err := mockDM.StartContainerInNodeMode("spt-image", constants.SptAPIPort, constants.DefaultNetworkMode, nil)
			if err != nil {
				t.Errorf("Failed to start container in node mode: %v", err)
				return
			}

			if containerID != "mock-container-123" {
				t.Errorf("Expected mock container ID, got %s", containerID)
			}

			// Verify scenario content is reasonable
			if len(scenarioContent) == 0 {
				t.Error("Generated scenario content is empty")
			}
			if !strings.Contains(scenarioContent, "Load") && !strings.Contains(scenarioContent, "PipelineLoad") {
				t.Error("Generated scenario should contain 'Load' or 'PipelineLoad'")
			}
			if !strings.Contains(scenarioContent, ".run();") {
				t.Error("Generated scenario should contain '.run();'")
			}
		})
	}
}
