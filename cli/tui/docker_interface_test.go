/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMockDockerManager(t *testing.T) {
	t.Run("basic container operations", func(t *testing.T) {
		mock := NewMockDockerManager()

		// Test ContainerID
		if id := mock.ContainerID(); id != "mock-container-123" {
			t.Errorf("Expected container ID 'mock-container-123', got %s", id)
		}

		// Test StartContainer
		containerID, err := mock.StartContainer("test-image", []string{"--help"})
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if containerID != "mock-container-123" {
			t.Errorf("Expected container ID 'mock-container-123', got %s", containerID)
		}

		// Verify call was recorded
		if count := mock.GetStartCallCount(); count != 1 {
			t.Errorf("Expected 1 start call, got %d", count)
		}

		calls := mock.GetContainerCalls()
		if len(calls) != 1 {
			t.Fatalf("Expected 1 container call, got %d", len(calls))
		}
		if calls[0].Image != "test-image" {
			t.Errorf("Expected image 'test-image', got %s", calls[0].Image)
		}
		if len(calls[0].Cmd) != 1 || calls[0].Cmd[0] != "--help" {
			t.Errorf("Expected command ['--help'], got %v", calls[0].Cmd)
		}
	})

	t.Run("scenario container operations", func(t *testing.T) {
		mock := NewMockDockerManager()

		// Test StartContainerWithScenario
		containerID, err := mock.StartContainerWithScenario("spt-image", "/tmp/scenario.js", []string{})
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if containerID != "mock-container-123" {
			t.Errorf("Expected container ID 'mock-container-123', got %s", containerID)
		}

		// Verify scenario call was recorded
		scenarioCalls := mock.GetScenarioCalls()
		if len(scenarioCalls) != 1 {
			t.Fatalf("Expected 1 scenario call, got %d", len(scenarioCalls))
		}
		if scenarioCalls[0].Image != "spt-image" {
			t.Errorf("Expected image 'spt-image', got %s", scenarioCalls[0].Image)
		}
		if scenarioCalls[0].ScenarioPath != "/tmp/scenario.js" {
			t.Errorf("Expected scenario path '/tmp/scenario.js', got %s", scenarioCalls[0].ScenarioPath)
		}
	})

	t.Run("stream output", func(t *testing.T) {
		mock := NewMockDockerManager()
		mock.SetOutputLines([]string{
			"Line 1",
			"Line 2",
			"Line 3",
		})
		mock.SetOutputDelay(5 * time.Millisecond)

		var receivedLines []string
		var mu sync.Mutex

		// Test StreamOutput
		mock.StreamOutput("test-container", func(line string) {
			mu.Lock()
			receivedLines = append(receivedLines, line)
			mu.Unlock()
		}, func(line string) {
			t.Errorf("Unexpected stderr: %s", line)
		})

		// Wait for all lines to be received
		time.Sleep(50 * time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		if len(receivedLines) != 3 {
			t.Errorf("Expected 3 lines, got %d", len(receivedLines))
		}

		for i, expected := range []string{"Line 1", "Line 2", "Line 3"} {
			if i < len(receivedLines) && receivedLines[i] != expected {
				t.Errorf("Line %d: expected %q, got %q", i, expected, receivedLines[i])
			}
		}

		if count := mock.GetStreamCallCount(); count != 1 {
			t.Errorf("Expected 1 stream call, got %d", count)
		}
	})

	t.Run("error scenarios", func(t *testing.T) {
		mock := NewMockDockerManager()

		// Test start failure
		mock.SetFailOnStart(true)
		_, err := mock.StartContainer("test-image", []string{"cmd"})
		if err == nil {
			t.Error("Expected error on start, got nil")
		}
		if !strings.Contains(err.Error(), "failed to start container") {
			t.Errorf("Expected error message about start failure, got: %v", err)
		}

		// Test scenario start failure
		_, err = mock.StartContainerWithScenario("test-image", "/tmp/scenario.js", []string{})
		if err == nil {
			t.Error("Expected error on scenario start, got nil")
		}

		// Test cleanup failure
		mock.SetFailOnStart(false)
		mock.SetFailOnCleanup(true)
		err = mock.Cleanup()
		if err == nil {
			t.Error("Expected error on cleanup, got nil")
		}
		if !strings.Contains(err.Error(), "failed to cleanup container") {
			t.Errorf("Expected error message about cleanup failure, got: %v", err)
		}
	})

	t.Run("cleanup and close", func(t *testing.T) {
		mock := NewMockDockerManager()
		mock.SetContainerID("custom-id")

		// Test successful cleanup
		err := mock.Cleanup()
		if err != nil {
			t.Errorf("Unexpected error on cleanup: %v", err)
		}

		// Container ID should be cleared
		if id := mock.ContainerID(); id != "" {
			t.Errorf("Expected empty container ID after cleanup, got %s", id)
		}

		if count := mock.GetCleanupCallCount(); count != 1 {
			t.Errorf("Expected 1 cleanup call, got %d", count)
		}

		// Test Close
		mock.Close()
		if count := mock.GetCloseCallCount(); count != 1 {
			t.Errorf("Expected 1 close call, got %d", count)
		}
	})

	t.Run("streaming callback", func(t *testing.T) {
		mock := NewMockDockerManager()

		var calledWithID string
		mock.SetOnStreamOutput(func(containerID string) {
			calledWithID = containerID
		})

		mock.StreamOutput("test-container-456", func(string) {}, func(string) {})

		// Give callback time to execute
		time.Sleep(10 * time.Millisecond)

		if calledWithID != "test-container-456" {
			t.Errorf("Expected callback with 'test-container-456', got %s", calledWithID)
		}
	})
}
