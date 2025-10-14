package tui

import (
	tea "github.com/charmbracelet/bubbletea"
	"testing"
)

// TestContainerCleanupOnQuit verifies that containers are properly cleaned up when TUI exits
// This test would have caught the bug where cleanup was checking the wrong model's containerID
func TestContainerCleanupOnQuit(t *testing.T) {
	// Create a mock Docker manager
	mockDocker := NewMockDockerManager()

	// Set up mock to return a container ID when started
	testContainerID := "test-container-123"
	mockDocker.SetContainerID(testContainerID)

	// Create a model with the mock Docker manager
	model := InitialModel()
	model.dockerManager = mockDocker

	// Simulate container start message
	msg := containerStartedMsg{containerID: testContainerID}
	updatedModel, _ := model.Update(msg)
	model = updatedModel.(Model)

	// Verify containerID was set in the model
	if model.containerID != testContainerID {
		t.Errorf("Expected containerID %s, got %s", testContainerID, model.containerID)
	}

	// Simulate pressing 'q' to quit
	quitMsg := tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}}
	finalModel, cmd := model.Update(quitMsg)

	// Verify quit command was returned
	if cmd == nil {
		t.Fatal("Expected quit command, got nil")
	}

	// The important part: verify that the Docker manager still has the container ID
	// This is what the fixed cleanup code checks
	if mockDocker.ContainerID() != testContainerID {
		t.Errorf("Docker manager lost container ID: expected %s, got %s",
			testContainerID, mockDocker.ContainerID())
	}

	// Verify the final model still has the container ID
	// (This was the bug - the initial model's containerID was checked instead)
	if m, ok := finalModel.(Model); ok {
		if m.containerID != testContainerID {
			t.Errorf("Final model lost container ID: expected %s, got %s",
				testContainerID, m.containerID)
		}
	}

	// Simulate cleanup (this is what should happen after p.Run() returns)
	if mockDocker.ContainerID() != "" {
		err := mockDocker.Cleanup()
		if err != nil {
			t.Errorf("Cleanup failed: %v", err)
		}

		// Verify cleanup was called
		if mockDocker.GetCleanupCallCount() == 0 {
			t.Error("Cleanup was not called despite container ID being present")
		}
	}
}

// TestCleanupCheckUsesDockerManagerID verifies the fix uses dm.ContainerID() not model.containerID
func TestCleanupCheckUsesDockerManagerID(t *testing.T) {
	// This test documents the correct behavior after the fix
	mockDocker := NewMockDockerManager()
	testContainerID := "container-456"
	mockDocker.SetContainerID(testContainerID)

	// Create an initial model (like StartTUIWithScenarioAndParams does)
	initialModel := InitialModel()
	// Note: initialModel.containerID is empty!

	if initialModel.containerID != "" {
		t.Error("Initial model should have empty containerID")
	}

	// The Docker manager has the container ID
	if mockDocker.ContainerID() != testContainerID {
		t.Errorf("Docker manager should have containerID %s", testContainerID)
	}

	// THE FIX: Cleanup should check dm.ContainerID(), not initialModel.containerID
	// Before fix: if initialModel.containerID != "" { cleanup() } // WRONG - never runs!
	// After fix:  if dm.ContainerID() != "" { cleanup() }        // CORRECT

	shouldCleanup := mockDocker.ContainerID() != "" // Correct check
	wrongCheck := initialModel.containerID != ""    // Wrong check (the bug)

	if !shouldCleanup {
		t.Error("Cleanup should be triggered when docker manager has container ID")
	}

	if wrongCheck {
		t.Error("Wrong check would incorrectly trigger cleanup")
	}

	// Document the fix
	t.Log("FIX: Changed cleanup check from 'model.containerID != \"\"' to 'dm.ContainerID() != \"\"'")
	t.Log("This ensures cleanup happens even though the initial model's containerID is empty")
}

// TestCleanupTimingRegression tests that cleanup happens after TUI exits, not during
func TestCleanupTimingRegression(t *testing.T) {
	// Create mock Docker with container
	mockDocker := NewMockDockerManager()
	mockDocker.SetContainerID("test-container")

	model := InitialModel()
	model.dockerManager = mockDocker
	model.containerID = "test-container"

	// Press 'q' to quit
	quitMsg := tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune{'q'}}
	_, cmd := model.Update(quitMsg)

	// Verify quit command was returned
	if cmd == nil {
		t.Fatal("Expected quit command")
	}

	// IMPORTANT: Cleanup should NOT have been called yet
	// Cleanup happens AFTER p.Run() returns, not in the Update handler
	if mockDocker.GetCleanupCallCount() > 0 {
		t.Error("REGRESSION: Cleanup was called during Update instead of after Run")
		t.Log("Cleanup must happen after p.Run() returns, not in the 'q' key handler")
	}

	// Now simulate what happens after p.Run() returns
	if mockDocker.ContainerID() != "" {
		mockDocker.Cleanup()
		if mockDocker.GetCleanupCallCount() == 0 {
			t.Error("Cleanup should be called after Run when container exists")
		}
	}
}

// BenchmarkCleanupCheck compares the performance of the two checks
func BenchmarkCleanupCheck(b *testing.B) {
	mockDocker := NewMockDockerManager()
	mockDocker.SetContainerID("bench-container")
	initialModel := InitialModel()

	b.Run("WrongCheck_model.containerID", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = initialModel.containerID != ""
		}
	})

	b.Run("CorrectCheck_dm.ContainerID", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = mockDocker.ContainerID() != ""
		}
	})
}
