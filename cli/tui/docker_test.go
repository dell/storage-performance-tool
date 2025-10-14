/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
)

// Note: Full mocking of Docker client would require implementing all the interface methods
// For brevity, we're focusing on testing the logic that doesn't require Docker daemon

func TestDockerManager_NewDockerManager(t *testing.T) {
	// Test actual Docker client creation
	dm, err := NewDockerManager()
	if err != nil {
		// Only skip if Docker is actually not available
		t.Skipf("Docker not available: %v", err)
	}

	// Ensure we clean up resources
	defer dm.Close()

	// Verify the DockerManager was created properly
	if dm == nil {
		t.Fatal("NewDockerManager() returned nil DockerManager")
	}

	if dm.client == nil {
		t.Error("DockerManager has nil Docker client")
	}

	if dm.ctx == nil {
		t.Error("DockerManager has nil context")
	}

	if dm.cancel == nil {
		t.Error("DockerManager has nil cancel function")
	}

	// Test that we can query Docker version to verify the client works
	if _, err = dm.GetDockerVersion(dm.ctx); err != nil {
		t.Errorf("GetDockerVersion failed: %v", err)
	}

	// Verify HostInfo is nil for legacy single-host mode
	if dm.HostInfo() != nil {
		t.Error("HostInfo() should return nil for legacy DockerManager")
	}
}

func TestDockerManager_ContainerID(t *testing.T) {
	dm := &DockerManager{
		containerID: "test-container-123",
	}

	if got := dm.ContainerID(); got != "test-container-123" {
		t.Errorf("ContainerID() = %v, want %v", got, "test-container-123")
	}
}

func TestDockerManager_Close(t *testing.T) {
	cancelCalled := false
	dm := &DockerManager{
		cancel: func() { cancelCalled = true },
		client: nil, // Will be nil in test
	}

	dm.Close()

	if !cancelCalled {
		t.Error("cancel function was not called")
	}
}

// Test message types
func TestContainerMessages(t *testing.T) {
	tests := []struct {
		name    string
		msg     tea.Msg
		msgType string
		content string
	}{
		{
			name:    "container output message",
			msg:     containerOutputMsg("test output"),
			msgType: "output",
			content: "test output",
		},
		{
			name:    "spt message",
			msg:     sptMessageMsg("error occurred"),
			msgType: "spt",
			content: "error occurred",
		},
		{
			name:    "container status message",
			msg:     containerStatusMsg("Running"),
			msgType: "status",
			content: "Running",
		},
		{
			name:    "container started message",
			msg:     containerStartedMsg{containerID: "abc123"},
			msgType: "started",
			content: "abc123",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			switch msg := tt.msg.(type) {
			case containerOutputMsg:
				if string(msg) != tt.content {
					t.Errorf("got %v, want %v", string(msg), tt.content)
				}
			case sptMessageMsg:
				if string(msg) != tt.content {
					t.Errorf("got %v, want %v", string(msg), tt.content)
				}
			case containerStatusMsg:
				if string(msg) != tt.content {
					t.Errorf("got %v, want %v", string(msg), tt.content)
				}
			case containerStartedMsg:
				if msg.containerID != tt.content {
					t.Errorf("got %v, want %v", msg.containerID, tt.content)
				}
			}
		})
	}
}

// Test helper functions
func TestFormatDuration(t *testing.T) {
	tests := []struct {
		name     string
		duration time.Duration
		want     string
	}{
		{
			name:     "zero duration",
			duration: 0,
			want:     "00:00:00",
		},
		{
			name:     "seconds only",
			duration: 45 * time.Second,
			want:     "00:00:45",
		},
		{
			name:     "minutes and seconds",
			duration: 2*time.Minute + 30*time.Second,
			want:     "00:02:30",
		},
		{
			name:     "hours, minutes and seconds",
			duration: 1*time.Hour + 15*time.Minute + 45*time.Second,
			want:     "01:15:45",
		},
		{
			name:     "large duration",
			duration: 25*time.Hour + 30*time.Minute + 15*time.Second,
			want:     "25:30:15",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := formatDuration(tt.duration); got != tt.want {
				t.Errorf("formatDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMinMax(t *testing.T) {
	tests := []struct {
		name string
		a, b int
		min  int
		max  int
	}{
		{"positive numbers", 5, 10, 5, 10},
		{"negative numbers", -10, -5, -10, -5},
		{"mixed numbers", -5, 5, -5, 5},
		{"equal numbers", 7, 7, 7, 7},
		{"zero", 0, 5, 0, 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := min(tt.a, tt.b); got != tt.min {
				t.Errorf("min(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.min)
			}
			if got := max(tt.a, tt.b); got != tt.max {
				t.Errorf("max(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.max)
			}
		})
	}
}

// Test cleanup with no container ID
func TestDockerManager_Cleanup_NoContainer(t *testing.T) {
	dm := &DockerManager{
		containerID: "",
	}

	err := dm.Cleanup()
	if err != nil {
		t.Errorf("Cleanup() with no container ID returned error: %v", err)
	}
}
