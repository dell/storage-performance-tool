/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"context"
	"testing"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	containertypes "github.com/docker/docker/api/types/container"
)

func TestIsSptContainer(t *testing.T) {
	tests := []struct {
		name      string
		container *ContainerInfo
		expected  bool
	}{
		{
			name:      "nil container",
			container: nil,
			expected:  false,
		},
		{
			name: "ghcr.io/dell/storage-performance-tool image",
			container: &ContainerInfo{
				Image: "ghcr.io/dell/storage-performance-tool:latest",
				Name:  "test-container",
			},
			expected: true,
		},
		{
			name: "dell internal spt image",
			container: &ContainerInfo{
				Image: "ghcr.io/dell/storage-performance-tool:5.0.2",
				Name:  "spt-test",
			},
			expected: true,
		},
		{
			name: "generic spt image",
			container: &ContainerInfo{
				Image: "spt:latest",
				Name:  "my-container",
			},
			expected: true,
		},
		{
			name: "spt in container name",
			container: &ContainerInfo{
				Image: "custom-image:latest",
				Name:  "spt-benchmark-01",
			},
			expected: true,
		},
		{
			name: "spt in labels",
			container: &ContainerInfo{
				Image: "custom:latest",
				Name:  "test",
				Labels: map[string]string{
					"app":         "spt",
					"environment": "test",
				},
			},
			expected: true,
		},
		{
			name: "spt in label value",
			container: &ContainerInfo{
				Image: "custom:latest",
				Name:  "test",
				Labels: map[string]string{
					"application": "spt-benchmark",
				},
			},
			expected: true,
		},
		{
			name: "non-spt container",
			container: &ContainerInfo{
				Image: "nginx:latest",
				Name:  "web-server",
			},
			expected: false,
		},
		{
			name: "case insensitive image check",
			container: &ContainerInfo{
				Image: "DellSPT/SPT:latest",
				Name:  "test",
			},
			expected: true,
		},
		{
			name: "case insensitive name check",
			container: &ContainerInfo{
				Image: "custom:latest",
				Name:  "SPT-test",
			},
			expected: true,
		},
		{
			name: "partial match in image",
			container: &ContainerInfo{
				Image: "registry.company.com/tools/spt-runner:v1.0",
				Name:  "benchmark",
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsSptContainer(tt.container)
			if result != tt.expected {
				t.Errorf("IsSptContainer() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDockerContainerFinder_containerUsesPort(t *testing.T) {
	// Create a mock DockerContainerFinder (we only need the method)
	finder := &DockerContainerFinder{}

	tests := []struct {
		name      string
		container containertypes.Summary
		port      string
		expected  bool
	}{
		{
			name: "container uses public port",
			container: containertypes.Summary{
				Ports: []containertypes.Port{
					{PublicPort: 9999, PrivatePort: 9999, Type: "tcp"},
				},
			},
			port:     constants.SptAPIPort,
			expected: true,
		},
		{
			name: "container uses private port",
			container: containertypes.Summary{
				Ports: []containertypes.Port{
					{PublicPort: 0, PrivatePort: 9999, Type: "tcp"},
				},
			},
			port:     constants.SptAPIPort,
			expected: true,
		},
		{
			name: "container uses different port",
			container: containertypes.Summary{
				Ports: []containertypes.Port{
					{PublicPort: 8080, PrivatePort: 8080, Type: "tcp"},
				},
			},
			port:     constants.SptAPIPort,
			expected: false,
		},
		{
			name: "container has multiple ports, one matches",
			container: containertypes.Summary{
				Ports: []containertypes.Port{
					{PublicPort: 8080, PrivatePort: 8080, Type: "tcp"},
					{PublicPort: 9999, PrivatePort: 9999, Type: "tcp"},
					{PublicPort: 9000, PrivatePort: 9000, Type: "tcp"},
				},
			},
			port:     constants.SptAPIPort,
			expected: true,
		},
		{
			name:      "container has no ports",
			container: containertypes.Summary{},
			port:      constants.SptAPIPort,
			expected:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := finder.containerUsesPort(tt.container, tt.port)
			if result != tt.expected {
				t.Errorf("containerUsesPort() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDockerContainerFinder_getContainerName(t *testing.T) {
	finder := &DockerContainerFinder{}

	tests := []struct {
		name      string
		container containertypes.Summary
		expected  string
	}{
		{
			name: "container with name",
			container: containertypes.Summary{
				ID:    "abc123def456",
				Names: []string{"/spt-test-01"},
			},
			expected: "spt-test-01",
		},
		{
			name: "container with multiple names",
			container: containertypes.Summary{
				ID:    "abc123def456",
				Names: []string{"/primary-name", "/secondary-name"},
			},
			expected: "primary-name",
		},
		{
			name: "container with name without leading slash",
			container: containertypes.Summary{
				ID:    "abc123def456",
				Names: []string{"no-slash-name"},
			},
			expected: "no-slash-name",
		},
		{
			name: "container without names",
			container: containertypes.Summary{
				ID: "abc123def456789abcdef",
			},
			expected: "abc123def456", // Should return first 12 chars of ID
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := finder.getContainerName(tt.container)
			if result != tt.expected {
				t.Errorf("getContainerName() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestDockerContainerFinder_containerToInfo(t *testing.T) {
	finder := &DockerContainerFinder{}

	container := containertypes.Summary{
		ID:      "abc123def456789",
		Image:   "nginx:latest",
		State:   "running",
		Created: 1642678800, // Unix timestamp
		Names:   []string{"/web-server"},
		Ports: []containertypes.Port{
			{IP: "0.0.0.0", PublicPort: 8080, PrivatePort: 80, Type: "tcp"},
			{PrivatePort: 443, Type: "tcp"},
		},
		Labels: map[string]string{
			"environment": "test",
			"app":         "web",
		},
	}

	info := finder.containerToInfo(container)

	if info.ID != "abc123def456789" {
		t.Errorf("ID = %v, want %v", info.ID, "abc123def456789")
	}
	if info.Image != "nginx:latest" {
		t.Errorf("Image = %v, want %v", info.Image, "nginx:latest")
	}
	if info.State != "running" {
		t.Errorf("State = %v, want %v", info.State, "running")
	}
	if info.Name != "web-server" {
		t.Errorf("Name = %v, want %v", info.Name, "web-server")
	}

	// Check ports formatting
	expectedPorts := []string{"0.0.0.0:8080->80/tcp", "443/tcp"}
	if len(info.Ports) != len(expectedPorts) {
		t.Errorf("Ports length = %v, want %v", len(info.Ports), len(expectedPorts))
	} else {
		for i, port := range info.Ports {
			if port != expectedPorts[i] {
				t.Errorf("Ports[%d] = %v, want %v", i, port, expectedPorts[i])
			}
		}
	}

	// Check labels
	if len(info.Labels) != 2 {
		t.Errorf("Labels length = %v, want %v", len(info.Labels), 2)
	}
	if info.Labels["environment"] != "test" {
		t.Errorf("Labels[environment] = %v, want %v", info.Labels["environment"], "test")
	}

	// Check created time
	expectedTime := time.Unix(1642678800, 0)
	if !info.Created.Equal(expectedTime) {
		t.Errorf("Created = %v, want %v", info.Created, expectedTime)
	}
}

// Mock Docker API tests would require more complex setup
// For now, we test the logic methods that don't require actual Docker connection

func TestNewDockerContainerFinder_Integration(t *testing.T) {
	// This test checks if we can create a finder without Docker connection errors
	// Skip if Docker is not available
	finder, err := NewDockerContainerFinder()
	if err != nil {
		t.Skipf("Docker not available, skipping integration test: %v", err)
	}
	defer func() { _ = finder.Close() }()

	// Just verify we got a finder instance
	if finder == nil {
		t.Errorf("NewDockerContainerFinder() returned nil")
	} else if finder.client == nil {
		t.Errorf("DockerContainerFinder.client is nil")
	}
}

func TestHelperFunctions(t *testing.T) {
	t.Run("FindContainersByPort with no Docker", func(t *testing.T) {
		ctx := context.Background()

		// This will likely fail if Docker is not available, which is expected
		_, err := FindContainersByPort(ctx, constants.SptAPIPort)
		if err != nil {
			t.Skipf("Docker not available for helper function test: %v", err)
		}
		// If no error, Docker is available and function worked
	})

	t.Run("StopContainerByID with invalid ID", func(t *testing.T) {
		ctx := context.Background()

		// This should fail with invalid container ID
		err := StopContainerByID(ctx, "invalid-container-id")
		if err == nil {
			t.Errorf("StopContainerByID() with invalid ID should return error")
		}
	})
}

// Regression tests for specific scenarios
func TestDockerContainerFinder_EdgeCases(t *testing.T) {
	t.Run("containerUsesPort with string conversion", func(t *testing.T) {
		finder := &DockerContainerFinder{}

		container := containertypes.Summary{
			Ports: []containertypes.Port{
				{PublicPort: 9999, PrivatePort: 9999, Type: "tcp"},
			},
		}

		// Test with string port that should match
		if !finder.containerUsesPort(container, constants.SptAPIPort) {
			t.Errorf("Should match port 9999 as string")
		}

		// Test with port that shouldn't match
		if finder.containerUsesPort(container, "8080") {
			t.Errorf("Should not match port 8080")
		}

		// Test with zero ports (common case)
		containerWithZeroPort := containertypes.Summary{
			Ports: []containertypes.Port{
				{PublicPort: 0, PrivatePort: 9999, Type: "tcp"},
			},
		}

		if !finder.containerUsesPort(containerWithZeroPort, constants.SptAPIPort) {
			t.Errorf("Should match private port 9999 even when public port is 0")
		}
	})

	t.Run("getContainerName with edge cases", func(t *testing.T) {
		finder := &DockerContainerFinder{}

		// Empty names array with short ID
		container := containertypes.Summary{
			ID:    "short",
			Names: []string{},
		}
		result := finder.getContainerName(container)
		if result != "short" {
			t.Errorf("Should return full ID when names is empty and ID is short, got %v", result)
		}

		// Very long ID
		longContainer := containertypes.Summary{
			ID:    "verylongcontaineridshouldbetruncated123456789",
			Names: []string{},
		}
		result = finder.getContainerName(longContainer)
		if len(result) != 12 {
			t.Errorf("Should return first 12 chars of long ID, got %v (length %d)", result, len(result))
		}
		if result != "verylongcont" {
			t.Errorf("Should return 'verylongcont', got %v", result)
		}
	})
}
