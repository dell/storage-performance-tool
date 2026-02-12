/*
Copyright © 2025 Dell Technologies
*/

package command

import (
	"reflect"
	"testing"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

func TestDockerCommandBuilderImpl_BuildRunCommand(t *testing.T) {
	builder := NewDockerCommandBuilder()

	tests := []struct {
		name     string
		config   ContainerConfig
		expected []string
	}{
		{
			name:   "minimal configuration",
			config: CreateMinimalContainerConfig(),
			expected: []string{
				"docker", "run", constants.DefaultSptImage,
			},
		},
		{
			name: "detached container with name",
			config: ContainerConfig{
				Image:    constants.DefaultSptImage,
				Name:     "test-container",
				Detached: true,
			},
			expected: []string{
				"docker", "run", "-d", "--name", "test-container", constants.DefaultSptImage,
			},
		},
		{
			name: "container with environment variables",
			config: ContainerConfig{
				Image: constants.DefaultSptImage,
				Environment: map[string]string{
					"ENV1": "value1",
					"ENV2": "value2",
				},
			},
			expected: []string{
				"docker", "run", "-e", "ENV1=value1", "-e", "ENV2=value2", constants.DefaultSptImage,
			},
		},
		{
			name: "container with host networking",
			config: ContainerConfig{
				Image:       constants.DefaultSptImage,
				NetworkMode: NetworkModeHost,
			},
			expected: []string{
				"docker", "run", "--network", "host", constants.DefaultSptImage,
			},
		},
		{
			name: "container with bridge networking and port mappings",
			config: ContainerConfig{
				Image:       constants.DefaultSptImage,
				NetworkMode: NetworkModeBridge,
				PortMappings: []PortMapping{
					{HostPort: 8080, ContainerPort: 8080, Protocol: "tcp"},
					{HostPort: 9090, ContainerPort: 9090, Protocol: "udp"},
				},
			},
			expected: []string{
				"docker", "run", "-p", "8080:8080/tcp", "-p", "9090:9090/udp", constants.DefaultSptImage,
			},
		},
		{
			name: "container with default protocol port mapping",
			config: ContainerConfig{
				Image:       constants.DefaultSptImage,
				NetworkMode: NetworkModeBridge,
				PortMappings: []PortMapping{
					{HostPort: 3000, ContainerPort: 3000}, // No protocol specified
				},
			},
			expected: []string{
				"docker", "run", "-p", "3000:3000/tcp", constants.DefaultSptImage,
			},
		},
		{
			name: "container with custom command",
			config: ContainerConfig{
				Image:   constants.DefaultSptImage,
				Command: []string{"--help", "--verbose"},
			},
			expected: []string{
				"docker", "run", constants.DefaultSptImage, "--help", "--verbose",
			},
		},
		{
			name: "full configuration",
			config: ContainerConfig{
				Image:       constants.DefaultSptImage,
				Name:        "full-test",
				NetworkMode: NetworkModeBridge,
				PortMappings: []PortMapping{
					{HostPort: 1099, ContainerPort: 1099, Protocol: "tcp"},
					{HostPort: 9999, ContainerPort: 9999, Protocol: "tcp"},
				},
				Environment: map[string]string{
					"JAVA_OPTS": "-Djava.rmi.server.hostname=localhost",
					"DEBUG":     "true",
				},
				Labels: map[string]string{
					"role":  "verify",
					"owner": "qa",
				},
				Command:  []string{"--run-node=true"},
				Detached: true,
			},
			expected: []string{
				"docker", "run", "-d", "--name", "full-test",
				"-e", "JAVA_OPTS=-Djava.rmi.server.hostname=localhost",
				"-e", "DEBUG=true",
				"--label", "owner=qa",
				"--label", "role=verify",
				"-p", "1099:1099/tcp", "-p", "9999:9999/tcp",
				constants.DefaultSptImage, "--run-node=true",
			},
		},
		{
			name: "container with RDMA device passthrough",
			config: ContainerConfig{
				Image:       constants.DefaultSptImage,
				Name:        "rdma-worker",
				NetworkMode: NetworkModeHost,
				Detached:    true,
				Devices:     []string{constants.RdmaDevicePath},
				CapAdd:      []string{constants.RdmaCapIpcLock},
				Ulimits:     []string{constants.RdmaUlimitMemlock},
			},
			expected: []string{
				"docker", "run", "-d", "--name", "rdma-worker",
				"--network", "host",
				"--device", constants.RdmaDevicePath,
				"--cap-add", constants.RdmaCapIpcLock,
				"--ulimit", constants.RdmaUlimitMemlock,
				constants.DefaultSptImage,
			},
		},
		{
			name: "empty container name",
			config: ContainerConfig{
				Image:    constants.DefaultSptImage,
				Name:     "", // Empty name should be skipped
				Detached: true,
			},
			expected: []string{
				"docker", "run", "-d", constants.DefaultSptImage,
			},
		},
		{
			name: "multiple port mappings",
			config: ContainerConfig{
				Image:       constants.DefaultSptImage,
				NetworkMode: NetworkModeBridge,
				PortMappings: []PortMapping{
					{HostPort: 40000, ContainerPort: 40000, Protocol: "tcp"},
					{HostPort: 40001, ContainerPort: 40001, Protocol: "tcp"},
					{HostPort: 40002, ContainerPort: 40002, Protocol: "tcp"},
					{HostPort: 40003, ContainerPort: 40003, Protocol: "tcp"},
					{HostPort: 40004, ContainerPort: 40004, Protocol: "tcp"},
				},
			},
			expected: []string{
				"docker", "run",
				"-p", "40000:40000/tcp",
				"-p", "40001:40001/tcp",
				"-p", "40002:40002/tcp",
				"-p", "40003:40003/tcp",
				"-p", "40004:40004/tcp",
				constants.DefaultSptImage,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := builder.BuildRunCommand(tt.config)

			// For tests with environment variables (which have unpredictable order),
			// we need to check the elements are present rather than exact order
			if len(tt.config.Environment) > 0 {
				assertCommandContainsElements(t, result, tt.expected, tt.name)
			} else {
				if !reflect.DeepEqual(result, tt.expected) {
					t.Errorf("BuildRunCommand() = %v, want %v", result, tt.expected)
				}
			}

			// Validate Docker command structure
			if err := AssertDockerCommand(result); err != nil {
				t.Errorf("Invalid Docker command structure: %v", err)
			}
		})
	}
}

func TestDockerCommandBuilderImpl_BuildWorkerNodeCommand(t *testing.T) {
	builder := NewDockerCommandBuilder()

	tests := []struct {
		name          string
		image         string
		containerName string
		rmiHostname   string
		rmiPortStart  int
		rmiPortCount  int
		wantContains  []string // Elements that should be present
		wantPortMaps  int      // Expected number of -p mappings (host net => 0)
	}{
		{
			name:          "basic worker node",
			image:         constants.DefaultSptImage,
			containerName: "worker-1",
			rmiHostname:   "localhost",
			rmiPortStart:  40000,
			rmiPortCount:  3,
			wantContains: []string{
				"docker", "run", "-d", "--name", "worker-1",
				"-e", "JAVA_OPTS=-Djava.rmi.server.hostname=localhost",
				"-e", "JAVA_TOOL_OPTIONS=-Djava.rmi.server.hostname=localhost",
				"--network", "host",
				constants.DefaultSptImage, "--run-node=true", "--run-port=9999", "--load-step-node-port=1099",
			},
			wantPortMaps: 0,
		},
		{
			name:          "worker node with zero RMI ports",
			image:         constants.DefaultSptImage,
			containerName: "worker-zero",
			rmiHostname:   "192.168.1.100",
			rmiPortStart:  40000,
			rmiPortCount:  0,
			wantContains: []string{
				"docker", "run", "-d", "--name", "worker-zero",
				"-e", "JAVA_OPTS=-Djava.rmi.server.hostname=192.168.1.100",
				"-e", "JAVA_TOOL_OPTIONS=-Djava.rmi.server.hostname=192.168.1.100",
				"--network", "host",
				constants.DefaultSptImage, "--run-node=true", "--run-port=9999", "--load-step-node-port=1099",
			},
			wantPortMaps: 0,
		},
		{
			name:          "worker node with large port range",
			image:         constants.DefaultSptImage,
			containerName: "worker-large",
			rmiHostname:   "host.docker.internal",
			rmiPortStart:  50000,
			rmiPortCount:  10,
			wantContains: []string{
				"docker", "run", "-d", "--name", "worker-large",
				"-e", "JAVA_OPTS=-Djava.rmi.server.hostname=host.docker.internal",
				"-e", "JAVA_TOOL_OPTIONS=-Djava.rmi.server.hostname=host.docker.internal",
				"--network", "host",
				constants.DefaultSptImage, "--run-node=true", "--run-port=9999", "--load-step-node-port=1099",
			},
			wantPortMaps: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := builder.BuildWorkerNodeCommand(tt.image, tt.containerName, tt.rmiHostname, tt.rmiPortStart, tt.rmiPortCount)

			// Check that all expected elements are present
			for _, expected := range tt.wantContains {
				if !containsElement(result, expected) {
					t.Errorf("BuildWorkerNodeCommand() missing expected element %q in result %v", expected, result)
				}
			}

			// Validate Docker command structure
			if err := AssertDockerCommand(result); err != nil {
				t.Errorf("Invalid Docker command structure: %v", err)
			}

			// Verify expected port mapping count
			portMappingCount := 0
			for i, arg := range result {
				if arg == "-p" && i+1 < len(result) {
					portMappingCount++
				}
			}
			if portMappingCount != tt.wantPortMaps {
				t.Errorf("Expected %d port mappings, got %d", tt.wantPortMaps, portMappingCount)
			}
		})
	}
}

func TestDockerCommandBuilderImpl_BuildEntryNodeCommand(t *testing.T) {
	builder := NewDockerCommandBuilder()

	tests := []struct {
		name            string
		image           string
		containerName   string
		workerAddresses []string
		networkMode     NetworkMode
		expected        []string
	}{
		{
			name:            "entry node with no workers - bridge mode",
			image:           constants.DefaultSptImage,
			containerName:   "entry-1",
			workerAddresses: []string{},
			networkMode:     NetworkModeBridge,
			expected: []string{
				"docker", "run", "-d", "--name", "entry-1",
				"-p", "9999:9999/tcp",
				constants.DefaultSptImage, "--run-node=true",
			},
		},
		{
			name:            "entry node with single worker - bridge mode",
			image:           constants.DefaultSptImage,
			containerName:   "entry-2",
			workerAddresses: []string{"worker1:9999"},
			networkMode:     NetworkModeBridge,
			expected: []string{
				"docker", "run", "-d", "--name", "entry-2",
				"-p", "9999:9999/tcp",
				constants.DefaultSptImage, "--load-step-node-addrs=worker1:9999", "--run-node=true",
			},
		},
		{
			name:            "entry node with multiple workers - bridge mode",
			image:           constants.DefaultSptImage,
			containerName:   "entry-3",
			workerAddresses: []string{"worker1:9999", "worker2:9999", "worker3:9999"},
			networkMode:     NetworkModeBridge,
			expected: []string{
				"docker", "run", "-d", "--name", "entry-3",
				"-p", "9999:9999/tcp",
				constants.DefaultSptImage, "--load-step-node-addrs=worker1:9999,worker2:9999,worker3:9999", "--run-node=true",
			},
		},
		{
			name:            "entry node with workers - host mode",
			image:           constants.DefaultSptImage,
			containerName:   "entry-host",
			workerAddresses: []string{"worker1:9999", "worker2:9999"},
			networkMode:     NetworkModeHost,
			expected: []string{
				"docker", "run", "-d", "--name", "entry-host",
				"--network", "host",
				constants.DefaultSptImage, "--load-step-node-addrs=worker1:9999,worker2:9999", "--run-node=true",
			},
		},
		{
			name:            "entry node with nil workers",
			image:           constants.DefaultSptImage,
			containerName:   "entry-nil",
			workerAddresses: nil,
			networkMode:     NetworkModeBridge,
			expected: []string{
				"docker", "run", "-d", "--name", "entry-nil",
				"-p", "9999:9999/tcp",
				constants.DefaultSptImage, "--run-node=true",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := builder.BuildEntryNodeCommand(tt.image, tt.containerName, tt.workerAddresses, tt.networkMode)

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("BuildEntryNodeCommand() = %v, want %v", result, tt.expected)
			}

			// Validate Docker command structure
			if err := AssertDockerCommand(result); err != nil {
				t.Errorf("Invalid Docker command structure: %v", err)
			}
		})
	}
}

func TestBuildPortMapping(t *testing.T) {
	tests := []struct {
		name          string
		hostPort      int
		containerPort int
		expected      PortMapping
	}{
		{
			name:          "standard port mapping",
			hostPort:      8080,
			containerPort: 8080,
			expected: PortMapping{
				HostPort:      8080,
				ContainerPort: 8080,
				Protocol:      constants.DefaultProtocol,
			},
		},
		{
			name:          "different host and container ports",
			hostPort:      9090,
			containerPort: 8080,
			expected: PortMapping{
				HostPort:      9090,
				ContainerPort: 8080,
				Protocol:      constants.DefaultProtocol,
			},
		},
		{
			name:          "zero ports",
			hostPort:      0,
			containerPort: 0,
			expected: PortMapping{
				HostPort:      0,
				ContainerPort: 0,
				Protocol:      constants.DefaultProtocol,
			},
		},
		{
			name:          "high port numbers",
			hostPort:      65535,
			containerPort: 65534,
			expected: PortMapping{
				HostPort:      65535,
				ContainerPort: 65534,
				Protocol:      constants.DefaultProtocol,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildPortMapping(tt.hostPort, tt.containerPort)
			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("BuildPortMapping() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestBuildRMIPortMappings(t *testing.T) {
	tests := []struct {
		name      string
		portStart int
		portCount int
		expected  []PortMapping
	}{
		{
			name:      "single port",
			portStart: 40000,
			portCount: 1,
			expected: []PortMapping{
				{HostPort: 40000, ContainerPort: 40000, Protocol: constants.DefaultProtocol},
			},
		},
		{
			name:      "multiple ports",
			portStart: 50000,
			portCount: 3,
			expected: []PortMapping{
				{HostPort: 50000, ContainerPort: 50000, Protocol: constants.DefaultProtocol},
				{HostPort: 50001, ContainerPort: 50001, Protocol: constants.DefaultProtocol},
				{HostPort: 50002, ContainerPort: 50002, Protocol: constants.DefaultProtocol},
			},
		},
		{
			name:      "zero ports",
			portStart: 40000,
			portCount: 0,
			expected:  []PortMapping{},
		},
		{
			name:      "large port range",
			portStart: 60000,
			portCount: 100,
			expected:  make([]PortMapping, 100),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := BuildRMIPortMappings(tt.portStart, tt.portCount)

			if len(result) != tt.portCount {
				t.Errorf("BuildRMIPortMappings() returned %d ports, want %d", len(result), tt.portCount)
			}

			// For large port range test, generate expected result
			if tt.name == "large port range" {
				for i := 0; i < 100; i++ {
					tt.expected[i] = PortMapping{
						HostPort:      60000 + i,
						ContainerPort: 60000 + i,
						Protocol:      constants.DefaultProtocol,
					}
				}
			}

			if !reflect.DeepEqual(result, tt.expected) {
				t.Errorf("BuildRMIPortMappings() = %v, want %v", result, tt.expected)
			}

			// Verify port sequence is correct
			for i, mapping := range result {
				expectedPort := tt.portStart + i
				if mapping.HostPort != expectedPort || mapping.ContainerPort != expectedPort {
					t.Errorf("Port mapping at index %d: got host=%d container=%d, want host=%d container=%d",
						i, mapping.HostPort, mapping.ContainerPort, expectedPort, expectedPort)
				}
				if mapping.Protocol != constants.DefaultProtocol {
					t.Errorf("Port mapping at index %d: got protocol=%s, want %s",
						i, mapping.Protocol, constants.DefaultProtocol)
				}
			}
		})
	}
}

func TestDockerCommandBuilderImpl_BuildWorkerNodeCommand_RdmaPassthrough(t *testing.T) {
	// Set SPT_RDMA=true to trigger RDMA device passthrough
	t.Setenv("SPT_RDMA", "true")

	builder := NewDockerCommandBuilder()
	result := builder.BuildWorkerNodeCommand(constants.DefaultSptImage, "rdma-worker", "10.247.128.125", 40000, 3)

	// Verify RDMA device passthrough is present
	wantElements := []string{
		"--device", constants.RdmaDevicePath,
		"--cap-add", constants.RdmaCapIpcLock,
		"--ulimit", constants.RdmaUlimitMemlock,
	}
	for _, elem := range wantElements {
		if !containsElement(result, elem) {
			t.Errorf("BuildWorkerNodeCommand() with SPT_RDMA=true missing %q in result %v", elem, result)
		}
	}

	if err := AssertDockerCommand(result); err != nil {
		t.Errorf("Invalid Docker command structure: %v", err)
	}
}

func TestDockerCommandBuilderImpl_BuildWorkerNodeCommand_NoRdmaByDefault(t *testing.T) {
	// Ensure SPT_RDMA is not set
	t.Setenv("SPT_RDMA", "")

	builder := NewDockerCommandBuilder()
	result := builder.BuildWorkerNodeCommand(constants.DefaultSptImage, "worker-noardma", "localhost", 40000, 3)

	// Verify RDMA flags are NOT present
	forbiddenElements := []string{
		"--device", "--cap-add", "--ulimit",
	}
	for _, elem := range forbiddenElements {
		if containsElement(result, elem) {
			t.Errorf("BuildWorkerNodeCommand() without SPT_RDMA should not contain %q, got %v", elem, result)
		}
	}
}

// Helper functions for testing

// containsElement checks if a slice contains a specific element
func containsElement(slice []string, element string) bool {
	for _, item := range slice {
		if item == element {
			return true
		}
	}
	return false
}

// assertCommandContainsElements checks that all expected elements are present in the result
// This is useful for commands with environment variables where order is not guaranteed
func assertCommandContainsElements(t *testing.T, result, expected []string, testName string) {
	t.Helper()
	for _, expectedElement := range expected {
		if !containsElement(result, expectedElement) {
			t.Errorf("%s: BuildRunCommand() missing expected element %q in result %v", testName, expectedElement, result)
		}
	}

	// Also check that the lengths are the same (no extra elements)
	if len(result) != len(expected) {
		t.Errorf("%s: BuildRunCommand() length mismatch: got %d elements, want %d elements", testName, len(result), len(expected))
	}
}
