/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"fmt"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// MockDockerManager is a mock implementation of DockerInterface for testing
type MockDockerManager struct {
	mu sync.Mutex

	// Configuration
	containerID      string
	failOnStart      bool
	failOnCleanup    bool
	outputLines      []string
	outputDelay      time.Duration
	streamCallCount  int
	startCallCount   int
	cleanupCallCount int
	closeCallCount   int
	fileMounts       []scenario.FileMount

	// For testing scenario support
	scenarioCalls []struct {
		Image          string
		ScenarioPath   string
		AdditionalArgs []string
	}

	// For testing regular container starts
	containerCalls []struct {
		Image string
		Cmd   []string
	}

	// For testing worker node starts
	workerNodeCalls []struct {
		Image          string
		RMIHostname    string
		RMIPortStart   int
		RMIPortCount   int
		AdditionalArgs []string
	}

	// For testing entry node starts
	entryNodeCalls []struct {
		Image           string
		WorkerAddresses []string
		AdditionalArgs  []string
		NetworkMode     string
	}

	// Callbacks for testing
	onStreamOutput func(containerID string)
}

// NewMockDockerManager creates a new mock Docker manager
func NewMockDockerManager() *MockDockerManager {
	return &MockDockerManager{
		containerID: "mock-container-123",
		outputLines: []string{
			"Starting Spt benchmark...",
			"| 2025-01-24 12:00:00 | 100.5 | 10.5 | 1.5 | 50.2 | 200.3 | 8 | 0 | 0 | 100 | 0 | 0 |",
			"Benchmark complete.",
		},
		outputDelay: 10 * time.Millisecond,
	}
}

// ContainerID returns the mock container ID
func (m *MockDockerManager) ContainerID() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.containerID
}

// SetFileMounts records configured file mounts.
func (m *MockDockerManager) SetFileMounts(mounts []scenario.FileMount) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.fileMounts = append([]scenario.FileMount(nil), mounts...)
	return nil
}

// StartContainer simulates starting a container
func (m *MockDockerManager) StartContainer(image string, cmd []string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.startCallCount++
	m.containerCalls = append(m.containerCalls, struct {
		Image string
		Cmd   []string
	}{
		Image: image,
		Cmd:   cmd,
	})

	if m.failOnStart {
		return "", fmt.Errorf("mock error: failed to start container")
	}

	return m.containerID, nil
}

// StartContainerWithScenario simulates starting a container with a scenario file
func (m *MockDockerManager) StartContainerWithScenario(image string, scenarioPath string, additionalArgs []string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.startCallCount++
	m.scenarioCalls = append(m.scenarioCalls, struct {
		Image          string
		ScenarioPath   string
		AdditionalArgs []string
	}{
		Image:          image,
		ScenarioPath:   scenarioPath,
		AdditionalArgs: additionalArgs,
	})

	if m.failOnStart {
		return "", fmt.Errorf("mock error: failed to start container with scenario")
	}

	return m.containerID, nil
}

// StartContainerInNodeMode simulates starting a container in API server mode.
func (m *MockDockerManager) StartContainerInNodeMode(image string, apiPort string, networkMode string, additionalArgs []string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.startCallCount++

	if m.failOnStart {
		return "", fmt.Errorf("mock start container in node mode failed")
	}

	cmd := []string{dockerNodeModeArg, "--run-port=" + apiPort}
	cmd = append(cmd, additionalArgs...)
	if networkMode != "" {
		cmd = append(cmd, "--network-mode="+networkMode)
	}

	// Record the call for testing
	m.containerCalls = append(m.containerCalls, struct {
		Image string
		Cmd   []string
	}{
		Image: image,
		Cmd:   cmd,
	})

	return m.containerID, nil
}

// StartWorkerNodeContainer simulates starting a worker node container
func (m *MockDockerManager) StartWorkerNodeContainer(image string, rmiHostname string, rmiPortStart, rmiPortCount int, additionalArgs []string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.startCallCount++

	if m.failOnStart {
		return "", fmt.Errorf("mock start worker node container failed")
	}

	// Record the call for testing
	m.workerNodeCalls = append(m.workerNodeCalls, struct {
		Image          string
		RMIHostname    string
		RMIPortStart   int
		RMIPortCount   int
		AdditionalArgs []string
	}{
		Image:          image,
		RMIHostname:    rmiHostname,
		RMIPortStart:   rmiPortStart,
		RMIPortCount:   rmiPortCount,
		AdditionalArgs: additionalArgs,
	})

	return m.containerID, nil
}

// StartEntryNodeContainer simulates starting an entry node container
func (m *MockDockerManager) StartEntryNodeContainer(image string, workerAddresses []string, additionalArgs []string, networkMode string) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.startCallCount++

	if m.failOnStart {
		return "", fmt.Errorf("mock start entry node container failed")
	}

	// Record the call for testing
	m.entryNodeCalls = append(m.entryNodeCalls, struct {
		Image           string
		WorkerAddresses []string
		AdditionalArgs  []string
		NetworkMode     string
	}{
		Image:           image,
		WorkerAddresses: workerAddresses,
		AdditionalArgs:  additionalArgs,
		NetworkMode:     networkMode,
	})

	return m.containerID, nil
}

// StreamOutput simulates streaming container output
func (m *MockDockerManager) StreamOutput(containerID string, stdoutCallback func(string), _ func(string)) {
	m.mu.Lock()
	m.streamCallCount++
	if m.onStreamOutput != nil {
		m.onStreamOutput(containerID)
	}
	outputLines := make([]string, len(m.outputLines))
	copy(outputLines, m.outputLines)
	delay := m.outputDelay
	m.mu.Unlock()

	// Simulate async streaming
	go func() {
		for _, line := range outputLines {
			time.Sleep(delay)
			stdoutCallback(line)
		}
	}()
}

// Cleanup simulates container cleanup
func (m *MockDockerManager) Cleanup() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.cleanupCallCount++

	if m.failOnCleanup {
		return fmt.Errorf("mock error: failed to cleanup container")
	}

	m.containerID = ""
	return nil
}

// Close simulates closing the Docker manager
func (m *MockDockerManager) Close() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.closeCallCount++
}

// Test helper methods

// SetContainerID sets the mock container ID
func (m *MockDockerManager) SetContainerID(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.containerID = id
}

// SetFailOnStart configures whether StartContainer should fail
func (m *MockDockerManager) SetFailOnStart(fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failOnStart = fail
}

// SetFailOnCleanup configures whether Cleanup should fail
func (m *MockDockerManager) SetFailOnCleanup(fail bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failOnCleanup = fail
}

// SetOutputLines sets the lines to output during streaming
func (m *MockDockerManager) SetOutputLines(lines []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outputLines = lines
}

// SetOutputDelay sets the delay between output lines
func (m *MockDockerManager) SetOutputDelay(delay time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.outputDelay = delay
}

// GetStreamCallCount returns the number of times StreamOutput was called
func (m *MockDockerManager) GetStreamCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.streamCallCount
}

// GetStartCallCount returns the number of times StartContainer was called
func (m *MockDockerManager) GetStartCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.startCallCount
}

// GetCleanupCallCount returns the number of times Cleanup was called
func (m *MockDockerManager) GetCleanupCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cleanupCallCount
}

// GetCloseCallCount returns the number of times Close was called
func (m *MockDockerManager) GetCloseCallCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.closeCallCount
}

// GetFileMounts returns configured file mounts.
func (m *MockDockerManager) GetFileMounts() []scenario.FileMount {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]scenario.FileMount, len(m.fileMounts))
	copy(result, m.fileMounts)
	return result
}

// GetScenarioCalls returns all scenario-based container starts
func (m *MockDockerManager) GetScenarioCalls() []struct {
	Image          string
	ScenarioPath   string
	AdditionalArgs []string
} {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]struct {
		Image          string
		ScenarioPath   string
		AdditionalArgs []string
	}, len(m.scenarioCalls))
	copy(result, m.scenarioCalls)
	return result
}

// GetContainerCalls returns all regular container starts
func (m *MockDockerManager) GetContainerCalls() []struct {
	Image string
	Cmd   []string
} {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]struct {
		Image string
		Cmd   []string
	}, len(m.containerCalls))
	copy(result, m.containerCalls)
	return result
}

// SetOnStreamOutput sets a callback to be called when StreamOutput is invoked
func (m *MockDockerManager) SetOnStreamOutput(callback func(containerID string)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onStreamOutput = callback
}

// GetWorkerNodeCalls returns all worker node container starts
func (m *MockDockerManager) GetWorkerNodeCalls() []struct {
	Image          string
	RMIHostname    string
	RMIPortStart   int
	RMIPortCount   int
	AdditionalArgs []string
} {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]struct {
		Image          string
		RMIHostname    string
		RMIPortStart   int
		RMIPortCount   int
		AdditionalArgs []string
	}, len(m.workerNodeCalls))
	copy(result, m.workerNodeCalls)
	return result
}

// GetEntryNodeCalls returns all entry node container starts
func (m *MockDockerManager) GetEntryNodeCalls() []struct {
	Image           string
	WorkerAddresses []string
	AdditionalArgs  []string
	NetworkMode     string
} {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]struct {
		Image           string
		WorkerAddresses []string
		AdditionalArgs  []string
		NetworkMode     string
	}, len(m.entryNodeCalls))
	copy(result, m.entryNodeCalls)
	return result
}
