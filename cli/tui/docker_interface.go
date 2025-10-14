/*
Copyright © 2025 Dell Technologies
*/

package tui

// DockerInterface defines the interface for Docker operations
// This allows for easy mocking in tests
type DockerInterface interface {
	// ContainerID returns the current container ID
	ContainerID() string

	// StartContainer creates and starts a new container with the given command
	StartContainer(image string, cmd []string) (string, error)

	// StartContainerWithScenario creates and starts a container with a scenario file
	StartContainerWithScenario(image string, scenarioPath string, additionalArgs []string) (string, error)

	// StartContainerInNodeMode starts a container in API server mode with port mapping
	StartContainerInNodeMode(image string, apiPort string) (string, error)

	// StartWorkerNodeContainer starts a container in RMI worker node mode
	StartWorkerNodeContainer(image string, rmiHostname string, rmiPortStart, rmiPortCount int) (string, error)

	// StartEntryNodeContainer starts a container as RMI entry node with worker addresses
	StartEntryNodeContainer(image string, workerAddresses []string, additionalArgs []string) (string, error)

	// StreamOutput starts streaming container output with callback functions
	StreamOutput(containerID string, stdoutCallback func(string), stderrCallback func(string))

	// Cleanup stops and removes the container
	Cleanup() error

	// Close cleans up resources
	Close()
}

// Ensure DockerManager implements DockerInterface
var _ DockerInterface = (*DockerManager)(nil)
