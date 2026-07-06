/*
Copyright © 2025 Dell Technologies
*/

package command

import (
	"context"
	"time"
)

// ContainerInfo holds information about a Docker container
type ContainerInfo struct {
	ID    string   // Container ID
	Name  string   // Container name
	Image string   // Docker image
	State string   // Container state (running, exited, etc.)
	Ports []string // Port mappings
}

// NetworkMode defines Docker networking modes
// NetworkMode defines the Docker networking mode for a container.
type NetworkMode string

const (
	// NetworkModeBridge runs containers in Docker's userland bridge mode.
	NetworkModeBridge NetworkMode = "bridge"
	// NetworkModeHost shares the host network namespace with the container.
	NetworkModeHost NetworkMode = "host"
)

// ContainerConfig holds configuration for starting containers
type ContainerConfig struct {
	Image        string            // Docker image to use
	Name         string            // Container name
	NetworkMode  NetworkMode       // Network mode (bridge/host)
	PortMappings []PortMapping     // Port mappings for bridge mode
	Labels       map[string]string // Optional labels to tag the container
	Environment  map[string]string // Environment variables
	EnvFiles     []string          // Docker env-file paths
	Command      []string          // Container command to run
	ExposedPorts []int             // Ports to expose
	Detached     bool              // Run in detached mode
	Devices      []string          // Host devices to pass through (--device flags)
	CapAdd       []string          // Linux capabilities to add (--cap-add flags)
	Ulimits      []string          // Resource limits (--ulimit flags, e.g. "memlock=-1:-1")
	BindMounts   []string          // Docker bind specs, e.g. host:container:ro
}

// PortMapping represents a Docker port mapping
type PortMapping struct {
	HostPort      int    // Port on host
	ContainerPort int    // Port in container
	Protocol      string // tcp/udp (defaults to tcp)
}

// DockerCommandBuilder provides methods for building Docker commands
type DockerCommandBuilder interface {
	// BuildRunCommand builds a docker run command with all necessary flags
	BuildRunCommand(config ContainerConfig) []string

	// BuildWorkerNodeCommand builds docker run for RMI worker nodes
	BuildWorkerNodeCommand(image, containerName, rmiHostname string, rmiPortStart, rmiPortCount int) []string

	// BuildEntryNodeCommand builds docker run for RMI entry nodes
	BuildEntryNodeCommand(image, containerName string, workerAddresses []string, networkMode NetworkMode) []string
}

// Result holds detailed command execution results
type Result struct {
	Command string // The command that was executed
	Stdout  string // Standard output from the command
	Stderr  string // Standard error from the command
	Error   error  // Any error that occurred
}

// CommandResult is a compatibility alias for Result during migration.
// TODO: remove after call sites are updated.
//
//nolint:revive // compatibility alias for pre-migration identifiers
type CommandResult = Result

// DockerOperations provides high-level Docker operations via SSH/local commands
type DockerOperations interface {
	// StartContainer starts a container and returns container ID plus detailed command results
	StartContainer(ctx context.Context, config ContainerConfig) (containerID string, result CommandResult, err error)

	// StopContainer stops and removes a container
	StopContainer(ctx context.Context, containerID string) error

	// IsContainerRunning checks if a container is running
	IsContainerRunning(ctx context.Context, containerID string) (bool, error)

	// GetContainerLogs streams logs from a container
	GetContainerLogs(ctx context.Context, containerID string, follow bool, stdoutCallback, stderrCallback func(string)) error

	// GetContainerLogsSince fetches logs newer than 'since'. When timestamps is true,
	// docker will prefix each line with an RFC3339 timestamp which callers can parse
	// for watermark advancement.
	GetContainerLogsSince(ctx context.Context, containerID string, since time.Time, timestamps bool, stdoutCallback, stderrCallback func(string)) error

	// ListContainers returns list of containers (optionally filtered)
	ListContainers(ctx context.Context, filters map[string]string) ([]ContainerInfo, error)

	// PullImage pulls a Docker image (always attempts to get latest)
	PullImage(ctx context.Context, image string) (CommandResult, error)

	// IsImageAvailable checks if a Docker image exists locally (any version)
	IsImageAvailable(ctx context.Context, image string) (bool, error)
}
