/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

// DockerManager handles Docker operations for the TUI
// dockerClient defines the subset of the Docker SDK we use, enabling tests to mock behavior.
type dockerClient interface {
	ImageInspect(ctx context.Context, imageID string, inspectOpts ...client.ImageInspectOption) (image.InspectResponse, error)
	ImagePull(ctx context.Context, ref string, options image.PullOptions) (io.ReadCloser, error)
	ContainerLogs(ctx context.Context, container string, options container.LogsOptions) (io.ReadCloser, error)
	ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *ocispec.Platform, containerName string) (container.CreateResponse, error)
	ContainerStart(ctx context.Context, containerID string, options container.StartOptions) error
	ContainerStop(ctx context.Context, containerID string, options container.StopOptions) error
	ContainerRemove(ctx context.Context, containerID string, options container.RemoveOptions) error
	ServerVersion(ctx context.Context) (types.Version, error)
	Close() error
}

// DockerManager provides Docker operations for the TUI. It wraps a minimal
// dockerClient interface for easier testing and can delegate to a
// RemoteDockerManager when managing remote hosts.
type DockerManager struct {
	client      dockerClient
	containerID string
	ctx         context.Context
	cancel      context.CancelFunc
	hostInfo    *hostparse.HostInfo  // New field to track host information
	remote      *RemoteDockerManager // When non-nil, delegate operations to remote CLI manager
}

func skipImagePull() bool {
	val := strings.TrimSpace(os.Getenv(constants.EnvSkipImagePull))
	if val == "" {
		return false
	}
	parsed, err := strconv.ParseBool(val)
	if err != nil {
		return false
	}
	return parsed
}

// ContainerID returns the current container ID
func (dm *DockerManager) ContainerID() string {
	if dm.remote != nil {
		return dm.remote.ContainerID()
	}
	return dm.containerID
}

// HostInfo returns the host information for this Docker manager
func (dm *DockerManager) HostInfo() *hostparse.HostInfo {
	return dm.hostInfo
}

func (dm *DockerManager) baseLabels(role string) map[string]string {
	hostName := "localhost"
	if dm.hostInfo != nil && strings.TrimSpace(dm.hostInfo.Host) != "" {
		hostName = dm.hostInfo.Host
	}
	return map[string]string{
		constants.DockerLabelManaged: "true",
		constants.DockerLabelRole:    role,
		constants.DockerLabelHost:    hostName,
	}
}

// NewDockerManager creates a new Docker manager
func NewDockerManager() (*DockerManager, error) {
	ctx, cancel := context.WithCancel(context.Background())

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	return &DockerManager{
		client:   cli,
		ctx:      ctx,
		cancel:   cancel,
		hostInfo: nil, // No host info for legacy single-host mode
	}, nil
}

// ensureImageAvailable checks for the image locally and pulls it if missing
func (dm *DockerManager) ensureImageAvailable(imageName string) error {
	if !skipImagePull() {
		logging.LogInfo("docker", "Pulling Docker image before start", "image", imageName)
		return dm.pullImage(imageName)
	}

	if _, err := dm.client.ImageInspect(dm.ctx, imageName); err == nil {
		logging.LogInfo("docker", "Using cached Docker image", "image", imageName)
		return nil
	}

	logging.LogInfo("docker", "Cached Docker image missing; pulling despite skip request", "image", imageName)
	return dm.pullImage(imageName)
}

func (dm *DockerManager) pullImage(imageName string) error {
	pullCtx, cancel := context.WithTimeout(dm.ctx, 5*time.Minute)
	defer cancel()

	rc, err := dm.client.ImagePull(pullCtx, imageName, image.PullOptions{})
	if err != nil {
		logging.LogError("docker", "failed to initiate image pull", err, "image", imageName)
		return fmt.Errorf("failed to pull image %s: %w", imageName, err)
	}
	defer func() { _ = rc.Close() }()

	if _, err := io.Copy(io.Discard, rc); err != nil {
		logging.LogError("docker", "failed reading image pull output", err, "image", imageName)
		return fmt.Errorf("failed to complete pull for image %s: %w", imageName, err)
	}

	logging.LogInfo("docker", "Docker image pulled successfully", "image", imageName)
	return nil
}

// StartContainer creates and starts a new container
func (dm *DockerManager) StartContainer(image string, cmd []string) (string, error) {
	if dm.remote != nil {
		return dm.remote.StartContainer(image, cmd)
	}
	logging.LogContainerEvent("creating", "", "image", image, "cmd", cmd)

	if err := dm.ensureImageAvailable(image); err != nil {
		return "", err
	}

	// Create container
	resp, err := dm.client.ContainerCreate(dm.ctx, &container.Config{
		Image:        image,
		Cmd:          cmd,
		Tty:          false,
		AttachStdout: true,
		AttachStderr: true,
	}, nil, nil, nil, "")

	if err != nil {
		logging.LogError("docker", "failed to create container", err, "image", image)
		return "", fmt.Errorf("failed to create container: %w", err)
	}

	dm.containerID = resp.ID
	logging.LogContainerEvent("created", resp.ID)

	// Start container
	if err := dm.client.ContainerStart(dm.ctx, resp.ID, container.StartOptions{}); err != nil {
		logging.LogError("docker", "failed to start container", err, "container_id", resp.ID)
		return "", fmt.Errorf("failed to start container: %w", err)
	}

	logging.LogContainerEvent("started", resp.ID)
	return resp.ID, nil
}

// StartContainerWithScenario creates and starts a container with a scenario file
func (dm *DockerManager) StartContainerWithScenario(image string, scenarioPath string, additionalArgs []string) (string, error) {
	if dm.remote != nil {
		return dm.remote.StartContainerWithScenario(image, scenarioPath, additionalArgs)
	}
	logging.LogContainerEvent("creating", "", "image", image, "scenario", scenarioPath)

	if err := dm.ensureImageAvailable(image); err != nil {
		return "", err
	}

	// Get absolute path for the scenario file
	absScenarioPath, err := filepath.Abs(scenarioPath)
	if err != nil {
		return "", fmt.Errorf("failed to get absolute path for scenario: %w", err)
	}

	// Build command with scenario file and additional CLI arguments
	cmd := []string{"--run-scenario=/tmp/scenario.js"}
	cmd = append(cmd, additionalArgs...)
	logging.LogContainerEvent("creating", "", "image", image, "mode", "scenario", "cmd", cmd)

	// Create container with volume mount
	resp, err := dm.client.ContainerCreate(dm.ctx, &container.Config{
		Image:        image,
		Cmd:          cmd,
		Tty:          false,
		AttachStdout: true,
		AttachStderr: true,
	}, &container.HostConfig{
		Binds: []string{
			fmt.Sprintf("%s:/tmp/scenario.js:ro", absScenarioPath),
		},
	}, nil, nil, "")

	if err != nil {
		logging.LogError("docker", "failed to create container", err, "image", image)
		return "", fmt.Errorf("failed to create container: %w", err)
	}

	dm.containerID = resp.ID
	logging.LogContainerEvent("created", resp.ID)

	// Start container
	if err := dm.client.ContainerStart(dm.ctx, resp.ID, container.StartOptions{}); err != nil {
		logging.LogError("docker", "failed to start container", err, "container_id", resp.ID)
		return "", fmt.Errorf("failed to start container: %w", err)
	}

	logging.LogContainerEvent("started", resp.ID)
	return resp.ID, nil
}

// StartContainerInNodeMode starts a Spt container in API server mode
func (dm *DockerManager) StartContainerInNodeMode(image string, apiPort string) (string, error) {
	if dm.remote != nil {
		return dm.remote.StartContainerInNodeMode(image, apiPort)
	}

	if err := dm.ensureImageAvailable(image); err != nil {
		return "", err
	}

	// Build command for node mode
	cmd := []string{
		"--run-node=true",
		"--run-port=" + apiPort,
	}

	logging.LogContainerEvent("creating", "", "image", image, "mode", "node", "port", apiPort, "cmd", cmd)

	// Configure port binding
	hostPort := nat.Port(apiPort + "/tcp")
	portBinding := nat.PortMap{
		hostPort: []nat.PortBinding{{
			HostIP:   "127.0.0.1",
			HostPort: apiPort,
		}},
	}

	// Optional: force JVM RMI hostname to loopback for single-node local runs
	var envVars []string
	if v := os.Getenv("SPT_LOCAL_RMI_LOOPBACK"); v == "1" || strings.EqualFold(v, "true") {
		envVars = append(envVars,
			"JAVA_OPTS=-Djava.rmi.server.hostname=127.0.0.1",
			"JAVA_TOOL_OPTIONS=-Djava.rmi.server.hostname=127.0.0.1",
		)
	}

	labels := dm.baseLabels(constants.DockerRoleNode)

	// Create container with port mapping
	resp, err := dm.client.ContainerCreate(dm.ctx, &container.Config{
		Image:        image,
		Cmd:          cmd,
		ExposedPorts: nat.PortSet{hostPort: struct{}{}},
		AttachStdout: true,
		AttachStderr: true,
		Env:          envVars,
		Labels:       labels,
	}, &container.HostConfig{
		PortBindings: portBinding,
	}, nil, nil, "")

	if err != nil {
		logging.LogError("docker", "failed to create container in node mode", err, "image", image, "port", apiPort)
		return "", fmt.Errorf("failed to create container: %w", err)
	}

	dm.containerID = resp.ID
	logging.LogContainerEvent("created", dm.containerID)

	// Start the container
	if err := dm.client.ContainerStart(dm.ctx, dm.containerID, container.StartOptions{}); err != nil {
		logging.LogError("docker", "failed to start container in node mode", err, "container_id", dm.containerID)

		// Clean up the created container since start failed
		if removeErr := dm.client.ContainerRemove(dm.ctx, dm.containerID, container.RemoveOptions{Force: true}); removeErr != nil {
			logging.LogError("docker", "failed to clean up container after start failure", removeErr, "container_id", dm.containerID)
		} else {
			logging.LogContainerEvent("cleaned up after start failure", dm.containerID)
		}
		dm.containerID = ""

		return "", fmt.Errorf("failed to start container: %w", err)
	}

	logging.LogContainerEvent("started", dm.containerID, "mode", "node", "port", apiPort)
	return dm.containerID, nil
}

// StartWorkerNodeContainer starts a container in RMI worker node mode
func (dm *DockerManager) StartWorkerNodeContainer(image string, rmiHostname string, rmiPortStart, rmiPortCount int) (string, error) {
	if dm.remote != nil {
		return dm.remote.StartWorkerNodeContainer(image, rmiHostname, rmiPortStart, rmiPortCount)
	}

	// Build command for worker node mode
	cmd := []string{
		"--run-node=true",
		"--run-port=" + constants.SptAPIPort, // REST API port
	}

	logging.LogContainerEvent("creating", "", "image", image, "mode", "worker_node", "rmi_hostname", rmiHostname, "cmd", cmd)

	// Configure port bindings
	portBindings := nat.PortMap{
		// RMI Registry port
		constants.RMIRegistryPort + "/tcp": []nat.PortBinding{{
			HostIP:   "0.0.0.0",
			HostPort: constants.RMIRegistryPort,
		}},
		// REST API port
		constants.SptAPIPort + "/tcp": []nat.PortBinding{{
			HostIP:   "0.0.0.0",
			HostPort: constants.SptAPIPort,
		}},
	}

	// Add RMI object ports
	for i := 0; i < rmiPortCount; i++ {
		port := rmiPortStart + i
		portKey := fmt.Sprintf("%d/tcp", port)
		portBindings[nat.Port(portKey)] = []nat.PortBinding{{
			HostIP:   "0.0.0.0",
			HostPort: fmt.Sprintf("%d", port),
		}}
	}

	// Prepare exposed ports
	exposedPorts := nat.PortSet{}
	for port := range portBindings {
		exposedPorts[port] = struct{}{}
	}

	// Ensure image present
	if err := dm.ensureImageAvailable(image); err != nil {
		return "", err
	}

	// Create container configuration
	labels := dm.baseLabels(constants.DockerRoleWorker)
	containerConfig := &container.Config{
		Image:        image,
		Cmd:          cmd,
		ExposedPorts: exposedPorts,
		AttachStdout: true,
		AttachStderr: true,
		Env: []string{
			// Critical: Tell RMI what hostname to advertise
			fmt.Sprintf("JAVA_OPTS=-Djava.rmi.server.hostname=%s", rmiHostname),
			fmt.Sprintf("JAVA_TOOL_OPTIONS=-Djava.rmi.server.hostname=%s", rmiHostname),
		},
		Labels: labels,
	}

	hostConfig := &container.HostConfig{
		PortBindings: portBindings,
	}

	// Create container
	resp, err := dm.client.ContainerCreate(dm.ctx, containerConfig, hostConfig, nil, nil, "")
	if err != nil {
		logging.LogError("docker", "failed to create worker node container", err, "image", image, "rmi_hostname", rmiHostname)
		return "", fmt.Errorf("failed to create container: %w", err)
	}

	dm.containerID = resp.ID
	logging.LogContainerEvent("created", dm.containerID)

	// Start the container
	if err := dm.client.ContainerStart(dm.ctx, dm.containerID, container.StartOptions{}); err != nil {
		logging.LogError("docker", "failed to start worker node container", err, "container_id", dm.containerID)

		// Clean up the created container since start failed
		if removeErr := dm.client.ContainerRemove(dm.ctx, dm.containerID, container.RemoveOptions{Force: true}); removeErr != nil {
			logging.LogError("docker", "failed to clean up container after start failure", removeErr, "container_id", dm.containerID)
		} else {
			logging.LogContainerEvent("cleaned up after start failure", dm.containerID)
		}
		dm.containerID = ""

		return "", fmt.Errorf("failed to start container: %w", err)
	}

	logging.LogContainerEvent("started", dm.containerID, "mode", "worker_node", "rmi_hostname", rmiHostname)
	return dm.containerID, nil
}

// StartEntryNodeContainer starts a container as RMI entry node with worker addresses
func (dm *DockerManager) StartEntryNodeContainer(image string, workerAddresses []string, additionalArgs []string) (string, error) {
	if dm.remote != nil {
		return dm.remote.StartEntryNodeContainer(image, workerAddresses, additionalArgs)
	}
	logging.LogContainerEvent("creating", "", "image", image, "mode", "entry_node", "workers", len(workerAddresses))

	if err := dm.ensureImageAvailable(image); err != nil {
		return "", err
	}

	// Build command for entry node (API server mode with RMI coordination)
	cmd := []string{
		"--load-step-node-addrs=" + strings.Join(workerAddresses, ","),
		"--run-node=true",
		"--run-port=" + constants.SptAPIPort,
	}

	// Add additional arguments (e.g., S3 endpoint parameters)
	cmd = append(cmd, additionalArgs...)

	// Create container configuration
	logging.LogContainerEvent("creating", "", "image", image, "mode", "entry_node", "workers", len(workerAddresses), "cmd", cmd)

	labels := dm.baseLabels(constants.DockerRoleEntry)
	containerConfig := &container.Config{
		Image:        image,
		Cmd:          cmd,
		AttachStdout: true,
		AttachStderr: true,
		ExposedPorts: nat.PortSet{
			constants.SptAPIPort + "/tcp": struct{}{}, // Only REST API port needed for entry node
		},
		Labels: labels,
	}

	hostConfig := &container.HostConfig{
		// No file mounting needed - scenario content sent via API
		// Only REST API port binding for entry node
		PortBindings: nat.PortMap{
			constants.SptAPIPort + "/tcp": []nat.PortBinding{{
				HostIP:   "0.0.0.0",
				HostPort: constants.SptAPIPort,
			}},
		},
	}

	// Create container
	resp, err := dm.client.ContainerCreate(dm.ctx, containerConfig, hostConfig, nil, nil, "")
	if err != nil {
		logging.LogError("docker", "failed to create entry node container", err, "image", image, "workers", len(workerAddresses))
		return "", fmt.Errorf("failed to create container: %w", err)
	}

	dm.containerID = resp.ID
	logging.LogContainerEvent("created", dm.containerID)

	// Start the container
	if err := dm.client.ContainerStart(dm.ctx, dm.containerID, container.StartOptions{}); err != nil {
		logging.LogError("docker", "failed to start entry node container", err, "container_id", dm.containerID)

		// Clean up the created container since start failed
		if removeErr := dm.client.ContainerRemove(dm.ctx, dm.containerID, container.RemoveOptions{Force: true}); removeErr != nil {
			logging.LogError("docker", "failed to clean up container after start failure", removeErr, "container_id", dm.containerID)
		} else {
			logging.LogContainerEvent("cleaned up after start failure", dm.containerID)
		}
		dm.containerID = ""

		return "", fmt.Errorf("failed to start container: %w", err)
	}

	logging.LogContainerEvent("started", dm.containerID, "mode", "entry_node", "workers", len(workerAddresses))
	return dm.containerID, nil
}

// StreamOutput starts streaming container output with callback functions
func (dm *DockerManager) StreamOutput(containerID string, stdoutCallback func(string), stderrCallback func(string)) {
	if dm.remote != nil {
		dm.remote.StreamOutput(containerID, stdoutCallback, stderrCallback)
		return
	}
	go func() {
		if containerID == "" {
			stderrCallback("Error: No container ID")
			return
		}

		// Get container logs
		options := container.LogsOptions{
			ShowStdout: true,
			ShowStderr: true,
			Follow:     true,
			Timestamps: false,
		}

		out, err := dm.client.ContainerLogs(dm.ctx, containerID, options)
		if err != nil {
			stderrCallback(fmt.Sprintf("Error getting container logs: %v", err))
			return
		}
		defer func() {
			if err := out.Close(); err != nil {
				slog.Warn("Failed to close container logs reader", "component", "docker", "error", err)
			}
		}()

		// Read and parse the output
		buffer := make([]byte, 4096)
		for {
			n, err := out.Read(buffer)
			if err != nil {
				if err != io.EOF {
					stderrCallback(fmt.Sprintf("Error reading container logs: %v", err))
				}
				break
			}

			if n > 0 {
				result := dm.processDockerOutput(buffer[:n])

				// Send stdout lines to stdout callback
				for _, line := range result.StdoutLines {
					stdoutCallback(line)
				}

				// Send stderr lines to stderr callback
				for _, line := range result.StderrLines {
					stderrCallback(line)
				}
			}
		}
	}()
}

// DockerStreamResult holds the parsed stdout and stderr lines
type DockerStreamResult struct {
	StdoutLines []string
	StderrLines []string
}

// processDockerOutput handles Docker's multiplexed stream format
// Returns separate stdout and stderr lines
func (dm *DockerManager) processDockerOutput(data []byte) DockerStreamResult {
	result := DockerStreamResult{
		StdoutLines: make([]string, 0),
		StderrLines: make([]string, 0),
	}
	offset := 0

	for offset < len(data) {
		// Check if we have enough bytes for a Docker header (8 bytes)
		if offset+8 > len(data) {
			// Not enough data for a complete header, treat as raw text (assume stdout)
			remaining := string(data[offset:])
			if remaining != "" {
				lines := strings.Split(strings.TrimSpace(remaining), "\n")
				for _, line := range lines {
					if line != "" {
						result.StdoutLines = append(result.StdoutLines, line)
					}
				}
			}
			break
		}

		// Extract stream type from byte 0 (0=stdin, 1=stdout, 2=stderr)
		streamType := data[offset]

		// Extract the payload size from the Docker header (bytes 4-7, big-endian)
		payloadSize := int(data[offset+4])<<24 | int(data[offset+5])<<16 | int(data[offset+6])<<8 | int(data[offset+7])

		// Skip the 8-byte header
		offset += 8

		// Check if we have the complete payload
		if offset+payloadSize > len(data) {
			// Don't have complete payload, treat remaining as raw text (assume stdout)
			remaining := string(data[offset-8:]) // Include header in raw text
			if remaining != "" {
				lines := strings.Split(strings.TrimSpace(remaining), "\n")
				for _, line := range lines {
					if line != "" {
						result.StdoutLines = append(result.StdoutLines, line)
					}
				}
			}
			break
		}

		// Extract the payload
		if payloadSize > 0 {
			payload := string(data[offset : offset+payloadSize])
			// Split into lines and add to appropriate stream
			payloadLines := strings.Split(strings.TrimRight(payload, "\n\r"), "\n")
			for _, line := range payloadLines {
				if line != "" {
					switch streamType {
					case 1: // stdout
						result.StdoutLines = append(result.StdoutLines, line)
					case 2: // stderr
						result.StderrLines = append(result.StderrLines, line)
					default: // stdin or unknown, treat as stdout
						result.StdoutLines = append(result.StdoutLines, line)
					}
				}
			}
		}

		offset += payloadSize
	}

	return result
}

// Cleanup stops and removes the container
func (dm *DockerManager) Cleanup() error {
	if dm.remote != nil {
		return dm.remote.Cleanup()
	}
	if dm.containerID == "" {
		return nil
	}

	logging.LogContainerEvent("stopping", dm.containerID)

	// Stop the container
	timeout := 10
	err := dm.client.ContainerStop(dm.ctx, dm.containerID, container.StopOptions{
		Timeout: &timeout,
	})
	if err != nil && !strings.Contains(err.Error(), "already stopped") {
		logging.LogError("docker", "failed to stop container", err, "container_id", dm.containerID)
		return fmt.Errorf("failed to stop container: %w", err)
	}

	logging.LogContainerEvent("stopped", dm.containerID)

	// Remove the container
	err = dm.client.ContainerRemove(dm.ctx, dm.containerID, container.RemoveOptions{
		Force: true,
	})
	if err != nil {
		logging.LogError("docker", "failed to remove container", err, "container_id", dm.containerID)
		return fmt.Errorf("failed to remove container: %w", err)
	}

	logging.LogContainerEvent("removed", dm.containerID)
	return nil
}

// Close cleans up resources
func (dm *DockerManager) Close() {
	if dm.cancel != nil {
		dm.cancel()
	}
	if dm.client != nil {
		if err := dm.client.Close(); err != nil {
			slog.Warn("Failed to close Docker client", "component", "docker", "error", err)
		}
	}
}

// GetDockerVersion returns the Docker daemon version
func (dm *DockerManager) GetDockerVersion(ctx context.Context) (string, error) {
	version, err := dm.client.ServerVersion(ctx)
	if err != nil {
		return "", err
	}
	return version.Version, nil
}

// CreateContainer creates a container with the given configuration
func (dm *DockerManager) CreateContainer(containerConfig *container.Config, hostConfig *container.HostConfig, containerName string) (string, error) {
	resp, err := dm.client.ContainerCreate(dm.ctx, containerConfig, hostConfig, nil, nil, containerName)
	if err != nil {
		return "", fmt.Errorf("failed to create container: %w", err)
	}

	logging.LogContainerEvent("created", resp.ID)
	return resp.ID, nil
}

// StartContainerByID starts an existing container by ID
func (dm *DockerManager) StartContainerByID(containerID string) error {
	if err := dm.client.ContainerStart(dm.ctx, containerID, container.StartOptions{}); err != nil {
		logging.LogError("docker", "failed to start container", err, "container_id", containerID)
		return fmt.Errorf("failed to start container: %w", err)
	}

	logging.LogContainerEvent("started", containerID)
	return nil
}

// StopContainer stops a container by ID
func (dm *DockerManager) StopContainer(containerID string, timeoutSeconds int) error {
	if containerID == "" {
		return nil
	}

	logging.LogContainerEvent("stopping", containerID)

	err := dm.client.ContainerStop(dm.ctx, containerID, container.StopOptions{
		Timeout: &timeoutSeconds,
	})
	if err != nil && !strings.Contains(err.Error(), "already stopped") {
		logging.LogError("docker", "failed to stop container", err, "container_id", containerID)
		return fmt.Errorf("failed to stop container: %w", err)
	}

	logging.LogContainerEvent("stopped", containerID)
	return nil
}

// RemoveContainer removes a container by ID
func (dm *DockerManager) RemoveContainer(containerID string) error {
	if containerID == "" {
		return nil
	}

	err := dm.client.ContainerRemove(dm.ctx, containerID, container.RemoveOptions{
		Force: true,
	})
	if err != nil {
		logging.LogError("docker", "failed to remove container", err, "container_id", containerID)
		return fmt.Errorf("failed to remove container: %w", err)
	}

	logging.LogContainerEvent("removed", containerID)
	return nil
}
