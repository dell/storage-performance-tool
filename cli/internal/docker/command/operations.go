/*
Copyright © 2025 Dell Technologies
*/

package command

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// DockerOperationsImpl implements DockerOperations using CommandExecutor
type DockerOperationsImpl struct {
	executor CommandExecutor
	host     *hostparse.HostInfo
	builder  DockerCommandBuilder
}

func shouldSkipImagePull(image string) bool {
	// Content-addressed local image IDs cannot be pulled from a registry. Integrity
	// runs deliberately bind container launch to the ID established by preflight.
	if isImmutableImageID(image) {
		return true
	}
	// Local-only dev images (spt_dev) are never in a registry; skip the pull so the
	// run uses the image distributed via engine/tools/push-worker-image.sh.
	if constants.IsDevImage(image) {
		return true
	}
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

func isImmutableImageID(image string) bool {
	return strings.HasPrefix(strings.TrimSpace(image), constants.DockerImageIDPrefix)
}

func missingDevImageError(image string, host *hostparse.HostInfo) error {
	hostName := "local host"
	if host != nil && strings.TrimSpace(host.Original) != "" {
		hostName = host.Original
	} else if host != nil && strings.TrimSpace(host.Host) != "" {
		hostName = host.Host
	}
	return fmt.Errorf("dev image %s not present on %s; build it with `make docker-local` and distribute it with engine/tools/push-worker-image.sh", image, hostName)
}

// NewDockerOperations creates a new DockerOperations instance
func NewDockerOperations(executor CommandExecutor, host *hostparse.HostInfo) DockerOperations {
	return &DockerOperationsImpl{
		executor: executor,
		host:     host,
		builder:  NewDockerCommandBuilder(),
	}
}

// StartContainer starts a container and returns container ID plus detailed command results
func (d *DockerOperationsImpl) StartContainer(ctx context.Context, config ContainerConfig) (string, CommandResult, error) {
	// Build the docker run command
	if err := d.prepareImageForLaunch(ctx, config.Image); err != nil {
		result := CommandResult{Error: err}
		return "", result, err
	}

	dockerCmd := d.builder.BuildRunCommand(config)

	result := CommandResult{
		Command: strings.Join(dockerCmd, " "),
	}

	logging.LogDebug("docker-command", "starting container",
		"host", d.host.Host,
		"image", config.Image,
		"name", config.Name,
		"command", result.Command)

	// Execute the command
	stdout, stderr, err := d.executor.ExecuteCommand(ctx, d.host, dockerCmd)
	result.Stdout = stdout
	result.Stderr = stderr

	if err != nil {
		result.Error = fmt.Errorf("failed to start container on %s: %w", d.host.Host, err)
		return "", result, result.Error
	}

	containerID := strings.TrimSpace(stdout)
	if containerID == "" {
		result.Error = fmt.Errorf("no container ID returned from docker run on %s", d.host.Host)
		return "", result, result.Error
	}

	logging.LogInfo("docker-command", "container started successfully",
		"host", d.host.Host,
		"container_id", containerID,
		"image", config.Image)

	return containerID, result, nil
}

// StopContainer stops and removes a container
func (d *DockerOperationsImpl) StopContainer(ctx context.Context, containerID string) error {
	if containerID == "" {
		return fmt.Errorf("empty container ID")
	}

	dockerCmd := []string{constants.DockerCommand, constants.DockerCmdRM, constants.DockerFlagForce, containerID}

	logging.LogDebug("docker-command", "stopping container",
		"host", d.host.Host,
		"container_id", containerID)

	stdout, stderr, err := d.executor.ExecuteCommand(ctx, d.host, dockerCmd)
	if err != nil {
		return fmt.Errorf("failed to stop container %s on %s: %w (stderr: %s)",
			containerID, d.host.Host, err, stderr)
	}

	logging.LogInfo("docker-command", "container stopped successfully",
		"host", d.host.Host,
		"container_id", containerID,
		"stdout", stdout)

	return nil
}

// IsContainerRunning checks if a container is running
func (d *DockerOperationsImpl) IsContainerRunning(ctx context.Context, containerID string) (bool, error) {
	dockerCmd := []string{constants.DockerCommand, constants.DockerCmdPS, constants.DockerFlagQuiet, constants.DockerFlagFilter, "id=" + containerID}

	stdout, stderr, err := d.executor.ExecuteCommand(ctx, d.host, dockerCmd)
	if err != nil {
		return false, fmt.Errorf("failed to check container status on %s: %w (stderr: %s)",
			d.host.Host, err, stderr)
	}

	return strings.TrimSpace(stdout) != "", nil
}

// GetContainerLogs streams logs from a container
func (d *DockerOperationsImpl) GetContainerLogs(ctx context.Context, containerID string, follow bool, stdoutCallback, stderrCallback func(string)) error {
	dockerCmd := []string{constants.DockerCommand, constants.DockerCmdLogs}

	if follow {
		dockerCmd = append(dockerCmd, constants.DockerFlagForce)
	}

	dockerCmd = append(dockerCmd, containerID)

	logging.LogDebug("docker-command", "getting container logs",
		"host", d.host.Host,
		"container_id", containerID,
		"follow", follow)

	// For now, we'll use a simple implementation that gets logs once
	// In the future, we might need to handle streaming differently
	stdout, stderr, err := d.executor.ExecuteCommand(ctx, d.host, dockerCmd)
	if err != nil {
		return fmt.Errorf("failed to get logs for container %s on %s: %w",
			containerID, d.host.Host, err)
	}

	// Split stdout by lines and call callback for each line
	if stdout != "" {
		lines := strings.Split(stdout, "\n")
		for _, line := range lines {
			if line != "" {
				stdoutCallback(line)
			}
		}
	}

	// Handle stderr if provided
	if stderr != "" {
		lines := strings.Split(stderr, "\n")
		for _, line := range lines {
			if line != "" {
				stderrCallback(line)
			}
		}
	}

	return nil
}

// GetContainerLogsSince fetches logs newer than 'since' and optionally includes timestamps per line.
func (d *DockerOperationsImpl) GetContainerLogsSince(ctx context.Context, containerID string, since time.Time, timestamps bool, stdoutCallback, stderrCallback func(string)) error {
	dockerCmd := []string{constants.DockerCommand, constants.DockerCmdLogs}

	if timestamps {
		dockerCmd = append(dockerCmd, constants.DockerFlagTimestamps)
	}
	if !since.IsZero() {
		dockerCmd = append(dockerCmd, constants.DockerFlagSince, since.Format(time.RFC3339Nano))
	}

	dockerCmd = append(dockerCmd, containerID)

	logging.LogDebug("docker-command", "getting container logs since",
		"host", d.host.Host,
		"container_id", containerID,
		"since", since,
		"timestamps", timestamps)

	stdout, stderr, err := d.executor.ExecuteCommand(ctx, d.host, dockerCmd)
	if err != nil {
		return fmt.Errorf("failed to get logs for container %s on %s: %w",
			containerID, d.host.Host, err)
	}

	if stdout != "" {
		lines := strings.Split(stdout, "\n")
		for _, line := range lines {
			if line != "" {
				stdoutCallback(line)
			}
		}
	}
	if stderr != "" {
		lines := strings.Split(stderr, "\n")
		for _, line := range lines {
			if line != "" {
				stderrCallback(line)
			}
		}
	}
	return nil
}

// ListContainers returns list of containers (optionally filtered)
func (d *DockerOperationsImpl) ListContainers(ctx context.Context, filters map[string]string) ([]ContainerInfo, error) {
	dockerCmd := make([]string, 0, 5+2*len(filters))
	dockerCmd = append(dockerCmd, constants.DockerCommand, constants.DockerCmdPS, constants.DockerFlagAll, constants.DockerFlagFormat, constants.DockerContainerFormat)

	// Add filters
	for key, value := range filters {
		dockerCmd = append(dockerCmd, constants.DockerFlagFilter, fmt.Sprintf("%s=%s", key, value))
	}

	stdout, stderr, err := d.executor.ExecuteCommand(ctx, d.host, dockerCmd)
	if err != nil {
		return nil, fmt.Errorf("failed to list containers on %s: %w (stderr: %s)",
			d.host.Host, err, stderr)
	}

	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	containers := make([]ContainerInfo, 0, len(lines))

	for _, line := range lines {
		if line == "" {
			continue
		}

		parts := strings.Split(line, constants.DockerOutputSeparator)
		if len(parts) < 4 {
			continue
		}

		container := ContainerInfo{
			ID:    parts[0],
			Name:  parts[1],
			Image: parts[2],
			State: parts[3],
		}

		// Parse ports if available
		if len(parts) > 4 && parts[4] != "" {
			container.Ports = []string{parts[4]}
		}

		containers = append(containers, container)
	}

	return containers, nil
}

// StartContainerWithBuilder starts a container using the built-in builder
func (d *DockerOperationsImpl) StartContainerWithBuilder(ctx context.Context, image, containerName string, networkMode NetworkMode, portMappings []PortMapping, environment map[string]string, command []string) (string, error) {
	config := ContainerConfig{
		Image:        image,
		Name:         containerName,
		NetworkMode:  networkMode,
		PortMappings: portMappings,
		Environment:  environment,
		Command:      command,
		Detached:     true,
	}

	containerID, _, err := d.StartContainer(ctx, config)
	return containerID, err
}

// StartWorkerNodeContainer starts a worker node container with RMI configuration
func (d *DockerOperationsImpl) StartWorkerNodeContainer(ctx context.Context, image, containerName, rmiHostname string, rmiPortStart, rmiPortCount int) (string, error) {
	// Ensure image is available locally; attempt auto-pull if not
	if err := d.prepareImageForLaunch(ctx, image); err != nil {
		return "", err
	}

	dockerCmd := d.builder.BuildWorkerNodeCommand(image, containerName, rmiHostname, rmiPortStart, rmiPortCount)

	// Echo full command for triage (visible in spt.log when --log-level=debug or --debug)
	logging.LogDebug("docker-command", "starting worker node (docker run)",
		"host", d.host.Host,
		"command", strings.Join(dockerCmd, " "))

	stdout, stderr, err := d.executor.ExecuteCommand(ctx, d.host, dockerCmd)
	if err != nil {
		return "", fmt.Errorf("failed to start worker node container on %s: %w (stderr: %s)",
			d.host.Host, err, stderr)
	}

	containerID := strings.TrimSpace(stdout)
	logging.LogInfo("docker-command", "worker node container started",
		"host", d.host.Host,
		"container_id", containerID,
		"rmi_hostname", rmiHostname)

	return containerID, nil
}

// StartEntryNodeContainer starts an entry node container with worker addresses
func (d *DockerOperationsImpl) StartEntryNodeContainer(ctx context.Context, image, containerName string, workerAddresses []string, networkMode NetworkMode) (string, error) {
	// Ensure image is available locally; attempt auto-pull if not
	if err := d.prepareImageForLaunch(ctx, image); err != nil {
		return "", err
	}

	dockerCmd := d.builder.BuildEntryNodeCommand(image, containerName, workerAddresses, networkMode)

	// Echo full command for triage
	logging.LogDebug("docker-command", "starting entry node (docker run)",
		"host", d.host.Host,
		"command", strings.Join(dockerCmd, " "))

	stdout, stderr, err := d.executor.ExecuteCommand(ctx, d.host, dockerCmd)
	if err != nil {
		return "", fmt.Errorf("failed to start entry node container on %s: %w (stderr: %s)",
			d.host.Host, err, stderr)
	}

	containerID := strings.TrimSpace(stdout)
	logging.LogInfo("docker-command", "entry node container started",
		"host", d.host.Host,
		"container_id", containerID,
		"worker_count", len(workerAddresses))

	return containerID, nil
}

// WaitForContainerReady waits for a container to be running and ready
func (d *DockerOperationsImpl) WaitForContainerReady(ctx context.Context, containerID string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		running, err := d.IsContainerRunning(ctx, containerID)
		if err != nil {
			return fmt.Errorf("error checking container status: %w", err)
		}

		if running {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(constants.ContainerReadyPollSecs * time.Second):
			// Continue checking
		}
	}

	return fmt.Errorf("container %s not ready after %v", containerID, timeout)
}

// GetContainerIP gets the IP address of a container
func (d *DockerOperationsImpl) GetContainerIP(ctx context.Context, containerID string) (string, error) {
	dockerCmd := []string{constants.DockerCommand, constants.DockerCmdInspect, constants.DockerFlagFormat, constants.DockerIPAddressFormat, containerID}

	stdout, stderr, err := d.executor.ExecuteCommand(ctx, d.host, dockerCmd)
	if err != nil {
		return "", fmt.Errorf("failed to get container IP on %s: %w (stderr: %s)",
			d.host.Host, err, stderr)
	}

	ip := strings.TrimSpace(stdout)
	if ip == "" {
		return "", fmt.Errorf("no IP address found for container %s", containerID)
	}

	return ip, nil
}

// ParsePortFromString parses a port number from string, returns -1 if invalid
func ParsePortFromString(portStr string) int {
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return -1
	}
	return port
}

// PullImage pulls a Docker image, ensuring we get the latest version
func (d *DockerOperationsImpl) PullImage(ctx context.Context, image string) (CommandResult, error) {
	dockerCmd := []string{constants.DockerCommand, constants.DockerCmdPull, image}

	result := CommandResult{
		Command: strings.Join(dockerCmd, " "),
	}

	logging.LogDebug("docker-command", "pulling Docker image",
		"host", d.host.Host,
		"image", image,
		"command", result.Command)

	stdout, stderr, err := d.executor.ExecuteCommand(ctx, d.host, dockerCmd)
	result.Stdout = stdout
	result.Stderr = stderr

	if err != nil {
		result.Error = fmt.Errorf("failed to pull image %s on %s: %w", image, d.host.Host, err)
		return result, result.Error
	}

	logging.LogInfo("docker-command", "Docker image pulled successfully",
		"host", d.host.Host,
		"image", image)

	return result, nil
}

func (d *DockerOperationsImpl) prepareImageForLaunch(ctx context.Context, image string) error {
	if !shouldSkipImagePull(image) {
		if _, err := d.PullImage(ctx, image); err != nil {
			return fmt.Errorf("failed to pull image %s: %w", image, err)
		}
		return nil
	}

	available, err := d.IsImageAvailable(ctx, image)
	if err != nil {
		return err
	}
	if available {
		return nil
	}
	if isImmutableImageID(image) {
		return fmt.Errorf("immutable image %s is not available on %s", image, d.host.Host)
	}
	if constants.IsDevImage(image) {
		return missingDevImageError(image, d.host)
	}

	logging.LogInfo("docker-command", "Docker image not found locally; pulling despite skip", "host", d.host.Host, "image", image)
	if _, err := d.PullImage(ctx, image); err != nil {
		return fmt.Errorf("image %s not available and pull failed: %w", image, err)
	}
	return nil
}

// IsImageAvailable checks if a Docker image exists locally. Immutable image IDs
// require inspect because `docker images -q <sha256-ID>` treats the ID as a
// repository filter and may return no output for an image that exists.
func (d *DockerOperationsImpl) IsImageAvailable(ctx context.Context, image string) (bool, error) {
	dockerCmd := []string{constants.DockerCommand, constants.DockerCmdImages, constants.DockerFlagQuiet, image}
	if isImmutableImageID(image) {
		dockerCmd = []string{constants.DockerCommand, constants.DockerCmdImage, constants.DockerCmdInspect, image}
	}

	logging.LogDebug("docker-command", "checking if Docker image exists locally",
		"host", d.host.Host,
		"image", image)

	stdout, stderr, err := d.executor.ExecuteCommand(ctx, d.host, dockerCmd)
	if err != nil {
		return false, fmt.Errorf("failed to check image availability on %s: %w (stderr: %s)",
			d.host.Host, err, stderr)
	}

	available := isImmutableImageID(image) || strings.TrimSpace(stdout) != ""
	logging.LogDebug("docker-command", "Docker image availability check result",
		"host", d.host.Host,
		"image", image,
		"available", available)

	return available, nil
}
