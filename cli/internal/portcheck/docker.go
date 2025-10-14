/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/logging"
	containertypes "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// DockerContainerFinder handles Docker container detection and management
type DockerContainerFinder struct {
	client *client.Client
	clock  Clock
}

// NewDockerContainerFinder creates a new Docker container finder
func NewDockerContainerFinder() (*DockerContainerFinder, error) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, fmt.Errorf("failed to create Docker client: %w", err)
	}

	return &DockerContainerFinder{
		client: cli,
		clock:  &RealClock{},
	}, nil
}

// FindContainerUsingPort finds Docker containers that are using the specified port
func (d *DockerContainerFinder) FindContainerUsingPort(ctx context.Context, port string) (*ContainerInfo, error) {
	logging.LogDebug("portcheck", "searching for containers using port", "port", port)

	// List all containers (running and stopped)
	containers, err := d.client.ContainerList(ctx, containertypes.ListOptions{All: true})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	for _, c := range containers {
		// Check if container is using the port
		if d.containerUsesPort(c, port) {
			logging.LogInfo("portcheck", "found container using port",
				"containerID", c.ID[:12],
				"name", d.getContainerName(c),
				"port", port)

			return d.containerToInfo(c), nil
		}
	}

	logging.LogDebug("portcheck", "no containers found using port", "port", port)
	return nil, nil
}

// FindSptContainers finds all containers running Spt images
func (d *DockerContainerFinder) FindSptContainers(ctx context.Context) ([]*ContainerInfo, error) {
	logging.LogDebug("portcheck", "searching for Spt containers")

	containers, err := d.client.ContainerList(ctx, containertypes.ListOptions{All: true})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	var sptContainers []*ContainerInfo
	for _, c := range containers {
		if IsSptContainer(d.containerToInfo(c)) {
			sptContainers = append(sptContainers, d.containerToInfo(c))
		}
	}

	logging.LogDebug("portcheck", "found Spt containers", "count", len(sptContainers))
	return sptContainers, nil
}

// containerUsesPort checks if a container is using the specified port
func (d *DockerContainerFinder) containerUsesPort(c containertypes.Summary, port string) bool {
	// Check published ports
	for _, p := range c.Ports {
		if p.PublicPort != 0 && fmt.Sprintf("%d", p.PublicPort) == port {
			return true
		}
		if p.PrivatePort != 0 && fmt.Sprintf("%d", p.PrivatePort) == port {
			return true
		}
	}

	return false
}

// containerToInfo converts Docker API container info to our ContainerInfo struct
func (d *DockerContainerFinder) containerToInfo(c containertypes.Summary) *ContainerInfo {
	info := &ContainerInfo{
		ID:      c.ID,
		Image:   c.Image,
		State:   c.State,
		Labels:  c.Labels,
		Created: time.Unix(c.Created, 0),
	}

	// Extract container name (remove leading slash)
	info.Name = d.getContainerName(c)

	// Format port information
	var ports []string
	for _, p := range c.Ports {
		if p.PublicPort != 0 {
			portStr := fmt.Sprintf("%s:%d->%d/%s", p.IP, p.PublicPort, p.PrivatePort, p.Type)
			ports = append(ports, portStr)
		} else {
			portStr := fmt.Sprintf("%d/%s", p.PrivatePort, p.Type)
			ports = append(ports, portStr)
		}
	}
	info.Ports = ports

	return info
}

// getContainerName extracts the container name, removing Docker's leading slash
func (d *DockerContainerFinder) getContainerName(c containertypes.Summary) string {
	if len(c.Names) > 0 {
		name := c.Names[0]
		if strings.HasPrefix(name, "/") {
			return name[1:]
		}
		return name
	}
	if len(c.ID) >= 12 {
		return c.ID[:12] // Fallback to short ID
	}
	return c.ID // Return full ID if shorter than 12 chars
}

// StopContainer stops a container by ID
func (d *DockerContainerFinder) StopContainer(ctx context.Context, containerID string) error {
	timeout := 10 * time.Second

	logging.LogInfo("portcheck", "stopping container",
		"containerID", containerID[:12],
		"timeout", timeout)

	err := d.client.ContainerStop(ctx, containerID, containertypes.StopOptions{
		Timeout: &[]int{int(timeout.Seconds())}[0],
	})
	if err != nil {
		return fmt.Errorf("failed to stop container %s: %w", containerID[:12], err)
	}

	logging.LogInfo("portcheck", "container stopped successfully",
		"containerID", containerID[:12])
	return nil
}

// RemoveContainer removes a container by ID
func (d *DockerContainerFinder) RemoveContainer(ctx context.Context, containerID string, force bool) error {
	logging.LogInfo("portcheck", "removing container",
		"containerID", containerID[:12],
		"force", force)

	options := containertypes.RemoveOptions{
		Force: force,
	}

	err := d.client.ContainerRemove(ctx, containerID, options)
	if err != nil {
		return fmt.Errorf("failed to remove container %s: %w", containerID[:12], err)
	}

	logging.LogInfo("portcheck", "container removed successfully",
		"containerID", containerID[:12])
	return nil
}

// GetContainerStatus gets the current status of a container
func (d *DockerContainerFinder) GetContainerStatus(ctx context.Context, containerID string) (string, error) {
	inspect, err := d.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return "", fmt.Errorf("failed to inspect container %s: %w", containerID[:12], err)
	}

	return inspect.State.Status, nil
}

// Close cleans up Docker client resources
func (d *DockerContainerFinder) Close() error {
	if d.client != nil {
		return d.client.Close()
	}
	return nil
}

// IsSptContainer checks if a container is running a Spt image
func IsSptContainer(container *ContainerInfo) bool {
	if container == nil {
		return false
	}

	// Known Spt image patterns
	sptImages := []string{
		"spt",
		"dellspt/spt",
		"dcr.octo.dell.com/spt/spt",
	}

	imageLower := strings.ToLower(container.Image)
	for _, pattern := range sptImages {
		if strings.Contains(imageLower, strings.ToLower(pattern)) {
			return true
		}
	}

	// Check labels for Spt indicators
	if container.Labels != nil {
		for key, value := range container.Labels {
			keyLower := strings.ToLower(key)
			valueLower := strings.ToLower(value)

			if strings.Contains(keyLower, "spt") ||
				strings.Contains(valueLower, "spt") {
				return true
			}
		}
	}

	// Check container name
	if strings.Contains(strings.ToLower(container.Name), "spt") {
		return true
	}

	return false
}

// ForceStopAndRemoveContainer forcefully stops and removes a container
func (d *DockerContainerFinder) ForceStopAndRemoveContainer(ctx context.Context, containerID string) error {
	logging.LogInfo("portcheck", "force stopping and removing container",
		"containerID", containerID[:12])

	// First try to stop the container
	if err := d.StopContainer(ctx, containerID); err != nil {
		logging.LogDebug("portcheck", "normal stop failed, trying force stop",
			"containerID", containerID[:12],
			"error", err.Error())
	}

	// Then remove it (with force flag)
	if err := d.RemoveContainer(ctx, containerID, true); err != nil {
		return fmt.Errorf("failed to force remove container: %w", err)
	}

	return nil
}

// WaitForContainerToStop waits for a container to stop within the timeout
func (d *DockerContainerFinder) WaitForContainerToStop(ctx context.Context, containerID string, timeout time.Duration) error {
	deadline := d.clock.Now().Add(timeout)

	for d.clock.Now().Before(deadline) {
		status, err := d.GetContainerStatus(ctx, containerID)
		if err != nil {
			// Container might be gone, which is what we want
			return nil
		}

		if status != "running" {
			return nil
		}

		// Wait a bit before checking again
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.clock.After(500 * time.Millisecond):
			continue
		}
	}

	return fmt.Errorf("container did not stop within %v", timeout)
}

// FindContainersByPort is a helper function that finds containers using a port
func FindContainersByPort(ctx context.Context, port string) (*ContainerInfo, error) {
	finder, err := NewDockerContainerFinder()
	if err != nil {
		return nil, err
	}
	defer func() { _ = finder.Close() }()

	return finder.FindContainerUsingPort(ctx, port)
}

// StopContainerByID is a helper function to stop a container
func StopContainerByID(ctx context.Context, containerID string) error {
	finder, err := NewDockerContainerFinder()
	if err != nil {
		return err
	}
	defer func() { _ = finder.Close() }()

	return finder.StopContainer(ctx, containerID)
}
