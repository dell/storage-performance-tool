/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/containerutil"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	containertypes "github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"
)

// dockerAPI is the part of the Docker client the finder uses; tests supply a fake.
type dockerAPI interface {
	ContainerList(ctx context.Context, options containertypes.ListOptions) ([]containertypes.Summary, error)
	ContainerStop(ctx context.Context, containerID string, options containertypes.StopOptions) error
	ContainerRemove(ctx context.Context, containerID string, options containertypes.RemoveOptions) error
	ContainerInspect(ctx context.Context, containerID string) (containertypes.InspectResponse, error)
	DaemonHost() string
	Close() error
}

// DockerContainerFinder handles Docker container detection and management
type DockerContainerFinder struct {
	client dockerAPI
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

// FindContainerUsingPort finds a running Docker container that publishes the
// specified host port over TCP. A container that only exposes the port, or maps
// a different host port to it, does not hold the host port and is not returned.
func (d *DockerContainerFinder) FindContainerUsingPort(ctx context.Context, port string) (*ContainerInfo, error) {
	logging.LogDebug("portcheck", "searching for containers using port", "port", port)

	// Only running (and paused) containers can hold a host port. Docker's
	// default listing omits stopped containers; containerHoldsPorts filters
	// the rest (a restarting container has no published ports).
	containers, err := d.client.ContainerList(ctx, containertypes.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	for _, c := range containers {
		if !containerHoldsPorts(c) {
			continue
		}
		if d.containerUsesPort(c, port) {
			logging.LogInfo("portcheck", "found container using port",
				"containerID", containerutil.ShortID(c.ID),
				"name", d.getContainerName(c),
				"port", port)

			return d.containerToInfo(c), nil
		}
	}

	logging.LogDebug("portcheck", "no containers found using port", "port", port)
	return nil, nil
}

// DaemonIsLocal reports whether the Docker daemon is reached through a local
// socket: a Unix socket, or a named pipe on this machine (npipe:////./pipe/...).
// A daemon reached over TCP or SSH, even through a forwarded local port, or a pipe
// on another machine (npipe:////host/pipe/...), may run on another host, so its
// containers need not hold this host's ports. An address of any other form is not
// taken as local.
//
// The scheme is compared as the Docker client's dialer compares it, letter case
// included: it reaches a socket only for "unix" and "npipe", and dials any other
// scheme, "UNIX" too, over TCP. Once the scheme is "npipe", the pipe path is a
// Windows path, so "//./pipe/" is compared without letter case.
func (d *DockerContainerFinder) DaemonIsLocal() bool {
	scheme, path, ok := strings.Cut(d.client.DaemonHost(), "://")
	if !ok {
		return false
	}
	switch scheme {
	case "unix":
		return true
	case "npipe":
		return strings.HasPrefix(strings.ToLower(path), "//./pipe/")
	default:
		return false
	}
}

// FindPublishers returns every running container that publishes TCP port on a
// binding that covers ip: one on ip itself, or on the wildcard of ip's family
// (0.0.0.0 for IPv4, :: for IPv6). A binding on another address or of the other
// family does not accept a connection to ip, and neither does one whose address
// the daemon does not give.
func (d *DockerContainerFinder) FindPublishers(ctx context.Context, ip net.IP, port string) ([]*ContainerInfo, error) {
	if ip == nil {
		return nil, nil
	}
	containers, err := d.client.ContainerList(ctx, containertypes.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}
	var publishers []*ContainerInfo
	for _, c := range containers {
		if !containerHoldsPorts(c) {
			continue
		}
		for _, p := range c.Ports {
			if p.Type == "tcp" && p.PublicPort != 0 && strconv.Itoa(int(p.PublicPort)) == port && bindingCovers(p.IP, ip) {
				publishers = append(publishers, d.containerToInfo(c))
				break
			}
		}
	}
	return publishers, nil
}

// FindPortNumberPublishers returns every running container that publishes TCP
// port, whatever the address of the binding, or with none given. It only names
// candidates in a message: a binding that does not cover the address that
// answered does not show that its container holds the port.
func (d *DockerContainerFinder) FindPortNumberPublishers(ctx context.Context, port string) ([]*ContainerInfo, error) {
	containers, err := d.client.ContainerList(ctx, containertypes.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}
	var publishers []*ContainerInfo
	for _, c := range containers {
		if containerHoldsPorts(c) && d.containerUsesPort(c, port) {
			publishers = append(publishers, d.containerToInfo(c))
		}
	}
	return publishers, nil
}

// bindingCovers reports whether a published binding on bindIP accepts a
// connection to ip.
func bindingCovers(bindIP string, ip net.IP) bool {
	bound := net.ParseIP(bindIP)
	if bound == nil {
		return false
	}
	if bound.IsUnspecified() {
		return (bound.To4() != nil) == (ip.To4() != nil)
	}
	return bound.Equal(ip)
}

// isEngineNodeContainer reports whether SPT labelled the container as an engine node.
func isEngineNodeContainer(labels map[string]string) bool {
	return labels[constants.DockerLabelManaged] == "true" && labels[constants.DockerLabelRole] == constants.DockerRoleNode
}

// FindHostNetworkEngine finds the SPT-managed node container that serves the API
// on port from the host's network. Such a container publishes no ports, so only
// its labels, network mode and --run-port argument identify it. A bridge engine
// with the same argument holds that port only inside its own network and is not
// returned. Two matches cannot be told apart, so they are an error.
func (d *DockerContainerFinder) FindHostNetworkEngine(ctx context.Context, port string) (*ContainerInfo, error) {
	containers, err := d.client.ContainerList(ctx, containertypes.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list containers: %w", err)
	}

	runPortArg := constants.EngineRunPortArgPrefix + port
	var found *ContainerInfo
	for _, c := range containers {
		if !containerHoldsPorts(c) || !isEngineNodeContainer(c.Labels) {
			continue
		}
		inspect, err := d.client.ContainerInspect(ctx, c.ID)
		if err != nil {
			return nil, fmt.Errorf("failed to inspect container %s: %w", containerutil.ShortID(c.ID), err)
		}
		if inspect.ContainerJSONBase == nil || inspect.HostConfig == nil ||
			!inspect.HostConfig.NetworkMode.IsHost() || !slices.Contains(inspect.Args, runPortArg) {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("more than one host-network SPT engine is set to serve port %s", port)
		}
		found = d.containerToInfo(c)
	}
	return found, nil
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

// containerHoldsPorts reports whether the container is in a state that can
// hold a published host port. It guards against a daemon that lists stopped
// containers, or reports ports for them, despite the default listing options.
func containerHoldsPorts(c containertypes.Summary) bool {
	return c.State == containertypes.StateRunning || c.State == containertypes.StatePaused
}

// containerUsesPort checks if a container publishes the specified host port
// over TCP. The container-side port (PrivatePort) holds no host port in
// bridge mode, so it is not compared.
func (d *DockerContainerFinder) containerUsesPort(c containertypes.Summary, port string) bool {
	for _, p := range c.Ports {
		if p.PublicPort != 0 && p.Type == "tcp" && fmt.Sprintf("%d", p.PublicPort) == port {
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
	return containerutil.ShortID(c.ID) // Fallback to short ID
}

// StopContainer stops a container by ID
func (d *DockerContainerFinder) StopContainer(ctx context.Context, containerID string) error {
	timeout := 10 * time.Second

	logging.LogInfo("portcheck", "stopping container",
		"containerID", containerutil.ShortID(containerID),
		"timeout", timeout)

	err := d.client.ContainerStop(ctx, containerID, containertypes.StopOptions{
		Timeout: &[]int{int(timeout.Seconds())}[0],
	})
	if err != nil {
		return fmt.Errorf("failed to stop container %s: %w", containerutil.ShortID(containerID), err)
	}

	logging.LogInfo("portcheck", "container stopped successfully",
		"containerID", containerutil.ShortID(containerID))
	return nil
}

// RemoveContainer removes a container by ID
func (d *DockerContainerFinder) RemoveContainer(ctx context.Context, containerID string, force bool) error {
	logging.LogInfo("portcheck", "removing container",
		"containerID", containerutil.ShortID(containerID),
		"force", force)

	options := containertypes.RemoveOptions{
		Force: force,
	}

	err := d.client.ContainerRemove(ctx, containerID, options)
	if err != nil {
		return fmt.Errorf("failed to remove container %s: %w", containerutil.ShortID(containerID), err)
	}

	logging.LogInfo("portcheck", "container removed successfully",
		"containerID", containerutil.ShortID(containerID))
	return nil
}

// GetContainerStatus gets the current status of a container
func (d *DockerContainerFinder) GetContainerStatus(ctx context.Context, containerID string) (string, error) {
	inspect, err := d.client.ContainerInspect(ctx, containerID)
	if err != nil {
		return "", fmt.Errorf("failed to inspect container %s: %w", containerutil.ShortID(containerID), err)
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
		"ghcr.io/dell/storage-performance-tool",
		"ghcr.io/dell/storage-performance-tool",
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
		"containerID", containerutil.ShortID(containerID))

	// First try to stop the container
	if err := d.StopContainer(ctx, containerID); err != nil {
		logging.LogDebug("portcheck", "normal stop failed, trying force stop",
			"containerID", containerutil.ShortID(containerID),
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
