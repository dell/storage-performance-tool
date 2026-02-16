/*
Copyright © 2025 Dell Technologies
*/

package docker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/containerutil"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/netprobe"
)

// ConflictInfo represents information about port conflicts on a Docker host
type ConflictInfo struct {
	Host           *hostparse.HostInfo // The host being checked
	ConflictPorts  []int               // Ports that are in use
	AvailablePorts []int               // Ports that are available
	Containers     []*ContainerInfo    // Found containers using the ports
	IsSptHost      bool                // Whether any Spt containers were found
	// Raw listener lines from ss/netstat for each conflicted port (helps users debug)
	PortDetails map[int][]string
	// Causes per port derived from listeners and container mapping
	PortCauses map[int][]PortCause
}

// PortCause describes the process (and optional container) bound to a port
type PortCause struct {
	Process       string
	PID           int
	ContainerName string
	ContainerID   string
}

// ContainerInfo represents a container found on a remote host
type ContainerInfo struct {
	ID      string    // Container ID
	Name    string    // Container name
	Image   string    // Container image
	State   string    // Container state (running, exited, etc.)
	Ports   []string  // Port bindings
	Created time.Time // When container was created
}

// PortChecker handles port conflict detection on hosts via SSH/local commands
type PortChecker struct {
	host        *hostparse.HostInfo
	cmdExecutor command.CommandExecutor
	ports       []int // Ports to check
}

// NewPortChecker creates a new port checker for Docker conflict detection
func NewPortChecker(host *hostparse.HostInfo, cmdExecutor command.CommandExecutor, ports []int) *PortChecker {
	return &PortChecker{
		host:        host,
		cmdExecutor: cmdExecutor,
		ports:       ports,
	}
}

// CheckForConflicts detects port conflicts and container issues on the host
func (pc *PortChecker) CheckForConflicts(ctx context.Context) (*ConflictInfo, error) {
	logging.LogInfo("docker-conflicts", "checking for port conflicts on host",
		"host", pc.host.Host,
		"ports", pc.ports)

	info := &ConflictInfo{
		Host:           pc.host,
		ConflictPorts:  make([]int, 0),
		AvailablePorts: make([]int, 0),
		Containers:     make([]*ContainerInfo, 0),
		IsSptHost:      false,
		PortDetails:    make(map[int][]string),
		PortCauses:     make(map[int][]PortCause),
	}

	// Take a single listeners snapshot and parse locally
	listeners, snapErr := netprobe.SnapshotListeners(ctx, pc.cmdExecutor, pc.host)
	if snapErr != nil {
		// If snapshot fails entirely, conservatively mark all ports as conflicted
		logging.LogWarn("docker-conflicts", "failed to snapshot listeners; assuming conflicts", "host", pc.host.Host, "error", snapErr.Error())
		info.ConflictPorts = append(info.ConflictPorts, pc.ports...)
	} else {
		for _, port := range pc.ports {
			matching := netprobe.FilterByPort(listeners, port)
			if len(matching) > 0 {
				info.ConflictPorts = append(info.ConflictPorts, port)
				// Record details and causes
				lines := make([]string, 0, len(matching))
				causes := make([]PortCause, 0, len(matching))
				for _, lst := range matching {
					lines = append(lines, lst.Raw)
					causes = append(causes, PortCause{Process: lst.Process, PID: lst.PID})
				}
				info.PortDetails[port] = lines
				info.PortCauses[port] = causes
				logging.LogDebug("docker-conflicts", "port in use",
					"host", pc.host.Host, "port", port)
			} else {
				info.AvailablePorts = append(info.AvailablePorts, port)
				logging.LogDebug("docker-conflicts", "port available",
					"host", pc.host.Host, "port", port)
			}
		}
	}

	// If we have conflicts, identify containers
	if len(info.ConflictPorts) > 0 {
		containers, err := pc.findContainersUsingPorts(ctx, info.ConflictPorts)
		if err != nil {
			logging.LogWarn("docker-conflicts", "failed to identify containers using conflict ports",
				"host", pc.host.Host, "error", err.Error())
		} else {
			info.Containers = containers
			info.IsSptHost = pc.containsSptContainers(containers)
			// Enrich causes with container mapping by port
			for _, port := range info.ConflictPorts {
				for i, cause := range info.PortCauses[port] {
					if c := pc.findContainerByPort(containers, port); c != nil {
						cause.ContainerName = c.Name
						cause.ContainerID = c.ID
						info.PortCauses[port][i] = cause
					}
				}
			}
		}
	}

	logging.LogInfo("docker-conflicts", "port conflict check complete",
		"host", pc.host.Host,
		"conflicts", len(info.ConflictPorts),
		"available", len(info.AvailablePorts),
		"containers", len(info.Containers),
		"spt_host", info.IsSptHost)

	return info, nil
}

// getPortDetails returns raw lines from ss/netstat for a given port
// getListenerSnapshot returns all listening sockets once (ss preferred, netstat fallback)
// findContainerByPort returns a container that binds the specific host port
func (pc *PortChecker) findContainerByPort(containers []*ContainerInfo, port int) *ContainerInfo {
	for _, c := range containers {
		for _, mapping := range c.Ports {
			if strings.Contains(mapping, fmt.Sprintf(":%d->", port)) || strings.Contains(mapping, fmt.Sprintf(":%d/", port)) {
				return c
			}
		}
	}
	return nil
}

// isPortAvailable checks if a specific port is available on the host
// isPortAvailable moved to test helpers (unused in production)

// findContainersUsingPorts identifies Docker containers that are using the specific ports in conflict
func (pc *PortChecker) findContainersUsingPorts(ctx context.Context, ports []int) ([]*ContainerInfo, error) {
	// Get ALL containers (running and stopped)
	cmd := []string{constants.DockerCommand, constants.DockerCmdPS, constants.DockerFlagAll, constants.DockerFlagFormat,
		constants.DockerConflictFormat}

	stdout, stderr, err := pc.cmdExecutor.ExecuteCommand(ctx, pc.host, cmd)
	if err != nil {
		if strings.Contains(stderr, "command not found") || strings.Contains(stderr, constants.DockerCommand+": not found") {
			logging.LogDebug("docker-conflicts", "Docker not available on host", "host", pc.host.Host)
			return nil, fmt.Errorf("%s not available on host %s", constants.DockerCommand, pc.host.Host)
		}
		return nil, fmt.Errorf("failed to list all containers: %w (stderr: %s)", err, stderr)
	}

	containers := make([]*ContainerInfo, 0)

	// Parse container output and filter by port usage
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		container, err := pc.parseContainerLine(line)
		if err != nil {
			logging.LogWarn("docker-conflicts", "failed to parse container line",
				"host", pc.host.Host, "line", line, "error", err.Error())
			continue
		}

		// Check if this container is using any of our ports
		if pc.containerUsesAnyPort(container, ports) {
			containers = append(containers, container)
		}
	}

	return containers, nil
}

// containerUsesAnyPort checks if a container is using any of the specified ports
func (pc *PortChecker) containerUsesAnyPort(container *ContainerInfo, ports []int) bool {
	return containerutil.PortsUseAnyTarget(container.Ports, ports)
}

// parseContainerLine parses a single line from docker ps output
func (pc *PortChecker) parseContainerLine(line string) (*ContainerInfo, error) {
	parts := strings.Split(line, "\t")
	if len(parts) < 6 {
		return nil, fmt.Errorf("invalid container line format: expected 6 parts, got %d", len(parts))
	}

	container := &ContainerInfo{
		ID:    strings.TrimSpace(parts[0]),
		Name:  strings.TrimSpace(parts[1]),
		Image: strings.TrimSpace(parts[2]),
		State: strings.TrimSpace(parts[3]),
		Ports: strings.Split(strings.TrimSpace(parts[4]), ","),
	}

	// Parse created time if possible
	createdStr := strings.TrimSpace(parts[5])
	if created, err := time.Parse("2006-01-02 15:04:05 -0700 MST", createdStr); err == nil {
		container.Created = created
	}

	return container, nil
}

// containsSptContainers checks if any containers are Spt-related
func (pc *PortChecker) containsSptContainers(containers []*ContainerInfo) bool {
	return containerutil.AnySpt[*ContainerInfo](containers, func(c *ContainerInfo) string { return c.Name }, func(c *ContainerInfo) string { return c.Image })
}

// HasConflicts returns true if there are any port conflicts
func (info *ConflictInfo) HasConflicts() bool { return containerutil.HasConflicts(info.ConflictPorts) }

// GetSptContainers returns only the Spt-related containers
func (info *ConflictInfo) GetSptContainers() []*ContainerInfo {
	sptContainers := make([]*ContainerInfo, 0)
	for _, container := range info.Containers {
		if strings.Contains(container.Image, "spt") ||
			strings.Contains(container.Image, "storage-performance-tool") ||
			strings.HasPrefix(container.Name, "spt-verify") {
			sptContainers = append(sptContainers, container)
		}
	}
	return sptContainers
}

// GenerateSSHStopCommands creates copy-pasteable SSH commands to stop conflicting containers
func (info *ConflictInfo) GenerateSSHStopCommands() []string {
	commands := make([]string, 0, len(info.Containers))

	for _, container := range info.Containers {
		containerIDShort := container.ID
		if len(containerIDShort) > constants.ContainerIDDisplayLength {
			containerIDShort = containerIDShort[:constants.ContainerIDDisplayLength]
		}

		var cmd string
		if info.Host.IsLocal {
			cmd = fmt.Sprintf("%s %s %s", constants.DockerCommand, constants.DockerCmdStop, containerIDShort)
		} else {
			cmd = fmt.Sprintf("%s %s '%s %s %s'", constants.SSHCommand, info.Host.GetSSHTarget(), constants.DockerCommand, constants.DockerCmdStop, containerIDShort)
		}

		commands = append(commands, cmd)
	}

	return commands
}

// DisplayConflictInfo shows detailed information about port conflicts in a user-friendly format
func (info *ConflictInfo) DisplayConflictInfo() {
	fmt.Printf("\n🔴 Port Conflicts Detected on %s!\n", info.Host.Host)
	fmt.Printf("Ports in use: %v\n", info.ConflictPorts)

	if len(info.Containers) > 0 {
		info.printContainerDetails()
	}

	// Always show raw listener details to aid debugging
	if len(info.PortDetails) > 0 {
		fmt.Printf("\nListener details (ss/netstat):\n")
		for _, p := range info.ConflictPorts {
			if lines, ok := info.PortDetails[p]; ok && len(lines) > 0 {
				fmt.Printf("  • Port %d:\n", p)
				for _, line := range lines {
					fmt.Printf("    %s\n", line)
				}
			}
		}
	} else {
		fmt.Printf("\n  ⚠️  Ports in use but no process details available (ss/netstat missing?)\n")
	}
}

// printContainerDetails prints the conflicting container table and helper commands.
func (info *ConflictInfo) printContainerDetails() {
	fmt.Printf("\nConflicting containers:\n")
	fmt.Printf("%-12s %-25s %-35s %-10s %-8s %s\n",
		"CONTAINER ID", "NAME", "IMAGE", "STATUS", "AGE", "PORTS")

	for _, container := range info.Containers {
		containerIDShort := container.ID
		if len(containerIDShort) > constants.ContainerIDDisplayLength {
			containerIDShort = containerIDShort[:constants.ContainerIDDisplayLength]
		}

		age := "unknown"
		if !container.Created.IsZero() {
			duration := time.Since(container.Created)
			switch {
			case duration.Hours() >= 24:
				age = fmt.Sprintf("%.0fd", duration.Hours()/24)
			case duration.Hours() >= 1:
				age = fmt.Sprintf("%.0fh", duration.Hours())
			case duration.Minutes() >= 1:
				age = fmt.Sprintf("%.0fm", duration.Minutes())
			default:
				age = fmt.Sprintf("%.0fs", duration.Seconds())
			}
		}

		name := container.Name
		if len(name) > 25 {
			name = name[:22] + "..."
		}
		image := container.Image
		if len(image) > 35 {
			image = image[:32] + "..."
		}

		ports := strings.Join(container.Ports, ",")
		if len(ports) > 40 {
			ports = ports[:37] + "..."
		}

		fmt.Printf("%-12s %-25s %-35s %-10s %-8s %s\n",
			containerIDShort, name, image, container.State, age, ports)
	}

	stopCommands := info.GenerateSSHStopCommands()
	if len(stopCommands) > 0 {
		fmt.Printf("\nTo manually stop conflicting containers, run:\n")
		for _, cmd := range stopCommands {
			fmt.Printf("  %s\n", cmd)
		}
	}

	sptContainers := info.GetSptContainers()
	if len(sptContainers) > 0 {
		fmt.Printf("\nFound %d Spt container(s) that can be automatically cleaned up with --force-cleanup.\n", len(sptContainers))
	}
}
