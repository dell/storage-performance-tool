/*
Copyright © 2025 Dell Technologies
*/

package verification

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
)

// VerificationConflictInfo represents information about port conflicts on a verification host.
//
//nolint:revive // keep exported name until API stabilizes; will alias/rename later
type VerificationConflictInfo struct {
	Host           *hostparse.HostInfo      // The host being checked
	ConflictPorts  []int                    // Ports that are in use
	AvailablePorts []int                    // Ports that are available
	Containers     []*VerificationContainer // Found containers using the ports
	IsSptHost      bool                     // Whether any Spt containers were found
	// Raw listener lines from ss/netstat for each conflicted port
	PortDetails map[int][]string
}

// VerificationContainer represents a container found on a remote host during verification.
//
//nolint:revive // keep exported name until API stabilizes; will alias/rename later
type VerificationContainer struct {
	ID      string    // Container ID
	Name    string    // Container name
	Image   string    // Container image
	State   string    // Container state (running, exited, etc.)
	Ports   []string  // Port bindings
	Created time.Time // When container was created
}

// VerificationResolutionStrategy defines how to handle verification conflicts.
//
//nolint:revive // keep exported name until API stabilizes; will alias/rename later
type VerificationResolutionStrategy string

const (
	// VerifyStrategyCleanup cleans up conflicting containers and continues.
	VerifyStrategyCleanup VerificationResolutionStrategy = "cleanup"
	// VerifyStrategySkip skips this host (marked failed) and continues.
	VerifyStrategySkip VerificationResolutionStrategy = "skip"
	// VerifyStrategyAbort aborts the entire verification.
	VerifyStrategyAbort VerificationResolutionStrategy = "abort"
	// VerifyStrategyRetry lets the user handle conflicts and retry.
	VerifyStrategyRetry VerificationResolutionStrategy = "retry"
)

// VerificationResolutionResult contains the outcome of conflict resolution.
//
//nolint:revive // keep exported name until API stabilizes; will alias/rename later
type VerificationResolutionResult struct {
	Strategy VerificationResolutionStrategy
	Success  bool
	Message  string
	Error    error
}

// RemotePortChecker handles port conflict detection on remote hosts via SSH.
type RemotePortChecker struct {
	host        *hostparse.HostInfo
	cmdExecutor command.CommandExecutor
	ports       []int // Ports to check (1099, 9999, etc.)
}

// NewRemotePortChecker creates a new remote port checker for verification.
func NewRemotePortChecker(host *hostparse.HostInfo, cmdExecutor command.CommandExecutor, ports []int) *RemotePortChecker {
	return &RemotePortChecker{
		host:        host,
		cmdExecutor: cmdExecutor,
		ports:       ports,
	}
}

// CheckForConflicts detects port conflicts and container issues on the remote host
func (rpc *RemotePortChecker) CheckForConflicts(ctx context.Context) (*VerificationConflictInfo, error) {
	logging.LogInfo("verify-conflicts", "checking for port conflicts on remote host",
		"host", rpc.host.Host,
		"ports", rpc.ports)

	info := &VerificationConflictInfo{
		Host:           rpc.host,
		ConflictPorts:  make([]int, 0),
		AvailablePorts: make([]int, 0),
		Containers:     make([]*VerificationContainer, 0),
		IsSptHost:      false,
		PortDetails:    make(map[int][]string),
	}

	// Snapshot listeners once and determine per-port status
	lines, snapErr := rpc.getListenerSnapshot(ctx)
	if snapErr != nil {
		logging.LogWarn("verify-conflicts", "failed to snapshot listeners", "host", rpc.host.Host, "error", snapErr.Error())
		// If snapshot fails, conservatively mark ports as conflicted to drive user action
		info.ConflictPorts = append(info.ConflictPorts, rpc.ports...)
	} else {
		for _, port := range rpc.ports {
			matching := filterLinesForPort(lines, port)
			if len(matching) > 0 {
				info.ConflictPorts = append(info.ConflictPorts, port)
				info.PortDetails[port] = matching
			} else {
				info.AvailablePorts = append(info.AvailablePorts, port)
			}
		}
	}

	// If we have conflicts, identify containers
	if len(info.ConflictPorts) > 0 {
		containers, err := rpc.findContainersUsingPorts(ctx, info.ConflictPorts)
		if err != nil {
			logging.LogWarn("verify-conflicts", "failed to identify containers using conflict ports",
				"host", rpc.host.Host, "error", err.Error())
		} else {
			info.Containers = containers
			info.IsSptHost = rpc.containsSptContainers(containers)
		}
	}

	logging.LogInfo("verify-conflicts", "port conflict check complete",
		"host", rpc.host.Host,
		"conflicts", len(info.ConflictPorts),
		"available", len(info.AvailablePorts),
		"containers", len(info.Containers),
		"spt_host", info.IsSptHost)

	return info, nil
}

// getListenerSnapshot returns all listeners (ss preferred, netstat fallback)
func (rpc *RemotePortChecker) getListenerSnapshot(ctx context.Context) ([]string, error) {
	cmd := []string{"sh", "-c", constants.PortSnapshotCommand}
	stdout, stderr, err := rpc.cmdExecutor.ExecuteCommand(ctx, rpc.host, cmd)
	if err != nil {
		return nil, fmt.Errorf("snapshot failed: %w (stderr: %s)", err, stderr)
	}
	if strings.TrimSpace(stdout) == "" {
		return []string{}, nil
	}
	return strings.Split(strings.TrimSpace(stdout), "\n"), nil
}

// filterLinesForPort filters snapshot lines for a specific port (shared helper signature)
func filterLinesForPort(lines []string, port int) []string {
	patterns := []string{
		fmt.Sprintf(":%d ", port),
		fmt.Sprintf(":%d]", port),
		fmt.Sprintf(":%d,", port),
		fmt.Sprintf(":%d\n", port),
		fmt.Sprintf(":%d\r", port),
	}
	out := []string{}
	for _, l := range lines {
		for _, p := range patterns {
			if strings.Contains(l, p) {
				out = append(out, l)
				break
			}
		}
	}
	return out
}

// isPortAvailable checks if a specific port is available on the remote host
// Deprecated helper: previously used by early verification flow; removed from call-sites.
// Keeping it created churn and linter noise, so delete unused implementation now.
// func (rpc *RemotePortChecker) isPortAvailable(ctx context.Context, port int) (bool, error) {
// 	// Prefer ss with netstat fallback; execute via sh -c for both local and remote
// 	cmd := []string{"sh", "-c", fmt.Sprintf(constants.PortCheckCommand, port, port)}
//
// 	stdout, stderr, err := rpc.cmdExecutor.ExecuteCommand(ctx, rpc.host, cmd)
//
// 	// grep exit code 1 => no matches => available (stderr often empty)
// 	if err != nil {
// 		if strings.TrimSpace(stdout) == "" && strings.TrimSpace(stderr) == "" {
// 			return true, nil
// 		}
// 		if strings.Contains(stderr, "exit status 1") {
// 			return true, nil
// 		}
// 	}
//
// 	// If tools are missing, assume available to avoid false conflicts
// 	if err != nil && (strings.Contains(stderr, "ss: not found") || strings.Contains(stderr, "netstat: not found") || strings.Contains(stderr, "command not found")) {
// 		logging.LogDebug("verify-conflicts", "port check tools missing; assuming available",
// 			"host", rpc.host.Host, "port", port, "stderr", stderr)
// 		return true, nil
// 	}
//
// 	if err != nil {
// 		return false, fmt.Errorf("failed to check port %d: %w (stderr: %s)", port, err, stderr)
// 	}
//
// 	if strings.TrimSpace(stdout) != "" {
// 		logging.LogDebug("verify-conflicts", "port in use",
// 			"host", rpc.host.Host, "port", port, "output", stdout)
// 		return false, nil
// 	}
//
// 	return true, nil
// }
// (deleted unused implementation)

// findConflictingContainers identifies Docker containers that might be causing conflicts
// Deprecated: not used by current verifier flow. Remove to satisfy unused checks.
// func (rpc *RemotePortChecker) findConflictingContainers(ctx context.Context) ([]*VerificationContainer, error) { /* deleted */ }

// findSptContainers looks for containers with spt-verify naming patterns
// Deprecated: internal helper no longer referenced.
// func (rpc *RemotePortChecker) findSptContainers(ctx context.Context) ([]*VerificationContainer, error) { /* deleted */ }

// findContainersUsingPorts identifies Docker containers that are using the specific ports in conflict
func (rpc *RemotePortChecker) findContainersUsingPorts(ctx context.Context, ports []int) ([]*VerificationContainer, error) {
	// Get ALL containers (running and stopped)
	cmd := []string{constants.DockerCommand, constants.DockerCmdPS, constants.DockerFlagAll, constants.DockerFlagFormat,
		constants.DockerConflictFormat}

	stdout, stderr, err := rpc.cmdExecutor.ExecuteCommand(ctx, rpc.host, cmd)
	if err != nil {
		if strings.Contains(stderr, "command not found") || strings.Contains(stderr, constants.DockerCommand+": not found") {
			logging.LogDebug("verify-conflicts", "Docker not available on host", "host", rpc.host.Host)
			return nil, fmt.Errorf("%s not available on host %s", constants.DockerCommand, rpc.host.Host)
		}
		return nil, fmt.Errorf("failed to list all containers: %w (stderr: %s)", err, stderr)
	}

	containers := make([]*VerificationContainer, 0)

	// Parse container output and filter by port usage
	lines := strings.Split(strings.TrimSpace(stdout), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}

		container, err := rpc.parseContainerLine(line)
		if err != nil {
			logging.LogWarn("verify-conflicts", "failed to parse container line",
				"host", rpc.host.Host, "line", line, "error", err.Error())
			continue
		}

		// Check if this container is using any of our ports
		if rpc.containerUsesAnyPort(container, ports) {
			containers = append(containers, container)
		}
	}

	return containers, nil
}

// containerUsesAnyPort checks if a container is using any of the specified ports
func (rpc *RemotePortChecker) containerUsesAnyPort(container *VerificationContainer, ports []int) bool {
	return containerutil.PortsUseAnyTarget(container.Ports, ports)
}

// parseContainerLine parses a single line from docker ps output
func (rpc *RemotePortChecker) parseContainerLine(line string) (*VerificationContainer, error) {
	parts := strings.Split(line, "\t")
	if len(parts) < 6 {
		return nil, fmt.Errorf("invalid container line format: expected 6 parts, got %d", len(parts))
	}

	container := &VerificationContainer{
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
func (rpc *RemotePortChecker) containsSptContainers(containers []*VerificationContainer) bool {
	return containerutil.AnySpt[*VerificationContainer](containers, func(c *VerificationContainer) string { return c.Name }, func(c *VerificationContainer) string { return c.Image })
}

// GetConflictDescription returns a human-readable description of the conflict
func (info *VerificationConflictInfo) GetConflictDescription() string {
	if len(info.ConflictPorts) == 0 {
		return "No port conflicts detected"
	}

	var description strings.Builder
	description.WriteString(fmt.Sprintf("Port conflicts on %s:", info.Host.Host))

	for _, port := range info.ConflictPorts {
		description.WriteString(fmt.Sprintf(" %d", port))
	}

	description.WriteString(fmt.Sprintf(" (%d containers found)", len(info.Containers)))

	return description.String()
}

// HasConflicts returns true if there are any port conflicts
func (info *VerificationConflictInfo) HasConflicts() bool {
	return containerutil.HasConflicts(info.ConflictPorts)
}

// GetSptContainers returns only the Spt-related containers
func (info *VerificationConflictInfo) GetSptContainers() []*VerificationContainer {
	sptContainers := make([]*VerificationContainer, 0)
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
func (info *VerificationConflictInfo) GenerateSSHStopCommands(_ string) []string {
	commands := []string{}

	for _, container := range info.Containers {
		// Only generate stop command for containers not matching the target image (in non-force mode)
		// or generate for all containers (to show user what would be stopped)
		containerIDShort := container.ID
		if len(containerIDShort) > 12 {
			containerIDShort = containerIDShort[:12]
		}

		var cmd string
		if info.Host.IsLocal {
			cmd = fmt.Sprintf("docker stop %s", containerIDShort)
		} else {
			cmd = fmt.Sprintf("ssh %s 'docker stop %s'", info.Host.GetSSHTarget(), containerIDShort)
		}

		commands = append(commands, cmd)
	}

	return commands
}

// GetContainerSummary returns a brief summary of container issues
func (info *VerificationConflictInfo) GetContainerSummary() string {
	if len(info.Containers) == 0 {
		return "No containers identified"
	}

	running := 0
	stopped := 0
	spt := 0

	for _, container := range info.Containers {
		if container.State == "running" {
			running++
		} else {
			stopped++
		}

		if strings.Contains(container.Image, "spt") {
			spt++
		}
	}

	summary := fmt.Sprintf("%d containers found", len(info.Containers))
	if running > 0 {
		summary += fmt.Sprintf(" (%d running", running)
		if stopped > 0 {
			summary += fmt.Sprintf(", %d stopped", stopped)
		}
		summary += ")"
	} else if stopped > 0 {
		summary += fmt.Sprintf(" (%d stopped)", stopped)
	}

	if spt > 0 {
		summary += fmt.Sprintf(", %d Spt", spt)
	}

	return summary
}

// VerificationConflictResolver handles conflict detection and resolution for verification
// VerificationConflictResolver guides user choices to resolve conflicts.
//
//nolint:revive // keep exported name until API stabilizes; will alias/rename later
type VerificationConflictResolver struct {
	cmdExecutor     command.CommandExecutor
	forceMode       bool
	interactiveMode bool
}

// NewVerificationConflictResolver creates a new verification conflict resolver
func NewVerificationConflictResolver(cmdExecutor command.CommandExecutor, forceMode bool) *VerificationConflictResolver {
	return &VerificationConflictResolver{
		cmdExecutor:     cmdExecutor,
		forceMode:       forceMode,
		interactiveMode: isInteractiveMode(),
	}
}

// CheckAndResolveConflicts checks for conflicts on a host and handles resolution
func (vcr *VerificationConflictResolver) CheckAndResolveConflicts(ctx context.Context, host *hostparse.HostInfo, ports []int) (*VerificationResolutionResult, error) {
	// Check for conflicts
	checker := NewRemotePortChecker(host, vcr.cmdExecutor, ports)
	conflictInfo, err := checker.CheckForConflicts(ctx)
	if err != nil {
		return &VerificationResolutionResult{
			Success: false,
			Error:   fmt.Errorf("failed to check for conflicts on %s: %w", host.Host, err),
		}, err
	}

	// No conflicts - proceed
	if !conflictInfo.HasConflicts() {
		logging.LogInfo("verify-conflicts", "no conflicts detected", "host", host.Host)
		return &VerificationResolutionResult{
			Strategy: VerifyStrategyRetry, // Not really a strategy, indicates success
			Success:  true,
			Message:  fmt.Sprintf("No port conflicts detected on %s", host.Host),
		}, nil
	}

	// Conflicts detected
	logging.LogInfo("verify-conflicts", "conflicts detected",
		"host", host.Host,
		"conflict_ports", len(conflictInfo.ConflictPorts),
		"containers", len(conflictInfo.Containers))

	// Handle force mode
	if vcr.forceMode {
		return vcr.executeForceResolution(ctx, conflictInfo)
	}

	// Present conflict and get user choice
	strategy, err := vcr.presentConflictAndGetChoice(ctx, conflictInfo)
	if err != nil {
		return &VerificationResolutionResult{
			Success: false,
			Error:   fmt.Errorf("failed to get user choice for %s: %w", host.Host, err),
		}, err
	}

	// Execute the chosen strategy
	return vcr.executeResolutionStrategy(ctx, strategy, conflictInfo)
}

// presentConflictAndGetChoice shows conflict info and gets user's resolution choice
func (vcr *VerificationConflictResolver) presentConflictAndGetChoice(ctx context.Context, info *VerificationConflictInfo) (VerificationResolutionStrategy, error) {
	// Display conflict information
	vcr.displayConflictInfo(info)

	// If not interactive (headless mode), default to cleanup if Spt containers found
	if !vcr.interactiveMode {
		if info.IsSptHost {
			logging.LogInfo("verify-conflicts", "non-interactive mode: defaulting to cleanup", "host", info.Host.Host)
			fmt.Printf("[CONFLICT] Non-interactive mode: attempting cleanup on %s\n", info.Host.Host)
			return VerifyStrategyCleanup, nil
		}
		logging.LogInfo("verify-conflicts", "non-interactive mode: unknown containers, skipping host", "host", info.Host.Host)
		fmt.Printf("[CONFLICT] Non-interactive mode: unknown containers on %s, skipping host\n", info.Host.Host)
		return VerifyStrategySkip, nil
	}

	// Interactive mode - present options and get user choice
	return vcr.getUserChoice(ctx, info)
}

// displayConflictInfo shows information about the port conflict (mirroring portcheck patterns)
func (vcr *VerificationConflictResolver) displayConflictInfo(info *VerificationConflictInfo) {
	fmt.Printf("\n🔴 Port Conflicts Detected on %s!\n", info.Host.Host)
	fmt.Printf("Ports in use: %v\n", info.ConflictPorts)

	if len(info.Containers) > 0 {
		fmt.Printf("\nConflicting containers:\n")

		// Display table header
		fmt.Printf("%-12s %-25s %-35s %-10s %-8s %s\n",
			"CONTAINER ID", "NAME", "IMAGE", "STATUS", "AGE", "PORTS")

		for _, container := range info.Containers {
			containerIDShort := container.ID
			if len(containerIDShort) > 12 {
				containerIDShort = containerIDShort[:12]
			}

			// Calculate age (simplified)
			age := "unknown"
			if !container.Created.IsZero() {
				duration := time.Since(container.Created)
				if duration.Hours() >= 24 {
					age = fmt.Sprintf("%.0fd", duration.Hours()/24)
				} else if duration.Hours() >= 1 {
					age = fmt.Sprintf("%.0fh", duration.Hours())
				} else if duration.Minutes() >= 1 {
					age = fmt.Sprintf("%.0fm", duration.Minutes())
				} else {
					age = fmt.Sprintf("%.0fs", duration.Seconds())
				}
			}

			// Truncate name and image for display
			name := container.Name
			if len(name) > 25 {
				name = name[:22] + "..."
			}
			image := container.Image
			if len(image) > 35 {
				image = image[:32] + "..."
			}

			// Join ports for display
			ports := strings.Join(container.Ports, ",")
			if len(ports) > 40 {
				ports = ports[:37] + "..."
			}

			fmt.Printf("%-12s %-25s %-35s %-10s %-8s %s\n",
				containerIDShort, name, image, container.State, age, ports)
		}

		// Show SSH commands to stop containers if not using force cleanup
		if !vcr.forceMode {
			targetImage := "ghcr.io/dell/storage-performance-tool" // Our target image
			stopCommands := info.GenerateSSHStopCommands(targetImage)
			if len(stopCommands) > 0 {
				fmt.Printf("\nTo manually stop conflicting containers, run:\n")
				for _, cmd := range stopCommands {
					fmt.Printf("  %s\n", cmd)
				}
			}
		}

		sptContainers := info.GetSptContainers()
		if len(sptContainers) > 0 {
			fmt.Printf("\nFound %d Spt container(s) that can be automatically cleaned up.\n", len(sptContainers))
		}
		// UX hint for quick remediation
		fmt.Printf("Tip: re-run verify with --force-cleanup to automatically stop these Spt containers.\n")
	}

	vcr.printListenerDetailsAndSuggestions(info)
}

func (vcr *VerificationConflictResolver) printListenerDetailsAndSuggestions(info *VerificationConflictInfo) {
	if len(info.PortDetails) == 0 {
		fmt.Printf("\n  ⚠️  Ports in use but no process details available (ss/netstat missing?)\n")
		return
	}
	fmt.Printf("\nListener details (ss/netstat):\n")
	for _, p := range info.ConflictPorts {
		if lines, ok := info.PortDetails[p]; ok && len(lines) > 0 {
			fmt.Printf("  • Port %d:\n", p)
			for _, line := range lines {
				fmt.Printf("    %s\n", line)
			}
		}
	}
	if sug := formatSuggestedActions(info); sug != "" {
		fmt.Print(sug)
	}
}

// getUserChoice presents options and gets the user's choice (mirroring portcheck patterns)
func (vcr *VerificationConflictResolver) getUserChoice(_ context.Context, info *VerificationConflictInfo) (VerificationResolutionStrategy, error) {
	fmt.Printf("\nResolution Options:\n")

	if info.IsSptHost || len(info.GetSptContainers()) > 0 {
		fmt.Printf("  1) [c] Cleanup - Remove conflicting containers and continue verification\n")
	} else {
		fmt.Printf("  1) [c] Cleanup - Attempt to clean up conflicting processes\n")
	}

	fmt.Printf("  2) [s] Skip - Skip this host (mark as failed) and continue with others\n")
	fmt.Printf("  3) [r] Retry - Check port status again (if you stopped processes manually)\n")
	fmt.Printf("  4) [a] Abort - Cancel entire verification process\n")

	fmt.Printf("\nWhat would you like to do for %s? ", info.Host.Host)

	// Read user input (mirroring portcheck input handling)
	input, err := vcr.readUserInput()
	if err != nil {
		return VerifyStrategyAbort, fmt.Errorf("failed to read user input: %w", err)
	}

	choice := strings.ToLower(strings.TrimSpace(input))

	switch choice {
	case "1", "c", "cleanup":
		return VerifyStrategyCleanup, nil
	case "2", "s", "skip":
		return VerifyStrategySkip, nil
	case "3", "r", "retry":
		return VerifyStrategyRetry, nil
	case "4", "a", "abort":
		return VerifyStrategyAbort, nil
	default:
		fmt.Printf("Invalid choice '%s'. Defaulting to skip host.\n", choice)
		return VerifyStrategySkip, nil
	}
}

// executeForceResolution handles --force-cleanup flag resolution
func (vcr *VerificationConflictResolver) executeForceResolution(ctx context.Context, info *VerificationConflictInfo) (*VerificationResolutionResult, error) {
	logging.LogInfo("verify-conflicts", "executing force resolution", "host", info.Host.Host)
	fmt.Printf("[FORCE] Port conflicts on %s - attempting automated cleanup\n", info.Host.Host)

	targetImage := "ghcr.io/dell/storage-performance-tool"
	matchingContainers, nonMatchingContainers := vcr.filterContainersByImage(info.Containers, targetImage)

	if len(matchingContainers) > 0 {
		fmt.Printf("[FORCE] Cleaning up %d matching Spt containers on %s...\n",
			len(matchingContainers), info.Host.Host)

		// Create a modified conflict info with only matching containers
		cleanupInfo := *info
		cleanupInfo.Containers = matchingContainers

		result := vcr.executeCleanup(ctx, &cleanupInfo)

		// If there are non-matching containers, add a warning to the result
		if len(nonMatchingContainers) > 0 && result.Success {
			result.Message = fmt.Sprintf("%s. Note: %d non-matching containers remain and may still cause conflicts",
				result.Message, len(nonMatchingContainers))
		}

		return result, nil
	}
	fmt.Printf("[FORCE] No matching containers found on %s - skipping host\n", info.Host.Host)
	fmt.Printf("[FORCE] Found %d non-matching containers that won't be auto-cleaned\n", len(nonMatchingContainers))
	return &VerificationResolutionResult{
		Strategy: VerifyStrategySkip,
		Success:  true,
		Message:  fmt.Sprintf("Host %s skipped - no matching containers for auto-cleanup", info.Host.Host),
	}, nil
}

// filterContainersByImage separates containers into matching and non-matching based on image name
func (vcr *VerificationConflictResolver) filterContainersByImage(containers []*VerificationContainer, targetImage string) ([]*VerificationContainer, []*VerificationContainer) {
	matching := make([]*VerificationContainer, 0)
	nonMatching := make([]*VerificationContainer, 0)

	for _, container := range containers {
		if strings.Contains(container.Image, "spt") || container.Image == targetImage {
			matching = append(matching, container)
		} else {
			nonMatching = append(nonMatching, container)
		}
	}

	return matching, nonMatching
}

// executeResolutionStrategy executes the chosen resolution strategy
func (vcr *VerificationConflictResolver) executeResolutionStrategy(ctx context.Context, strategy VerificationResolutionStrategy, info *VerificationConflictInfo) (*VerificationResolutionResult, error) {
	logging.LogInfo("verify-conflicts", "executing resolution strategy",
		"strategy", string(strategy),
		"host", info.Host.Host)

	switch strategy {
	case VerifyStrategyCleanup:
		return vcr.executeCleanup(ctx, info), nil
	case VerifyStrategySkip:
		return &VerificationResolutionResult{
			Strategy: VerifyStrategySkip,
			Success:  true,
			Message:  fmt.Sprintf("Host %s skipped by user choice", info.Host.Host),
		}, nil
	case VerifyStrategyRetry:
		return vcr.executeRetry(ctx, info), nil
	case VerifyStrategyAbort:
		return &VerificationResolutionResult{
			Strategy: VerifyStrategyAbort,
			Success:  false,
			Message:  "Verification cancelled by user",
		}, nil
	default:
		return &VerificationResolutionResult{
			Success: false,
			Error:   fmt.Errorf("unknown resolution strategy: %s", strategy),
		}, nil
	}
}

// executeCleanup removes conflicting containers
func (vcr *VerificationConflictResolver) executeCleanup(ctx context.Context, info *VerificationConflictInfo) *VerificationResolutionResult {
	if len(info.Containers) == 0 {
		return &VerificationResolutionResult{
			Strategy: VerifyStrategyCleanup,
			Success:  false,
			Message:  fmt.Sprintf("No containers to clean up on %s", info.Host.Host),
		}
	}

	fmt.Printf("Cleaning up containers on %s...\n", info.Host.Host)

	removedCount := 0
	errors := make([]string, 0)

	// Create Docker operations instance
	dockerOps := command.NewDockerOperations(vcr.cmdExecutor, info.Host)

	for _, container := range info.Containers {
		containerIDShort := container.ID
		if len(containerIDShort) > 12 {
			containerIDShort = containerIDShort[:12]
		}
		fmt.Printf("  Removing container %s (%s)...\n", container.Name, containerIDShort)

		// Force remove container using common operations
		err := dockerOps.StopContainer(ctx, container.ID)
		if err != nil {
			containerIDShort := container.ID
			if len(containerIDShort) > 12 {
				containerIDShort = containerIDShort[:12]
			}
			errorMsg := fmt.Sprintf("Failed to remove %s: %s", containerIDShort, err.Error())
			errors = append(errors, errorMsg)
			logging.LogWarn("verify-conflicts", "failed to remove container",
				"host", info.Host.Host,
				"container", container.ID,
				"error", err.Error())
			continue
		}

		logging.LogInfo("verify-conflicts", "removed container",
			"host", info.Host.Host,
			"container", container.ID)
		removedCount++
	}

	// Wait a moment for cleanup to complete
	time.Sleep(2 * time.Second)

	// Verify cleanup was successful
	checker := NewRemotePortChecker(info.Host, vcr.cmdExecutor, info.ConflictPorts)
	newInfo, err := checker.CheckForConflicts(ctx)
	if err != nil {
		return &VerificationResolutionResult{
			Strategy: VerifyStrategyCleanup,
			Success:  false,
			Message:  fmt.Sprintf("Failed to verify cleanup on %s: %s", info.Host.Host, err.Error()),
			Error:    err,
		}
	}

	if !newInfo.HasConflicts() {
		fmt.Printf("✅ Cleanup successful on %s - %d containers removed, ports now available\n",
			info.Host.Host, removedCount)
		return &VerificationResolutionResult{
			Strategy: VerifyStrategyCleanup,
			Success:  true,
			Message:  fmt.Sprintf("Cleanup successful on %s", info.Host.Host),
		}
	}

	// Some conflicts remain
	message := fmt.Sprintf("Partial cleanup on %s: %d containers removed, but %d ports still conflicted",
		info.Host.Host, removedCount, len(newInfo.ConflictPorts))

	if len(errors) > 0 {
		message += fmt.Sprintf(" (errors: %s)", strings.Join(errors, "; "))
	}

	return &VerificationResolutionResult{
		Strategy: VerifyStrategyCleanup,
		Success:  false,
		Message:  message,
	}
}

// executeRetry re-checks port status on the host
func (vcr *VerificationConflictResolver) executeRetry(ctx context.Context, info *VerificationConflictInfo) *VerificationResolutionResult {
	fmt.Printf("Rechecking port status on %s...\n", info.Host.Host)

	checker := NewRemotePortChecker(info.Host, vcr.cmdExecutor, info.ConflictPorts)
	newInfo, err := checker.CheckForConflicts(ctx)
	if err != nil {
		return &VerificationResolutionResult{
			Strategy: VerifyStrategyRetry,
			Success:  false,
			Message:  fmt.Sprintf("Failed to recheck ports on %s: %s", info.Host.Host, err.Error()),
			Error:    err,
		}
	}

	if !newInfo.HasConflicts() {
		fmt.Printf("✅ Ports on %s are now available!\n", info.Host.Host)
		return &VerificationResolutionResult{
			Strategy: VerifyStrategyRetry,
			Success:  true,
			Message:  fmt.Sprintf("Ports on %s are now available", info.Host.Host),
		}
	}

	fmt.Printf("❌ Ports on %s are still conflicted: %v\n", info.Host.Host, newInfo.ConflictPorts)
	return &VerificationResolutionResult{
		Strategy: VerifyStrategyRetry,
		Success:  false,
		Message:  fmt.Sprintf("Ports on %s still conflicted", info.Host.Host),
	}
}

// readUserInput reads user input with basic validation
func (vcr *VerificationConflictResolver) readUserInput() (string, error) {
	// Simple input reading - in a full implementation, might use bufio.Reader
	var input string
	_, err := fmt.Scanln(&input)
	return input, err
}

// isInteractiveMode detects if we're in interactive mode (TTY available)
// Mirroring the implementation from portcheck/resolver.go
func isInteractiveMode() bool {
	// For now, assume interactive. In production, would check os.Stdin.Stat()
	return true
}
