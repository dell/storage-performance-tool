/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/logging"
)

// ResolutionStrategy defines how to handle port conflicts
type ResolutionStrategy string

const (
	// StrategyAbort cancels the pending operation due to conflicts.
	StrategyAbort ResolutionStrategy = "abort"
	// StrategyGracefulStop attempts to stop the conflicting service via its API.
	StrategyGracefulStop ResolutionStrategy = "graceful"
	// StrategyForceStop forcefully stops/removes the conflicting process/container.
	StrategyForceStop ResolutionStrategy = "force"
	// StrategyRetry re-checks the port after the user handles it manually.
	StrategyRetry ResolutionStrategy = "retry"
)

// ResolutionResult contains the outcome of conflict resolution
type ResolutionResult struct {
	Strategy   ResolutionStrategy
	Success    bool
	Message    string
	Error      error
	RetryAfter time.Duration // For retry strategy
}

// ConflictResolver handles port conflict detection and resolution
type ConflictResolver struct {
	Port            string
	checker         *PortChecker
	dockerFinder    *DockerContainerFinder
	forceMode       bool
	interactiveMode bool
	clock           Clock
}

// NewConflictResolver creates a new conflict resolver
func NewConflictResolver(port string, forceMode bool) (*ConflictResolver, error) {
	checker := NewPortChecker(port)

	dockerFinder, err := NewDockerContainerFinder()
	if err != nil {
		// Docker not available - continue without container management
		logging.LogDebug("portcheck", "Docker not available for conflict resolution",
			"error", err.Error())
		dockerFinder = nil
	}

	// Detect if we're in interactive mode (TTY available)
	interactiveMode := isInteractiveMode()

	return &ConflictResolver{
		Port:            port,
		checker:         checker,
		dockerFinder:    dockerFinder,
		forceMode:       forceMode,
		interactiveMode: interactiveMode,
		clock:           &RealClock{},
	}, nil
}

// CheckAndResolveConflicts detects port conflicts and handles resolution
func (r *ConflictResolver) CheckAndResolveConflicts(ctx context.Context) (*ResolutionResult, error) {
	logging.LogInfo("portcheck", "checking for port conflicts", "port", r.Port)

	// First, check if port is actually in use
	serviceInfo, err := r.checker.CheckService(ctx)
	if err != nil {
		return &ResolutionResult{
			Success: false,
			Error:   fmt.Errorf("failed to check port %s: %w", r.Port, err),
		}, err
	}

	// No conflict - port is available
	if !serviceInfo.IsListening {
		logging.LogInfo("portcheck", "port is available", "port", r.Port)
		return &ResolutionResult{
			Strategy: StrategyRetry, // Not really a strategy, but indicates success
			Success:  true,
			Message:  fmt.Sprintf("Port %s is available", r.Port),
		}, nil
	}

	// Port is in use - we have a conflict
	logging.LogInfo("portcheck", "port conflict detected",
		"port", r.Port,
		"is_spt", serviceInfo.IsSpt)

	// If --force flag is set, skip user interaction
	if r.forceMode {
		return r.executeForceResolution(ctx, serviceInfo)
	}

	// Present conflict information and get user choice
	strategy, err := r.presentConflictAndGetChoice(ctx, serviceInfo)
	if err != nil {
		return &ResolutionResult{
			Success: false,
			Error:   fmt.Errorf("failed to get user choice: %w", err),
		}, err
	}

	// Execute the chosen strategy
	return r.executeResolutionStrategy(ctx, strategy, serviceInfo)
}

// presentConflictAndGetChoice shows conflict info and gets the user's resolution choice.
func (r *ConflictResolver) presentConflictAndGetChoice(ctx context.Context, info *ServiceInfo) (ResolutionStrategy, error) {
	// Display conflict information
	r.displayConflictInfo(info)

	// If not interactive (headless mode), default to graceful approach
	if !r.interactiveMode {
		logging.LogInfo("portcheck", "non-interactive mode: defaulting to graceful resolution")
		fmt.Printf("[CONFLICT] Non-interactive mode: attempting graceful resolution for port %s\n", r.Port)
		return StrategyGracefulStop, nil
	}

	// Interactive mode - present options and get user choice
	return r.getUserChoice(ctx, info)
}

// displayConflictInfo shows information about the port conflict
func (r *ConflictResolver) displayConflictInfo(info *ServiceInfo) {
	fmt.Printf("\n🔴 Port Conflict Detected!\n")
	fmt.Printf("Port %s is currently in use by: %s\n", r.Port, info.GetServiceDescription())

	if info.IsSpt && info.Status != nil {
		fmt.Printf("\nSpt Test Details:\n")
		if info.Status.RunID != "" {
			fmt.Printf("  Run ID: %s\n", info.Status.RunID)
		}
		if info.Status.State != "" {
			fmt.Printf("  Status: %s\n", info.Status.State)
		}
		if info.Status.Message != "" {
			fmt.Printf("  Message: %s\n", info.Status.Message)
		}

		if info.IsTestActive() {
			fmt.Printf("  ⚠️  Active test is currently running\n")
		}
	}

	if info.Container != nil {
		fmt.Printf("\nContainer Details:\n")
		fmt.Printf("  Name: %s\n", info.Container.Name)
		fmt.Printf("  Image: %s\n", info.Container.Image)
		fmt.Printf("  State: %s\n", info.Container.State)
		fmt.Printf("  ID: %s\n", info.Container.ID[:12])
	}
}

// getUserChoice presents options and gets the user's choice.
func (r *ConflictResolver) getUserChoice(_ context.Context, info *ServiceInfo) (ResolutionStrategy, error) {
	fmt.Printf("\nResolution Options:\n")

	if info.IsSpt {
		fmt.Printf("  1) [g] Graceful stop - Stop the Spt test via API (recommended)\n")
		fmt.Printf("  2) [f] Force stop - Forcefully stop/remove containers\n")
	} else {
		fmt.Printf("  1) [f] Force stop - Forcefully stop the service/container\n")
	}

	fmt.Printf("  3) [r] Retry - Check port status again (if you stopped it manually)\n")
	fmt.Printf("  4) [a] Abort - Cancel spt operation and exit\n")

	fmt.Printf("\nWhat would you like to do? ")

	// Read user input
	reader := bufio.NewReader(os.Stdin)
	for {
		input, err := reader.ReadString('\n')
		if err != nil {
			return StrategyAbort, fmt.Errorf("failed to read user input: %w", err)
		}

		choice := strings.ToLower(strings.TrimSpace(input))

		switch choice {
		case "1", "g", "graceful":
			if info.IsSpt {
				return StrategyGracefulStop, nil
			}
			// For non-Spt services, option 1 is force stop
			return StrategyForceStop, nil

		case "2", "f", "force":
			return StrategyForceStop, nil

		case "3", "r", "retry":
			return StrategyRetry, nil

		case "4", "a", "abort":
			return StrategyAbort, nil

		default:
			fmt.Printf("Invalid choice '%s'. Please enter 1-4 or g/f/r/a: ", choice)
		}
	}
}

// executeForceResolution handles --force flag resolution
func (r *ConflictResolver) executeForceResolution(ctx context.Context, info *ServiceInfo) (*ResolutionResult, error) {
	logging.LogInfo("portcheck", "executing force resolution", "port", r.Port)
	fmt.Printf("[FORCE] Port %s conflict - attempting automated resolution\n", r.Port)

	// For Spt services, try graceful first, then force
	if info.IsSpt {
		fmt.Printf("[FORCE] Attempting graceful stop of Spt test...\n")
		result := r.executeGracefulStop(ctx, info)
		if result.Success {
			return result, nil
		}

		fmt.Printf("[FORCE] Graceful stop failed, trying force stop: %s\n", result.Message)
	}

	// Force stop containers or services
	fmt.Printf("[FORCE] Force stopping service on port %s\n", r.Port)
	return r.executeForceStop(ctx, info), nil
}

// executeResolutionStrategy executes the chosen resolution strategy.
func (r *ConflictResolver) executeResolutionStrategy(ctx context.Context, strategy ResolutionStrategy, info *ServiceInfo) (*ResolutionResult, error) {
	logging.LogInfo("portcheck", "executing resolution strategy",
		"strategy", string(strategy),
		"port", r.Port)

	switch strategy {
	case StrategyGracefulStop:
		return r.executeGracefulStop(ctx, info), nil

	case StrategyForceStop:
		return r.executeForceStop(ctx, info), nil

	case StrategyRetry:
		return r.executeRetry(ctx), nil

	case StrategyAbort:
		return &ResolutionResult{
			Strategy: StrategyAbort,
			Success:  false,
			Message:  "Operation cancelled by user",
		}, nil

	default:
		return &ResolutionResult{
			Success: false,
			Error:   fmt.Errorf("unknown resolution strategy: %s", strategy),
		}, nil
	}
}

// executeGracefulStop tries to stop Spt via API
func (r *ConflictResolver) executeGracefulStop(ctx context.Context, info *ServiceInfo) *ResolutionResult {
	if !info.IsSpt {
		return &ResolutionResult{
			Strategy: StrategyGracefulStop,
			Success:  false,
			Message:  "Graceful stop only works for Spt services",
		}
	}

	fmt.Printf("Attempting to stop Spt test gracefully...\n")

	baseURL := fmt.Sprintf("http://localhost:%s", r.Port)
	err := StopSptTest(ctx, baseURL)
	if err != nil {
		return &ResolutionResult{
			Strategy: StrategyGracefulStop,
			Success:  false,
			Message:  fmt.Sprintf("Failed to stop Spt test: %s", err.Error()),
			Error:    err,
		}
	}

	// Wait a moment and verify the test stopped
	r.clock.Sleep(2 * time.Second)

	// Re-check port status
	newInfo, err := r.checker.CheckService(ctx)
	if err != nil {
		return &ResolutionResult{
			Strategy: StrategyGracefulStop,
			Success:  false,
			Message:  fmt.Sprintf("Failed to verify port status after stop: %s", err.Error()),
			Error:    err,
		}
	}

	if !newInfo.IsListening {
		fmt.Printf("✅ Spt test stopped successfully - port %s is now available\n", r.Port)
		return &ResolutionResult{
			Strategy: StrategyGracefulStop,
			Success:  true,
			Message:  fmt.Sprintf("Spt test stopped gracefully, port %s is available", r.Port),
		}
	}

	// Test might still be stopping - give it more time
	return &ResolutionResult{
		Strategy: StrategyGracefulStop,
		Success:  false,
		Message:  "Spt test stop command sent but port is still in use",
	}
}

// executeForceStop forcefully stops containers or services
func (r *ConflictResolver) executeForceStop(ctx context.Context, info *ServiceInfo) *ResolutionResult {
	if r.dockerFinder == nil {
		return &ResolutionResult{
			Strategy: StrategyForceStop,
			Success:  false,
			Message:  "Docker not available for force stop operations",
		}
	}

	// If we have container info, stop it directly
	if info.Container != nil {
		fmt.Printf("Force stopping container: %s (%s)\n", info.Container.Name, info.Container.ID[:12])

		err := r.dockerFinder.ForceStopAndRemoveContainer(ctx, info.Container.ID)
		if err != nil {
			return &ResolutionResult{
				Strategy: StrategyForceStop,
				Success:  false,
				Message:  fmt.Sprintf("Failed to force stop container: %s", err.Error()),
				Error:    err,
			}
		}

		// Wait and verify
		r.clock.Sleep(3 * time.Second)
		return r.verifyResolution(ctx, StrategyForceStop)
	}

	// Try to find any containers using the port
	container, err := r.dockerFinder.FindContainerUsingPort(ctx, r.Port)
	if err != nil {
		return &ResolutionResult{
			Strategy: StrategyForceStop,
			Success:  false,
			Message:  fmt.Sprintf("Failed to find containers using port: %s", err.Error()),
			Error:    err,
		}
	}

	if container != nil {
		fmt.Printf("Found container using port %s: %s\n", r.Port, container.Name)
		fmt.Printf("Force stopping container...\n")

		err := r.dockerFinder.ForceStopAndRemoveContainer(ctx, container.ID)
		if err != nil {
			return &ResolutionResult{
				Strategy: StrategyForceStop,
				Success:  false,
				Message:  fmt.Sprintf("Failed to force stop container: %s", err.Error()),
				Error:    err,
			}
		}

		r.clock.Sleep(3 * time.Second)
		return r.verifyResolution(ctx, StrategyForceStop)
	}

	return &ResolutionResult{
		Strategy: StrategyForceStop,
		Success:  false,
		Message:  fmt.Sprintf("No containers found using port %s - service may not be containerized", r.Port),
	}
}

// executeRetry re-checks port status
func (r *ConflictResolver) executeRetry(ctx context.Context) *ResolutionResult {
	fmt.Printf("Rechecking port %s status...\n", r.Port)

	newInfo, err := r.checker.CheckService(ctx)
	if err != nil {
		return &ResolutionResult{
			Strategy: StrategyRetry,
			Success:  false,
			Message:  fmt.Sprintf("Failed to recheck port: %s", err.Error()),
			Error:    err,
		}
	}

	if !newInfo.IsListening {
		fmt.Printf("✅ Port %s is now available!\n", r.Port)
		return &ResolutionResult{
			Strategy: StrategyRetry,
			Success:  true,
			Message:  fmt.Sprintf("Port %s is now available", r.Port),
		}
	}

	fmt.Printf("❌ Port %s is still in use by: %s\n", r.Port, newInfo.GetServiceDescription())
	return &ResolutionResult{
		Strategy:   StrategyRetry,
		Success:    false,
		Message:    fmt.Sprintf("Port %s is still in use", r.Port),
		RetryAfter: 5 * time.Second,
	}
}

// verifyResolution checks if the resolution was successful
func (r *ConflictResolver) verifyResolution(ctx context.Context, strategy ResolutionStrategy) *ResolutionResult {
	newInfo, err := r.checker.CheckService(ctx)
	if err != nil {
		return &ResolutionResult{
			Strategy: strategy,
			Success:  false,
			Message:  fmt.Sprintf("Failed to verify resolution: %s", err.Error()),
			Error:    err,
		}
	}

	if !newInfo.IsListening {
		fmt.Printf("✅ Resolution successful - port %s is now available\n", r.Port)
		return &ResolutionResult{
			Strategy: strategy,
			Success:  true,
			Message:  fmt.Sprintf("Port %s is now available", r.Port),
		}
	}

	return &ResolutionResult{
		Strategy: strategy,
		Success:  false,
		Message:  fmt.Sprintf("Port %s is still in use after %s", r.Port, strategy),
	}
}

// Close cleans up resolver resources
func (r *ConflictResolver) Close() error {
	if r.checker != nil {
		if err := r.checker.Close(); err != nil {
			logging.LogDebug("portcheck", "failed to close port checker", "error", err.Error())
		}
	}

	if r.dockerFinder != nil {
		if err := r.dockerFinder.Close(); err != nil {
			logging.LogDebug("portcheck", "failed to close docker finder", "error", err.Error())
		}
	}

	return nil
}

// isInteractiveMode detects if we're in interactive mode (TTY available)
func isInteractiveMode() bool {
	fileInfo, err := os.Stdin.Stat()
	if err != nil {
		return false
	}

	// Check if stdin is a character device (TTY)
	return (fileInfo.Mode() & os.ModeCharDevice) != 0
}

// Helper function for easy conflict resolution
// ResolvePortConflict detects and interacts with any service using the given
// port, optionally forcing cleanup when forceMode is true. It returns a
// ResolutionResult describing the outcome.
//
//nolint:revive // comment form is acceptable in this codebase
func ResolvePortConflict(ctx context.Context, port string, forceMode bool) (*ResolutionResult, error) {
	resolver, err := NewConflictResolver(port, forceMode)
	if err != nil {
		return nil, fmt.Errorf("failed to create conflict resolver: %w", err)
	}
	defer func() { _ = resolver.Close() }()

	return resolver.CheckAndResolveConflicts(ctx)
}
