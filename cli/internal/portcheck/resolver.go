/*
Copyright © 2025 Dell Technologies
*/

package portcheck

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/containerutil"
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

// ErrPortConflictUnattended marks a conflict abort made because nobody could
// choose a resolution: no terminal, or end of input at the prompt. The result's
// Message says what holds the port and what to do about it.
var ErrPortConflictUnattended = errors.New("port conflict left unresolved without an operator decision")

// errNoOperatorAnswer reports that no resolution choice can be obtained.
var errNoOperatorAnswer = errors.New("no operator answer available")

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
	// input supplies the operator's answers; nil means os.Stdin.
	input io.Reader
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
	serviceInfo, err := r.checkService(ctx)
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
	if errors.Is(err, errNoOperatorAnswer) {
		return r.abortUnattended(serviceInfo), nil
	}
	if err != nil {
		return &ResolutionResult{
			Success: false,
			Error:   fmt.Errorf("failed to get user choice: %w", err),
		}, err
	}

	// Execute the chosen strategy
	return r.executeResolutionStrategy(ctx, strategy, serviceInfo)
}

// checkService identifies what holds the port, within the detection budget.
func (r *ConflictResolver) checkService(ctx context.Context) (*ServiceInfo, error) {
	ctx, cancel := context.WithTimeout(ctx, constants.PortConflictDetectionTimeout)
	defer cancel()
	return r.checker.CheckService(ctx)
}

// presentConflictAndGetChoice shows conflict info and gets the user's resolution choice.
func (r *ConflictResolver) presentConflictAndGetChoice(ctx context.Context, info *ServiceInfo) (ResolutionStrategy, error) {
	// Display conflict information
	r.displayConflictInfo(info)

	if !r.interactiveMode {
		// Stopping an engine may end another user's live run, so it takes
		// --force or an operator's choice.
		if info.IsSpt {
			logging.LogInfo("portcheck", "non-interactive mode: leaving the SPT engine alone")
			return StrategyAbort, errNoOperatorAnswer
		}
		logging.LogInfo("portcheck", "non-interactive mode: defaulting to graceful resolution")
		fmt.Printf("[CONFLICT] Non-interactive mode: attempting graceful resolution for port %s\n", r.Port)
		return StrategyGracefulStop, nil
	}

	// Interactive mode - present options and get user choice
	return r.getUserChoice(ctx, info)
}

// abortUnattended ends a conflict nobody can decide, leaving the holder alone.
func (r *ConflictResolver) abortUnattended(info *ServiceInfo) *ResolutionResult {
	return &ResolutionResult{
		Strategy: StrategyAbort,
		Success:  false,
		Message:  r.unattendedAbortMessage(info),
		Error:    ErrPortConflictUnattended,
	}
}

func (r *ConflictResolver) unattendedAbortMessage(info *ServiceInfo) string {
	if info.APIConfirmed {
		return fmt.Sprintf("port %s is held by an SPT engine%s; it was left running. "+
			"Use --force to stop it and free the port, or --api-port with --rmi-registry-port to run alongside it",
			r.Port, engineRunDetails(info.Status))
	}
	if info.IsSpt {
		return fmt.Sprintf("port %s is held by a container that looks like SPT, but no SPT API answered there (%s); "+
			"it was left running. Use --force to remove it and free the port, "+
			"or --api-port with --rmi-registry-port to run alongside it",
			r.Port, info.GetServiceDescription())
	}
	return fmt.Sprintf("port %s is in use (%s); it was left running. "+
		"Free the port, or use --api-port with --rmi-registry-port to run alongside it",
		r.Port, info.GetServiceDescription())
}

// engineRunDetails formats the engine's run ID and state for a message.
func engineRunDetails(status *SptStatus) string {
	if status == nil {
		return ""
	}
	var details []string
	if status.RunID != "" {
		details = append(details, "run "+status.RunID)
	}
	if status.State != "" {
		details = append(details, "state "+status.State)
	}
	if len(details) == 0 {
		return ""
	}
	return " (" + strings.Join(details, ", ") + ")"
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
		fmt.Printf("  ID: %s\n", containerutil.ShortID(info.Container.ID))
	}
}

// getUserChoice presents options and gets the user's choice.
func (r *ConflictResolver) getUserChoice(_ context.Context, info *ServiceInfo) (ResolutionStrategy, error) {
	fmt.Printf("\nResolution Options:\n")

	if info.APIConfirmed {
		fmt.Printf("  1) [g] Graceful stop - Stop the run and shut the engine down via its API (recommended)\n")
		fmt.Printf("  2) [f] Force stop - Forcefully stop/remove containers\n")
	} else {
		fmt.Printf("  1) [f] Force stop - Forcefully stop the service/container\n")
	}

	fmt.Printf("  3) [r] Retry - Check port status again (if you stopped it manually)\n")
	fmt.Printf("  4) [a] Abort - Cancel spt operation and exit\n")

	fmt.Printf("\nWhat would you like to do? ")

	// Read user input
	var source io.Reader = os.Stdin
	if r.input != nil {
		source = r.input
	}
	reader := bufio.NewReader(source)
	for {
		input, err := reader.ReadString('\n')
		if errors.Is(err, io.EOF) {
			fmt.Println()
			return StrategyAbort, errNoOperatorAnswer
		}
		if err != nil {
			return StrategyAbort, fmt.Errorf("failed to read user input: %w", err)
		}

		choice := strings.ToLower(strings.TrimSpace(input))

		switch choice {
		case "1":
			if info.APIConfirmed {
				return StrategyGracefulStop, nil
			}
			// Without an SPT API to ask, option 1 is force stop
			return StrategyForceStop, nil

		case "g", "graceful":
			if info.APIConfirmed {
				return StrategyGracefulStop, nil
			}
			// Not offered here, and it must not turn into a force stop.
			fmt.Printf("Graceful stop needs an SPT API on the port. Please enter 1-4 or f/r/a: ")

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

	// When the SPT API answers, try graceful first, then force
	if info.APIConfirmed {
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

// executeGracefulStop stops the engine's active run, shuts the engine down and
// waits for the port to close. The whole sequence runs within
// PortConflictGracefulStopTimeout, or less if ctx ends first.
func (r *ConflictResolver) executeGracefulStop(ctx context.Context, info *ServiceInfo) *ResolutionResult {
	if !info.IsSpt {
		return &ResolutionResult{
			Strategy: StrategyGracefulStop,
			Success:  false,
			Message:  "Graceful stop only works for Spt services",
		}
	}
	if !info.APIConfirmed {
		return &ResolutionResult{
			Strategy: StrategyGracefulStop,
			Success:  false,
			Message:  fmt.Sprintf("Graceful stop needs an SPT API answering on port %s", r.Port),
		}
	}

	deadline := r.clock.Now().Add(constants.PortConflictGracefulStopTimeout)
	baseURL := "http://" + net.JoinHostPort(r.checker.Host, r.Port)
	client := NewSptAPIClientWithClock(baseURL, DefaultHTTPTimeout, r.clock)

	fmt.Printf("Stopping the run on the SPT engine at port %s...\n", r.Port)
	runID, err := r.stopEngineRun(ctx, client, info)
	if err != nil {
		return gracefulStopFailure("Failed to stop the SPT run", err)
	}
	if runID != 0 {
		fmt.Printf("Stopped SPT run %d\n", runID)
	} else {
		fmt.Printf("No SPT run is active\n")
	}

	// Shutdown ends whatever runs on the engine, so another run must not be active.
	if err := client.ConfirmNoOtherRun(ctx, runID); err != nil {
		if r.checker.IsPortHeld() {
			return gracefulStopFailure("Left the SPT engine running", err)
		}
		return r.waitForPortRelease(ctx, deadline)
	}

	fmt.Printf("Shutting down the SPT engine (waiting up to %s for port %s)...\n",
		constants.PortConflictGracefulStopTimeout, r.Port)
	if err := client.Shutdown(ctx); err != nil && r.checker.IsPortHeld() {
		return gracefulStopFailure("Failed to shut down the SPT engine", err)
	}
	return r.waitForPortRelease(ctx, deadline)
}

// stopEngineRun stops the engine's run. An operator's answer covers only the run
// shown at the prompt; --force covers whatever holds the port.
func (r *ConflictResolver) stopEngineRun(ctx context.Context, client *SptAPIClient, info *ServiceInfo) (int64, error) {
	if r.forceMode {
		return client.StopActiveRun(ctx)
	}
	return client.StopShownRun(ctx, shownRunID(info))
}

// shownRunID returns the run ID detection reported, or 0 if it reported none.
func shownRunID(info *ServiceInfo) int64 {
	if info.Status == nil {
		return 0
	}
	runID, err := strconv.ParseInt(info.Status.RunID, 10, 64)
	if err != nil {
		return 0
	}
	return runID
}

func gracefulStopFailure(what string, err error) *ResolutionResult {
	return &ResolutionResult{
		Strategy: StrategyGracefulStop,
		Success:  false,
		Message:  fmt.Sprintf("%s: %s", what, err.Error()),
		Error:    err,
	}
}

// waitForPortRelease polls until the port closes, the deadline passes or ctx ends.
func (r *ConflictResolver) waitForPortRelease(ctx context.Context, deadline time.Time) *ResolutionResult {
	for r.checker.IsPortHeld() {
		if !r.clock.Now().Before(deadline) {
			message := fmt.Sprintf("Port %s is still in use %s after the SPT engine was asked to stop; "+
				"its api.linger.sec may be longer", r.Port, constants.PortConflictGracefulStopTimeout)
			if !r.forceMode {
				message += ". Retry later, or use --force to remove its container"
			}
			return &ResolutionResult{Strategy: StrategyGracefulStop, Success: false, Message: message}
		}
		select {
		case <-ctx.Done():
			return gracefulStopFailure(
				fmt.Sprintf("Stopped waiting for port %s to close", r.Port), ctx.Err())
		case <-r.clock.After(constants.PortConflictReleasePollInterval):
		}
	}

	fmt.Printf("✅ SPT engine stopped - port %s is now available\n", r.Port)
	return &ResolutionResult{
		Strategy: StrategyGracefulStop,
		Success:  true,
		Message:  fmt.Sprintf("SPT engine stopped gracefully, port %s is available", r.Port),
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

	ctx, cancel := context.WithTimeout(ctx, constants.PortConflictForceStopTimeout)
	defer cancel()

	target, result := r.findForceStopTarget(ctx, info)
	if result != nil {
		return result
	}

	fmt.Printf("Force stopping container: %s (%s)\n", target.Name, containerutil.ShortID(target.ID))
	if err := r.dockerFinder.ForceStopAndRemoveContainer(ctx, target.ID); err != nil {
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

// findForceStopTarget returns the one container that holds the port: the one
// detection found, the one publishing the port, or, when an SPT engine answers
// on the port, its host-network container. Otherwise it returns the failure.
func (r *ConflictResolver) findForceStopTarget(ctx context.Context, info *ServiceInfo) (*ContainerInfo, *ResolutionResult) {
	if info.Container != nil {
		return info.Container, nil
	}

	lookupFailure := func(err error) *ResolutionResult {
		return &ResolutionResult{
			Strategy: StrategyForceStop,
			Success:  false,
			Message:  fmt.Sprintf("Failed to find containers using port: %s", err.Error()),
			Error:    err,
		}
	}
	container, err := r.dockerFinder.FindContainerUsingPort(ctx, r.Port)
	if err != nil {
		return nil, lookupFailure(err)
	}
	if container == nil && info.APIConfirmed {
		if !r.dockerFinder.DaemonIsLocal() {
			return nil, &ResolutionResult{
				Strategy: StrategyForceStop,
				Success:  false,
				Message: fmt.Sprintf("No containers found using port %s - the Docker daemon is not local, "+
					"so its host-network containers cannot be matched to this port", r.Port),
			}
		}
		container, err = r.dockerFinder.FindHostNetworkEngine(ctx, r.Port)
		if err != nil {
			return nil, lookupFailure(err)
		}
	}
	if container == nil {
		return nil, &ResolutionResult{
			Strategy: StrategyForceStop,
			Success:  false,
			Message:  fmt.Sprintf("No containers found using port %s - service may not be containerized", r.Port),
		}
	}
	return container, nil
}

// executeRetry re-checks port status
func (r *ConflictResolver) executeRetry(ctx context.Context) *ResolutionResult {
	fmt.Printf("Rechecking port %s status...\n", r.Port)

	newInfo, err := r.checkService(ctx)
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
	newInfo, err := r.checkService(ctx)
	if err != nil {
		return &ResolutionResult{
			Strategy: strategy,
			Success:  false,
			Message:  fmt.Sprintf("Failed to verify resolution: %s", err.Error()),
			Error:    err,
		}
	}

	if !newInfo.IsListening && !r.checker.IsPortHeld() {
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
