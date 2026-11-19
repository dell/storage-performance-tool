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
	if ctx.Err() != nil {
		// A probe that ctx ended shows nothing about the port.
		result := r.interrupted(ctx, StrategyRetry, "checking port "+r.Port)
		return result, ctx.Err()
	}
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
		holder := "a container"
		if info.Container != nil {
			holder = fmt.Sprintf("container %s (%s)", info.Container.Name, info.Container.Image)
		}
		return fmt.Sprintf("port %s is held by %s, which looks like SPT but no SPT API answered there; "+
			"it was left running. Use --force to remove it and free the port, "+
			"or --api-port with --rmi-registry-port to run alongside it",
			r.Port, holder)
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

	if info.APIConfirmed && info.Status != nil {
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
		if note := runIdentityNote(info.Status); note != "" {
			fmt.Printf("  Note: %s\n", note)
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

// runIdentityNote says when a later run that reuses the active run's ID could not
// be told apart from it: the engine reports a start time only for a run it started.
func runIdentityNote(status *SptStatus) string {
	if status == nil || !status.RunActive || status.RunningSince != "" {
		return ""
	}
	return fmt.Sprintf("this engine reports no start time for run %[1]s, so g cannot tell it apart "+
		"from a later run that reuses ID %[1]s", status.RunID)
}

// resolutionOptions is the prompt's menu. g is offered only when the SPT API answered.
func resolutionOptions(info *ServiceInfo) string {
	var menu strings.Builder
	menu.WriteString("\nResolution Options:\n")
	if info.APIConfirmed {
		menu.WriteString("  1) [g] Graceful stop - Stop the run and shut the engine down via its API (recommended)\n")
		menu.WriteString("  2) [f] Force stop - Forcefully stop/remove containers\n")
	} else {
		menu.WriteString("  1) [f] Force stop - Forcefully stop the service/container\n")
	}
	menu.WriteString("  3) [r] Retry - Check port status again (if you stopped it manually)\n")
	menu.WriteString("  4) [a] Abort - Cancel spt operation and exit\n")
	menu.WriteString("\nWhat would you like to do? ")
	return menu.String()
}

// getUserChoice presents options and gets the user's choice.
func (r *ConflictResolver) getUserChoice(ctx context.Context, info *ServiceInfo) (ResolutionStrategy, error) {
	fmt.Print(resolutionOptions(info))

	// Read user input
	var source io.Reader = os.Stdin
	if r.input != nil {
		source = r.input
	}
	reader := bufio.NewReader(source)
	for {
		input, err := readAnswer(ctx, reader)
		if errors.Is(err, io.EOF) {
			fmt.Println()
			return StrategyAbort, errNoOperatorAnswer
		}
		if ctx.Err() != nil {
			fmt.Println()
			return StrategyAbort, ctx.Err()
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

// readAnswer reads one line, or returns when ctx ends. A read of a terminal cannot
// be interrupted, so on cancellation it stays blocked until the process exits.
func readAnswer(ctx context.Context, reader *bufio.Reader) (string, error) {
	type answer struct {
		text string
		err  error
	}
	answers := make(chan answer, 1)
	go func() {
		text, err := reader.ReadString('\n')
		answers <- answer{text, err}
	}()
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case got := <-answers:
		return got.text, got.err
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
		// After Ctrl-C, nothing further may be stopped or removed.
		if result.Success || ctx.Err() != nil {
			return result, nil
		}
		// A port that nothing accepts connections on names no holder to remove.
		if !r.checker.isListening(ctx) && ctx.Err() == nil {
			return r.nothingListensResult(result), nil
		}

		fmt.Printf("[FORCE] Graceful stop failed, trying force stop: %s\n", result.Message)
	}
	if ctx.Err() != nil {
		return r.interrupted(ctx, StrategyForceStop, "resolving the conflict on port "+r.Port), nil
	}

	// Force stop containers or services
	fmt.Printf("[FORCE] Force stopping service on port %s\n", r.Port)
	return r.executeForceStop(ctx, info), nil
}

// nothingListensResult is the end of --force when the graceful stop failed and
// nothing accepts connections on the port any more.
func (r *ConflictResolver) nothingListensResult(graceful *ResolutionResult) *ResolutionResult {
	message := graceful.Message + ". No container was removed; retry when the port can be bound"
	if !errors.Is(graceful.Error, errPortNotBindable) {
		message = fmt.Sprintf("%s. Nothing accepts connections on port %s now, so the SPT engine may have "+
			"exited by itself; no container was removed. Run the command again", graceful.Message, r.Port)
	}
	return &ResolutionResult{Strategy: StrategyForceStop, Success: false, Message: message, Error: graceful.Error}
}

// errPortNotBindable marks a wait that ended with nothing accepting connections
// on the port, which could still not be bound.
var errPortNotBindable = errors.New("port cannot be bound yet")

// interrupted is the result of work that ctx ended: Ctrl-C or SIGTERM, or a
// time limit of the CLI's own.
func (r *ConflictResolver) interrupted(ctx context.Context, strategy ResolutionStrategy, doing string) *ResolutionResult {
	message := fmt.Sprintf("Interrupted while %s: %s", doing, ctx.Err())
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		message = fmt.Sprintf("Gave up %s: the time limit for it ran out", doing)
	}
	return &ResolutionResult{Strategy: strategy, Success: false, Message: message, Error: ctx.Err()}
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
	stopped, err := r.stopEngineRun(ctx, client, info)
	if err != nil {
		return gracefulStopFailure("Failed to stop the SPT run", err)
	}
	fmt.Println(stopMessage(stopped))

	// Shutdown ends whatever runs on the engine, so another run must not be active.
	if err := client.ConfirmNoOtherRun(ctx, stopped); err != nil {
		if r.checker.IsPortHeld(ctx) {
			what := "Did not shut down the SPT engine"
			if !r.forceMode {
				what += ", which was left running"
			}
			return gracefulStopFailure(what, err)
		}
		return r.waitForPortRelease(ctx, deadline)
	}

	fmt.Printf("Shutting down the SPT engine (waiting up to %s for port %s)...\n",
		constants.PortConflictGracefulStopTimeout, r.Port)
	if err := client.Shutdown(ctx); err != nil && r.checker.IsPortHeld(ctx) {
		return gracefulStopFailure("Failed to shut down the SPT engine", err)
	}
	return r.waitForPortRelease(ctx, deadline)
}

// stopMessage says what the stop did. Only an engine whose /status shows it
// running or ending the run itself ends the run on a stop. A worker answers the
// stop and the run goes on from its entry node; an entry engine whose status a
// refused POST /run overwrote looks the same, so the message holds for both.
func stopMessage(stopped RunExecution) string {
	switch {
	case stopped.ID == 0:
		return "No SPT run is active"
	case stopped.confirmedEntry():
		return fmt.Sprintf("Stopped SPT run %d", stopped.ID)
	case stopped.Worker:
		return fmt.Sprintf("Asked the engine to stop run %d; its status does not name that run, so this node "+
			"may be a worker of it, and then its entry node goes on running it", stopped.ID)
	default:
		return fmt.Sprintf("Asked the engine to stop run %d; its status could not be read, so the CLI "+
			"cannot confirm that the run stopped", stopped.ID)
	}
}

// stopEngineRun stops the engine's run. An operator's answer covers only the run
// shown at the prompt; --force covers whatever holds the port.
func (r *ConflictResolver) stopEngineRun(ctx context.Context, client *SptAPIClient, info *ServiceInfo) (RunExecution, error) {
	if r.forceMode {
		return client.StopActiveRun(ctx, shownExecution(info))
	}
	return client.StopShownRun(ctx, shownExecution(info))
}

// shownExecution returns the run detection reported, with a zero ID if none.
func shownExecution(info *ServiceInfo) RunExecution {
	if info.Status == nil {
		return RunExecution{}
	}
	runID, err := strconv.ParseInt(info.Status.RunID, 10, 64)
	if err != nil {
		return RunExecution{}
	}
	return RunExecution{ID: runID, Since: info.Status.RunningSince, Worker: info.Status.Worker}
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
	for {
		held := r.checker.IsPortHeld(ctx)
		// A probe that ctx ended shows nothing, free or held.
		if ctx.Err() != nil {
			return gracefulStopFailure(
				fmt.Sprintf("Stopped waiting for port %s to close", r.Port), ctx.Err())
		}
		if !held {
			break
		}
		if !r.clock.Now().Before(deadline) {
			reason, err := "its api.linger.sec may be longer", error(nil)
			if !r.checker.isListening(ctx) && ctx.Err() == nil {
				reason, err = "nothing accepts connections on it, but it cannot be bound yet", errPortNotBindable
			}
			message := fmt.Sprintf("Port %s is still in use %s after the SPT engine was asked to stop; %s",
				r.Port, constants.PortConflictGracefulStopTimeout, reason)
			if !r.forceMode {
				message += ". Retry later, or use --force to remove its container"
			}
			return &ResolutionResult{Strategy: StrategyGracefulStop, Success: false, Message: message, Error: err}
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
	if ctx.Err() != nil {
		return r.interrupted(ctx, StrategyForceStop, "resolving the conflict on port "+r.Port)
	}

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
	select {
	case <-ctx.Done():
		result := r.interrupted(ctx, StrategyForceStop, "waiting for port "+r.Port+" to close")
		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			result.Message = fmt.Sprintf("Stopped waiting for port %s to close: %s", r.Port, ctx.Err())
		}
		return result
	case <-r.clock.After(3 * time.Second):
	}
	return r.verifyResolution(ctx, StrategyForceStop)
}

// findForceStopTarget returns the one container shown to hold the port, or the
// refusal. Only a daemon reached through a local socket can hold this host's
// ports. The holder must publish the port on a binding that covers the address
// that accepted the CLI's connection, and be the only container that does; when
// an SPT engine answers there, it must also be an SPT engine node container. A
// container on host networking publishes nothing, so nothing shows that it, and
// not another process, holds the port: it is named, and left running.
func (r *ConflictResolver) findForceStopTarget(ctx context.Context, info *ServiceInfo) (*ContainerInfo, *ResolutionResult) {
	refusal := func(format string, args ...any) *ResolutionResult {
		return &ResolutionResult{Strategy: StrategyForceStop, Success: false, Message: fmt.Sprintf(format, args...)}
	}
	if !r.dockerFinder.DaemonIsLocal() {
		return nil, refusal("No container was matched to port %s - the Docker daemon is not reached "+
			"through a local socket, so it may not be local; nothing was removed", r.Port)
	}
	address, ok := r.checker.answeringAddress(ctx)
	if ctx.Err() != nil {
		return nil, r.interrupted(ctx, StrategyForceStop, "finding the container on port "+r.Port)
	}
	if !ok {
		return nil, refusal("Nothing accepts connections on port %s now, so no container can be shown to "+
			"hold it; nothing was removed", r.Port)
	}
	endpoint := net.JoinHostPort(address.String(), r.Port)

	lookupFailure := func(err error) *ResolutionResult {
		return &ResolutionResult{
			Strategy: StrategyForceStop,
			Success:  false,
			Message:  fmt.Sprintf("Failed to find containers using port: %s", err.Error()),
			Error:    err,
		}
	}
	publishers, err := r.dockerFinder.FindPublishers(ctx, address, r.Port)
	if err != nil {
		return nil, lookupFailure(err)
	}
	switch {
	case len(publishers) > 1:
		names := make([]string, 0, len(publishers))
		for _, c := range publishers {
			names = append(names, c.Name)
		}
		return nil, refusal("Containers %s all publish %s, so none can be shown to hold it; nothing was "+
			"removed. Remove the right one by name", strings.Join(names, ", "), endpoint)
	case len(publishers) == 1 && info.APIConfirmed && !isEngineNodeContainer(publishers[0].Labels):
		return nil, refusal("Container %s publishes %s but is not an SPT engine container, so it cannot be "+
			"matched to the SPT engine answering there; it was left running", publishers[0].Name, endpoint)
	case len(publishers) == 1:
		return publishers[0], nil
	}

	if info.APIConfirmed {
		candidate, err := r.dockerFinder.FindHostNetworkEngine(ctx, r.Port)
		if err != nil {
			return nil, lookupFailure(err)
		}
		if candidate != nil {
			return nil, refusal("No container publishes %s. Container %s runs on host networking with run port %s, "+
				"but nothing shows that it holds the port, so it was left running; if it is the SPT engine, "+
				"remove it by name (docker rm -f %s)", endpoint, candidate.Name, r.Port, candidate.Name)
		}
	}
	return nil, refusal("No container publishes %s - the service may not be containerized; nothing was removed", endpoint)
}

// executeRetry re-checks port status
func (r *ConflictResolver) executeRetry(ctx context.Context) *ResolutionResult {
	fmt.Printf("Rechecking port %s status...\n", r.Port)

	newInfo, err := r.checkService(ctx)
	if ctx.Err() != nil {
		return r.interrupted(ctx, StrategyRetry, "rechecking port "+r.Port)
	}
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
	if err == nil && !newInfo.IsListening && !r.checker.IsPortHeld(ctx) && ctx.Err() == nil {
		fmt.Printf("✅ Resolution successful - port %s is now available\n", r.Port)
		return &ResolutionResult{
			Strategy: strategy,
			Success:  true,
			Message:  fmt.Sprintf("Port %s is now available", r.Port),
		}
	}
	if ctx.Err() != nil {
		return r.interrupted(ctx, strategy, "checking port "+r.Port)
	}
	if err != nil {
		return &ResolutionResult{
			Strategy: strategy,
			Success:  false,
			Message:  fmt.Sprintf("Failed to verify resolution: %s", err.Error()),
			Error:    err,
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
