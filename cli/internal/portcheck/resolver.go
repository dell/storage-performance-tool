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
	"net/http"
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
	if ctx.Err() != nil {
		// Ctrl-C at the prompt: nothing was chosen, and nothing was changed.
		return r.interrupted(ctx, StrategyRetry, "waiting for an answer about port "+r.Port), ctx.Err()
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
	info, err := r.checker.CheckService(ctx)
	if err != nil || !info.IsListening || !info.APIConfirmed || r.dockerFinder == nil {
		return info, err
	}
	// The checker looks for a container only when no SPT API answered. A force
	// stop of an engine may remove only the container shown, and an API request
	// that changes it is sent only while that container still publishes the
	// address, so look for it here. A daemon that does not answer in time shows
	// no container; it does not hold up detection.
	listCtx, cancelList := context.WithTimeout(ctx, constants.PortConflictContainerListTimeout)
	defer cancelList()
	if err := identifyContainer(listCtx, r.dockerFinder, r.Port, info); err != nil {
		logging.LogDebug("portcheck", "failed to find the SPT engine's container", "port", r.Port, "error", err.Error())
	}
	return info, nil
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
		removal := "remove it yourself"
		if info.Container != nil {
			removal = fmt.Sprintf("remove it by name (docker rm -f %s)", info.Container.Name)
		}
		return fmt.Sprintf("port %s is held by %s, which looks like SPT but no SPT API answered there; "+
			"it was left running. If it should go, %s; --force removes it when nothing answers on the port. "+
			"Or use --api-port with --rmi-registry-port to run alongside it",
			r.Port, holder, removal)
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

	if len(info.Publishers) > 1 {
		fmt.Printf("\nContainers %s all publish the port on the address that answered, so none is shown as its holder\n",
			strings.Join(info.Publishers, ", "))
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
		// Removing the container would end the run the CLI just refused to touch.
		if IsRunIdentityRefusal(result.Error) {
			return r.identityRefusalResult(result, info), nil
		}
		// The engine answering may not be the one shown: nothing is removed.
		if errors.Is(result.Error, errHolderChanged) {
			result.Strategy = StrategyForceStop
			return result, nil
		}
		// A port that nothing accepts connections on names no holder to remove.
		listening := r.checker.isListening(ctx)
		if ctx.Err() != nil {
			return r.interrupted(ctx, StrategyForceStop, "checking port "+r.Port+" after the graceful stop"), nil
		}
		if !listening {
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

// identityRefusalResult ends --force when the graceful step refused because the
// engine's run could not be identified: nothing is removed.
func (r *ConflictResolver) identityRefusalResult(graceful *ResolutionResult, info *ServiceInfo) *ResolutionResult {
	advice := "stop the run yourself, then run the command again"
	if info.Container != nil {
		advice = fmt.Sprintf("stop the run yourself, or remove the engine's container by name (docker rm -f %s), "+
			"then run the command again", info.Container.Name)
	}
	return &ResolutionResult{
		Strategy: StrategyForceStop,
		Success:  false,
		Message:  fmt.Sprintf("%s. No container was removed: %s", graceful.Message, advice),
		Error:    graceful.Error,
	}
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
	client := r.apiClient(info)

	// Each request that changes the engine follows a fresh check: the container
	// shown still publishes the address, and then the run, read from the same
	// address, is the one identified. A run that starts after the last read and
	// before the request is not seen: the engine's API offers no request that
	// is conditional on the run for the shutdown.
	if refusal := r.shownContainerRefusal(ctx, info, "stop its run"); refusal != nil {
		return refusal
	}
	fmt.Printf("Stopping the run on the SPT engine at port %s...\n", r.Port)
	stopped, err := r.stopEngineRun(ctx, client, info)
	if ctx.Err() != nil {
		return r.interrupted(ctx, StrategyGracefulStop, "stopping the run on the SPT engine at port "+r.Port)
	}
	if err != nil {
		return gracefulStopFailure("Failed to stop the SPT run", err)
	}

	if refusal := r.shownContainerRefusal(ctx, info, "shut it down"); refusal != nil {
		fmt.Println(stopMessage(stopped, false))
		// The engine may have closed after the stop, as its own CLI closes it at
		// the run's end. A port that can be bound now is free, and no container
		// is in doubt; one still held keeps the refusal.
		if errors.Is(refusal.Error, errNoPublisher) {
			held := r.checker.IsPortHeld(ctx)
			if ctx.Err() != nil {
				return r.interrupted(ctx, StrategyGracefulStop, "checking port "+r.Port+" after the stop")
			}
			if !held {
				return r.waitForPortRelease(ctx, deadline)
			}
		}
		return refusal
	}
	// Shutdown ends whatever runs on the engine, so another run must not be active.
	ended, err := client.ConfirmNoOtherRun(ctx, stopped)
	if ctx.Err() != nil {
		return r.interrupted(ctx, StrategyGracefulStop, "checking the SPT engine's run before its shutdown")
	}
	fmt.Println(stopMessage(stopped, ended))
	if err != nil {
		held := r.checker.IsPortHeld(ctx)
		if ctx.Err() != nil {
			return r.interrupted(ctx, StrategyGracefulStop, "checking port "+r.Port+" before the shutdown")
		}
		if held {
			return gracefulStopFailure("Did not shut down the SPT engine, which was left running", err)
		}
		return r.waitForPortRelease(ctx, deadline)
	}

	fmt.Printf("Shutting down the SPT engine (waiting up to %s for port %s)...\n",
		constants.PortConflictGracefulStopTimeout, r.Port)
	err = client.Shutdown(ctx)
	if ctx.Err() != nil {
		return r.interrupted(ctx, StrategyGracefulStop, "shutting down the SPT engine at port "+r.Port)
	}
	if err != nil {
		held := r.checker.IsPortHeld(ctx)
		if ctx.Err() != nil {
			return r.interrupted(ctx, StrategyGracefulStop, "checking port "+r.Port+" after the shutdown request")
		}
		if held {
			return gracefulStopFailure("The request to shut down the SPT engine failed", err)
		}
	}
	return r.waitForPortRelease(ctx, deadline)
}

// apiClient is a client of the SPT API at the address that answered detection,
// the address every later request goes to; it follows no redirect. Each request
// opens its own connection, so a check reads what listens there now, not a
// connection kept from before, and goes through no proxy. Each connection
// counts the bytes it delivers, so that only a request that got not one byte
// back counts as unanswered.
func (r *ConflictResolver) apiClient(info *ServiceInfo) *SptAPIClient {
	client := NewSptAPIClientWithClock(r.checker.apiBaseURL(info.Address), DefaultHTTPTimeout, r.clock)
	client.HTTPClient.Transport = &http.Transport{
		Proxy:             nil,
		DisableKeepAlives: true,
		DialContext:       countingDial((&net.Dialer{Timeout: r.checker.ConnectTimeout}).DialContext),
	}
	return client
}

// errHolderChanged marks a refusal because the port's holder may not be the one
// shown: nothing was sent to the engine or removed.
var errHolderChanged = errors.New("the port's holder changed since it was shown")

// errNoPublisher is the errHolderChanged of a check that found no container
// publishing the address.
var errNoPublisher = fmt.Errorf("%w: no container publishes the address", errHolderChanged)

// shownContainerRefusal is the container half of the check before an API request
// that changes the engine. When detection showed a container, the one container
// that publishes the address must still be that one, by its full ID, or the
// engine answering may be another, and nothing is sent. With no container shown
// there is nothing to compare, and the run check alone decides.
func (r *ConflictResolver) shownContainerRefusal(ctx context.Context, info *ServiceInfo, action string) *ResolutionResult {
	if info.Container == nil {
		return nil
	}
	endpoint := net.JoinHostPort(ipOrUnknown(info.Address), r.Port)
	refuseWith := func(now string, err error) *ResolutionResult {
		return &ResolutionResult{
			Strategy: StrategyGracefulStop,
			Success:  false,
			Message: fmt.Sprintf("The container on port %s changed since it was shown (shown: %s; now: %s), so the "+
				"SPT engine answering there may be another one. The CLI did not ask it to %s, and removed nothing. "+
				"Run the command again", r.Port, info.Container.Name, now, action),
			Error: err,
		}
	}
	refuse := func(now string) *ResolutionResult { return refuseWith(now, errHolderChanged) }
	if r.dockerFinder == nil || !r.dockerFinder.DaemonIsLocal() {
		return refuse("the containers could not be listed")
	}
	listCtx, cancel := context.WithTimeout(ctx, constants.PortConflictContainerListTimeout)
	defer cancel()
	publishers, err := r.dockerFinder.FindPublishers(listCtx, info.Address, r.Port)
	if ctx.Err() != nil {
		return r.interrupted(ctx, StrategyGracefulStop, "checking the container on port "+r.Port)
	}
	switch {
	case err != nil:
		return refuse("the containers could not be listed: " + err.Error())
	case len(publishers) == 0:
		return refuseWith("no container publishes "+endpoint, errNoPublisher)
	case len(publishers) > 1:
		names := make([]string, 0, len(publishers))
		for _, c := range publishers {
			names = append(names, c.Name)
		}
		return refuse(strings.Join(names, ", ") + " all publish " + endpoint)
	case publishers[0].ID != info.Container.ID:
		return refuse(publishers[0].Name)
	}
	return nil
}

// stopMessage says what the stop did. ended is what the engine reported after
// the stop: the run no longer active, or /status naming it in a terminal state.
// A worker answers the stop and the run goes on from its entry node; an entry
// engine whose status a refused POST /run overwrote looks the same, so the
// worker message holds for both.
func stopMessage(stopped RunExecution, ended bool) string {
	switch {
	case stopped.ID == 0:
		return "No SPT run is active"
	case stopped.Worker && stopped.Known:
		return fmt.Sprintf("Asked the engine to stop run %d; its status does not name that run, so this node "+
			"may be a worker of it, and then its entry node goes on running it", stopped.ID)
	case stopped.Worker:
		return fmt.Sprintf("Asked the engine to stop run %d; its status could not be read, and when it was shown "+
			"it did not name that run, so this node may be a worker of it, and then its entry node goes on running it",
			stopped.ID)
	case ended:
		return fmt.Sprintf("Stopped SPT run %d", stopped.ID)
	case !stopped.Known:
		return fmt.Sprintf("Asked the engine to stop run %d; its status could not be read, so the CLI "+
			"cannot confirm that the run stopped", stopped.ID)
	default:
		return fmt.Sprintf("Asked the engine to stop run %d; its status does not show that the run ended, so the "+
			"CLI cannot confirm that it stopped", stopped.ID)
	}
}

// stopEngineRun stops the run detection showed active, and only that run. g
// needs it to be read again in full; --force also stops it by its ID alone when
// detection itself had no start time for it (see StopActiveRun).
func (r *ConflictResolver) stopEngineRun(ctx context.Context, client *SptAPIClient, info *ServiceInfo) (RunExecution, error) {
	if r.forceMode {
		return client.StopActiveRun(ctx, shownExecution(info))
	}
	return client.StopShownRun(ctx, shownExecution(info))
}

// shownExecution returns the run detection saw active, through HEAD /run, with
// a zero ID if none. A run ID that /status gave for an engine with no active
// run is a run that had ended, shown for information only: a run with that ID
// active later is another execution.
func shownExecution(info *ServiceInfo) RunExecution {
	if info.Status == nil || !info.Status.RunActive {
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
			listening := r.checker.isListening(ctx)
			if ctx.Err() != nil {
				return gracefulStopFailure(
					fmt.Sprintf("Stopped waiting for port %s to close", r.Port), ctx.Err())
			}
			if !listening {
				reason, err = "nothing accepts connections on it, but it cannot be bound yet", errPortNotBindable
			}
			message := fmt.Sprintf("Port %s is still in use %s after the SPT engine was asked to stop; %s",
				r.Port, constants.PortConflictGracefulStopTimeout, reason)
			if !r.forceMode {
				message += ". Retry later. --force removes the engine's container only if it publishes the port; " +
					"an engine on host networking is not removed"
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
	if result := r.removalRefusal(ctx, info, target); result != nil {
		return result
	}

	fmt.Printf("Force stopping container: %s (%s)\n", target.Name, containerutil.ShortID(target.ID))
	if err := r.dockerFinder.StopContainer(ctx, target.ID); err != nil {
		if ctx.Err() != nil {
			return r.interrupted(ctx, StrategyForceStop, "stopping container "+target.Name)
		}
		logging.LogDebug("portcheck", "the container's stop failed; checking again before its removal",
			"container", target.Name, "error", err.Error())
	}
	// The removal is a write of its own. The stop can fail, or the container can
	// restart after it and take a run, so the check is made again.
	if result := r.checkAgainAfterTheStop(ctx, info, target); result != nil {
		return result
	}
	if err := r.dockerFinder.RemoveContainer(ctx, target.ID, true); err != nil {
		if ctx.Err() != nil {
			return r.interrupted(ctx, StrategyForceStop, "removing container "+target.Name)
		}
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

// findForceStopTarget returns the container detection showed as the holder, if
// it still is, or the refusal. Only a daemon reached through a local socket can
// hold this host's ports. The address that accepts the CLI's connection must be
// the one that answered detection. The holder must publish the port on a binding
// that covers that address, be the only container that does, and be the
// container shown; when an SPT engine answers there, it must also be an SPT
// engine node container. A container on host networking publishes nothing, so
// nothing shows that it, and not another process, holds the port: it is named,
// and left running.
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
	if info.Address == nil || !address.Equal(info.Address) {
		return nil, refusal("The address answering port %s changed since it was shown (%s, now %s); nothing "+
			"was removed. Run the command again", r.Port, ipOrUnknown(info.Address), address)
	}

	lookupFailure := func(err error) *ResolutionResult {
		if ctx.Err() != nil {
			return r.interrupted(ctx, StrategyForceStop, "finding the container on port "+r.Port)
		}
		return &ResolutionResult{
			Strategy: StrategyForceStop,
			Success:  false,
			Message:  fmt.Sprintf("Failed to find containers using port: %s", err.Error()),
			Error:    err,
		}
	}
	changed := func(now string) *ResolutionResult {
		if info.Container == nil && info.ListingFailed {
			return refusal("The containers could not be listed when port %s was shown, so none was shown, and now %s publishes it; "+
				"nothing was removed. Run the command again", r.Port, now)
		}
		return refusal("The container on port %s changed since it was shown (shown: %s; now: %s); nothing was "+
			"removed. Run the command again", r.Port, containerOrNone(info.Container), now)
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
	case len(publishers) == 1 && (info.Container == nil || publishers[0].ID != info.Container.ID):
		return nil, changed(publishers[0].Name)
	case len(publishers) == 1:
		return publishers[0], nil
	case info.Container != nil:
		return nil, changed("no container publishes " + endpoint)
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
	others, err := r.dockerFinder.FindPortNumberPublishers(ctx, r.Port)
	if err != nil {
		return nil, lookupFailure(err)
	}
	if len(others) > 0 {
		names := make([]string, 0, len(others))
		for _, c := range others {
			names = append(names, c.Name)
		}
		return nil, refusal("No container publishes %s, so nothing was removed. Port %s is published by %s, but on "+
			"another address, or with no address given (as Podman's default publish gives), so nothing shows that it "+
			"holds %s; if it does, remove it by name (docker rm -f <name>)",
			endpoint, r.Port, strings.Join(names, ", "), endpoint)
	}
	return nil, refusal("No container publishes %s - the service may not be containerized; nothing was removed", endpoint)
}

// removalRefusal is the check right before the container is stopped (see
// CheckBeforeRemoval), on the address that answered detection: it returns nil to
// go on, or the refusal. No answer at all is the fallback for an engine whose
// API is unavailable, or for a service with none; an idle engine and the run
// shown may be removed; anything else may end a run nobody chose, or remove a
// service that is not an engine, so the container is left running. A run that
// starts after this check and before the stop is not seen.
func (r *ConflictResolver) removalRefusal(ctx context.Context, info *ServiceInfo, target *ContainerInfo) *ResolutionResult {
	return r.checkRemoval(ctx, info, target, "Did not stop or remove container %s", "It was left running")
}

// checkAgainAfterTheStop is the check right before the removal, once the stop
// has returned or failed: the address must have no publisher but the container
// stopped, and the engine's API must pass the same check as before the stop.
func (r *ConflictResolver) checkAgainAfterTheStop(ctx context.Context, info *ServiceInfo, target *ContainerInfo) *ResolutionResult {
	const did, left = "Asked Docker to stop container %s, and then did not remove it", "It was not removed"
	endpoint := net.JoinHostPort(ipOrUnknown(info.Address), r.Port)
	listCtx, cancel := context.WithTimeout(ctx, constants.PortConflictContainerListTimeout)
	defer cancel()
	publishers, err := r.dockerFinder.FindPublishers(listCtx, info.Address, r.Port)
	if ctx.Err() != nil {
		return r.interrupted(ctx, StrategyForceStop, "checking port "+r.Port+" before removing container "+target.Name)
	}
	refuse := func(reason string) *ResolutionResult {
		return &ResolutionResult{
			Strategy: StrategyForceStop,
			Success:  false,
			Message:  fmt.Sprintf(did+": %s. %s; run the command again", target.Name, reason, left),
			Error:    errHolderChanged,
		}
	}
	switch {
	case err != nil:
		return refuse("the containers could not be listed again (" + err.Error() + ")")
	case len(publishers) > 1:
		names := make([]string, 0, len(publishers))
		for _, c := range publishers {
			names = append(names, c.Name)
		}
		return refuse(strings.Join(names, ", ") + " all publish " + endpoint + " now")
	case len(publishers) == 1 && publishers[0].ID != target.ID:
		return refuse(publishers[0].Name + " publishes " + endpoint + " now")
	}
	return r.checkRemoval(ctx, info, target, did, left)
}

// checkRemoval runs CheckBeforeRemoval and words its refusal: did says what was
// done to the container, with %s for its name, and left what became of it.
func (r *ConflictResolver) checkRemoval(ctx context.Context, info *ServiceInfo, target *ContainerInfo, did, left string) *ResolutionResult {
	check, err := r.apiClient(info).CheckBeforeRemoval(ctx, shownExecution(info))
	if ctx.Err() != nil {
		return r.interrupted(ctx, StrategyForceStop, "checking the run on port "+r.Port+" before removing a container")
	}
	if err != nil {
		return &ResolutionResult{
			Strategy: StrategyForceStop,
			Success:  false,
			Message:  fmt.Sprintf(did+": %s", target.Name, r.removalRefusalReason(info, target, err, left)),
			Error:    err,
		}
	}
	switch check {
	case RemovalNoAnswer:
		logging.LogInfo("portcheck", "nothing answers on the port; removing its container", "port", r.Port, "container", target.Name)
	case RemovalIdle:
		logging.LogInfo("portcheck", "HEAD /run reports no active run; removing the container", "port", r.Port, "container", target.Name)
	case RemovalIdentified:
		logging.LogInfo("portcheck", "the SPT engine runs the run shown; removing its container", "port", r.Port, "container", target.Name)
	}
	return nil
}

// removalRefusalReason says why the check before a removal refused, and what
// to do. An answer that names no run is the SPT engine's only when detection
// confirmed one and the answer is HTTP; otherwise the service may not be an
// engine at all.
func (r *ConflictResolver) removalRefusalReason(info *ServiceInfo, target *ContainerInfo, err error, left string) string {
	var unreadable *unreadableAnswerError
	switch {
	case !errors.As(err, &unreadable):
		return fmt.Sprintf("%s. %s; stop the run yourself, then run the command again", err.Error(), left)
	case info.APIConfirmed && unreadable.http:
		return fmt.Sprintf("the SPT engine on port %s answered, but its active run could not be read (%v), so a "+
			"run may be active. %s; stop the run yourself, or remove the container by name (docker rm -f %s), "+
			"then run the command again", r.Port, unreadable.err, left, target.Name)
	case unreadable.http:
		return fmt.Sprintf("the service on port %s answers HTTP, but not as an SPT engine (%v), so the CLI can't "+
			"tell whether it is safe to remove. %s; if it should go, remove it by name (docker rm -f %s), or use "+
			"--api-port with --rmi-registry-port to run alongside it", r.Port, unreadable.err, left, target.Name)
	default:
		return fmt.Sprintf("the service on port %s answered, but not with an HTTP answer the CLI can read (%v), "+
			"so the CLI can't tell whether it is safe to remove. %s; if it should go, remove it by name "+
			"(docker rm -f %s), or use --api-port with --rmi-registry-port to run alongside it",
			r.Port, unreadable.err, left, target.Name)
	}
}

func ipOrUnknown(ip net.IP) string {
	if ip == nil {
		return "none recorded"
	}
	return ip.String()
}

func containerOrNone(c *ContainerInfo) string {
	if c == nil {
		return "none"
	}
	return c.Name
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
