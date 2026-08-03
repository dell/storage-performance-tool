/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/preflight"
	"github.com/dell/storage-performance-tool/cli/internal/remoteip"
	"github.com/dell/storage-performance-tool/cli/internal/runcontrol"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// TestOrchestratorInterface defines the interface that test orchestrators must implement
type TestOrchestratorInterface interface {
	SetCallbacks(
		onStatus func(*TestStatus),
		onMetrics func(*MultiNodeMetricsUpdate),
		onOutput func(string),
		onError func(string),
	)
	StartTest(ctx context.Context, image string, params scenario.ScenarioParams) error
	StopTest() error
}

// HostConnection represents a connection to a Docker host
type HostConnection struct {
	Info          *hostparse.HostInfo
	DockerManager DockerInterface
	APIClient     *SptAPIClient
	ContainerID   string
	AdvertisedIP  string // Routable IP used for java.rmi.server.hostname and worker address CSV
	Status        HostStatus
	Error         error
	Managed       bool // true when spt launched the container
	mu            sync.Mutex
	phase         NodePhase
}

// HostStatus represents the lifecycle state of a remote host.
type HostStatus string

const (
	// HostStatusPending indicates the host has not begun connecting.
	HostStatusPending HostStatus = "pending"
	// HostStatusConnecting indicates a connection attempt is in progress.
	HostStatusConnecting HostStatus = "connecting"
	// HostStatusReady indicates the host is ready to start workloads.
	HostStatusReady HostStatus = "ready"
	// HostStatusRunning indicates the test container is running on the host.
	HostStatusRunning HostStatus = "running"
	// HostStatusFailed indicates an error occurred on the host.
	HostStatusFailed HostStatus = "failed"
	// HostStatusStopped indicates the test container has been stopped.
	HostStatusStopped HostStatus = "stopped"

	entryLogRelayModeStream = "stream"
	entryLogRelayModePoll   = "poll"
)

// MultiHostOrchestrator manages tests across multiple Docker hosts
type MultiHostOrchestrator struct {
	hosts           []*HostConnection
	minHosts        int
	scenarioContent string
	image           string
	apiPort         string
	attachWorkers   bool
	itemFileMounts  []scenario.FileMount
	resultsRoot     string

	// RMI Configuration
	networkMode  string
	rmiPortStart int
	rmiPortCount int

	mu           sync.Mutex
	preflight    preflight.Checker
	forceCleanup bool

	// Detection hook for advertised IP (overridable in tests)
	detectAdvIP func(ctx context.Context, host *hostparse.HostInfo) (string, error)

	// Optional notifier for user-facing progress messages (TUI-safe).
	// If nil, messages are printed to stdout as before.
	notifier func(string)

	// Concurrent finalization callers share one active attempt. A successful
	// attempt is sticky; a failed/canceled attempt releases the slot for a
	// bounded retry while hosts retain cleanup ownership.
	finalizeMu         sync.Mutex
	finalizeAttempt    *cleanupAttempt
	rollbackTimeout    time.Duration
	diagnosticsTimeout time.Duration
	cleanupTimeout     time.Duration

	runtimeIdentityRecorder  func(DistributedRuntimeIdentityEvidence)
	runtimeIdentityTier      string
	runtimeIdentityReference string
	runtimeIdentityEvidence  *DistributedRuntimeIdentityEvidence
	executionParticipantKeys []string
}

type cleanupAttempt struct {
	done    chan struct{}
	outcome runcontrol.FinalizationOutcome
}

// RMIConfig holds RMI configuration parameters
type RMIConfig struct {
	NetworkMode string
	PortStart   int
	PortCount   int
}

// DistributedRuntimeIdentityEvidence records the mandatory immutable-image tier used by a
// distributed persisted-data verification run.
type DistributedRuntimeIdentityEvidence struct {
	Tier           string                                  `json:"tier"`
	ImageReference string                                  `json:"imageReference"`
	ImageID        string                                  `json:"imageId"`
	PayloadSHA256  string                                  `json:"payloadSha256,omitempty"`
	Participants   []DistributedRuntimeIdentityParticipant `json:"participants"`
}

// DistributedRuntimeIdentityParticipant is host-specific image evidence.
type DistributedRuntimeIdentityParticipant struct {
	Host          string   `json:"host"`
	ImageID       string   `json:"imageId"`
	RepoDigests   []string `json:"repoDigests,omitempty"`
	PayloadSHA256 string   `json:"payloadSha256,omitempty"`
}

func runtimeIdentityHostKey(info *hostparse.HostInfo) string {
	if info == nil {
		return ""
	}
	if host := strings.TrimSpace(info.Host); host != "" {
		return host
	}
	return strings.TrimSpace(info.Original)
}

// NewMultiHostOrchestrator creates a new multi-host orchestrator
func NewMultiHostOrchestrator(hostInfos []*hostparse.HostInfo, minHosts int) *MultiHostOrchestrator {
	return NewMultiHostOrchestratorWithRMI(hostInfos, minHosts, RMIConfig{
		NetworkMode: constants.DefaultNetworkMode,
		PortStart:   constants.DefaultRMIPortStart,
		PortCount:   constants.DefaultRMIPortCount,
	})
}

// NewMultiHostOrchestratorWithRMI creates a new multi-host orchestrator with RMI configuration
func NewMultiHostOrchestratorWithRMI(hostInfos []*hostparse.HostInfo, minHosts int, rmiConfig RMIConfig) *MultiHostOrchestrator {
	hosts := make([]*HostConnection, len(hostInfos))
	for i, info := range hostInfos {
		hosts[i] = &HostConnection{
			Info:   info,
			Status: HostStatusPending,
			phase:  NodePhasePending,
		}
	}

	o := &MultiHostOrchestrator{
		hosts:               hosts,
		minHosts:            minHosts,
		apiPort:             constants.SptAPIPort, // Use Spt standard port on all hosts
		networkMode:         rmiConfig.NetworkMode,
		rmiPortStart:        rmiConfig.PortStart,
		rmiPortCount:        rmiConfig.PortCount,
		preflight:           preflight.NewDefaultChecker(),
		runtimeIdentityTier: constants.IntegrityRuntimeIdentityTierImage,
		rollbackTimeout:     constants.StartupRollbackTimeout,
		diagnosticsTimeout:  constants.DiagnosticsCollectionTimeout,
		cleanupTimeout:      constants.ContainerCleanupTimeout,
	}
	// Default detection uses remoteip via the command executor (SSH or local)
	o.detectAdvIP = func(ctx context.Context, host *hostparse.HostInfo) (string, error) {
		exec := command.NewCommandExecutor()
		// We import here to avoid a hard dependency in other files
		return detectAdvertisedIPWrapper(ctx, exec, host)
	}
	return o
}

// SetNotifier configures a callback for progress messages so that callers
// (like the TUI) can render them inside a messages pane instead of stdout.
func (o *MultiHostOrchestrator) SetNotifier(fn func(string)) {
	o.notifier = fn
}

// notify emits a single-line message either via the notifier callback (if set)
// or directly to stdout as a fallback for non-TUI contexts.
func (o *MultiHostOrchestrator) notify(msg string) {
	if o.notifier != nil {
		o.notifier(msg)
		return
	}
	fmt.Println(msg)
}

func (o *MultiHostOrchestrator) notifyf(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	// Trim any trailing newline to keep one-line semantics consistent.
	msg = strings.TrimRight(msg, "\n")
	o.notify(msg)
}

// detectAdvertisedIPWrapper bridges to internal/remoteip without exporting it in this file's imports.
// This indirection makes it easier to stub in tests and keeps imports tidy.
func detectAdvertisedIPWrapper(ctx context.Context, exec command.CommandExecutor, host *hostparse.HostInfo) (string, error) {
	return remoteip.DetectAdvertisedIP(ctx, exec, host)
}

// SetImage sets the image used for distributed runs (used by preflight)
func (o *MultiHostOrchestrator) SetImage(image string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.image = image
}

// SetRuntimeIdentityRecorder receives immutable runtime evidence before any verification container
// starts. The command layer uses it to persist evidence in run metadata.
func (o *MultiHostOrchestrator) SetRuntimeIdentityRecorder(recorder func(DistributedRuntimeIdentityEvidence)) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.runtimeIdentityRecorder = recorder
}

// SetIntegrityRuntimeIdentityTier selects the mandatory image-only tier or the stronger image
// plus /opt/spt payload tier for distributed persisted-data verification.
func (o *MultiHostOrchestrator) SetIntegrityRuntimeIdentityTier(tier string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.runtimeIdentityTier = strings.ToLower(strings.TrimSpace(tier))
	o.runtimeIdentityReference = ""
	o.runtimeIdentityEvidence = nil
}

// SetResultsRoot configures the run results directory used for local diagnostics collection.
func (o *MultiHostOrchestrator) SetResultsRoot(resultsRoot string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.resultsRoot = strings.TrimSpace(resultsRoot)
}

// SetAPIPort overrides the Spt API port used for node startup and polling.
func (o *MultiHostOrchestrator) SetAPIPort(apiPort string) {
	apiPort = strings.TrimSpace(apiPort)
	if apiPort == "" {
		apiPort = constants.SptAPIPort
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.apiPort = apiPort
}

// SetForceCleanup enables stopping conflicting containers during preflight
func (o *MultiHostOrchestrator) SetForceCleanup(force bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.forceCleanup = force
}

// SetAttachExistingWorkers toggles attach mode (skip worker launches, do not manage worker lifecycle).
func (o *MultiHostOrchestrator) SetAttachExistingWorkers(attach bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.attachWorkers = attach
}

// AttachExistingWorkersEnabled reports if attach mode is enabled.
func (o *MultiHostOrchestrator) AttachExistingWorkersEnabled() bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.attachWorkers
}

// GetStatus returns the current status of a host connection
func (hc *HostConnection) GetStatus() HostStatus {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	return hc.Status
}

// SetStatus updates the status of a host connection
func (hc *HostConnection) SetStatus(status HostStatus) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.Status = status
}

// SetError sets an error for the host connection
func (hc *HostConnection) SetError(err error) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.Error = err
	hc.Status = HostStatusFailed
}

// SetManaged records whether the host's container lifecycle is owned by spt.
func (hc *HostConnection) SetManaged(managed bool) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.Managed = managed
}

// IsManaged reports whether spt started and should stop the container.
func (hc *HostConnection) IsManaged() bool {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	return hc.Managed
}

// GetError returns the last recorded error for the host connection (if any).
func (hc *HostConnection) GetError() error {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	return hc.Error
}

// SetPhase records the current node lifecycle phase.
func (hc *HostConnection) SetPhase(phase NodePhase) {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	hc.phase = phase
}

// GetPhase returns the node lifecycle phase.
func (hc *HostConnection) GetPhase() NodePhase {
	hc.mu.Lock()
	defer hc.mu.Unlock()
	return hc.phase
}

// ConnectHosts establishes Docker connections to all hosts
func (o *MultiHostOrchestrator) ConnectHosts(ctx context.Context) error {
	var wg sync.WaitGroup
	var connectErrors []string
	var connectErrorsMu sync.Mutex

	o.notifyf("🔗 Connecting to %d Docker host(s)...", len(o.hosts))
	attachWorkers := o.AttachExistingWorkersEnabled()
	var entryHost *HostConnection
	if len(o.hosts) > 0 {
		entryHost = o.hosts[0]
	}

	for _, host := range o.hosts {
		wg.Add(1)
		go func(h *HostConnection) {
			defer wg.Done()

			h.SetStatus(HostStatusConnecting)
			isEntry := entryHost != nil && h == entryHost

			logging.LogInfo("docker-multi", "attempting connection",
				"host", h.Info.Original)

			dm, err := NewDockerManagerForHost(ctx, h.Info)
			if err != nil {
				h.SetError(err)

				connectErrorsMu.Lock()
				errMsg := fmt.Sprintf("  ❌ %s: %v", h.Info.Original, err)
				connectErrors = append(connectErrors, errMsg)
				connectErrorsMu.Unlock()

				logging.LogError("docker-multi", "connection failed",
					fmt.Errorf("host %s: %w", h.Info.Original, err))
				return
			}
			o.mu.Lock()
			resultsRoot := o.resultsRoot
			o.mu.Unlock()
			dm.setDiagnosticsResultsRoot(resultsRoot)

			// Run preflight checks (docker availability, image presence, port readiness)
			if o.preflight != nil {
				// 1) Docker reachable
				if ver, derr := o.preflight.CheckDocker(ctx, h.Info); derr != nil {
					h.SetError(fmt.Errorf("docker check failed: %w", derr))
					connectErrorsMu.Lock()
					connectErrors = append(connectErrors, fmt.Sprintf("  ❌ %s: Docker check failed: %v", h.Info.Original, derr))
					connectErrorsMu.Unlock()
					logging.LogError("docker-multi", "preflight docker check failed", derr, "host", h.Info.Original)
					return
				} else { //nolint:revive // keep else for clarity here
					logging.LogInfo("docker-multi", "docker available", "host", h.Info.Original, "version", ver)
				}
				// 2) Ensure image if known
				o.mu.Lock()
				img := o.image
				o.mu.Unlock()
				if img != "" {
					if ierr := o.preflight.EnsureImage(ctx, h.Info, img); ierr != nil {
						h.SetError(fmt.Errorf("image preflight failed: %w", ierr))
						connectErrorsMu.Lock()
						connectErrors = append(connectErrors, fmt.Sprintf("  ❌ %s: Image check failed: %v", h.Info.Original, ierr))
						connectErrorsMu.Unlock()
						logging.LogError("docker-multi", "preflight image ensure failed", ierr, "host", h.Info.Original, "image", img)
						return
					}
				}
				// 3) Check ports
				apiPort, convErr := strconv.Atoi(o.apiPort)
				if convErr != nil {
					h.SetError(fmt.Errorf("invalid API port %q: %w", o.apiPort, convErr))
					connectErrorsMu.Lock()
					connectErrors = append(connectErrors, fmt.Sprintf("  ❌ %s: Invalid API port %q", h.Info.Original, o.apiPort))
					connectErrorsMu.Unlock()
					return
				}
				if cinfo, perr := o.preflight.CheckPorts(ctx, h.Info, apiPort, o.rmiPortStart, o.rmiPortCount); perr != nil {
					h.SetError(fmt.Errorf("port check failed: %w", perr))
					connectErrorsMu.Lock()
					connectErrors = append(connectErrors, fmt.Sprintf("  ❌ %s: Port check failed: %v", h.Info.Original, perr))
					connectErrorsMu.Unlock()
					logging.LogError("docker-multi", "preflight port check failed", perr, "host", h.Info.Original)
					return
				} else if len(cinfo.ConflictPorts) > 0 {
					if attachWorkers && !isEntry {
						logging.LogInfo("docker-multi", "attach mode: existing container using required ports",
							"host", h.Info.Original,
							"ports", cinfo.ConflictPorts)
						if len(cinfo.Containers) == 0 {
							logging.LogWarn("docker-multi", "attach mode proceeding with unknown port owner",
								"host", h.Info.Original,
								"ports", cinfo.ConflictPorts)
						} else {
							logging.LogDebug("docker-multi", "attach mode conflict accepted",
								"host", h.Info.Original,
								"containers", cinfo.Containers)
						}
					} else {
						o.mu.Lock()
						doCleanup := o.forceCleanup
						o.mu.Unlock()

						resolved := false
						if doCleanup && len(cinfo.Containers) > 0 {
							exec := command.NewCommandExecutor()
							ops := command.NewDockerOperations(exec, h.Info)
							stopped := 0
							for _, c := range cinfo.Containers {
								name := strings.ToLower(c.Name)
								image := strings.ToLower(c.Image)
								if strings.Contains(name, "spt") ||
									strings.Contains(name, "storage-performance-tool") ||
									strings.Contains(image, strings.ToLower(constants.DefaultSptImage)) {
									if err := ops.StopContainer(ctx, c.ID); err == nil {
										stopped++
									}
								}
							}
							if stopped > 0 {
								if c2, e2 := o.preflight.CheckPorts(ctx, h.Info, apiPort, o.rmiPortStart, o.rmiPortCount); e2 == nil && len(c2.ConflictPorts) == 0 {
									logging.LogInfo("docker-multi", "resolved port conflicts via cleanup", "host", h.Info.Original)
									resolved = true
								} else {
									h.SetError(fmt.Errorf("ports still in use after cleanup: %v", c2.ConflictPorts))
									connectErrorsMu.Lock()
									connectErrors = append(connectErrors, fmt.Sprintf("  ❌ %s: Ports still in use: %v", h.Info.Original, c2.ConflictPorts))
									connectErrorsMu.Unlock()
									return
								}
							}
						}

						if !resolved {
							h.SetError(fmt.Errorf("conflicting ports in use: %v", cinfo.ConflictPorts))
							connectErrorsMu.Lock()
							connectErrors = append(connectErrors, fmt.Sprintf("  ❌ %s: Ports in use: %v", h.Info.Original, cinfo.ConflictPorts))
							connectErrorsMu.Unlock()
							logging.LogError("docker-multi", "preflight port conflicts", fmt.Errorf("%v", cinfo.ConflictPorts), "host", h.Info.Original)
							return
						}
					}
				}
			}

			h.mu.Lock()
			h.DockerManager = dm
			h.Status = HostStatusReady
			h.mu.Unlock()
			h.SetPhase(NodePhaseContacted)

			o.notifyf("  ✅ Connected to %s", h.Info.Original)
			logging.LogInfo("docker-multi", "connection successful",
				"host", h.Info.Original)
		}(host)
	}

	wg.Wait()

	// Count successful connections
	successCount := 0
	for _, host := range o.hosts {
		if host.GetStatus() == HostStatusReady {
			successCount++
		}
	}

	// Print summary
	o.notifyf("Connection Summary: %d/%d hosts connected", successCount, len(o.hosts))

	// Print failures if any
	if len(connectErrors) > 0 {
		o.notify("Failed connections:")
		for _, err := range connectErrors {
			o.notify(err)
		}
	}

	// Check if we met minimum hosts requirement
	if successCount < o.minHosts {
		return fmt.Errorf("insufficient hosts connected: %d connected, %d required (--min-hosts=%d)",
			successCount, o.minHosts, o.minHosts)
	}

	if successCount < len(o.hosts) {
		o.notifyf("⚠️  Continuing with %d hosts (minimum threshold met)", successCount)
	}

	return nil
}

// GetReadyHosts returns all hosts that are ready for testing
func (o *MultiHostOrchestrator) GetReadyHosts() []*HostConnection {
	readyHosts := make([]*HostConnection, 0, len(o.hosts))
	for _, host := range o.hosts {
		if host.GetStatus() == HostStatusReady || host.GetStatus() == HostStatusRunning {
			readyHosts = append(readyHosts, host)
		}
	}
	return readyHosts
}

func (o *MultiHostOrchestrator) configureItemFileMounts(
	ctx context.Context, hosts []*HostConnection, mounts []scenario.FileMount,
) error {
	if len(mounts) == 0 {
		return nil
	}
	for _, host := range hosts {
		if host.DockerManager == nil {
			cause := fmt.Errorf("host %s has no Docker manager configured", host.Info.Original)
			return errors.Join(cause, o.cleanupManagedContainersAfterStartFailure(ctx))
		}
		// Remote item staging is itself a managed resource. Claim ownership
		// before staging so a failed remote rm remains reachable by StopAll.
		host.SetManaged(true)
		if err := configureDockerFileMounts(ctx, host.DockerManager, mounts); err != nil {
			cause := fmt.Errorf("configure item file mounts on %s: %w", host.Info.Original, err)
			return errors.Join(cause, o.cleanupManagedContainersAfterStartFailure(ctx))
		}
	}
	return nil
}

// StartContainers starts Spt containers on all ready hosts
func (o *MultiHostOrchestrator) StartContainers(ctx context.Context, image string, additionalArgs []string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	o.mu.Lock()
	o.image = image
	o.mu.Unlock()

	readyHosts := o.GetReadyHosts()
	if len(readyHosts) == 0 {
		return fmt.Errorf("no ready hosts available for container startup")
	}
	if err := o.configureItemFileMounts(ctx, readyHosts, o.itemFileMounts); err != nil {
		return err
	}

	o.notifyf("🚀 Starting containers on %d host(s)...", len(readyHosts))

	var wg sync.WaitGroup
	var startErrors []string
	var startErrorsMu sync.Mutex

	for _, host := range readyHosts {
		wg.Add(1)
		go func(h *HostConnection) {
			defer wg.Done()
			if err := ctx.Err(); err != nil {
				h.SetError(err)
				return
			}

			h.SetStatus(HostStatusConnecting) // Repurpose for container starting

			// Check if DockerManager is nil
			if h.DockerManager == nil {
				h.SetError(fmt.Errorf("no Docker manager available for host"))

				startErrorsMu.Lock()
				errMsg := fmt.Sprintf("  ❌ %s: no Docker manager available", h.Info.Original)
				startErrors = append(startErrors, errMsg)
				startErrorsMu.Unlock()

				logging.LogError("docker-multi", "no Docker manager",
					fmt.Errorf("host %s has no Docker manager", h.Info.Original),
					"host", h.Info.Original)
				return
			}

			containerID, err := startContainerInNodeModeContext(
				ctx, h.DockerManager, image, o.apiPort, o.networkMode, additionalArgs)
			if err != nil {
				h.SetError(fmt.Errorf("failed to start container: %w", err))

				startErrorsMu.Lock()
				errMsg := fmt.Sprintf("  ❌ %s: failed to start container: %v", h.Info.Original, err)
				startErrors = append(startErrors, errMsg)
				startErrorsMu.Unlock()

				logging.LogError("docker-multi", "container start failed",
					fmt.Errorf("host %s: %w", h.Info.Original, err),
					"host", h.Info.Original)
				return
			}

			h.mu.Lock()
			h.ContainerID = containerID
			h.Status = HostStatusRunning
			h.mu.Unlock()
			h.SetManaged(true)
			h.SetPhase(NodePhaseContainerStarting)

			o.notifyf("  ✅ Container started on %s (ID: %s)", h.Info.Original, containerID[:12])
			logging.LogInfo("docker-multi", "container started",
				"host", h.Info.Original,
				"container_id", containerID)
		}(host)
	}

	wg.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}

	// Count successful starts
	runningCount := 0
	for _, host := range readyHosts {
		if host.GetStatus() == HostStatusRunning {
			runningCount++
		}
	}

	// Print failures if any
	if len(startErrors) > 0 {
		o.notify("Container startup failures:")
		for _, err := range startErrors {
			o.notify(err)
		}
	}

	o.notifyf("Container Summary: %d/%d containers started successfully", runningCount, len(readyHosts))

	if runningCount == 0 {
		return fmt.Errorf("failed to start containers on any hosts")
	}

	if runningCount < len(readyHosts) {
		o.notifyf("⚠️  Continuing with %d containers (some failed to start)", runningCount)
	}

	return nil
}

// WaitForAPIs waits for all container APIs to be ready
func (o *MultiHostOrchestrator) WaitForAPIs(ctx context.Context, timeout time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	runningHosts := make([]*HostConnection, 0)
	for _, host := range o.hosts {
		if host.GetStatus() == HostStatusRunning {
			runningHosts = append(runningHosts, host)
		}
	}

	if len(runningHosts) == 0 {
		return fmt.Errorf("no running containers to wait for")
	}

	o.notifyf("⏳ Waiting for APIs to be ready on %d host(s)...", len(runningHosts))

	var wg sync.WaitGroup
	var apiErrors []string
	var apiErrorsMu sync.Mutex

	for _, host := range runningHosts {
		wg.Add(1)
		go func(h *HostConnection) {
			defer wg.Done()

			apiURL := h.Info.GetAPIURL(o.apiPort)
			apiClient := NewSptAPIClient(apiURL)

			if err := apiClient.WaitForAPIReadyContext(ctx, timeout); err != nil {
				h.SetError(fmt.Errorf("API not ready: %w", err))

				apiErrorsMu.Lock()
				errMsg := fmt.Sprintf("  ❌ %s: API timeout: %v", h.Info.Original, err)
				apiErrors = append(apiErrors, errMsg)
				apiErrorsMu.Unlock()

				logging.LogError("docker-multi", "API wait failed",
					fmt.Errorf("host %s: %w", h.Info.Original, err),
					"host", h.Info.Original,
					"api_url", apiURL)
				return
			}

			h.mu.Lock()
			h.APIClient = apiClient
			h.mu.Unlock()
			h.SetPhase(NodePhaseAPIReady)

			o.notifyf("  ✅ API ready on %s (%s)", h.Info.Original, apiURL)
			logging.LogInfo("docker-multi", "API ready",
				"host", h.Info.Original,
				"api_url", apiURL)
		}(host)
	}

	wg.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}

	// Count successful APIs
	apiReadyCount := 0
	for _, host := range runningHosts {
		if host.GetStatus() == HostStatusRunning && host.APIClient != nil {
			apiReadyCount++
		}
	}

	// Print failures if any
	if len(apiErrors) > 0 {
		o.notify("API readiness failures:")
		for _, err := range apiErrors {
			o.notify(err)
		}
	}

	o.notifyf("API Summary: %d/%d APIs ready", apiReadyCount, len(runningHosts))

	if apiReadyCount == 0 {
		return fmt.Errorf("no APIs became ready within timeout")
	}

	return nil
}

// cleanupManagedContainersAfterStartFailure removes containers immediately when no workload was
// submitted. There is no run to shut down or linger window to preserve in this path.
func (o *MultiHostOrchestrator) cleanupManagedContainersAfterStartFailure(ctx context.Context) error {
	timeout := o.rollbackTimeout
	if timeout <= 0 {
		timeout = constants.StartupRollbackTimeout
	}
	cleanupCtx, cancel := boundedDetachedContext(ctx, timeout)
	defer cancel()
	var cleanupErrors []error
	for _, host := range o.hosts {
		if host == nil || !host.IsManaged() || host.DockerManager == nil {
			continue
		}
		cleanupErr := cleanupDockerWithinContext(cleanupCtx, host.DockerManager)
		if hasManagedDockerResources(host.DockerManager) {
			if cleanupErr != nil {
				cleanupErrors = append(cleanupErrors, fmt.Errorf("cleanup %s: %w", host.Info.Original, cleanupErr))
			}
			cleanupErrors = append(cleanupErrors, fmt.Errorf(
				"cleanup %s returned with managed resources still owned", host.Info.Original))
			continue
		}
		markHostCleanupComplete(host)
		if cleanupErr != nil {
			cleanupErrors = append(cleanupErrors, fmt.Errorf("cleanup %s evidence: %w", host.Info.Original, cleanupErr))
		}
	}
	return errors.Join(cleanupErrors...)
}

func (o *MultiHostOrchestrator) cleanupAmbiguousSubmission(ctx context.Context, hooks LaunchHooks) error {
	if hooks.SessionManaged() {
		return nil
	}
	return o.cleanupManagedContainersAfterStartFailure(ctx)
}

func cleanupDockerWithinContext(ctx context.Context, manager DockerInterface) error {
	return manager.CleanupContext(ctx)
}

type resourceOnlyDockerCleaner interface {
	CleanupResourcesContext(context.Context) error
}

func cleanupDockerResourcesWithinContext(ctx context.Context, manager DockerInterface) error {
	if cleaner, ok := manager.(resourceOnlyDockerCleaner); ok {
		return cleaner.CleanupResourcesContext(ctx)
	}
	return manager.CleanupContext(ctx)
}

type managedDockerResourceReporter interface {
	HasManagedResources() bool
}

func hasManagedDockerResources(manager DockerInterface) bool {
	if reporter, ok := manager.(managedDockerResourceReporter); ok {
		return reporter.HasManagedResources()
	}
	return manager.ContainerID() != ""
}

func markHostCleanupComplete(host *HostConnection) {
	host.mu.Lock()
	host.ContainerID = ""
	host.Status = HostStatusStopped
	host.Managed = false
	host.mu.Unlock()
}

// StopAllContainers stops all managed containers within the caller's budget.
func (o *MultiHostOrchestrator) StopAllContainers(ctx context.Context) error {
	return o.stopAllContainers(ctx, false)
}

func (o *MultiHostOrchestrator) stopAllContainers(ctx context.Context, resourcesOnly bool) error {
	if ctx == nil {
		ctx = context.Background()
	}
	managedHosts := make([]*HostConnection, 0)
	for _, host := range o.hosts {
		if host != nil && host.DockerManager != nil && host.IsManaged() {
			managedHosts = append(managedHosts, host)
		}
	}

	if len(managedHosts) == 0 {
		return nil // Nothing to stop
	}

	o.notifyf("🛑 Stopping containers on %d host(s)...", len(managedHosts))

	type stopResult struct {
		record *diagnosticsRecord
		err    error
	}
	var wg sync.WaitGroup
	resultsCh := make(chan stopResult, len(managedHosts))
	for _, host := range managedHosts {
		wg.Add(1)
		go func(h *HostConnection) {
			defer wg.Done()
			result := stopResult{}
			defer func() { resultsCh <- result }()

			if !h.IsManaged() {
				return
			}

			containerID := h.ContainerID
			if containerID == "" {
				containerID = h.DockerManager.ContainerID()
			}

			// Try graceful API shutdown first if API client is available
			if h.APIClient != nil && containerID != "" {
				labelPrefix := h.Info.Original
				h.APIClient.LogReadySnapshotContext(ctx, "pre-shutdown/"+labelPrefix)
				if err := h.APIClient.ShutdownContext(ctx); err != nil {
					logging.LogError("docker-multi", "graceful API shutdown failed, using container stop",
						err,
						"host", h.Info.Original)
				}
				// Wait for linger window best-effort
				_ = h.APIClient.WaitForLingerContext(ctx, constants.APILingerDefault)
				grace := time.NewTimer(constants.ContainerShutdownGrace)
				select {
				case <-ctx.Done():
					if !grace.Stop() {
						select {
						case <-grace.C:
						default:
						}
					}
					result.err = ctx.Err()
					return
				case <-grace.C:
				}
				h.APIClient.LogReadySnapshotContext(ctx, "post-shutdown/"+labelPrefix)
			}

			// Stop the container - need to use the underlying Docker client
			// Double-check DockerManager is not nil (defensive programming)
			if h.DockerManager == nil {
				logging.LogError("docker-multi", "Docker manager is nil during container stop",
					fmt.Errorf("host %s has nil Docker manager", h.Info.Original),
					"host", h.Info.Original,
					"container_id", containerID)
				return
			}

			var err error
			if resourcesOnly {
				err = cleanupDockerResourcesWithinContext(ctx, h.DockerManager)
			} else {
				err = cleanupDockerWithinContext(ctx, h.DockerManager)
			}
			if collector, ok := h.DockerManager.(diagnosticsCollector); ok {
				if record := collector.diagnosticsRecord(); record != nil {
					result.record = record
				}
			}
			resourcesRemain := hasManagedDockerResources(h.DockerManager)
			if err == nil && resourcesRemain {
				err = fmt.Errorf("cleanup returned with managed resources still present")
			}
			if err != nil {
				result.err = fmt.Errorf("%s: %w", h.Info.Original, err)
				logging.LogError("docker-multi", "container stop failed",
					fmt.Errorf("host %s container %s: %w", h.Info.Original, containerID, err),
					"host", h.Info.Original,
					"container_id", containerID)
			}
			if !resourcesRemain {
				o.notifyf("  ✅ Container stopped on %s", h.Info.Original)
				logging.LogInfo("docker-multi", "container stopped",
					"host", h.Info.Original,
					"container_id", containerID)
				markHostCleanupComplete(h)
			}
		}(host)
	}

	wg.Wait()
	var records []diagnosticsRecord
	var stopErrors []error
	for range managedHosts {
		result := <-resultsCh
		if result.record != nil {
			records = append(records, *result.record)
		}
		if result.err != nil {
			stopErrors = append(stopErrors, result.err)
		}
	}
	if err := writeDiagnosticsAggregateManifest(o.resultsRoot, records); err != nil {
		stopErrors = append(stopErrors, err)
	}
	return errors.Join(stopErrors...)
}

// CollectDiagnostics copies diagnostics from all managed hosts into the run results directory.
func (o *MultiHostOrchestrator) CollectDiagnostics(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	type diagnosticsResult struct {
		host   string
		record *diagnosticsRecord
		err    error
	}

	var wg sync.WaitGroup
	workerCount := 0
	resultsCh := make(chan diagnosticsResult, len(o.hosts))
	for _, host := range o.hosts {
		if host == nil || !host.IsManaged() || host.DockerManager == nil {
			continue
		}
		collector, ok := host.DockerManager.(diagnosticsCollector)
		if !ok {
			continue
		}
		wg.Add(1)
		workerCount++
		go func(h *HostConnection, c diagnosticsCollector) {
			defer wg.Done()
			hostCtx, cancel := context.WithTimeout(ctx, constants.DiagnosticsCollectionTimeout)
			defer cancel()
			// JFR/GC artifacts use dumponexit, which only guarantees the file
			// exists once the JVM has exited. Stop the container first so
			// collection isn't racing a still-running process; this is a
			// standalone collection pass (unlike Cleanup) so nothing else in
			// this call path stops the container for us.
			if err := c.gracefulStopForDiagnostics(hostCtx); err != nil {
				logging.LogWarn("docker-multi", "diagnostics graceful stop failed",
					"host", h.Info.Original,
					"error", err.Error())
			}
			record, err := c.collectDiagnostics(hostCtx)
			resultsCh <- diagnosticsResult{host: h.Info.Original, record: record, err: err}
		}(host, collector)
	}

	wg.Wait()

	var records []diagnosticsRecord
	var errs []error
	for pending := workerCount; pending > 0; pending-- {
		result := <-resultsCh
		if result.record != nil {
			records = append(records, *result.record)
		}
		if result.err != nil {
			errs = append(errs, fmt.Errorf("%s: %w", result.host, result.err))
			logging.LogWarn("docker-multi", "diagnostics collection failed",
				"host", result.host,
				"error", result.err.Error())
		}
	}
	if err := writeDiagnosticsAggregateManifest(o.resultsRoot, records); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// FinalizeDiagnosticsAndCleanup collects diagnostics from every managed host
// and then fully stops/removes their containers (and cleans up remote
// staging). It is the single, canonical end-of-run action for delegated
// auto-results shutdown: without it, containers were only ever stopped as a
// side effect of collecting diagnostics (never removed, staging never
// cleaned up) on the normal-completion path, and a slow diagnostics copy
// could otherwise race a caller-side timeout fallback trying to do the same
// cleanup concurrently.
//
// Safe to call more than once, including concurrently. Work starts once, while
// each waiter remains independently cancellable.
func (o *MultiHostOrchestrator) FinalizeDiagnosticsAndCleanup(ctx context.Context) error {
	return o.FinalizeDiagnosticsAndCleanupOutcome(ctx).Error()
}

// FinalizeDiagnosticsAndCleanupOutcome preserves diagnostics, mandatory
// removal, and final ownership as independently inspectable results.
func (o *MultiHostOrchestrator) FinalizeDiagnosticsAndCleanupOutcome(
	ctx context.Context,
) runcontrol.FinalizationOutcome {
	ctx = normalizeContext(ctx)
	if err := ctx.Err(); err != nil {
		return runcontrol.FinalizationOutcome{
			WaitErr: err,
			Resources: func() runcontrol.ResourceDisposition {
				if o.hasManagedResourceOwnership() {
					return runcontrol.ResourceDispositionRetained
				}
				return runcontrol.ResourceDispositionNotOwned
			}(),
		}
	}
	o.finalizeMu.Lock()
	attempt := o.finalizeAttempt
	if attempt == nil {
		attempt = &cleanupAttempt{done: make(chan struct{})}
		o.finalizeAttempt = attempt
		hadResources := o.hasManagedResourceOwnership()
		diagnosticsTimeout := o.diagnosticsTimeout
		if diagnosticsTimeout <= 0 {
			diagnosticsTimeout = constants.DiagnosticsCollectionTimeout
		}
		cleanupTimeout := o.cleanupTimeout
		if cleanupTimeout <= 0 {
			cleanupTimeout = constants.ContainerCleanupTimeout
		}
		attemptBase := context.WithoutCancel(ctx)
		go func(active *cleanupAttempt) {
			diagnosticsCtx, cancelDiagnostics := context.WithTimeout(attemptBase, diagnosticsTimeout)
			diagErr := o.CollectDiagnostics(diagnosticsCtx)
			cancelDiagnostics()
			active.outcome.Diagnostics = runcontrol.CompletedPhase(diagErr)

			cleanupCtx, cancelCleanup := context.WithTimeout(attemptBase, cleanupTimeout)
			stopErr := o.stopAllContainers(cleanupCtx, true)
			cancelCleanup()
			active.outcome.Removal = runcontrol.CompletedPhase(stopErr)
			switch {
			case o.hasManagedResourceOwnership():
				active.outcome.Resources = runcontrol.ResourceDispositionRetained
			case hadResources:
				active.outcome.Resources = runcontrol.ResourceDispositionRemoved
			default:
				active.outcome.Resources = runcontrol.ResourceDispositionNotOwned
			}
			if active.outcome.Error() != nil {
				o.finalizeMu.Lock()
				if o.finalizeAttempt == active {
					o.finalizeAttempt = nil
				}
				o.finalizeMu.Unlock()
			}
			close(active.done)
		}(attempt)
	}
	o.finalizeMu.Unlock()
	select {
	case <-attempt.done:
		return attempt.outcome
	case <-ctx.Done():
		return runcontrol.FinalizationOutcome{
			WaitErr:   ctx.Err(),
			Resources: runcontrol.ResourceDispositionRetained,
		}
	}
}

func (o *MultiHostOrchestrator) hasManagedResourceOwnership() bool {
	for _, host := range o.hosts {
		if host == nil {
			continue
		}
		if host.IsManaged() || (host.DockerManager != nil && hasManagedDockerResources(host.DockerManager)) {
			return true
		}
	}
	return false
}

// GetHostCount returns the total number of hosts
func (o *MultiHostOrchestrator) GetHostCount() int {
	return len(o.hosts)
}

// GetReadyHostCount returns the number of ready hosts
func (o *MultiHostOrchestrator) GetReadyHostCount() int {
	count := 0
	for _, host := range o.hosts {
		status := host.GetStatus()
		if status == HostStatusReady || status == HostStatusRunning {
			count++
		}
	}
	return count
}

// GetRunningHostCount returns the number of hosts with running containers
func (o *MultiHostOrchestrator) GetRunningHostCount() int {
	count := 0
	for _, host := range o.hosts {
		if host.GetStatus() == HostStatusRunning {
			count++
		}
	}
	return count
}

// GetHostsInfo returns a summary of all hosts and their status
func (o *MultiHostOrchestrator) GetHostsInfo() []map[string]interface{} {
	info := make([]map[string]interface{}, len(o.hosts))
	for i, host := range o.hosts {
		host.mu.Lock()
		info[i] = map[string]interface{}{
			"host":         host.Info.Original,
			"status":       string(host.Status),
			"container_id": host.ContainerID,
			"error":        host.Error,
		}
		host.mu.Unlock()
	}
	return info
}

// MultiHostTestOrchestrator wraps MultiHostOrchestrator to provide TestOrchestrator interface
type MultiHostTestOrchestrator struct {
	multiHost *MultiHostOrchestrator

	generateScenario  func(scenario.Params) (string, error)
	buildEndpointArgs func(scenario.Params) ([]string, error)
	generateDefaults  func(scenario.Params) ([]byte, error)
	waitForAPIs       func(context.Context, time.Duration) error

	// Callbacks
	onStatusUpdate func(status *TestStatus)
	onMetrics      func(update *MultiNodeMetricsUpdate) // Changed to multi-node
	onOutput       func(line string)
	onError        func(err string)

	completionCh   chan struct{}
	completionOnce sync.Once

	// New fields for multi-node metrics
	aggregator    *MetricsAggregator
	metricsPoller MetricsPoller
	pollInterval  time.Duration
	pollingCancel context.CancelFunc
	pollStates    map[string]*nodePollState
	pollStateMu   sync.Mutex
	backoffCfg    backoffConfig
	randMu        sync.Mutex
	randSource    *rand.Rand

	// Diagnostics
	logJSONBodies bool

	// Entry node log relay
	entryRelay *EntryLogRelay

	// Message sink for TUI (e.g., p.Send(sptMessageMsg(...)))
	messageSink func(string)

	expectedStepIDs []string

	compatOnce sync.Once
}

// APIMetricsPoller implements MetricsPoller using SptAPIClient
type APIMetricsPoller struct {
	timeout time.Duration
}

// NewAPIMetricsPoller creates a new API-based metrics poller
func NewAPIMetricsPoller() *APIMetricsPoller {
	return &APIMetricsPoller{
		timeout: constants.APIPollingTimeout, // Quick timeout for concurrent polling
	}
}

// PollMetrics polls metrics from a specific node using its API client
func (p *APIMetricsPoller) PollMetrics(_ context.Context, _ string) (*PerformanceMetric, error) {
	// This is a placeholder - in practice, this would need access to the host's APIClient
	// For now, we'll implement the actual polling logic in the orchestrator methods
	return nil, fmt.Errorf("APIMetricsPoller.PollMetrics not implemented - use orchestrator polling")
}

// NewMultiHostTestOrchestrator creates a wrapper around MultiHostOrchestrator
func NewMultiHostTestOrchestrator(multiHost *MultiHostOrchestrator) *MultiHostTestOrchestrator {
	entryAggregator := NewMetricsAggregator()
	if multiHost != nil && len(multiHost.hosts) > 0 && multiHost.hosts[0] != nil && multiHost.hosts[0].Info != nil {
		entryAggregator = NewMetricsAggregatorWithEntry(multiHost.hosts[0].Info.Original)
	}

	return &MultiHostTestOrchestrator{
		multiHost:         multiHost,
		generateScenario:  scenario.GenerateScenario,
		buildEndpointArgs: scenario.BuildEndpointArgs,
		generateDefaults:  scenario.GenerateDefaults,
		waitForAPIs:       multiHost.WaitForAPIs,
		completionCh:      make(chan struct{}),
		aggregator:        entryAggregator,
		metricsPoller:     NewAPIMetricsPoller(),         // Default implementation
		pollInterval:      constants.MetricsPollInterval, // Default poll interval
		pollStates:        make(map[string]*nodePollState),
		backoffCfg:        multiNodeBackoff,
		randSource:        rand.New(rand.NewSource(time.Now().UnixNano())), // #nosec G404 -- jitter only
		logJSONBodies:     os.Getenv("SPT_LOG_METRICS_BODY") == "1",
	}
}

// SetExpectedStepIDs constrains completion detection to the final step in the
// generated scenario. This prevents a precondition/seed step from ending a
// multi-step headless run.
func (m *MultiHostTestOrchestrator) SetExpectedStepIDs(stepIDs []string) {
	m.expectedStepIDs = append([]string(nil), stepIDs...)
}

// BuildBaselineUpdate constructs a status-only update for all configured hosts (no per-node samples).
func (m *MultiHostTestOrchestrator) BuildBaselineUpdate() *MultiNodeMetricsUpdate {
	if m.multiHost == nil {
		return nil
	}
	now := time.Now()
	status := make(map[string]NodeConnectionStatus)
	for _, h := range m.multiHost.hosts {
		nodeID := h.Info.Original
		isConn := h.GetStatus() == HostStatusRunning && h.APIClient != nil
		phase := h.GetPhase()
		if phase == "" {
			phase = NodePhasePending
		}
		if phase == NodePhasePending {
			switch h.GetStatus() {
			case HostStatusReady:
				phase = NodePhaseContacted
			case HostStatusRunning:
				phase = NodePhaseContainerStarting
			case HostStatusFailed, HostStatusStopped, HostStatusPending, HostStatusConnecting:
				phase = NodePhasePending
			}
		}
		if h.APIClient != nil && phase == NodePhaseContainerStarting {
			phase = NodePhaseAPIReady
		}
		s := NodeConnectionStatus{LastSeen: now, IsConnected: isConn, IsActive: false, Phase: phase}
		if h.GetStatus() == HostStatusFailed && h.Error != nil {
			s.Error = h.Error
		}
		status[nodeID] = s
	}
	placeholder := &PerformanceMetric{Timestamp: now}
	return &MultiNodeMetricsUpdate{
		Timestamp:  now,
		Aggregated: placeholder,
		PerNode:    map[string]*PerformanceMetric{},
		NodeStatus: status,
	}
}

// SetCallbacks sets the callback functions for status, metrics, and output events
func (m *MultiHostTestOrchestrator) SetCallbacks(
	onStatusUpdate func(status *TestStatus),
	onMetrics func(update *MultiNodeMetricsUpdate), // Changed signature
	onOutput func(line string),
	onError func(err string),
) {
	m.onStatusUpdate = onStatusUpdate
	m.onMetrics = onMetrics
	m.onOutput = onOutput
	m.onError = onError
}

// SetMessageSink configures a sink for user-visible messages
// (wired by the TUI to append lines to the Messages viewport).
func (m *MultiHostTestOrchestrator) SetMessageSink(fn func(string)) {
	m.messageSink = fn
}

func (m *MultiHostTestOrchestrator) getPollState(nodeID string) *nodePollState {
	m.pollStateMu.Lock()
	defer m.pollStateMu.Unlock()
	if m.pollStates == nil {
		m.pollStates = make(map[string]*nodePollState)
	}
	state, ok := m.pollStates[nodeID]
	if !ok {
		state = newNodePollState()
		m.pollStates[nodeID] = state
	}
	return state
}

func (m *MultiHostTestOrchestrator) randomJitter(limit time.Duration) time.Duration {
	if limit <= 0 {
		return 0
	}
	m.randMu.Lock()
	defer m.randMu.Unlock()
	return time.Duration(m.randSource.Int63n(int64(limit) + 1))
}

func (m *MultiHostTestOrchestrator) logVerboseMetrics(nodeID string, client *SptAPIClient, parseErr error) {
	if client == nil {
		return
	}
	payload, err := client.GetJSONMetricsVerbose()
	if err != nil {
		logging.LogDebug("metrics-poll", "failed to fetch verbose metrics payload", "node", nodeID, "error", err.Error(), "parse_error", parseErr.Error())
		return
	}
	head := truncateForLog(payload, metricsPayloadPreviewLen)
	logging.LogDebug("metrics-poll", "metrics payload (verbose)",
		"node", nodeID,
		"len", len(payload),
		"head", head,
		"parse_error", parseErr.Error())
}

func (m *MultiHostTestOrchestrator) warnCompatibilityOnce(err error) {
	if err == nil {
		return
	}
	m.compatOnce.Do(func() {
		msg := fmt.Sprintf("Incompatible metrics JSON from one or more nodes: %v. This spt build requires metrics_schema >= 2. Please upgrade the Spt deployment.", err)
		logging.LogError("metrics-poll", "metrics compatibility error", err)
		sink := m.messageSink
		if sink != nil {
			sink(msg)
			return
		}
		if m.onError != nil {
			m.onError(msg)
			return
		}
		if m.onOutput != nil {
			m.onOutput(msg)
		}
	})
}

// StartMetricsPolling begins concurrent metrics collection from all ready hosts
func (m *MultiHostTestOrchestrator) StartMetricsPolling(ctx context.Context) {
	if m.pollingCancel != nil {
		// Already polling
		return
	}

	// Create cancellable context for polling
	pollingCtx, cancel := context.WithCancel(ctx)
	m.pollingCancel = cancel

	logging.LogInfo("metrics-poll", "starting metrics polling",
		"interval", m.pollInterval.String(), "hosts", len(m.multiHost.GetReadyHosts()))
	// Start the polling loop in a goroutine
	go m.metricsPollingLoop(pollingCtx)
}

// StopMetricsPolling stops the metrics collection
func (m *MultiHostTestOrchestrator) StopMetricsPolling() {
	if m.pollingCancel != nil {
		m.pollingCancel()
		m.pollingCancel = nil
		logging.LogInfo("metrics-poll", "stopped metrics polling")
	}
}

// metricsPollingLoop runs the main metrics collection loop
func (m *MultiHostTestOrchestrator) metricsPollingLoop(ctx context.Context) {
	ticker := time.NewTicker(m.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			update := m.collectAllMetrics(ctx)
			if update != nil && m.onMetrics != nil {
				m.onMetrics(update)
			}
			if update != nil && shouldSignalCompletionForExpectedSteps(update.Aggregated, m.expectedStepIDs) {
				m.signalCompletion()
			}
		case <-ctx.Done():
			return
		}
	}
}

// shouldSignalCompletion reports whether an aggregated metrics sample indicates
// the distributed test has truly finished and the headless coordinator should
// stop.
//
// The engine reports test_state=Completed whenever operations have started but
// the instantaneous concurrency is 0. The asynchronous Netty S3 driver hits that
// transiently between operations at low thread counts, so a single Completed
// sample is not a reliable end-of-test signal: in headless mode it caused
// duration-bounded runs to abort within seconds of starting. For a time-bounded
// run we therefore only honor completion once the configured duration has
// actually elapsed (StepTime carries the engine-reported elapsed_time_seconds).
// Op-count and unbounded runs are guarded by the engine-side fix and keep the
// prior trust-the-state behavior here.
func shouldSignalCompletion(agg *PerformanceMetric) bool {
	if agg == nil || agg.TestState < constants.TestStateCompleted {
		return false
	}
	if agg.HasLimit && agg.LimitType == constants.LimitTypeTime && agg.LimitTimeSec > 0 {
		return agg.StepTime >= float64(agg.LimitTimeSec)
	}
	// For op-count-bounded runs, require the fleet to have reached 100% completion
	// before trusting the Completed state. CompletionPercent is normalization-safe
	// across N nodes (it is the slowest node's progress); comparing a summed
	// SuccessCount against a per-node LimitOpCount would be wrong once aggregated.
	if agg.HasLimit && agg.LimitType == constants.LimitTypeOpCount {
		return agg.CompletionPercent >= 100
	}
	return true
}

func shouldSignalCompletionForExpectedSteps(agg *PerformanceMetric, expectedStepIDs []string) bool {
	if len(expectedStepIDs) == 0 {
		return shouldSignalCompletion(agg)
	}
	if agg == nil {
		return false
	}
	finalStepID := expectedStepIDs[len(expectedStepIDs)-1]
	if finalStepID == "" || agg.StepID != finalStepID {
		return false
	}
	return shouldSignalCompletion(agg)
}

// collectAllMetrics polls all ready hosts and creates a MultiNodeMetricsUpdate
func (m *MultiHostTestOrchestrator) collectAllMetrics(ctx context.Context) *MultiNodeMetricsUpdate {
	readyHosts := m.multiHost.GetReadyHosts()
	if len(readyHosts) == 0 {
		return nil
	}

	// Poll all nodes concurrently
	results := m.pollNodesConcurrently(ctx, readyHosts)

	// Aggregate the per-node primary metrics (existing behavior)
	aggregated := m.aggregator.Aggregate(results.Metrics)

	// Aggregate per-op-type across all nodes: collect every metric from every node
	// into a single slice so AggregateByOpType can group by OpType.
	var allMetrics []*PerformanceMetric
	for _, nodeMetrics := range results.AllMetrics {
		allMetrics = append(allMetrics, nodeMetrics...)
	}
	var perOpType map[string]*PerformanceMetric
	if len(allMetrics) > 0 {
		combined, perOp := AggregateByOpType(allMetrics)
		perOpType = perOp
		// If AggregateByOpType produced a combined metric (MIXED), use it as the
		// aggregated metric instead so the headline numbers reflect all op types.
		if combined != nil && len(perOp) > 1 {
			aggregated = combined
		}
	}

	return &MultiNodeMetricsUpdate{
		Timestamp:  time.Now(),
		Aggregated: aggregated,
		PerNode:    results.Metrics,
		PerOpType:  perOpType,
		NodeStatus: results.Status,
	}
}

// PollResults holds the results of concurrent node polling
type PollResults struct {
	Metrics    map[string]*PerformanceMetric
	AllMetrics map[string][]*PerformanceMetric // All op-type metrics per node
	Status     map[string]NodeConnectionStatus
}

// pollNodesConcurrently polls metrics from all hosts in parallel
func (m *MultiHostTestOrchestrator) pollNodesConcurrently(ctx context.Context, hosts []*HostConnection) PollResults {
	results := PollResults{
		Metrics:    make(map[string]*PerformanceMetric),
		AllMetrics: make(map[string][]*PerformanceMetric),
		Status:     make(map[string]NodeConnectionStatus),
	}

	if len(hosts) == 0 {
		return results
	}

	pollCtx, cancel := context.WithTimeout(ctx, m.metricsPoller.(*APIMetricsPoller).timeout)
	defer cancel()

	type nodeResult struct {
		nodeID       string
		metrics      *PerformanceMetric
		allMetrics   []*PerformanceMetric
		err          error
		skipped      bool
		apiReachable bool
		phase        NodePhase
	}

	resultsChan := make(chan nodeResult, len(hosts))

	for _, host := range hosts {
		nodeID := host.Info.Original
		state := m.getPollState(nodeID)
		now := time.Now()
		if !state.shouldPoll(now) {
			logging.LogDebug("metrics-poll", "skipping node poll due to backoff", "node", nodeID)
			resultsChan <- nodeResult{
				nodeID:  nodeID,
				metrics: nil,
				err:     state.lastErrorSnapshot(),
				skipped: true,
				phase:   host.GetPhase(),
			}
			continue
		}

		go func(h *HostConnection, st *nodePollState, nodeID string) {
			var (
				metrics      *PerformanceMetric
				allMetrics   []*PerformanceMetric
				err          error
				apiReachable bool
				phase        = h.GetPhase()
			)
			kind := pollErrorNone

			if h.APIClient != nil && h.GetStatus() == HostStatusRunning {
				logging.LogDebug("metrics-poll", "polling node", "node", nodeID)
				jsonData, apiErr := h.APIClient.GetJSONMetrics()
				if apiErr != nil {
					err = apiErr
					kind = classifyPollError(apiErr)
				} else if len(jsonData) == 0 {
					err = fmt.Errorf("empty metrics response")
					kind = pollErrorRetryable
					apiReachable = true
				} else {
					apiReachable = true
					allMetrics, err = h.APIClient.ParseJSONMetrics(jsonData)
					if err != nil {
						kind = classifyPollError(err)
						if m.logJSONBodies {
							head := truncateForLog(jsonData, metricsPayloadPreviewLen)
							logging.LogDebug("metrics-poll", "raw json metrics",
								"node", nodeID,
								"len", len(jsonData),
								"head", head,
							)
							if kind == pollErrorParse {
								m.logVerboseMetrics(nodeID, h.APIClient, err)
							}
						}
					} else if len(allMetrics) > 0 {
						// Use the first (latest) metric for the existing per-node pipeline.
						metrics = allMetrics[0]
					}
				}
			} else {
				err = fmt.Errorf("host %s not running or no API client", nodeID)
				kind = pollErrorFatal
			}

			nowResult := time.Now()
			if err == nil && metrics != nil {
				st.recordSuccess(nowResult)
			} else if err != nil {
				delay, warn := st.recordFailure(nowResult, err, kind, m.backoffCfg, m.randomJitter)
				if warn {
					logging.LogWarn("metrics-poll", "node metrics polling has been failing", "node", nodeID, "error", err.Error())
				} else {
					logging.LogDebug("metrics-poll", "node metrics poll failed", "node", nodeID, "error", err.Error(), "next_delay", delay.String())
				}
			}

			if err == nil && metrics != nil {
				phase = NodePhaseMetricsFlowing
				h.SetPhase(phase)
			} else if err != nil {
				switch {
				case apiReachable:
					phase = NodePhaseAPIReady
				case h.APIClient != nil:
					phase = NodePhaseContainerStarting
				case h.GetStatus() == HostStatusReady:
					phase = NodePhaseContacted
				default:
					phase = NodePhasePending
				}
				h.SetPhase(phase)
			}

			resultsChan <- nodeResult{
				nodeID:       nodeID,
				metrics:      metrics,
				allMetrics:   allMetrics,
				err:          err,
				skipped:      false,
				apiReachable: apiReachable,
				phase:        phase,
			}
		}(host, state, nodeID)
	}

collectLoop:
	for i := 0; i < len(hosts); i++ {
		select {
		case result := <-resultsChan:
			now := time.Now()

			if result.skipped {
				phase := result.phase
				if phase == "" {
					phase = NodePhasePending
				}
				results.Status[result.nodeID] = NodeConnectionStatus{
					LastSeen:    now,
					IsConnected: false,
					IsActive:    false,
					Error:       result.err,
					Phase:       phase,
				}
				continue
			}

			if result.err == nil && result.metrics != nil {
				isActive := result.metrics.TestState == constants.TestStateRunning
				results.Metrics[result.nodeID] = result.metrics
				if len(result.allMetrics) > 0 {
					results.AllMetrics[result.nodeID] = result.allMetrics
				}
				results.Status[result.nodeID] = NodeConnectionStatus{
					LastSeen:    now,
					IsConnected: true,
					IsActive:    isActive,
					Error:       nil,
					Phase:       result.phase,
				}
				logging.LogMetricsParsing("polled node metrics", "node", result.nodeID,
					"ops_per_sec", result.metrics.OpsPerSec, "mib_per_sec", result.metrics.MiBPerSec,
					"success", result.metrics.SuccessCount)
			} else {
				results.Status[result.nodeID] = NodeConnectionStatus{
					LastSeen:    now,
					IsConnected: result.apiReachable,
					IsActive:    false,
					Error:       result.err,
					Phase:       result.phase,
				}
				if result.err != nil {
					if errors.Is(result.err, ErrMetricsIncompatible) {
						m.warnCompatibilityOnce(result.err)
					}
				}
			}
		case <-pollCtx.Done():
			break collectLoop
		}
	}

	return results
}

// StartTest starts monitoring tests on all hosts
func (m *MultiHostTestOrchestrator) StartTest(ctx context.Context, image string, params scenario.ScenarioParams) error {
	return m.StartTestWithLaunchHooks(ctx, image, params, LaunchHooks{})
}

// StartTestWithLaunchHooks starts a host-orchestrated test and reports its API
// submission through explicit orchestration hooks.
func (m *MultiHostTestOrchestrator) StartTestWithLaunchHooks(
	ctx context.Context, image string, params scenario.ScenarioParams, hooks LaunchHooks,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var err error
	params, err = scenario.PrepareExternalItemFiles(params)
	if err != nil {
		return err
	}
	readyHosts := m.multiHost.GetReadyHosts()

	if len(readyHosts) == 0 {
		return fmt.Errorf("no ready hosts available to start tests")
	}

	// Determine if this is a single-host or multi-host test
	if len(readyHosts) == 1 {
		scenarioContent, err := m.generateScenario(params)
		if err != nil {
			return fmt.Errorf("failed to generate scenario content: %w", err)
		}
		defaultsContent, err := m.generateDefaults(params)
		if err != nil {
			return fmt.Errorf("failed to generate defaults: %w", err)
		}
		return m.StartTestWithContentAndLaunchHooks(
			ctx, image, params, []byte(scenarioContent), defaultsContent, hooks)

	} else { //nolint:revive // keep else for clarity here
		// Special-case: LIST workload is intentionally single-host for now.
		// Behavior: run the workload only on the PRIMARY (first) host; start API-only
		// containers on remaining hosts so they appear in the live view as idle.
		if strings.EqualFold(params.WorkloadType, scenario.WorkloadTypeList) {
			submitted := false
			failBeforeSubmission := func(cause error) error {
				if cause == nil || submitted {
					return cause
				}
				return errors.Join(cause, m.multiHost.cleanupManagedContainersAfterStartFailure(ctx))
			}
			startupArgs, err := scenario.BuildEngineStartupArgs(params)
			if err != nil {
				return fmt.Errorf("resolve engine startup settings: %w", err)
			}
			// Status/message hooks
			if m.onStatusUpdate != nil {
				m.onStatusUpdate(&TestStatus{
					State:   constants.StateRunning,
					Message: fmt.Sprintf("LIST workload on primary only; %d worker(s) idle", len(readyHosts)-1),
				})
			}
			if m.onOutput != nil {
				hostList := make([]string, len(readyHosts))
				for i, host := range readyHosts {
					hostList[i] = host.Info.Original
				}
				m.onOutput(fmt.Sprintf("LIST workload is single-host; starting primary %s; keeping workers idle: %s",
					readyHosts[0].Info.Original, strings.Join(hostList[1:], ", ")))
			}

			// 1) Start API-only containers on all workers so they can report idle status.
			workers := readyHosts[1:]
			for _, w := range workers {
				if err := ctx.Err(); err != nil {
					return failBeforeSubmission(err)
				}
				if w.DockerManager == nil {
					continue
				}
				w.SetPhase(NodePhaseContainerStarting)
				cid, err := startContainerInNodeModeContext(
					ctx, w.DockerManager, image, m.multiHost.apiPort, m.multiHost.networkMode, startupArgs)
				if err != nil {
					// Record but continue; non-critical for primary execution
					w.SetError(err)
					logging.LogError("orchestrator", "failed to start idle worker API container", err, "host", w.Info.Original)
					continue
				}
				w.mu.Lock()
				w.ContainerID = cid
				w.Status = HostStatusRunning
				w.mu.Unlock()
				w.SetManaged(true)
			}

			// 2) Wait for APIs on any running hosts (likely just workers for now)
			if err := m.waitForAPIs(ctx, constants.APIReadinessTimeout); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return failBeforeSubmission(ctxErr)
				}
				// Not fatal to the primary run; continue but log
				logging.LogError("orchestrator", "API readiness wait (workers) returned error", err)
			}

			// Emit a baseline so the UI shows all configured nodes before metrics flow.
			if m.onMetrics != nil {
				if baseline := m.BuildBaselineUpdate(); baseline != nil {
					m.onMetrics(baseline)
				}
			}
			if err := ctx.Err(); err != nil {
				return failBeforeSubmission(err)
			}

			// 3) Generate scenario and start ENTRY container on primary (no worker addrs)
			scenarioContent, err := m.generateScenario(params)
			if err != nil {
				return failBeforeSubmission(fmt.Errorf("failed to generate scenario: %w", err))
			}
			if err := ctx.Err(); err != nil {
				return failBeforeSubmission(err)
			}
			m.multiHost.scenarioContent = scenarioContent

			additionalArgs, err := m.buildEndpointArgs(params)
			if err != nil {
				return failBeforeSubmission(fmt.Errorf("failed to build endpoint args: %w", err))
			}
			if err := ctx.Err(); err != nil {
				return failBeforeSubmission(err)
			}
			additionalArgs = append(additionalArgs, startupArgs...)

			primary := readyHosts[0]
			if primary.DockerManager == nil {
				return failBeforeSubmission(fmt.Errorf(
					"primary host %s has no Docker manager configured", primary.Info.Original))
			}
			primary.SetPhase(NodePhaseContainerStarting)
			cid, err := startEntryNodeContainerContext(
				ctx, primary.DockerManager, image, nil, additionalArgs, m.multiHost.networkMode)
			if err != nil {
				return failBeforeSubmission(err)
			}
			primary.mu.Lock()
			primary.ContainerID = cid
			primary.Status = HostStatusRunning
			primary.mu.Unlock()
			primary.SetManaged(true)
			if err := ctx.Err(); err != nil {
				return failBeforeSubmission(err)
			}

			// 4) Ensure APIs ready on all running nodes (now includes primary)
			if err := m.waitForAPIs(ctx, constants.APIReadinessTimeout); err != nil {
				return failBeforeSubmission(fmt.Errorf("failed waiting for APIs after LIST startup: %w", err))
			}

			if len(m.multiHost.hosts) > 0 {
				if entry := m.multiHost.hosts[0]; entry != nil && entry.APIClient != nil {
					entry.APIClient.LogReadySnapshot("pre-start")
				}
			}
			if err := ctx.Err(); err != nil {
				return failBeforeSubmission(err)
			}

			// 5) Start test via entry node API
			if primary.APIClient == nil {
				return failBeforeSubmission(fmt.Errorf("primary host %s API client not initialized", primary.Info.Original))
			}
			defaultsContent, derr := m.generateDefaults(params)
			if derr != nil {
				return failBeforeSubmission(fmt.Errorf("failed to generate defaults: %w", derr))
			}
			if err := ctx.Err(); err != nil {
				return failBeforeSubmission(err)
			}
			submission, submitErr := primary.APIClient.StartTestContext(
				ctx, []byte(m.multiHost.scenarioContent), defaultsContent, params.RunID)
			if submission.Submission == SubmissionUnknown {
				hooks.NotifySubmissionUnknown()
				return errors.Join(
					fmt.Errorf("LIST submission remains ambiguous after POST /run: %w", submitErr),
					m.multiHost.cleanupAmbiguousSubmission(ctx, hooks),
				)
			}
			if submission.Submission == SubmissionNotSubmitted {
				return failBeforeSubmission(fmt.Errorf("failed to start LIST via entry node API: %w", submitErr))
			}
			submitted = true
			notifyAcceptedSubmission(hooks, submitErr)
			if submitErr != nil {
				return fmt.Errorf("LIST submission was confirmed after POST /run returned an error: %w", submitErr)
			}
			runID := submission.RunID
			if m.onOutput != nil {
				m.onOutput(fmt.Sprintf("LIST started on primary with run ID: %s", runID))
			}

			// 6) Start polling metrics
			m.StartMetricsPolling(ctx)

			// 7) Start entry-node log relay to surface Spt stdout in headless/TUI
			if len(m.multiHost.hosts) > 0 {
				entry := m.multiHost.hosts[0]
				if entry != nil && entry.DockerManager != nil && entry.ContainerID != "" {
					var relay *EntryLogRelay
					mode := ""
					if dm, ok := entry.DockerManager.(*DockerManager); ok {
						if dm.client != nil {
							fetcher := newSDKLogFetcher(dm, entry.ContainerID)
							relay = NewEntryLogRelay(fetcher, true, 0)
							mode = entryLogRelayModeStream
						} else if dm.remote != nil {
							fetcher := newRemoteLogFetcher(dm.remote.ops, entry.ContainerID)
							relay = NewEntryLogRelay(fetcher, false, constants.APIPollingTimeout)
							mode = entryLogRelayModePoll
						}
					}
					if relay != nil {
						if m.multiHost != nil && len(m.multiHost.hosts) > 0 {
							logging.LogInfo("entry-log-relay", "starting entry log relay",
								"host", m.multiHost.hosts[0].Info.Original,
								"mode", mode)
						}
						sink := m.messageSink
						if sink == nil {
							sink = func(s string) {
								if m.multiHost != nil && m.multiHost.notifier != nil {
									m.multiHost.notifier(s)
									return
								}
								if m.onOutput != nil {
									m.onOutput(s)
								}
							}
						}
						relay.Start(ctx, sink)
						m.entryRelay = relay
					}
				}
			}
			return nil
		}
		// Multi-host distributed test - use RMI coordination
		if m.onStatusUpdate != nil {
			m.onStatusUpdate(&TestStatus{
				State:   constants.StateRunning,
				Message: fmt.Sprintf("Multi-host distributed test running on %d hosts", len(readyHosts)),
			})
		}

		if m.onOutput != nil {
			hostList := make([]string, len(readyHosts))
			for i, host := range readyHosts {
				hostList[i] = host.Info.Original
			}
			m.onOutput(fmt.Sprintf("Starting distributed test on hosts: %s", strings.Join(hostList, ", ")))
		}

		// Use the full distributed test flow with RMI coordination
		err = m.multiHost.StartDistributedTest(ctx, image, params)
		if err != nil {
			return errors.Join(err, m.multiHost.cleanupManagedContainersAfterStartFailure(ctx))
		}
		defaultsContent, derr := m.generateDefaults(params)
		if derr != nil {
			return errors.Join(
				fmt.Errorf("failed to generate defaults: %w", derr),
				m.multiHost.cleanupManagedContainersAfterStartFailure(ctx),
			)
		}
		return m.startEntryAPIRun(
			ctx, image, params, []byte(m.multiHost.scenarioContent), defaultsContent,
			"Distributed test", hooks)
	}
}

// StartTestWithContent starts a test using caller-provided scenario and
// defaults content.
func (m *MultiHostTestOrchestrator) StartTestWithContent(ctx context.Context, image string, params scenario.ScenarioParams, scenarioContent, defaultsContent []byte) error {
	return m.StartTestWithContentAndLaunchHooks(
		ctx, image, params, scenarioContent, defaultsContent, LaunchHooks{})
}

// StartTestWithContentAndLaunchHooks starts caller-provided content and reports
// the accepted API submission through explicit orchestration hooks.
func (m *MultiHostTestOrchestrator) StartTestWithContentAndLaunchHooks(
	ctx context.Context,
	image string,
	params scenario.ScenarioParams,
	scenarioContent, defaultsContent []byte,
	hooks LaunchHooks,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	readyHosts := m.multiHost.GetReadyHosts()
	if len(readyHosts) == 0 {
		return fmt.Errorf("no ready hosts available to start tests")
	}
	if len(scenarioContent) == 0 {
		return fmt.Errorf("scenario content is empty")
	}
	startupArgs, err := scenario.BuildEngineStartupArgs(params)
	if err != nil {
		return fmt.Errorf("resolve engine startup settings: %w", err)
	}
	m.multiHost.itemFileMounts = params.ItemFileMounts
	if len(readyHosts) != 1 {
		if m.onStatusUpdate != nil {
			m.onStatusUpdate(&TestStatus{
				State:   constants.StateRunning,
				Message: fmt.Sprintf("Multi-host distributed replay running on %d hosts", len(readyHosts)),
			})
		}
		if m.onOutput != nil {
			hostList := make([]string, len(readyHosts))
			for i, host := range readyHosts {
				hostList[i] = host.Info.Original
			}
			m.onOutput(fmt.Sprintf("Starting distributed replay on hosts: %s", strings.Join(hostList, ", ")))
		}

		if err := m.multiHost.StartDistributedTestWithContent(ctx, image, params, scenarioContent); err != nil {
			return errors.Join(err, m.multiHost.cleanupManagedContainersAfterStartFailure(ctx))
		}
		return m.startEntryAPIRun(
			ctx, image, params, scenarioContent, defaultsContent, "Distributed replay", hooks)
	}

	host := readyHosts[0]
	if host.DockerManager == nil {
		return fmt.Errorf("host %s has no Docker manager configured", host.Info.Original)
	}
	runtimeImage := image
	if scenario.IsIntegrityWorkload(params) {
		evidence, identityErr := m.multiHost.PrepareDistributedIntegrityRuntimeIdentity(ctx, image)
		if identityErr != nil {
			return identityErr
		}
		// Bind container creation to the immutable ID that preflight inspected;
		// never resolve the caller's possibly mutable tag a second time.
		runtimeImage = evidence.ImageID
	}

	if m.onStatusUpdate != nil {
		m.onStatusUpdate(&TestStatus{
			State:   constants.StateRunning,
			Message: "Single-host test running",
		})
	}
	if m.onOutput != nil {
		m.onOutput(fmt.Sprintf("Starting single-host test on %s", host.Info.Original))
	}

	m.multiHost.scenarioContent = string(scenarioContent)

	if err := m.multiHost.StartContainers(ctx, runtimeImage, startupArgs); err != nil {
		return errors.Join(err, m.multiHost.cleanupManagedContainersAfterStartFailure(ctx))
	}
	if err := m.waitForAPIs(ctx, constants.APIReadinessTimeout); err != nil {
		return errors.Join(
			fmt.Errorf("failed waiting for single-host API: %w", err),
			m.multiHost.cleanupManagedContainersAfterStartFailure(ctx),
		)
	}

	if m.onMetrics != nil {
		if baseline := m.BuildBaselineUpdate(); baseline != nil {
			m.onMetrics(baseline)
		}
	}

	if host.APIClient == nil {
		return errors.Join(
			fmt.Errorf("host %s API client not initialized", host.Info.Original),
			m.multiHost.cleanupManagedContainersAfterStartFailure(ctx),
		)
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, m.multiHost.cleanupManagedContainersAfterStartFailure(ctx))
	}
	if scenario.IsIntegrityWorkload(params) {
		if err := m.multiHost.VerifyRunningIntegrityRuntimeIdentity(ctx); err != nil {
			return errors.Join(err, m.multiHost.cleanupManagedContainersAfterStartFailure(ctx))
		}
		if err := host.APIClient.VerifyIntegrityCapabilityContext(ctx, image); err != nil {
			return errors.Join(err, m.multiHost.cleanupManagedContainersAfterStartFailure(ctx))
		}
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, m.multiHost.cleanupManagedContainersAfterStartFailure(ctx))
	}
	host.APIClient.LogReadySnapshot("pre-start")

	if m.onOutput != nil {
		m.onOutput("Starting test via host API...")
	}
	submission, submitErr := host.APIClient.StartTestContext(ctx, scenarioContent, defaultsContent, params.RunID)
	if submission.Submission == SubmissionUnknown {
		hooks.NotifySubmissionUnknown()
		return errors.Join(
			fmt.Errorf("host API submission remains ambiguous after POST /run: %w", submitErr),
			m.multiHost.cleanupAmbiguousSubmission(ctx, hooks),
		)
	}
	if submission.Submission == SubmissionNotSubmitted {
		return errors.Join(
			fmt.Errorf("failed to start test via host API: %w", submitErr),
			m.multiHost.cleanupManagedContainersAfterStartFailure(ctx),
		)
	}
	notifyAcceptedSubmission(hooks, submitErr)
	if submitErr != nil {
		return fmt.Errorf("host API submission was confirmed after POST /run returned an error: %w", submitErr)
	}
	runID := submission.RunID
	if m.onOutput != nil {
		m.onOutput(fmt.Sprintf("Single-host test started with run ID: %s", runID))
	}

	m.StartMetricsPolling(ctx)
	m.startEntryLogRelay(ctx)
	return nil
}

func (m *MultiHostTestOrchestrator) startEntryAPIRun(
	ctx context.Context,
	image string,
	params scenario.ScenarioParams,
	scenarioContent, defaultsContent []byte,
	runLabel string,
	hooks LaunchHooks,
) error {
	// Wait for APIs to be ready on all running hosts (ensures API clients set)
	if err := m.waitForAPIs(ctx, constants.APIReadinessTimeout); err != nil {
		return errors.Join(
			fmt.Errorf("failed waiting for APIs after distributed start: %w", err),
			m.multiHost.cleanupManagedContainersAfterStartFailure(ctx),
		)
	}

	if m.onMetrics != nil {
		if baseline := m.BuildBaselineUpdate(); baseline != nil {
			m.onMetrics(baseline)
		}
	}

	if len(m.multiHost.hosts) > 0 {
		if entry := m.multiHost.hosts[0]; entry != nil && entry.APIClient != nil {
			entry.APIClient.LogReadySnapshot("pre-start")
		}
	}

	// Start the distributed test via the entry node API (first host is entry)
	if len(m.multiHost.hosts) == 0 || m.multiHost.hosts[0].APIClient == nil {
		return errors.Join(
			fmt.Errorf("entry node API client not initialized"),
			m.multiHost.cleanupManagedContainersAfterStartFailure(ctx),
		)
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, m.multiHost.cleanupManagedContainersAfterStartFailure(ctx))
	}
	if scenario.IsIntegrityWorkload(params) {
		if err := m.multiHost.VerifyRunningIntegrityRuntimeIdentity(ctx); err != nil {
			return errors.Join(err, m.multiHost.cleanupManagedContainersAfterStartFailure(ctx))
		}
		if err := m.multiHost.hosts[0].APIClient.VerifyIntegrityCapabilityContext(ctx, image); err != nil {
			return errors.Join(err, m.multiHost.cleanupManagedContainersAfterStartFailure(ctx))
		}
	}
	if err := ctx.Err(); err != nil {
		return errors.Join(err, m.multiHost.cleanupManagedContainersAfterStartFailure(ctx))
	}
	if m.onOutput != nil {
		m.onOutput("Starting test via entry node API...")
	}
	submission, submitErr := m.multiHost.hosts[0].APIClient.StartTestContext(
		ctx, scenarioContent, defaultsContent, params.RunID)
	if submission.Submission == SubmissionUnknown {
		hooks.NotifySubmissionUnknown()
		return errors.Join(
			fmt.Errorf("entry node API submission remains ambiguous after POST /run: %w", submitErr),
			m.multiHost.cleanupAmbiguousSubmission(ctx, hooks),
		)
	}
	if submission.Submission == SubmissionNotSubmitted {
		return errors.Join(
			fmt.Errorf("failed to start test via entry node API: %w", submitErr),
			m.multiHost.cleanupManagedContainersAfterStartFailure(ctx),
		)
	}
	notifyAcceptedSubmission(hooks, submitErr)
	if submitErr != nil {
		return fmt.Errorf("entry node submission was confirmed after POST /run returned an error: %w", submitErr)
	}
	runID := submission.RunID
	if m.onOutput != nil {
		m.onOutput(fmt.Sprintf("%s started with run ID: %s", runLabel, runID))
	}

	m.StartMetricsPolling(ctx)
	m.startEntryLogRelay(ctx)
	return nil
}

func (m *MultiHostTestOrchestrator) startEntryLogRelay(ctx context.Context) {
	if m.multiHost == nil || len(m.multiHost.hosts) == 0 {
		return
	}
	entry := m.multiHost.hosts[0]
	if entry == nil || entry.DockerManager == nil || entry.ContainerID == "" {
		return
	}
	var relay *EntryLogRelay
	mode := ""
	if dm, ok := entry.DockerManager.(*DockerManager); ok {
		if dm.client != nil {
			fetcher := newSDKLogFetcher(dm, entry.ContainerID)
			relay = NewEntryLogRelay(fetcher, true, 0)
			mode = entryLogRelayModeStream
		} else if dm.remote != nil {
			fetcher := newRemoteLogFetcher(dm.remote.ops, entry.ContainerID)
			relay = NewEntryLogRelay(fetcher, false, constants.APIPollingTimeout)
			mode = entryLogRelayModePoll
		}
	}
	if relay == nil {
		return
	}
	logging.LogInfo("entry-log-relay", "starting entry log relay",
		"host", entry.Info.Original,
		"mode", mode)
	sink := m.messageSink
	if sink == nil {
		sink = func(s string) {
			if m.multiHost != nil && m.multiHost.notifier != nil {
				m.multiHost.notifier(s)
				return
			}
			if m.onOutput != nil {
				m.onOutput(s)
			}
		}
	}
	relay.Start(ctx, sink)
	m.entryRelay = relay
}

// StopTest stops monitoring tests on all hosts
func (m *MultiHostTestOrchestrator) StopTest() error {
	if m.onOutput != nil {
		m.onOutput("Stopping multi-host test monitoring...")
	}

	// Stop metrics polling first
	m.StopMetricsPolling()

	// Stop entry log relay if running
	if m.entryRelay != nil {
		m.entryRelay.Stop()
		m.entryRelay = nil
	}

	// The actual container stopping is handled by MultiHostOrchestrator.StopAllContainers()
	// This just stops the monitoring

	if m.onStatusUpdate != nil {
		m.onStatusUpdate(&TestStatus{
			State:   constants.StateCompleted,
			Message: "Multi-host test monitoring stopped",
		})
	}
	m.signalCompletion()

	return nil
}

func (m *MultiHostTestOrchestrator) signalCompletion() {
	m.completionOnce.Do(func() {
		close(m.completionCh)
	})
}

// CompletionCh returns a channel that closes when orchestration is complete.
func (m *MultiHostTestOrchestrator) CompletionCh() <-chan struct{} {
	return m.completionCh
}

// attachWorkerNode marks a prestarted worker node as ready for orchestration.
func (o *MultiHostOrchestrator) attachWorkerNode(ctx context.Context, host *HostConnection) error {
	if host.DockerManager == nil {
		return fmt.Errorf("host %s has no Docker manager configured", host.Info.Original)
	}

	if o.detectAdvIP == nil {
		return fmt.Errorf("no advertised IP detector configured")
	}

	ctx, cancel := context.WithTimeout(normalizeContext(ctx), constants.AdvertisedIPDetectionTimeout)
	defer cancel()
	ip, derr := o.detectAdvIP(ctx, host.Info)
	if derr != nil {
		return fmt.Errorf("advertised IP detection failed on %s: %w", host.Info.Original, derr)
	}
	if strings.TrimSpace(ip) == "" {
		return fmt.Errorf("advertised IP detection returned an empty address on %s", host.Info.Original)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	advIP := strings.TrimSpace(ip)

	host.mu.Lock()
	host.AdvertisedIP = advIP
	host.ContainerID = ""
	host.Status = HostStatusRunning
	host.mu.Unlock()
	host.SetManaged(false)

	logging.LogInfo("orchestrator", "attached worker node",
		"host", host.Info.Host,
		"rmi_hostname", advIP)

	return nil
}

// startWorkerNode starts a worker node container with RMI configuration
func (o *MultiHostOrchestrator) startWorkerNode(ctx context.Context, host *HostConnection, additionalArgs []string) error {
	// Check if host has a Docker manager configured
	if host.DockerManager == nil {
		return fmt.Errorf("host %s has no Docker manager configured", host.Info.Original)
	}

	// Determine advertised IP for java.rmi.server.hostname
	// Prefer detection; fall back to SSH host if detection fails
	advIP := ""
	if o.detectAdvIP == nil {
		return fmt.Errorf("no advertised IP detector configured")
	}
	ctx, cancel := context.WithTimeout(normalizeContext(ctx), constants.AdvertisedIPDetectionTimeout)
	defer cancel()
	ip, derr := o.detectAdvIP(ctx, host.Info)
	if derr != nil {
		return fmt.Errorf("advertised IP detection failed on %s: %w", host.Info.Original, derr)
	}
	if strings.TrimSpace(ip) == "" {
		return fmt.Errorf("advertised IP detection returned an empty address on %s", host.Info.Original)
	}
	advIP = strings.TrimSpace(ip)
	logging.LogInfo("orchestrator", "advertised IP selected", "host", host.Info.Original, "ip", advIP)
	if err := ctx.Err(); err != nil {
		return err
	}

	containerID, err := startWorkerNodeContainerContext(
		ctx, host.DockerManager,
		o.image,
		advIP,
		o.rmiPortStart,
		o.rmiPortCount,
		additionalArgs,
	)
	if err != nil {
		return fmt.Errorf("failed to start worker on %s: %w", host.Info.Host, err)
	}

	host.mu.Lock()
	host.ContainerID = containerID
	host.AdvertisedIP = advIP
	host.Status = HostStatusRunning
	host.mu.Unlock()

	logging.LogInfo("orchestrator", "started worker node",
		"host", host.Info.Host,
		"container", containerID[:12],
		"rmi_hostname", advIP)

	host.SetManaged(true)

	return nil
}

func (o *MultiHostOrchestrator) startEntryNodeContext(
	ctx context.Context,
	primary *HostConnection,
	workers []*HostConnection,
	params scenario.ScenarioParams,
	startupArgs []string,
) error {
	// Build worker addresses for RMI using detected advertised IPs where available
	workerAddresses := make([]string, 0, len(workers))
	for _, worker := range workers {
		hostOrIP := worker.AdvertisedIP
		if strings.TrimSpace(hostOrIP) == "" {
			hostOrIP = worker.Info.Host
		}
		addr := fmt.Sprintf("%s:%s", hostOrIP, constants.RMIRegistryPort)
		workerAddresses = append(workerAddresses, addr)
	}
	logging.LogInfo("orchestrator", "entry worker addresses", "csv", strings.Join(workerAddresses, ","))

	// Check if primary host has a Docker manager configured
	if primary.DockerManager == nil {
		return fmt.Errorf("primary host %s has no Docker manager configured", primary.Info.Original)
	}

	// Build additional arguments from scenario parameters
	additionalArgs, err := scenario.BuildEndpointArgs(params)
	if err != nil {
		return fmt.Errorf("failed to build endpoint args: %w", err)
	}
	additionalArgs = append(additionalArgs, startupArgs...)

	containerID, err := startEntryNodeContainerContext(
		ctx, primary.DockerManager,
		o.image,
		workerAddresses,
		additionalArgs,
		o.networkMode,
	)
	if err != nil {
		return fmt.Errorf("failed to start entry node: %w", err)
	}

	primary.mu.Lock()
	primary.ContainerID = containerID
	primary.Status = HostStatusRunning
	primary.mu.Unlock()
	primary.SetManaged(true)

	logging.LogInfo("orchestrator", "started entry node",
		"host", primary.Info.Host,
		"workers", strings.Join(workerAddresses, ","),
		"container", containerID[:12])

	return nil
}

const (
	distributedRuntimeIdentityTierImmutableImage  = "immutable_image"
	distributedRuntimeIdentityTierImageAndPayload = "immutable_image_and_payload"
	// RuntimeIdentityTierAvailableImage records one participant's available immutable image
	// evidence without representing a cross-host equality proof.
	RuntimeIdentityTierAvailableImage = "available_image"
)

func (o *MultiHostOrchestrator) verifyDistributedIntegrityImageIdentity(
	ctx context.Context,
	image string,
) (DistributedRuntimeIdentityEvidence, error) {
	evidence := DistributedRuntimeIdentityEvidence{ImageReference: image}
	o.mu.Lock()
	attachWorkers := o.attachWorkers
	checker := o.preflight
	recorder := o.runtimeIdentityRecorder
	selectedTier := o.runtimeIdentityTier
	o.mu.Unlock()
	switch selectedTier {
	case "", constants.IntegrityRuntimeIdentityTierImage:
		evidence.Tier = distributedRuntimeIdentityTierImmutableImage
	case constants.IntegrityRuntimeIdentityTierPayload:
		evidence.Tier = distributedRuntimeIdentityTierImageAndPayload
	default:
		return evidence, fmt.Errorf("distributed verification runtime identity tier %q is invalid", selectedTier)
	}
	if attachWorkers {
		return evidence, fmt.Errorf("distributed verification cannot attach existing workers because their runtime image identity is not provable")
	}
	identityChecker, ok := checker.(preflight.ImageIdentityChecker)
	if !ok {
		return evidence, fmt.Errorf("distributed verification requires an immutable image identity checker")
	}
	hosts := o.GetReadyHosts()
	if len(hosts) < 1 {
		return evidence, fmt.Errorf("remote verification image identity requires at least one ready participant")
	}
	type identityResult struct {
		index    int
		identity preflight.ImageIdentity
		err      error
	}
	resultsCh := make(chan identityResult, len(hosts))
	for index, host := range hosts {
		go func(index int, host *HostConnection) {
			identity, err := identityChecker.InspectImageIdentity(ctx, host.Info, image)
			resultsCh <- identityResult{index: index, identity: identity, err: err}
		}(index, host)
	}
	identities := make([]preflight.ImageIdentity, len(hosts))
	var identityErrors []error
	for range hosts {
		result := <-resultsCh
		if result.err != nil {
			identityErrors = append(identityErrors, fmt.Errorf("%s: %w", hosts[result.index].Info.Original, result.err))
			continue
		}
		identities[result.index] = result.identity
	}
	if len(identityErrors) > 0 {
		return evidence, fmt.Errorf("distributed verification image identity unavailable: %w", errors.Join(identityErrors...))
	}
	evidence.ImageID = identities[0].ID
	for index, identity := range identities {
		evidence.Participants = append(evidence.Participants, DistributedRuntimeIdentityParticipant{
			Host:        runtimeIdentityHostKey(hosts[index].Info),
			ImageID:     identity.ID,
			RepoDigests: append([]string(nil), identity.RepoDigests...),
		})
		if identity.ID != evidence.ImageID {
			return evidence, fmt.Errorf(
				"distributed verification image identity mismatch: %s uses %s but %s uses %s",
				hosts[0].Info.Original,
				evidence.ImageID,
				hosts[index].Info.Original,
				identity.ID,
			)
		}
	}
	if selectedTier == constants.IntegrityRuntimeIdentityTierPayload {
		payloadChecker, ok := checker.(preflight.PayloadIdentityChecker)
		if !ok {
			return evidence, fmt.Errorf("distributed verification payload tier requires a payload identity checker")
		}
		type payloadResult struct {
			index  int
			digest string
			err    error
		}
		payloadResults := make(chan payloadResult, len(hosts))
		for index, host := range hosts {
			go func(index int, host *HostConnection) {
				digest, err := payloadChecker.InspectPayloadIdentity(ctx, host.Info, evidence.ImageID)
				payloadResults <- payloadResult{index: index, digest: digest, err: err}
			}(index, host)
		}
		payloadDigests := make([]string, len(hosts))
		var payloadErrors []error
		for range hosts {
			result := <-payloadResults
			if result.err != nil {
				payloadErrors = append(payloadErrors, fmt.Errorf("%s: %w", hosts[result.index].Info.Original, result.err))
				continue
			}
			payloadDigests[result.index] = result.digest
		}
		if len(payloadErrors) > 0 {
			return evidence, fmt.Errorf("distributed verification payload identity unavailable: %w", errors.Join(payloadErrors...))
		}
		evidence.PayloadSHA256 = payloadDigests[0]
		for index, digest := range payloadDigests {
			evidence.Participants[index].PayloadSHA256 = digest
			if digest != evidence.PayloadSHA256 {
				return evidence, fmt.Errorf(
					"distributed verification payload identity mismatch: %s uses %s but %s uses %s",
					hosts[0].Info.Original,
					evidence.PayloadSHA256,
					hosts[index].Info.Original,
					digest,
				)
			}
		}
	}
	if recorder != nil {
		recorder(evidence)
	}
	o.notifyf("Integrity runtime identity: tier=%s image_id=%s payload_sha256=%s participants=%d",
		evidence.Tier, evidence.ImageID, evidence.PayloadSHA256, len(evidence.Participants))
	return evidence, nil
}

type runningIntegrityParticipant struct {
	host        *HostConnection
	hostKey     string
	label       string
	containerID string
	prepared    DistributedRuntimeIdentityParticipant
}

func reconcileRunningIntegrityParticipants(
	current *DistributedRuntimeIdentityEvidence,
	hosts []*HostConnection,
	executionParticipantKeys []string,
	minHosts int,
) ([]runningIntegrityParticipant, error) {
	preparedByHost := make(map[string]DistributedRuntimeIdentityParticipant, len(current.Participants))
	for _, participant := range current.Participants {
		hostKey := strings.TrimSpace(participant.Host)
		if hostKey == "" {
			return nil, fmt.Errorf("prepared runtime identity evidence contains an empty participant host")
		}
		if _, duplicate := preparedByHost[hostKey]; duplicate {
			return nil, fmt.Errorf("prepared runtime identity evidence contains duplicate host %q", hostKey)
		}
		if participant.ImageID != current.ImageID {
			return nil, fmt.Errorf(
				"prepared runtime identity for %s uses image %s, expected %s",
				hostKey, participant.ImageID, current.ImageID)
		}
		preparedByHost[hostKey] = participant
	}

	hostsByKey := make(map[string]*HostConnection, len(hosts))
	for _, host := range hosts {
		if host == nil {
			continue
		}
		hostKey := runtimeIdentityHostKey(host.Info)
		if hostKey == "" {
			return nil, fmt.Errorf("running identity verification found a participant without host identity")
		}
		if _, duplicate := hostsByKey[hostKey]; duplicate {
			return nil, fmt.Errorf("running identity verification found duplicate host %q", hostKey)
		}
		hostsByKey[hostKey] = host
	}

	lockedTopology := len(executionParticipantKeys) > 0
	participantKeys := append([]string(nil), executionParticipantKeys...)
	if !lockedTopology {
		for _, host := range hosts {
			if host != nil && host.GetStatus() == HostStatusRunning {
				participantKeys = append(participantKeys, runtimeIdentityHostKey(host.Info))
			}
		}
	}
	if len(participantKeys) == 0 {
		return nil, fmt.Errorf("running identity verification found no running participants")
	}
	if len(participantKeys) < minHosts {
		return nil, fmt.Errorf(
			"running identity verification found %d participants, below min-hosts %d",
			len(participantKeys), minHosts)
	}

	running := make([]runningIntegrityParticipant, 0, len(participantKeys))
	seenRunning := make(map[string]struct{}, len(participantKeys))
	for _, hostKey := range participantKeys {
		host := hostsByKey[hostKey]
		if host == nil {
			return nil, fmt.Errorf("configured execution participant %q is unavailable", hostKey)
		}
		label := hostKey
		if host.Info != nil && strings.TrimSpace(host.Info.Original) != "" {
			label = host.Info.Original
		}
		if _, duplicate := seenRunning[hostKey]; duplicate {
			return nil, fmt.Errorf("execution topology contains duplicate host %q", hostKey)
		}
		seenRunning[hostKey] = struct{}{}
		prepared, expected := preparedByHost[hostKey]
		if !expected {
			return nil, fmt.Errorf(
				"running participant %s was not present in prepared runtime identity evidence", label)
		}
		host.mu.Lock()
		containerID := host.ContainerID
		status := host.Status
		apiClient := host.APIClient
		host.mu.Unlock()
		if status != HostStatusRunning {
			return nil, fmt.Errorf("configured execution participant %s is not running (status=%s)", label, status)
		}
		if lockedTopology && apiClient == nil {
			return nil, fmt.Errorf("configured execution participant %s has no ready API proof", label)
		}
		running = append(running, runningIntegrityParticipant{
			host: host, hostKey: hostKey, label: label, containerID: containerID, prepared: prepared,
		})
	}
	return running, nil
}

// VerifyRunningIntegrityRuntimeIdentity reconciles prepared image evidence against the exact
// containers that will execute the scenario, then rechecks payload bytes when that tier is
// selected. It must run after start and before API submission.
func (o *MultiHostOrchestrator) VerifyRunningIntegrityRuntimeIdentity(ctx context.Context) error {
	o.mu.Lock()
	selectedTier := o.runtimeIdentityTier
	checker := o.preflight
	recorder := o.runtimeIdentityRecorder
	var current *DistributedRuntimeIdentityEvidence
	if o.runtimeIdentityEvidence != nil {
		snapshot := *o.runtimeIdentityEvidence
		snapshot.Participants = append(
			[]DistributedRuntimeIdentityParticipant(nil), o.runtimeIdentityEvidence.Participants...)
		current = &snapshot
	}
	executionParticipantKeys := append([]string(nil), o.executionParticipantKeys...)
	minHosts := o.minHosts
	o.mu.Unlock()
	if selectedTier != "" && selectedTier != constants.IntegrityRuntimeIdentityTierImage &&
		selectedTier != constants.IntegrityRuntimeIdentityTierPayload {
		return fmt.Errorf("running verification runtime identity tier %q is invalid", selectedTier)
	}
	if current == nil || current.ImageID == "" {
		return fmt.Errorf("running identity verification requires prepared immutable-image evidence")
	}
	running, err := reconcileRunningIntegrityParticipants(
		current, o.hosts, executionParticipantKeys, minHosts)
	if err != nil {
		return err
	}
	updated := *current
	updated.Participants = make([]DistributedRuntimeIdentityParticipant, 0, len(running))
	for _, actual := range running {
		participant := actual.prepared
		participant.RepoDigests = append([]string(nil), participant.RepoDigests...)
		updated.Participants = append(updated.Participants, participant)
	}
	if selectedTier != constants.IntegrityRuntimeIdentityTierPayload {
		o.mu.Lock()
		o.runtimeIdentityEvidence = &updated
		o.mu.Unlock()
		if recorder != nil {
			recorder(updated)
		}
		o.notifyf("Integrity running image identity reconciled: image_id=%s participants=%d",
			updated.ImageID, len(updated.Participants))
		return nil
	}
	if current.PayloadSHA256 == "" {
		return fmt.Errorf("running payload verification requires prepared immutable-image payload evidence")
	}
	runningChecker, ok := checker.(preflight.RunningPayloadIdentityChecker)
	if !ok {
		return fmt.Errorf("payload tier requires a running-container payload identity checker")
	}
	for _, participant := range running {
		if strings.TrimSpace(participant.containerID) == "" {
			return fmt.Errorf("running participant %s has no container identity", participant.label)
		}
	}

	type payloadResult struct {
		hostKey string
		label   string
		digest  string
		err     error
	}
	results := make(chan payloadResult, len(running))
	for _, participant := range running {
		go func(participant runningIntegrityParticipant) {
			digest, err := runningChecker.InspectRunningPayloadIdentity(
				ctx, participant.host.Info, participant.containerID)
			results <- payloadResult{
				hostKey: participant.hostKey, label: participant.label, digest: digest, err: err,
			}
		}(participant)
	}
	digestsByHost := make(map[string]string, len(running))
	var failures []error
	for range running {
		result := <-results
		if result.err != nil {
			failures = append(failures, fmt.Errorf("%s: %w", result.label, result.err))
			continue
		}
		if result.digest != current.PayloadSHA256 {
			failures = append(failures, fmt.Errorf(
				"running payload identity mismatch on %s: image payload %s, running payload %s",
				result.label, current.PayloadSHA256, result.digest))
			continue
		}
		digestsByHost[result.hostKey] = result.digest
	}
	if len(failures) > 0 {
		return fmt.Errorf("running integrity payload identity unavailable: %w", errors.Join(failures...))
	}
	for index, actual := range running {
		updated.Participants[index].PayloadSHA256 = digestsByHost[actual.hostKey]
	}
	o.mu.Lock()
	o.runtimeIdentityEvidence = &updated
	o.mu.Unlock()
	if recorder != nil {
		recorder(updated)
	}
	o.notifyf("Integrity running payload identity verified: sha256=%s participants=%d",
		updated.PayloadSHA256, len(updated.Participants))
	return nil
}

// VerifyRunningIntegrityPayloadIdentity is retained for driver and extension compatibility.
// It now enforces the common running-participant image contract for both identity tiers.
func (o *MultiHostOrchestrator) VerifyRunningIntegrityPayloadIdentity(ctx context.Context) error {
	return o.VerifyRunningIntegrityRuntimeIdentity(ctx)
}

// PrepareDistributedIntegrityRuntimeIdentity runs and caches the selected distributed identity
// gate. The command layer calls this after host preflight but before starting auto-results, so a
// pre-I/O gate failure cannot leave a completion tracker waiting for an engine that never starts.
func (o *MultiHostOrchestrator) PrepareDistributedIntegrityRuntimeIdentity(
	ctx context.Context,
	image string,
) (DistributedRuntimeIdentityEvidence, error) {
	o.mu.Lock()
	if o.runtimeIdentityEvidence != nil && o.runtimeIdentityReference == image {
		evidence := *o.runtimeIdentityEvidence
		evidence.Participants = append([]DistributedRuntimeIdentityParticipant(nil), evidence.Participants...)
		o.mu.Unlock()
		return evidence, nil
	}
	o.mu.Unlock()

	evidence, err := o.verifyDistributedIntegrityImageIdentity(ctx, image)
	if err != nil {
		return evidence, err
	}
	o.mu.Lock()
	cached := evidence
	cached.Participants = append([]DistributedRuntimeIdentityParticipant(nil), evidence.Participants...)
	o.runtimeIdentityReference = image
	o.runtimeIdentityEvidence = &cached
	o.mu.Unlock()
	return evidence, nil
}

// StartDistributedTest starts a distributed test using RMI coordination
func (o *MultiHostOrchestrator) StartDistributedTest(ctx context.Context, image string, params scenario.ScenarioParams) error {
	scenarioContent, err := scenario.GenerateScenario(params)
	if err != nil {
		return fmt.Errorf("failed to generate scenario: %w", err)
	}
	return o.StartDistributedTestWithContent(ctx, image, params, []byte(scenarioContent))
}

// StartDistributedTestWithContent starts the RMI entry/worker topology with
// caller-provided scenario content for later API submission.
func (o *MultiHostOrchestrator) StartDistributedTestWithContent(ctx context.Context, image string, params scenario.ScenarioParams, scenarioContent []byte) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	o.mu.Lock()
	o.executionParticipantKeys = nil
	o.mu.Unlock()
	if len(scenarioContent) == 0 {
		return fmt.Errorf("scenario content is empty")
	}
	runtimeImage := image
	if scenario.IsIntegrityWorkload(params) {
		evidence, identityErr := o.PrepareDistributedIntegrityRuntimeIdentity(ctx, image)
		if identityErr != nil {
			return identityErr
		}
		runtimeImage = evidence.ImageID
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	startupArgs, err := scenario.BuildEngineStartupArgs(params)
	if err != nil {
		return fmt.Errorf("resolve engine startup settings: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	o.mu.Lock()
	defer o.mu.Unlock()

	o.image = runtimeImage
	attachWorkers := o.attachWorkers
	if attachWorkers && len(startupArgs) > 0 {
		return fmt.Errorf("engine startup settings cannot be applied with attached workers; start every worker with %s", startupArgs[0])
	}

	// 1. Determine roles: first host is entry node, rest are workers
	if len(o.hosts) == 0 {
		return fmt.Errorf("no hosts available")
	}

	entryNode := o.hosts[0]
	workerNodes := o.hosts[1:]
	if err := o.configureItemFileMounts(ctx, o.hosts, params.ItemFileMounts); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	logging.LogInfo("orchestrator", "starting distributed test",
		"entry_node", entryNode.Info.Host,
		"worker_count", len(workerNodes))

	// 2. Store scenario content for API submission - no file creation needed
	o.scenarioContent = string(scenarioContent)

	// 3. Start worker nodes first (they need to be ready for entry node)
	if len(workerNodes) > 0 {
		logging.LogInfo("orchestrator", "starting worker nodes")

		var wg sync.WaitGroup
		var errors []error
		var errorsMu sync.Mutex

		for _, worker := range workerNodes {
			wg.Add(1)
			go func(w *HostConnection) {
				defer wg.Done()

				var err error
				if attachWorkers {
					err = o.attachWorkerNode(ctx, w)
				} else {
					err = o.startWorkerNode(ctx, w, startupArgs)
				}

				if err != nil {
					errorsMu.Lock()
					errors = append(errors, err)
					errorsMu.Unlock()
					w.SetError(err)
				}
			}(worker)
		}
		wg.Wait()
		if err := ctx.Err(); err != nil {
			return err
		}

		// Check if enough workers started
		activeWorkers := 0
		for _, w := range workerNodes {
			if w.GetStatus() == HostStatusRunning {
				activeWorkers++
			}
		}

		if activeWorkers < (o.minHosts - 1) { // -1 for entry node
			// Collect per-worker failure details to aid triage
			var details []string
			for _, w := range workerNodes {
				if w.GetStatus() != HostStatusRunning {
					// Prefer explicit error text when available
					if err := w.GetError(); err != nil {
						details = append(details, fmt.Sprintf("%s status=%s error=%v", w.Info.Original, w.GetStatus(), err))
					} else {
						details = append(details, fmt.Sprintf("%s status=%s", w.Info.Original, w.GetStatus()))
					}
				}
			}

			msg := fmt.Sprintf("insufficient workers: %d running, %d required", activeWorkers, o.minHosts-1)
			if len(details) > 0 {
				msg = fmt.Sprintf("%s; failures: %s", msg, strings.Join(details, "; "))
			}
			return fmt.Errorf("%s", msg)
		}

		// 4. Wait for RMI registries to be ready
		logging.LogInfo("orchestrator", "waiting for RMI registries to be ready")
		startedWorkers := make([]*HostConnection, 0, activeWorkers)
		for _, worker := range workerNodes {
			if worker.GetStatus() == HostStatusRunning {
				startedWorkers = append(startedWorkers, worker)
			}
		}
		if err := o.waitForAllWorkersReadyContext(ctx, startedWorkers, constants.RMIReadinessTimeout); err != nil {
			return fmt.Errorf("workers not ready for RMI: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// 5. Start entry node with worker addresses
	logging.LogInfo("orchestrator", "starting entry node")

	activeWorkers := []*HostConnection{}
	for _, w := range workerNodes {
		if w.GetStatus() == HostStatusRunning {
			activeWorkers = append(activeWorkers, w)
		}
	}

	if err := o.startEntryNodeContext(ctx, entryNode, activeWorkers, params, startupArgs); err != nil {
		return fmt.Errorf("failed to start entry node: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	executionParticipants := make([]string, 0, 1+len(activeWorkers))
	executionParticipants = append(executionParticipants, runtimeIdentityHostKey(entryNode.Info))
	for _, worker := range activeWorkers {
		executionParticipants = append(executionParticipants, runtimeIdentityHostKey(worker.Info))
	}
	o.executionParticipantKeys = executionParticipants

	logging.LogInfo("orchestrator", "distributed test started successfully",
		"entry_node", entryNode.Info.Host,
		"active_workers", len(activeWorkers))

	return nil
}

func (o *MultiHostOrchestrator) checkPortContext(ctx context.Context, host string, port int, timeout time.Duration) bool {
	// Use net.JoinHostPort to be IPv6-safe
	address := net.JoinHostPort(host, strconv.Itoa(port))

	logging.LogDebug("orchestrator", "checking port connectivity",
		"address", address,
		"timeout", timeout)

	dialer := net.Dialer{Timeout: timeout}
	conn, err := dialer.DialContext(normalizeContext(ctx), "tcp", address)
	if err != nil {
		logging.LogDebug("orchestrator", "port connection failed",
			"address", address,
			"error", err.Error())
		return false
	}

	_ = conn.Close()
	logging.LogDebug("orchestrator", "port connection successful",
		"address", address)
	return true
}

func (o *MultiHostOrchestrator) waitForRMIReadyContext(ctx context.Context, host *HostConnection, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(normalizeContext(ctx), timeout)
	defer cancel()
	rmiRegistryPort := constants.RMIRegistryPortInt
	retryInterval := constants.RMIRetryInterval
	// Use the hostname/IP from the connection info - this is what we used for SSH
	// and what other containers will use to reach this host via RMI
	rmiAddress := host.Info.Host

	logging.LogInfo("orchestrator", "waiting for RMI registry readiness",
		"host", host.Info.Host,
		"address", rmiAddress,
		"port", rmiRegistryPort,
		"timeout", timeout)

	for {
		if o.checkPortContext(ctx, rmiAddress, rmiRegistryPort, constants.PortCheckTimeout) {
			logging.LogInfo("orchestrator", "RMI registry ready",
				"host", host.Info.Host,
				"address", rmiAddress)
			return nil
		}

		timer := time.NewTimer(retryInterval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return fmt.Errorf("RMI registry not ready on %s: %w", host.Info.Host, ctx.Err())
		case <-timer.C:
		}
	}
}

func (o *MultiHostOrchestrator) waitForAllWorkersReadyContext(ctx context.Context, workers []*HostConnection, timeout time.Duration) error {
	if len(workers) == 0 {
		logging.LogInfo("orchestrator", "no workers to check for readiness")
		return nil
	}

	logging.LogInfo("orchestrator", "checking RMI readiness for all workers",
		"worker_count", len(workers),
		"timeout", timeout)

	var wg sync.WaitGroup
	var readinessErrors []error
	var errorsMu sync.Mutex
	readyWorkers := 0
	var readyMu sync.Mutex

	for _, worker := range workers {
		wg.Add(1)
		go func(w *HostConnection) {
			defer wg.Done()

			if err := o.waitForRMIReadyContext(ctx, w, timeout); err != nil {
				errorsMu.Lock()
				readinessErrors = append(readinessErrors, fmt.Errorf("worker %s: %w", w.Info.Host, err))
				errorsMu.Unlock()
				w.SetError(err)
			} else {
				readyMu.Lock()
				readyWorkers++
				readyMu.Unlock()
			}
		}(worker)
	}

	wg.Wait()
	if err := ctx.Err(); err != nil {
		return err
	}

	logging.LogInfo("orchestrator", "worker readiness check complete",
		"ready_workers", readyWorkers,
		"total_workers", len(workers),
		"required_minimum", o.minHosts-1) // -1 for entry node

	// Check if we have enough ready workers (respecting minHosts - 1 for entry node)
	requiredWorkers := o.minHosts - 1 // Subtract 1 for the entry node
	if requiredWorkers < 0 {
		requiredWorkers = 0
	}

	if readyWorkers < requiredWorkers {
		errorDetails := ""
		if len(readinessErrors) > 0 {
			errorMessages := make([]string, len(readinessErrors))
			for i, err := range readinessErrors {
				errorMessages[i] = err.Error()
			}
			errorDetails = fmt.Sprintf(": %s", strings.Join(errorMessages, "; "))
		}

		return fmt.Errorf("insufficient ready workers: %d ready, %d required%s",
			readyWorkers, requiredWorkers, errorDetails)
	}

	if readyWorkers < len(workers) {
		logging.LogInfo("orchestrator", "continuing with partial workers",
			"ready_workers", readyWorkers,
			"total_workers", len(workers))
	}

	return nil
}
