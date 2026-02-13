/*
Copyright © 2025 Dell Technologies
*/

package verification

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/preflight"
)

// NewVerifier creates a new verifier instance with default implementations
func NewVerifier(hosts []*hostparse.HostInfo, config Config) *Verifier {
	return NewVerifierWithDeps(hosts, config, command.NewCommandExecutor(), &RealNetworkChecker{}, &RealTimeProvider{})
}

// NewVerifierWithDeps creates a new verifier instance with custom dependencies (for testing)
func NewVerifierWithDeps(hosts []*hostparse.HostInfo, config Config, cmdExecutor command.CommandExecutor, netChecker NetworkChecker, timeProvider TimeProvider) *Verifier {
	return &Verifier{
		hosts:        hosts,
		config:       config,
		results:      make(map[string]*NodeResult),
		cmdExecutor:  cmdExecutor,
		netChecker:   netChecker,
		timeProvider: timeProvider,
		preflight:    preflight.NewCheckerWithExecutor(cmdExecutor),
	}
}

// NewVerifierWithDepsAndPreflight allows injecting a custom preflight.Checker (tests/mocks)
func NewVerifierWithDepsAndPreflight(hosts []*hostparse.HostInfo, config Config, cmdExecutor command.CommandExecutor, netChecker NetworkChecker, timeProvider TimeProvider, pf preflight.Checker) *Verifier {
	if pf == nil {
		pf = preflight.NewCheckerWithExecutor(cmdExecutor)
	}
	return &Verifier{
		hosts:        hosts,
		config:       config,
		results:      make(map[string]*NodeResult),
		cmdExecutor:  cmdExecutor,
		netChecker:   netChecker,
		timeProvider: timeProvider,
		preflight:    pf,
	}
}

// VerifyAll performs verification on all hosts and returns a report
func (v *Verifier) VerifyAll() *Report {
	startTime := v.timeProvider.Now()

	logging.LogInfo("verifier", "starting verification",
		"host_count", len(v.hosts),
		"min_hosts", v.config.MinHosts,
		"network_mode", v.config.NetworkMode)

	// Verify all nodes in parallel
	type hostResult struct {
		host   *hostparse.HostInfo
		result *NodeResult
	}

	resultChan := make(chan hostResult, len(v.hosts))
	var wg sync.WaitGroup

	// Start verification for each host in parallel
	for _, host := range v.hosts {
		wg.Add(1)
		go func(h *hostparse.HostInfo) {
			defer wg.Done()
			result := v.verifyNode(h)
			resultChan <- hostResult{host: h, result: result}
		}(host)
	}

	// Wait for all verifications to complete
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Collect results
	for hr := range resultChan {
		v.results[hr.host.Host] = hr.result

		// For multi-host verification, show completion messages
		if len(v.hosts) > 1 {
			if hr.result.Overall.Passed {
				fmt.Printf("  ✅ %s verification complete\n", hr.host.Host)
			} else {
				fmt.Printf("  ❌ %s verification failed (%s)\n", hr.host.Host, hr.result.Overall.Message)
			}
		}
	}

	// Add blank line after multi-host progress messages
	if len(v.hosts) > 1 {
		fmt.Println()
	}

	// Generate report
	report := &Report{
		Results:  v.results,
		Duration: v.timeProvider.Since(startTime),
		Config:   v.config,
	}

	report.assess()
	return report
}

// verifyNode performs all checks on a single node
func (v *Verifier) verifyNode(host *hostparse.HostInfo) *NodeResult {
	result := &NodeResult{Host: host.Host, HostInfo: host}
	logging.LogInfo("verifier", "verifying node", "host", host.Host)

	if v.stepSSHConnectivity(host, result) {
		return result
	}
	if !host.IsLocal && len(v.hosts) > 1 {
		v.stepClockSkew(host, result)
	}
	if v.stepDockerDaemon(host, result) {
		return result
	}
	if v.stepImagePull(host, result) {
		return result
	}
	if v.stepPortConflicts(host, result) {
		return result
	}
	if v.stepStartContainer(host, result) {
		return result
	}
	if v.config.UseRdma {
		if v.stepRdmaDevice(host, result) {
			v.stepCleanup(host, result)
			return result
		}
		v.stepRdmaDriver(host, result)
		v.stepRdmaReadiness(host, result)
	}
	v.stepWaitForServices(result)
	v.stepVerifyPorts(host, result)
	v.stepCheckEndpoints(host, result)
	v.stepCleanup(host, result)

	result.Overall = v.assessNode(result)
	return result
}

func (v *Verifier) stepSSHConnectivity(host *hostparse.HostInfo, result *NodeResult) bool {
	if v.config.ShowProgress {
		if host.IsLocal {
			fmt.Print("  ⏳ Checking local Docker connectivity...")
		} else {
			fmt.Print("  ⏳ Checking SSH connectivity...")
		}
	}
	result.SSHConnectivity = v.checkSSHDocker(host)
	if !result.SSHConnectivity.Passed {
		if v.config.ShowProgress {
			fmt.Println(" ❌")
		}
		result.Overall = result.SSHConnectivity
		return true
	}
	if v.config.ShowProgress {
		fmt.Println(" ✅")
	}
	return false
}

func (v *Verifier) stepClockSkew(host *hostparse.HostInfo, result *NodeResult) {
	if v.config.ShowProgress {
		fmt.Print("  ⏳ Checking clock synchronization...")
	}
	result.ClockSkew = v.checkClockSkew(host)
	if v.config.ShowProgress {
		if result.ClockSkew.Passed {
			fmt.Println(" ✅")
		} else {
			fmt.Println(" ⚠️")
		}
	}
}

// checkClockSkew compares the remote host's clock to the local clock.
// SigV4 authentication requires clocks within ~15 minutes; we warn at 1 minute.
func (v *Verifier) checkClockSkew(host *hostparse.HostInfo) Check {
	start := v.timeProvider.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stdout, _, err := v.cmdExecutor.ExecuteCommand(ctx, host, []string{"date", "-u", "+%s"})
	if err != nil {
		return Check{
			Passed:   true, // Don't fail verification if we can't check
			Message:  "Could not read remote clock",
			Duration: v.timeProvider.Since(start),
			Error:    err,
		}
	}

	remoteEpoch := strings.TrimSpace(stdout)
	var remoteTime int64
	if _, err := fmt.Sscanf(remoteEpoch, "%d", &remoteTime); err != nil {
		return Check{
			Passed:   true,
			Message:  fmt.Sprintf("Could not parse remote clock: %s", remoteEpoch),
			Duration: v.timeProvider.Since(start),
			Error:    err,
		}
	}

	localTime := v.timeProvider.Now().Unix()
	skew := localTime - remoteTime
	if skew < 0 {
		skew = -skew
	}

	skewDuration := time.Duration(skew) * time.Second

	const warnThreshold = 60 // 1 minute — well before SigV4's 15 min limit

	if skew > warnThreshold {
		return Check{
			Passed:   false,
			Message:  fmt.Sprintf("Clock skew: %s behind entry node (S3 SigV4 auth requires <15m)", skewDuration),
			Duration: v.timeProvider.Since(start),
		}
	}

	return Check{
		Passed:   true,
		Message:  fmt.Sprintf("Clock offset: %s", skewDuration),
		Duration: v.timeProvider.Since(start),
	}
}

func (v *Verifier) stepDockerDaemon(host *hostparse.HostInfo, result *NodeResult) bool {
	if v.config.ShowProgress {
		fmt.Print("  ⏳ Verifying Docker daemon...")
	}
	result.DockerAvailable = v.checkDockerDaemon(host)
	if !result.DockerAvailable.Passed {
		if v.config.ShowProgress {
			fmt.Println(" ❌")
		}
		result.Overall = Check{Passed: false, Message: "Docker not available"}
		return true
	}
	if v.config.ShowProgress {
		fmt.Println(" ✅")
	}
	return false
}

func (v *Verifier) stepImagePull(host *hostparse.HostInfo, result *NodeResult) bool {
	if v.config.ShowProgress {
		fmt.Print("  ⏳ Pulling latest Spt Docker image...")
	}
	result.DockerImagePull = v.pullDockerImage(host)
	if !result.DockerImagePull.Passed {
		if v.config.ShowProgress {
			fmt.Println(" ❌")
		}
		result.Overall = Check{Passed: false, Message: "Cannot pull Docker image"}
		return true
	}
	if v.config.ShowProgress {
		fmt.Println(" ✅")
	}
	return false
}

func (v *Verifier) stepPortConflicts(host *hostparse.HostInfo, result *NodeResult) bool {
	if v.config.ShowProgress {
		fmt.Print("  ⏳ Checking for port conflicts...")
	}
	conflictCheck := v.checkForPortConflicts(host)
	if !conflictCheck.Passed {
		if v.config.ShowProgress {
			fmt.Println(" ❌")
		}
		result.Overall = conflictCheck
		return true
	}
	if v.config.ShowProgress {
		fmt.Println(" ✅")
	}
	return false
}

func (v *Verifier) stepStartContainer(host *hostparse.HostInfo, result *NodeResult) bool {
	if v.config.ShowProgress {
		fmt.Print("  ⏳ Starting Spt container...")
	}
	result.ContainerStart = v.startNodeContainer(host, result)
	if !result.ContainerStart.Passed {
		if v.config.ShowProgress {
			fmt.Println(" ❌")
		}
		result.Overall = Check{Passed: false, Message: "Cannot start container"}
		return true
	}
	if v.config.ShowProgress {
		fmt.Println(" ✅")
	}
	return false
}

func (v *Verifier) stepWaitForServices(result *NodeResult) {
	if result.containerID == "" {
		return
	}
	if v.config.ShowProgress {
		fmt.Print("  ⏳ Waiting for services to initialize (10s)...")
	}
	time.Sleep(constants.ServiceInitializationSecs * time.Second)
	if v.config.ShowProgress {
		fmt.Println(" ✅")
	}
}

func (v *Verifier) stepVerifyPorts(host *hostparse.HostInfo, result *NodeResult) {
	if v.config.ShowProgress {
		fmt.Printf("  ⏳ Verifying network ports (1099, %d)...", v.config.APIPort)
	}
	result.PortsAccessible = v.checkPorts(host)
	if v.config.ShowProgress {
		if result.PortsAccessible.Passed {
			fmt.Println(" ✅")
		} else {
			fmt.Println(" ❌")
		}
	}
}

func (v *Verifier) stepCheckEndpoints(host *hostparse.HostInfo, result *NodeResult) {
	if result.containerID == "" {
		return
	}
	if v.config.ShowProgress {
		fmt.Print("  ⏳ Testing REST API endpoints...")
	}
	result.MetricsEndpoint = v.checkMetricsEndpoint(host)
	result.ControlEndpoint = v.checkControlEndpoint(host)
	if v.config.ShowProgress {
		if result.MetricsEndpoint.Passed && result.ControlEndpoint.Passed {
			fmt.Println(" ✅")
		} else {
			fmt.Println(" ❌")
		}
	}
}

func (v *Verifier) stepCleanup(host *hostparse.HostInfo, result *NodeResult) {
	if v.config.ShowProgress {
		fmt.Print("  ⏳ Cleaning up test container...")
	}
	result.ContainerCleanup = v.stopContainer(host, result.containerID)
	if result.ContainerCleanup.Passed {
		dockerOps := command.NewDockerOperations(v.cmdExecutor, host)
		v.cleanupVerifyContainers(host, dockerOps)
	}
	if v.config.ShowProgress {
		if result.ContainerCleanup.Passed {
			fmt.Println(" ✅")
		} else {
			fmt.Println(" ❌")
		}
	}
}

func (v *Verifier) stepRdmaDevice(host *hostparse.HostInfo, result *NodeResult) bool {
	if v.config.ShowProgress {
		fmt.Print("  ⏳ Checking RDMA device availability...")
	}
	result.RdmaDevice = v.checkRdmaDevice(host)
	if !result.RdmaDevice.Passed {
		if v.config.ShowProgress {
			fmt.Println(" ❌")
		}
		result.Overall = Check{Passed: false, Message: "RDMA device not available"}
		return true
	}
	if v.config.ShowProgress {
		fmt.Println(" ✅")
	}
	return false
}

func (v *Verifier) stepRdmaDriver(host *hostparse.HostInfo, result *NodeResult) {
	if v.config.ShowProgress {
		fmt.Print("  ⏳ Checking RDMA driver in container...")
	}
	result.RdmaDriver = v.checkRdmaDriver(host, result.containerID)
	if v.config.ShowProgress {
		if result.RdmaDriver.Passed {
			fmt.Println(" ✅")
		} else {
			fmt.Println(" ⚠️")
		}
	}
}

func (v *Verifier) stepRdmaReadiness(host *hostparse.HostInfo, result *NodeResult) {
	if v.config.ShowProgress {
		fmt.Print("  ⏳ Probing RDMA device readiness in container...")
	}
	result.RdmaReadiness = v.checkRdmaReadiness(host, result.containerID)
	if v.config.ShowProgress {
		if result.RdmaReadiness.Passed {
			fmt.Println(" ✅")
		} else {
			fmt.Println(" ⚠️")
		}
	}
}

// checkRdmaReadiness verifies that the RDMA device is accessible and active
// from inside the container by reading /sys/class/infiniband/.  This proves
// the full stack: device mapped in, kernel driver loaded, and port link up.
func (v *Verifier) checkRdmaReadiness(host *hostparse.HostInfo, containerID string) Check {
	start := v.timeProvider.Now()

	if containerID == "" {
		return Check{
			Passed:   false,
			Message:  "No container to check RDMA readiness",
			Duration: v.timeProvider.Since(start),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Discover RDMA devices visible inside the container via sysfs
	stdout, _, err := v.cmdExecutor.ExecuteCommand(ctx, host, []string{
		"docker", "exec", containerID, "ls", "/sys/class/infiniband/",
	})
	if err != nil || strings.TrimSpace(stdout) == "" {
		return Check{
			Passed:   false,
			Message:  "No RDMA devices found in container (/sys/class/infiniband/ empty or missing)",
			Duration: v.timeProvider.Since(start),
			Error:    err,
		}
	}

	device := strings.Fields(stdout)[0]

	// Read port state — expect "4: ACTIVE"
	portState, _, err := v.cmdExecutor.ExecuteCommand(ctx, host, []string{
		"docker", "exec", containerID, "cat", fmt.Sprintf("/sys/class/infiniband/%s/ports/1/state", device),
	})
	if err != nil {
		return Check{
			Passed:   false,
			Message:  fmt.Sprintf("Cannot read port state for %s", device),
			Duration: v.timeProvider.Since(start),
			Error:    err,
		}
	}
	portState = strings.TrimSpace(portState)

	if !strings.Contains(portState, "ACTIVE") {
		return Check{
			Passed:   false,
			Message:  fmt.Sprintf("RDMA port not active: %s (device %s)", portState, device),
			Duration: v.timeProvider.Since(start),
		}
	}

	// Read link layer (Ethernet = RoCE, InfiniBand = IB)
	linkLayer, _, _ := v.cmdExecutor.ExecuteCommand(ctx, host, []string{
		"docker", "exec", containerID, "cat", fmt.Sprintf("/sys/class/infiniband/%s/ports/1/link_layer", device),
	})
	linkLayer = strings.TrimSpace(linkLayer)

	return Check{
		Passed:   true,
		Message:  fmt.Sprintf("%s port 1 %s (%s)", device, portState, linkLayer),
		Duration: v.timeProvider.Since(start),
	}
}

// checkRdmaDevice verifies that RDMA hardware is accessible on the host
func (v *Verifier) checkRdmaDevice(host *hostparse.HostInfo) Check {
	start := v.timeProvider.Now()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Check for /dev/infiniband/ uverbs devices
	stdout, _, err := v.cmdExecutor.ExecuteCommand(ctx, host, []string{"ls", constants.RdmaDevicePath})
	if err != nil {
		return Check{
			Passed:   false,
			Message:  fmt.Sprintf("RDMA device directory %s not found", constants.RdmaDevicePath),
			Duration: v.timeProvider.Since(start),
			Error:    err,
		}
	}

	if !strings.Contains(stdout, "uverbs") {
		return Check{
			Passed:   false,
			Message:  fmt.Sprintf("No uverbs devices found in %s", constants.RdmaDevicePath),
			Duration: v.timeProvider.Since(start),
		}
	}

	return Check{
		Passed:   true,
		Message:  fmt.Sprintf("RDMA devices found: %s", strings.TrimSpace(stdout)),
		Duration: v.timeProvider.Since(start),
	}
}

// checkRdmaDriver verifies the container has RDMA runtime libraries
func (v *Verifier) checkRdmaDriver(host *hostparse.HostInfo, containerID string) Check {
	start := v.timeProvider.Now()

	if containerID == "" {
		return Check{
			Passed:   false,
			Message:  "No container to check RDMA driver",
			Duration: v.timeProvider.Since(start),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Use ldconfig to check whether libibverbs is loadable inside the container.
	// This is distro-agnostic — it works regardless of whether the library lives
	// under /usr/lib64 (RHEL/SUSE), /usr/lib/x86_64-linux-gnu (Debian/Ubuntu),
	// or any other path the dynamic linker knows about.
	//
	// We run ldconfig -p without a shell pipe and check the output in Go to
	// avoid quoting issues between local exec and remote SSH execution.
	stdout, _, err := v.cmdExecutor.ExecuteCommand(ctx, host, []string{
		"docker", "exec", containerID, "ldconfig", "-p",
	})
	if err != nil {
		return Check{
			Passed:   false,
			Message:  "RDMA runtime library (libibverbs) not found in container",
			Duration: v.timeProvider.Since(start),
			Error:    err,
		}
	}
	if !strings.Contains(stdout, "libibverbs") {
		return Check{
			Passed:   false,
			Message:  "RDMA runtime library (libibverbs) not found in container",
			Duration: v.timeProvider.Since(start),
		}
	}

	return Check{
		Passed:   true,
		Message:  "RDMA runtime libraries present in container",
		Duration: v.timeProvider.Since(start),
	}
}

// checkSSHDocker verifies SSH connectivity and Docker access
func (v *Verifier) checkSSHDocker(host *hostparse.HostInfo) Check {
	start := v.timeProvider.Now()

	// For local, just return success
	if host.IsLocal {
		return Check{
			Passed:   true,
			Message:  "Local Docker connection",
			Duration: v.timeProvider.Since(start),
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), constants.SSHConnectTimeoutSecs*time.Second)
	defer cancel()

	sshTarget := host.GetSSHTarget()

	logging.LogDebug("verifier", "probing ssh connectivity",
		"host", host.Host,
		"ssh_target", sshTarget)

	_, stderr, err := v.cmdExecutor.ExecuteCommand(ctx, host, []string{"true"})
	if err != nil {
		msg := fmt.Sprintf("SSH connection failed for %s", sshTarget)
		if stderr != "" {
			err = fmt.Errorf("%w (stderr: %s)", err, stderr)
		}
		return Check{
			Passed:   false,
			Message:  msg,
			Duration: v.timeProvider.Since(start),
			Error:    err,
		}
	}

	return Check{
		Passed:   true,
		Message:  fmt.Sprintf("SSH connectivity established to %s", sshTarget),
		Duration: v.timeProvider.Since(start),
	}
}

// checkDockerDaemon uses SSH to verify Docker is accessible
func (v *Verifier) checkDockerDaemon(host *hostparse.HostInfo) Check {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), constants.DockerDaemonTimeoutSecs*time.Second)
	defer cancel()

	version, err := v.preflight.CheckDocker(ctx, host)
	if err != nil {
		msg := "Docker not accessible on remote host"
		if host.IsLocal {
			msg = "Local Docker not accessible"
		}
		return Check{Passed: false, Message: msg, Duration: v.timeProvider.Since(start), Error: err}
	}

	version = strings.TrimSpace(version)
	if host.IsLocal {
		return Check{Passed: true, Message: fmt.Sprintf("Local Docker %s accessible", version), Duration: v.timeProvider.Since(start)}
	}
	return Check{Passed: true, Message: fmt.Sprintf("Docker %s accessible via SSH", version), Duration: v.timeProvider.Since(start)}
}

// checkLocalDocker tests local Docker availability
// checkLocalDocker moved to test helpers (unused in production)

// checkRemoteDocker tests Docker on remote host via SSH
// checkRemoteDocker moved to test helpers (unused in production)

// startNodeContainer starts a Spt container in node mode using SSH
func (v *Verifier) startNodeContainer(host *hostparse.HostInfo, result *NodeResult) Check {
	start := v.timeProvider.Now()

	// Determine external IP
	externalIP := v.getExternalIP(host)

	// Create container name
	containerName := fmt.Sprintf("spt-verify-%d-%s", time.Now().Unix(), host.Host)
	containerName = strings.ReplaceAll(containerName, ".", "-") // Replace dots for valid name

	// Create Docker operations instance
	dockerOps := command.NewDockerOperations(v.cmdExecutor, host)

	// Remove any lingering verification containers from prior runs to avoid port conflicts.
	v.cleanupVerifyContainers(host, dockerOps)

	// Build container configuration
	var networkMode command.NetworkMode
	if v.config.NetworkMode == "host" {
		networkMode = command.NetworkModeHost
	} else {
		networkMode = command.NetworkModeBridge
	}

	// Build port mappings for bridge mode
	var portMappings []command.PortMapping
	if networkMode == command.NetworkModeBridge {
		rmiMappings := command.BuildRMIPortMappings(v.config.RMIPortStart, v.config.RMIPortCount)
		portMappings = make([]command.PortMapping, 0, 2+len(rmiMappings))
		// Standard ports
		portMappings = append(portMappings,
			command.PortMapping{HostPort: constants.DefaultRMIRegistryPort, ContainerPort: constants.DefaultRMIRegistryPort}, // RMI Registry
			command.PortMapping{HostPort: v.config.APIPort, ContainerPort: v.config.APIPort},                                 // REST API
		)

		// Add RMI object port range
		portMappings = append(portMappings, rmiMappings...)
	}

	config := command.ContainerConfig{
		Image:        constants.EffectiveSptImage(),
		Name:         containerName,
		NetworkMode:  networkMode,
		PortMappings: portMappings,
		Labels: map[string]string{
			"spt.verify":                 "true",
			"spt.verify.host":            host.Host,
			constants.DockerLabelManaged: "true",
			constants.DockerLabelRole:    constants.DockerRoleVerify,
			constants.DockerLabelHost:    host.Host,
		},
		Environment: map[string]string{
			constants.JavaOptsEnvVar: fmt.Sprintf("%s%s", constants.JavaRMIHostnamePrefix, externalIP),
		},
		Command:  []string{constants.SptNodeCommand},
		Detached: true,
	}

	// Add RDMA device passthrough when verifying RDMA support
	if v.config.UseRdma {
		config.Devices = []string{constants.RdmaDevicePath}
		config.CapAdd = []string{constants.RdmaCapIpcLock}
		config.Ulimits = []string{constants.RdmaUlimitMemlock}
	}

	// Start the container
	ctx, cancel := context.WithTimeout(context.Background(), constants.ContainerStartTimeoutSecs*time.Second)
	defer cancel()

	containerID, cmdResult, err := dockerOps.StartContainer(ctx, config)
	if err != nil {
		// Best-effort cleanup using the generated container name; docker rm accepts names or IDs.
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()

		if cleanupErr := dockerOps.StopContainer(cleanupCtx, containerName); cleanupErr != nil {
			// Ignore "No such container" errors; these indicate nothing remained to clean up.
			if cleanupErr != nil && !strings.Contains(strings.ToLower(cleanupErr.Error()), "no such container") {
				logging.LogWarn("verifier", "failed to clean up container after start error",
					"host", host.Host,
					"container", containerName,
					"error", cleanupErr.Error())
			}
		} else {
			logging.LogInfo("verifier", "cleaned up container after start error",
				"host", host.Host,
				"container", containerName)
		}

		return Check{
			Passed:        false,
			Message:       "Failed to start container",
			Duration:      v.timeProvider.Since(start),
			Error:         fmt.Errorf("container start failed: %w", err),
			CommandResult: &cmdResult,
		}
	}

	result.containerID = containerID

	logging.LogInfo("verifier", "started container",
		"host", host.Host,
		"container", containerID[:constants.ContainerIDDisplayLength],
		"external_ip", externalIP)

	return Check{
		Passed:   true,
		Message:  fmt.Sprintf("Container started: %s", containerID[:constants.ContainerIDDisplayLength]),
		Duration: v.timeProvider.Since(start),
	}
}

// checkPorts verifies that required ports are accessible
func (v *Verifier) checkPorts(host *hostparse.HostInfo) Check {
	start := v.timeProvider.Now()
	targetIP := v.getExternalIP(host)

	failed := []string{}

	// Check RMI registry
	if !v.netChecker.IsPortOpen(targetIP, constants.DefaultRMIRegistryPort, constants.PortCheckTimeoutSecs*time.Second) {
		failed = append(failed, fmt.Sprintf("%d (RMI registry)", constants.DefaultRMIRegistryPort))
	}

	// Check REST API
	if !v.netChecker.IsPortOpen(targetIP, v.config.APIPort, constants.PortCheckTimeoutSecs*time.Second) {
		failed = append(failed, fmt.Sprintf("%d (REST API)", v.config.APIPort))
	}

	// NOTE: RMI object ports (40000-40009) are only created during active load testing
	// For infrastructure verification, we only check the service ports that are always available

	if len(failed) > 0 {
		return Check{
			Passed:   false,
			Message:  fmt.Sprintf("Ports not accessible: %v", failed),
			Duration: v.timeProvider.Since(start),
		}
	}

	return Check{
		Passed:   true,
		Message:  fmt.Sprintf("Essential service ports accessible (%d RMI registry, %d REST API)", constants.DefaultRMIRegistryPort, v.config.APIPort),
		Duration: v.timeProvider.Since(start),
	}
}

// checkMetricsEndpoint verifies the /metrics/json endpoint works
func (v *Verifier) checkMetricsEndpoint(host *hostparse.HostInfo) Check {
	start := v.timeProvider.Now()
	url := fmt.Sprintf("http://%s:%d/metrics/json", v.getExternalIP(host), v.config.APIPort)

	ctx, cancel := context.WithTimeout(context.Background(), constants.HTTPRequestTimeoutSecs*time.Second)
	defer cancel()

	resp, err := v.netChecker.HTTPGet(ctx, url)
	if err != nil {
		return Check{
			Passed:   false,
			Message:  "Failed to reach /metrics/json endpoint",
			Duration: v.timeProvider.Since(start),
			Error:    err,
		}
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return Check{
			Passed:   false,
			Message:  fmt.Sprintf("Metrics endpoint returned status %d", resp.StatusCode),
			Duration: v.timeProvider.Since(start),
		}
	}

	// Try to parse the response
	var metrics interface{}
	if err := json.NewDecoder(resp.Body).Decode(&metrics); err != nil {
		return Check{
			Passed:   false,
			Message:  "Failed to parse metrics JSON",
			Duration: time.Since(start),
			Error:    err,
		}
	}

	return Check{
		Passed:   true,
		Message:  "Returns valid JSON metrics",
		Duration: time.Since(start),
	}
}

// checkControlEndpoint verifies the /run endpoint accepts control commands
func (v *Verifier) checkControlEndpoint(host *hostparse.HostInfo) Check {
	start := time.Now()
	baseURL := fmt.Sprintf("http://%s:%d", v.getExternalIP(host), v.config.APIPort)

	client := &http.Client{Timeout: 5 * time.Second}

	// Try a simple GET to /run (should return current state)
	resp, err := client.Get(baseURL + "/run")
	if err != nil {
		return Check{
			Passed:   false,
			Message:  "Failed to reach /run endpoint",
			Duration: time.Since(start),
			Error:    err,
		}
	}
	_ = resp.Body.Close()

	// Accept any response code for GET (200, 404, etc. are all valid)
	return Check{
		Passed:   true,
		Message:  "Control endpoint accessible",
		Duration: time.Since(start),
	}
}

// stopContainer stops and removes the test container
func (v *Verifier) stopContainer(host *hostparse.HostInfo, containerID string) Check {
	start := time.Now()

	if containerID == "" {
		return Check{
			Passed:   true,
			Message:  "No container to clean up",
			Duration: time.Since(start),
		}
	}

	// Create Docker operations instance
	dockerOps := command.NewDockerOperations(v.cmdExecutor, host)

	// Stop and remove container
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	err := dockerOps.StopContainer(ctx, containerID)
	if err != nil {
		// Log warning but don't fail completely - cleanup is best effort
		logging.LogWarn("verifier", "failed to remove container",
			"container", containerID[:12],
			"host", host.Host,
			"error", err.Error())

		return Check{
			Passed:   false,
			Message:  "Failed to remove container (cleanup incomplete)",
			Duration: time.Since(start),
			Error:    fmt.Errorf("container removal failed: %w", err),
		}
	}

	logging.LogInfo("verifier", "container removed",
		"host", host.Host,
		"container", containerID[:12])

	return Check{
		Passed:   true,
		Message:  "Container stopped and removed",
		Duration: time.Since(start),
	}
}

// Helper methods

func (v *Verifier) getExternalIP(host *hostparse.HostInfo) string {
	if host.IsLocal {
		return "127.0.0.1"
	}
	return host.Host
}

// isPortOpen removed as unused (dead code)

func (v *Verifier) cleanupVerifyContainers(host *hostparse.HostInfo, dockerOps command.DockerOperations) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stdout, stderr, err := v.cmdExecutor.ExecuteCommand(ctx, host, []string{
		constants.DockerCommand,
		constants.DockerCmdPS,
		constants.DockerFlagQuiet,
		constants.DockerFlagFilter, "label=spt.verify=true",
	})
	if err != nil {
		logging.LogWarn("verifier", "failed to list verification containers",
			"host", host.Host,
			"stderr", stderr,
			"error", err.Error())
		return
	}

	ids := strings.Fields(stdout)
	for _, id := range ids {
		if id == "" {
			continue
		}
		rmCtx, rmCancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := dockerOps.StopContainer(rmCtx, id)
		rmCancel()
		if err != nil {
			if !strings.Contains(strings.ToLower(err.Error()), "no such container") {
				logging.LogWarn("verifier", "failed to remove lingering verification container",
					"host", host.Host,
					"container", id,
					"error", err.Error())
			}
		} else {
			logging.LogInfo("verifier", "removed lingering verification container",
				"host", host.Host,
				"container", id)
		}
	}
}

func (v *Verifier) assessNode(result *NodeResult) Check {
	// All critical checks must pass
	criticalChecks := []Check{
		result.SSHConnectivity,
		result.DockerAvailable,
		result.ContainerStart,
		result.PortsAccessible,
		result.ContainerCleanup,
	}

	for _, check := range criticalChecks {
		if !check.Passed {
			return Check{
				Passed:  false,
				Message: check.Message,
			}
		}
	}

	// Endpoint checks are nice-to-have but not critical for basic infrastructure
	endpointIssues := []string{}
	if !result.MetricsEndpoint.Passed {
		endpointIssues = append(endpointIssues, "metrics endpoint")
	}
	if !result.ControlEndpoint.Passed {
		endpointIssues = append(endpointIssues, "control endpoint")
	}

	if len(endpointIssues) > 0 {
		return Check{
			Passed:  true, // Still pass overall
			Message: fmt.Sprintf("READY (issues with: %s)", strings.Join(endpointIssues, ", ")),
		}
	}

	return Check{
		Passed:  true,
		Message: "READY",
	}
}

// checkForPortConflicts checks for port conflicts before attempting container start
func (v *Verifier) checkForPortConflicts(host *hostparse.HostInfo) Check {
	start := v.timeProvider.Now()

	logging.LogInfo("verifier", "checking for port conflicts", "host", host.Host)

	// Check for conflicts
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conflictInfo, err := v.preflight.CheckPorts(ctx, host, v.config.APIPort, v.config.RMIPortStart, v.config.RMIPortCount)
	if err != nil {
		return Check{
			Passed:   false,
			Message:  "Failed to check for port conflicts",
			Duration: v.timeProvider.Since(start),
			Error:    fmt.Errorf("conflict detection failed: %w", err),
		}
	}

	// No conflicts - proceed
	if !conflictInfo.HasConflicts() {
		return Check{
			Passed:   true,
			Message:  "No port conflicts detected",
			Duration: v.timeProvider.Since(start),
		}
	}

	// Conflicts detected - show details to user
	conflictInfo.DisplayConflictInfo()

	// If force cleanup enabled, try to clean up matching containers
	if v.config.ForceCleanup {
		sptContainers := conflictInfo.GetSptContainers()
		if len(sptContainers) > 0 {
			fmt.Printf("\n[FORCE] Attempting to clean up %d Spt containers...\n", len(sptContainers))

			// Create docker operations to stop containers
			dockerOps := command.NewDockerOperations(v.cmdExecutor, host)

			cleanedUp := 0
			for _, container := range sptContainers {
				fmt.Printf("  Stopping container %s (%s)...\n", container.Name, container.ID[:12])
				if err := dockerOps.StopContainer(ctx, container.ID); err != nil {
					logging.LogWarn("verifier", "failed to stop container during force cleanup",
						"host", host.Host, "container", container.ID, "error", err)
					fmt.Printf("    Failed to stop %s: %s\n", container.ID[:12], err.Error())
				} else {
					cleanedUp++
				}
			}

			if cleanedUp > 0 {
				fmt.Printf("[FORCE] Successfully cleaned up %d containers. Proceeding with verification.\n", cleanedUp)
				return Check{
					Passed:   true,
					Message:  fmt.Sprintf("Port conflicts resolved by cleaning up %d containers", cleanedUp),
					Duration: v.timeProvider.Since(start),
				}
			}
		}

		// Force cleanup enabled but no matching containers or cleanup failed
		fmt.Printf("[FORCE] No matching containers found for auto-cleanup. Verification will fail.\n")
	}

	// Conflicts exist and not resolved - fail the verification
	return Check{
		Passed:   false,
		Message:  fmt.Sprintf("Port conflicts detected on ports %v", conflictInfo.ConflictPorts),
		Duration: v.timeProvider.Since(start),
		Error:    fmt.Errorf("ports %v are in use - use --force-cleanup to automatically remove Spt containers or manually stop conflicting containers", conflictInfo.ConflictPorts),
	}
}

// pullDockerImage pulls the latest Spt Docker image
func (v *Verifier) pullDockerImage(host *hostparse.HostInfo) Check {
	start := v.timeProvider.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	logging.LogInfo("verifier", "ensuring Docker image present",
		"host", host.Host,
		"image", constants.EffectiveSptImage())
	if err := v.preflight.EnsureImage(ctx, host, constants.EffectiveSptImage()); err != nil {
		logging.LogError("verifier", "Docker image ensure failed", err,
			"host", host.Host, "image", constants.EffectiveSptImage())
		return Check{Passed: false, Message: "Failed to pull Docker image", Duration: v.timeProvider.Since(start), Error: err}
	}
	logging.LogInfo("verifier", "Docker image pulled/available",
		"host", host.Host, "duration", v.timeProvider.Since(start))
	return Check{Passed: true, Message: "Docker image pulled successfully", Duration: v.timeProvider.Since(start)}
}
