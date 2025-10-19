/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/health"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/remoteip"
)

// RemoteDockerManager implements DockerInterface by executing docker CLI via SSH
// using the shared command layer (internal/docker/command).
// It is used for remote hosts where the Docker SDK ssh:// transport is unavailable.
type RemoteDockerManager struct {
	host        *hostparse.HostInfo
	exec        command.CommandExecutor
	ops         command.DockerOperations
	containerID string
	proberRun   func(ctx context.Context, baseURL string, pollInterval time.Duration) error
}

func sanitizeHostComponent(host string) string {
	lowered := strings.ToLower(host)
	var b strings.Builder
	lastDash := false
	for _, r := range lowered {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	result := strings.Trim(b.String(), "-")
	if result == "" {
		result = "host"
	}
	if len(result) > 40 {
		result = result[:40]
	}
	return result
}

func generateRemoteContainerName(prefix, host string) string {
	return fmt.Sprintf("spt-%s-%d-%s", prefix, time.Now().Unix(), sanitizeHostComponent(host))
}

func (m *RemoteDockerManager) baseLabels(role string) map[string]string {
	hostName := m.host.Host
	return map[string]string{
		constants.DockerLabelManaged: "true",
		constants.DockerLabelRole:    role,
		constants.DockerLabelHost:    hostName,
	}
}

// newRemoteDockerManagerWithExecutor allows tests to inject a custom executor.
func newRemoteDockerManagerWithExecutor(host *hostparse.HostInfo, exec command.CommandExecutor) (*RemoteDockerManager, error) {
	if host == nil {
		return nil, fmt.Errorf("host cannot be nil")
	}
	if exec == nil {
		exec = command.NewCommandExecutor()
	}
	return &RemoteDockerManager{
		host: host,
		exec: exec,
		ops:  command.NewDockerOperations(exec, host),
		// Default to no-op prober in tests; NewRemoteDockerManager sets a real prober.
		proberRun: func(context.Context, string, time.Duration) error { return nil },
	}, nil
}

// NewRemoteDockerManager constructs a RemoteDockerManager for the given host.
func NewRemoteDockerManager(host *hostparse.HostInfo) (*RemoteDockerManager, error) {
	mgr, err := newRemoteDockerManagerWithExecutor(host, nil)
	if err != nil {
		return nil, err
	}
	mgr.proberRun = func(ctx context.Context, baseURL string, interval time.Duration) error {
		return health.WaitForRunReady(ctx, baseURL, interval)
	}
	return mgr, nil
}

// ContainerID returns the last started container ID (if any)
func (m *RemoteDockerManager) ContainerID() string { return m.containerID }

// StartContainer is not used for remote multi-host mode; return a clear error
func (m *RemoteDockerManager) StartContainer(_ string, _ []string) (string, error) {
	return "", fmt.Errorf("StartContainer not supported for remote manager; use node/worker/entry helpers")
}

// StartContainerWithScenario is not used in remote multi-host mode.
func (m *RemoteDockerManager) StartContainerWithScenario(_ string, _ string, _ []string) (string, error) {
	return "", fmt.Errorf("StartContainerWithScenario not supported for remote manager")
}

// StartContainerInNodeMode starts a single Spt node exposing the provided API port.
func (m *RemoteDockerManager) StartContainerInNodeMode(image string, apiPort string) (string, error) {
	port := command.ParsePortFromString(apiPort)
	if port <= 0 {
		return "", fmt.Errorf("invalid api port: %q", apiPort)
	}

	name := generateRemoteContainerName("node", m.host.Host)
	cfg := command.ContainerConfig{
		Image:        image,
		Name:         name,
		NetworkMode:  command.NetworkModeBridge,
		Detached:     true,
		PortMappings: []command.PortMapping{{HostPort: port, ContainerPort: port}},
		Labels:       m.baseLabels(constants.DockerRoleNode),
		Command:      []string{"--run-node=true", "--run-port=" + apiPort},
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(constants.ContainerStartTimeoutSecs)*time.Second)
	defer cancel()

	id, _, err := m.ops.StartContainer(ctx, cfg)
	if err != nil {
		return "", err
	}
	m.containerID = strings.TrimSpace(id)
	return m.containerID, nil
}

// StartWorkerNodeContainer starts a worker in RMI mode on the remote host.
func (m *RemoteDockerManager) StartWorkerNodeContainer(image string, rmiHostname string, _ int, _ int) (string, error) {
	// Detect advertised IP on the remote host if not explicitly provided
	advIP := strings.TrimSpace(rmiHostname)
	if advIP == "" {
		ip, err := remoteip.DetectAdvertisedIP(context.Background(), m.exec, m.host)
		if err != nil {
			return "", err
		}
		advIP = ip
	}
	logging.LogInfo("remote-docker", "using advertised IP", "host", m.host.Original, "ip", advIP)

	// Host networking with explicit JVM RMI hostname and explicit ports
	name := generateRemoteContainerName("worker", m.host.Host)
	cfg := command.ContainerConfig{
		Image:       image,
		Name:        name,
		NetworkMode: command.NetworkModeHost,
		Detached:    true,
		Labels:      m.baseLabels(constants.DockerRoleWorker),
		Environment: map[string]string{
			constants.JavaOptsEnvVar:        fmt.Sprintf("%s%s", constants.JavaRMIHostnamePrefix, advIP),
			constants.JavaToolOptionsEnvVar: fmt.Sprintf("%s%s", constants.JavaRMIHostnamePrefix, advIP),
		},
		Command: []string{"--run-node=true", "--run-port=9999", "--load-step-node-port=1099"},
	}

	// Echo exact JAVA_OPTS for triage
	logging.LogInfo("remote-docker", "JAVA_OPTS configured", "host", m.host.Original, "JAVA_OPTS", cfg.Environment[constants.JavaOptsEnvVar])

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(constants.ContainerStartTimeoutSecs)*time.Second)
	defer cancel()

	id, _, err := m.ops.StartContainer(ctx, cfg)
	if err != nil {
		return "", err
	}
	m.containerID = strings.TrimSpace(id)

	// Readiness probe against remote host over network
	if m.proberRun != nil {
		baseURL := fmt.Sprintf("http://%s:%d", m.host.Host, constants.DefaultSptAPIPort)
		rctx, rcancel := context.WithTimeout(context.Background(), constants.APIReadinessTimeout)
		defer rcancel()
		if err := m.proberRun(rctx, baseURL, constants.APIPollingTimeout); err != nil {
			return m.containerID, fmt.Errorf("worker API readiness: %w", err)
		}
	}
	return m.containerID, nil
}

// StartEntryNodeContainer starts the entry node with worker addresses and extra args (e.g., S3 params).
func (m *RemoteDockerManager) StartEntryNodeContainer(image string, workerAddresses []string, additionalArgs []string) (string, error) {
	// Build command: [--load-step-node-addrs=<csv>, --run-node, --run-port=9999] + additionalArgs
	cmd := []string{}
	if len(workerAddresses) > 0 {
		cmd = append(cmd, "--load-step-node-addrs="+strings.Join(workerAddresses, ","))
	}
	cmd = append(cmd, "--run-node=true", "--run-port="+constants.SptAPIPort)
	cmd = append(cmd, additionalArgs...)

	name := generateRemoteContainerName("entry", m.host.Host)
	cfg := command.ContainerConfig{
		Image:       image,
		Name:        name,
		NetworkMode: command.NetworkModeHost,
		Detached:    true,
		Labels:      m.baseLabels(constants.DockerRoleEntry),
		Command:     cmd,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(constants.ContainerStartTimeoutSecs)*time.Second)
	defer cancel()

	id, _, err := m.ops.StartContainer(ctx, cfg)
	if err != nil {
		return "", err
	}
	m.containerID = strings.TrimSpace(id)
	if m.proberRun != nil {
		baseURL := fmt.Sprintf("http://%s:%d", m.host.Host, constants.DefaultSptAPIPort)
		rctx, rcancel := context.WithTimeout(context.Background(), constants.APIReadinessTimeout)
		defer rcancel()
		if err := m.proberRun(rctx, baseURL, constants.APIPollingTimeout); err != nil {
			return m.containerID, fmt.Errorf("entry API readiness: %w", err)
		}
	}
	return m.containerID, nil
}

// StreamOutput fetches logs once and forwards lines to callbacks (non-streaming fallback).
func (m *RemoteDockerManager) StreamOutput(containerID string, stdoutCallback func(string), stderrCallback func(string)) {
	if containerID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = m.ops.GetContainerLogs(ctx, containerID, false,
		func(line string) {
			if stdoutCallback != nil {
				stdoutCallback(line)
			}
		},
		func(line string) {
			if stderrCallback != nil {
				stderrCallback(line)
			}
		},
	)
}

// Cleanup force-removes the last started container (if any)
func (m *RemoteDockerManager) Cleanup() error {
	if m.containerID == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := m.ops.StopContainer(ctx, m.containerID); err != nil {
		return err
	}
	m.containerID = ""
	return nil
}

// Close is a no-op for the remote manager
func (m *RemoteDockerManager) Close() {}
