/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/health"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/remoteip"
	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

const (
	remoteRemoveRecursiveFlag = "-rf"
	remoteChmodCommand        = "chmod"
	remoteCleanupTimeout      = 10 * time.Second
	remoteDiagnosticsSlack    = 30 * time.Second
)

// RemoteDockerManager implements DockerInterface by executing docker CLI via SSH
// using the shared command layer (internal/docker/command).
// It is used for remote hosts where the Docker SDK ssh:// transport is unavailable.
type RemoteDockerManager struct {
	host                *hostparse.HostInfo
	exec                command.CommandExecutor
	ops                 command.DockerOperations
	containerID         string
	proberRun           func(ctx context.Context, baseURL string, pollInterval time.Duration) error
	stagedBindMounts    []string
	stagingDir          string
	diagnosticsRoot     string
	diagnosticsDir      string
	diagnosticsRole     string
	diagnosticsDone     bool
	diagnosticsRec      *diagnosticsRecord
	diagnosticsStopDone bool
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
		constants.DockerLabelManaged: dockerLabelTrue,
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

func (m *RemoteDockerManager) cleanupStaging(ctx context.Context) error {
	if m.stagingDir == "" {
		return nil
	}
	_, stderr, err := m.exec.ExecuteCommand(ctx, m.host, []string{"rm", remoteRemoveRecursiveFlag, m.stagingDir})
	if err != nil {
		return fmt.Errorf("remove remote staging directory %s on %s: %w (stderr: %s)",
			m.stagingDir, m.host.Original, err, stderr)
	}
	m.stagingDir = ""
	m.stagedBindMounts = nil
	return nil
}

func (m *RemoteDockerManager) ensureStagingDir(ctx context.Context) error {
	if m.stagingDir != "" {
		return nil
	}
	m.stagingDir = fmt.Sprintf("/tmp/spt-items-%d", time.Now().UnixNano())
	if _, stderr, err := m.exec.ExecuteCommand(ctx, m.host, []string{"mkdir", "-p", m.stagingDir}); err != nil {
		m.stagingDir = ""
		return fmt.Errorf("create remote staging directory on %s: %w (stderr: %s)", m.host.Original, err, stderr)
	}
	return nil
}

func (m *RemoteDockerManager) setDiagnosticsResultsRoot(resultsRoot string) {
	m.diagnosticsRoot = strings.TrimSpace(resultsRoot)
}

// SetFileMounts stages local item files onto the remote Docker host for bind mounting.
func (m *RemoteDockerManager) SetFileMounts(mounts []scenario.FileMount) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(constants.ContainerStartTimeoutSecs)*time.Second)
	defer cancel()
	if err := m.cleanupStaging(ctx); err != nil {
		return err
	}
	if len(mounts) == 0 {
		return nil
	}
	if err := m.ensureStagingDir(ctx); err != nil {
		return err
	}
	for _, mount := range mounts {
		remotePath := path.Join(m.stagingDir, path.Base(mount.ContainerPath))
		if err := m.exec.CopyFile(ctx, m.host, mount.HostPath, remotePath); err != nil {
			return errors.Join(err, m.cleanupStaging(ctx))
		}
		if _, stderr, err := m.exec.ExecuteCommand(ctx, m.host, []string{remoteChmodCommand, "0444", remotePath}); err != nil {
			return errors.Join(
				fmt.Errorf("chmod remote item file on %s: %w (stderr: %s)", m.host.Original, err, stderr),
				m.cleanupStaging(ctx),
			)
		}
		m.stagedBindMounts = append(m.stagedBindMounts, fmt.Sprintf("%s:%s:ro", remotePath, mount.ContainerPath))
	}
	return nil
}

func (m *RemoteDockerManager) prepareDiagnostics(ctx context.Context, role string) ([]string, []string, error) {
	if !diagnosticsEnabled() {
		return nil, nil, nil
	}
	if err := m.ensureStagingDir(ctx); err != nil {
		return nil, nil, err
	}

	envFile, err := os.CreateTemp("", "spt-java-opts-*.env")
	if err != nil {
		return nil, nil, fmt.Errorf("create local env-file: %w", err)
	}
	envFilePath := envFile.Name()
	defer func() { _ = os.Remove(envFilePath) }()
	if _, err := fmt.Fprintf(envFile, "%s=%s\n", constants.EnvSptJavaOpts, os.Getenv(constants.EnvSptJavaOpts)); err != nil {
		_ = envFile.Close()
		return nil, nil, fmt.Errorf("write local env-file: %w", err)
	}
	if err := envFile.Close(); err != nil {
		return nil, nil, fmt.Errorf("close local env-file: %w", err)
	}

	remoteEnvFile := path.Join(m.stagingDir, diagnosticsHostComponent(role)+".env")
	if err := m.exec.CopyFile(ctx, m.host, envFilePath, remoteEnvFile); err != nil {
		return nil, nil, fmt.Errorf("stage remote env-file on %s: %w", m.host.Original, err)
	}
	if _, stderr, err := m.exec.ExecuteCommand(ctx, m.host, []string{remoteChmodCommand, "0444", remoteEnvFile}); err != nil {
		return nil, nil, fmt.Errorf("chmod remote env-file on %s: %w (stderr: %s)", m.host.Original, err, stderr)
	}

	remoteDir := path.Join(
		remoteDiagnosticsBaseDir,
		diagnosticsRunID(m.diagnosticsRoot),
		diagnosticsHostComponent(m.host.Host),
		diagnosticsHostComponent(role),
	)
	if _, stderr, err := m.exec.ExecuteCommand(ctx, m.host, []string{"mkdir", "-p", remoteDir}); err != nil {
		return nil, nil, fmt.Errorf("create remote diagnostics directory on %s: %w (stderr: %s)", m.host.Original, err, stderr)
	}
	if _, stderr, err := m.exec.ExecuteCommand(ctx, m.host, []string{remoteChmodCommand, "0777", remoteDir}); err != nil {
		return nil, nil, fmt.Errorf("chmod remote diagnostics directory on %s: %w (stderr: %s)", m.host.Original, err, stderr)
	}

	m.diagnosticsDir = remoteDir
	m.diagnosticsRole = role
	m.diagnosticsDone = false
	m.diagnosticsRec = nil
	m.diagnosticsStopDone = false
	return []string{fmt.Sprintf("%s:%s", remoteDir, dockerDiagnosticsMount)}, []string{remoteEnvFile}, nil
}

func (m *RemoteDockerManager) cleanupRemoteDiagnostics(ctx context.Context) error {
	if m.diagnosticsDir == "" {
		return nil
	}
	_, stderr, err := m.exec.ExecuteCommand(ctx, m.host, []string{"rm", remoteRemoveRecursiveFlag, m.diagnosticsDir})
	if err != nil {
		return fmt.Errorf("remove remote diagnostics directory %s on %s: %w (stderr: %s)",
			m.diagnosticsDir, m.host.Original, err, stderr)
	}
	m.diagnosticsDir = ""
	m.diagnosticsRole = ""
	m.diagnosticsDone = false
	m.diagnosticsRec = nil
	m.diagnosticsStopDone = false
	return nil
}

func (m *RemoteDockerManager) cleanupFailedStart(ctx context.Context, cause error) error {
	return errors.Join(cause, m.cleanupStaging(ctx), m.cleanupRemoteDiagnostics(ctx))
}

// StartContainer is not used for remote multi-host mode; return a clear error
func (m *RemoteDockerManager) StartContainer(_ string, _ []string) (string, error) {
	return "", fmt.Errorf("StartContainer not supported for remote manager; use node/worker/entry helpers")
}

// StartContainerWithScenario is not used in remote multi-host mode.
func (m *RemoteDockerManager) StartContainerWithScenario(_ string, _ string, _ []string) (string, error) {
	return "", fmt.Errorf("StartContainerWithScenario not supported for remote manager")
}

// StartContainerInNodeMode starts a single Spt node exposing the provided API port.
func (m *RemoteDockerManager) StartContainerInNodeMode(image string, apiPort string, networkMode string, additionalArgs []string) (string, error) {
	port := command.ParsePortFromString(apiPort)
	if port <= 0 {
		return "", fmt.Errorf("invalid api port: %q", apiPort)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(constants.ContainerStartTimeoutSecs)*time.Second)
	defer cancel()
	diagnosticBinds, envFiles, err := m.prepareDiagnostics(ctx, constants.DockerRoleNode)
	if err != nil {
		return "", m.cleanupFailedStart(ctx, err)
	}

	name := generateRemoteContainerName("node", m.host.Host)
	mode := selectedNodeNetworkMode(networkMode)
	cmd := make([]string, 0, 2+len(additionalArgs))
	cmd = append(cmd, dockerNodeModeArg, "--run-port="+apiPort)
	cmd = append(cmd, additionalArgs...)
	cfg := command.ContainerConfig{
		Image:       image,
		Name:        name,
		NetworkMode: mode,
		Detached:    true,
		Labels:      m.baseLabels(constants.DockerRoleNode),
		Command:     cmd,
		BindMounts:  append(diagnosticBinds, m.stagedBindMounts...),
		EnvFiles:    envFiles,
	}
	if mode != command.NetworkModeHost {
		cfg.PortMappings = []command.PortMapping{{HostPort: port, ContainerPort: port}}
	}

	id, _, err := m.ops.StartContainer(ctx, cfg)
	if err != nil {
		return "", m.cleanupFailedStart(ctx, err)
	}
	m.containerID = strings.TrimSpace(id)
	return m.containerID, nil
}

func selectedNodeNetworkMode(networkMode string) command.NetworkMode {
	if strings.EqualFold(strings.TrimSpace(networkMode), string(command.NetworkModeHost)) {
		return command.NetworkModeHost
	}
	return command.NetworkModeBridge
}

// StartWorkerNodeContainer starts a worker in RMI mode on the remote host.
func (m *RemoteDockerManager) StartWorkerNodeContainer(image string, rmiHostname string, _ int, _ int, additionalArgs []string) (string, error) {
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

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(constants.ContainerStartTimeoutSecs)*time.Second)
	defer cancel()
	diagnosticBinds, envFiles, err := m.prepareDiagnostics(ctx, constants.DockerRoleWorker)
	if err != nil {
		return "", m.cleanupFailedStart(ctx, err)
	}

	// Host networking with explicit JVM RMI hostname and explicit ports
	name := generateRemoteContainerName("worker", m.host.Host)
	cmd := make([]string, 0, 3+len(additionalArgs))
	cmd = append(cmd, dockerNodeModeArg, "--run-port=9999", "--load-step-node-port=1099")
	cmd = append(cmd, additionalArgs...)
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
		Command:    cmd,
		BindMounts: append(diagnosticBinds, m.stagedBindMounts...),
		EnvFiles:   envFiles,
	}

	// RDMA device passthrough when SPT_RDMA is enabled
	if constants.IsRdmaEnabled() {
		cfg.Devices = []string{constants.RdmaDevicePath}
		cfg.CapAdd = []string{constants.RdmaCapIpcLock}
		cfg.Ulimits = []string{constants.RdmaUlimitMemlock}
	}

	// Echo exact JAVA_OPTS for triage
	logging.LogInfo("remote-docker", "JAVA_OPTS configured", "host", m.host.Original, "JAVA_OPTS", cfg.Environment[constants.JavaOptsEnvVar])

	id, _, err := m.ops.StartContainer(ctx, cfg)
	if err != nil {
		return "", m.cleanupFailedStart(ctx, err)
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
func (m *RemoteDockerManager) StartEntryNodeContainer(image string, workerAddresses []string, additionalArgs []string, networkMode string) (string, error) {
	// Build command: [--load-step-node-addrs=<csv>, --run-node, --run-port=9999] + additionalArgs
	cmd := []string{}
	if len(workerAddresses) > 0 {
		cmd = append(cmd, "--load-step-node-addrs="+strings.Join(workerAddresses, ","))
	}
	cmd = append(cmd, dockerNodeModeArg, "--run-port="+constants.SptAPIPort)
	cmd = append(cmd, additionalArgs...)

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(constants.ContainerStartTimeoutSecs)*time.Second)
	defer cancel()
	diagnosticBinds, envFiles, err := m.prepareDiagnostics(ctx, constants.DockerRoleEntry)
	if err != nil {
		return "", m.cleanupFailedStart(ctx, err)
	}

	name := generateRemoteContainerName("entry", m.host.Host)
	cfg := command.ContainerConfig{
		Image:       image,
		Name:        name,
		NetworkMode: selectedNodeNetworkMode(networkMode),
		Detached:    true,
		Labels:      m.baseLabels(constants.DockerRoleEntry),
		Command:     cmd,
		BindMounts:  append(diagnosticBinds, m.stagedBindMounts...),
		EnvFiles:    envFiles,
	}

	// RDMA device passthrough when SPT_RDMA is enabled
	if constants.IsRdmaEnabled() {
		cfg.Devices = []string{constants.RdmaDevicePath}
		cfg.CapAdd = []string{constants.RdmaCapIpcLock}
		cfg.Ulimits = []string{constants.RdmaUlimitMemlock}
	}

	id, _, err := m.ops.StartContainer(ctx, cfg)
	if err != nil {
		return "", m.cleanupFailedStart(ctx, err)
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

func (m *RemoteDockerManager) collectDiagnostics(ctx context.Context) (*diagnosticsRecord, error) {
	if m.diagnosticsDir == "" {
		return nil, nil
	}
	if m.diagnosticsDone && m.diagnosticsRec != nil {
		return m.diagnosticsRec, nil
	}

	localDir := ""
	if strings.TrimSpace(m.diagnosticsRoot) != "" {
		localDir = diagnosticsLocalDir(m.diagnosticsRoot, m.host.Host, m.diagnosticsRole)
	}
	record := diagnosticsRecord{
		Host:         m.host.Original,
		Role:         m.diagnosticsRole,
		ContainerID:  m.containerID,
		ContainerDir: dockerDiagnosticsMount,
		RemoteDir:    m.diagnosticsDir,
		LocalDir:     localDir,
	}
	if localDir == "" {
		if _, stderr, err := m.exec.ExecuteCommand(ctx, m.host, []string{"rm", remoteRemoveRecursiveFlag, m.diagnosticsDir}); err != nil {
			record.Error = fmt.Sprintf("remove remote diagnostics directory without local results root: %v (stderr: %s)", err, stderr)
			record.PreservedRemoteDir = true
			m.diagnosticsRec = &record
			return &record, errors.New(record.Error)
		}
		record.RemoteDirRemoved = true
		m.diagnosticsDone = true
		m.diagnosticsRec = &record
		return &record, nil
	}
	if err := os.MkdirAll(localDir, 0o750); err != nil {
		record.Error = err.Error()
		record.PreservedRemoteDir = true
		m.diagnosticsRec = &record
		return &record, fmt.Errorf("create local diagnostics directory: %w", err)
	}

	stdout, stderr, err := m.exec.ExecuteCommand(ctx, m.host, []string{"find", m.diagnosticsDir, "-maxdepth", "1", "-type", "f", "-print"})
	if err != nil {
		record.Error = fmt.Sprintf("list remote diagnostics: %v (stderr: %s)", err, stderr)
		record.PreservedRemoteDir = true
		_ = writeDiagnosticsRecordManifest(localDir, record)
		m.diagnosticsRec = &record
		return &record, errors.New(record.Error)
	}

	remoteFiles := strings.Fields(stdout)
	for _, remoteFile := range remoteFiles {
		name := path.Base(remoteFile)
		localPath := filepath.Join(localDir, name)
		if err := m.exec.CopyFromHost(ctx, m.host, remoteFile, localPath); err != nil {
			record.Error = fmt.Sprintf("copy %s: %v", remoteFile, err)
			record.PreservedRemoteDir = true
			_ = writeDiagnosticsRecordManifest(localDir, record)
			m.diagnosticsRec = &record
			return &record, errors.New(record.Error)
		}
	}

	files, err := listDiagnosticFiles(localDir)
	if err != nil {
		record.Error = fmt.Sprintf("list local diagnostics: %v", err)
		record.PreservedRemoteDir = true
		_ = writeDiagnosticsRecordManifest(localDir, record)
		m.diagnosticsRec = &record
		return &record, errors.New(record.Error)
	}
	record.Files = files
	record.Collected = true

	if !m.diagnosticsStopDone {
		// The container was never confirmed stopped (graceful stop failed, or
		// was never attempted), so dumponexit JFR/GC artifacts may not have
		// been flushed yet. Preserve the remote directory for manual recovery
		// instead of deleting a source that might still be mid-write.
		record.Error = "graceful stop was not confirmed before collection; remote directory preserved"
		record.PreservedRemoteDir = true
		_ = writeDiagnosticsRecordManifest(localDir, record)
		m.diagnosticsRec = &record
		return &record, errors.New(record.Error)
	}

	if _, stderr, err := m.exec.ExecuteCommand(ctx, m.host, []string{"rm", remoteRemoveRecursiveFlag, m.diagnosticsDir}); err != nil {
		record.Error = fmt.Sprintf("remove remote diagnostics directory: %v (stderr: %s)", err, stderr)
		record.PreservedRemoteDir = true
		_ = writeDiagnosticsRecordManifest(localDir, record)
		m.diagnosticsRec = &record
		return &record, errors.New(record.Error)
	}
	record.RemoteDirRemoved = true
	if err := writeDiagnosticsRecordManifest(localDir, record); err != nil {
		m.diagnosticsRec = &record
		return &record, fmt.Errorf("write diagnostics manifest: %w", err)
	}

	m.diagnosticsDone = true
	m.diagnosticsRec = &record
	return &record, nil
}

func (m *RemoteDockerManager) diagnosticsRecord() *diagnosticsRecord {
	return m.diagnosticsRec
}

func (m *RemoteDockerManager) gracefulStopForDiagnostics(ctx context.Context) error {
	if m.diagnosticsDone || m.diagnosticsStopDone {
		return nil
	}
	if m.containerID == "" || m.diagnosticsDir == "" {
		return nil
	}
	_, stderr, err := m.exec.ExecuteCommand(ctx, m.host, []string{
		constants.DockerCommand,
		constants.DockerCmdStop,
		"-t",
		fmt.Sprintf("%d", dockerDiagnosticsStopTimeoutSeconds),
		m.containerID,
	})
	if err != nil {
		return fmt.Errorf("graceful docker stop for diagnostics: %w (stderr: %s)", err, stderr)
	}
	m.diagnosticsStopDone = true
	return nil
}

// Cleanup force-removes the last started container (if any)
func (m *RemoteDockerManager) Cleanup() error {
	timeout := remoteCleanupTimeout
	if m.diagnosticsDir != "" {
		timeout = time.Duration(dockerDiagnosticsStopTimeoutSeconds)*time.Second + remoteDiagnosticsSlack
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	return m.CleanupContext(ctx)
}

// CleanupContext force-removes the last container and staging within the caller's budget.
func (m *RemoteDockerManager) CleanupContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	var cleanupErrs []error
	var diagnosticsRecords []diagnosticsRecord
	if m.containerID != "" {
		if m.diagnosticsDir != "" && !m.diagnosticsDone {
			if err := m.gracefulStopForDiagnostics(ctx); err != nil {
				logging.LogWarn("remote-docker", "diagnostics graceful stop failed", "host", m.host.Original, "error", err.Error())
				cleanupErrs = append(cleanupErrs, err)
			}
			record, err := m.collectDiagnostics(ctx)
			if record != nil {
				diagnosticsRecords = append(diagnosticsRecords, *record)
			}
			if err != nil {
				logging.LogWarn("remote-docker", "diagnostics collection failed", "host", m.host.Original, "remote_dir", m.diagnosticsDir, "error", err.Error())
				cleanupErrs = append(cleanupErrs, err)
			}
		}
		if err := m.ops.StopContainer(ctx, m.containerID); err != nil {
			cleanupErrs = append(cleanupErrs, err)
			return errors.Join(cleanupErrs...)
		}
		m.containerID = ""
	}
	if err := m.cleanupStaging(ctx); err != nil {
		cleanupErrs = append(cleanupErrs, err)
	}
	if err := writeDiagnosticsAggregateManifest(m.diagnosticsRoot, diagnosticsRecords); err != nil {
		cleanupErrs = append(cleanupErrs, err)
	}
	return errors.Join(cleanupErrs...)
}

// Close is a no-op for the remote manager
func (m *RemoteDockerManager) Close() {}
