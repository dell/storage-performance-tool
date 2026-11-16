/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/docker/docker/client"
)

// remoteDockerPingTimeout bounds the remote connect check; a variable so
// tests can reach the deadline quickly.
var remoteDockerPingTimeout = constants.RemoteDockerPingTimeout

// NewDockerManagerForHost creates a Docker client for a specific host
func NewDockerManagerForHost(ctx context.Context, hostInfo *hostparse.HostInfo) (*DockerManager, error) {
	// Create a child context that can be cancelled
	childCtx, cancel := context.WithCancel(ctx)

	// LOCAL: keep using Docker SDK client
	if hostInfo.IsLocal {
		dockerHost := hostInfo.GetDockerHost()

		logging.LogDebug("docker-ssh", "creating Docker client",
			"host", hostInfo.Host,
			"user", hostInfo.User,
			"docker_host", dockerHost,
			"is_local", hostInfo.IsLocal)

		opts := []client.Opt{
			client.WithHost(dockerHost),
			client.WithAPIVersionNegotiation(),
		}

		cli, err := client.NewClientWithOpts(opts...)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("failed to create Docker client for %s: %w",
				hostInfo.Original, err)
		}

		// Test connection with timeout
		pingCtx, pingCancel := context.WithTimeout(childCtx, 10*time.Second)
		defer pingCancel()

		_, err = cli.Ping(pingCtx)
		if err != nil {
			_ = cli.Close()
			cancel()
			return nil, fmt.Errorf("failed to connect to local Docker: %w", err)
		}

		logging.LogInfo("docker-ssh", "successfully connected to Docker",
			"host", hostInfo.Original)

		return &DockerManager{
			client:   cli,
			ctx:      childCtx,
			cancel:   cancel,
			hostInfo: hostInfo,
		}, nil
	}

	// REMOTE: use RemoteDockerManager (SSH + docker CLI)
	if os.Getenv("SSH_AUTH_SOCK") == "" {
		logging.LogInfo("docker-ssh", "SSH_AUTH_SOCK not set - SSH key authentication may fail")
	}

	rdm, err := NewRemoteDockerManager(hostInfo)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create remote docker manager for %s: %w", hostInfo.Original, err)
	}

	// Validate remote Docker connectivity quickly via SSH + docker CLI (consistent with verify)
	pingCtx, pingCancel := context.WithTimeout(childCtx, remoteDockerPingTimeout)
	defer pingCancel()
	exec := command.NewCommandExecutor()
	if _, stderr, err := exec.ExecuteCommand(pingCtx, hostInfo, []string{constants.DockerCommand, constants.DockerCmdVersion, constants.DockerFlagFormat, constants.DockerVersionFormat}); err != nil {
		cancel()
		// A cancel by the caller (Ctrl-C, SIGTERM) kills ssh, which reads
		// as an SSH failure; report the cancel instead. A deadline, the
		// ping's own or the caller's, is a slow or unreachable host and
		// keeps the advice below.
		if errors.Is(ctx.Err(), context.Canceled) {
			return nil, fmt.Errorf("connection to Docker on %s stopped: %w (%w)", hostInfo.Original, command.ContextEnd(ctx), err)
		}
		return nil, fmt.Errorf("failed to connect to Docker on %s: %w\nEnsure:\n  1. SSH key authentication is configured: ssh %s@%s\n  2. Docker is accessible to user '%s' on the remote host\n  3. The Docker daemon is running on the remote host\n  (stderr: %s)",
			hostInfo.Original, err, hostInfo.User, hostInfo.Host, hostInfo.User, stderr)
	}

	// Wrap RemoteDockerManager into a DockerManager-like holder so callers can Close() via interface
	// We don't need childCtx here, but preserve cancel() to match lifecycle expectations
	_ = childCtx
	return &DockerManager{
		// client is nil for remote CLI path
		ctx:      childCtx,
		cancel:   cancel,
		hostInfo: hostInfo,
		remote:   rdm,
	}, nil
}
