/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/docker/docker/client"
)

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
	pingCtx, pingCancel := context.WithTimeout(childCtx, 10*time.Second)
	defer pingCancel()
	exec := command.NewCommandExecutor()
	if _, stderr, err := exec.ExecuteCommand(pingCtx, hostInfo, []string{constants.DockerCommand, constants.DockerCmdVersion, constants.DockerFlagFormat, constants.DockerVersionFormat}); err != nil {
		cancel()
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
