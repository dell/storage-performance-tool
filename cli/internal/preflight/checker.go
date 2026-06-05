/*
Copyright © 2025 Dell Technologies
*/

package preflight

import (
	"context"
	"fmt"
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	dockerconf "github.com/dell/storage-performance-tool/cli/internal/docker"
	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
)

// Checker defines preflight validation operations for a host
type Checker interface {
	// CheckDocker validates that Docker is accessible and returns the server version
	CheckDocker(ctx context.Context, host *hostparse.HostInfo) (string, error)
	// EnsureImage ensures the image exists locally (pulls if missing)
	EnsureImage(ctx context.Context, host *hostparse.HostInfo, image string) error
	// CheckPorts verifies required ports are available and reports conflicts
	CheckPorts(ctx context.Context, host *hostparse.HostInfo, apiPort int, rmiPortStart, rmiPortCount int) (*dockerconf.ConflictInfo, error)
}

// DefaultChecker implements Checker using the command-layer (SSH/local)
type DefaultChecker struct {
	exec command.CommandExecutor
}

// NewDefaultChecker creates a default preflight checker
func NewDefaultChecker() *DefaultChecker {
	return &DefaultChecker{exec: command.NewCommandExecutor()}
}

// NewCheckerWithExecutor creates a preflight checker that uses the provided
// command executor (useful for tests so mocks see the same calls).
func NewCheckerWithExecutor(exec command.CommandExecutor) *DefaultChecker {
	if exec == nil {
		exec = command.NewCommandExecutor()
	}
	return &DefaultChecker{exec: exec}
}

// withTimeout derives a context with timeout
func withTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithTimeout(ctx, d) // #nosec G118 -- cancel is called by all callers
}

// CheckDocker runs `docker version --format '{{.Server.Version}}'`
func (c *DefaultChecker) CheckDocker(ctx context.Context, host *hostparse.HostInfo) (string, error) {
	tctx, cancel := withTimeout(ctx, time.Duration(constants.DockerDaemonTimeoutSecs)*time.Second)
	defer cancel()
	stdout, stderr, err := c.exec.ExecuteCommand(tctx, host, []string{constants.DockerCommand, constants.DockerCmdVersion, constants.DockerFlagFormat, constants.DockerVersionFormat})
	if err != nil {
		return "", fmt.Errorf("docker not accessible on %s: %w (stderr: %s)", host.Original, err, stderr)
	}
	return stdout, nil
}

// EnsureImage checks if image exists and pulls when missing
func (c *DefaultChecker) EnsureImage(ctx context.Context, host *hostparse.HostInfo, image string) error {
	ops := command.NewDockerOperations(c.exec, host)
	tctx, cancel := withTimeout(ctx, 3*time.Minute)
	defer cancel()
	available, err := ops.IsImageAvailable(tctx, image)
	if err != nil {
		return err
	}
	if available {
		return nil
	}
	if constants.IsDevImage(image) {
		return fmt.Errorf("dev image %s not present on %s; build it with `make docker-local` and distribute it with engine/tools/push-worker-image.sh", image, host.Original)
	}
	_, err = ops.PullImage(tctx, image)
	return err
}

// CheckPorts verifies that the provided ports are not in use
func (c *DefaultChecker) CheckPorts(ctx context.Context, host *hostparse.HostInfo, apiPort int, rmiPortStart, rmiPortCount int) (*dockerconf.ConflictInfo, error) {
	ports := []int{constants.DefaultRMIRegistryPort, apiPort}
	for i := 0; i < rmiPortCount; i++ {
		ports = append(ports, rmiPortStart+i)
	}
	checker := dockerconf.NewPortChecker(host, c.exec, ports)
	tctx, cancel := withTimeout(ctx, 10*time.Second)
	defer cancel()
	return checker.CheckForConflicts(tctx)
}
