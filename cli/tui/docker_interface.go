/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"fmt"

	"github.com/dell/storage-performance-tool/cli/internal/scenario"
)

// DockerInterface defines the interface for Docker operations
// This allows for easy mocking in tests
type DockerInterface interface {
	// ContainerID returns the current container ID
	ContainerID() string

	// StartContainer creates and starts a new container with the given command
	StartContainer(image string, cmd []string) (string, error)

	// StartContainerWithScenario creates and starts a container with a scenario file
	StartContainerWithScenario(image string, scenarioPath string, additionalArgs []string) (string, error)

	// StartContainerInNodeMode starts a container in API server mode.
	StartContainerInNodeMode(image string, apiPort string, networkMode string, additionalArgs []string) (string, error)

	// StartWorkerNodeContainer starts a container in RMI worker node mode
	StartWorkerNodeContainer(image string, rmiHostname string, rmiPortStart, rmiPortCount int, additionalArgs []string) (string, error)

	// StartEntryNodeContainer starts a container as RMI entry node with worker addresses
	StartEntryNodeContainer(image string, workerAddresses []string, additionalArgs []string, networkMode string) (string, error)

	// StreamOutput starts streaming container output with callback functions
	StreamOutput(containerID string, stdoutCallback func(string), stderrCallback func(string))

	// Cleanup stops and removes the container
	Cleanup() error

	// CleanupContext stops/removes the container and ancillary staging within
	// the caller's bounded lifecycle budget.
	CleanupContext(context.Context) error

	// Close cleans up resources
	Close()
}

// contextDockerLauncher is an additive production capability. Keeping it
// separate from DockerInterface preserves compatibility for extensions and
// mocks while ensuring the concrete managers can bind launch I/O to the
// command lifecycle.
type contextDockerLauncher interface {
	StartContainerContext(context.Context, string, []string) (string, error)
	StartContainerWithScenarioContext(context.Context, string, string, []string) (string, error)
	StartContainerInNodeModeContext(context.Context, string, string, string, []string) (string, error)
	StartWorkerNodeContainerContext(context.Context, string, string, int, int, []string) (string, error)
	StartEntryNodeContainerContext(context.Context, string, []string, []string, string) (string, error)
}

type contextFileMountConfigurer interface {
	SetFileMountsContext(context.Context, []scenario.FileMount) error
}

func startContainerInNodeModeContext(
	ctx context.Context, dm DockerInterface, image, apiPort, networkMode string, additionalArgs []string,
) (string, error) {
	if launcher, ok := dm.(contextDockerLauncher); ok {
		return launcher.StartContainerInNodeModeContext(
			normalizeContext(ctx), image, apiPort, networkMode, additionalArgs)
	}
	if err := normalizeContext(ctx).Err(); err != nil {
		return "", err
	}
	return dm.StartContainerInNodeMode(image, apiPort, networkMode, additionalArgs)
}

func startWorkerNodeContainerContext(
	ctx context.Context,
	dm DockerInterface,
	image, rmiHostname string,
	rmiPortStart, rmiPortCount int,
	additionalArgs []string,
) (string, error) {
	if launcher, ok := dm.(contextDockerLauncher); ok {
		return launcher.StartWorkerNodeContainerContext(
			normalizeContext(ctx), image, rmiHostname, rmiPortStart, rmiPortCount, additionalArgs)
	}
	if err := normalizeContext(ctx).Err(); err != nil {
		return "", err
	}
	return dm.StartWorkerNodeContainer(image, rmiHostname, rmiPortStart, rmiPortCount, additionalArgs)
}

func startEntryNodeContainerContext(
	ctx context.Context,
	dm DockerInterface,
	image string,
	workerAddresses, additionalArgs []string,
	networkMode string,
) (string, error) {
	if launcher, ok := dm.(contextDockerLauncher); ok {
		return launcher.StartEntryNodeContainerContext(
			normalizeContext(ctx), image, workerAddresses, additionalArgs, networkMode)
	}
	if err := normalizeContext(ctx).Err(); err != nil {
		return "", err
	}
	return dm.StartEntryNodeContainer(image, workerAddresses, additionalArgs, networkMode)
}

func setFileMountsContext(
	ctx context.Context, dm DockerInterface, mounts []scenario.FileMount,
) error {
	if configurer, ok := dm.(contextFileMountConfigurer); ok {
		return configurer.SetFileMountsContext(normalizeContext(ctx), mounts)
	}
	if err := normalizeContext(ctx).Err(); err != nil {
		return err
	}
	configurer, ok := dm.(fileMountConfigurer)
	if !ok {
		return fmt.Errorf("docker manager does not support external item file mounts")
	}
	return configurer.SetFileMounts(mounts)
}

var _ contextDockerLauncher = (*DockerManager)(nil)
var _ contextDockerLauncher = (*RemoteDockerManager)(nil)

// Ensure DockerManager implements DockerInterface
var _ DockerInterface = (*DockerManager)(nil)
