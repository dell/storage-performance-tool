/*
Copyright © 2025 Dell Technologies
*/

package tui

import (
	"context"
	"fmt"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
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

// enginePorts are the REST API and RMI registry ports that every engine in one run serves.
type enginePorts struct {
	api      int
	registry int
}

func defaultEnginePorts() enginePorts {
	return enginePorts{api: constants.DefaultSptAPIPort, registry: constants.DefaultRMIRegistryPort}
}

func (p enginePorts) isDefault() bool {
	return p == defaultEnginePorts()
}

func (p enginePorts) runPortArg() string {
	return fmt.Sprintf("%s%d", constants.EngineRunPortArgPrefix, p.api)
}

func (p enginePorts) nodePortArg() string {
	return fmt.Sprintf("%s%d", constants.EngineNodePortArgPrefix, p.registry)
}

// nonDefaultNodePortArgs returns the RMI registry port argument for an engine that has always
// started without one, on the engine default (1099): an entry, a node-mode engine, and a worker
// the controller's own Docker starts. The default adds nothing, so its command is unchanged.
func nonDefaultNodePortArgs(ports enginePorts) []string {
	if ports.registry == constants.DefaultRMIRegistryPort {
		return nil
	}
	return []string{ports.nodePortArg()}
}

// enginePortsLauncher starts RMI workers and entries on a run's engine ports. Only this package's
// own managers take this path, and only on other ports; see builtinEnginePortsLauncher.
type enginePortsLauncher interface {
	startWorkerNodeOnPorts(
		ctx context.Context, image, rmiHostname string, ports enginePorts, rmiPortStart, rmiPortCount int,
		additionalArgs []string,
	) (string, error)
	startEntryNodeOnPorts(
		ctx context.Context, image string, ports enginePorts, workerAddresses, additionalArgs []string,
		networkMode string,
	) (string, error)
}

// builtinEnginePortsLauncher returns the port-aware start of one of this package's own managers,
// chosen by concrete type. A type that embeds one, such as an extension's wrapper, inherits its
// unexported methods too; calling them would bypass the wrapper's exported launch overrides, so
// only the managers themselves qualify.
func builtinEnginePortsLauncher(dm DockerInterface) (enginePortsLauncher, bool) {
	switch launcher := dm.(type) {
	case *DockerManager:
		return launcher, true
	case *RemoteDockerManager:
		return launcher, true
	case *MockDockerManager:
		return launcher, true
	default:
		return nil, false
	}
}

func errDefaultPortsOnly(dm DockerInterface, role string, ports enginePorts) error {
	return fmt.Errorf("%T cannot start an %s on API port %d and RMI registry port %d; it supports only %d and %d",
		dm, role, ports.api, ports.registry, constants.DefaultSptAPIPort, constants.DefaultRMIRegistryPort)
}

func startWorkerNodeContainerContext(
	ctx context.Context,
	dm DockerInterface,
	image, rmiHostname string,
	ports enginePorts,
	rmiPortStart, rmiPortCount int,
	additionalArgs []string,
) (string, error) {
	if !ports.isDefault() {
		launcher, ok := builtinEnginePortsLauncher(dm)
		if !ok {
			return "", errDefaultPortsOnly(dm, "RMI worker", ports)
		}
		return launcher.startWorkerNodeOnPorts(
			normalizeContext(ctx), image, rmiHostname, ports, rmiPortStart, rmiPortCount, additionalArgs)
	}
	// The default ports start every manager through its exported launch, overrides included.
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
	ports enginePorts,
	workerAddresses, additionalArgs []string,
	networkMode string,
) (string, error) {
	if !ports.isDefault() {
		launcher, ok := builtinEnginePortsLauncher(dm)
		if !ok {
			return "", errDefaultPortsOnly(dm, "RMI entry", ports)
		}
		return launcher.startEntryNodeOnPorts(
			normalizeContext(ctx), image, ports, workerAddresses, additionalArgs, networkMode)
	}
	// The default ports start every manager through its exported launch, overrides included.
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
var _ enginePortsLauncher = (*DockerManager)(nil)
var _ enginePortsLauncher = (*RemoteDockerManager)(nil)
var _ enginePortsLauncher = (*MockDockerManager)(nil)

// Ensure DockerManager implements DockerInterface
var _ DockerInterface = (*DockerManager)(nil)
