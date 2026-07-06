/*
Copyright © 2025 Dell Technologies
*/

// Package command builds docker CLI invocations abstracted behind helpers.
package command

import (
	"fmt"
	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"sort"
	"strings"
)

// DockerCommandBuilderImpl implements DockerCommandBuilder
type DockerCommandBuilderImpl struct{}

// NewDockerCommandBuilder creates a new DockerCommandBuilder
func NewDockerCommandBuilder() DockerCommandBuilder {
	return &DockerCommandBuilderImpl{}
}

// BuildRunCommand builds a docker run command with all necessary flags
func (b *DockerCommandBuilderImpl) BuildRunCommand(config ContainerConfig) []string {
	dockerArgs := []string{"docker", "run"}

	// Detached mode
	if config.Detached {
		dockerArgs = append(dockerArgs, "-d")
	}

	// Container name
	if config.Name != "" {
		dockerArgs = append(dockerArgs, "--name", config.Name)
	}

	// Environment variables (stable order for testability)
	if len(config.Environment) > 0 {
		keys := make([]string, 0, len(config.Environment))
		for k := range config.Environment {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, key := range keys {
			dockerArgs = append(dockerArgs, "-e", fmt.Sprintf("%s=%s", key, config.Environment[key]))
		}
	}

	for _, envFile := range config.EnvFiles {
		dockerArgs = append(dockerArgs, "--env-file", envFile)
	}

	// Labels (stable order for testability)
	if len(config.Labels) > 0 {
		labelKeys := make([]string, 0, len(config.Labels))
		for k := range config.Labels {
			labelKeys = append(labelKeys, k)
		}
		sort.Strings(labelKeys)
		for _, key := range labelKeys {
			dockerArgs = append(dockerArgs, "--label", fmt.Sprintf("%s=%s", key, config.Labels[key]))
		}
	}

	// Network mode and port mappings
	if config.NetworkMode == NetworkModeHost {
		dockerArgs = append(dockerArgs, "--network", "host")
	} else {
		// Bridge mode - add port mappings
		for _, portMapping := range config.PortMappings {
			protocol := portMapping.Protocol
			if protocol == "" {
				protocol = "tcp"
			}
			portSpec := fmt.Sprintf("%d:%d/%s", portMapping.HostPort, portMapping.ContainerPort, protocol)
			dockerArgs = append(dockerArgs, "-p", portSpec)
		}
	}

	// Device passthrough (e.g., RDMA /dev/infiniband/)
	for _, device := range config.Devices {
		dockerArgs = append(dockerArgs, "--device", device)
	}

	// Linux capabilities
	for _, cap := range config.CapAdd {
		dockerArgs = append(dockerArgs, "--cap-add", cap)
	}

	// Resource limits
	for _, ulimit := range config.Ulimits {
		dockerArgs = append(dockerArgs, "--ulimit", ulimit)
	}

	for _, bind := range config.BindMounts {
		dockerArgs = append(dockerArgs, "-v", bind)
	}

	// Add image
	dockerArgs = append(dockerArgs, config.Image)

	// Add command
	dockerArgs = append(dockerArgs, config.Command...)

	return dockerArgs
}

// BuildWorkerNodeCommand builds docker run for RMI worker nodes
func (b *DockerCommandBuilderImpl) BuildWorkerNodeCommand(image, containerName, rmiHostname string, _ int, _ int) []string {
	// Host networking and explicit JVM RMI advertised hostname to avoid Docker bridge/NAT issues.
	config := ContainerConfig{
		Image:       image,
		Name:        containerName,
		NetworkMode: NetworkModeHost,
		Detached:    true,
		Environment: map[string]string{
			constants.JavaOptsEnvVar:        fmt.Sprintf("%s%s", constants.JavaRMIHostnamePrefix, rmiHostname),
			constants.JavaToolOptionsEnvVar: fmt.Sprintf("%s%s", constants.JavaRMIHostnamePrefix, rmiHostname),
		},
		Command: []string{
			"--run-node=true",
			fmt.Sprintf("--run-port=%d", constants.DefaultSptAPIPort),
			fmt.Sprintf("--load-step-node-port=%d", constants.DefaultRMIRegistryPort),
		},
	}

	// RDMA device passthrough when SPT_RDMA is enabled
	if constants.IsRdmaEnabled() {
		config.Devices = []string{constants.RdmaDevicePath}
		config.CapAdd = []string{constants.RdmaCapIpcLock}
		config.Ulimits = []string{constants.RdmaUlimitMemlock}
	}

	// No port mappings in host network mode.
	return b.BuildRunCommand(config)
}

// BuildEntryNodeCommand builds docker run for RMI entry nodes
func (b *DockerCommandBuilderImpl) BuildEntryNodeCommand(image, containerName string, workerAddresses []string, networkMode NetworkMode) []string {
	config := ContainerConfig{
		Image:       image,
		Name:        containerName,
		NetworkMode: networkMode,
		Detached:    true,
		Command:     []string{constants.SptNodeCommand + "=true"},
	}

	// Add worker addresses if provided
	if len(workerAddresses) > 0 {
		config.Command = append([]string{"--load-step-node-addrs=" + strings.Join(workerAddresses, ",")}, config.Command...)
	}

	// Port mappings for entry node (only needs API port in bridge mode)
	if networkMode == NetworkModeBridge {
		config.PortMappings = []PortMapping{
			{HostPort: constants.DefaultSptAPIPort, ContainerPort: constants.DefaultSptAPIPort}, // REST API only
		}
	}

	return b.BuildRunCommand(config)
}

// BuildPortMapping creates a PortMapping
func BuildPortMapping(hostPort, containerPort int) PortMapping {
	return PortMapping{
		HostPort:      hostPort,
		ContainerPort: containerPort,
		Protocol:      constants.DefaultProtocol,
	}
}

// BuildRMIPortMappings creates port mappings for RMI object ports
func BuildRMIPortMappings(portStart, portCount int) []PortMapping {
	mappings := make([]PortMapping, portCount)
	for i := 0; i < portCount; i++ {
		port := portStart + i
		mappings[i] = BuildPortMapping(port, port)
	}
	return mappings
}
