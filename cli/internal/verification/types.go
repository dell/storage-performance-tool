/*
Copyright © 2025 Dell Technologies
*/

package verification

import (
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/preflight"
)

// Config holds the verification configuration
type Config struct {
	NetworkMode  string // "bridge" or "host"
	APIPort      int    // Spt API port (default 9999)
	RMIPortStart int    // Starting port for RMI range
	RMIPortCount int    // Number of RMI ports
	MinHosts     int    // Minimum hosts that must pass
	ForceCleanup bool   // Automatically clean up conflicts without prompting
	ShowProgress bool   // Show detailed step-by-step progress messages
	UseRdma      bool   // Include RDMA hardware/config verification
}

// Check represents the result of a single verification check
type Check struct {
	Passed        bool
	Message       string
	Duration      time.Duration
	Error         error
	CommandResult *command.CommandResult // Optional detailed command results
}

// NodeResult holds the verification results for a single node
type NodeResult struct {
	Host             string
	HostInfo         *hostparse.HostInfo
	SSHConnectivity  Check
	DockerAvailable  Check
	DockerImagePull  Check
	ContainerStart   Check
	RdmaDevice       Check // /dev/infiniband/ accessible (RDMA only)
	RdmaDriver       Check // libibverbs loadable in container (RDMA only)
	RdmaReadiness    Check // RDMA port active inside container (RDMA only)
	PortsAccessible  Check
	MetricsEndpoint  Check
	ControlEndpoint  Check
	ContainerCleanup Check
	Overall          Check
	containerID      string // For cleanup
}

// Report represents the complete verification report
type Report struct {
	Results  map[string]*NodeResult
	Duration time.Duration
	Config   Config
	Ready    bool
	Summary  string
}

// Verifier performs verification checks on multiple hosts
type Verifier struct {
	hosts   []*hostparse.HostInfo
	config  Config
	results map[string]*NodeResult

	// Dependencies for testability
	cmdExecutor  command.CommandExecutor
	netChecker   NetworkChecker
	timeProvider TimeProvider
	preflight    preflight.Checker
}
