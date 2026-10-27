/*
Copyright © 2025 Dell Technologies
*/

package verification

import (
	"time"

	"github.com/dell/storage-performance-tool/cli/internal/docker/command"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
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

	// Interfaces is the NIC set to plan on each host (--interfaces); empty lists the NICs only.
	Interfaces []string
	// Endpoints are the S3 endpoints as the engine's drivers name them, before slicing.
	Endpoints []string
	// SliceEndpoints divides Endpoints among the hosts in run order, the entry node first.
	SliceEndpoints bool
	// Threads is the driver concurrency limit whose per-NIC shares verify shows.
	Threads int
}

// Check represents the result of a single verification check
type Check struct {
	Passed        bool
	Warning       bool // Soft failure: doesn't block overall readiness but needs attention
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
	ClockSkew        Check // Clock skew relative to entry node (multi-host)
	RdmaDevice       Check // /dev/infiniband/ accessible (RDMA only)
	RdmaDriver       Check // libibverbs loadable in container (RDMA only)
	RdmaReadiness    Check // RDMA port active inside container (RDMA only)
	NicPlan          Check // The host's NIC plan (--interfaces only)
	PortsAccessible  Check
	MetricsEndpoint  Check
	ControlEndpoint  Check
	ContainerCleanup Check
	Overall          Check
	NICs             *NicListing // The host's NICs, when listed
	containerID      string      // For cleanup
}

// NicListing is a host's NIC inventory and plan, or the note that replaces them.
type NicListing struct {
	Output  *nicplan.Output
	Options nicplan.RenderOptions
	// Note replaces the listing when the image could not list the NICs.
	Note string
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
