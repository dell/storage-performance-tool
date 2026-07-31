/*
Copyright © 2025 Dell Technologies
*/
//revive:disable:package-comments
// Package constants defines shared constants used across the application.
package constants

// Docker command constants
const (
	DockerCommand    = "docker"
	DockerCmdPull    = "pull"
	DockerCmdRun     = "run"
	DockerCmdPS      = "ps"
	DockerCmdRM      = "rm"
	DockerCmdLogs    = "logs"
	DockerCmdImages  = "images"
	DockerCmdImage   = "image"
	DockerCmdInspect = "inspect"
	DockerCmdVersion = "version"
	DockerCmdStop    = "stop"
)

// Docker CLI flag constants
const (
	DockerFlagDetached   = "-d"
	DockerFlagName       = "--name"
	DockerFlagEnv        = "-e"
	DockerFlagNetwork    = "--network"
	DockerFlagPort       = "-p"
	DockerFlagForce      = "-f"
	DockerFlagQuiet      = "-q"
	DockerFlagFilter     = "--filter"
	DockerFlagFormat     = "--format"
	DockerFlagAll        = "-a"
	DockerFlagTimestamps = "--timestamps"
	DockerFlagSince      = "--since"
	DockerFlagRemove     = "--rm"
	DockerFlagReadOnly   = "--read-only"
	DockerFlagEntrypoint = "--entrypoint"
)

// SSH connection constants
const (
	SSHCommand        = "ssh"
	SCPCommand        = "scp"
	SSHConnectTimeout = "ConnectTimeout=10"
	SSHBatchMode      = "BatchMode=yes"
)

// Docker format string constants
const (
	DockerVersionFormat   = "{{.Server.Version}}"
	DockerContainerFormat = "{{.ID}}|{{.Names}}|{{.Image}}|{{.Status}}|{{.Ports}}"
	DockerIPAddressFormat = "{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}"
	DockerConflictFormat  = "{{.ID}}\\\\t{{.Names}}\\\\t{{.Image}}\\\\t{{.State}}\\\\t{{.Ports}}\\\\t{{.CreatedAt}}"
)

// Port number constants
const (
	DefaultRMIRegistryPort = 1099
	DefaultSptAPIPort      = 9999
)

// Timeout constants (in seconds)
const (
	SSHConnectTimeoutSecs     = 10
	DockerDaemonTimeoutSecs   = 10
	ContainerStartTimeoutSecs = 30
	PortCheckTimeoutSecs      = 2
	HTTPRequestTimeoutSecs    = 5
	ContainerReadyPollSecs    = 1
	ServiceInitializationSecs = 10
)

// Container configuration constants
const (
	DefaultProtocol          = "tcp"
	ContainerIDDisplayLength = 12
	JavaOptsEnvVar           = "JAVA_OPTS"
	JavaToolOptionsEnvVar    = "JAVA_TOOL_OPTIONS"
	JavaRMIHostnamePrefix    = "-Djava.rmi.server.hostname="
	SptNodeCommand           = "--run-node"

	DockerLabelManaged = "spt.managed"
	DockerLabelRole    = "spt.role"
	DockerLabelHost    = "spt.host"
	DockerLabelRunID   = "spt.run.id"

	DockerRoleEntry  = "entry"
	DockerRoleWorker = "worker"
	DockerRoleVerify = "verify"
	DockerRoleNode   = "node"
)

// Docker image references
const (
	// LegacyMongooseImage is the old internal Dell registry (deprecated)
	LegacyMongooseImage = "dcr.octo.dell.com/mongoose-v5/mongoose"
	// DefaultSptImage is the official GHCR image for SPT
	DefaultSptImage = "ghcr.io/dell/storage-performance-tool"
)

// RDMA container configuration
const (
	// RdmaDevicePath is the host device directory passed to --device for RDMA access
	RdmaDevicePath = "/dev/infiniband/"
	// RdmaCapIpcLock is the Linux capability required for ibv_reg_mr memory pinning
	RdmaCapIpcLock = "IPC_LOCK"
	// RdmaUlimitMemlock removes the locked-memory limit so RDMA buffers can be pinned
	RdmaUlimitMemlock = "memlock=-1:-1"
)

// REST API endpoint constants
const (
	SptMetricsEndpoint      = "/metrics/json"
	SptConfigSchemaEndpoint = "/config/schema"
)

// Engine log artifact constants
const (
	// EngineLogArtifactPageSize mirrors LogServlet.LOG_PAGE_SIZE_LIMIT.
	EngineLogArtifactPageSize      = 1024 * 1024
	EnginePlainLogArtifactMaxBytes = EngineLogArtifactPageSize - 1
)

// Binary byte-unit constants.
const (
	BytesPerKiB int64 = 1024
	BytesPerMiB       = BytesPerKiB * 1024
	BytesPerGiB       = BytesPerMiB * 1024
	BytesPerTiB       = BytesPerGiB * 1024
	BytesPerPiB       = BytesPerTiB * 1024
	BytesPerEiB       = BytesPerPiB * 1024

	UnitByte         = "B"
	UnitKiB          = "KiB"
	UnitMiB          = "MiB"
	UnitGiB          = "GiB"
	UnitTiB          = "TiB"
	UnitPiB          = "PiB"
	UnitEiB          = "EiB"
	UnitMiBPerSecond = "MiB/s"
)

// Read shuffle tuning constants
const (
	// ReadShuffleDefaultBatchSize is the implicit read-phase batch size when --shuffle is enabled without an explicit override.
	ReadShuffleDefaultBatchSize = 512000
	// ReadShuffleMaxBatchSize bounds --shuffle-batch-size to avoid pathological engine buffer growth.
	ReadShuffleMaxBatchSize = 1000000
)

// Progress indicator constants
const (
	ProgressSpinner = "⏳"
	ProgressSuccess = "✅"
	ProgressFailure = "❌"
)

// Parsing separator constants
const (
	DockerOutputSeparator       = "|"
	ContainerNameDotReplacement = "-"
)

// Netstat command constants
const (
	// Prefer ss (modern) with process info; fallback to netstat with PIDs. printf twice for port.
	PortCheckCommand = "command -v ss >/dev/null 2>&1 && ss -ltnp | grep ':%d ' || netstat -lnp | grep ':%d '"
	// Snapshot all listeners once; used to parse locally instead of grepping per port
	PortSnapshotCommand = "command -v ss >/dev/null 2>&1 && ss -ltnp || netstat -lnp"
)
