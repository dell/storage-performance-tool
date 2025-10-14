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
)

// SSH connection constants
const (
	SSHCommand        = "ssh"
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
)

// Docker image references
const (
	LegacyMongooseImage = "dcr.octo.dell.com/mongoose-v5/mongoose"
	DefaultSptImage     = LegacyMongooseImage
)

// REST API endpoint constants
const (
	SptMetricsEndpoint = "/metrics/json"
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
