/*
Copyright © 2025 Dell Technologies
*/

package constants

// Docker network configuration constants
const (
	// Network modes
	// DefaultNetworkMode is "host".
	// Remote workers always use host networking for Java RMI, whatever the mode; the bridge mode
	// applies to the entry, a lone engine and the idle LIST engines, which publish their API port.
	// See: mike/planning/RMI_HOST_NETWORKING_REQUIREMENT.md
	DefaultNetworkMode = "host"
	BridgeNetworkMode  = "bridge" // Bridge network mode (remote workers still use host networking)
	HostNetworkMode    = "host"   // Host network mode for RMI communication

	// Container configuration
	DefaultRestartPolicy = "no" // Default restart policy for containers
)
