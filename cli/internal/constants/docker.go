/*
Copyright © 2025 Dell Technologies
*/

package constants

// Docker network configuration constants
const (
	// Network modes
	// DefaultNetworkMode is "host" because Java RMI requires host networking for distributed
	// testing. Bridge networking fails due to RMI's local-host security check rejecting
	// connections from the Docker bridge gateway IP (172.17.0.1).
	// See: mike/planning/RMI_HOST_NETWORKING_REQUIREMENT.md
	DefaultNetworkMode = "host"
	BridgeNetworkMode  = "bridge" // Bridge network mode (not compatible with distributed RMI)
	HostNetworkMode    = "host"   // Host network mode for RMI communication

	// Container configuration
	DefaultRestartPolicy = "no" // Default restart policy for containers
)
