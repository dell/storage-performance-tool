/*
Copyright © 2025 Dell Technologies
*/

package constants

// Spt port constants
const (
	SptAPIPort          = "9999"  // Spt standard REST API port
	SptLegacyAPIPort    = "43234" // Legacy spt API port
	RMIRegistryPort     = "1099"  // RMI registry port for distributed testing
	DefaultRMIPortStart = 40000   // Default starting port for RMI object ports
	DefaultRMIPortCount = 10      // Default number of RMI ports to allocate per host
)

// Default HTTP port constants
const (
	DefaultHTTPPort  = "80"
	DefaultHTTPSPort = "443"
)

// Port constants for network operations (integers)
const (
	RMIRegistryPortInt = 1099 // RMI registry port for network operations
	SptAPIPortInt      = 9999 // Spt API port for network operations
)
