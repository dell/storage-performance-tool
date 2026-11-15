/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"fmt"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

// validateNetworkMode accepts only the exact --network-mode values spt verify, run and replay
// support. The remote Docker managers treat any value other than host as bridge, so a typo must
// stop the command before any host is contacted.
func validateNetworkMode(networkMode string) error {
	if networkMode != constants.BridgeNetworkMode && networkMode != constants.HostNetworkMode {
		return fmt.Errorf("network-mode must be '%s' or '%s', got: %s", constants.BridgeNetworkMode, constants.HostNetworkMode, networkMode)
	}
	return nil
}
