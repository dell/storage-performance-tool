/*
Copyright © 2026 Dell Technologies
*/

package cmd

import (
	"fmt"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/spf13/cobra"
)

// validateNetworkMode accepts only the exact --network-mode values spt verify supports. The
// remote Docker managers treat any value other than host as bridge, so a typo must stop the
// command before any host is contacted.
func validateNetworkMode(networkMode string) error {
	if networkMode != constants.BridgeNetworkMode && networkMode != constants.HostNetworkMode {
		return networkModeError(networkMode)
	}
	return nil
}

// normalizeNetworkMode returns host or bridge for a --network-mode value that names one of them
// once surrounding spaces are trimmed, ignoring case, as the 5.17.0 remote route matched it. Any
// other value, the empty one included, is refused with verify's message.
func normalizeNetworkMode(networkMode string) (string, error) {
	trimmed := strings.TrimSpace(networkMode)
	for _, mode := range []string{constants.HostNetworkMode, constants.BridgeNetworkMode} {
		if strings.EqualFold(trimmed, mode) {
			return mode, nil
		}
	}
	return "", networkModeError(networkMode)
}

// normalizeNetworkModeFlag refuses a bad --network-mode for spt run and replay and rewrites an
// accepted spelling in place, so every later reader of the flag (the bridge warning, the
// --interfaces checks, run metadata and the local, remote node and remote entry routes) sees the
// same normalised value. An unset or already normalised flag is left untouched.
func normalizeNetworkModeFlag(cmd *cobra.Command) error {
	value, _ := cmd.Flags().GetString("network-mode")
	mode, err := normalizeNetworkMode(value)
	if err != nil {
		return err
	}
	if mode == value {
		return nil
	}
	return cmd.Flags().Set("network-mode", mode)
}

func networkModeError(networkMode string) error {
	return fmt.Errorf("network-mode must be '%s' or '%s', got: %s", constants.BridgeNetworkMode, constants.HostNetworkMode, networkMode)
}
