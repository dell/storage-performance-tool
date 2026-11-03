package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/hostparse"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
	"github.com/dell/storage-performance-tool/cli/internal/results"
	"github.com/spf13/cobra"
)

// rdmaDeviceAuto is --rdma-device's default, which leaves each NIC's device to the NIC set.
const rdmaDeviceAuto = "auto"

// flagSource names where a flag's value came from, for messages: the flag, or its environment
// variable, which cli/.env also sets.
func flagSource(cmd *cobra.Command, flag, envKey string) string {
	if _, fromEnv := cmd.Annotations[envAppliedAnnotationPrefix+flag]; fromEnv {
		return envKey + " (environment or .env)"
	}
	return "--" + flag
}

// readInterfaces returns the command's --interfaces entries, none without a NIC set, and the name
// of the value's source. It checks the entries and the network mode; scenario.ValidateInterfaces
// checks the driver, workload and endpoint mode, then rejectSingleNicRdmaSettings the RDMA flags.
func readInterfaces(cmd *cobra.Command) ([]string, string, error) {
	if cmd.Flags().Lookup(flagInterfaces) == nil {
		return nil, "", nil
	}
	source := flagSource(cmd, flagInterfaces, constants.EnvInterfaces)
	value, _ := cmd.Flags().GetString(flagInterfaces)
	entries, err := nicplan.ParseInterfaces(value)
	if err != nil {
		if source != "--"+flagInterfaces {
			return nil, source, fmt.Errorf("%s: %w", source, err)
		}
		return nil, source, err
	}
	if len(entries) == 0 {
		return nil, source, nil
	}
	if err := requireHostNetwork(cmd, source); err != nil {
		return nil, source, err
	}
	return entries, source, nil
}

func requireHostNetwork(cmd *cobra.Command, source string) error {
	if networkMode, _ := cmd.Flags().GetString("network-mode"); networkMode == constants.BridgeNetworkMode {
		return fmt.Errorf("%s needs --network-mode %s: a bridge-network container sees only its own interface",
			source, constants.HostNetworkMode)
	}
	return nil
}

// readHostInterfaces returns the command's --host-interfaces values, checked against its
// --test-hosts, and the name of the values' source. A value that asks for NICs needs host networking.
func readHostInterfaces(cmd *cobra.Command) ([]nicplan.HostInterfaces, string, error) {
	if cmd.Flags().Lookup(flagHostInterfaces) == nil {
		return nil, "", nil
	}
	source := flagSource(cmd, flagHostInterfaces, constants.EnvHostInterfaces)
	values, _ := cmd.Flags().GetStringArray(flagHostInterfaces)
	if len(values) == 0 {
		return nil, source, nil
	}
	testHosts, _ := cmd.Flags().GetString("test-hosts")
	hostInfos, err := hostparse.ParseTestHosts(testHosts)
	if err != nil {
		return nil, source, fmt.Errorf("%s: --test-hosts: %w", source, err)
	}
	hosts := make([]string, 0, len(hostInfos))
	for _, info := range hostInfos {
		hosts = append(hosts, info.Host)
	}
	overrides, err := nicplan.ParseHostInterfaces(values, hosts)
	if err != nil {
		if source != "--"+flagHostInterfaces {
			return nil, source, fmt.Errorf("%s: %w", source, err)
		}
		return nil, source, err
	}
	if len(nicplan.AnyNicSet(overrides)) > 0 {
		if err := requireHostNetwork(cmd, source); err != nil {
			return nil, source, err
		}
	}
	return overrides, source, nil
}

// applyHostInterfacesEnv sets --host-interfaces from SPT_HOST_INTERFACES, with ; between hosts.
func applyHostInterfacesEnv(cmd *cobra.Command) error {
	if f := cmd.Flags().Lookup(flagHostInterfaces); f == nil || cmd.Flags().Changed(flagHostInterfaces) {
		return nil
	}
	v := strings.TrimSpace(os.Getenv(constants.EnvHostInterfaces))
	if v == "" {
		return nil
	}
	values := splitEngineOverridesEnv(v)
	for _, value := range values {
		if err := cmd.Flags().Set(flagHostInterfaces, value); err != nil {
			return err
		}
	}
	if len(values) > 0 {
		markEnvApplied(cmd, flagHostInterfaces, strings.Join(values, "; "))
	}
	return nil
}

// rejectSingleNicRdmaSettings rejects an explicit --rdma-device or --rdma-local-ip with a NIC set,
// where each NIC brings its own RDMA device and address (spec item 13). Values from the
// environment or cli/.env count, and the message names their source.
func rejectSingleNicRdmaSettings(cmd *cobra.Command, source string) error {
	for _, setting := range []struct {
		flag, env string
		explicit  func(string) bool
	}{
		{"rdma-device", constants.EnvRdmaDevice, func(v string) bool { return v != "" && v != rdmaDeviceAuto }},
		{"rdma-local-ip", constants.EnvRdmaLocalIP, func(v string) bool { return v != "" }},
	} {
		if cmd.Flags().Lookup(setting.flag) == nil || !cmd.Flags().Changed(setting.flag) {
			continue
		}
		if v, _ := cmd.Flags().GetString(setting.flag); setting.explicit(strings.TrimSpace(v)) {
			return fmt.Errorf("%s cannot be combined with %s %s: name each NIC's RDMA device or address in --interfaces",
				source, flagSource(cmd, setting.flag, setting.env), strings.TrimSpace(v))
		}
	}
	return nil
}

// captureNicEngines stores the NIC-set reports fetched into root (D64). The pre-summary
// metadata write keeps them. A report that cannot be read is recorded and never fails the run.
func captureNicEngines(root string, stepIDs []string, metadata *runMetadata) {
	if metadata == nil || len(metadata.NicNodes) == 0 {
		return
	}
	engines, err := results.ReadNicEngines(root, stepIDs, metadata.NicNodes)
	metadata.NicEngines = engines
	metadata.NicEnginesError = ""
	if err != nil {
		metadata.NicEnginesError = err.Error()
		logging.LogWarn("auto-results", "NIC-set reports not stored", "reason", metadata.NicEnginesError)
	}
}
