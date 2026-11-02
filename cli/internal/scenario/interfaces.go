package scenario

import (
	"fmt"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/secretmask"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

// The NIC-set engine settings. The CLI writes storage.net.interfaces from --interfaces only, and
// rejects engine overrides of either setting so its checks cannot be bypassed.
const (
	InterfacesPath     = "storage.net.interfaces"
	NodeInterfacesPath = "storage.net.nodeInterfaces"
)

// InterfacesTarget is the rest of the run configuration a NIC set is validated against. Source
// names where the --interfaces value came from, such as "--interfaces" or "SPT_INTERFACES".
type InterfacesTarget struct {
	Source            string
	S3Driver          string
	WorkloadType      string
	EndpointSelection EndpointSelection
}

// ValidateInterfaces checks a NIC set against the driver, workload and endpoint mode; the mock
// workload's dummy driver has no NICs. Errors name the value's source. The engine repeats the
// driver and endpoint-mode checks for direct engine configuration. No entries means no NIC set,
// which is always valid.
func ValidateInterfaces(interfaces []string, target InterfacesTarget) error {
	if len(interfaces) == 0 {
		return nil
	}
	source := target.Source
	if source == "" {
		source = "--interfaces"
	}
	if target.WorkloadType == workload.Tables || target.WorkloadType == workload.Mock {
		return fmt.Errorf("%s does not support the %s workload", source, target.WorkloadType)
	}
	if driver := resolveStorageDriverType(target.S3Driver); driver != storageDriverTypeS3 {
		return fmt.Errorf("%s requires the default Netty S3 driver, not --s3-driver %s", source, target.S3Driver)
	}
	if target.EndpointSelection.Active() {
		return fmt.Errorf("%s supports only --endpoint-selection %s, not %s",
			source, EndpointSelectionDefault, target.EndpointSelection.Mode)
	}
	return nil
}

// RejectInterfacesOverride rejects an engine override of either NIC-set setting, in either
// spelling, and points to the flag that sets it.
func RejectInterfacesOverride(override string) error {
	for _, path := range []string{InterfacesPath, NodeInterfacesPath} {
		if EngineOverrideSets(override, strings.Split(path, ".")...) {
			return fmt.Errorf("engine override %q sets %s, which is not supported; use --interfaces",
				secretmask.EngineOverride(override), path)
		}
	}
	return nil
}
