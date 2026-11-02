package scenario

import (
	"fmt"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
	"github.com/dell/storage-performance-tool/cli/internal/secretmask"
	"github.com/dell/storage-performance-tool/cli/internal/workload"
)

// The NIC-set engine settings. The CLI writes storage.net.interfaces from --interfaces and
// storage.net.nodeInterfaces from --host-interfaces only, and rejects engine overrides of either
// setting so its checks cannot be bypassed.
const (
	InterfacesPath     = "storage.net.interfaces"
	NodeInterfacesPath = "storage.net.nodeInterfaces"

	nodeInterfacesArgPrefix = "--storage-net-nodeInterfaces-"
	nodeInterfacesEntryKey  = "entry"
)

// WorkerNode is a started worker: its host as in --test-hosts, and the address the Entry Node has
// for it in load.step.node.addrs.
type WorkerNode struct {
	Host string
	Addr string
}

// NodeInterfacesArgs returns the Entry Node's arguments for the host overrides, one per host with a
// value: the entry host's key is entry, and a worker's is its address. A host that did not start
// has no key, since the engine would reject one.
func NodeInterfacesArgs(overrides []nicplan.HostInterfaces, entryHost string, workers []WorkerNode) []string {
	args := make([]string, 0, len(overrides))
	if override, ok := nicplan.ForHost(overrides, entryHost); ok {
		args = append(args, nodeInterfacesArgPrefix+nodeInterfacesEntryKey+"="+override.Value)
	}
	for _, worker := range workers {
		if override, ok := nicplan.ForHost(overrides, worker.Host); ok {
			args = append(args, nodeInterfacesArgPrefix+worker.Addr+"="+override.Value)
		}
	}
	return args
}

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
	if driver := resolveStorageDriverType(target.S3Driver); driver != storageDriverTypeS3 && driver != storageDriverTypeS3Rdma {
		return fmt.Errorf("%s requires the default Netty S3 driver or --s3-driver %s, not --s3-driver %s", source, S3DriverRdma, target.S3Driver)
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
	for _, setting := range []struct{ path, flag string }{
		{InterfacesPath, "--interfaces"},
		{NodeInterfacesPath, "--host-interfaces"},
	} {
		if EngineOverrideSets(override, strings.Split(setting.path, ".")...) {
			return fmt.Errorf("engine override %q sets %s, which is not supported; use %s",
				secretmask.EngineOverride(override), setting.path, setting.flag)
		}
	}
	return nil
}
