package tui

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
	"github.com/dell/storage-performance-tool/cli/internal/logging"
	"github.com/docker/docker/api/types/container"
)

// numaNodeRoot is the host's NUMA node directory; a variable for tests only.
var numaNodeRoot = "/sys/devices/system/node"

// applyEnginePlacement holds a local engine container to one NUMA node of this host when SPT_ENGINE_NUMA_NODE names one:
// the container gets only that node's CPUs and memory. On a two-socket client an engine whose threads spread over both
// sockets spends about 9% more CPU per small GET (.scratch/engine-numa-placement). Unset, the container is unchanged.
// It reads this host's sysfs, so it serves containers on the local Docker daemon only.
func applyEnginePlacement(hostConfig *container.HostConfig) error {
	value := strings.TrimSpace(os.Getenv(constants.EnvSptEngineNumaNode))
	if value == "" {
		return nil
	}
	node, err := strconv.Atoi(value)
	if err != nil || node < 0 {
		return fmt.Errorf("%s=%q is not a NUMA node number", constants.EnvSptEngineNumaNode, value)
	}
	raw, err := os.ReadFile(filepath.Join(numaNodeRoot, "node"+strconv.Itoa(node), "cpulist"))
	if err != nil {
		return fmt.Errorf("%s=%d: this host has no such NUMA node: %w", constants.EnvSptEngineNumaNode, node, err)
	}
	cpus := strings.TrimSpace(string(raw))
	if cpus == "" {
		return fmt.Errorf("%s=%d: that NUMA node has no CPUs", constants.EnvSptEngineNumaNode, node)
	}
	hostConfig.CpusetCpus = cpus
	hostConfig.CpusetMems = strconv.Itoa(node)
	logging.LogInfo("docker", "engine held to one NUMA node", "node", node, "cpus", cpus)
	return nil
}
