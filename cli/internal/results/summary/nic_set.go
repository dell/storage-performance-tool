package summary

import (
	"fmt"
	"slices"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/results"
)

// nicEventLoopsVaryByStep is the Event loops bullet of a run whose engines changed between steps.
const nicEventLoopsVaryByStep = "vary by step; see nicEngines in spt_run_params.json"

// nicEventLoops describes the event loops of a run's NIC-set reports (D64): one value when every
// report of every step agrees, one per node when only the engines differ, and a pointer to the
// metadata when an engine changed between steps. A run without reports has none, and a missing
// report adds nothing, since Artifact Health names its node.
func nicEventLoops(engines map[string][]results.NicEngine, stepOrder []string) string {
	byNode := map[int]string{}
	var nodes []int
	for _, stepID := range orderedNicSteps(engines, stepOrder) {
		for _, engine := range engines[stepID] {
			described := nicEngineEventLoops(engine)
			previous, seen := byNode[engine.Node]
			switch {
			case !seen:
				byNode[engine.Node] = described
				nodes = append(nodes, engine.Node)
			case previous != described:
				return nicEventLoopsVaryByStep
			}
		}
	}
	if len(nodes) == 0 {
		return ""
	}
	slices.Sort(nodes)
	first := byNode[nodes[0]]
	if !slices.ContainsFunc(nodes, func(node int) bool { return byNode[node] != first }) {
		return strings.Replace(first, " ", " per engine ", 1)
	}
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		parts = append(parts, fmt.Sprintf("node-%03d %s", node, byNode[node]))
	}
	return strings.Join(parts, "; ")
}

// orderedNicSteps lists the steps with reports: the run's step order, then any others sorted.
func orderedNicSteps(engines map[string][]results.NicEngine, stepOrder []string) []string {
	out := make([]string, 0, len(engines))
	for _, stepID := range stepOrder {
		if _, ok := engines[stepID]; ok && !slices.Contains(out, stepID) {
			out = append(out, stepID)
		}
	}
	var rest []string
	for stepID := range engines {
		if !slices.Contains(out, stepID) {
			rest = append(rest, stepID)
		}
	}
	slices.Sort(rest)
	return append(out, rest...)
}

// nicEngineEventLoops is one engine's total and how it is made up, such as
// "16 (2 NICs × 8, default)". Drivers that disagree point to the metadata instead.
func nicEngineEventLoops(engine results.NicEngine) string {
	if engine.DriverThreads == nil || engine.EventLoopsPerNic == nil || engine.Nics == nil {
		return fmt.Sprintf("%d (%d drivers; see nicEngines in spt_run_params.json)", engine.EventLoops, engine.Drivers)
	}
	source := "default"
	if *engine.DriverThreads > 0 {
		source = "storage.driver.threads"
	}
	nics := fmt.Sprintf("%d NICs", len(engine.Nics))
	if len(engine.Nics) == 1 {
		nics = "1 NIC"
	}
	if engine.Drivers > 1 {
		nics = fmt.Sprintf("%d drivers × %s", engine.Drivers, nics)
	}
	return fmt.Sprintf("%d (%s × %d, %s)", engine.EventLoops, nics, *engine.EventLoopsPerNic, source)
}
