package summary

import (
	"fmt"
	"slices"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/results"
)

// nicEventLoopsVaryByStep is the Event loops bullet of a run whose engines changed between steps.
const nicEventLoopsVaryByStep = "vary by step; see nicEngines in spt_run_params.json"

// nicEventLoopsOtherReports ends the Event loops bullet of a run with a report that was not read.
const nicEventLoopsOtherReports = "no readable report from the other engines"

// nicEventLoops describes the event loops of a run's NIC-set reports (D64): one value when every
// expected report was read and every step agrees, one per node when the engines differ or some
// reports were not read, since those engines may differ, and a pointer to the metadata when an
// engine changed between steps. A run without reports has none. Artifact Health and the warnings
// name the reports that were not read.
func nicEventLoops(engines map[string][]results.NicEngine, stepOrder []string, incomplete bool) string {
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
	if !incomplete && !slices.ContainsFunc(nodes, func(node int) bool { return byNode[node] != first }) {
		return strings.Replace(first, " ", " per engine ", 1)
	}
	parts := make([]string, 0, len(nodes))
	for _, node := range nodes {
		parts = append(parts, fmt.Sprintf("node-%03d %s", node, byNode[node]))
	}
	if incomplete {
		parts = append(parts, nicEventLoopsOtherReports)
	}
	return strings.Join(parts, "; ")
}

// nicReportsIncomplete reports whether an expected NIC-set report is missing or could not be read.
// A step without metrics totals has no expected reports: a read's seed step suppresses its totals
// by design, so its absence says nothing about the engines.
func nicReportsIncomplete(data *RunData) bool {
	if data.Params.NicEnginesError != "" {
		return true
	}
	for _, step := range data.Steps {
		if step != nil && len(step.NicSetMissing) > 0 {
			return true
		}
	}
	return false
}

// nicEnginesWarnings names the NIC-set reports that were fetched but could not be read; the fetch
// records absent ones under Artifact Health.
func nicEnginesWarnings(reason string) []string {
	if reason == "" {
		return nil
	}
	return []string{"NIC-set reports not read: " + strings.ReplaceAll(reason, "\n", "; ")}
}

// missingNicSetReports returns the NIC-set reports the step was expected to write, by the rules
// of its per-path CSVs, that its index does not list as fetched.
func missingNicSetReports(sm *results.StepManifest, nicNodes []results.NicNode, metrics *MetricsTotals) []results.NicNodeFile {
	return missingNicFiles(sm, results.ExpectedNicSetFiles(sm.StepID, nicNodes, nicStepParticipants(metrics)))
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

// nicEngineEventLoops is one engine's per-NIC event loops and how they are made up, such as
// "16 (2 NICs × 8, default)", followed by its base groups and the total it owns when the report
// has them: "16 (2 NICs × 8, default) + 2 base = 18". Drivers that disagree point to the
// metadata instead.
func nicEngineEventLoops(engine results.NicEngine) string {
	return nicEngineNicLoops(engine) + nicEngineBaseLoops(engine) + nicEngineOtherContexts(engine)
}

// nicEngineOtherContexts says that the step's drivers without a NIC set are not counted.
func nicEngineOtherContexts(engine results.NicEngine) string {
	switch engine.ContextsWithoutNicSet {
	case 0:
		return ""
	case 1:
		return ", not counting 1 driver without a NIC set"
	default:
		return fmt.Sprintf(", not counting %d drivers without a NIC set", engine.ContextsWithoutNicSet)
	}
}

func nicEngineBaseLoops(engine results.NicEngine) string {
	if engine.BaseEventLoops == nil || engine.TotalEventLoops == nil {
		return ""
	}
	return fmt.Sprintf(" + %d base = %d", *engine.BaseEventLoops, *engine.TotalEventLoops)
}

func nicEngineNicLoops(engine results.NicEngine) string {
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
