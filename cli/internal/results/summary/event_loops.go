package summary

import (
	"fmt"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/results"
)

// eventLoopsVaryByStep is the Event loops bullet of a run whose event-loop reports changed between
// steps.
const eventLoopsVaryByStep = "vary by step; see eventLoopEngines in spt_run_params.json"

// runEventLoops is the run's Event loops bullet: its NIC-set reports' when it has any, which
// also name each NIC's group, otherwise its event-loop reports' (D88).
func runEventLoops(data *RunData) string {
	if loops := nicEventLoops(data.Params.NicEngines, data.StepOrder, nicReportsIncomplete(data)); loops != "" {
		return loops
	}
	return eventLoops(data.Params.EventLoopEngines, data.StepOrder, eventLoopReportsIncomplete(data))
}

// eventLoops describes a run's event-loop reports by the rules of nicEventLoops. An engine whose
// drivers own no event loops adds nothing, so a run of such drivers, or of engines that predate
// the report, has no bullet.
func eventLoops(engines map[string][]results.EventLoopEngine, stepOrder []string, incomplete bool) string {
	ordered := orderedSteps(engines, stepOrder)
	steps := make([][]nodeEventLoops, 0, len(ordered))
	for _, stepID := range ordered {
		var step []nodeEventLoops
		for _, engine := range engines[stepID] {
			if described := engineEventLoops(engine); described != "" {
				step = append(step, nodeEventLoops{node: engine.Node, described: described})
			}
		}
		steps = append(steps, step)
	}
	return eventLoopsByNode(steps, incomplete, eventLoopsVaryByStep)
}

// eventLoopReportsIncomplete reports whether an expected event-loop report is missing or could
// not be read.
func eventLoopReportsIncomplete(data *RunData) bool {
	if data.Params.EventLoopEnginesError != "" {
		return true
	}
	for _, step := range data.Steps {
		if step != nil && len(step.EventLoopsMissing) > 0 {
			return true
		}
	}
	return false
}

// eventLoopEnginesWarnings names the event-loop reports that were fetched but could not be read;
// Artifact Health names the absent node copies of a distributed step.
func eventLoopEnginesWarnings(reason string) []string {
	if reason == "" {
		return nil
	}
	return []string{"Event-loop reports not read: " + strings.ReplaceAll(reason, "\n", "; ")}
}

// engineEventLoops is one engine's event loops and where they came from, such as
// "24 (default, 192 hardware threads)", "12 (storage.driver.threads)",
// "98 (NIC set, default, 192 hardware threads)" or, for drivers that agree,
// "48 (2 drivers × 24, default, 192 hardware threads)". Drivers that differ point to the
// metadata. An engine whose drivers own none has no description.
func engineEventLoops(engine results.EventLoopEngine) string {
	if len(engine.Contexts) == 0 {
		return ""
	}
	first := engine.Contexts[0]
	for _, context := range engine.Contexts[1:] {
		if context.Route != first.Route || context.Source != first.Source || context.EventLoops != first.EventLoops {
			return fmt.Sprintf("%d (%d drivers; see eventLoopEngines in spt_run_params.json)",
				engine.EventLoops, len(engine.Contexts))
		}
	}
	var parts []string
	if n := len(engine.Contexts); n > 1 {
		parts = append(parts, fmt.Sprintf("%d drivers × %d", n, first.EventLoops))
	}
	if first.Route == results.EventLoopRouteNicSet {
		parts = append(parts, "NIC set")
	}
	switch first.Source {
	case results.EventLoopSourceDefault:
		parts = append(parts, "default", fmt.Sprintf("%d hardware threads", engine.HardwareThreads))
	case "":
		parts = append(parts, "source not reported")
	default:
		parts = append(parts, first.Source)
	}
	return fmt.Sprintf("%d (%s)", engine.EventLoops, strings.Join(parts, ", "))
}
