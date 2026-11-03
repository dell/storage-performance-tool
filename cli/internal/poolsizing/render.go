package poolsizing

import (
	"fmt"
	"io"
	"strings"

	"github.com/dell/storage-performance-tool/cli/internal/nicplan"
)

// Finding is what spt verify concludes about one engine's pools: a short pool fails the check under
// fail and warns under warn.
type Finding struct {
	Host    string
	Message string
	Failure bool
}

// Findings gives the checks' results for every engine. An attached worker's limit is its own, so
// only its engine can check it.
func (p Plan) Findings() []Finding {
	var findings []Finding
	for _, engine := range p.Engines {
		switch {
		case engine.Failure != "":
			findings = append(findings, Finding{Host: engine.Host, Message: engine.Failure, Failure: true})
		case engine.Warning:
			findings = append(findings, Finding{Host: engine.Host, Message: engine.Message})
		case engine.Mode == ModeExplicit && engine.NeedBytes > engine.ExplicitBytes:
			message := fmt.Sprintf("%s needs %s of direct memory for its RDMA pools, but %s sets %s", engine.Host,
				format(engine.NeedBytes), "SPT_JAVA_OPTS", formatForJVM(engine.ExplicitBytes))
			findings = append(findings, Finding{Host: engine.Host, Message: message, Failure: engine.ShortfallMode == ShortfallFail})
		}
	}
	return findings
}

// Render writes each engine's pool lines for spt verify (item 16).
func (p Plan) Render(w io.Writer) {
	for _, engine := range p.Engines {
		if engine.Mode == ModeNoNicSet {
			continue
		}
		if engine.Mode == ModeAttached {
			_, _ = fmt.Fprintf(w, "%s\n", engine.Message)
			continue
		}
		_, _ = fmt.Fprintf(w, "RDMA pools on %s (shortfall: %s):\n", engine.Host, engine.ShortfallMode)
		output := p.Outputs[engine.Host]
		if output != nil && output.PoolDemand != nil {
			for _, step := range output.PoolDemand.Steps {
				renderStep(w, step)
			}
			renderChecks(w, engine, output.PoolDemand.Steps)
		} else {
			_, _ = fmt.Fprintf(w, "  No estimate: %s\n", orText(engine.Message, "the image gave none"))
		}
		renderCapacity(w, engine.Capacity)
		_, _ = fmt.Fprintf(w, "  MaxDirectMemorySize: %s\n", describeValue(engine))
		if output != nil && output.PoolDemand != nil {
			renderPrewarm(w, output.PoolDemand.Steps)
		}
	}
}

func renderStep(w io.Writer, step nicplan.PoolStep) {
	_, _ = fmt.Fprintf(w, "  Step %s: needs %s\n", step.ID, format(step.LimitBytes))
	for i, driver := range step.Drivers {
		for _, operation := range driver.Operations {
			_, _ = fmt.Fprintf(w, "    Driver %d %s: %s\n", i+1, operation.Op, describeOperation(operation))
		}
		for _, nic := range driver.Nics {
			if nic.Demand == "known" {
				_, _ = fmt.Fprintf(w, "      NIC %s: minimum %d, cap %d, need %s (%d x %s)\n", nic.Nic, nic.Minimum, nic.Cap,
					format(nic.NeedBytes), nic.Count, format(nic.SizeClassBytes))
			} else {
				_, _ = fmt.Fprintf(w, "      NIC %s: minimum %d, cap %d, demand %s\n", nic.Nic, nic.Minimum, nic.Cap, nic.Demand)
			}
		}
	}
	if len(step.Unknown) > 0 {
		_, _ = fmt.Fprintf(w, "    Unknown: %s\n", strings.Join(step.Unknown, "; "))
	}
}

func describeOperation(operation nicplan.PoolOperation) string {
	switch operation.Demand {
	case "known":
		sizes := format(operation.Smallest)
		if operation.Largest != operation.Smallest {
			sizes += " to " + format(operation.Largest)
		}
		return fmt.Sprintf("%s in the %s class, from %s", sizes, format(operation.SizeClass), operation.Source)
	case "unknown":
		return "unknown: " + operation.Reason
	default:
		if operation.Source != "" {
			return "zero, from " + operation.Source
		}
		return "zero"
	}
}

// renderChecks gives the two results apart, at the value the engine would run with.
func renderChecks(w io.Writer, engine Engine, steps []nicplan.PoolStep) {
	var pools, process, limit, held int64
	for _, step := range steps {
		held = maximum(held, step.PoolsNeedBytes)
		pools = maximum(pools, step.LimitForPoolsBytes)
		process = maximum(process, step.LimitForProcessBytes)
		limit = maximum(limit, step.LimitBytes)
	}
	at := engine.PassedBytes
	// Only an engine left at the shipped default runs at the floor: zero can be a user's own value.
	if at == 0 && engine.Mode != ModeExplicit {
		at = FloorBytes
	}
	_, _ = fmt.Fprintf(w, "  Checks at %s: each pool within its budget: %s (needs %s); the process limit: %s (needs %s); the steps need %s\n",
		formatForJVM(at), passOrShort(at >= pools), format(pools), passOrShort(at >= process), format(process), format(limit))
	_, _ = fmt.Fprintf(w, "  Pools hold: %s, which is what capacity is compared with\n", format(held))
}

func passOrShort(passed bool) string {
	if passed {
		return "passes"
	}
	return "short"
}

func renderCapacity(w io.Writer, capacity *CapacityRecord) {
	if capacity == nil {
		_, _ = fmt.Fprintln(w, "  Capacity: not read")
		return
	}
	source := "host memory"
	if capacity.Source == "cgroup" {
		source = "cgroup limit"
		if capacity.Cgroup != "" {
			source += " on " + capacity.Cgroup
		}
	}
	_, _ = fmt.Fprintf(w, "  Capacity: %s from %s of %s, less %s heap and a %s reserve\n", format(capacity.EffectiveBytes), source,
		format(capacity.MemoryBytes), format(capacity.HeapBytes), format(capacity.ReserveBytes))
	if capacity.Note != "" {
		_, _ = fmt.Fprintf(w, "  Capacity note: %s\n", capacity.Note)
	}
}

func describeValue(engine Engine) string {
	switch engine.Mode {
	case ModeSized:
		return "the CLI would set " + engine.Value()
	case ModeExplicit:
		return engine.Value() + ", set in SPT_JAVA_OPTS"
	case ModeZero:
		return "unchanged: no pool needs a buffer"
	default:
		return "unchanged: the shipped default"
	}
}

func renderPrewarm(w io.Writer, steps []nicplan.PoolStep) {
	var resident int64
	var skips []string
	for _, step := range steps {
		resident = maximum(resident, step.PrewarmResidentBytes)
		for _, driver := range step.Drivers {
			if !driver.Prewarm && driver.PrewarmSkip != "" && driver.PrewarmSkip != "no demand" {
				skips = append(skips, step.ID+": "+driver.PrewarmSkip)
			}
		}
	}
	if resident > 0 {
		_, _ = fmt.Fprintf(w, "  Pre-warm: makes %s resident\n", format(resident))
	}
	if len(skips) > 0 {
		_, _ = fmt.Fprintf(w, "  Pre-warm skipped, cold pools: %s\n", strings.Join(skips, "; "))
	}
	if resident == 0 && len(skips) == 0 {
		_, _ = fmt.Fprintln(w, "  Pre-warm: nothing to warm")
	}
}

func maximum(a, b int64) int64 {
	if b > a {
		return b
	}
	return a
}

func orText(value, fallback string) string {
	if value != "" {
		return value
	}
	return fallback
}
