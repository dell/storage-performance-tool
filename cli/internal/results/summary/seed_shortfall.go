package summary

import (
	"fmt"
	"math"

	"github.com/dell/storage-performance-tool/cli/internal/constants"
)

const seedOperationCreate = "create"

// seedShortfallWarning names a seed step that wrote fewer objects than it was asked to. The seed of
// spt run read and spt run mixed (PreconditionLoad) has its metrics suppressed, so neither its
// failures nor an engine stop at its failure limit reach the summary, though every later phase
// runs on the objects it wrote. laterPhases says whether any step follows it.
//
// It fires only for a suppressed, count-limited CREATE step (load.op.limit.count set, no time or
// size limit), whose selected count is the number of objects asked for: on a time-limited step
// selected is the generator's prefetch. Its counts come from operation.lifecycle.csv; with that
// file absent or unreadable (an engine before v5.15.0) there is no warning. A multipart seed
// (item.data.ranges.threshold set) gets none either: its rows count every request of an upload
// (the initiation, each part and the completion), and nothing in the file says which were whole
// objects. A row that selected more than config.yaml's load.op.limit.count, which holds the largest
// slice's share, is not counting objects either, so it also leaves the step without a warning.
//
// The failure-limit cause is checked per worker row against the share of load.op.limit.fail.count
// its slice can hold (seedSliceFailShare). It is named even on a failed run: a seed whose every PUT
// failed fails the run in the next step. Only the hedged failure-rate wording needs a run the engine
// is known to have ended on its own, since a Ctrl-C or auto-terminate also leaves objects not
// attempted.
func seedShortfallWarning(label string, end RunEnd, step *StepData, laterPhases bool) string {
	if step == nil || !step.MetricsSuppressed || step.Limits == nil || step.Lifecycle == nil || !step.Lifecycle.CountsKnown {
		return ""
	}
	limits := step.Limits
	if limits.OpType != seedOperationCreate || limits.OpCount <= 0 || limits.TimeSeconds > 0 || limits.SizeLimited || limits.Multipart {
		return ""
	}
	total := step.Lifecycle.Total
	if total.Selected <= 0 || total.Accepted >= total.Selected {
		return ""
	}
	rows := step.Lifecycle.Rows
	for _, row := range rows {
		if row.Selected > limits.OpCount {
			return ""
		}
	}
	noun := "objects"
	if total.Selected == 1 {
		noun = "object"
	}
	text := fmt.Sprintf("%s: wrote %s of %s %s (%s failed, %s not attempted)",
		label, formatInt(total.Accepted), formatInt(total.Selected), noun,
		formatInt(total.Failed), formatInt(total.Unattempted))
	switch {
	case seedFailCountReached(limits.FailCount, rows):
		if len(rows) > 1 {
			text += fmt.Sprintf(" and stopped at a node's failure limit (load.op.limit.fail.count of up to %s per node)", formatInt(limits.FailCount))
		} else {
			text += fmt.Sprintf(" and stopped at its failure limit (load.op.limit.fail.count %s)", formatInt(limits.FailCount))
		}
	case limits.FailRate && total.Failed > 0 && total.Unattempted > 0 && (end == RunEndCompleted || end == RunEndFailed):
		text += " with the failure-rate limit on (load.op.limit.fail.rate), which stops a step when failures outpace successes"
	}
	// With nothing written the next step fails on an empty items file, so no later phase ran on it.
	if laterPhases && total.Accepted > 0 {
		text += "; later phases used only the objects written"
	}
	return fmt.Sprintf("%s (%s)", text, constants.ResultsArtifactSuffixOperationLifecycle)
}

// seedFailCountReached reports whether a worker row stopped short (an operation it selected neither
// succeeded nor failed) after more failures than its slice's load.op.limit.fail.count can be.
// failCount is config.yaml's value, slice 0's share; the rows of the merged file are the slices in
// order. A row that ran every operation did not stop at a limit, whatever it failed.
func seedFailCountReached(failCount int64, rows []LifecycleCounts) bool {
	if failCount <= 0 {
		return false
	}
	for i, row := range rows {
		short := row.Accepted+row.Failed < row.Selected
		if short && row.Failed > seedSliceFailShare(failCount, len(rows), i) {
			return true
		}
	}
	return false
}

// seedSliceFailShare is the smallest share of load.op.limit.fail.count above 0 that slice of
// slices can hold when slice 0 holds share. The engine's ConfigSliceUtil.sliceLongValue gives each
// slice ceil(total / slices) while more than that remains, then the rest, then 0, which a slice
// reads as no limit; slice i holds min(share, max(0, total - i*share)). The total is not in the
// results, only that ceil(total / slices) is share, so it is somewhere from slices*(share-1)+1 to
// slices*share. Slice i's share grows with the total, and at slices*share every slice holds share,
// so its smallest share above 0 is min(share, max(1, (slices-i)*share - slices + 1)). For example
// 100 over 3 gives 34/34/32, 10 over 4 gives 3/3/3/1 and 20 over 6 gives 4/4/4/4/4/0.
func seedSliceFailShare(share int64, slices, slice int) int64 {
	if slices <= 1 || slice <= 0 {
		return share
	}
	left := int64(slices - slice)
	if left > 1 && share > (math.MaxInt64-int64(slices))/left {
		return share
	}
	return min(share, max(1, left*share-int64(slices)+1))
}
