package summary

import (
	"fmt"

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
// file absent or unreadable (an engine before v5.15.0) there is no warning.
//
// The failure-limit cause is checked per worker row, because a distributed step's config.yaml
// holds slice 0's share of load.op.limit.fail.count (the ceiling of the total over the nodes) and
// the other slices may have one less. It is named even on a failed run: a seed whose every PUT
// failed fails the run in the next step. Only the hedged failure-rate wording needs a run the
// engine ended on its own, since a Ctrl-C or auto-terminate also leaves objects not attempted.
func seedShortfallWarning(label string, end RunEnd, step *StepData, laterPhases bool) string {
	if step == nil || !step.MetricsSuppressed || step.Limits == nil || step.Lifecycle == nil || !step.Lifecycle.CountsKnown {
		return ""
	}
	limits := step.Limits
	if limits.OpType != seedOperationCreate || limits.OpCount <= 0 || limits.TimeSeconds > 0 || limits.SizeLimited {
		return ""
	}
	total := step.Lifecycle.Total
	if total.Selected <= 0 || total.Accepted >= total.Selected {
		return ""
	}
	noun := "objects"
	if total.Selected == 1 {
		noun = "object"
	}
	text := fmt.Sprintf("%s: wrote %s of %s %s (%s failed, %s not attempted)",
		label, formatInt(total.Accepted), formatInt(total.Selected), noun,
		formatInt(total.Failed), formatInt(total.Unattempted))
	switch rows := step.Lifecycle.Rows; {
	case seedFailCountReached(limits.FailCount, rows):
		if len(rows) > 1 {
			text += fmt.Sprintf(" and stopped at a node's failure limit (load.op.limit.fail.count of up to %s per node)", formatInt(limits.FailCount))
		} else {
			text += fmt.Sprintf(" and stopped at its failure limit (load.op.limit.fail.count %s)", formatInt(limits.FailCount))
		}
	case limits.FailRate && total.Failed > 0 && total.Unattempted > 0 && (end == RunEndCompleted || end == RunEndFailed):
		text += " with the failure-rate limit on (load.op.limit.fail.rate), which stops a step when failures outpace successes"
	}
	if laterPhases {
		text += "; later phases used only the objects written"
	}
	return fmt.Sprintf("%s (%s)", text, constants.ResultsArtifactSuffixOperationLifecycle)
}

// seedFailCountReached reports whether any worker row failed more than its slice's
// load.op.limit.fail.count. failCount is slice 0's share; with more than one row the other slices
// may have one less, so a row stops above failCount-1 (but never at 0, which is no limit).
func seedFailCountReached(failCount int64, rows []LifecycleCounts) bool {
	if failCount <= 0 {
		return false
	}
	limit := failCount
	if len(rows) > 1 && failCount > 1 {
		limit = failCount - 1
	}
	for _, row := range rows {
		if row.Failed > limit {
			return true
		}
	}
	return false
}
