package summary

import (
	"fmt"
	"strings"
)

// failureLimitEarlyEndMarginSeconds is how much shorter than its time limit a phase must be to
// count as ended early. A phase that ran to its limit reports a little over it (8.07 to 8.34 s
// for an 8 s limit), and a fixed margin, unlike a share of the limit, still catches a long phase
// stopped minutes early.
const failureLimitEarlyEndMarginSeconds = 1.0

// failureLimitStopWarning names a phase that the engine most likely stopped at its failure limit
// (load.op.limit.fail.count or load.op.limit.fail.rate). The run still exits 0 unless the phase
// had no successes. The CLI cannot see the engine's reason, so it fires only when the phase's
// failures pass its limit and the phase also ended before its own time or count limit, on a run
// the engine ended on its own.
//
// The failure test copies the engine's rule in LoadStepContextImpl.isFailThresholdReached
// (failures > fail.count, or a failure rate above the success rate with fail.rate on).
// mixed-fail-limit-first-type 01 and weighted-fail-limit-sub-load 01 change which counts that rule
// reads; the early-end test keeps this warning right before and after both.
func failureLimitStopWarning(label string, end RunEnd, step *StepData, operation string, metrics *PhaseMetrics) string {
	if end != RunEndCompleted || step == nil || metrics == nil || step.Limits == nil || step.MetricsSuppressed {
		return ""
	}
	// A standalone DELETE has no engine failure limit; it has its own --max-failed-objects budget.
	if step.Delete != nil || step.DeleteEvidence != nil || strings.EqualFold(operation, reportOperationDelete) {
		return ""
	}
	// The LIST workload is frozen until its refactor, and its successes count objects, not requests.
	if strings.EqualFold(operation, workloadTypeList) {
		return ""
	}
	limits := step.Limits
	failed := metrics.FailureCount
	if failed <= 0 {
		return ""
	}
	countReached := limits.FailCount > 0 && failed > limits.FailCount
	rateOn := limits.FailRate
	if !countReached && !rateOn {
		return ""
	}
	timeShort := limits.TimeSeconds > 0 &&
		metrics.DurationSeconds < limits.TimeSeconds-failureLimitEarlyEndMarginSeconds
	countShort := limits.OpCount > 0 && metrics.SuccessCount+failed < limits.OpCount
	if limits.TimeSeconds > 0 || limits.OpCount > 0 {
		// A phase that ran to its time limit or to its count ended on its own.
		if (limits.TimeSeconds > 0 && !timeShort) || (limits.OpCount > 0 && !countShort) {
			return ""
		}
	} else if !countReached {
		// With no own limit only the count can show the stop; the rate never can.
		return ""
	}
	progress := failureLimitProgress(limits, metrics)
	if countReached {
		if progress != "" {
			progress = ", " + progress
		}
		return fmt.Sprintf("%s: stopped at its failure limit after %s failed %s (load.op.limit.fail.count %s)%s",
			label, formatInt(failed), requestWord(failed), formatInt(limits.FailCount), progress)
	}
	return fmt.Sprintf("%s: ended %s with the failure-rate limit on (load.op.limit.fail.rate) and %s failed %s; "+
		"the engine stops a phase when failures outpace successes",
		label, progress, formatInt(failed), requestWord(failed))
}

// failureLimitProgress says how far into its own time or count limit the phase got; empty with neither.
func failureLimitProgress(limits *StepLimits, metrics *PhaseMetrics) string {
	switch {
	case limits.TimeSeconds > 0:
		return fmt.Sprintf("%s into its %s time limit", formatSeconds(metrics.DurationSeconds), formatSeconds(limits.TimeSeconds))
	case limits.OpCount > 0:
		return fmt.Sprintf("after %s of its %s requests", formatInt(metrics.SuccessCount+metrics.FailureCount), formatInt(limits.OpCount))
	}
	return ""
}
