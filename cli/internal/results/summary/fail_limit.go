package summary

import (
	"fmt"
	"strings"

	workloadreg "github.com/dell/storage-performance-tool/cli/internal/workload"
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
//
// It gives no warning where the limits can't be read reliably: a step bounded by
// load.step.limit.size, whose end the summary can't compare; a step with more than one metrics
// row outside an spt run mixed workload (a weighted or pipeline step, whose sub-loads have their
// own limits and whose config.yaml shows only the base); and, in a distributed step, any count
// limit, because the entry's config.yaml holds slice 0's share of load.op.limit.count and
// load.op.limit.fail.count while the metrics are the fleet's totals.
func failureLimitStopWarning(label string, end RunEnd, workloadType string, step *StepData, operation string, metrics *PhaseMetrics) string {
	if end != RunEndCompleted || step == nil || metrics == nil || step.Limits == nil || step.MetricsSuppressed {
		return ""
	}
	// No DELETE phase gets the warning: a standalone DELETE has no engine failure limit and its
	// own --max-failed-objects budget, and a --cleanup DELETE is best-effort and has no time or
	// count limit to show an early end.
	if step.Delete != nil || step.DeleteEvidence != nil || strings.EqualFold(operation, reportOperationDelete) {
		return ""
	}
	// The LIST workload is frozen until its refactor, and its successes count objects, not requests.
	if strings.EqualFold(operation, workloadTypeList) {
		return ""
	}
	limits := step.Limits
	if limits.SizeLimited {
		return ""
	}
	if step.Metrics != nil && len(step.Metrics.Rows) > 1 && !strings.EqualFold(workloadType, workloadreg.Mixed) {
		return ""
	}
	failed := metrics.FailureCount
	if failed <= 0 {
		return ""
	}
	if metrics.NodeCount > 1 {
		return distributedFailureRateWarning(label, limits, metrics)
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
	return failureRateWarning(label, progress, failed)
}

func failureRateWarning(label, progress string, failed int64) string {
	return fmt.Sprintf("%s: ended %s with the failure-rate limit on (load.op.limit.fail.rate) and %s failed %s; "+
		"the engine stops a phase when failures outpace successes",
		label, progress, formatInt(failed), requestWord(failed))
}

// distributedFailureRateWarning is the only form of the warning for a distributed step. Its
// config.yaml holds slice 0's share of the count limits, not the fleet's, so neither count is
// compared; the rate switch and the time limit aren't sliced, so a fleet that ended well before
// its time limit with the rate limit on and no count limit gets the hedged rate wording.
func distributedFailureRateWarning(label string, limits *StepLimits, metrics *PhaseMetrics) string {
	if !limits.FailRate || limits.OpCount > 0 || limits.TimeSeconds <= 0 ||
		metrics.DurationSeconds >= limits.TimeSeconds-failureLimitEarlyEndMarginSeconds {
		return ""
	}
	return failureRateWarning(label, failureLimitProgress(limits, metrics), metrics.FailureCount)
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
