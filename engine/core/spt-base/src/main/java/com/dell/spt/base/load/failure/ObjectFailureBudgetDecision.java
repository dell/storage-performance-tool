package com.dell.spt.base.load.failure;

/** One immutable controller evaluation. A breach is a stop trigger, not a hard cap. */
public record ObjectFailureBudgetDecision(
			ObjectFailureBudgetOutcome outcome,
			boolean stopScheduling,
			ObjectFailureBudgetConfig policy,
			ObjectFailureBudgetCounters counters,
			double observedFailurePercent,
			String reason) {}
