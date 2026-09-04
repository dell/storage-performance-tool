package com.dell.spt.base.load.failure;

/** Controller outcome for the standalone DELETE failed-object policy. */
public enum ObjectFailureBudgetOutcome {
	RUNNING, COMPLETED_CLEANLY, COMPLETED_WITHIN_BUDGET, FAILED
}
