package com.dell.spt.base.item.op;

/** Why an operation assembler is being finalized. */
public enum OperationAssemblyStopReason {
	/** The finite item input ended normally, so one retained tail may be dispatched. */
	NORMAL_COMPLETION,
	/** Step or driver admission closed; retained work must be recovered as unattempted. */
	ADMISSION_CLOSED,
	/** Assembly failed; retained work must be recovered rather than dispatched. */
	ABORTED
}
