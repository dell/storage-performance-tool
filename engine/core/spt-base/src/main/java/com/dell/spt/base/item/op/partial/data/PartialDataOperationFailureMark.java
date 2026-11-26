package com.dell.spt.base.item.op.partial.data;

/**
 * The mark a driver sets on a {@link PartialDataOperationImpl} out of retries once its completion has
 * recorded the part's failure on its parent, which may then end. A driver sets it, and reads it,
 * holding the parent's monitor. The mark is never cleared, and {@link PartialDataOperationImpl#result()}
 * does not copy it: a retried or recycled composite makes new parts.
 *
 * <p>These methods are on a class of their own, so that {@code PartialDataOperationImpl}, its
 * supertypes and its subclasses have no method of these names that an extension's own could
 * override, collide with, or be resolved to.
 */
public final class PartialDataOperationFailureMark {

	private PartialDataOperationFailureMark() {}

	/** Marks {@code part} as out of retries with its failure recorded on its parent. */
	public static void markFailureRecorded(final PartialDataOperationImpl<?> part) {
		part.failureRecorded = true;
	}

	/** Whether {@link #markFailureRecorded} was called for {@code part}. */
	public static boolean failureRecorded(final PartialDataOperationImpl<?> part) {
		return part.failureRecorded;
	}
}
