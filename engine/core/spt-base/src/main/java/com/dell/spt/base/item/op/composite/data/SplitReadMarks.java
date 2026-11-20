package com.dell.spt.base.item.op.composite.data;

/**
 * Sets and reads the split-read mark of a {@link CompositeDataOperationImpl}: whether the driver sent
 * the operation's sub-operations as the attempt's only requests. The operation's own passes then sent
 * no request and did no I/O, so its sub-operations' timings are the only request timings it has, and
 * the load step records theirs and none of its own. Only a READ is read for it.
 *
 * <p>The mark belongs to one attempt: {@link CompositeDataOperationImpl#reset()}, which admission calls
 * before each attempt, and {@link CompositeDataOperationImpl#resetSubOperations()}, which a whole retry
 * calls, clear it, and {@link CompositeDataOperationImpl#result()} copies it.
 *
 * <p>The methods live on this class, not on {@link CompositeDataOperationImpl}, so that its class
 * hierarchy declares no method of either name: no method a subclass, or an interface of a subclass,
 * already has under either name, from before 5.17.1, overrides, is called in place of, or is linked
 * to one of these.
 */
public final class SplitReadMarks {

	private SplitReadMarks() {}

	/** Whether a driver marked {@code op} with {@link #markRangesSentAsRequests(CompositeDataOperationImpl)}. */
	public static boolean rangesSentAsRequests(final CompositeDataOperationImpl<?> op) {
		return op.rangesSentAsRequests;
	}

	/**
	 * Called by a driver that sends {@code op}'s sub-operations as the attempt's only requests, as the
	 * {@code s3}, {@code s3-rdma} and {@code s3-aws} drivers do for a read split by the size threshold.
	 */
	public static void markRangesSentAsRequests(final CompositeDataOperationImpl<?> op) {
		op.rangesSentAsRequests = true;
	}
}
