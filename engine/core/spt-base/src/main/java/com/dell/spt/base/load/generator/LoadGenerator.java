package com.dell.spt.base.load.generator;

import com.dell.spt.base.concurrent.Task;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import java.util.List;

/** Created on 11.07.16. */
public interface LoadGenerator<I extends Item, O extends Operation<I>> extends Task, AutoCloseable {

	/** Returns true when the item input has been fully consumed. */
	boolean isItemInputFinished();

	/** Returns the number of operations generated including recycled ones. */
	long generatedOpCount();

	/**
	 * Returns the number of input identities consumed while generating fresh operations.
	 *
	 * <p>The compatibility default preserves the historical one-item/one-operation contract
	 * for third-party generator implementations.
	 */
	default long consumedItemCount() {
		return generatedOpCount();
	}

	/**
	 * Enqueues the task for further recycling.
	 *
	 * @param op the task to recycle
	 */
	void recycle(final O op);

	/** Returns true when the recycle queue is currently empty. */
	boolean isNothingToRecycle();

	/**
	 * Enqueues an operation for a {@code load-op-retry} redispatch. Deliberately a separate
	 * path from {@link #recycle}: a retry is a re-attempt of an operation that was already
	 * counted once (at its original dispatch), not a new unit of work, so — unlike {@link
	 * #recycle}, which true recycle-mode workloads rely on counting toward {@code
	 * load-op-limit-count} to eventually terminate a duration-bound loop — a retry must not
	 * be gated by, or count against, that limit: an operation whose original dispatch
	 * happened to exhaust the configured count limit still deserves its full configured
	 * retry budget, not zero attempts. Implementations must also make sure a retry enqueued
	 * here is not abandoned if the generator was otherwise about to consider itself finished
	 * (see {@code LoadGeneratorImpl#isFinished()}).
	 *
	 * @param op the operation to redispatch
	 */
	default void retry(final O op) {
		recycle(op);
	}

	/**
	 * Returns {@code true} when there is currently no {@link #retry}-enqueued operation
	 * still waiting to be drained/redispatched. {@code LoadStepContextImpl} polls this
	 * during shutdown, after confirming no more retries can newly be enqueued, to make
	 * sure the generator's own work loop has actually had a chance to hand off whatever
	 * was already enqueued (to the storage driver) before the generator itself is
	 * stopped - {@link #retry} only enqueues; nothing about a caller's own bookkeeping
	 * (e.g. having awaited every retry-scheduling task's body finishing) proves this
	 * generator has since drained what those tasks enqueued into it.
	 */
	default boolean isNothingPendingRetry() {
		return true;
	}

	/**
	 * Atomically removes and returns every operation currently sitting in the {@link
	 * #retry} queue, without redispatching them. This is the last resort for {@code
	 * LoadStepContextImpl}'s shutdown sequence: {@link #isNothingPendingRetry} only lets
	 * it *ask* whether anything is still queued, and a bounded wait for it to become true
	 * can legitimately time out (e.g. the generator is stuck, output is backpressured, a
	 * throttle keeps denying permits, or the driver simply cannot accept the redispatch).
	 * On that timeout, the caller has no other way to retrieve the still-queued operations
	 * and give each a definite terminal outcome instead of letting {@link #close} silently
	 * discard them later with no outcome recorded at all - neither retried, nor redispatch
	 * attempted, nor counted as failed.
	 *
	 * @return the operations that were queued for retry, now removed from that queue
	 */
	default List<O> drainPendingRetries() {
		return List.of();
	}

	/**
	 * Legacy compatibility hook for the removed direct fast-recycle path.
	 *
	 * @deprecated always a no-op; completed operations must return through {@link #recycle}
	 */
	@Deprecated
	default void enableFastRecycleQuiesce() {}

	/**
	 * Returns {@code true} if {@link #recycle} actually requeues an operation for
	 * redispatch. Some generators (e.g. mixed-mode, which handles its own per-op-type
	 * recycling/pooling and treats {@link #recycle} as a deliberate no-op) don't support
	 * requeueing at all. {@code load-op-retry} depends on this being accurate: enabling retry
	 * against a generator that reports {@code true} here but doesn't really requeue would
	 * silently drop failed operations — neither retried nor counted as failed.
	 */
	default boolean supportsRetry() {
		return true;
	}

	/**
	 * Returns the terminal integrity/input failure observed by the generator task, if any.
	 *
	 * <p>The generator runs asynchronously, so throwing from its work loop alone cannot notify the
	 * load-step thread. This compatibility default lets implementations publish a typed failure for
	 * the step completion loop to rethrow without changing third-party generators.
	 */
	default IntegrityTerminalException terminalFailure() {
		return null;
	}
}
