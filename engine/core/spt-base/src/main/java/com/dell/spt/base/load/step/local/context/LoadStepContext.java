package com.dell.spt.base.load.step.local.context;

import com.dell.spt.base.concurrent.Daemon;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import com.dell.spt.base.item.op.deletion.DeletePhaseTimingSnapshot;
import com.dell.spt.base.load.lifecycle.OperationLifecycleSnapshot;
import com.dell.spt.base.concurrent.AsyncRunnable;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.io.IOException;
import java.util.OptionalLong;

/** Created on 11.07.16. */
public interface LoadStepContext<I extends Item, O extends Operation<I>> extends Daemon, Output<O> {

	void operationsResultsOutput(final Output<O> opsResultsOutput);

	void operationsMetricsOutput(final Output<O> opsMetricsOutput);

	int activeOpCount();

	/** Returns lifecycle counts and recoverable terminal identities for this step. */
	default OperationLifecycleSnapshot<O> operationLifecycle() {
		return new OperationLifecycleSnapshot<>(0, 0, 0, 0, 0, 0, 0, java.util.List.of(), java.util.List.of());
	}

	/** Returns object-level identity accounting, or {@code null} when the context cannot supply it. */
	default DeleteObjectLifecycleSnapshot deleteObjectLifecycle() {
		return null;
	}

	/** Returns scheduled/admission and post-admission drain timing for standalone DELETE. */
	default DeletePhaseTimingSnapshot deletePhaseTiming() {
		return DeletePhaseTimingSnapshot.empty();
	}

	/** Holds standalone DELETE count scheduling until every controller slice is ready. */
	default void holdObjectFailureBudgetAdmission() {
		throw unsupportedStandaloneDeleteCountAdmission();
	}

	/** Releases standalone DELETE count scheduling after the controller start barrier. */
	default void releaseObjectFailureBudgetAdmission() {
		throw unsupportedStandaloneDeleteCountAdmission();
	}

	private static UnsupportedOperationException unsupportedStandaloneDeleteCountAdmission() {
		return new UnsupportedOperationException("standalone DELETE count admission is unsupported");
	}

	/**
	 * Returns the source-monotonic inventory exhaustion transition, using {@link Long#MAX_VALUE} as
	 * the compatibility sentinel. Use {@link #schedulingExhaustionNanos()} for explicit presence.
	 */
	default long schedulingExhaustedAtNanos() {
		return Long.MAX_VALUE;
	}

	/**
	 * Returns the scheduling-exhaustion transition with explicit presence.
	 *
	 * <p>The compatibility default adapts the historical {@link Long#MAX_VALUE} sentinel.
	 */
	default OptionalLong schedulingExhaustionNanos() {
		final long exhaustedAtNanos = schedulingExhaustedAtNanos();
		return exhaustedAtNanos == Long.MAX_VALUE
						? OptionalLong.empty()
						: OptionalLong.of(exhaustedAtNanos);
	}

	/** Releases duration scheduling against a step-local monotonic interval. */
	default void startDurationInterval(final long startNanos, final long deadlineNanos) {}

	/** Fails closed after resources stop if standalone DELETE terminal accounting is incomplete. */
	default void validateTerminalState() {}

	/** Runs idempotent full post-delete verification after the dispatched-operation drain. */
	default void verifyDeleteInventoryAfterDrain() {}

	/** Runs full strict inventory validation while operation admission remains held. */
	default void validateDeleteInventoryBeforeAdmission() {}

	/** Records a distributed strict-pre abort so this locally passing slice skips post-verification. */
	default void skipDeleteInventoryPostVerificationAfterStrictPreValidationFailure() {}

	/** Closes scheduling and driver admission without waiting for dispatched operations. */
	default void closeOperationAdmissionForStepStop() {}

	/** Recovers work which did not cross the driver-dispatch boundary. */
	default void recoverQueuedOperationsForStepStop() {}

	/** Arms the absolute worker-local cutoff for dispatched terminal outcomes. */
	default void enforceDispatchedOperationsDeadlineForStepStop(final long deadlineNanos) {}

	/** Resolves dispatched work still outstanding at the worker-local cutoff. */
	default void expireDispatchedOperationsDeadlineForStepStop() {}

	/** Drains dispatched work against the step-wide absolute monotonic deadline. */
	default void drainDispatchedOperationsForStepStop(final long deadlineNanos) {}

	boolean isDone();

	@Override
	default Input<O> getInput() {
		throw new AssertionError("Shouldn't be invoked");
	}

	@Override
	AsyncRunnable stop();

	@Override
	void close() throws IOException;
}
