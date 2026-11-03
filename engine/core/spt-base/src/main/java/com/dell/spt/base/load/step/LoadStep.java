package com.dell.spt.base.load.step;

import com.dell.spt.base.concurrent.Daemon;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import com.dell.spt.base.concurrent.AsyncRunnable;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public interface LoadStep extends Daemon {
	/** The message of the admission barrier methods' defaults, which the Entry Node recognizes. */
	String NO_ADMISSION_BARRIER = "worker does not support the admission barrier";

	/** Returns the unique identifier for this step. */
	String loadStepId() throws RemoteException;

	long runId() throws RemoteException;

	String getTypeName() throws RemoteException;

	List<? extends AllMetricsSnapshot> metricsSnapshots() throws RemoteException;

	/** Publishes worker object counters for the controller; compatibility workers provide no evidence. */
	default DeleteObjectLifecycleSnapshot deleteObjectLifecycle() throws RemoteException {
		return null;
	}

	/** Releases controller-gated standalone DELETE count scheduling. */
	default void releaseObjectFailureBudgetAdmission() throws RemoteException {}

	/** Prepares the worker-local standalone DELETE interval without releasing scheduling. */
	default void prepareDurationInterval(final long durationNanos) throws RemoteException {}

	/** Arms the worker-local standalone DELETE duration interval and releases scheduling. */
	default void startDurationInterval(final long durationNanos) throws RemoteException {}

	/** Closes operation admission for a coordinated duration stop without recovering work. */
	default void closeOperationAdmissionForStepStop() throws RemoteException {}

	/**
	 * Tightens the dispatched-operation terminal deadline before admission closure or recovery.
	 * Compatibility workers must fail closed rather than silently accepting a later completion.
	 */
	default void enforceDispatchedOperationsDeadlineForStepStop(final long remainingNanos)
					throws RemoteException {
		throw new RemoteException(
						"worker does not support the coordinated dispatched-operation deadline");
	}

	/** Recovers undispatched work after every distributed slice has closed admission. */
	default void recoverQueuedOperationsForStepStop() throws RemoteException {}

	/** Drains dispatched work using the supplied remaining nanosecond budget. */
	default void drainDispatchedOperationsForStepStop(final long remainingNanos)
					throws RemoteException {}

	/** Starts the potentially long drain without holding a distributed control-plane RPC open. */
	default void startDispatchedOperationsDrainForStepStop(final long remainingNanos)
					throws RemoteException {
		drainDispatchedOperationsForStepStop(remainingNanos);
	}

	/** Returns whether the asynchronously started drain has actually completed. */
	default boolean isDispatchedOperationsDrainCompleteForStepStop() throws RemoteException {
		return true;
	}

	/** Audits deterministic terminal accounting after the dispatched-operation drain. */
	default void validateTerminalStateForStepStop() throws RemoteException {}

	/** Starts idempotent full DELETE pre-validation while timed admission remains held. */
	default void startDeleteInventoryPreValidation() throws RemoteException {}

	/** Polls full DELETE pre-validation without holding a long-lived control-plane RPC. */
	default boolean isDeleteInventoryPreValidationComplete() throws RemoteException {
		return true;
	}

	/** Prevents post-verification after strict pre-validation failed on any distributed slice. */
	default void skipDeleteInventoryPostVerificationAfterStrictPreValidationFailure()
					throws RemoteException {
		throw new RemoteException(
						"worker does not support distributed strict pre-validation abort propagation");
	}

	/** Starts idempotent post-delete inventory verification after every dispatched request drains. */
	default void verifyDeleteInventoryForStepStop() throws RemoteException {}

	/** Polls post-delete verification without holding a long-lived control-plane RPC. */
	default boolean isDeleteInventoryVerificationCompleteForStepStop() throws RemoteException {
		return true;
	}

	/**
	 * Returns worker-local duration validity evidence after operation admission has closed.
	 * Compatibility implementations which do not provide evidence fail closed at the controller.
	 */
	default DurationAwaitStatus durationAwaitStatus() throws RemoteException {
		return DurationAwaitStatus.NOT_STARTED;
	}

	/**
	 * Arms the next {@link #start()} as an admission-barrier start: it holds admission, prepares on
	 * its own thread and returns. The worker fences itself when the budget runs out, on its own
	 * clock, even if the Entry Node is gone.
	 *
	 * @param preparationId a token the Entry Node draws for the step
	 */
	default void startPreparation(final long preparationId, final long budgetNanos) throws RemoteException {
		throw new RemoteException(NO_ADMISSION_BARRIER);
	}

	/** Polls the preparation; a stale token reads as fenced. */
	default PreparationStatus preparationStatus(final long preparationId) throws RemoteException {
		throw new RemoteException(NO_ADMISSION_BARRIER);
	}

	/**
	 * Opens admission and starts the Load Interval, once. Returns true once it is released, and
	 * false, changing nothing, for a stale token or a preparation that is not ready.
	 */
	default boolean releasePreparedAdmission(final long preparationId) throws RemoteException {
		throw new RemoteException(NO_ADMISSION_BARRIER);
	}

	/**
	 * Fences the preparation for good and starts stopping the contexts. It waits out a release in
	 * progress but not the contexts' stop. Idempotent.
	 */
	default void fencePreparation(final long preparationId) throws RemoteException {
		throw new RemoteException(NO_ADMISSION_BARRIER);
	}

	@Override
	AsyncRunnable start() throws RemoteException;

	@Override
	AsyncRunnable await() throws InterruptedException, RemoteException;

	@Override
	boolean await(final long timeout, final TimeUnit timeUnit)
					throws InterruptedException, RemoteException;

	@Override
	AsyncRunnable stop() throws RemoteException;

	@Override
	void close() throws IOException;
}
