package com.dell.spt.base.load.step.client;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.PreparationStatus;
import com.dell.spt.base.nic.pool.ItemSizeRecording;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * The Entry Node's side of the admission barrier of a step in which any engine has a NIC set: no
 * slice admits an operation until all are ready. A deadline, a failure or a stop fences every slice
 * for good. The Load Interval starts at the release, so preparation counts in no rate or duration.
 */
final class AdmissionBarrier {

	static final String TIMEOUT_SETTING = "load.step.preparation.timeout";

	private static final String ENTRY_DISTRIBUTING = "distributing items";

	private record Slice(String node, LoadStep step) {}

	private final String stepId;
	private final long id;
	private final String timeoutText;
	private final LongSupplier nanoTime;
	private final long pollMillis;
	private final long startNanos;
	private final long deadlineNanos;
	private final long sizeSummaryDeadlineNanos;
	private final List<Slice> slices = new ArrayList<>();
	private long distributionNanos;

	AdmissionBarrier(final String stepId, final long budgetNanos, final String timeoutText) {
		this(stepId, budgetNanos, timeoutText, System::nanoTime, 250);
	}

	AdmissionBarrier(
					final String stepId,
					final long budgetNanos,
					final String timeoutText,
					final LongSupplier nanoTime,
					final long pollMillis) {
		this.stepId = stepId;
		this.id = ThreadLocalRandom.current().nextLong();
		this.timeoutText = timeoutText;
		this.nanoTime = nanoTime;
		this.pollMillis = pollMillis;
		this.startNanos = nanoTime.getAsLong();
		this.deadlineNanos = startNanos + budgetNanos;
		this.sizeSummaryDeadlineNanos = startNanos + budgetNanos / ItemSizeRecording.BUDGET_DIVISOR;
	}

	/** The item-size summary's budget, which is smaller than this deadline and only ever makes a summary unknown. */
	ItemSizeRecording itemSizeRecording() {
		return new ItemSizeRecording(sizeSummaryDeadlineNanos, nanoTime);
	}

	long id() {
		return id;
	}

	/** Runs the Entry Node's item distribution within the deadline, and fences it at the deadline. */
	void distribute(final Runnable distribution) throws InterruptedException {
		final var task = new FutureTask<Void>(distribution, null);
		final var thread = Thread.ofPlatform().daemon().name("spt-item-distribution-" + stepId).start(task);
		try {
			task.get(remainingNanos(), TimeUnit.NANOSECONDS);
		} catch (final java.util.concurrent.TimeoutException e) {
			task.cancel(true);
			thread.interrupt();
			// A write in progress cannot be interrupted, but the slicers write no further batch.
			thread.join(TimeUnit.SECONDS.toMillis(5));
			throw deadlineFailure("entry: still " + ENTRY_DISTRIBUTING);
		} catch (final InterruptedException e) {
			task.cancel(true);
			thread.interrupt();
			throw e;
		} catch (final ExecutionException e) {
			final Throwable cause = e.getCause();
			if (cause instanceof RuntimeException runtime) {
				throw runtime;
			}
			if (cause instanceof Error error) {
				throw error;
			}
			throw new IllegalStateException(cause);
		}
		distributionNanos = nanoTime.getAsLong() - startNanos;
	}

	/** Arms the slice's next start as a barrier start. A worker without the methods fails the step. */
	void arm(final String node, final LoadStep slice) {
		try {
			slice.startPreparation(id, Math.max(1, remainingNanos()));
		} catch (final RemoteException e) {
			if (lacksTheMethods(e)) {
				throw unsupported(node, e);
			}
			throw failure("Step " + stepId + " did not start: " + node + ": " + e.getMessage(), e);
		}
		slices.add(new Slice(node, slice));
	}

	/**
	 * A worker's server wraps its own failure, so an unknown method reaches this side as a
	 * {@code ServerException} around an {@code UnmarshalException}.
	 */
	private static boolean lacksTheMethods(final RemoteException failure) {
		for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
			final String message = cause.getMessage();
			if (message != null && message.contains(LoadStep.NO_ADMISSION_BARRIER)) {
				return true;
			}
			if (cause instanceof UnmarshalException && message != null && message.contains("unrecognized method hash")) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Waits until every armed slice is ready and releases them, or fences them all and throws.
	 *
	 * @return the milliseconds each engine took, by {@code entry} or node address
	 */
	Map<String, Long> awaitAndRelease() throws InterruptedException {
		final Map<String, Long> millis = new LinkedHashMap<>();
		try {
			while (true) {
				final var failures = new ArrayList<String>();
				final var waiting = new ArrayList<String>();
				var deadlinePassed = false;
				millis.clear();
				for (final var slice : slices) {
					final PreparationStatus status = status(slice);
					millis.put(slice.node(), TimeUnit.NANOSECONDS.toMillis(status.elapsedNanos()));
					if (status.state() == PreparationStatus.State.RUNNING) {
						waiting.add(slice.node() + ": still " + status.phase());
					} else if (status.deadlinePassed()) {
						// A worker's own deadline ends a moment before this side's next poll.
						deadlinePassed = true;
						waiting.add(slice.node() + ": still " + status.phase());
					} else if (status.state() != PreparationStatus.State.READY) {
						failures.add(slice.node() + ": " + status.message());
					}
				}
				if (!failures.isEmpty()) {
					throw failure(
									"Step " + stepId + " did not start: " + failures.size() + " of " + slices.size()
													+ " engines failed preparation — " + String.join("\n", failures),
									null);
				}
				if (deadlinePassed || remainingNanos() <= 0) {
					throw deadlineFailure(waiting.isEmpty() ? "every engine became ready only after it" : String.join("; ", waiting));
				}
				if (waiting.isEmpty()) {
					break;
				}
				TimeUnit.MILLISECONDS.sleep(pollMillis);
			}
			release();
			return millis;
		} catch (final RuntimeException | InterruptedException | Error e) {
			fenceAll();
			throw e;
		}
	}

	private PreparationStatus status(final Slice slice) {
		try {
			return slice.step().preparationStatus(id);
		} catch (final RemoteException e) {
			throw failure("Step " + stepId + " did not start: " + slice.node() + ": " + e.getMessage(), e);
		}
	}

	private void release() {
		for (final var slice : slices) {
			final String cause;
			Throwable thrown = null;
			try {
				if (slice.step().releasePreparedAdmission(id)) {
					continue;
				}
				cause = "its preparation was fenced";
			} catch (final Exception e) {
				thrown = e;
				throw failure(
								"Step " + stepId + " stopped: admission could not be released on " + slice.node() + ": " + e.getMessage(),
								thrown);
			}
			throw failure("Step " + stepId + " stopped: admission could not be released on " + slice.node() + ": " + cause, null);
		}
	}

	/** Fences every armed slice. A slice that cannot be reached fences itself at its own deadline. */
	void fenceAll() {
		for (final var slice : slices) {
			try {
				slice.step().fencePreparation(id);
			} catch (final Exception e) {
				com.dell.spt.base.logging.Loggers.MSG.warn(
								"{}: could not fence the preparation on {}: {}", stepId, slice.node(), e.getMessage());
			}
		}
	}

	long distributionMillis() {
		return TimeUnit.NANOSECONDS.toMillis(distributionNanos);
	}

	long elapsedMillis() {
		return TimeUnit.NANOSECONDS.toMillis(nanoTime.getAsLong() - startNanos);
	}

	private long remainingNanos() {
		return deadlineNanos - nanoTime.getAsLong();
	}

	private RunTerminalException deadlineFailure(final String still) {
		return failure(
						"Step " + stepId + " did not start: preparation did not finish within " + timeoutText + " (" + TIMEOUT_SETTING
										+ ") — " + still,
						null);
	}

	private RunTerminalException unsupported(final String node, final Exception cause) {
		return failure(
						"Step " + stepId + " did not start: the engine on " + node + " does not support the admission barrier, which a "
										+ "step with a NIC set needs; run the same SPT image on every host",
						cause);
	}

	private RunTerminalException failure(final String message, final Throwable cause) {
		return new RunTerminalException(IntegrityTerminalException.Category.EXECUTION, stepId, message, cause);
	}
}
