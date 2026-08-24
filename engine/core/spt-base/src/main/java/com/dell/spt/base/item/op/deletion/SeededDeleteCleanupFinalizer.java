package com.dell.spt.base.item.op.deletion;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.dell.spt.base.load.step.LoadStepBase;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;

/** Preserves the measured seeded DELETE verdict after best-effort residual cleanup. */
public final class SeededDeleteCleanupFinalizer {

	private static final String DEFAULT_STEP_ID = "seeded-delete-cleanup";

	private SeededDeleteCleanupFinalizer() {}

	/** Terminal best-effort cleanup counts and the retained cleanup-only failure, if any. */
	public record Outcome(
					long selectedOperations,
					long succeededOperations,
					long failedOperations,
					Throwable failure) {}

	/**
	 * Finalizes cleanup evidence without changing the benchmark verdict.
	 *
	 * <p>Externally caused interruption is always propagated; other cleanup-only failures are
	 * retained in the returned outcome.
	 *
	 * @param stepId cleanup step identifier used in the terminal log
	 * @param durationNanos elapsed cleanup duration
	 * @param benchmarkFailure measured benchmark failure to preserve and rethrow
	 * @param caughtCleanupFailure cleanup failure caught by the generated scenario
	 * @param cleanupStep configured cleanup load step, if construction succeeded
	 * @param residualFile measured residual manifest consumed by cleanup
	 * @return terminal cleanup counts and any cleanup-only failure
	 */
	public static Outcome finish(
					final String stepId,
					final long durationNanos,
					final Throwable benchmarkFailure,
					final Throwable caughtCleanupFailure,
					final LoadStepBase cleanupStep,
					final String residualFile) {
		rethrowIfInterrupted(benchmarkFailure);
		rethrowIfInterrupted(caughtCleanupFailure);
		final var resolvedStepId = stepId == null || stepId.isBlank() ? DEFAULT_STEP_ID : stepId;
		final var durationMillis = TimeUnit.NANOSECONDS.toMillis(Math.max(0, durationNanos));
		final Outcome outcome = outcome(cleanupStep, caughtCleanupFailure, residualFile);
		if (outcome.failure() == null) {
			Loggers.MSG.info(
							"{}: seeded DELETE cleanup completed in {} ms (selected={}, succeeded={}, failed={}); benchmark verdict unchanged",
							resolvedStepId,
							durationMillis,
							outcome.selectedOperations(),
							outcome.succeededOperations(),
							outcome.failedOperations());
		} else {
			LogUtil.exception(
							Level.WARN,
							outcome.failure(),
							"{}: seeded DELETE cleanup completed with errors after {} ms (selected={}, succeeded={}, failed={}); benchmark verdict unchanged",
							resolvedStepId,
							durationMillis,
							outcome.selectedOperations(),
							outcome.succeededOperations(),
							outcome.failedOperations());
		}
		if (benchmarkFailure != null) {
			throwUnchecked(benchmarkFailure);
		}
		return outcome;
	}

	private static Outcome outcome(
					final LoadStepBase cleanupStep,
					final Throwable caughtCleanupFailure,
					final String residualFile) {
		long selected = 0;
		long succeeded = 0;
		long failed = 0;
		Throwable failure = caughtCleanupFailure;
		try {
			selected = residualCount(residualFile);
		} catch (final Throwable residualFailure) {
			rethrowIfInterrupted(residualFailure);
			failure = combine(failure, residualFailure);
		}
		if (cleanupStep == null) {
			if (failure == null) {
				failure = new IllegalStateException("seeded DELETE cleanup step was not configured");
			}
			return new Outcome(selected, succeeded, failed, failure);
		}

		final Throwable runFailure = cleanupStep.runFailure();
		rethrowIfInterrupted(runFailure);
		failure = combine(failure, runFailure);
		try {
			for (final var snapshot : cleanupStep.metricsSnapshots()) {
				if (snapshot == null || snapshot.successSnapshot() == null || snapshot.failsSnapshot() == null) {
					throw new IllegalStateException("seeded DELETE cleanup terminal metrics are incomplete");
				}
				final long snapshotSucceeded = snapshot.successSnapshot().count();
				final long snapshotFailed = snapshot.failsSnapshot().count();
				if (snapshotSucceeded < 0 || snapshotFailed < 0) {
					throw new IllegalStateException("seeded DELETE cleanup terminal metrics are negative");
				}
				succeeded = Math.addExact(succeeded, snapshotSucceeded);
				failed = Math.addExact(failed, snapshotFailed);
			}
		} catch (final Throwable metricsFailure) {
			rethrowIfInterrupted(metricsFailure);
			failure = combine(failure, metricsFailure);
		}
		if (failed > 0) {
			failure = combine(
							failure,
							new IllegalStateException(
											"seeded DELETE cleanup completed with " + failed + " failed operations"));
		}
		try {
			if (Math.addExact(succeeded, failed) != selected) {
				failure = combine(
								failure,
								new IllegalStateException(
												"seeded DELETE cleanup terminal operation count does not match residual selection"));
			}
		} catch (final ArithmeticException countFailure) {
			failure = combine(failure, countFailure);
		}
		return new Outcome(selected, succeeded, failed, failure);
	}

	/**
	 * Propagates an externally caused interruption and otherwise returns normally.
	 *
	 * @param failure candidate failure caught at a scenario or cleanup boundary
	 */
	public static void rethrowIfInterrupted(final Throwable failure) {
		if (failure instanceof InterruptedException) {
			throwUnchecked(failure);
		}
	}

	private static long residualCount(final String residualFile) throws Exception {
		if (residualFile == null || residualFile.isBlank()) {
			throw new IllegalStateException("seeded DELETE cleanup residual path is missing");
		}
		long count = 0;
		try (var input = new IntegrityManifestItemInput(Path.of(residualFile))) {
			while (input.get() != null) {
				count = Math.addExact(count, 1);
			}
		}
		return count;
	}

	private static Throwable combine(final Throwable primary, final Throwable additional) {
		if (additional == null || additional == primary) {
			return primary;
		}
		if (primary == null) {
			return additional;
		}
		for (final Throwable suppressed : primary.getSuppressed()) {
			if (suppressed == additional) {
				return primary;
			}
		}
		primary.addSuppressed(additional);
		return primary;
	}
}
