package com.dell.spt.base.item.op.deletion;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.io.IntegrityManifestItemInput;
import com.dell.spt.base.logging.Loggers;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Performs one complete inventory pass followed by a bounded settle loop. The first retry pass
 * follows the complete pass at once; later passes back off exponentially, so a probe that fails at
 * once (a closed or refused connection) is not retried at the pause rate for the whole timeout.
 */
public final class DeleteInventoryVerifier {
	/** The pause after the first retry pass, doubled after each later pass. */
	static final long FIRST_RETRY_PAUSE_MILLIS = 10;
	/** The longest pause between retry passes, short against an eventual-consistency window. */
	static final long MAX_RETRY_PAUSE_MILLIS = 1_000;

	private DeleteInventoryVerifier() {}

	public static DeleteVerificationReport verify(
					final Path selectionManifest,
					final long selectedCount,
					final DeleteVerificationPhase phase,
					final Duration retryTimeout,
					final DeleteVerificationProbe probe) throws IOException {
		Objects.requireNonNull(selectionManifest, "selectionManifest");
		Objects.requireNonNull(phase, "phase");
		Objects.requireNonNull(retryTimeout, "retryTimeout");
		Objects.requireNonNull(probe, "probe");
		if (selectedCount < 0) {
			throw new IllegalArgumentException("DELETE verification count is outside supported bounds");
		}
		if (retryTimeout.isNegative()) {
			throw new IllegalArgumentException("DELETE verification timeout must be nonnegative");
		}
		throwIfInterrupted();

		final long started = System.nanoTime();
		Path results = null;
		try {
			results = Files.createTempFile("spt-delete-verification-", ".bin");
			final var causes = new UnresolvedCause();
			completePass(selectionManifest, selectedCount, probe, results, causes);

			final long retryNanos;
			try {
				retryNanos = retryTimeout.toNanos();
			} catch (final ArithmeticException overflow) {
				throw new IllegalArgumentException("DELETE verification timeout is too large", overflow);
			}
			final long deadline = retryNanos >= Long.MAX_VALUE - started
							? Long.MAX_VALUE
							: started + retryNanos;
			int retryPasses = 0;
			while (hasRetryable(phase, results, selectedCount) && System.nanoTime() < deadline) {
				retryPass(selectionManifest, phase, deadline, probe, results, selectedCount, causes);
				retryPasses++;
				if (hasRetryable(phase, results, selectedCount)) {
					final long remainingNanos = deadline - System.nanoTime();
					if (remainingNanos > 0) {
						try {
							TimeUnit.NANOSECONDS.sleep(Math.min(
											remainingNanos,
											TimeUnit.MILLISECONDS.toNanos(retryPauseMillis(retryPasses))));
						} catch (final InterruptedException interrupted) {
							Thread.currentThread().interrupt();
							throwUnchecked(interrupted);
						}
					}
				}
			}
			final var report = new DeleteVerificationReport(
							phase, results, selectedCount, true, System.nanoTime() - started);
			// One WARN per phase, after its last pass, so a target that settles late is not reported.
			final long unresolved = report.unresolved();
			if (unresolved > 0) {
				Loggers.MSG.warn(unresolvedWarning(
								phase, unresolved, retryPasses + 1, report.elapsedNanos(), causes.last));
			}
			results = null;
			return report;
		} finally {
			if (results != null) {
				Files.deleteIfExists(results);
			}
		}
	}

	private static void completePass(
					final Path selectionManifest,
					final long selectedCount,
					final DeleteVerificationProbe probe,
					final Path results,
					final UnresolvedCause causes) throws IOException {
		try (var input = new IntegrityManifestItemInput(selectionManifest);
						OutputStream output = new BufferedOutputStream(Files.newOutputStream(results))) {
			long index = 0;
			for (IntegrityManifestDataItem item = input.get(); item != null; item = input.get()) {
				throwIfInterrupted();
				if (index >= selectedCount) {
					throw new IOException("DELETE verification inventory exceeds its frozen count");
				}
				final DeleteTarget target = new DeleteTarget(item, index);
				output.write(DeleteVerificationReport.encode(safePresence(probe, target, causes)));
				index++;
			}
			if (index != selectedCount) {
				throw new IOException(
								"DELETE verification inventory row count " + index
												+ " does not match frozen count " + selectedCount);
			}
		}
	}

	private static void retryPass(
					final Path selectionManifest,
					final DeleteVerificationPhase phase,
					final long deadline,
					final DeleteVerificationProbe probe,
					final Path results,
					final long selectedCount,
					final UnresolvedCause causes) throws IOException {
		try (var input = new IntegrityManifestItemInput(selectionManifest);
						var ledger = new RandomAccessFile(results.toFile(), "rw")) {
			long index = 0;
			for (IntegrityManifestDataItem item = input.get(); item != null; item = input.get()) {
				throwIfInterrupted();
				if (index >= selectedCount || System.nanoTime() >= deadline) {
					return;
				}
				ledger.seek(index);
				if (retryable(phase, DeleteVerificationReport.decode(ledger.readByte()))) {
					final DeleteTarget target = new DeleteTarget(item, index);
					ledger.seek(index);
					ledger.write(DeleteVerificationReport.encode(safePresence(probe, target, causes)));
				}
				index++;
			}
		}
	}

	private static DeleteVerificationProbe.Presence safePresence(
					final DeleteVerificationProbe probe,
					final DeleteTarget target,
					final UnresolvedCause causes) {
		try {
			final var observation = probe.observe(target);
			final var value = observation == null ? null : observation.presence();
			if (value == null || value == DeleteVerificationProbe.Presence.UNRESOLVED) {
				causes.record(observation == null ? null : observation.unresolvedCause());
				return DeleteVerificationProbe.Presence.UNRESOLVED;
			}
			return value;
		} catch (final RuntimeException failure) {
			causes.record(failure.toString());
			return DeleteVerificationProbe.Presence.UNRESOLVED;
		}
	}

	/**
	 * The pause after retry pass {@code retryPass} (1-based): {@value #FIRST_RETRY_PAUSE_MILLIS} ms,
	 * doubled per pass up to {@value #MAX_RETRY_PAUSE_MILLIS} ms.
	 */
	static long retryPauseMillis(final int retryPass) {
		// Ten doublings already pass the cap, and bounding the shift keeps it from overflowing.
		final int doublings = Math.clamp(retryPass - 1L, 0, 10);
		return Math.min(FIRST_RETRY_PAUSE_MILLIS << doublings, MAX_RETRY_PAUSE_MILLIS);
	}

	static String unresolvedWarning(
					final DeleteVerificationPhase phase,
					final long unresolved,
					final int passes,
					final long elapsedNanos,
					final String lastCause) {
		return "Standalone DELETE " + phaseName(phase) + " left " + unresolved
						+ " selected identity/identities unresolved after " + passes + " pass(es) in "
						+ TimeUnit.NANOSECONDS.toMillis(elapsedNanos) + " ms; last cause: "
						+ (lastCause == null || lastCause.isBlank() ? "not reported by the storage driver" : lastCause);
	}

	private static String phaseName(final DeleteVerificationPhase phase) {
		return phase == DeleteVerificationPhase.PRE_DELETE ? "pre-validation" : "post-verification";
	}

	/** The most recent reason a probe left a target unresolved during one verification phase. */
	private static final class UnresolvedCause {
		private String last;

		void record(final String cause) {
			if (cause != null && !cause.isBlank()) {
				last = cause;
			}
		}
	}

	private static boolean hasRetryable(
					final DeleteVerificationPhase phase,
					final Path results,
					final long selectedCount) throws IOException {
		throwIfInterrupted();
		try (InputStream input = new BufferedInputStream(Files.newInputStream(results))) {
			for (long index = 0; index < selectedCount; index++) {
				throwIfInterrupted();
				final int presence = input.read();
				if (presence < 0) {
					throw new IOException("DELETE verification evidence ended before its frozen count");
				}
				throwIfInterrupted();
				if (retryable(phase, DeleteVerificationReport.decode((byte) presence))) {
					return true;
				}
			}
		}
		throwIfInterrupted();
		return false;
	}

	private static void throwIfInterrupted() {
		if (Thread.currentThread().isInterrupted()) {
			throwUnchecked(new InterruptedException("DELETE verification interrupted"));
		}
	}

	private static boolean retryable(
					final DeleteVerificationPhase phase,
					final DeleteVerificationProbe.Presence presence) {
		return presence == DeleteVerificationProbe.Presence.UNRESOLVED
						|| (phase == DeleteVerificationPhase.POST_DELETE
										&& presence == DeleteVerificationProbe.Presence.PRESENT);
	}
}
