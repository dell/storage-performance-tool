package com.dell.spt.base.item.op.deletion;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.io.IntegrityManifestItemInput;
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

/** Performs one complete inventory pass followed by a bounded settle loop. */
public final class DeleteInventoryVerifier {
	private static final long RETRY_PAUSE_MILLIS = 10;

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

		final long started = System.nanoTime();
		Path results = null;
		try {
			results = Files.createTempFile("spt-delete-verification-", ".bin");
			completePass(selectionManifest, selectedCount, probe, results);

			final long retryNanos;
			try {
				retryNanos = retryTimeout.toNanos();
			} catch (final ArithmeticException overflow) {
				throw new IllegalArgumentException("DELETE verification timeout is too large", overflow);
			}
			final long deadline = retryNanos >= Long.MAX_VALUE - started
							? Long.MAX_VALUE
							: started + retryNanos;
			while (hasRetryable(phase, results, selectedCount) && System.nanoTime() < deadline) {
				retryPass(selectionManifest, phase, deadline, probe, results, selectedCount);
				if (hasRetryable(phase, results, selectedCount) && System.nanoTime() < deadline) {
					try {
						TimeUnit.MILLISECONDS.sleep(RETRY_PAUSE_MILLIS);
					} catch (final InterruptedException interrupted) {
						Thread.currentThread().interrupt();
						break;
					}
				}
			}
			final var report = new DeleteVerificationReport(
							phase, results, selectedCount, true, System.nanoTime() - started);
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
					final Path results) throws IOException {
		try (var input = new IntegrityManifestItemInput(selectionManifest);
						OutputStream output = new BufferedOutputStream(Files.newOutputStream(results))) {
			long index = 0;
			for (IntegrityManifestDataItem item = input.get(); item != null; item = input.get()) {
				if (index >= selectedCount) {
					throw new IOException("DELETE verification inventory exceeds its frozen count");
				}
				final DeleteTarget target = new DeleteTarget(item, index);
				output.write(DeleteVerificationReport.encode(safePresence(probe, target)));
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
					final long selectedCount) throws IOException {
		try (var input = new IntegrityManifestItemInput(selectionManifest);
						var ledger = new RandomAccessFile(results.toFile(), "rw")) {
			long index = 0;
			for (IntegrityManifestDataItem item = input.get(); item != null; item = input.get()) {
				if (index >= selectedCount || System.nanoTime() >= deadline) {
					return;
				}
				ledger.seek(index);
				if (retryable(phase, DeleteVerificationReport.decode(ledger.readByte()))) {
					final DeleteTarget target = new DeleteTarget(item, index);
					ledger.seek(index);
					ledger.write(DeleteVerificationReport.encode(safePresence(probe, target)));
				}
				index++;
			}
		}
	}

	private static DeleteVerificationProbe.Presence safePresence(
					final DeleteVerificationProbe probe, final DeleteTarget target) {
		try {
			final var value = probe.presence(target);
			return value == null ? DeleteVerificationProbe.Presence.UNRESOLVED : value;
		} catch (final RuntimeException failure) {
			return DeleteVerificationProbe.Presence.UNRESOLVED;
		}
	}

	private static boolean hasRetryable(
					final DeleteVerificationPhase phase,
					final Path results,
					final long selectedCount) throws IOException {
		try (InputStream input = new BufferedInputStream(Files.newInputStream(results))) {
			for (long index = 0; index < selectedCount; index++) {
				final int presence = input.read();
				if (presence < 0) {
					throw new IOException("DELETE verification evidence ended before its frozen count");
				}
				if (retryable(phase, DeleteVerificationReport.decode((byte) presence))) {
					return true;
				}
			}
		}
		return false;
	}

	private static boolean retryable(
					final DeleteVerificationPhase phase,
					final DeleteVerificationProbe.Presence presence) {
		return presence == DeleteVerificationProbe.Presence.UNRESOLVED
						|| (phase == DeleteVerificationPhase.POST_DELETE
										&& presence == DeleteVerificationProbe.Presence.PRESENT);
	}
}
