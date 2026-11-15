package com.dell.spt.base.metrics.range;

import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import org.apache.commons.csv.CSVFormat;

/** One bounded terminal row per range context; no object identities or endpoint data. */
public final class RangeReadArtifact {
	public static final String FILE_NAME = "range.read.csv";
	public static final String HEADER = "schema_version,engine_run_id,step_id,worker_id,context_index,terminal,mode,size,fixed_offset_present,fixed_offset,alignment,selected,accepted,failed,unattempted,unresolved,terminal_results,generator_buffered,driver_queued,in_flight,attempted,requests_sent,successful_bytes,local_selection_errors,http_failures,response_validation_failures,transport_failures,http_attempt_failures,response_validation_attempt_failures,transport_attempt_failures,failed_received_bytes,unresolved_received_bytes,overflow";

	private RangeReadArtifact() {}

	/** Publish synchronously after stop; async logging plus flush does not drain its queue. */
	public static void publish(final long runId, final String stepId, final String workerId,
					final List<RangeReadSnapshot> counters, final boolean completeContexts) {
		Path staging = null;
		try {
			final var path = Path.of(FileManager.INSTANCE.logFileName(
							Loggers.RANGE_READ.getName(), stepId));
			Files.createDirectories(path.toAbsolutePath().getParent());
			if (Files.exists(path)) {
				throw new IOException("stale range read artifact exists");
			}
			staging = Files.createTempFile(path.toAbsolutePath().getParent(), ".range-read-", ".tmp");
			Files.writeString(staging, HEADER + "\n" + rows(runId, stepId, workerId, counters, completeContexts) + "\n");
			Files.move(staging, path, StandardCopyOption.ATOMIC_MOVE);
		} catch (final IOException e) {
			throw new IllegalStateException("failed to publish terminal range read evidence", e);
		} finally {
			if (staging != null) {
				try {
					Files.deleteIfExists(staging);
				} catch (final IOException ignored) { /* Primary publication error is retained. */ }
			}
		}
	}

	public static String rows(final long runId, final String stepId, final String workerId,
					final List<RangeReadSnapshot> snapshots, final boolean completeContexts) {
		final var rows = new java.util.ArrayList<String>();
		for (int i = 0; i < snapshots.size(); i++) {
			final var s = snapshots.get(i);
			if (s == null)
				continue;
			final var p = s.policy();
			final var c = s.logical();
			rows.add(CSVFormat.RFC4180.format(1, runId, stepId, workerId, i,
							completeContexts && s.reconciled(), p.fixedOffset() == null ? "random" : "fixed",
							p.length(), p.fixedOffset() != null, p.fixedOffset(), p.alignment(),
							c.selected(), c.accepted(), c.failed(), c.unattempted(), c.unresolved(), c.terminalResults(), c.generatorBuffered(), c.driverQueued(), c.inFlight(),
							s.attempted(), s.requestsSent(), s.successfulBytes(), s.localSelectionErrors(),
							s.httpFailures(), s.responseValidationFailures(), s.transportFailures(),
							s.httpAttemptFailures(), s.responseValidationAttemptFailures(), s.transportAttemptFailures(),
							s.failedReceivedBytes(), s.unresolvedReceivedBytes(), s.overflow()));
		}
		return String.join("\n", rows);
	}
}
