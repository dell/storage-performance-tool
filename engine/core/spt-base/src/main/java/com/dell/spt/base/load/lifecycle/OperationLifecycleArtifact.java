package com.dell.spt.base.load.lifecycle;

import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.List;
import org.apache.commons.csv.CSVFormat;

/** One bounded terminal record per worker step; no operation identities or endpoint data. */
public final class OperationLifecycleArtifact {
	public static final String FILE_NAME = "operation.lifecycle.csv";
	public static final String HEADER = "schema_version,engine_run_id,step_id,worker_id,contexts,terminal,selected,accepted,failed,unattempted,unresolved,terminal_results,generator_buffered,driver_queued,in_flight";

	private OperationLifecycleArtifact() {}

	/** Publish synchronously after stop; async logging plus flush does not drain its queue. */
	public static void publish(final long runId, final String stepId, final String workerId,
					final List<OperationLifecycleCounters> counters, final boolean completeContexts) {
		Path staging = null;
		try {
			final var path = Path.of(FileManager.INSTANCE.logFileName(
							Loggers.OPERATION_LIFECYCLE.getName(), stepId));
			Files.createDirectories(path.toAbsolutePath().getParent());
			if (Files.exists(path)) {
				throw new IOException("stale operation lifecycle artifact exists");
			}
			staging = Files.createTempFile(path.toAbsolutePath().getParent(), ".operation-lifecycle-", ".tmp");
			Files.writeString(staging, HEADER + "\n" + row(runId, stepId, workerId, counters, completeContexts) + "\n");
			Files.move(staging, path, StandardCopyOption.ATOMIC_MOVE);
		} catch (final IOException e) {
			throw new IllegalStateException("failed to publish terminal operation lifecycle evidence", e);
		} finally {
			if (staging != null) {
				try {
					Files.deleteIfExists(staging);
				} catch (final IOException ignored) { /* Primary publication error is retained. */ }
			}
		}
	}

	public static String row(final long runId, final String stepId, final String workerId,
					final List<OperationLifecycleCounters> counters, final boolean completeContexts) {
		long selected = 0, accepted = 0, failed = 0, unattempted = 0, unresolved = 0;
		long terminalResults = 0, buffered = 0, queued = 0, inFlight = 0;
		boolean terminal = completeContexts && !counters.isEmpty();
		for (final var c : counters) {
			if (c == null) {
				terminal = false;
				continue;
			}
			terminal &= c.reconciled();
			selected = Math.addExact(selected, c.selected());
			accepted = Math.addExact(accepted, c.accepted());
			failed = Math.addExact(failed, c.failed());
			unattempted = Math.addExact(unattempted, c.unattempted());
			unresolved = Math.addExact(unresolved, c.unresolved());
			terminalResults = Math.addExact(terminalResults, c.terminalResults());
			buffered = Math.addExact(buffered, c.generatorBuffered());
			queued = Math.addExact(queued, c.driverQueued());
			inFlight = Math.addExact(inFlight, c.inFlight());
		}
		return CSVFormat.RFC4180.format(1, runId, stepId, workerId, counters.size(), terminal,
						selected, accepted, failed, unattempted, unresolved, terminalResults, buffered, queued, inFlight);
	}
}
