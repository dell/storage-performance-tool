package com.dell.spt.base.nic;

import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVFormat;

/**
 * The step's per-path CSV, schema 1 (item 18): every NIC-set context's rows, then one {@code end}
 * record with the totals, whether every context handed over complete rows, and which did not. A
 * file without its {@code end} record is truncated. No operation identities.
 */
public final class NicPathsArtifact {
	public static final String FILE_NAME = "nic.paths.csv";
	public static final String HEADER = "schema_version,engine_run_id,step_id,worker_id,context_index,row,interface,ipv4,rdma_device,rdma_port,gid_index,numa_node,shared_port,permit_share,endpoint,endpoint_ip,endpoint_port,attempts,succeeded,failed,interrupted,unresolved,bytes,complete,missing_contexts";

	/**
	 * One NIC-set context of the step.
	 *
	 * @param contextIndex its position in the step's expected contexts
	 * @param rows its terminal rows, or null when it has none: it never started, or stopped without them
	 */
	public record Context(int contextIndex, NicPathRows rows) {}

	private NicPathsArtifact() {}

	/** Publish synchronously after stop; async logging plus flush does not drain its queue. */
	public static void publish(final long runId, final String stepId, final String workerId, final List<Context> contexts) {
		Path staging = null;
		try {
			final var path = Path.of(FileManager.INSTANCE.logFileName(Loggers.NIC_PATHS.getName(), stepId));
			Files.createDirectories(path.toAbsolutePath().getParent());
			if (Files.exists(path)) {
				throw new IOException("stale NIC paths artifact exists");
			}
			staging = Files.createTempFile(path.toAbsolutePath().getParent(), ".nic-paths-", ".tmp");
			Files.writeString(staging, HEADER + "\n" + rows(runId, stepId, workerId, contexts) + "\n");
			Files.move(staging, path, StandardCopyOption.ATOMIC_MOVE);
		} catch (final IOException e) {
			throw new IllegalStateException("failed to publish the terminal NIC paths evidence", e);
		} finally {
			if (staging != null) {
				try {
					Files.deleteIfExists(staging);
				} catch (final IOException ignored) { /* Primary publication error is retained. */ }
			}
		}
	}

	public static String rows(final long runId, final String stepId, final String workerId, final List<Context> contexts) {
		final var lines = new ArrayList<String>();
		final var missing = new ArrayList<String>();
		long attempts = 0, succeeded = 0, failed = 0, interrupted = 0, unresolved = 0, bytes = 0;
		var complete = true;
		for (final var context : contexts) {
			final var rows = context.rows();
			if (rows == null) {
				complete = false;
				missing.add(Integer.toString(context.contextIndex()));
				continue;
			}
			complete &= rows.complete();
			for (final var row : rows.rows()) {
				final var nic = row.nic();
				final var endpoint = row.endpoint();
				lines.add(CSVFormat.RFC4180.format(1, runId, stepId, workerId, context.contextIndex(), row.kind().label(),
								nic == null ? null : nic.iface(), nic == null ? null : nic.ipv4(),
								nic == null ? null : nic.rdmaDevice(), nic == null ? null : nic.rdmaPort(),
								nic == null ? null : nic.gidIndex(), nic == null ? null : nic.numaNode(),
								nic == null ? null : nic.sharedPort(), nic == null ? null : nic.permitShare(),
								endpoint == null ? null : endpoint.endpoint(), endpoint == null ? null : endpoint.ip(),
								endpoint == null ? null : endpoint.port(),
								row.attempts(), row.succeeded(), row.failed(), row.interrupted(), row.unresolved(), row.bytes(),
								null, null));
				attempts = Math.addExact(attempts, row.attempts());
				succeeded = Math.addExact(succeeded, row.succeeded());
				failed = Math.addExact(failed, row.failed());
				interrupted = Math.addExact(interrupted, row.interrupted());
				unresolved = Math.addExact(unresolved, row.unresolved());
				bytes = Math.addExact(bytes, row.bytes());
			}
		}
		lines.add(CSVFormat.RFC4180.format(1, runId, stepId, workerId, null, "end",
						null, null, null, null, null, null, null, null, null, null, null,
						attempts, succeeded, failed, interrupted, unresolved, bytes, complete, String.join(";", missing)));
		return String.join("\n", lines);
	}
}
