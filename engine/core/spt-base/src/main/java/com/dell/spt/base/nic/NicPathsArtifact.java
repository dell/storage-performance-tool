package com.dell.spt.base.nic;

import com.dell.spt.base.logging.Loggers;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.csv.CSVFormat;

/**
 * The step's per-path CSV, schema 2 (item 18): every NIC-set context's rows, then one {@code end}
 * record with the totals, whether every context handed over complete rows, and which did not. A
 * file without its {@code end} record is truncated. No operation identities.
 *
 * <p>Schema 2 appends, after schema 1's columns, {@code rdma_succeeded} and {@code http_succeeded} on
 * every row, and on per-NIC rows the NIC Pool's counters. They are empty for a driver without RDMA,
 * such as {@code s3}, and numbers for {@code s3-rdma}, so a reader tells absent from zero by the
 * version and the driver.
 */
public final class NicPathsArtifact {
	public static final String FILE_NAME = "nic.paths.csv";
	public static final int SCHEMA_VERSION = 2;
	public static final String HEADER = "schema_version,engine_run_id,step_id,worker_id,context_index,row,interface,ipv4,rdma_device,rdma_port,gid_index,numa_node,shared_port,permit_share,endpoint,endpoint_ip,endpoint_port,attempts,succeeded,failed,interrupted,unresolved,bytes,complete,missing_contexts,rdma_succeeded,http_succeeded,pool_created,pool_hits,pool_exhausted,pool_unpooled,pool_prewarmed";

	/**
	 * One NIC-set context of the step.
	 *
	 * @param contextIndex its position in the step's expected contexts
	 * @param rows its terminal rows, or null when it has none: it never started, or stopped without them
	 */
	public record Context(int contextIndex, NicPathRows rows) {}

	private NicPathsArtifact() {}

	/**
	 * Writes the step's per-path file. Publish synchronously after stop; async logging plus flush does not drain its queue.
	 *
	 * @param admitted false when the step failed at the admission barrier: its end record then says
	 *     incomplete, even on an engine that was ready and whose contexts handed over complete rows
	 */
	public static void publish(
					final long runId, final String stepId, final String workerId, final List<Context> contexts, final boolean admitted) {
		NicStepFile.publish(Loggers.NIC_PATHS, stepId, "NIC paths", HEADER + "\n" + rows(runId, stepId, workerId, contexts, admitted) + "\n");
	}

	public static String rows(final long runId, final String stepId, final String workerId, final List<Context> contexts) {
		return rows(runId, stepId, workerId, contexts, true);
	}

	public static String rows(
					final long runId, final String stepId, final String workerId, final List<Context> contexts, final boolean admitted) {
		final var lines = new ArrayList<String>();
		final var missing = new ArrayList<String>();
		long attempts = 0, succeeded = 0, failed = 0, interrupted = 0, unresolved = 0, bytes = 0;
		Long rdmaSucceeded = null, httpSucceeded = null;
		var complete = admitted;
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
				final var pool = row.pool();
				lines.add(CSVFormat.RFC4180.format(SCHEMA_VERSION, runId, stepId, workerId, context.contextIndex(), row.kind().label(),
								nic == null ? null : nic.iface(), nic == null ? null : nic.ipv4(),
								nic == null ? null : nic.rdmaDevice(), nic == null ? null : nic.rdmaPort(),
								nic == null ? null : nic.gidIndex(), nic == null ? null : nic.numaNode(),
								nic == null ? null : nic.sharedPort(), nic == null ? null : nic.permitShare(),
								endpoint == null ? null : endpoint.endpoint(), endpoint == null ? null : endpoint.ip(),
								endpoint == null ? null : endpoint.port(),
								row.attempts(), row.succeeded(), row.failed(), row.interrupted(), row.unresolved(), row.bytes(),
								null, null, row.rdmaSucceeded(), row.httpSucceeded(),
								pool == null ? null : pool.created(), pool == null ? null : pool.hits(),
								pool == null ? null : pool.exhausted(), pool == null ? null : pool.unpooled(),
								pool == null ? null : pool.prewarmed()));
				attempts = Math.addExact(attempts, row.attempts());
				succeeded = Math.addExact(succeeded, row.succeeded());
				failed = Math.addExact(failed, row.failed());
				interrupted = Math.addExact(interrupted, row.interrupted());
				unresolved = Math.addExact(unresolved, row.unresolved());
				bytes = Math.addExact(bytes, row.bytes());
				rdmaSucceeded = addIfPresent(rdmaSucceeded, row.rdmaSucceeded());
				httpSucceeded = addIfPresent(httpSucceeded, row.httpSucceeded());
			}
		}
		lines.add(CSVFormat.RFC4180.format(SCHEMA_VERSION, runId, stepId, workerId, null, "end",
						null, null, null, null, null, null, null, null, null, null, null,
						attempts, succeeded, failed, interrupted, unresolved, bytes, complete, String.join(";", missing),
						rdmaSucceeded, httpSucceeded, null, null, null, null, null));
		return String.join("\n", lines);
	}

	/** A column's total, which stays empty when no row of the file has the column. */
	private static Long addIfPresent(final Long total, final Long value) {
		if (value == null) {
			return total;
		}
		return total == null ? value : Long.valueOf(Math.addExact(total, value));
	}
}
