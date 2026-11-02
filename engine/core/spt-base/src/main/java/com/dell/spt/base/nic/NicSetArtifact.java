package com.dell.spt.base.nic;

import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.nic.pool.PoolPreparation;
import com.dell.spt.base.nic.pool.PoolReportJson;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.akurilov.commons.concurrent.ThreadUtil;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * The step's NIC-set report, schema 1 (D64): the engine's hardware threads and its NIC-set
 * drivers' event loops, and for NIC Pools a {@code pool} object with the step's checks and pre-warm. One driver's fields are at the top level. A step with several (a
 * weighted or pipeline step) adds {@code contexts}, one object per NIC-set context, and keeps
 * a per-driver field at the top level only when every context agrees on it. {@code event_loops}
 * is always the engine's total. Adding fields keeps the schema.
 */
public final class NicSetArtifact {
	public static final String FILE_NAME = "nic.set.json";
	public static final int SCHEMA_VERSION = 1;

	private static final ObjectMapper MAPPER = new ObjectMapper();

	/**
	 * One NIC-set context of the step.
	 *
	 * @param contextIndex its position in the step's expected contexts
	 * @param resources its driver's event loops
	 */
	public record Context(int contextIndex, NicSetResources resources) {

		public Context {
			Objects.requireNonNull(resources, "resources");
		}
	}

	private NicSetArtifact() {}

	/** Publish synchronously after stop; async logging plus flush does not drain its queue. */
	public static void publish(final long runId, final String stepId, final String workerId, final List<Context> contexts,
					final int contextsWithoutNicSet) {
		publish(runId, stepId, workerId, contexts, contextsWithoutNicSet, null);
	}

	/**
	 * Publishes the report with the step's pool work.
	 *
	 * @param pool the step's pool checks and pre-warm, or {@code null} when it has no NIC Pools
	 */
	public static void publish(final long runId, final String stepId, final String workerId, final List<Context> contexts,
					final int contextsWithoutNicSet, final PoolPreparation.Report pool) {
		NicStepFile.publish(Loggers.NIC_SET, stepId, "NIC set",
						json(runId, stepId, workerId, ThreadUtil.getHardwareThreadCount(), contexts, contextsWithoutNicSet, pool) + "\n");
	}

	public static String json(final long runId, final String stepId, final String workerId, final int hardwareThreads,
					final List<Context> contexts, final int contextsWithoutNicSet) {
		return json(runId, stepId, workerId, hardwareThreads, contexts, contextsWithoutNicSet, null);
	}

	public static String json(final long runId, final String stepId, final String workerId, final int hardwareThreads,
					final List<Context> contexts, final int contextsWithoutNicSet, final PoolPreparation.Report pool) {
		final ObjectNode root = MAPPER.createObjectNode();
		root.put("schema_version", SCHEMA_VERSION);
		root.put("engine_run_id", runId);
		root.put("step_id", stepId);
		root.put("worker_id", workerId);
		root.put("hardware_threads", hardwareThreads);
		if (!contexts.isEmpty()) {
			final var first = contexts.get(0).resources();
			if (agree(contexts, NicSetResources::driverThreads)) {
				root.put("driver_threads", first.driverThreads());
			}
			if (agree(contexts, NicSetResources::eventLoopsPerNic)) {
				root.put("event_loops_per_nic", first.eventLoopsPerNic());
			}
			if (agree(contexts, NicSetResources::nics)) {
				nics(root, first);
			}
		}
		root.put("event_loops", contexts.stream().mapToInt(context -> context.resources().eventLoops()).sum());
		root.put("base_event_loops", contexts.stream().mapToInt(context -> context.resources().baseEventLoops()).sum());
		root.put("total_event_loops", contexts.stream().mapToInt(context -> context.resources().totalEventLoops()).sum());
		if (contextsWithoutNicSet > 0) {
			// Their drivers' loops are not in the totals, so the report says what it leaves out.
			root.put("contexts_without_nic_set", contextsWithoutNicSet);
		}
		if (contexts.size() > 1) {
			final var array = root.putArray("contexts");
			for (final var context : contexts) {
				final var node = array.addObject();
				final var resources = context.resources();
				node.put("context_index", context.contextIndex());
				node.put("driver_threads", resources.driverThreads());
				node.put("event_loops_per_nic", resources.eventLoopsPerNic());
				nics(node, resources);
				node.put("event_loops", resources.eventLoops());
				node.put("base_event_loops", resources.baseEventLoops());
				node.put("total_event_loops", resources.totalEventLoops());
			}
		}
		if (pool != null) {
			PoolReportJson.write(root.putObject("pool"), pool);
		}
		try {
			return MAPPER.writeValueAsString(root);
		} catch (final JsonProcessingException e) {
			throw new IllegalStateException("Failed to serialize the NIC set report", e);
		}
	}

	private static boolean agree(final List<Context> contexts, final Function<NicSetResources, ?> field) {
		final var first = field.apply(contexts.get(0).resources());
		return contexts.stream().allMatch(context -> first.equals(field.apply(context.resources())));
	}

	private static void nics(final ObjectNode node, final NicSetResources resources) {
		final var nics = node.putArray("nics");
		resources.nics().forEach(nics::add);
	}
}
