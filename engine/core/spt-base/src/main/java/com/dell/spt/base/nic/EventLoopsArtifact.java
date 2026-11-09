package com.dell.spt.base.nic;

import com.dell.spt.base.logging.Loggers;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.akurilov.commons.concurrent.ThreadUtil;
import java.util.List;
import java.util.Objects;

/**
 * The step's event-loop report, schema 1: the engine's hardware threads and the Netty event loops of
 * each driver context that owns some, with the route they serve and what sized them. Every step
 * writes one; an empty {@code contexts} means its drivers own none. {@code event_loops} is the
 * engine's total. Adding fields keeps the schema.
 */
public final class EventLoopsArtifact {
	public static final String FILE_NAME = "event.loops.json";
	public static final int SCHEMA_VERSION = 1;

	private static final ObjectMapper MAPPER = new ObjectMapper();

	/**
	 * One driver context of the step that owns event loops.
	 *
	 * @param contextIndex its position in the step's expected contexts
	 * @param loops its driver's event loops
	 */
	public record Context(int contextIndex, EventLoops loops) {

		public Context {
			Objects.requireNonNull(loops, "loops");
		}
	}

	private EventLoopsArtifact() {}

	/**
	 * Publish synchronously after stop; async logging plus flush does not drain its queue. A report
	 * left by an earlier step with the same id is replaced: the file describes the step that ran last.
	 */
	public static void publish(final long runId, final String stepId, final String workerId, final List<Context> contexts) {
		NicStepFile.publish(Loggers.EVENT_LOOPS, stepId, "event loops",
						json(runId, stepId, workerId, ThreadUtil.getHardwareThreadCount(), contexts) + "\n", true);
	}

	/**
	 * Removes a report an earlier step with the same id left, and its node copies, at the step's
	 * start, so a step that never publishes its own has none rather than an older one.
	 */
	public static void discardEarlier(final String stepId) {
		NicStepFile.discard(Loggers.EVENT_LOOPS, stepId, "event loops");
	}

	public static String json(final long runId, final String stepId, final String workerId, final int hardwareThreads,
					final List<Context> contexts) {
		final ObjectNode root = MAPPER.createObjectNode();
		root.put("schema_version", SCHEMA_VERSION);
		root.put("engine_run_id", runId);
		root.put("step_id", stepId);
		root.put("worker_id", workerId);
		root.put("hardware_threads", hardwareThreads);
		root.put("event_loops", contexts.stream().mapToInt(context -> context.loops().eventLoops()).sum());
		final var array = root.putArray("contexts");
		for (final var context : contexts) {
			final var node = array.addObject();
			final var loops = context.loops();
			node.put("context_index", context.contextIndex());
			node.put("route", loops.route().reportName());
			node.put("driver_threads", loops.driverThreads());
			node.put("source", loops.source());
			node.put("event_loops", loops.eventLoops());
		}
		try {
			return MAPPER.writeValueAsString(root);
		} catch (final JsonProcessingException e) {
			throw new IllegalStateException("Failed to serialize the event-loop report", e);
		}
	}
}
