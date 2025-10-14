package com.dell.spt.base.control;

import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.ShardSnapshot;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.Snapshot;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.TerminalStepEntry;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.github.akurilov.confuse.Config;
import java.net.InetAddress;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

final class MetricsJsonResponder {

	private static final int METRICS_SCHEMA_VERSION = 2;

	private final MetricsManager metricsManager;
	private final Config config;
	private final ObjectMapper objectMapper = new ObjectMapper();
	private final String nodeId;
	private final String clusterId;
	private final long configuredRunId;

	MetricsJsonResponder(final MetricsManager metricsManager, final Config config) {
		this.metricsManager = metricsManager;
		this.config = config;
		this.nodeId = resolveNodeId(config);
		this.clusterId = resolveClusterId(config);
		this.configuredRunId = resolveConfiguredRunId(config);
	}

	ArrayNode buildNodeMetrics(final boolean verbose) {
		final ArrayNode jsonArray = objectMapper.createArrayNode();
		final Set<DistributedMetricsContext> distributed = metricsManager.getDistributedContexts();
		final Set<MetricsContext> allContexts = metricsManager.getAllContexts();

		final Set<String> activeStepIds = new HashSet<>();

		for (MetricsContext ctx : allContexts) {
			if (ctx instanceof DistributedMetricsContext) {
				continue;
			}
			ctx.refreshLastSnapshot();
			final AllMetricsSnapshot snapshot = ctx.lastSnapshot();
			if (snapshot == null) {
				continue;
			}
			final ObjectNode jsonObj = buildContextNode(ctx, snapshot, distributed, allContexts, verbose);
			jsonArray.add(jsonObj);
			activeStepIds.add(ctx.loadStepId());
		}

		appendTerminalEntries(jsonArray, activeStepIds);

		// Emit a single "idle" sample if nothing is available yet.
		if (jsonArray.size() == 0) {
			jsonArray.add(buildIdleNodeSample());
		}
		return jsonArray;
	}

	ArrayNode buildFleetMetrics(final boolean verbose) {
		final ArrayNode jsonArray = objectMapper.createArrayNode();
		final Set<DistributedMetricsContext> distributed = metricsManager.getDistributedContexts();
		final Set<MetricsContext> allContexts = metricsManager.getAllContexts();

		for (DistributedMetricsContext ctx : distributed) {
			ctx.refreshLastSnapshot();
			final AllMetricsSnapshot snapshot = ctx.lastSnapshot();
			if (snapshot == null) {
				continue;
			}
			final ObjectNode jsonObj = buildDistributedNode(ctx, snapshot, distributed, allContexts, verbose);
			jsonArray.add(jsonObj);
		}
		return jsonArray;
	}

	private ObjectNode buildContextNode(
					final MetricsContext ctx,
					final AllMetricsSnapshot snapshot,
					final Set<DistributedMetricsContext> distributed,
					final Set<MetricsContext> allContexts,
					final boolean verbose) {
		final ObjectNode jsonObj = objectMapper.createObjectNode();
		applyCommonMetadata(jsonObj, "node", ctx.runId());
		jsonObj.put("step_id", ctx.loadStepId());
		jsonObj.put("op_type", ctx.opType().name());
		jsonObj.put("timestamp", System.currentTimeMillis());
		jsonObj.put("elapsed_time_seconds", snapshot.elapsedTimeMillis() / 1000.0);
		jsonObj.put("test_state", calculateTestState(snapshot));
		jsonObj.put(MetricsConstants.METRIC_NAME_COMPLETION, calculateCompletionPercent(ctx.metadata(), snapshot));

		addLimitFields(jsonObj, ctx.metadata());
		addMetrics(jsonObj, snapshot);
		addListShardMetrics(jsonObj, ctx.metadata());

		jsonObj.put(
						"overall_completion_percent",
						calculateOverallCompletionPercentForStep(ctx.loadStepId(), distributed, allContexts));
		jsonObj.put("overall_unbounded", calculateOverallUnboundedForStep(ctx.loadStepId(), distributed, allContexts));

		if (verbose) {
			jsonObj.put("diag_distributed_contexts", distributed.size());
			jsonObj.put("diag_local_contexts", Math.max(0, allContexts.size() - distributed.size()));
		}
		return jsonObj;
	}

	private ObjectNode buildDistributedNode(
					final DistributedMetricsContext ctx,
					final AllMetricsSnapshot snapshot,
					final Set<DistributedMetricsContext> distributed,
					final Set<MetricsContext> allContexts,
					final boolean verbose) {
		final ObjectNode jsonObj = objectMapper.createObjectNode();
		applyCommonMetadata(jsonObj, "fleet", ctx.runId());
		jsonObj.put("step_id", ctx.loadStepId());
		jsonObj.put("op_type", ctx.opType().name());
		jsonObj.put("timestamp", System.currentTimeMillis());
		jsonObj.put("elapsed_time_seconds", snapshot.elapsedTimeMillis() / 1000.0);
		jsonObj.put("test_state", calculateTestState(snapshot));
		jsonObj.put(MetricsConstants.METRIC_NAME_COMPLETION, calculateCompletionPercent(ctx.metadata(), snapshot));

		addLimitFields(jsonObj, ctx.metadata());
		addMetrics(jsonObj, snapshot);
		addListShardMetrics(jsonObj, ctx.metadata());

		jsonObj.put(
						"overall_completion_percent",
						calculateOverallCompletionPercentForStep(ctx.loadStepId(), distributed, allContexts));
		jsonObj.put("overall_unbounded", calculateOverallUnboundedForStep(ctx.loadStepId(), distributed, allContexts));

		jsonObj.put("nodes_count", ctx.nodeCount());
		final ArrayNode nodesPresent = objectMapper.createArrayNode();
		final List<String> addrs = ctx.nodeAddrs();
		if (addrs != null) {
			addrs.forEach(nodesPresent::add);
		}
		jsonObj.set("nodes_present", nodesPresent);
		final boolean partial = addrs != null && ctx.nodeCount() > addrs.size();
		jsonObj.put("partial", partial);

		if (verbose) {
			jsonObj.put("diag_distributed_contexts", distributed.size());
			jsonObj.put("diag_local_contexts", Math.max(0, allContexts.size() - distributed.size()));
		}
		return jsonObj;
	}

	private void addLimitFields(final ObjectNode jsonObj, final java.util.Map meta) {
		long countLimit = 0L;
		long timeLimitSec = 0L;
		try {
			final Object v = meta.get(MetricsConstants.METADATA_LIMIT_OP_COUNT);
			if (v instanceof Number) {
				countLimit = ((Number) v).longValue();
			}
		} catch (Exception ignore) {}
		try {
			final Object v = meta.get(MetricsConstants.METADATA_LIMIT_TIME_SEC);
			if (v instanceof Number) {
				timeLimitSec = ((Number) v).longValue();
			}
		} catch (Exception ignore) {}

		final boolean hasCountLimit = countLimit > 0L;
		final boolean hasTimeLimit = timeLimitSec > 0L;
		final boolean unbounded = !(hasCountLimit || hasTimeLimit);
		jsonObj.put("unbounded", unbounded);

		final ObjectNode limit = objectMapper.createObjectNode();
		if (hasCountLimit) {
			limit.put("type", "op_count");
			limit.put("op_count", countLimit);
		} else if (hasTimeLimit) {
			limit.put("type", "time");
			limit.put("time_sec", timeLimitSec);
		} else {
			limit.put("type", "none");
		}
		jsonObj.set("limit", limit);
	}

	private void addMetrics(final ObjectNode jsonObj, final AllMetricsSnapshot snapshot) {
		final ObjectNode operations = objectMapper.createObjectNode();
		operations.put("success_count", snapshot.successSnapshot().count());
		operations.put("failed_count", snapshot.failsSnapshot().count());
		operations.put("success_rate_last", snapshot.successSnapshot().last());
		operations.put("failed_rate_last", snapshot.failsSnapshot().last());
		jsonObj.set("operations", operations);

		final ObjectNode bandwidth = objectMapper.createObjectNode();
		bandwidth.put("bytes_total", snapshot.byteSnapshot().count());
		bandwidth.put("bytes_rate_last", snapshot.byteSnapshot().last());
		jsonObj.set("bandwidth", bandwidth);

		final ObjectNode timing = objectMapper.createObjectNode();
		timing.put("latency_mean_us", snapshot.latencySnapshot().mean());
		timing.put("duration_mean_us", snapshot.durationSnapshot().mean());
		jsonObj.set("timing", timing);

		final ObjectNode concurrency = objectMapper.createObjectNode();
		concurrency.put("current", snapshot.concurrencySnapshot().last());
		concurrency.put("mean", snapshot.concurrencySnapshot().mean());
		jsonObj.set("concurrency", concurrency);
	}

	private void addListShardMetrics(final ObjectNode jsonObj, final java.util.Map metadata) {
		if (metadata == null) {
			return;
		}
		final Object value = metadata.get(MetricsConstants.METADATA_LIST_SHARD_METRICS);
		if (!(value instanceof ListShardMetricsRecorder)) {
			return;
		}
		final ListShardMetricsRecorder recorder = (ListShardMetricsRecorder) value;
		final Snapshot snapshot = recorder.snapshot();
		if (snapshot == null) {
			return;
		}
		final ObjectNode listMetrics = objectMapper.createObjectNode();
		listMetrics.put("total_pages", snapshot.totalPages());
		listMetrics.put("total_objects", snapshot.totalObjects());
		listMetrics.put("avg_page_millis", snapshot.avgPageMillis());
		listMetrics.put("captured_at_millis", snapshot.capturedAtMillis());
		listMetrics.put("total_splits", snapshot.totalSplits());
		listMetrics.put("max_depth", snapshot.maxDepth());
		final ObjectNode splitReasons = objectMapper.createObjectNode();
		snapshot.splitReasons().forEach(splitReasons::put);
		listMetrics.set("split_reasons", splitReasons);
		final ArrayNode shardArray = objectMapper.createArrayNode();
		for (ShardSnapshot shard : snapshot.shards()) {
			final ObjectNode shardNode = objectMapper.createObjectNode();
			shardNode.put("prefix", shard.prefix());
			shardNode.put("pages", shard.pages());
			shardNode.put("objects", shard.objects());
			shardNode.put("avg_page_millis", shard.avgPageMillis());
			shardNode.put("first_update_millis", shard.firstUpdateMillis());
			shardNode.put("last_update_millis", shard.lastUpdateMillis());
			shardNode.put("active", shard.active());
			shardNode.put("stall_warnings", shard.stallWarnings());
			shardNode.put("consecutive_full_pages", shard.consecutiveFullPages());
			shardNode.put("last_page_objects", shard.lastPageObjects());
			shardNode.put("last_page_max_keys", shard.lastPageMaxKeys());
			shardNode.put("last_full_page_millis", shard.lastFullPageMillis());
			shardNode.put("split_count", shard.splitCount());
			shardNode.put("max_depth", shard.maxDepth());
			shardNode.put("last_split_millis", shard.lastSplitMillis());
			shardNode.put("last_split_reason", shard.lastSplitReason() == null ? "" : shard.lastSplitReason());
			shardNode.put("last_split_children", shard.lastSplitChildren());
			shardArray.add(shardNode);
		}
		listMetrics.set("shards", shardArray);
		jsonObj.set("list_shards", listMetrics);
	}

	/**
	 * Build a minimal idle node sample to ensure /metrics/json is never empty at startup.
	 * Matches the schema emitted for live snapshots with zeroed values.
	 */
	private ObjectNode buildIdleNodeSample() {
		final ObjectNode jsonObj = objectMapper.createObjectNode();
		// Common metadata; override run_id to empty string for pre-run idle state
		applyCommonMetadata(jsonObj, "node", configuredRunId);
		jsonObj.put("run_id", "");

		// Required identifiers
		jsonObj.put("step_id", "");
		jsonObj.put("op_type", "none");
		jsonObj.put("timestamp", System.currentTimeMillis());
		jsonObj.put("elapsed_time_seconds", 0.0);
		jsonObj.put("test_state", 0);
		jsonObj.put(MetricsConstants.METRIC_NAME_COMPLETION, 0);

		// Limits and unbounded flags (idle -> explicit none)
		jsonObj.put("unbounded", false);
		final ObjectNode limit = objectMapper.createObjectNode();
		limit.put("type", "none");
		jsonObj.set("limit", limit);

		// Metrics blocks with zero values
		final ObjectNode operations = objectMapper.createObjectNode();
		operations.put("success_count", 0L);
		operations.put("failed_count", 0L);
		operations.put("success_rate_last", 0.0);
		operations.put("failed_rate_last", 0.0);
		jsonObj.set("operations", operations);

		final ObjectNode bandwidth = objectMapper.createObjectNode();
		bandwidth.put("bytes_total", 0L);
		bandwidth.put("bytes_rate_last", 0.0);
		jsonObj.set("bandwidth", bandwidth);

		final ObjectNode timing = objectMapper.createObjectNode();
		timing.put("latency_mean_us", 0.0);
		timing.put("duration_mean_us", 0.0);
		jsonObj.set("timing", timing);

		final ObjectNode concurrency = objectMapper.createObjectNode();
		concurrency.put("current", 0.0);
		concurrency.put("mean", 0.0);
		jsonObj.set("concurrency", concurrency);

		// Overall values (no fleet aggregates exposed at /metrics/json)
		jsonObj.put("overall_completion_percent", 0);
		jsonObj.put("overall_unbounded", false);

		return jsonObj;
	}

	private void appendTerminalEntries(final ArrayNode jsonArray, final Set<String> activeStepIds) {
		for (TerminalStepEntry entry : metricsManager.getTerminalSteps()) {
			if (activeStepIds.contains(entry.stepId)) {
				continue;
			}
			final ObjectNode jsonObj = objectMapper.createObjectNode();
			applyCommonMetadata(jsonObj, "node", configuredRunId);
			jsonObj.put("step_id", entry.stepId);
			jsonObj.put("op_type", entry.opType.name());
			jsonObj.put("timestamp", entry.recordedAtMillis);
			jsonObj.put("elapsed_time_seconds", entry.elapsedTimeMillis / 1000.0);
			jsonObj.put("test_state", 2);

			final boolean hasCountLimit = entry.countLimit > 0L;
			final boolean hasTimeLimit = entry.timeLimitSec > 0L;
			final boolean unbounded = !(hasCountLimit || hasTimeLimit);
			jsonObj.put("unbounded", unbounded);

			final ObjectNode limit = objectMapper.createObjectNode();
			if (hasCountLimit) {
				limit.put("type", "op_count");
				limit.put("op_count", entry.countLimit);
			} else if (hasTimeLimit) {
				limit.put("type", "time");
				limit.put("time_sec", entry.timeLimitSec);
			} else {
				limit.put("type", "none");
			}
			jsonObj.set("limit", limit);

			int completionPercent = 0;
			if (hasCountLimit && entry.countLimit > 0) {
				final long done = entry.successCount + entry.failedCount;
				final double completion = Math.min(1.0, Math.max(0.0, (double) done / (double) entry.countLimit));
				completionPercent = (int) Math.round(completion * 100.0);
			} else if (hasTimeLimit && entry.timeLimitSec > 0) {
				final double completion = Math.min(1.0, Math.max(0.0, (entry.elapsedTimeMillis / 1000.0) / (double) entry.timeLimitSec));
				completionPercent = (int) Math.round(completion * 100.0);
			}
			jsonObj.put(MetricsConstants.METRIC_NAME_COMPLETION, completionPercent);
			jsonObj.put("overall_completion_percent", completionPercent);
			jsonObj.put("overall_unbounded", unbounded);

			final ObjectNode operations = objectMapper.createObjectNode();
			operations.put("success_count", entry.successCount);
			operations.put("failed_count", entry.failedCount);
			operations.put("success_rate_last", 0.0);
			operations.put("failed_rate_last", 0.0);
			jsonObj.set("operations", operations);

			final ObjectNode bandwidth = objectMapper.createObjectNode();
			bandwidth.put("bytes_total", entry.bytesTotal);
			bandwidth.put("bytes_rate_last", 0.0);
			jsonObj.set("bandwidth", bandwidth);

			final ObjectNode timing = objectMapper.createObjectNode();
			timing.put("latency_mean_us", entry.latencyMeanUs);
			timing.put("duration_mean_us", entry.durationMeanUs);
			jsonObj.set("timing", timing);

			final ObjectNode concurrency = objectMapper.createObjectNode();
			concurrency.put("current", entry.concurrencyLast);
			concurrency.put("mean", entry.concurrencyMean);
			jsonObj.set("concurrency", concurrency);

			jsonObj.put("terminal", true);
			jsonArray.add(jsonObj);
		}
	}

	private int calculateTestState(final AllMetricsSnapshot snapshot) {
		final long successCount = snapshot.successSnapshot().count();
		final long failsCount = snapshot.failsSnapshot().count();
		final long totalOps = successCount + failsCount;
		if (totalOps > 0) {
			final long concurrency = snapshot.concurrencySnapshot().last();
			return concurrency > 0 ? 1 : 2;
		}
		return 1;
	}

	private int calculateCompletionPercent(final java.util.Map meta, final AllMetricsSnapshot snapshot) {
		long countLimit = 0L;
		long timeLimitSec = 0L;
		try {
			final Object v = meta.get(MetricsConstants.METADATA_LIMIT_OP_COUNT);
			if (v instanceof Number) {
				countLimit = ((Number) v).longValue();
			}
		} catch (Exception ignore) {}
		try {
			final Object v = meta.get(MetricsConstants.METADATA_LIMIT_TIME_SEC);
			if (v instanceof Number) {
				timeLimitSec = ((Number) v).longValue();
			}
		} catch (Exception ignore) {}

		double completion = 0.0;
		if (countLimit > 0) {
			final long succ = snapshot.successSnapshot().count();
			final long fail = snapshot.failsSnapshot().count();
			completion = Math.min(1.0, Math.max(0.0, (double) (succ + fail) / (double) countLimit));
		} else if (timeLimitSec > 0) {
			completion = Math.min(1.0, Math.max(0.0, (snapshot.elapsedTimeMillis() / 1000.0) / (double) timeLimitSec));
		}
		return (int) Math.round(completion * 100.0);
	}

	private int calculateOverallCompletionPercentForStep(
					final String stepId,
					final Set<DistributedMetricsContext> distributed,
					final Set<MetricsContext> allContexts) {
		long sumCountLimit = 0L;
		long sumCompleted = 0L;
		boolean allHaveCount = false;
		boolean first = true;
		long maxElapsed = 0L;
		long timeLimitSec = 0L;

		final java.util.List<java.util.Map> metas = new java.util.ArrayList<>();
		final java.util.List<AllMetricsSnapshot> snaps = new java.util.ArrayList<>();
		boolean usingDistributed = false;

		for (DistributedMetricsContext ctx : distributed) {
			if (!stepId.equals(ctx.loadStepId())) {
				continue;
			}
			ctx.refreshLastSnapshot();
			final AllMetricsSnapshot snapshot = ctx.lastSnapshot();
			if (snapshot == null) {
				continue;
			}
			usingDistributed = true;
			metas.add(ctx.metadata());
			snaps.add(snapshot);
		}

		if (!usingDistributed) {
			for (MetricsContext ctx : allContexts) {
				if (!(stepId.equals(ctx.loadStepId()))) {
					continue;
				}
				ctx.refreshLastSnapshot();
				final AllMetricsSnapshot snapshot = ctx.lastSnapshot();
				if (snapshot == null) {
					continue;
				}
				metas.add(ctx.metadata());
				snaps.add(snapshot);
			}
		}

		for (int i = 0; i < snaps.size(); i++) {
			final AllMetricsSnapshot snapshot = snaps.get(i);
			final java.util.Map meta = metas.get(i);
			if (first) {
				allHaveCount = true;
				first = false;
			}
			final Object cl = meta.get(MetricsConstants.METADATA_LIMIT_OP_COUNT);
			final long countLimit = (cl instanceof Number) ? ((Number) cl).longValue() : 0L;
			if (countLimit <= 0) {
				allHaveCount = false;
			} else {
				sumCountLimit += countLimit;
				sumCompleted += (snapshot.successSnapshot().count() + snapshot.failsSnapshot().count());
			}
			final Object tl = meta.get(MetricsConstants.METADATA_LIMIT_TIME_SEC);
			if (tl instanceof Number && ((Number) tl).longValue() > 0) {
				timeLimitSec = Math.max(timeLimitSec, ((Number) tl).longValue());
			}
			maxElapsed = Math.max(maxElapsed, snapshot.elapsedTimeMillis());
		}

		double completion = 0.0;
		if (!first && allHaveCount && sumCountLimit > 0) {
			completion = Math.min(1.0, Math.max(0.0, (double) sumCompleted / (double) sumCountLimit));
		} else if (!first && timeLimitSec > 0) {
			completion = Math.min(1.0, Math.max(0.0, (maxElapsed / 1000.0) / (double) timeLimitSec));
		}
		return (int) Math.round(completion * 100.0);
	}

	private boolean calculateOverallUnboundedForStep(
					final String stepId,
					final Set<DistributedMetricsContext> distributed,
					final Set<MetricsContext> allContexts) {
		boolean sawContext = false;
		for (DistributedMetricsContext ctx : distributed) {
			if (!stepId.equals(ctx.loadStepId())) {
				continue;
			}
			sawContext = true;
			if (hasLimit(ctx.metadata())) {
				return false;
			}
		}
		if (!sawContext) {
			for (MetricsContext ctx : allContexts) {
				if (!stepId.equals(ctx.loadStepId())) {
					continue;
				}
				if (hasLimit(ctx.metadata())) {
					return false;
				}
			}
		}
		return true;
	}

	private boolean hasLimit(final java.util.Map meta) {
		try {
			final Object cl = meta.get(MetricsConstants.METADATA_LIMIT_OP_COUNT);
			if (cl instanceof Number && ((Number) cl).longValue() > 0L) {
				return true;
			}
		} catch (Exception ignore) {}
		try {
			final Object tl = meta.get(MetricsConstants.METADATA_LIMIT_TIME_SEC);
			if (tl instanceof Number && ((Number) tl).longValue() > 0L) {
				return true;
			}
		} catch (Exception ignore) {}
		return false;
	}

	private void applyCommonMetadata(final ObjectNode jsonObj, final String scope, final long runId) {
		jsonObj.put("metrics_schema", METRICS_SCHEMA_VERSION);
		jsonObj.put("scope", scope);
		jsonObj.put("role", resolveRole());
		jsonObj.put("node_id", nodeId);
		if (clusterId != null && !clusterId.isBlank()) {
			jsonObj.put("cluster_id", clusterId);
		}
		jsonObj.put("run_id", String.valueOf(runId));
		jsonObj.put("sample_ts", Instant.now().toString());
	}

	private String resolveRole() {
		return metricsManager.getDistributedContexts().isEmpty() ? "worker" : "entry";
	}

	private static String resolveNodeId(final Config config) {
		for (String path : List.of("run-node-id", "output-metrics-node-id", "run-comment")) {
			try {
				final String value = config.stringVal(path);
				if (value != null && !value.isBlank()) {
					return value;
				}
			} catch (Exception ignore) {}
		}
		try {
			final String host = InetAddress.getLocalHost().getHostName();
			int port = 0;
			try {
				port = config.intVal("run-port");
			} catch (Exception ignore) {}
			return port > 0 ? host + ':' + port : host;
		} catch (Exception ignore) {
			return "spt-node";
		}
	}

	private static String resolveClusterId(final Config config) {
		for (String path : List.of("run-cluster-id", "run-cluster")) {
			try {
				final String value = config.stringVal(path);
				if (value != null && !value.isBlank()) {
					return value;
				}
			} catch (Exception ignore) {}
		}
		return null;

	}

	private static long resolveConfiguredRunId(final Config config) {
		try {
			return config.longVal("run-id");
		} catch (Exception ignore) {
			return 0L;
		}
	}
}
