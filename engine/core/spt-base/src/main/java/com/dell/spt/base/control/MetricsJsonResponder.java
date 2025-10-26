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
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import com.dell.spt.base.logging.Loggers;

final class MetricsJsonResponder {

	private static final int METRICS_SCHEMA_VERSION = 2;

	private final MetricsManager metricsManager;
	private final ObjectMapper objectMapper = new ObjectMapper();
	private final String nodeId;
	private final String clusterId;
	private final long configuredRunId;
	private final boolean entryRoleHint;

	MetricsJsonResponder(final MetricsManager metricsManager, final Config config) {
		this.metricsManager = metricsManager;
		this.nodeId = resolveNodeId(config);
		this.clusterId = resolveClusterId(config);
		this.configuredRunId = resolveConfiguredRunId(config);
		this.entryRoleHint = resolveEntryRoleHint(config);
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

	ArrayNode buildClusterMetrics(final boolean verbose) {
		final ArrayNode jsonArray = objectMapper.createArrayNode();
		final Set<DistributedMetricsContext> distributed = metricsManager.getDistributedContexts();
		final Set<MetricsContext> allContexts = metricsManager.getAllContexts();

		final Set<String> activeStepIds = new HashSet<>();

		for (DistributedMetricsContext ctx : distributed) {
			ctx.refreshLastSnapshot();
			final AllMetricsSnapshot snapshot = ctx.lastSnapshot();
			if (snapshot == null) {
				continue;
			}
			final ObjectNode jsonObj = buildDistributedNode(ctx, snapshot, distributed, allContexts, verbose);
			jsonObj.put("role", "aggregate");
			jsonArray.add(jsonObj);
			activeStepIds.add(ctx.loadStepId());
		}
		appendDistributedTerminalEntries(jsonArray, activeStepIds);
		return jsonArray;
	}

	@Deprecated
	@SuppressWarnings("InlineMeSuggester")
	ArrayNode buildFleetMetrics(final boolean verbose) {
		return buildClusterMetrics(verbose);
	}

	private ObjectNode buildContextNode(
					final MetricsContext ctx,
					final AllMetricsSnapshot snapshot,
					final Set<DistributedMetricsContext> distributed,
					final Set<MetricsContext> allContexts,
					final boolean verbose) {
		final TerminalStepEntry progressEntry = metricsManager.getLastProgressSnapshot(ctx.loadStepId()).orElse(null);

		long successCount = snapshot.successSnapshot().count();
		long failCount = snapshot.failsSnapshot().count();
		long bytesTotal = snapshot.byteSnapshot().count();
		double latencyMean = snapshot.latencySnapshot().mean();
		double durationMean = snapshot.durationSnapshot().mean();
		long elapsedMillis = snapshot.elapsedTimeMillis();

		boolean cachedProgress = false;
		if (progressEntry != null) {
			successCount = Math.max(successCount, progressEntry.successCount);
			failCount = Math.max(failCount, progressEntry.failedCount);
			bytesTotal = Math.max(bytesTotal, progressEntry.bytesTotal);
			if (progressEntry.latencyMeanUs > 0.0) {
				latencyMean = progressEntry.latencyMeanUs;
			}
			if (progressEntry.durationMeanUs > 0.0) {
				durationMean = progressEntry.durationMeanUs;
			}
			elapsedMillis = Math.max(elapsedMillis, progressEntry.elapsedTimeMillis);
			Loggers.MSG.debug(
							"{}: node metrics using cached progress (success={}, fails={}, bytes={}, elapsed_ms={})",
							ctx.loadStepId(),
							successCount,
							failCount,
							bytesTotal,
							elapsedMillis);
			cachedProgress = true;
		}

		final int testState = calculateTestState(snapshot, successCount, failCount);
		final int completionPercent = calculateCompletionPercent(ctx.metadata(), successCount, failCount, elapsedMillis);

		final ObjectNode jsonObj = objectMapper.createObjectNode();
		applyCommonMetadata(jsonObj, "node", ctx.runId());
		jsonObj.put("step_id", ctx.loadStepId());
		jsonObj.put("op_type", ctx.opType().name());
		jsonObj.put("timestamp", System.currentTimeMillis());
		jsonObj.put("elapsed_time_seconds", elapsedMillis / 1000.0);
		jsonObj.put("test_state", testState);
		jsonObj.put(MetricsConstants.METRIC_NAME_COMPLETION, completionPercent);

		addLimitFields(jsonObj, ctx.metadata());
		addMetrics(jsonObj, snapshot, successCount, failCount, bytesTotal, latencyMean, durationMean);
		addListShardMetrics(jsonObj, ctx.metadata());

		jsonObj.put(
						"overall_completion_percent",
						calculateOverallCompletionPercentForStep(ctx.loadStepId(), distributed, allContexts));
		jsonObj.put("overall_unbounded", calculateOverallUnboundedForStep(ctx.loadStepId(), distributed, allContexts));

		if (verbose && cachedProgress) {
			jsonObj.put("diag_cached_progress", true);
		}
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
		final TerminalStepEntry progressEntry = metricsManager.getLastProgressSnapshot(ctx.loadStepId(), true).orElse(null);

		long successCount = snapshot.successSnapshot().count();
		long failCount = snapshot.failsSnapshot().count();
		long bytesTotal = snapshot.byteSnapshot().count();
		double latencyMean = snapshot.latencySnapshot().mean();
		double durationMean = snapshot.durationSnapshot().mean();
		long elapsedMillis = snapshot.elapsedTimeMillis();

		boolean cachedProgress = false;
		if (progressEntry != null) {
			successCount = Math.max(successCount, progressEntry.successCount);
			failCount = Math.max(failCount, progressEntry.failedCount);
			bytesTotal = Math.max(bytesTotal, progressEntry.bytesTotal);
			if (progressEntry.latencyMeanUs > 0.0) {
				latencyMean = progressEntry.latencyMeanUs;
			}
			if (progressEntry.durationMeanUs > 0.0) {
				durationMean = progressEntry.durationMeanUs;
			}
			elapsedMillis = Math.max(elapsedMillis, progressEntry.elapsedTimeMillis);
			Loggers.MSG.debug(
							"{}: aggregate metrics using cached progress (success={}, fails={}, bytes={}, elapsed_ms={})",
							ctx.loadStepId(),
							successCount,
							failCount,
							bytesTotal,
							elapsedMillis);
			cachedProgress = true;
		}

		final int testState = calculateTestState(snapshot, successCount, failCount);
		final int completionPercent = calculateCompletionPercent(ctx.metadata(), successCount, failCount, elapsedMillis);

		final ObjectNode jsonObj = objectMapper.createObjectNode();
		applyCommonMetadata(jsonObj, "fleet", ctx.runId());
		jsonObj.put("step_id", ctx.loadStepId());
		jsonObj.put("op_type", ctx.opType().name());
		jsonObj.put("timestamp", System.currentTimeMillis());
		jsonObj.put("elapsed_time_seconds", elapsedMillis / 1000.0);
		jsonObj.put("test_state", testState);
		jsonObj.put(MetricsConstants.METRIC_NAME_COMPLETION, completionPercent);

		addLimitFields(jsonObj, ctx.metadata());
		addMetrics(jsonObj, snapshot, successCount, failCount, bytesTotal, latencyMean, durationMean);
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

		if (verbose && cachedProgress) {
			jsonObj.put("diag_cached_progress", true);
		}
		if (verbose) {
			jsonObj.put("diag_distributed_contexts", distributed.size());
			jsonObj.put("diag_local_contexts", Math.max(0, allContexts.size() - distributed.size()));
		}
		return jsonObj;
	}

	private void addLimitFields(final ObjectNode jsonObj, final java.util.Map meta) {
		long countLimit = metadataLong(meta, MetricsConstants.METADATA_LIMIT_OP_COUNT);
		long timeLimitSec = metadataLong(meta, MetricsConstants.METADATA_LIMIT_TIME_SEC);

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

	private void addMetrics(
					final ObjectNode jsonObj,
					final AllMetricsSnapshot snapshot,
					final long successCount,
					final long failCount,
					final long bytesTotal,
					final double latencyMean,
					final double durationMean) {
		final ObjectNode operations = objectMapper.createObjectNode();
		operations.put("success_count", successCount);
		operations.put("failed_count", failCount);
		operations.put("success_rate_last", snapshot.successSnapshot().last());
		operations.put("failed_rate_last", snapshot.failsSnapshot().last());
		jsonObj.set("operations", operations);

		final ObjectNode bandwidth = objectMapper.createObjectNode();
		bandwidth.put("bytes_total", bytesTotal);
		bandwidth.put("bytes_rate_last", snapshot.byteSnapshot().last());
		jsonObj.set("bandwidth", bandwidth);

		final ObjectNode timing = objectMapper.createObjectNode();
		timing.put("latency_mean_us", latencyMean);
		timing.put("duration_mean_us", durationMean);
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
			if (entry.distributed) {
				continue;
			}
			if (activeStepIds.contains(entry.stepId)) {
				continue;
			}
			final ObjectNode jsonObj = objectMapper.createObjectNode();
			final long runId = entry.runId > 0 ? entry.runId : configuredRunId;
			applyCommonMetadata(jsonObj, "node", runId);
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

	private void appendDistributedTerminalEntries(final ArrayNode jsonArray, final Set<String> activeStepIds) {
		for (TerminalStepEntry entry : metricsManager.getTerminalSteps()) {
			if (!entry.distributed) {
				continue;
			}
			if (activeStepIds.contains(entry.stepId)) {
				continue;
			}
			final ObjectNode jsonObj = objectMapper.createObjectNode();
			final long runId = entry.runId > 0 ? entry.runId : configuredRunId;
			applyCommonMetadata(jsonObj, "fleet", runId);
			jsonObj.put("role", "aggregate");
			jsonObj.put("step_id", entry.stepId);
			jsonObj.put("op_type", entry.opType.name());
			jsonObj.put("timestamp", entry.recordedAtMillis);
			jsonObj.put("elapsed_time_seconds", entry.elapsedTimeMillis / 1000.0);
			jsonObj.put("test_state", 2);

			final boolean hasCountLimit = entry.countLimit > 0L;
			final boolean hasTimeLimit = entry.timeLimitSec > 0L;
			final boolean unbounded = !(hasCountLimit || hasTimeLimit);
			jsonObj.put("unbounded", unbounded);

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

			jsonObj.put("overall_completion_percent", completionPercent);
			jsonObj.put("overall_unbounded", unbounded);

			jsonObj.put("nodes_count", Math.max(0, entry.nodeCount));
			final ArrayNode nodesPresent = objectMapper.createArrayNode();
			if (entry.nodesPresent != null) {
				entry.nodesPresent.forEach(nodesPresent::add);
			}
			jsonObj.set("nodes_present", nodesPresent);
			final boolean partial = entry.partial && entry.nodeCount > 0;
			jsonObj.put("partial", partial);
			jsonObj.put("terminal", true);
			jsonArray.add(jsonObj);
		}
	}

	private int calculateTestState(final AllMetricsSnapshot snapshot, final long successCount, final long failsCount) {
		final long totalOps = successCount + failsCount;
		if (totalOps > 0) {
			final long concurrency = snapshot.concurrencySnapshot().last();
			return concurrency > 0 ? 1 : 2;
		}
		return 1;
	}

	private int calculateCompletionPercent(
					final java.util.Map meta, final long successCount, final long failsCount, final long elapsedTimeMillis) {
		final long countLimit = metadataLong(meta, MetricsConstants.METADATA_LIMIT_OP_COUNT);
		final long timeLimitSec = metadataLong(meta, MetricsConstants.METADATA_LIMIT_TIME_SEC);

		double completion = 0.0;
		if (countLimit > 0) {
			completion = Math.min(1.0, Math.max(0.0, (double) (successCount + failsCount) / (double) countLimit));
		} else if (timeLimitSec > 0) {
			completion = Math.min(1.0, Math.max(0.0, (elapsedTimeMillis / 1000.0) / (double) timeLimitSec));
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

		final Optional<TerminalStepEntry> distributedProgress = metricsManager.getLastProgressSnapshot(stepId, true);
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
				if (!stepId.equals(ctx.loadStepId())) {
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

		final Optional<TerminalStepEntry> localProgress = usingDistributed ? Optional.empty() : metricsManager.getLastProgressSnapshot(stepId);

		for (int i = 0; i < snaps.size(); i++) {
			final AllMetricsSnapshot snapshot = snaps.get(i);
			final java.util.Map meta = metas.get(i);
			if (first) {
				allHaveCount = true;
				first = false;
			}
			final Object cl = meta.get(MetricsConstants.METADATA_LIMIT_OP_COUNT);
			final long countLimit = (cl instanceof Number) ? ((Number) cl).longValue() : 0L;

			long successCount = snapshot.successSnapshot().count();
			long failCount = snapshot.failsSnapshot().count();
			long elapsedMillis = snapshot.elapsedTimeMillis();

			if (usingDistributed) {
				if (distributedProgress.isPresent()) {
					final TerminalStepEntry entry = distributedProgress.get();
					successCount = Math.max(successCount, entry.successCount);
					failCount = Math.max(failCount, entry.failedCount);
					elapsedMillis = Math.max(elapsedMillis, entry.elapsedTimeMillis);
				}
			} else if (localProgress.isPresent()) {
				final TerminalStepEntry entry = localProgress.get();
				successCount = Math.max(successCount, entry.successCount);
				failCount = Math.max(failCount, entry.failedCount);
				elapsedMillis = Math.max(elapsedMillis, entry.elapsedTimeMillis);
			}

			if (countLimit <= 0) {
				allHaveCount = false;
			} else {
				sumCountLimit += countLimit;
				sumCompleted += (successCount + failCount);
			}
			final Object tl = meta.get(MetricsConstants.METADATA_LIMIT_TIME_SEC);
			if (tl instanceof Number && ((Number) tl).longValue() > 0) {
				timeLimitSec = Math.max(timeLimitSec, ((Number) tl).longValue());
			}
			maxElapsed = Math.max(maxElapsed, elapsedMillis);
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
		return metadataLong(meta, MetricsConstants.METADATA_LIMIT_OP_COUNT) > 0L
						|| metadataLong(meta, MetricsConstants.METADATA_LIMIT_TIME_SEC) > 0L;
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
		if (!metricsManager.getDistributedContexts().isEmpty() || entryRoleHint) {
			return "entry";
		}
		return "worker";
	}

	private static boolean resolveEntryRoleHint(final Config config) {
		final Boolean value = safeBoolean(config, "run-node");
		return Boolean.TRUE.equals(value);
	}

	private static String resolveNodeId(final Config config) {
		for (String path : List.of("run-node-id", "output-metrics-node-id", "run-comment")) {
			final String value = safeString(config, path);
			if (value != null && !value.isBlank()) {
				return value;
			}
		}
		final String host = safeHostname();
		final Integer port = safeInteger(config, "run-port");
		if (host == null) {
			return "spt-node";
		}
		return port != null && port > 0 ? host + ':' + port : host;
	}

	private static String resolveClusterId(final Config config) {
		for (String path : List.of("run-cluster-id", "run-cluster")) {
			final String value = safeString(config, path);
			if (value != null && !value.isBlank()) {
				return value;
			}
		}
		return null;

	}

	private static long resolveConfiguredRunId(final Config config) {
		final Long runId = safeLong(config, "run-id");
		return runId == null ? 0L : runId;
	}

	private static long metadataLong(final java.util.Map meta, final String key) {
		if (meta == null) {
			return 0L;
		}
		final Object value = meta.get(key);
		if (value instanceof Number) {
			return ((Number) value).longValue();
		}
		if (value != null) {
			Loggers.MSG.debug(
							"MetricsJsonResponder: metadata value for key {} is not numeric (type={})",
							key,
							value.getClass().getSimpleName());
		}
		return 0L;
	}

	private static String safeString(final Config config, final String path) {
		try {
			return config.stringVal(path);
		} catch (final RuntimeException e) {
			Loggers.MSG.debug("MetricsJsonResponder: unable to read string config \"{}\": {}", path, e.getMessage());
			return null;
		}
	}

	private static Integer safeInteger(final Config config, final String path) {
		try {
			return config.intVal(path);
		} catch (final RuntimeException e) {
			Loggers.MSG.debug("MetricsJsonResponder: unable to read int config \"{}\": {}", path, e.getMessage());
			return null;
		}
	}

	private static Long safeLong(final Config config, final String path) {
		try {
			return config.longVal(path);
		} catch (final RuntimeException e) {
			Loggers.MSG.debug("MetricsJsonResponder: unable to read long config \"{}\": {}", path, e.getMessage());
			return null;
		}
	}

	private static Boolean safeBoolean(final Config config, final String path) {
		try {
			return config.boolVal(path);
		} catch (final RuntimeException e) {
			Loggers.MSG.debug("MetricsJsonResponder: unable to read boolean config \"{}\": {}", path, e.getMessage());
			return null;
		}
	}

	private static String safeHostname() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (final UnknownHostException e) {
			Loggers.ERR.debug("MetricsJsonResponder: unable to resolve local host name", e);
			return null;
		}
	}
}
