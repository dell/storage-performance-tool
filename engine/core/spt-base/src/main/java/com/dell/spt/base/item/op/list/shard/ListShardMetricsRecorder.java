package com.dell.spt.base.item.op.list.shard;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 * Hook for capturing per-slice instrumentation. Implementations can emit metrics for slice lifecycle
 * events and per-page throughput.
 */
public interface ListShardMetricsRecorder {

	ListShardMetricsRecorder NO_OP = new ListShardMetricsRecorder() {
		@Override
		public Snapshot snapshot() {
			return Snapshot.EMPTY;
		}
	};

	record ShardSnapshot(
					String prefix,
					long pages,
					long objects,
					double avgPageMillis,
					long firstUpdateMillis,
					long lastUpdateMillis,
					boolean active,
					int stallWarnings,
					int consecutiveFullPages,
					int lastPageObjects,
					int lastPageMaxKeys,
					long lastFullPageMillis,
					int splitCount,
					int maxDepth,
					long lastSplitMillis,
					String lastSplitReason,
					int lastSplitChildren,
					int rescueCount,
					long lastRescueMillis,
					int retryCount,
					String lastRetryStatus) {}

	record Snapshot(
					Collection<ShardSnapshot> shards,
					long totalPages,
					long totalObjects,
					double avgPageMillis,
					long capturedAtMillis,
					long totalSplits,
					int maxDepth,
					Map<String, Integer> splitReasons,
					long totalRescues) {

		static final Snapshot EMPTY =
					new Snapshot(Collections.emptyList(), 0L, 0L, 0.0d, 0L, 0L, 0, Collections.emptyMap(), 0L);
	}

	default void onLease(final ListShard shard) {}

	default void onRequeue(final ListShard shard) {}

	default void onComplete(final ListShard shard) {}

	/**
	 * Invoked when the watchdog rescues a shard from an abandoned lease and requeues it for continued
	 * processing.
	 */
	default void onRescue(final ListShard shard) {}

	/**
	 * Invoked when a LIST operation is retried due to a service/client error while retaining the same shard
	 * metadata.
	 */
	default void onRetry(final ListShard shard, final com.dell.spt.base.item.op.Operation.Status status) {}

	default void onSplit(
					final ListShard parent,
					final ListShardSplitHeuristics.Decision decision,
					final java.util.List<ListShard> children) {}

	default void onPageResult(
					final ListShard shard,
					final int objectsListed,
					final int maxKeys,
					final long durationNanos) {}

	/**
	 * Exposes the configured stall threshold (in milliseconds) so queue implementations can align
	 * heartbeat timeouts with operator expectations.
	 */
	default long stallThresholdMillis() {
		return 0L;
	}

	default Snapshot snapshot() {
		return Snapshot.EMPTY;
	}

	default void emitSummary(final String stepId) {}
}
