package com.dell.spt.base.item.op.list.shard;

import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.ShardSnapshot;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.Snapshot;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Evaluates shard telemetry to decide whether adaptive splitting should trigger.
 */
public final class ListShardSplitHeuristics {

	private final ListShardingConfig.AdaptiveHeuristicsConfig config;
	private final ConcurrentMap<String, Long> lastSplitMillisByPrefix = new ConcurrentHashMap<>();

	public ListShardSplitHeuristics(final ListShardingConfig.AdaptiveHeuristicsConfig config) {
		this.config = Objects.requireNonNull(config, "config");
	}

	public Decision evaluate(
					final ShardSnapshot shard,
					final QueueSnapshot queue,
					final long nowMillis) {
		if (shard == null) {
			return Decision.noSplit();
		}
		if (isCoolingDown(shard.prefix(), nowMillis)) {
			return Decision.cooldown();
		}
		final int fullPageStreak = Math.max(0, shard.consecutiveFullPages());
		final int stallWarnings = Math.max(0, shard.stallWarnings());
		final double backlogRatio = backlogRatio(queue);
		final int pendingShards = queue == null ? 0 : Math.max(0, queue.pendingShards());
		final int activeLeases = queue == null ? 0 : Math.max(0, queue.activeLeases());
		if (config.fullPageThreshold() > 0 && fullPageStreak >= config.fullPageThreshold()) {
			return new Decision(true, DecisionReason.FULL_PAGE_STREAK, fullPageStreak, stallWarnings, backlogRatio, pendingShards, activeLeases);
		}
		if (config.stallWarningThreshold() > 0 && stallWarnings >= config.stallWarningThreshold()) {
			return new Decision(true, DecisionReason.STALL_WARNINGS, fullPageStreak, stallWarnings, backlogRatio, pendingShards, activeLeases);
		}
		if (shouldSplitByBacklog(pendingShards, activeLeases, backlogRatio)) {
			return new Decision(true, DecisionReason.BACKLOG, fullPageStreak, stallWarnings, backlogRatio, pendingShards, activeLeases);
		}
		return new Decision(false, DecisionReason.NONE, fullPageStreak, stallWarnings, backlogRatio, pendingShards, activeLeases);
	}

	public void recordSplit(final String prefix, final long atMillis) {
		if (prefix == null) {
			return;
		}
		lastSplitMillisByPrefix.put(prefix, atMillis);
	}

	public void resetCooldown(final String prefix) {
		if (prefix == null) {
			return;
		}
		lastSplitMillisByPrefix.remove(prefix);
	}

	private boolean isCoolingDown(final String prefix, final long nowMillis) {
		final Duration cooldown = config.cooldown();
		if (cooldown.isZero() || cooldown.isNegative() || prefix == null) {
			return false;
		}
		final Long lastSplit = lastSplitMillisByPrefix.get(prefix);
		if (lastSplit == null) {
			return false;
		}
		return nowMillis - lastSplit < cooldown.toMillis();
	}

	private boolean shouldSplitByBacklog(
					final int pendingShards, final int activeLeases, final double backlogRatio) {
		if (pendingShards <= 0) {
			return false;
		}
		if (pendingShards < config.minPendingShards()) {
			return false;
		}
		if (config.backlogSplitRatio() <= 0) {
			return false;
		}
		final int normalizedActive = activeLeases <= 0 ? 1 : activeLeases;
		final double effectiveRatio = backlogRatio > 0 ? backlogRatio : (double) pendingShards / normalizedActive;
		return effectiveRatio >= config.backlogSplitRatio();
	}

	private static double backlogRatio(final QueueSnapshot queue) {
		if (queue == null) {
			return 0.0d;
		}
		final int pending = Math.max(0, queue.pendingShards());
		final int active = Math.max(1, queue.activeLeases());
		return pending / (double) active;
	}

	public record QueueSnapshot(int pendingShards, int activeLeases) {}

	public enum DecisionReason {
		NONE, FULL_PAGE_STREAK, STALL_WARNINGS, BACKLOG, COOLDOWN
	}

	public record Decision(
				boolean shouldSplit,
				DecisionReason reason,
				int fullPageStreak,
				int stallWarnings,
				double backlogRatio,
				int pendingShards,
				int activeLeases) {

		private static Decision noSplit() {
			return new Decision(false, DecisionReason.NONE, 0, 0, 0.0d, 0, 0);
		}

		private static Decision cooldown() {
			return new Decision(false, DecisionReason.COOLDOWN, 0, 0, 0.0d, 0, 0);
		}
 	}

	public static QueueSnapshot queueSnapshot(final ListShardQueue queue, final int activeLeases) {
		if (queue == null) {
			return new QueueSnapshot(0, activeLeases);
		}
		return new QueueSnapshot(queue.pendingCount(), activeLeases);
	}

	public Decision evaluateSnapshot(final Snapshot snapshot, final String prefix, final QueueSnapshot queue) {
		if (snapshot == null || prefix == null) {
			return Decision.noSplit();
		}
		for (ShardSnapshot shard : snapshot.shards()) {
			if (prefix.equals(shard.prefix())) {
				return evaluate(shard, queue, snapshot.capturedAtMillis());
			}
		}
		return Decision.noSplit();
	}
}
