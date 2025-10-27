package com.dell.spt.base.item.op.list.shard;

import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.Snapshot;
import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Applies adaptive sharding heuristics and, when triggered, replaces a parent shard with child shards that
 * continue work from the last observed key.
 */
public final class ListShardSplitPlanner {

	private final ListShardQueue queue;
	private final ListShardSplitHeuristics heuristics;
	private final Clock clock;
	private final char[] alphabet;
	private final int branchFactor;

	public ListShardSplitPlanner(
					final ListShardQueue queue,
					final ListShardSplitHeuristics heuristics,
					final ListShardingConfig config) {
		this(queue, heuristics, config, Clock.systemUTC());
	}

	public ListShardSplitPlanner(
					final ListShardQueue queue,
					final ListShardSplitHeuristics heuristics,
					final ListShardingConfig config,
					final Clock clock) {
		this.queue = Objects.requireNonNull(queue, "queue");
		this.heuristics = Objects.requireNonNull(heuristics, "heuristics");
		Objects.requireNonNull(config, "config");
		this.clock = Objects.requireNonNull(clock, "clock");
		final char[] configuredAlphabet = config.alphabet();
		if (configuredAlphabet == null || configuredAlphabet.length == 0) {
			throw new IllegalArgumentException("Sharding alphabet must contain at least one character");
		}
		final int configuredRadix = config.radix() <= 0 ? configuredAlphabet.length : config.radix();
		this.branchFactor = Math.min(Math.max(0, configuredRadix), configuredAlphabet.length);
		this.alphabet = new char[branchFactor];
		System.arraycopy(configuredAlphabet, 0, this.alphabet, 0, branchFactor);
	}

	/**
	 * Evaluate the heuristics for the leased shard and, if a split is warranted, enqueue child shards.
	 *
	 * @param lease the exclusive lease for the parent shard
	 * @param metrics the latest recorder snapshot (may be {@code null})
	 * @param activeLeases current number of active leases
	 * @return {@code true} if a split was performed
	 */
	public boolean maybeSplit(
					final ListShardQueue.Lease lease,
					final Snapshot metrics,
					final int activeLeases) {
		if (lease == null) {
			return false;
		}
		final ListShard parent = lease.shard();
		if (parent == null) {
			return false;
		}
		if (branchFactor <= 1) {
			return false;
		}
		final var queueSnapshot = ListShardSplitHeuristics.queueSnapshot(queue, activeLeases);
		final var decision = heuristics.evaluateSnapshot(metrics, parent.prefix(), queueSnapshot);
		if (!decision.shouldSplit()) {
			return false;
		}
		final List<ListShard> children = createChildren(parent);
		if (children.isEmpty()) {
			return false;
		}
		if (!lease.split(children)) {
			return false;
		}
		queue.metricsRecorder().onSplit(parent, decision, children);
		heuristics.recordSplit(parent.prefix(), clock.millis());
		return true;
	}

	private List<ListShard> createChildren(final ListShard parent) {
		if (branchFactor <= 1) {
			return List.of();
		}
		final var children = new ArrayList<ListShard>(branchFactor);
		final String basePrefix = sanitize(parent.prefix());
		final String parentUpperBound = parent.upperBound();
		final int nextDepth = parent.splitDepth() + 1;
		final int startingIndex = determineStartIndex(parent, basePrefix);
		final String resumeKey = effectiveLastKey(parent);
		for (int i = startingIndex; i < branchFactor; i++) {
			final char c = alphabet[i];
			final String childPrefix = basePrefix + c;
			final String upperBound = computeUpperBound(basePrefix, parentUpperBound, i);
			String childStartAfter = null;
			if (resumeKey != null && resumeKey.startsWith(childPrefix) && i == startingIndex) {
				childStartAfter = resumeKey;
			}
			final ListShard child = new ListShard(childPrefix, upperBound, null, childStartAfter, null, nextDepth);
			children.add(child);
		}
		return List.copyOf(children);
	}

	private int determineStartIndex(final ListShard shard, final String basePrefix) {
		final String lastKey = effectiveLastKey(shard);
		if (lastKey == null || lastKey.length() <= basePrefix.length()) {
			return 0;
		}
		if (!lastKey.startsWith(basePrefix)) {
			return 0;
		}
		final char pivot = lastKey.charAt(basePrefix.length());
		for (int i = 0; i < alphabet.length; i++) {
			if (alphabet[i] == pivot) {
				return i;
			}
		}
		return 0;
	}

	private static String effectiveLastKey(final ListShard shard) {
		if (shard.lastKey() != null) {
			return shard.lastKey();
		}
		return shard.startAfter();
	}

	private String computeUpperBound(
					final String basePrefix, final String parentUpperBound, final int childIndex) {
		if (childIndex + 1 < branchFactor) {
			return basePrefix + alphabet[childIndex + 1];
		}
		return parentUpperBound;
	}

	private static String sanitize(final String prefix) {
		if (prefix == null || prefix.isEmpty() || "/".equals(prefix)) {
			return "";
		}
		return prefix;
	}
}
