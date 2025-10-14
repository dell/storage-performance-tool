package com.dell.spt.base.item.op.list.shard;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.ShardSnapshot;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.Snapshot;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ListShardSplitPlannerTest {

	private ListShardingConfig shardingConfig;
	private ListShardSplitHeuristics heuristics;
	private ListShardQueue queue;
	private ListShardSplitPlanner planner;

	@BeforeEach
	void setUp() throws IllegalConfigurationException {
		shardingConfig = ListShardingConfig.parse(staticAdaptiveConfig(4, "0123", Map.of("full_page_threshold", 1)));
		heuristics = new ListShardSplitHeuristics(shardingConfig.adaptive());
		queue = ListShardQueue.inMemory();
		planner = new ListShardSplitPlanner(queue, heuristics, shardingConfig);
	}

	@Test
	void splitsParentAndEnqueuesChildren() throws Exception {
		final ListShard parent = new ListShard("a", null, null, null, "a0zzz", 0);
		queue.offer(parent);
		try (ListShardQueue.Lease lease = queue.acquire()) {
			final var metrics = snapshotFor(parent.prefix(), 5, 2, 100);
			final boolean split = planner.maybeSplit(lease, metrics, 1);
			assertTrue(split, "expected split to occur");
		}
		assertEquals(4, queue.pendingCount());
		final List<ListShard> children = drainQueue(queue);
		assertEquals(List.of("a0", "a1", "a2", "a3"), prefixes(children));
		assertTrue(children.stream().allMatch(child -> child.splitDepth() == 1));
		final ListShard continuingChild = children.stream().filter(shard -> shard.prefix().equals("a0")).findFirst().orElseThrow();
		assertEquals("a0zzz", continuingChild.startAfter());
	}

	@Test
	void startAfterAppliesOnlyToCurrentPrefix() throws Exception {
		final ListShard parent = new ListShard("logs/a", null, null, null, "logs/a2last", 2);
		queue.offer(parent);
		try (ListShardQueue.Lease lease = queue.acquire()) {
			final var metrics = snapshotFor(parent.prefix(), 10, 0, 50);
			assertTrue(planner.maybeSplit(lease, metrics, 2));
		}
		final List<ListShard> children = drainQueue(queue);
		final ListShard match = children.stream().filter(shard -> shard.prefix().equals("logs/a2")).findFirst().orElseThrow();
		assertEquals("logs/a2last", match.startAfter());
		children.stream()
						.filter(shard -> !shard.prefix().equals("logs/a2"))
						.forEach(shard -> assertNull(shard.startAfter(), () -> "unexpected startAfter for " + shard.prefix()));
	}

	@Test
	void respectsHeuristicNoSplitDecision() throws Exception {
		final ListShard parent = new ListShard("p", null, null, null);
		queue.offer(parent);
		try (ListShardQueue.Lease lease = queue.acquire()) {
			final var metrics = snapshotFor(parent.prefix(), 0, 0, 0);
			assertFalse(planner.maybeSplit(lease, metrics, 1));
			lease.requeue();
		}
		assertEquals(1, queue.pendingCount());
	}

	@Test
	void maybeSplitReturnsFalseIfLeaseAlreadyClosed() throws Exception {
		final var splits = new ArrayList<ListShard>();
		final var recorder = new ListShardMetricsRecorderAdapter() {
			@Override
			public void onSplit(final ListShard parent, final ListShardSplitHeuristics.Decision decision, final List<ListShard> children) {
				splits.add(parent);
			}
		};
		final var localQueue = ListShardQueue.inMemory(recorder);
		final var localHeuristics = new ListShardSplitHeuristics(shardingConfig.adaptive());
		final var localPlanner = new ListShardSplitPlanner(localQueue, localHeuristics, shardingConfig);
		final ListShard parent = new ListShard("c", null, null, null);
		localQueue.offer(parent);
		try (ListShardQueue.Lease lease = localQueue.acquire()) {
			lease.complete();
			final var metrics = snapshotFor(parent.prefix(), 5, 0, 100);
			assertFalse(localPlanner.maybeSplit(lease, metrics, 1));
		}
		assertTrue(splits.isEmpty(), "onSplit should not be invoked when lease is closed");
	}

	@Test
	void cooldownPreventsImmediateResplit() throws Exception {
		final var clock = new MutableClock();
		final var configWithCooldown = ListShardingConfig.parse(staticAdaptiveConfig(4, "abcd", Map.of("cooldown", "10s")));
		final var localHeuristics = new ListShardSplitHeuristics(configWithCooldown.adaptive());
		final var localQueue = ListShardQueue.inMemory();
		final var localPlanner = new ListShardSplitPlanner(localQueue, localHeuristics, configWithCooldown, clock);

		final ListShard parent = new ListShard("logs/", null, null, null, "logs/z", 0);
		localQueue.offer(parent);
		try (ListShardQueue.Lease lease = localQueue.acquire()) {
			final var metrics = snapshotFor(parent.prefix(), 5, 0, clock.millis());
			assertTrue(localPlanner.maybeSplit(lease, metrics, 1));
		}
		assertTrue(localQueue.pendingCount() > 0);
		// Offer a fresh shard with the same prefix before cooldown expires
		final ListShard retryShard = new ListShard("logs/", null, null, null, "logs/z", 0);
		localQueue.offer(retryShard);
		try (ListShardQueue.Lease lease = localQueue.acquire()) {
			final var metrics = snapshotFor(retryShard.prefix(), 5, 0, clock.millis());
			assertFalse(localPlanner.maybeSplit(lease, metrics, 1), "cooldown should block immediate resplit");
			lease.requeue();
		}
	}

	@Test
	void branchFactorZeroSkipsSplitting() throws Exception {
		final var configSingle = ListShardingConfig.parse(staticAdaptiveConfig(0, "x", Map.of()));
		final var noSplitPlanner = new ListShardSplitPlanner(queue, heuristics, configSingle);
		queue.offer(new ListShard("x", null, null, null));
		try (ListShardQueue.Lease lease = queue.acquire()) {
			assertFalse(noSplitPlanner.maybeSplit(lease, snapshotFor("x", 10, 0, 0), 1));
			lease.requeue();
		}
	}

	private abstract static class ListShardMetricsRecorderAdapter implements ListShardMetricsRecorder {
		@Override
		public void onLease(final ListShard shard) {}

		@Override
		public void onRequeue(final ListShard shard) {}

		@Override
		public void onComplete(final ListShard shard) {}
	}

	private static final class MutableClock extends java.time.Clock {
		private long currentMillis;

		@Override
		public java.time.ZoneId getZone() {
			return java.time.ZoneOffset.UTC;
		}

		@Override
		public java.time.Clock withZone(final java.time.ZoneId zone) {
			return this;
		}

		@Override
		public long millis() {
			return currentMillis;
		}

		@Override
		public java.time.Instant instant() {
			return java.time.Instant.ofEpochMilli(currentMillis);
		}
	}

	private static Config staticAdaptiveConfig(
					final int radix,
					final String charset,
					final Map<String, Object> adaptiveOverrides) {
		final Map<String, Object> adaptiveSchema = new HashMap<>();
		adaptiveSchema.put("full_page_threshold", Integer.class);
		adaptiveSchema.put("stall_warning_threshold", Integer.class);
		adaptiveSchema.put("backlog_ratio", Double.class);
		adaptiveSchema.put("min_pending_shards", Integer.class);
		adaptiveSchema.put("cooldown", String.class);
		final Map<String, Object> shardingSchema = new HashMap<>();
		shardingSchema.put("mode", String.class);
		shardingSchema.put("radix", Integer.class);
		shardingSchema.put("charset", String.class);
		shardingSchema.put("adaptive", adaptiveSchema);
		final Map<String, Object> schema = new HashMap<>();
		schema.put("sharding", shardingSchema);

		final Map<String, Object> adaptiveValues = new HashMap<>();
		adaptiveValues.put("full_page_threshold", adaptiveOverrides.getOrDefault("full_page_threshold", 1));
		adaptiveValues.put("stall_warning_threshold", adaptiveOverrides.getOrDefault("stall_warning_threshold", 0));
		adaptiveValues.put("backlog_ratio", adaptiveOverrides.getOrDefault("backlog_ratio", 1.0d));
		adaptiveValues.put("min_pending_shards", adaptiveOverrides.getOrDefault("min_pending_shards", 0));
		adaptiveValues.put("cooldown", adaptiveOverrides.getOrDefault("cooldown", "0s"));

		final Map<String, Object> shardingValues = new HashMap<>();
		shardingValues.put("mode", "static");
		shardingValues.put("radix", radix);
		shardingValues.put("charset", charset);
		shardingValues.put("adaptive", adaptiveValues);

		final Map<String, Object> values = new HashMap<>();
		values.put("sharding", shardingValues);
		return new BasicConfig("-", schema, values);
	}

	private static Snapshot snapshotFor(
					final String prefix,
					final long pages,
					final int stallWarnings,
					final long capturedAtMillis) {
		final ShardSnapshot shardSnapshot = new ShardSnapshot(
						prefix,
						pages,
						pages * 1_000,
						1.0d,
						0L,
						capturedAtMillis,
						true,
						stallWarnings,
						(int) pages,
						1_000,
						1_000,
						capturedAtMillis,
						0,
						0,
						0L,
						"NONE",
						0,
						0,
						0L,
						0,
						"NONE");
		return new Snapshot(
						List.of(shardSnapshot),
						pages,
						pages * 1_000,
						1.0d,
						capturedAtMillis,
						0L,
						0,
						java.util.Collections.emptyMap(),
						0L);
	}

	private static List<String> prefixes(final List<ListShard> shards) {
		return shards.stream().map(ListShard::prefix).toList();
	}

	private static List<ListShard> drainQueue(final ListShardQueue queue) throws Exception {
		final List<ListShard> result = new ArrayList<>();
		while (queue.pendingCount() > 0) {
			try (ListShardQueue.Lease lease = queue.acquire()) {
				result.add(lease.shard());
				lease.complete();
			}
		}
		return result;
	}
}
