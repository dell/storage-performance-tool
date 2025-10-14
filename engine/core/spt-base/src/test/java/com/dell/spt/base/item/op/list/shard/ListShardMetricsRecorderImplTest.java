package com.dell.spt.base.item.op.list.shard;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

class ListShardMetricsRecorderImplTest {

	@Test
	void recordsPageStatistics() {
		final var clock = new MutableClock();
		final var recorder = new ListShardMetricsRecorderImpl(clock, Duration.ZERO, Duration.ZERO);
		final var shard = new ListShard("prefix", null, null, null);

		recorder.onLease(shard);
		recorder.onPageResult(shard, 100, 1_000, 1_000_000L);

		final var snapshot = recorder.snapshot();
		assertEquals(1L, snapshot.totalPages());
		assertEquals(100L, snapshot.totalObjects());
		assertEquals(1.0d, snapshot.avgPageMillis(), 1e-6);
		assertEquals(0L, snapshot.totalSplits());
		assertEquals(0, snapshot.maxDepth());
		assertTrue(snapshot.splitReasons().isEmpty());
		assertEquals(0L, snapshot.totalRescues());

		final var shardStats = snapshot.shards().iterator().next();
		assertEquals("prefix", shardStats.prefix());
		assertEquals(1L, shardStats.pages());
		assertEquals(100L, shardStats.objects());
		assertEquals(1.0d, shardStats.avgPageMillis(), 1e-6);
		assertTrue(shardStats.active());
		assertEquals(0, shardStats.consecutiveFullPages());
		assertEquals(100, shardStats.lastPageObjects());
		assertEquals(1_000, shardStats.lastPageMaxKeys());
		assertEquals(0L, shardStats.lastFullPageMillis());
		assertEquals(0, shardStats.splitCount());
		assertEquals(0, shardStats.maxDepth());
		assertEquals(0L, shardStats.lastSplitMillis());
		assertEquals("NONE", shardStats.lastSplitReason());
		assertEquals(0, shardStats.lastSplitChildren());
		assertEquals(0, shardStats.rescueCount());
		assertEquals(0L, shardStats.lastRescueMillis());
	}

	@Test
	void detectsStallWhenNoProgress() {
		final var clock = new MutableClock();
		final var recorder = new ListShardMetricsRecorderImpl(clock, Duration.ofSeconds(30), Duration.ZERO);
		final var shardA = new ListShard("A", null, null, null);
		final var shardB = new ListShard("B", null, null, null);

		recorder.onLease(shardA);
		recorder.onPageResult(shardA, 10, 1_000, 500_000L);
		clock.advance(Duration.ofSeconds(31));

		recorder.onLease(shardB);
		recorder.onPageResult(shardB, 1, 1_000, 500_000L);

		final var snapshot = recorder.snapshot();
		final var byPrefix = snapshot.shards().stream().collect(Collectors.toMap(ListShardMetricsRecorder.ShardSnapshot::prefix, s -> s));
		assertEquals(1, byPrefix.get("A").stallWarnings());
		assertTrue(byPrefix.get("A").active());
		assertEquals(0, byPrefix.get("B").stallWarnings());
	}

	@Test
	void doesNotWarnImmediatelyAfterLease() {
		final var clock = new MutableClock();
		clock.advance(Duration.ofHours(1));
		final var recorder = new ListShardMetricsRecorderImpl(clock, Duration.ofSeconds(30), Duration.ZERO);
		final var shardA = new ListShard("A", null, null, null);
		final var shardB = new ListShard("B", null, null, null);

		recorder.onLease(shardA);
		recorder.onPageResult(shardA, 10, 1_000, 500_000L);
		recorder.onLease(shardB);
		clock.advance(Duration.ofSeconds(1));
		recorder.onPageResult(shardA, 5, 1_000, 500_000L);

		final var snapshot = recorder.snapshot();
		final var byPrefix = snapshot.shards().stream().collect(Collectors.toMap(ListShardMetricsRecorder.ShardSnapshot::prefix, s -> s));
		assertEquals(0, byPrefix.get("B").stallWarnings(), "newly leased shard should not generate immediate stall warnings");
	}

	@Test
	void consecutiveFullPagesUsesDefaultMaxKeys() {
		final var clock = new MutableClock();
		final var recorder = new ListShardMetricsRecorderImpl(clock, Duration.ZERO, Duration.ZERO);
		final var shard = new ListShard("prefix", null, null, null);

		recorder.onLease(shard);
		recorder.onPageResult(shard, 1000, 0, 1_000_000L);

		final var snapshot = recorder.snapshot().shards().iterator().next();
		assertEquals(1, snapshot.consecutiveFullPages());
		assertEquals(1000, snapshot.lastPageMaxKeys());
	}

	@Test
	void stallWarningsAccumulateOverTime() {
		final var clock = new MutableClock();
		final var recorder = new ListShardMetricsRecorderImpl(clock, Duration.ofSeconds(30), Duration.ZERO);
		final var shardA = new ListShard("hot", null, null, null);
		final var shardB = new ListShard("other", null, null, null);

		recorder.onLease(shardA);
		recorder.onPageResult(shardA, 10, 1000, 500_000L);
		clock.advance(Duration.ofSeconds(31));
		recorder.onLease(shardB);
		recorder.onPageResult(shardB, 5, 1000, 200_000L);
		clock.advance(Duration.ofSeconds(31));
		recorder.onPageResult(shardB, 1, 1000, 200_000L);

		final var byPrefix = recorder.snapshot().shards().stream().collect(Collectors.toMap(ListShardMetricsRecorder.ShardSnapshot::prefix, s -> s));
		assertEquals(2, byPrefix.get("hot").stallWarnings());
	}

	@Test
	void requeueAndRetryPreserveTotals() {
		final var clock = new MutableClock();
		final var recorder = new ListShardMetricsRecorderImpl(clock, Duration.ZERO, Duration.ZERO);
		final var shard = new ListShard("logs/", null, null, null);

		recorder.onLease(shard);
		recorder.onPageResult(shard, 1000, 1000, 1_000_000L);
		recorder.onRequeue(shard);
		recorder.onPageResult(shard, 20, 1000, 500_000L);

		final var snapshot = recorder.snapshot();
		assertEquals(2, snapshot.totalPages());
		assertEquals(1020, snapshot.totalObjects());
		final var shardSnapshot = snapshot.shards().iterator().next();
		assertEquals(2, shardSnapshot.pages());
		assertEquals(1020, shardSnapshot.objects());
		assertEquals(0, shardSnapshot.consecutiveFullPages());
		assertEquals(20, shardSnapshot.lastPageObjects());
	}

	@Test
	void marksShardComplete() {
		final var clock = new MutableClock();
		final var recorder = new ListShardMetricsRecorderImpl(clock, Duration.ZERO, Duration.ZERO);
		final var shard = new ListShard("complete", null, null, null);

		recorder.onLease(shard);
		recorder.onPageResult(shard, 5, 1_000, 1_500_000L);
		recorder.onComplete(shard);

		final var snapshot = recorder.snapshot();
		final var shardStats = snapshot.shards().iterator().next();
		assertFalse(shardStats.active());
	}

	@Test
	void tracksFullPageStreaksAndResets() {
		final var clock = new MutableClock();
		final var recorder = new ListShardMetricsRecorderImpl(clock, Duration.ZERO, Duration.ZERO);
		final var shard = new ListShard("hot", null, null, null);

		recorder.onLease(shard);
		recorder.onPageResult(shard, 1_000, 1_000, 1_000_000L);
		clock.advance(Duration.ofMillis(1));
		final var firstSnapshot = recorder.snapshot().shards().iterator().next();
		assertEquals(1, firstSnapshot.consecutiveFullPages());
		assertEquals(1_000, firstSnapshot.lastPageObjects());
		assertEquals(1_000, firstSnapshot.lastPageMaxKeys());
		assertTrue(firstSnapshot.lastFullPageMillis() >= 0);

		recorder.onPageResult(shard, 1_000, 1_000, 1_000_000L);
		clock.advance(Duration.ofMillis(1));
		final var secondSnapshot = recorder.snapshot().shards().iterator().next();
		assertEquals(2, secondSnapshot.consecutiveFullPages());

		recorder.onPageResult(shard, 42, 1_000, 1_000_000L);
		final var finalSnapshot = recorder.snapshot().shards().iterator().next();
		assertEquals(0, finalSnapshot.consecutiveFullPages());
		assertEquals(42, finalSnapshot.lastPageObjects());
		assertEquals(1_000, finalSnapshot.lastPageMaxKeys());
	}

	@Test
	void recordsSplitStatistics() {
		final var clock = new MutableClock();
		final var recorder = new ListShardMetricsRecorderImpl(clock, Duration.ZERO, Duration.ZERO, 1, false);
		final var parent = new ListShard("p", null, null, null, null, 0);
		final var children = java.util.List.of(
						new ListShard("pa", null, null, null, null, 1),
						new ListShard("pb", null, null, null, null, 1));
		final var decision = new ListShardSplitHeuristics.Decision(true, ListShardSplitHeuristics.DecisionReason.FULL_PAGE_STREAK, 4, 1, 0.0d, 0, 2);

		recorder.onSplit(parent, decision, children);
		final var snapshot = recorder.snapshot();
		assertEquals(1L, snapshot.totalSplits());
		assertEquals(1, snapshot.maxDepth());
		assertEquals(1, snapshot.splitReasons().get("FULL_PAGE_STREAK"));
		final var parentStats = snapshot.shards().stream().filter(s -> s.prefix().equals("p")).findFirst().orElseThrow();
		assertEquals(1, parentStats.splitCount());
		assertEquals("FULL_PAGE_STREAK", parentStats.lastSplitReason());
		assertEquals(2, parentStats.lastSplitChildren());
		assertTrue(parentStats.lastSplitMillis() >= 0L);
	}

	@Test
	void emitSummaryDoesNotThrow() {
		final var clock = new MutableClock();
		final var recorder = new ListShardMetricsRecorderImpl(clock, Duration.ZERO, Duration.ZERO, 1, true);
		recorder.emitSummary("step-test");
		final var parent = new ListShard("p", null, null, null, null, 0);
		final var children = java.util.List.of(new ListShard("pa", null, null, null, null, 1));
		final var decision = new ListShardSplitHeuristics.Decision(true, ListShardSplitHeuristics.DecisionReason.BACKLOG, 0, 0, 0.0d, 1, 1);
		recorder.onSplit(parent, decision, children);
		recorder.emitSummary("step-test");
	}

	@Test
	void recordsRescueMetrics() {
		final var clock = new MutableClock();
		final var recorder = new ListShardMetricsRecorderImpl(clock, Duration.ZERO, Duration.ZERO);
		final var shard = new ListShard("rescue", null, null, null);

		recorder.onLease(shard);
		clock.advance(Duration.ofSeconds(5));
		recorder.onRescue(shard);

		final var snapshot = recorder.snapshot();
		assertEquals(1L, snapshot.totalRescues());
		final var shardSnapshot = snapshot.shards().iterator().next();
		assertEquals(1, shardSnapshot.rescueCount());
		assertEquals(clock.millis(), shardSnapshot.lastRescueMillis());
	}

	@Test
	void recordsRetryMetrics() {
		final var clock = new MutableClock();
		final var recorder = new ListShardMetricsRecorderImpl(clock, Duration.ZERO, Duration.ZERO);
		final var shard = new ListShard("retry", null, null, null);

		recorder.onLease(shard);
		recorder.onRetry(shard, com.dell.spt.base.item.op.Operation.Status.RESP_FAIL_SVC);
		recorder.onRetry(shard, com.dell.spt.base.item.op.Operation.Status.RESP_FAIL_SVC);
		recorder.onRetry(shard, com.dell.spt.base.item.op.Operation.Status.RESP_FAIL_SVC);

		final var snapshot = recorder.snapshot();
		assertEquals(0L, snapshot.totalRescues());
		final var shardSnapshot = snapshot.shards().iterator().next();
		assertEquals(3, shardSnapshot.retryCount());
		assertEquals("RESP_FAIL_SVC", shardSnapshot.lastRetryStatus());
	}

	private static final class MutableClock extends Clock {

		private long currentMillis;

		void advance(final Duration duration) {
			currentMillis += duration.toMillis();
		}

		@Override
		public ZoneId getZone() {
			return ZoneOffset.UTC;
		}

		@Override
		public Clock withZone(final ZoneId zone) {
			return this;
		}

		@Override
		public long millis() {
			return currentMillis;
		}

		@Override
		public Instant instant() {
			return Instant.ofEpochMilli(currentMillis);
		}
	}
}
