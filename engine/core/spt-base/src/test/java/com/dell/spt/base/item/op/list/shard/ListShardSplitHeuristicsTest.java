package com.dell.spt.base.item.op.list.shard;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ListShardSplitHeuristicsTest {

	private ListShardingConfig.AdaptiveHeuristicsConfig config;

	@BeforeEach
	void setUp() throws IllegalConfigurationException {
		config = ListShardingConfig.AdaptiveHeuristicsConfig.parse(adaptiveConfig(Map.of()));
	}

	@Test
	void fullPageStreakTriggersDecision() {
		final var heuristics = new ListShardSplitHeuristics(config);
		final var shard = shardSnapshotWith(4, 0);
		final var decision = heuristics.evaluate(shard, new ListShardSplitHeuristics.QueueSnapshot(0, 2), 0L);
		assertTrue(decision.shouldSplit());
		assertEquals(ListShardSplitHeuristics.DecisionReason.FULL_PAGE_STREAK, decision.reason());
	}

	@Test
	void stallWarningsTriggerWhenStreakBelowThreshold() {
		final var heuristics = new ListShardSplitHeuristics(config);
		final var shard = new ListShardMetricsRecorder.ShardSnapshot(
						"p",
						1L,
						10L,
						1.0d,
						0L,
						0L,
						true,
						3,
						1,
						500,
						1_000,
						0L,
						0,
						0,
						0L,
						"NONE",
						0,
						0,
						0L,
						0,
						"NONE");
		final var decision = heuristics.evaluate(shard, null, 0L);
		assertTrue(decision.shouldSplit());
		assertEquals(ListShardSplitHeuristics.DecisionReason.STALL_WARNINGS, decision.reason());
	}

	@Test
	void backlogTriggersWhenPendingExceedsThreshold() throws IllegalConfigurationException {
		final var heuristics = new ListShardSplitHeuristics(configWith(Map.of("backlog_ratio", 1.2, "min_pending_shards", 3)));
		final var shard = shardSnapshotWith(1, 0);
		final var decision = heuristics.evaluate(
						shard,
						new ListShardSplitHeuristics.QueueSnapshot(6, 2),
						0L);
		assertTrue(decision.shouldSplit());
		assertEquals(ListShardSplitHeuristics.DecisionReason.BACKLOG, decision.reason());
	}

	@Test
	void cooldownSuppressesRapidSplits() throws IllegalConfigurationException {
		final var heuristics = new ListShardSplitHeuristics(
						configWith(Map.of("cooldown", "10s", "full_page_threshold", 1)));
		final var shard = shardSnapshotWith(2, 0);
		final var initialDecision = heuristics.evaluate(shard, null, 0L);
		assertTrue(initialDecision.shouldSplit());
		heuristics.recordSplit("p", 0L);
		final var cooledDecision = heuristics.evaluate(shard, null, 5_000L);
		assertFalse(cooledDecision.shouldSplit());
		assertEquals(ListShardSplitHeuristics.DecisionReason.COOLDOWN, cooledDecision.reason());
		final var postCooldown = heuristics.evaluate(shard, null, 11_000L);
		assertTrue(postCooldown.shouldSplit());
	}

	@Test
	void evaluateSnapshotFindsShardByPrefix() {
		final var heuristics = new ListShardSplitHeuristics(config);
		final var shard = shardSnapshotWith(4, 0);
		final var snapshot = new ListShardMetricsRecorder.Snapshot(
						java.util.List.of(shard),
						4L,
						100L,
						5.0d,
						1234L,
						0L,
						0,
						java.util.Collections.emptyMap(),
						0L);
		final var decision = heuristics.evaluateSnapshot(snapshot, "p", new ListShardSplitHeuristics.QueueSnapshot(0, 1));
		assertTrue(decision.shouldSplit());
		assertEquals(ListShardSplitHeuristics.DecisionReason.FULL_PAGE_STREAK, decision.reason());
	}

	private static ListShardingConfig.AdaptiveHeuristicsConfig configWith(final Map<String, Object> overrides)
					throws IllegalConfigurationException {
		return ListShardingConfig.AdaptiveHeuristicsConfig.parse(adaptiveConfig(overrides));
	}

	private static BasicConfig adaptiveConfig(final Map<String, Object> overrides) {
		final var adaptiveSchema = new java.util.HashMap<String, Object>();
		adaptiveSchema.put("full_page_threshold", Integer.class);
		adaptiveSchema.put("stall_warning_threshold", Integer.class);
		adaptiveSchema.put("backlog_ratio", Double.class);
		adaptiveSchema.put("min_pending_shards", Integer.class);
		adaptiveSchema.put("cooldown", String.class);
		final var schema = new java.util.HashMap<String, Object>();
		schema.put("adaptive", adaptiveSchema);

		final var adaptiveValues = new java.util.HashMap<String, Object>();
		adaptiveValues.put("full_page_threshold", overrides.getOrDefault("full_page_threshold", 4));
		adaptiveValues.put("stall_warning_threshold", overrides.getOrDefault("stall_warning_threshold", 2));
		adaptiveValues.put("backlog_ratio", overrides.getOrDefault("backlog_ratio", 1.5));
		adaptiveValues.put("min_pending_shards", overrides.getOrDefault("min_pending_shards", 4));
		adaptiveValues.put("cooldown", overrides.getOrDefault("cooldown", "0s"));
		final var values = new java.util.HashMap<String, Object>();
		values.put("adaptive", adaptiveValues);
		return new BasicConfig("sharding", schema, values);
	}

	private static ListShardMetricsRecorder.ShardSnapshot shardSnapshotWith(
					final int streak,
					final int stallWarnings) {
		return new ListShardMetricsRecorder.ShardSnapshot(
						"p",
						10L,
						1_000L,
						12.5d,
						1_000L,
						1_500L,
						true,
						stallWarnings,
						streak,
						1_000,
						1_000,
						1_500L,
						0,
						0,
						0L,
						"NONE",
						0,
						0,
						0L,
						0,
						"NONE");
	}
}
