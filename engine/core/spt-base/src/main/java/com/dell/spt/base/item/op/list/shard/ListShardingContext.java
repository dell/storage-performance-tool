package com.dell.spt.base.item.op.list.shard;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ListShardingContext {

	private final List<ListShard> shards;
	private final Map<String, ListShard> shardByPrefix;
	private final ListShardMetricsRecorder metricsRecorder;
	private final ListShardingConfig.AdaptiveHeuristicsConfig adaptiveConfig;

	public ListShardingContext(final List<ListShard> shards) {
		this(shards, ListShardMetricsRecorder.NO_OP, ListShardingConfig.AdaptiveHeuristicsConfig.defaults());
	}

	public ListShardingContext(
					final List<ListShard> shards, final ListShardMetricsRecorder metricsRecorder) {
		this(shards, metricsRecorder, ListShardingConfig.AdaptiveHeuristicsConfig.defaults());
	}

	public ListShardingContext(
					final List<ListShard> shards,
					final ListShardMetricsRecorder metricsRecorder,
					final ListShardingConfig.AdaptiveHeuristicsConfig adaptiveConfig) {
		this.shards = List.copyOf(shards);
		final var map = new HashMap<String, ListShard>(shards.size());
		for (final var shard : shards) {
			map.put(shard.prefix(), shard);
		}
		this.shardByPrefix = Collections.unmodifiableMap(map);
		this.metricsRecorder = metricsRecorder == null ? ListShardMetricsRecorder.NO_OP : metricsRecorder;
		this.adaptiveConfig = adaptiveConfig == null
						? ListShardingConfig.AdaptiveHeuristicsConfig.defaults()
						: adaptiveConfig;
	}

	public List<ListShard> shards() {
		return shards;
	}

	public ListShard shardForPrefix(final String prefix) {
		return shardByPrefix.get(prefix);
	}

	public ListShardMetricsRecorder metricsRecorder() {
		return metricsRecorder;
	}

	public ListShardingConfig.AdaptiveHeuristicsConfig adaptiveConfig() {
		return adaptiveConfig;
	}
}
