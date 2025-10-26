package com.dell.spt.base.item.op.list.shard;

import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.ShardSnapshot;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder.Snapshot;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Default implementation that records per-shard metrics and exposes immutable snapshots for
 * observability layers such as /metrics/json.
 */
public final class ListShardMetricsRecorderImpl implements ListShardMetricsRecorder {

	private static final long NANOS_PER_MILLI = 1_000_000L;
	private static final double MILLIS_PER_NANO = 1.0d / NANOS_PER_MILLI;
	private static final int DEFAULT_MAX_KEYS = 1_000;

	private final ConcurrentMap<String, ShardStats> statsByPrefix = new ConcurrentHashMap<>();
	private final Clock clock;
	private final long stallThresholdMillis;
	private final long progressLogIntervalMillis;
	private final int splitLogRate;
	private final boolean summaryEnabled;
	// Delimiter-first runtime split settings
	private final int splitPages;
	private final String delimiters;

	private final AtomicLong totalObjects = new AtomicLong();
	private final AtomicLong totalPages = new AtomicLong();
	private final AtomicLong totalDurationNanos = new AtomicLong();
	private final AtomicLong totalSplits = new AtomicLong();
	private final AtomicInteger maxDepthObserved = new AtomicInteger();
	private final ConcurrentMap<String, AtomicInteger> splitReasons = new ConcurrentHashMap<>();
	private final AtomicLong totalRescues = new AtomicLong();

	public ListShardMetricsRecorderImpl(final Duration stallThreshold, final Duration progressLogInterval) {
		this(stallThreshold, progressLogInterval, 1, false, 50, "/-_.");
	}

	public ListShardMetricsRecorderImpl(
					final Duration stallThreshold,
					final Duration progressLogInterval,
					final int splitLogRate,
					final boolean summaryEnabled) {
		this(stallThreshold, progressLogInterval, splitLogRate, summaryEnabled, 50, "/-_.");
	}

	public ListShardMetricsRecorderImpl(
					final Clock clock,
					final Duration stallThreshold,
					final Duration progressLogInterval) {
		this(clock, stallThreshold, progressLogInterval, 1, false, 50, "/-_.");
	}

	public ListShardMetricsRecorderImpl(
					final Duration stallThreshold,
					final Duration progressLogInterval,
					final int splitLogRate,
					final boolean summaryEnabled,
					final int splitPages,
					final String delimiters) {
		this(Clock.systemUTC(), stallThreshold, progressLogInterval, splitLogRate, summaryEnabled, splitPages, delimiters);
	}

	public ListShardMetricsRecorderImpl(
					final Clock clock,
					final Duration stallThreshold,
					final Duration progressLogInterval,
					final int splitLogRate,
					final boolean summaryEnabled,
					final int splitPages,
					final String delimiters) {
		this.clock = Objects.requireNonNull(clock, "clock");
		this.stallThresholdMillis = Math.max(0L, stallThreshold == null ? 0L : stallThreshold.toMillis());
		this.progressLogIntervalMillis = Math.max(0L, progressLogInterval == null ? 0L : progressLogInterval.toMillis());
		this.splitLogRate = splitLogRate <= 0 ? 0 : splitLogRate;
		this.summaryEnabled = summaryEnabled;
		this.splitPages = splitPages <= 0 ? 50 : splitPages;
		this.delimiters = (delimiters == null || delimiters.isEmpty()) ? "/-_." : delimiters;
	}

	public ListShardMetricsRecorderImpl(
					final Clock clock,
					final Duration stallThreshold,
					final Duration progressLogInterval,
					final int splitLogRate,
					final boolean summaryEnabled) {
		this(clock, stallThreshold, progressLogInterval, splitLogRate, summaryEnabled, 50, "/-_.");
	}

	public int splitPages() {
		return splitPages;
	}

	public String delimiters() {
		return delimiters;
	}

	@Override
	public void onLease(final ListShard shard) {
		if (shard == null) {
			return;
		}
		final var stats = statsFor(shard);
		final long now = clock.millis();
		stats.markLease(now);
	}

	@Override
	public void onRequeue(final ListShard shard) {
		if (shard == null) {
			return;
		}
		statsFor(shard); // ensure stats exist
	}

	@Override
	public void onComplete(final ListShard shard) {
		if (shard == null) {
			return;
		}
		final var stats = statsFor(shard);
		final long now = clock.millis();
		if (stats.markComplete(now) && Loggers.MSG.isTraceEnabled()) {
			Loggers.MSG.trace(
							"LIST shard completed prefix={} pages={} objects={} avgPageMillis={}",
							shard.prefix(),
							stats.pages.get(),
							stats.objects.get(),
							formatDurationMillis(stats.avgPageMillis()));
		}
	}

	@Override
	public void onRescue(final ListShard shard) {
		if (shard == null) {
			return;
		}
		final var stats = statsFor(shard);
		final long now = clock.millis();
		final long lastUpdateAgo = now - stats.lastUpdateMillis();
		stats.recordRescue(now);
		totalRescues.incrementAndGet();
		Loggers.MSG.warn(
						"LIST shard watchdog recovered prefix={} lastUpdateMsAgo={}",
						shard.prefix(),
						lastUpdateAgo);
	}

	@Override
	public void onRetry(
					final ListShard shard, final com.dell.spt.base.item.op.Operation.Status status) {
		if (shard == null) {
			return;
		}
		final var stats = statsFor(shard);
		final long now = clock.millis();
		stats.recordRetry(status == null ? "UNKNOWN" : status.name(), now);
	}

	@Override
	public void onSplit(
					final ListShard parent,
					final ListShardSplitHeuristics.Decision decision,
					final List<ListShard> children) {
		if (parent == null) {
			return;
		}
		final var stats = statsFor(parent);
		final long now = clock.millis();
		final int maxChildDepth = children == null || children.isEmpty()
						? parent.splitDepth()
						: children.stream().mapToInt(ListShard::splitDepth).max().orElse(parent.splitDepth());
		final int childCount = children == null ? 0 : children.size();
		stats.recordSplit(decision, maxChildDepth, childCount, now, splitLogRate);
		totalSplits.incrementAndGet();
		maxDepthObserved.accumulateAndGet(maxChildDepth, Math::max);
		if (decision != null) {
			final String reason = decision.reason().name();
			splitReasons.computeIfAbsent(reason, key -> new AtomicInteger()).incrementAndGet();
		}
		if (children != null) {
			for (final ListShard child : children) {
				statsFor(child);
			}
		}
	}

	@Override
	public void onPageResult(
					final ListShard shard,
					final int objectsListed,
					final int maxKeys,
					final long durationNanos) {
		if (shard == null) {
			return;
		}
		final var stats = statsFor(shard);
		final long now = clock.millis();
		stats.recordPage(objectsListed, maxKeys, durationNanos, now);
		totalObjects.addAndGet(Math.max(0, objectsListed));
		totalPages.incrementAndGet();
		totalDurationNanos.addAndGet(Math.max(0L, durationNanos));

		maybeEmitProgressLog(shard, stats, now);
		maybeEmitStallWarnings(now);
	}

	private void maybeEmitProgressLog(final ListShard shard, final ShardStats stats, final long nowMillis) {
		if (!Loggers.MSG.isTraceEnabled() || progressLogIntervalMillis <= 0L) {
			return;
		}
		final long lastProgress = stats.lastProgressLogMillis.get();
		if (nowMillis - lastProgress >= progressLogIntervalMillis) {
			if (stats.lastProgressLogMillis.compareAndSet(lastProgress, nowMillis)) {
				Loggers.MSG.trace(
								"LIST shard progress prefix={} pages={} objects={} avgPageMillis={}",
								shard.prefix(),
								stats.pages.get(),
								stats.objects.get(),
								formatDurationMillis(stats.avgPageMillis()));
			}
		}
	}

	private void maybeEmitStallWarnings(final long nowMillis) {
		if (stallThresholdMillis <= 0L) {
			return;
		}
		for (final var entry : statsByPrefix.entrySet()) {
			final var stats = entry.getValue();
			if (!stats.active.get()) {
				continue;
			}
			final long lastUpdate = stats.lastUpdateMillis.get();
			if (nowMillis - lastUpdate < stallThresholdMillis) {
				continue;
			}
			final long lastWarning = stats.lastStallWarningMillis.get();
			if (nowMillis - lastWarning < stallThresholdMillis) {
				continue;
			}
			if (stats.lastStallWarningMillis.compareAndSet(lastWarning, nowMillis)) {
				final int warningCount = stats.stallWarnings.incrementAndGet();
				Loggers.MSG.warn(
								"LIST shard stalled prefix={} lastUpdateMsAgo={} warnings={}",
								entry.getKey(),
								nowMillis - lastUpdate,
								warningCount);
			}
		}
	}

	private ShardStats statsFor(final ListShard shard) {
		final String prefix = shard.prefix() == null ? "" : shard.prefix();
		final int depth = Math.max(0, shard.splitDepth());
		return statsByPrefix.compute(
						prefix,
						(key, existing) -> {
							if (existing == null) {
								return new ShardStats(key, depth);
							}
							existing.ensureDepthAtLeast(depth);
							return existing;
						});
	}

	@Override
	public long stallThresholdMillis() {
		return stallThresholdMillis;
	}

	@Override
	public Snapshot snapshot() {
		final Collection<ShardSnapshot> shards;
		if (statsByPrefix.isEmpty()) {
			shards = Collections.emptyList();
		} else {
			final List<ShardSnapshot> list = new ArrayList<>(statsByPrefix.size());
			for (ShardStats stats : statsByPrefix.values()) {
				list.add(stats.snapshot());
			}
			shards = list;
		}
		final long pages = totalPages.get();
		final long objects = totalObjects.get();
		final long duration = totalDurationNanos.get();
		final double avgMillis = pages > 0 ? (duration * MILLIS_PER_NANO) / pages : 0.0d;
		final long splits = totalSplits.get();
		final int maxDepth = maxDepthObserved.get();
		final Map<String, Integer> reasons;
		if (splitReasons.isEmpty()) {
			reasons = Collections.emptyMap();
		} else {
			final Map<String, Integer> copy = new LinkedHashMap<>(splitReasons.size());
			splitReasons.forEach((reason, count) -> copy.put(reason, count.get()));
			reasons = Collections.unmodifiableMap(copy);
		}
		final long rescues = totalRescues.get();
		return new Snapshot(
						shards, pages, objects, avgMillis, clock.millis(), splits, maxDepth, reasons, rescues);
	}

	@Override
	public void emitSummary(final String stepId) {
		if (!summaryEnabled) {
			return;
		}
		final Snapshot snapshot = snapshot();
		final long splits = snapshot.totalSplits();
		final int shardCount = snapshot.shards().size();
		if (splits == 0L) {
			Loggers.MSG.info("{}: LIST sharding summary: no adaptive splits ({} shards observed)", stepId, shardCount);
			return;
		}
		final int maxDepth = snapshot.maxDepth();
		Loggers.MSG.info(
						"{}: LIST sharding summary: total_splits={} shards={} max_depth={}",
						stepId,
						splits,
						shardCount,
						maxDepth);
		if (!snapshot.splitReasons().isEmpty()) {
			final String reasonSummary = snapshot.splitReasons().entrySet().stream()
							.sorted(Map.Entry.<String, Integer> comparingByValue().reversed())
							.map(entry -> entry.getKey().toLowerCase(Locale.ROOT) + '=' + entry.getValue())
							.collect(Collectors.joining(", "));
			Loggers.MSG.info("{}: LIST split reasons: {}", stepId, reasonSummary);
		}
		final List<ShardSnapshot> hotShards = snapshot.shards().stream()
						.sorted(
										Comparator.comparingInt(ShardSnapshot::splitCount)
														.reversed()
														.thenComparing(ShardSnapshot::prefix))
						.limit(5)
						.toList();
		if (!hotShards.isEmpty()) {
			final String summary = hotShards.stream()
							.map(shard -> shard.prefix()
											+ "(splits="
											+ shard.splitCount()
											+ ", depth="
											+ shard.maxDepth()
											+ ")")
							.collect(Collectors.joining(", "));
			Loggers.MSG.info("{}: LIST split hot shards: {}", stepId, summary);
		}
	}

	private static String formatDurationMillis(final double value) {
		return String.format("%.3f", value);
	}

	private static final class ShardStats {

		private final String prefix;
		private final AtomicLong pages = new AtomicLong();
		private final AtomicLong objects = new AtomicLong();
		private final AtomicLong totalDurationNanos = new AtomicLong();
		private final AtomicLong firstUpdateMillis = new AtomicLong();
		private final AtomicLong lastUpdateMillis = new AtomicLong();
		private final AtomicBoolean active = new AtomicBoolean();
		private final AtomicLong lastProgressLogMillis = new AtomicLong();
		private final AtomicLong lastStallWarningMillis = new AtomicLong();
		private final AtomicInteger stallWarnings = new AtomicInteger();
		private final AtomicInteger consecutiveFullPages = new AtomicInteger();
		private final AtomicInteger lastPageObjects = new AtomicInteger();
		private final AtomicInteger lastPageMaxKeys = new AtomicInteger();
		private final AtomicLong lastFullPageMillis = new AtomicLong();
		private final AtomicInteger splitCount = new AtomicInteger();
		private final AtomicInteger maxDepth = new AtomicInteger();
		private final AtomicLong lastSplitMillis = new AtomicLong();
		private final AtomicReference<String> lastSplitReason = new AtomicReference<>("NONE");
		private final AtomicInteger lastSplitChildren = new AtomicInteger();
		private final AtomicInteger splitLogCounter = new AtomicInteger();
		private final AtomicInteger rescueCount = new AtomicInteger();
		private final AtomicLong lastRescueMillis = new AtomicLong();
		private final AtomicInteger retryCount = new AtomicInteger();
		private final AtomicReference<String> lastRetryStatus = new AtomicReference<>("NONE");

		private ShardStats(final String prefix, final int initialDepth) {
			this.prefix = prefix;
			this.maxDepth.set(Math.max(0, initialDepth));
		}

		private void markLease(final long nowMillis) {
			active.set(true);
			lastUpdateMillis.set(nowMillis);
			if (firstUpdateMillis.compareAndSet(0L, nowMillis) && Loggers.MSG.isTraceEnabled()) {
				Loggers.MSG.trace("LIST shard lease acquired prefix={}", prefix);
			}
		}

		private void recordPage(
						final int objectsListed,
						final int maxKeys,
						final long durationNanos,
						final long nowMillis) {
			active.set(true);
			if (firstUpdateMillis.compareAndSet(0L, nowMillis)) {
				if (Loggers.MSG.isTraceEnabled()) {
					Loggers.MSG.trace("LIST shard started prefix={}", prefix);
				}
				lastProgressLogMillis.compareAndSet(0L, nowMillis);
			}
			pages.incrementAndGet();
			objects.addAndGet(Math.max(0, objectsListed));
			totalDurationNanos.addAndGet(Math.max(0L, durationNanos));
			lastUpdateMillis.set(nowMillis);
			lastPageObjects.set(Math.max(0, objectsListed));
			final int resolvedMaxKeys = maxKeys > 0 ? maxKeys : DEFAULT_MAX_KEYS;
			lastPageMaxKeys.set(resolvedMaxKeys);
			if (objectsListed >= resolvedMaxKeys) {
				consecutiveFullPages.updateAndGet(prev -> prev + 1);
				lastFullPageMillis.set(nowMillis);
			} else {
				consecutiveFullPages.set(0);
			}
		}

		private void recordSplit(
						final ListShardSplitHeuristics.Decision decision,
						final int childMaxDepth,
						final int childCount,
						final long nowMillis,
						final int logRate) {
			splitCount.incrementAndGet();
			ensureDepthAtLeast(childMaxDepth);
			lastSplitMillis.set(nowMillis);
			lastSplitChildren.set(Math.max(0, childCount));
			final String reason = decision == null ? "NONE" : decision.reason().name();
			lastSplitReason.set(reason);
			if (logRate > 0 && Loggers.MSG.isTraceEnabled()) {
				final int next = splitLogCounter.incrementAndGet();
				if (next >= logRate) {
					splitLogCounter.addAndGet(-logRate);
					Loggers.MSG.trace(
									"LIST shard split prefix={} reason={} children={} maxDepth={}",
									prefix,
									reason,
									childCount,
									maxDepth.get());
				}
			}
		}

		private void ensureDepthAtLeast(final int depth) {
			maxDepth.accumulateAndGet(Math.max(0, depth), Math::max);
		}

		private long lastUpdateMillis() {
			return lastUpdateMillis.get();
		}

		private void recordRescue(final long nowMillis) {
			active.set(true);
			lastUpdateMillis.set(nowMillis);
			lastRescueMillis.set(nowMillis);
			rescueCount.incrementAndGet();
		}

		private void recordRetry(final String status, final long nowMillis) {
			active.set(true);
			lastUpdateMillis.set(nowMillis);
			lastRetryStatus.set(status == null ? "UNKNOWN" : status);
			final int attempts = retryCount.incrementAndGet();
			if (attempts % 3 == 0) {
				Loggers.MSG.warn(
								"LIST shard retry prefix={} status={} attempts={}",
								prefix,
								lastRetryStatus.get(),
								attempts);
			} else if (Loggers.MSG.isDebugEnabled()) {
				Loggers.MSG.debug(
								"LIST shard retry prefix={} status={} attempts={}",
								prefix,
								lastRetryStatus.get(),
								attempts);
			}
		}

		private boolean markComplete(final long nowMillis) {
			if (active.compareAndSet(true, false)) {
				lastUpdateMillis.set(nowMillis);
				return true;
			}
			return false;
		}

		private double avgPageMillis() {
			final long pageCount = pages.get();
			if (pageCount == 0L) {
				return 0.0d;
			}
			return (totalDurationNanos.get() * MILLIS_PER_NANO) / pageCount;
		}

		private ShardSnapshot snapshot() {
			return new ShardSnapshot(
							prefix,
							pages.get(),
							objects.get(),
							avgPageMillis(),
							firstUpdateMillis.get(),
							lastUpdateMillis.get(),
							active.get(),
							stallWarnings.get(),
							consecutiveFullPages.get(),
							lastPageObjects.get(),
							lastPageMaxKeys.get(),
							lastFullPageMillis.get(),
							splitCount.get(),
							maxDepth.get(),
							lastSplitMillis.get(),
							lastSplitReason.get(),
							lastSplitChildren.get(),
							rescueCount.get(),
							lastRescueMillis.get(),
							retryCount.get(),
							lastRetryStatus.get());
		}
	}
}
