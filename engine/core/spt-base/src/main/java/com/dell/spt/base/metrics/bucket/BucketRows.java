package com.dell.spt.base.metrics.bucket;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.dell.spt.base.metrics.steady.SteadySettings;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Builds the rows of one operation type from its contributors' records, one row at a time, so an
 * entry node never holds every merged histogram. A local step is the one-contributor case.
 */
public final class BucketRows {

	/** One worker's answer, or why it has none. */
	public record Contribution(String node, BucketMetricsReply reply, String failure) {}

	/** Which contributors a row of one operation type merges, and why the others are missing. */
	public record Cohort(
					OpType opType,
					List<String> expected,
					List<String> received,
					Map<String, String> missing,
					BucketData wholeStepData,
					BucketData steadyData) {}

	/** One row of a bucket file. */
	public record Row(
					BucketKind kind,
					int memberIndex,
					String bucket,
					OpType opType,
					int nodesExpected,
					int nodesReceived,
					BucketData data,
					long succeeded,
					long failed,
					long corrupt,
					long bytes,
					double stepSeconds,
					TimingMetricSnapshot duration,
					TimingMetricSnapshot latency,
					TimingMetricSnapshot timeToFirstByte,
					Steady steady) {}

	/** A steady row's window columns; the row has no statistics unless its status has them. */
	public record Steady(
					Double startSeconds,
					Double turnovers,
					Double trendPercent,
					Double cv,
					String status,
					boolean hasStatistics,
					Double startDelayMillis,
					Double endDelayMillis,
					Double passMillis) {}

	private static final String STATUS_VALID = "valid";
	private static final String STATUS_SHORT = "short";
	private static final String STATUS_NOT_REACHED = "not-reached";
	private static final String STATUS_INCOMPLETE = "incomplete";
	private static final double NANOS_PER_MILLI = TimeUnit.MILLISECONDS.toNanos(1);
	private static final double NANOS_PER_SECOND = TimeUnit.SECONDS.toNanos(1);

	private BucketRows() {}

	private static final class Usable {
		final List<BucketContextReply> replies = new ArrayList<>();
		final List<String> received = new ArrayList<>();
		final Map<String, String> missing = new LinkedHashMap<>();
		List<String> members;
	}

	/** Validates each contributor for one operation type, which can differ between types. */
	private static Usable usable(final String stepId, final OpType opType, final List<Contribution> contributions) {
		final Usable usable = new Usable();
		final List<String> instances = new ArrayList<>();
		for (final Contribution contribution : contributions) {
			final String node = contribution.node();
			final BucketMetricsReply reply = contribution.reply();
			if (reply == null) {
				usable.missing.put(node, contribution.failure() == null ? "unsupported call" : contribution.failure());
				continue;
			}
			if (!Objects.equals(stepId, reply.stepId())) {
				usable.missing.put(node, "mismatched step");
				continue;
			}
			if (instances.contains(reply.instanceId())) {
				usable.missing.put(node, "duplicate");
				continue;
			}
			final BucketContextReply context = reply.contexts().stream()
							.filter(candidate -> candidate.opType() == opType)
							.findFirst()
							.orElse(null);
			if (context == null) {
				usable.missing.put(node, "mismatched type");
				continue;
			}
			if (context.wholeStep().size() != context.members().size() + 1) {
				usable.missing.put(node, "unusable payload for this type");
				continue;
			}
			if (usable.members != null && !usable.members.equals(context.members())) {
				usable.missing.put(node, "mismatched set");
				continue;
			}
			usable.members = context.members();
			usable.replies.add(context);
			usable.received.add(node);
			instances.add(reply.instanceId());
		}
		return usable;
	}

	/**
	 * Builds one operation type's whole-step rows.
	 *
	 * @param stepSeconds the step's elapsed time, which whole-step rates use as the totals do
	 * @param sink receives each row as it is built
	 * @return the contributors the rows merge
	 */
	public static Cohort wholeStep(
					final String stepId,
					final OpType opType,
					final List<Contribution> contributions,
					final double stepSeconds,
					final Consumer<Row> sink) {
		final Usable usable = usable(stepId, opType, contributions);
		final int expected = contributions.size();
		final BucketData data = usable.missing.isEmpty() && !usable.replies.isEmpty()
						? BucketData.COMPLETE
						: BucketData.INCOMPLETE;
		if (!usable.replies.isEmpty()) {
			final int slots = usable.members.size() + 1;
			for (int slot = 0; slot < slots; slot++) {
				final List<BucketTerminalRecord> records = new ArrayList<>(usable.replies.size());
				for (final BucketContextReply reply : usable.replies) {
					records.add(reply.wholeStep().get(slot));
				}
				sink.accept(row(
								slot, usable.members, opType, expected, usable.replies.size(), data, records, stepSeconds, null));
			}
		}
		return cohort(opType, contributions, usable, data, data);
	}

	/**
	 * Builds one operation type's steady rows.
	 *
	 * @param aggregate the aggregate's merged window, whose turnovers, trend and CV the rows repeat
	 * @param settings what makes a window {@code valid}
	 * @param sink receives each row as it is built
	 * @return the contributors the rows merge
	 */
	public static Cohort steady(
					final String stepId,
					final OpType opType,
					final List<Contribution> contributions,
					final SteadyResult aggregate,
					final SteadySettings settings,
					final Consumer<Row> sink) {
		final Usable usable = usable(stepId, opType, contributions);
		final int expected = contributions.size();
		final List<BucketContextReply> measuring = new ArrayList<>(usable.replies.size());
		boolean unmeasured = false;
		boolean notReached = aggregate != null && !aggregate.status().hasStatistics()
						&& STATUS_NOT_REACHED.equals(aggregate.status().label());
		String unmeasuredReason = null;
		for (final BucketContextReply reply : usable.replies) {
			final BucketSteadyOutcome outcome = reply.steady();
			if (outcome == null) {
				unmeasured = true;
				unmeasuredReason = "a contributor measured no window";
			} else if (outcome.kind() == BucketSteadyOutcome.Kind.UNMEASURED) {
				unmeasured = true;
				unmeasuredReason = outcome.reason();
			} else if (outcome.kind() == BucketSteadyOutcome.Kind.NOT_REACHED) {
				notReached = true;
			} else if (outcome.records().size() == reply.members().size() + 1) {
				measuring.add(reply);
			} else {
				unmeasured = true;
				unmeasuredReason = "an unusable steady payload";
			}
		}
		final BucketData data;
		if (!usable.missing.isEmpty() || usable.replies.isEmpty()) {
			data = BucketData.INCOMPLETE;
		} else if (unmeasured) {
			data = BucketData.UNMEASURED;
		} else {
			data = BucketData.COMPLETE;
		}
		if (!usable.replies.isEmpty()) {
			final int slots = usable.members.size() + 1;
			final boolean statistics = data == BucketData.COMPLETE && !notReached;
			for (int slot = 0; slot < slots; slot++) {
				final List<BucketTerminalRecord> records = new ArrayList<>(measuring.size());
				long shortestWindow = Long.MAX_VALUE;
				long startDelay = 0;
				long endDelay = 0;
				long pass = 0;
				if (statistics) {
					for (final BucketContextReply reply : measuring) {
						final BucketSteadyRecord record = reply.steady().records().get(slot);
						records.add(record.values());
						shortestWindow = Math.min(shortestWindow, record.windowNanos());
						startDelay = Math.max(startDelay, record.startDelayNanos());
						endDelay = Math.max(endDelay, record.endDelayNanos());
						pass = Math.max(pass, record.passNanos());
					}
				}
				final double windowSeconds = statistics ? shortestWindow / NANOS_PER_SECOND : 0.0;
				final Row merged = row(
								slot, usable.members, opType, expected, usable.replies.size(), data, records, windowSeconds, null);
				final String status;
				if (data != BucketData.COMPLETE) {
					status = STATUS_INCOMPLETE;
				} else if (notReached) {
					status = STATUS_NOT_REACHED;
				} else {
					status = windowStatus(aggregate, settings, shortestWindow, merged.succeeded() + merged.failed());
				}
				sink.accept(new Row(
								merged.kind(),
								merged.memberIndex(),
								merged.bucket(),
								merged.opType(),
								merged.nodesExpected(),
								merged.nodesReceived(),
								merged.data(),
								merged.succeeded(),
								merged.failed(),
								merged.corrupt(),
								merged.bytes(),
								merged.stepSeconds(),
								merged.duration(),
								merged.latency(),
								merged.timeToFirstByte(),
								new Steady(
												statistics ? startSeconds(aggregate, startDelay) : null,
												aggregate == null || !statistics ? null : aggregate.turnovers(),
												aggregate == null || !statistics ? null : aggregate.trendPercent(),
												aggregate == null || !statistics ? null : aggregate.cv(),
												status,
												statistics,
												statistics ? startDelay / NANOS_PER_MILLI : null,
												statistics ? endDelay / NANOS_PER_MILLI : null,
												statistics ? pass / NANOS_PER_MILLI : null)));
			}
		}
		final Cohort cohort = cohort(opType, contributions, usable, data, data);
		if (unmeasuredReason != null) {
			cohort.missing().putIfAbsent("(window)", unmeasuredReason);
		}
		return cohort;
	}

	private static Double startSeconds(final SteadyResult aggregate, final long startDelayNanos) {
		if (aggregate == null || aggregate.startOffsetMillis() == SteadyResult.NONE) {
			return null;
		}
		return aggregate.startOffsetMillis() / 1e3 + startDelayNanos / NANOS_PER_SECOND;
	}

	/** The aggregate's rule with its turnovers inherited, on this row's own window and completions. */
	private static String windowStatus(
					final SteadyResult aggregate,
					final SteadySettings settings,
					final long windowNanos,
					final long completions) {
		final boolean valid = aggregate != null
						&& completions > 0
						&& windowNanos > 0
						&& (settings == null
										|| (aggregate.turnovers() >= settings.minTurnovers()
														&& windowNanos >= TimeUnit.MILLISECONDS.toNanos(settings.minDurationMillis())));
		return valid ? STATUS_VALID : STATUS_SHORT;
	}

	private static Row row(
					final int slot,
					final List<String> members,
					final OpType opType,
					final int expected,
					final int received,
					final BucketData data,
					final List<BucketTerminalRecord> records,
					final double seconds,
					final Steady steady) {
		long succeeded = 0;
		long failed = 0;
		long corrupt = 0;
		long bytes = 0;
		final List<TimingMetricSnapshot> durations = new ArrayList<>(records.size());
		final List<TimingMetricSnapshot> latencies = new ArrayList<>(records.size());
		final List<TimingMetricSnapshot> ttfbs = new ArrayList<>(records.size());
		for (final BucketTerminalRecord record : records) {
			succeeded += record.succeeded();
			failed += record.failed();
			corrupt += record.corrupt();
			bytes += record.bytes();
			durations.add(record.duration());
			latencies.add(record.latency());
			ttfbs.add(record.timeToFirstByte());
		}
		final boolean member = slot < members.size();
		return new Row(
						member ? BucketKind.MEMBER : BucketKind.CATCH_ALL,
						member ? slot : -1,
						member ? members.get(slot) : null,
						opType,
						expected,
						received,
						data,
						succeeded,
						failed,
						corrupt,
						bytes,
						seconds,
						TimingMetricSnapshotImpl.aggregate(durations, MetricsConstants.METRIC_NAME_DUR),
						TimingMetricSnapshotImpl.aggregate(latencies, MetricsConstants.METRIC_NAME_LAT),
						TimingMetricSnapshotImpl.aggregate(ttfbs, MetricsConstants.METRIC_NAME_TTFB),
						steady);
	}

	private static Cohort cohort(
					final OpType opType,
					final List<Contribution> contributions,
					final Usable usable,
					final BucketData wholeStep,
					final BucketData steady) {
		return new Cohort(
						opType,
						contributions.stream().map(Contribution::node).toList(),
						List.copyOf(usable.received),
						new LinkedHashMap<>(usable.missing),
						wholeStep,
						steady);
	}
}
