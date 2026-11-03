package com.dell.spt.base.logging;

import static com.dell.spt.base.Constants.M;
import static com.dell.spt.base.Constants.MIB;
import static com.dell.spt.base.Constants.COLUMN_BW_AVG_MIB_PER_SECOND;

import com.dell.spt.base.env.DateUtil;
import com.dell.spt.base.metrics.bucket.BucketKind;
import com.dell.spt.base.metrics.bucket.BucketRows;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshotImpl;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The rows of {@code metrics.bucket.total.csv} and {@code metrics.bucket.steady.total.csv}. Against
 * the total file, a row is identified by its kind, member index and operation type, has no
 * concurrency or last-interval columns, and states its contributors in place of a node count.
 *
 * <p>In a steady row {@code SteadyTurnovers}, {@code SteadyTrend[%]} and {@code SteadyCV} are the
 * aggregate's values repeated, because turnover needs an in-flight mean that bucket rows lack.
 */
public final class MetricsBucketCsvLogMessage {

	/** The window columns of the steady file, which follow the columns of both files. */
	public static final String STEADY_COLUMNS = "SteadyStart[s],SteadyTurnovers,SteadyTrend[%],SteadyCV,SteadyStatus,StartDelay[ms],EndDelay[ms],PassDuration[ms]";

	private static final int SCALAR_COLUMNS = 8;
	private static final int TIMING_FIXED_COLUMNS = 3;

	private MetricsBucketCsvLogMessage() {}

	/** The header of a bucket file, with a column per quantile in the order the totals use. */
	public static String header(final List<Double> quantiles, final boolean steady) {
		final StringBuilder header = new StringBuilder(
						"DateTimeISO8601,BucketKind,MemberIndex,Bucket,OpType,NodesExpected,NodesReceived,BucketData,"
										+ "CountSucc,CountFail,CountCorrupt,Size,StepDuration[s],DurationSum[s],TPAvg[op/s],")
						.append(COLUMN_BW_AVG_MIB_PER_SECOND);
		appendTimingHeader(header, "Duration", quantiles);
		appendTimingHeader(header, "Latency", quantiles);
		appendTimingHeader(header, "Ttfb", quantiles);
		if (steady) {
			header.append(',').append(STEADY_COLUMNS);
		}
		return header.toString();
	}

	private static void appendTimingHeader(
					final StringBuilder header, final String name, final List<Double> quantiles) {
		header.append(',').append(name).append("Avg[us],").append(name).append("Min[us],");
		for (final Double quantile : quantiles) {
			header.append(name).append("Q_").append(quantile).append("[us],");
		}
		header.append(name).append("Max[us]");
	}

	/** One row. A steady row without statistics leaves every statistic empty. */
	public static String row(final BucketRows.Row row, final List<Double> quantiles) {
		final List<String> cells = new ArrayList<>();
		cells.add('"' + DateUtil.formatIso8601(Instant.now()) + '"');
		final boolean member = row.kind() == BucketKind.MEMBER;
		cells.add(member ? "member" : "catch-all");
		cells.add(member ? String.valueOf(row.memberIndex()) : "");
		cells.add(member ? csvCell(row.bucket()) : "");
		cells.add(row.opType().name());
		cells.add(String.valueOf(row.nodesExpected()));
		cells.add(String.valueOf(row.nodesReceived()));
		cells.add(row.data().label());
		final BucketRows.Steady steady = row.steady();
		if (steady != null && !steady.hasStatistics()) {
			empty(cells, SCALAR_COLUMNS + 3 * (TIMING_FIXED_COLUMNS + quantiles.size()));
		} else {
			cells.add(String.valueOf(row.succeeded()));
			cells.add(String.valueOf(row.failed()));
			cells.add(String.valueOf(row.corrupt()));
			cells.add(String.valueOf(row.bytes()));
			cells.add(String.valueOf(row.stepSeconds()));
			cells.add(observed(row.duration()) ? String.valueOf(row.duration().sum() / M) : "");
			cells.add(String.valueOf(row.succeededPerSecond()));
			cells.add(String.valueOf(row.bytesPerSecond() / MIB));
			timing(cells, row.duration(), quantiles);
			timing(cells, row.latency(), quantiles);
			timing(cells, row.timeToFirstByte(), quantiles);
		}
		if (steady != null) {
			cells.add(text(steady.startSeconds()));
			cells.add(text(steady.turnovers()));
			cells.add(text(steady.trendPercent()));
			cells.add(text(steady.cv()));
			cells.add(steady.status());
			cells.add(text(steady.startDelayMillis()));
			cells.add(text(steady.endDelayMillis()));
			cells.add(text(steady.passMillis()));
		}
		return String.join(",", cells);
	}

	/** A listed bucket name can hold a quote, which a CSV reader would otherwise strip. */
	private static String csvCell(final String value) {
		if (value.indexOf('"') < 0 && value.indexOf(',') < 0) {
			return value;
		}
		return '"' + value.replace("\"", "\"\"") + '"';
	}

	private static String text(final Double value) {
		return value == null ? "" : String.valueOf(value);
	}

	private static boolean observed(final TimingMetricSnapshot timing) {
		return timing != null && timing.count() > 0;
	}

	private static void timing(
					final List<String> cells, final TimingMetricSnapshot timing, final List<Double> quantiles) {
		if (!observed(timing)) {
			empty(cells, TIMING_FIXED_COLUMNS + quantiles.size());
			return;
		}
		cells.add(String.valueOf(timing.mean()));
		cells.add(String.valueOf(timing.min()));
		for (final long value : TimingMetricSnapshotImpl.percentiles(timing, quantiles)) {
			cells.add(String.valueOf(value));
		}
		cells.add(String.valueOf(timing.max()));
	}

	private static void empty(final List<String> cells, final int count) {
		for (int i = 0; i < count; i++) {
			cells.add("");
		}
	}
}
