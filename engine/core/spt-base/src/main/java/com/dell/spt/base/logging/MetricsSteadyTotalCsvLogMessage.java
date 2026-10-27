package com.dell.spt.base.logging;

import static com.dell.spt.base.Constants.K;
import static com.dell.spt.base.Constants.M;
import static com.dell.spt.base.Constants.MIB;

import com.dell.spt.base.env.DateUtil;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.steady.SteadyResult;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.message.AsynchronouslyFormattable;

/**
 * One row of {@code metrics.steady.total.csv}: the total file's columns, in the same order,
 * computed over the Steady-State Window, followed by the window columns.
 *
 * <p>Identity columns and {@code SteadyStatus} are always filled. A {@code not-reached} or
 * {@code incomplete} row leaves every statistic empty, and a timing meter without observations
 * in the window leaves its cells empty, so consumers check {@code SteadyStatus} first.
 */
@AsynchronouslyFormattable
public class MetricsSteadyTotalCsvLogMessage extends LogMessageBase {

	/** The window columns that follow the total file's columns. */
	public static final String STEADY_COLUMNS = "SteadyStart[s],SteadyTurnovers,SteadyTrend[%],SteadyCV,SteadyStatus";

	private final SteadyResult steady;
	private final OpType opType;
	private final int concurrencyLimit;
	private final int nodeCount;
	private final Map<Double, Long> latencies;
	private final Map<Double, Long> durations;
	private final Map<Double, Long> ttfbs;

	/**
	 * @param latencyQuantiles the window's latency quantiles; its keys name the quantile columns
	 * @param durationQuantiles the window's duration quantiles, keyed like the latency ones
	 * @param ttfbQuantiles the window's TTFB quantiles, keyed like the latency ones
	 */
	public MetricsSteadyTotalCsvLogMessage(
					final SteadyResult steady,
					final OpType opType,
					final int concurrencyLimit,
					final int nodeCount,
					final Map<Double, Long> latencyQuantiles,
					final Map<Double, Long> durationQuantiles,
					final Map<Double, Long> ttfbQuantiles) {
		this.steady = steady;
		this.opType = opType;
		this.concurrencyLimit = concurrencyLimit;
		this.nodeCount = nodeCount;
		this.latencies = latencyQuantiles;
		this.durations = durationQuantiles;
		this.ttfbs = ttfbQuantiles;
	}

	@Override
	public final void formatTo(final StringBuilder strb) {
		formatTo(strb, true);
	}

	public final void formatTo(final StringBuilder strb, final boolean includeHeader) {
		if (includeHeader) {
			MetricsTotalCsvLogMessage.appendHeader(strb, durations, latencies, ttfbs);
			strb.append(',').append(STEADY_COLUMNS).append(System.lineSeparator());
		}
		strb.append(String.join(",", valueRow()));
	}

	private List<String> valueRow() {
		final List<String> cells = new ArrayList<>();
		cells.add('"' + DateUtil.formatIso8601(Instant.now()) + '"');
		cells.add(opType.name());
		cells.add(String.valueOf(concurrencyLimit));
		cells.add(String.valueOf(nodeCount));
		final AllMetricsSnapshot window = steady.statistics();
		if (window == null) {
			empty(cells, 12 + 3 * 3 + durations.size() + latencies.size() + ttfbs.size());
		} else {
			final RateMetricSnapshot succeeded = window.successSnapshot();
			final RateMetricSnapshot bytes = window.byteSnapshot();
			final RateMetricSnapshot corrupt = window.corruptSnapshot();
			final TimingMetricSnapshot duration = window.durationSnapshot();
			cells.add(String.valueOf(window.concurrencySnapshot().last()));
			cells.add(String.valueOf(window.concurrencySnapshot().mean()));
			cells.add(String.valueOf(succeeded.count()));
			cells.add(String.valueOf(window.failsSnapshot().count()));
			cells.add(String.valueOf(corrupt == null ? 0L : corrupt.count()));
			cells.add(String.valueOf(bytes.count()));
			cells.add(String.valueOf(window.elapsedTimeMillis() / K));
			cells.add(observed(duration) ? String.valueOf(duration.sum() / M) : "");
			cells.add(String.valueOf(succeeded.mean()));
			cells.add(String.valueOf(succeeded.last()));
			cells.add(String.valueOf(bytes.mean() / MIB));
			cells.add(String.valueOf(bytes.last() / MIB));
			timing(cells, duration, durations);
			timing(cells, window.latencySnapshot(), latencies);
			timing(cells, window.ttfbSnapshot(), ttfbs);
		}
		cells.add(steady.startOffsetMillis() == SteadyResult.NONE ? "" : String.valueOf(steady.startOffsetMillis() / K));
		cells.add(window == null ? "" : String.valueOf(steady.turnovers()));
		cells.add(steady.trendPercent() == null ? "" : String.valueOf(steady.trendPercent()));
		cells.add(steady.cv() == null ? "" : String.valueOf(steady.cv()));
		cells.add(steady.status().label());
		return cells;
	}

	private static boolean observed(final TimingMetricSnapshot timing) {
		return timing != null && timing.count() > 0;
	}

	/** Average, minimum, quantiles and maximum, or empty cells when the window observed none. */
	private static void timing(
					final List<String> cells, final TimingMetricSnapshot timing, final Map<Double, Long> quantiles) {
		if (!observed(timing)) {
			empty(cells, 3 + quantiles.size());
			return;
		}
		cells.add(String.valueOf(timing.mean()));
		cells.add(String.valueOf(timing.min()));
		for (final Long value : quantiles.values()) {
			cells.add(value == null ? "" : String.valueOf(value));
		}
		cells.add(String.valueOf(timing.max()));
	}

	private static void empty(final List<String> cells, final int count) {
		for (int i = 0; i < count; i++) {
			cells.add("");
		}
	}
}
