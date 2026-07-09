package com.dell.spt.base.logging;

import com.dell.spt.base.env.DateUtil;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import org.apache.logging.log4j.message.AsynchronouslyFormattable;

import java.util.Map;
import java.time.Instant;

import static com.dell.spt.base.Constants.COLUMN_BW_AVG_MIB_PER_SECOND;
import static com.dell.spt.base.Constants.COLUMN_BW_LAST_MIB_PER_SECOND;
import static com.dell.spt.base.Constants.K;
import static com.dell.spt.base.Constants.M;
import static com.dell.spt.base.Constants.MIB;

// metricsTotalCsv unlike metricsCsv also output the quantile values for the timing metrics
// it is only used at the end of each test step and only in DistributedMetricsContext
@AsynchronouslyFormattable
public class MetricsTotalCsvLogMessage extends LogMessageBase {

	private final AllMetricsSnapshot snapshot;
	private final OpType opType;
	private final int concurrencyLimit;
	private final Map<Double, Long> latencies;
	private final Map<Double, Long> durations;
	private final Map<Double, Long> ttfbs;

	public MetricsTotalCsvLogMessage(
					final AllMetricsSnapshot snapshot, final OpType opType, final int concurrencyLimit,
					final Map<Double, Long> latencyQuantiles, final Map<Double, Long> durationQuantiles) {
		this(snapshot, opType, concurrencyLimit, latencyQuantiles, durationQuantiles, Map.of());
	}

	public MetricsTotalCsvLogMessage(
					final AllMetricsSnapshot snapshot, final OpType opType, final int concurrencyLimit,
					final Map<Double, Long> latencyQuantiles, final Map<Double, Long> durationQuantiles,
					final Map<Double, Long> ttfbQuantiles) {
		this.snapshot = snapshot;
		this.opType = opType;
		this.concurrencyLimit = concurrencyLimit;
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
			appendHeader(strb);
			strb.append(System.lineSeparator());
		}
		appendValueRow(strb);
	}

	private void appendHeader(final StringBuilder strb) {
		strb.append("DateTimeISO8601,OpType,Concurrency,NodeCount,ConcurrencyCurr,ConcurrencyMean,CountSucc,")
						.append("CountFail,Size,StepDuration[s],DurationSum[s],TPAvg[op/s],TPLast[op/s],")
						.append(COLUMN_BW_AVG_MIB_PER_SECOND)
						.append(',')
						.append(COLUMN_BW_LAST_MIB_PER_SECOND)
						.append(",DurationAvg[us],DurationMin[us],");

		for (Double quantile : durations.keySet()) {
			strb.append("DurationQ_")
							.append(quantile)
							.append("[us],");
		}
		strb.append("DurationMax[us],LatencyAvg[us],LatencyMin[us],");

		for (Double quantile : latencies.keySet()) {
			strb.append("LatencyQ_")
							.append(quantile)
							.append("[us],");
		}
		strb.append("LatencyMax[us],TtfbAvg[us],TtfbMin[us],");
		for (Double quantile : ttfbs.keySet()) {
			strb.append("TtfbQ_")
							.append(quantile)
							.append("[us],");
		}
		strb.append("TtfbMax[us]");
	}

	private void appendValueRow(final StringBuilder strb) {
		final ConcurrencyMetricSnapshot concurrencySnapshot = snapshot.concurrencySnapshot();
		final TimingMetricSnapshot durationSnapshot = snapshot.durationSnapshot();
		final RateMetricSnapshot successCountSnapshot = snapshot.successSnapshot();
		final RateMetricSnapshot byteCountSnapshot = snapshot.byteSnapshot();
		final TimingMetricSnapshot latencySnapshot = snapshot.latencySnapshot();
		final TimingMetricSnapshot ttfbSnapshot = snapshot.ttfbSnapshot();
		final boolean ttfbAvailable = ttfbSnapshot != null && ttfbSnapshot.count() > 0;

		strb.append('"')
						.append(DateUtil.formatIso8601(Instant.now()))
						.append('"')
						.append(',')
						.append(opType.name())
						.append(',')
						.append(concurrencyLimit)
						.append(',')
						.append(((DistributedAllMetricsSnapshot) snapshot).nodeCount())
						.append(',')
						.append(concurrencySnapshot.last())
						.append(',')
						.append(concurrencySnapshot.mean())
						.append(',')
						.append(successCountSnapshot.count())
						.append(',')
						.append(snapshot.failsSnapshot().count())
						.append(',')
						.append(byteCountSnapshot.count())
						.append(',')
						.append(snapshot.elapsedTimeMillis() / K)
						.append(',')
						.append(durationSnapshot.sum() / M)
						.append(',')
						.append(successCountSnapshot.mean())
						.append(',')
						.append(successCountSnapshot.last())
						.append(',')
						.append(byteCountSnapshot.mean() / MIB)
						.append(',')
						.append(byteCountSnapshot.last() / MIB)
						.append(',')
						.append(durationSnapshot.mean())
						.append(',')
						.append(durationSnapshot.min())
						.append(',');

		for (Double quantile : durations.keySet()) {
			appendCsvValue(strb, durations.get(quantile)).append(',');
		}

		strb.append(durationSnapshot.max())
						.append(',')
						.append(latencySnapshot.mean())
						.append(',')
						.append(latencySnapshot.min())
						.append(',');

		for (Double quantile : latencies.keySet()) {
			appendCsvValue(strb, latencies.get(quantile)).append(',');
		}

		strb.append(latencySnapshot.max())
						.append(',');
		if (ttfbAvailable) {
			strb.append(ttfbSnapshot.mean())
							.append(',')
							.append(ttfbSnapshot.min())
							.append(',');
			for (Double quantile : ttfbs.keySet()) {
				appendCsvValue(strb, ttfbs.get(quantile)).append(',');
			}
			strb.append(ttfbSnapshot.max());
		} else {
			strb.append(',');
			for (Double ignored : ttfbs.keySet()) {
				strb.append(',');
			}
			strb.append(',');
		}
	}

	private static StringBuilder appendCsvValue(final StringBuilder strb, final Long value) {
		return value == null ? strb : strb.append(value);
	}
}
