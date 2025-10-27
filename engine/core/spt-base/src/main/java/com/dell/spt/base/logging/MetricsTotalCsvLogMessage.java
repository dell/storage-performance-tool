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

	public MetricsTotalCsvLogMessage(
					final AllMetricsSnapshot snapshot, final OpType opType, final int concurrencyLimit,
					final Map<Double, Long> latencyQuantiles, final Map<Double, Long> durationQuantiles) {
		this.snapshot = snapshot;
		this.opType = opType;
		this.concurrencyLimit = concurrencyLimit;
		this.latencies = latencyQuantiles;
		this.durations = durationQuantiles;
	}

	@Override
	public final void formatTo(final StringBuilder strb) {
		final String lineSep = System.lineSeparator();
		// log4j2 supports file headers to avoid this, but we need a dynamic header based on the provided quantiles
		// headers are printed every time formatTo is called because each time we call it for a new step, hence
		// for a new file
		strb.append("DateTimeISO8601,OpType,Concurrency,NodeCount,ConcurrencyCurr,ConcurrencyMean,CountSucc,")
						.append("CountFail,Size,StepDuration[s],DurationSum[s],TPAvg[op/s],TPLast[op/s],BWAvg[MB/s],")
						.append("BWLast[MB/s],DurationAvg[us],DurationMin[us],");

		for (Double quantile : durations.keySet()) {
			strb.append("DurationQ_")
							.append(quantile)
							.append("[us],");
		}
		strb.append("DurationMax[us],LatencyAvg[us],LatencyMin[us],");

		// not like quantiles are different for duration and latency, but just to be precise
		for (Double quantile : latencies.keySet()) {
			strb.append("LatencyQ_")
							.append(quantile)
							.append("[us],");
		}
		strb.append("LatencyMax[us]")
						.append(lineSep);

		final ConcurrencyMetricSnapshot concurrencySnapshot = snapshot.concurrencySnapshot();
		final TimingMetricSnapshot durationSnapshot = snapshot.durationSnapshot();
		final RateMetricSnapshot successCountSnapshot = snapshot.successSnapshot();
		final RateMetricSnapshot byteCountSnapshot = snapshot.byteSnapshot();
		final TimingMetricSnapshot latencySnapshot = snapshot.latencySnapshot();

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
			strb.append(durations.get(quantile))
							.append(',');
		}

		strb.append(durationSnapshot.max())
						.append(',')
						.append(latencySnapshot.mean())
						.append(',')
						.append(latencySnapshot.min())
						.append(',');

		for (Double quantile : latencies.keySet()) {
			strb.append(latencies.get(quantile))
							.append(',');
		}

		strb.append(latencySnapshot.max());
	}
}
