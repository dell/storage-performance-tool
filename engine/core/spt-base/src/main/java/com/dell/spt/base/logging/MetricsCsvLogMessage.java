package com.dell.spt.base.logging;

import static com.dell.spt.base.Constants.K;
import static com.dell.spt.base.Constants.M;
import static com.dell.spt.base.Constants.MIB;

import com.dell.spt.base.env.DateUtil;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import java.time.Instant;
import org.apache.logging.log4j.message.AsynchronouslyFormattable;

/** Created by kurila on 18.05.17. */
@AsynchronouslyFormattable
public final class MetricsCsvLogMessage extends LogMessageBase {

	private static final double LOW_QUARTILE = 0.25;
	private static final double MEDIAN = 0.5;
	private static final double HIGH_QUARTILE = 0.75;

	private final AllMetricsSnapshot snapshot;
	private final OpType opType;
	private final int concurrencyLimit;

	public MetricsCsvLogMessage(
					final AllMetricsSnapshot snapshot, final OpType opType, final int concurrencyLimit) {
		this.snapshot = snapshot;
		this.opType = opType;
		this.concurrencyLimit = concurrencyLimit;
	}

	@Override
	public final void formatTo(final StringBuilder strb) {

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
						.append(
										snapshot instanceof DistributedAllMetricsSnapshot
														? ((DistributedAllMetricsSnapshot) snapshot).nodeCount()
														: 1)
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
						.append(',')
						.append(durationSnapshot.percentile(LOW_QUARTILE))
						.append(',')
						.append(durationSnapshot.percentile(MEDIAN))
						.append(',')
						.append(durationSnapshot.percentile(HIGH_QUARTILE))
						.append(',')
						.append(durationSnapshot.max())
						.append(',')
						.append(latencySnapshot.mean())
						.append(',')
						.append(latencySnapshot.min())
						.append(',')
						.append(latencySnapshot.percentile(LOW_QUARTILE))
						.append(',')
						.append(latencySnapshot.percentile(MEDIAN))
						.append(',')
						.append(latencySnapshot.percentile(HIGH_QUARTILE))
						.append(',')
						.append(latencySnapshot.max());
	}
}
