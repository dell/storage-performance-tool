package com.dell.spt.base.logging;

import static com.dell.spt.base.Constants.K;
import static com.dell.spt.base.Constants.M;
import static com.dell.spt.base.Constants.MIB;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.github.akurilov.commons.system.SizeInBytes;
import org.apache.logging.log4j.message.AsynchronouslyFormattable;

import java.util.Map;

/** Created by kurila on 18.05.17. */
@AsynchronouslyFormattable
public class StepResultsMetricsLogMessage extends LogMessageBase {

	private final OpType opType;
	private final String stepId;
	private final int concurrencyLimit;
	private final DistributedAllMetricsSnapshot snapshot;
	private final Map<Double, Long> latencies;
	private final Map<Double, Long> durations;
	// assuming 0.999999 is the most detailed quantile user would want to use
	private final int LENGTH_OF_LONGEST_QUANTILE = 8;

	public StepResultsMetricsLogMessage(
					final OpType opType,
					final String stepId,
					final int concurrencyLimit,
					final DistributedAllMetricsSnapshot snapshot,
					final Map<Double, Long> latencyQuantiles,
					final Map<Double, Long> durationQuantiles) {
		this.opType = opType;
		this.stepId = stepId;
		this.snapshot = snapshot;
		this.concurrencyLimit = concurrencyLimit;
		this.latencies = latencyQuantiles;
		this.durations = durationQuantiles;

	}

	@Override
	public final void formatTo(final StringBuilder buff) {
		final String lineSep = System.lineSeparator();
		buff.append("---")
						.append(lineSep)
						.append(
										"# Results ##############################################################################################################")
						.append(lineSep)
						.append("- Load Step Id:                ")
						.append(stepId)
						.append(lineSep)
						.append("  Operation Type:              ")
						.append(opType)
						.append(lineSep)
						.append("  Node Count:                  ")
						.append(snapshot.nodeCount())
						.append(lineSep)
						.append("  Concurrency:                 ")
						.append(lineSep)
						.append("    Limit Per Storage Driver:  ")
						.append(concurrencyLimit)
						.append(lineSep)
						.append("    Actual:                    ")
						.append(lineSep)
						.append("      Last:                    ")
						.append(snapshot.concurrencySnapshot().last())
						.append(lineSep)
						.append("      Mean:                    ")
						.append(snapshot.concurrencySnapshot().mean())
						.append(lineSep)
						.append("  Operations Count:            ")
						.append(lineSep)
						.append("    Successful:                ")
						.append(snapshot.successSnapshot().count())
						.append(lineSep)
						.append("    Failed:                    ")
						.append(snapshot.failsSnapshot().count())
						.append(lineSep)
						.append("  Transfer Size:               ")
						.append(SizeInBytes.formatFixedSize(snapshot.byteSnapshot().count()))
						.append(lineSep)
						.append("  Duration [s]:                ")
						.append(lineSep)
						.append("    Elapsed:                   ")
						.append(snapshot.elapsedTimeMillis() / K)
						.append(lineSep)
						.append("    Sum:                       ")
						.append(snapshot.durationSnapshot().sum() / M)
						.append(lineSep)
						.append("  Throughput [op/s]:           ")
						.append(lineSep)
						.append("    Last:                      ")
						.append(snapshot.successSnapshot().last())
						.append(lineSep)
						.append("    Mean:                      ")
						.append(snapshot.successSnapshot().mean())
						.append(lineSep)
						.append("  Bandwidth [MB/s]:            ")
						.append(lineSep)
						.append("    Last:                      ")
						.append(snapshot.byteSnapshot().last() / MIB)
						.append(lineSep)
						.append("    Mean:                      ")
						.append(snapshot.byteSnapshot().mean() / MIB)
						.append(lineSep)
						.append("  Operations Duration [us]:    ")
						.append(lineSep)
						.append("    Avg:                       ")
						.append(snapshot.durationSnapshot().mean())
						.append(lineSep)
						.append("    Min:                       ")
						.append(snapshot.durationSnapshot().min())
						.append(lineSep);

		for (Double quantile : durations.keySet()) {
			buff.append("    Quantile ")
							.append(quantile)
							.append(":         ");
			final int quantileLengthDifference = LENGTH_OF_LONGEST_QUANTILE - String.valueOf(quantile).length();
			if (quantileLengthDifference > 0) {
				buff.append(" ".repeat(quantileLengthDifference));
			}
			buff.append(durations.get(quantile))
							.append(lineSep);
		}

		buff.append("    Max:                       ")
						.append(snapshot.durationSnapshot().max())
						.append(lineSep)
						.append("  Operations Latency [us]:     ")
						.append(lineSep)
						.append("    Avg:                       ")
						.append(snapshot.latencySnapshot().mean())
						.append(lineSep)
						.append("    Min:                       ")
						.append(snapshot.latencySnapshot().min())
						.append(lineSep);

		for (Double quantile : latencies.keySet()) {
			buff.append("    Quantile ")
							.append(quantile)
							.append(":         ");
			final int quantileLengthDifference = LENGTH_OF_LONGEST_QUANTILE - String.valueOf(quantile).length();
			if (quantileLengthDifference > 0) {
				buff.append(" ".repeat(quantileLengthDifference));
			}
			buff.append(latencies.get(quantile))
							.append(lineSep);
		}

		buff.append("    Max:                       ")
						.append(snapshot.latencySnapshot().max())
						.append(lineSep)
						.append("...")
						.append(lineSep);
	}
}
