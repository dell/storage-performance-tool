package com.dell.spt.base.logging;

import static com.dell.spt.base.Constants.K;
import static com.dell.spt.base.Constants.M;
import static com.dell.spt.base.Constants.MIB;
import static com.dell.spt.base.Constants.UNIT_MIB_PER_SECOND;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_DURATION_DEFINITION_DISPLAY;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_LATENCY_DEFINITION_DISPLAY;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_OBJECT_UNIT;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_OUTCOME_ACCEPTED;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_REQUEST_UNIT;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_TIMING_MARKER_SOURCE;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_VERIFICATION_NOTICE;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.util.BinarySizeFormat;
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
	private final Map<Double, Long> ttfbs;
	// assuming 0.999999 is the most detailed quantile user would want to use
	private final int LENGTH_OF_LONGEST_QUANTILE = 8;

	public StepResultsMetricsLogMessage(
					final OpType opType,
					final String stepId,
					final int concurrencyLimit,
					final DistributedAllMetricsSnapshot snapshot,
					final Map<Double, Long> latencyQuantiles,
					final Map<Double, Long> durationQuantiles) {
		this(opType, stepId, concurrencyLimit, snapshot, latencyQuantiles, durationQuantiles, Map.of());
	}

	public StepResultsMetricsLogMessage(
					final OpType opType,
					final String stepId,
					final int concurrencyLimit,
					final DistributedAllMetricsSnapshot snapshot,
					final Map<Double, Long> latencyQuantiles,
					final Map<Double, Long> durationQuantiles,
					final Map<Double, Long> ttfbQuantiles) {
		this.opType = opType;
		this.stepId = stepId;
		this.snapshot = snapshot;
		this.concurrencyLimit = concurrencyLimit;
		this.latencies = latencyQuantiles;
		this.durations = durationQuantiles;
		this.ttfbs = ttfbQuantiles;

	}

	@Override
	public final void formatTo(final StringBuilder buff) {
		final String lineSep = System.lineSeparator();
		final DeleteMetricsSnapshot deleteMetrics = snapshot.deleteMetrics();
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
						.append(deleteMetrics == null
										? BinarySizeFormat.formatFixedSize(snapshot.byteSnapshot().count())
										: "N/A")
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
						.append("  Bandwidth [")
						.append(UNIT_MIB_PER_SECOND)
						.append("]:           ");
		if (deleteMetrics == null) {
			buff.append(lineSep)
							.append("    Last:                      ")
							.append(snapshot.byteSnapshot().last() / MIB)
							.append(lineSep)
							.append("    Mean:                      ")
							.append(snapshot.byteSnapshot().mean() / MIB)
							.append(lineSep);
		} else {
			buff.append("N/A").append(lineSep);
		}
		appendOperationTiming(
						buff, "  Operations Duration [us]:    ", snapshot.durationSnapshot(), durations, lineSep);
		appendOperationTiming(
						buff, "  Operations Latency [us]:     ", snapshot.latencySnapshot(), latencies, lineSep);
		buff.append("  Time To First Byte [us]:     ").append(lineSep);

		if (snapshot.ttfbSnapshot() == null || snapshot.ttfbSnapshot().count() == 0) {
			buff.append("    N/A").append(lineSep);
		} else {
			buff.append("    Avg:                       ")
							.append(snapshot.ttfbSnapshot().mean())
							.append(lineSep)
							.append("    Min:                       ")
							.append(snapshot.ttfbSnapshot().min())
							.append(lineSep);
			for (Double quantile : ttfbs.keySet()) {
				buff.append("    Quantile ")
								.append(quantile)
								.append(":         ");
				final int quantileLengthDifference = LENGTH_OF_LONGEST_QUANTILE - String.valueOf(quantile).length();
				if (quantileLengthDifference > 0) {
					buff.append(" ".repeat(quantileLengthDifference));
				}
				appendQuantileValue(buff, ttfbs.get(quantile))
								.append(lineSep);
			}
			buff.append("    Max:                       ")
							.append(snapshot.ttfbSnapshot().max())
							.append(lineSep);
		}

		if (deleteMetrics != null) {
			appendDeleteMetrics(buff, deleteMetrics, snapshot, lineSep);
		}

		buff.append("...")
						.append(lineSep);
	}

	private static void appendDeleteMetrics(
					final StringBuilder buff,
					final DeleteMetricsSnapshot metrics,
					final DistributedAllMetricsSnapshot allMetrics,
					final String lineSep) {
		buff.append("  DELETE Results:").append(lineSep)
						.append("    Units:").append(lineSep)
						.append("      Requests:                  ").append(DELETE_REQUEST_UNIT).append(lineSep)
						.append("      Objects:                   ").append(DELETE_OBJECT_UNIT).append(lineSep)
						.append("      Batches:                   ").append(DELETE_REQUEST_UNIT).append(lineSep)
						.append("    Requests:").append(lineSep)
						.append("      Attempted:                 ").append(metrics.requestAttempted()).append(lineSep)
						.append("      Full Success:              ").append(metrics.requestFullSuccess()).append(lineSep)
						.append("      Partial:                   ").append(metrics.requestPartial()).append(lineSep)
						.append("      Failed:                    ").append(metrics.requestFailed()).append(lineSep)
						.append("      Unresolved:                ").append(metrics.requestUnresolved()).append(lineSep)
						.append("      Per Second:                ").append(metrics.requestsPerSecond()).append(lineSep)
						.append("    Objects:").append(lineSep)
						.append("      Selected:                  ").append(metrics.objectSelected()).append(lineSep)
						.append("      Attempted:                 ").append(metrics.objectAttempted()).append(lineSep)
						.append("      Accepted:                   ").append(metrics.objectAccepted()).append(lineSep)
						.append("      Failed:                    ").append(metrics.objectFailed()).append(lineSep)
						.append("      Unattempted:               ").append(metrics.objectUnattempted()).append(lineSep)
						.append("      Unresolved:                ").append(metrics.objectUnresolved()).append(lineSep)
						.append("      Per Second:                ").append(metrics.objectsPerSecond()).append(lineSep)
						.append("    Batches:").append(lineSep)
						.append("      Configured Size:           ").append(metrics.configuredBatchSize()).append(lineSep)
						.append("      Actual Requests:           ").append(metrics.actualRequestCount()).append(lineSep)
						.append("      Actual Objects:            ").append(metrics.actualObjectCount()).append(lineSep)
						.append("      Mean Objects Per Request:  ")
						.append(metrics.actualRequestCount() == 0 ? 0.0
										: (double) metrics.actualObjectCount() / metrics.actualRequestCount())
						.append(lineSep)
						.append("      Full Batches:              ").append(metrics.fullBatchCount()).append(lineSep)
						.append("      Partial Batches:           ").append(metrics.partialBatchCount()).append(lineSep)
						.append("      Full Batch Percent:        ")
						.append(metrics.actualRequestCount() == 0 ? 0.0
										: metrics.fullBatchCount() * 100.0 / metrics.actualRequestCount())
						.append(lineSep)
						.append("    Completion Percent:").append(lineSep)
						.append("      Requests:                  ")
						.append(metrics.requestAttempted() == 0 ? 0.0
										: (metrics.requestFullSuccess() + metrics.requestPartial()
														+ metrics.requestFailed() + metrics.requestUnresolved())
														* 100.0 / metrics.requestAttempted())
						.append(lineSep)
						.append("      Objects:                   ")
						.append(metrics.objectSelected() == 0 ? 0.0
										: (metrics.objectAccepted() + metrics.objectFailed()
														+ metrics.objectUnattempted() + metrics.objectUnresolved())
														* 100.0 / metrics.objectSelected())
						.append(lineSep)
						.append("    Versions:").append(lineSep)
						.append("      Current Key:               ").append(metrics.currentKeyCount()).append(lineSep)
						.append("      Exact Version:             ").append(metrics.exactVersionCount()).append(lineSep)
						.append("    Identity:").append(lineSep)
						.append("      Mode:                      ").append(metrics.mode()).append(lineSep)
						.append("      Configured Batch Size:     ").append(metrics.configuredBatchSize()).append(lineSep)
						.append("      Selection Order:           ").append(metrics.selectionOrder()).append(lineSep)
						.append("    Failure Policy:").append(lineSep)
						.append("      Outcome:                   ")
						.append(humanFailureOutcome(metrics.failureOutcome())).append(lineSep)
						.append("      Mode:                      ").append(metrics.failurePolicyMode()).append(lineSep)
						.append("      Max Failed Objects:        ").append(metrics.maxFailedObjects()).append(lineSep)
						.append("      Max Failure Percent:       ").append(metrics.maxFailurePercent()).append(lineSep)
						.append("      Grace Seconds:             ").append(metrics.graceSeconds()).append(lineSep)
						.append("      Operational Failures:      ")
						.append(metrics.operationalFailedObjects()).append(lineSep)
						.append("      Excluded Failures:         ")
						.append(metrics.excludedFailedObjects()).append(lineSep)
						.append("      Observed Failure Percent:  ").append(metrics.observedFailurePercent()).append(lineSep)
						.append("    Phase Seconds:").append(lineSep);
		appendPhaseSeconds(buff, "      Seed:                      ", metrics.seedNanos(), lineSep);
		appendPhaseSeconds(buff, "      Discovery:                 ", metrics.discoveryNanos(), lineSep);
		appendPhaseSeconds(buff, "      Pre Validation:            ", metrics.preValidationNanos(), lineSep);
		appendPhaseSeconds(buff, "      Scheduled DELETE:          ", metrics.scheduledDeleteNanos(), lineSep);
		appendPhaseSeconds(buff, "      Drain:                     ", metrics.drainNanos(), lineSep);
		appendPhaseSeconds(
						buff, "      Post Verification:         ", metrics.postVerificationNanos(), lineSep);
		appendPhaseSeconds(buff, "      Cleanup:                   ", metrics.cleanupNanos(), lineSep);
		appendPhaseSeconds(buff, "      Total Wall:                ", metrics.totalWallNanos(), lineSep);
		buff.append("    Timing Definitions:").append(lineSep)
						.append("      Request Latency:           ").append(DELETE_LATENCY_DEFINITION_DISPLAY)
						.append(lineSep)
						.append("      Request Duration:          ").append(DELETE_DURATION_DEFINITION_DISPLAY)
						.append(lineSep)
						.append("      Marker Source:             ").append(DELETE_TIMING_MARKER_SOURCE)
						.append(lineSep)
						.append("      Object Latency:            N/A (not derived from batch requests)")
						.append(lineSep)
						.append("    Request Latency Quantiles [us]:").append(lineSep);
		appendStandardQuantiles(buff, allMetrics.latencySnapshot(), lineSep);
		buff.append("    Request Duration Quantiles [us]:").append(lineSep);
		appendStandardQuantiles(buff, allMetrics.durationSnapshot(), lineSep);
		buff
						.append("    Performance:").append(lineSep)
						.append("      Object Size:               N/A").append(lineSep)
						.append("      Data Moved:                N/A").append(lineSep)
						.append("      Bandwidth:                 N/A").append(lineSep)
						.append("      TTFB:                      N/A").append(lineSep)
						.append("      Object Latency:            N/A").append(lineSep)
						.append("    Outcome Terminology:         ").append(DELETE_OUTCOME_ACCEPTED).append(lineSep)
						.append("    Verification:").append(lineSep)
						.append("      Enabled:                   false").append(lineSep)
						.append("      Removal Confirmed:          false").append(lineSep)
						.append("      Notice:                    ").append(DELETE_VERIFICATION_NOTICE).append(lineSep)
						.append("    Terminal Reconciled:         ").append(metrics.reconciled()).append(lineSep);
		if (!metrics.buckets().isEmpty()) {
			buff.append("    Buckets:").append(lineSep);
			for (final DeleteMetricsSnapshot.Bucket bucket : metrics.buckets()) {
				buff.append("      - Bucket:                  ").append(bucket.bucket()).append(lineSep)
								.append("        Selected:                ").append(bucket.selected()).append(lineSep)
								.append("        Attempted:               ").append(bucket.attempted()).append(lineSep)
								.append("        Accepted:                ").append(bucket.accepted()).append(lineSep)
								.append("        Failed:                  ").append(bucket.failed()).append(lineSep);
			}
		}
	}

	private static String humanFailureOutcome(final String outcome) {
		return outcome == null ? "N/A" : outcome.replace('_', ' ');
	}

	private static void appendPhaseSeconds(
					final StringBuilder buff,
					final String label,
					final long nanos,
					final String lineSep) {
		buff.append(label);
		if (nanos < 0) {
			buff.append("N/A");
		} else {
			buff.append(nanos / 1_000_000_000.0);
		}
		buff.append(lineSep);
	}

	private static void appendStandardQuantiles(
					final StringBuilder buff,
					final TimingMetricSnapshot timing,
					final String lineSep) {
		if (timing == null || timing.count() == 0) {
			buff.append("      P50:                       N/A").append(lineSep)
							.append("      P90:                       N/A").append(lineSep)
							.append("      P99:                       N/A").append(lineSep)
							.append("      P99.9:                     N/A").append(lineSep);
			return;
		}
		buff.append("      P50:                       ").append(timing.percentile(0.5)).append(lineSep)
						.append("      P90:                       ").append(timing.percentile(0.9)).append(lineSep)
						.append("      P99:                       ").append(timing.percentile(0.99)).append(lineSep)
						.append("      P99.9:                     ").append(timing.percentile(0.999)).append(lineSep);
	}

	private void appendOperationTiming(
					final StringBuilder buff,
					final String heading,
					final TimingMetricSnapshot timing,
					final Map<Double, Long> quantiles,
					final String lineSep) {
		buff.append(heading).append(lineSep);
		if (timing == null || timing.count() == 0) {
			buff.append("    N/A").append(lineSep);
			return;
		}
		buff.append("    Avg:                       ").append(timing.mean()).append(lineSep)
						.append("    Min:                       ").append(timing.min()).append(lineSep);
		for (Double quantile : quantiles.keySet()) {
			buff.append("    Quantile ").append(quantile).append(":         ");
			final int quantileLengthDifference = LENGTH_OF_LONGEST_QUANTILE - String.valueOf(quantile).length();
			if (quantileLengthDifference > 0) {
				buff.append(" ".repeat(quantileLengthDifference));
			}
			appendQuantileValue(buff, quantiles.get(quantile)).append(lineSep);
		}
		buff.append("    Max:                       ").append(timing.max()).append(lineSep);
	}

	private static StringBuilder appendQuantileValue(final StringBuilder buff, final Long value) {
		return value == null ? buff.append("N/A") : buff.append(value);
	}
}
