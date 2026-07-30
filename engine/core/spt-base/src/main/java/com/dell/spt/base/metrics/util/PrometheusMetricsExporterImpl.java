package com.dell.spt.base.metrics.util;

import static com.dell.spt.base.metrics.MetricsConstants.*;
import static io.prometheus.client.Collector.MetricFamilySamples.Sample;

import com.dell.spt.base.Constants;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.NamedMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.MetricsConstants;
import io.prometheus.client.Collector;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/** Prometheus collector that exports SPT distributed metrics snapshots. */
public class PrometheusMetricsExporterImpl extends Collector implements PrometheusMetricsExporter {

	private static final AtomicLong COLLECTOR_IDS = new AtomicLong(0);
	private final List<String> labelValues = new ArrayList<>();
	private final List<String> labelNames = new ArrayList<>();
	private final DistributedMetricsContext metricsContext;
	private final List<Double> quantileValues = new ArrayList<>();
	private final long collectorId = COLLECTOR_IDS.incrementAndGet();
	private String help = "";

	public PrometheusMetricsExporterImpl(final DistributedMetricsContext context) {
		this.metricsContext = context;
	}

	@Override
	public PrometheusMetricsExporterImpl quantile(final double value) {
		if (value < 1.0 && value >= 0.0) {
			quantileValues.add(value);
		} else {
			throw new IllegalArgumentException("Invalid quantiele value : " + value);
		}
		return this;
	}

	@Override
	public PrometheusMetricsExporterImpl quantiles(final double[] values) {
		for (int i = 0; i < values.length; ++i) {
			quantile(values[i]);
		}
		return this;
	}

	@Override
	public PrometheusMetricsExporterImpl quantiles(final List<Double> values) {
		for (int i = 0; i < values.size(); ++i) {
			quantile(values.get(i));
		}
		return this;
	}

	@Override
	public PrometheusMetricsExporterImpl label(final String name, final String value) {
		this.labelNames.add(name);
		this.labelValues.add(value);
		return this;
	}

	@Override
	public PrometheusMetricsExporterImpl labels(final String[] names, final String[] values) {
		if (names.length != values.length) {
			throw new IllegalArgumentException(
							"The number of label names("
											+ names.length
											+ ") does not match the number of values("
											+ values.length
											+ ")");
		}
		this.labelNames.addAll(Arrays.asList(names));
		this.labelValues.addAll(Arrays.asList(values));
		return this;
	}

	@Override
	public PrometheusMetricsExporterImpl help(final String helpInfo) {
		help = helpInfo;
		return this;
	}

	@Override
	public List<MetricFamilySamples> collect() {
		final List<MetricFamilySamples> mfsList = new ArrayList<>();

		// CRITICAL FIX: Always refresh snapshot before Prometheus collection
		// This ensures we get fresh metrics instead of potentially stale data
		// that could cause zero values even when operations are running
		metricsContext.refreshLastSnapshot();

		final DistributedAllMetricsSnapshot snapshot = metricsContext.lastSnapshot();
		if (snapshot != null) {
			collectSnapshot(snapshot.durationSnapshot(), mfsList);
			collectSnapshot(snapshot.latencySnapshot(), mfsList);
			collectSnapshot(snapshot.concurrencySnapshot(), mfsList);
			collectSnapshot(snapshot.byteSnapshot(), mfsList);
			collectSnapshot(snapshot.successSnapshot(), mfsList);
			collectSnapshot(snapshot.failsSnapshot(), mfsList);
			if (snapshot.corruptSnapshot() != null) {
				collectSnapshot(snapshot.corruptSnapshot(), mfsList);
			}
			collectElapsedTime(snapshot.elapsedTimeMillis(), mfsList);
			collectTestState(snapshot, mfsList);
			collectCompletionPercent(snapshot, mfsList);
		} else {
			// No active test - still report test state for TUI clients
			collectTestState(null, mfsList);
		}
		return mfsList;
	}

	private void collectElapsedTime(
					final long timeMillis, final List<MetricFamilySamples> mfsList) {
		final double timeSeconds = (double) timeMillis / Constants.K;
		mfsList.add(
						new MetricFamilySamples(
										metricFamilyName(METRIC_NAME_TIME),
										Type.GAUGE,
										help,
										Collections.singletonList(
														newSample(METRIC_NAME_TIME, "value", timeSeconds))));
	}

	private void collectCompletionPercent(
					final DistributedAllMetricsSnapshot snapshot, final List<MetricFamilySamples> mfsList) {
		// Determine limits from metadata
		final var metadata = metricsContext.metadata();
		long countLimit = 0L;
		long timeLimitSec = 0L;
		if (metadata != null) {
			final Long countLimitCandidate = toLong(metadata.get(MetricsConstants.METADATA_LIMIT_OP_COUNT),
							MetricsConstants.METADATA_LIMIT_OP_COUNT);
			final Long timeLimitCandidate = toLong(metadata.get(MetricsConstants.METADATA_LIMIT_TIME_SEC),
							MetricsConstants.METADATA_LIMIT_TIME_SEC);
			if (countLimitCandidate != null && countLimitCandidate > 0L) {
				countLimit = countLimitCandidate;
			}
			if (timeLimitCandidate != null && timeLimitCandidate > 0L) {
				timeLimitSec = timeLimitCandidate;
			}
		}

		// Compute completion fraction using count if available, otherwise time
		double completion = 0.0;
		if (countLimit > 0) {
			final long succ = snapshot.successSnapshot().count();
			final long fail = snapshot.failsSnapshot().count();
			final double frac = (double) (succ + fail) / (double) countLimit;
			completion = Math.min(1.0, Math.max(0.0, frac));
		} else if (timeLimitSec > 0) {
			final double frac = (snapshot.elapsedTimeMillis() / 1000.0) / (double) timeLimitSec;
			completion = Math.min(1.0, Math.max(0.0, frac));
		}
		final int percent = (int) Math.round(completion * 100.0);

		mfsList.add(
						new MetricFamilySamples(
										metricFamilyName(MetricsConstants.METRIC_NAME_COMPLETION),
										Type.GAUGE,
										help,
										Collections.singletonList(
														newSample(MetricsConstants.METRIC_NAME_COMPLETION, "value", percent))));
	}

	private void collectTestState(
					final DistributedAllMetricsSnapshot snapshot, final List<MetricFamilySamples> mfsList) {
		// Test state metric for TUI client integration
		// 0 = no test active/idle, 1 = test running, 2 = test completed with results
		double testState = 0.0;  // Default: no test active

		if (snapshot != null) {
			// We have a snapshot - determine if test is actively running or completed
			final var successCount = snapshot.successSnapshot().count();
			final var failsCount = snapshot.failsSnapshot().count();
			final var totalOps = successCount + failsCount;

			if (totalOps > 0) {
				// We have completed operations
				final var concurrency = snapshot.concurrencySnapshot().last();
				if (concurrency > 0) {
					testState = 1.0;  // Test is actively running
				} else {
					testState = 2.0;  // Test completed with results
				}
			} else {
				// No operations completed yet, but we have a snapshot
				testState = 1.0;  // Test is starting/running
			}
		}

		mfsList.add(
						new MetricFamilySamples(
										metricFamilyName(METRIC_NAME_TEST_STATE),
										Type.GAUGE,
										help,
										Collections.singletonList(
														newSample(METRIC_NAME_TEST_STATE, "value", testState))));
	}

	private void collectSnapshot(
					final NamedMetricSnapshot snapshot, final List<MetricFamilySamples> mfsList) {
		final List<Sample> samples = new ArrayList<>();
		if (snapshot instanceof TimingMetricSnapshot) {
			samples.addAll(collect((TimingMetricSnapshot) snapshot));
		} else if (snapshot instanceof RateMetricSnapshot) {
			samples.addAll(collect((RateMetricSnapshot) snapshot));
		} else if (snapshot instanceof ConcurrencyMetricSnapshot) {
			samples.addAll(collect((ConcurrencyMetricSnapshot) snapshot));
		} else {
			Loggers.ERR.warn("Unexpected metric snapshot type: {}", snapshot.getClass());
		}
		final MetricFamilySamples mfs = new MetricFamilySamples(metricFamilyName(snapshot.name()), Type.GAUGE, help, samples);
		mfsList.add(mfs);
	}

	private List<Sample> collect(final RateMetricSnapshot metric) {
		final String metricName = metric.name();
		final List<Sample> samples = new ArrayList<>();
		samples.add(newSample(metricName, "count", (double) metric.count()));
		samples.add(newSample(metricName, "rate_mean", metric.mean()));
		samples.add(newSample(metricName, "rate_last", metric.last()));

		return samples;
	}

	private List<Sample> collect(final TimingMetricSnapshot metric) {
		final List<Sample> samples = new ArrayList<>();
		//final HistogramSnapshot snapshot = metric.histogramSnapshot(); // for quantiles
		final String metricName = metric.name();
		samples.add(newSample(metricName, "count", (double) metric.count()));
		samples.add(newSample(metricName, "sum", metric.sum() / Constants.M));
		samples.add(newSample(metricName, "mean", metric.mean() / Constants.M));
		samples.add(newSample(metricName, "min", metric.min() / Constants.M));
		/*for (int i = 0; i < quantileValues.size(); ++i) {
			samples.add(
							newSample(
											metricName,
											"quantile_" + quantileValues.get(i).toString().replaceAll("\\.", "_"),
											snapshot.quantile(quantileValues.get(i)) / Constants.M));
		}*/
		samples.add(newSample(metricName, "max", metric.max() / Constants.M));

		// DEBUG: Log if timing values are zero when count > 0
		if (metric.count() > 0 && metric.mean() == 0.0) {
			Loggers.ERR.warn("Timing metric {} has count {} but mean is 0.0 - potential snapshot timing issue",
							metricName, metric.count());
		}

		return samples;
	}

	private List<Sample> collect(final ConcurrencyMetricSnapshot metric) {
		final String metricName = metric.name();
		final List<Sample> samples = new ArrayList<>();
		samples.add(newSample(metricName, "mean", metric.mean()));
		samples.add(newSample(metricName, "last", (double) metric.last()));
		return samples;
	}

	private Sample newSample(
					final String metricName, final String aggregationType, final double value) {
		return new Sample(
						String.format(METRIC_FORMAT, metricName) + "_" + aggregationType, labelNames, labelValues, value);
	}

	private String metricFamilyName(final String metricName) {
		return String.format(METRIC_FORMAT, metricName + collectorId);
	}

	private static Long toLong(final Object raw, final String key) {
		if (raw == null) {
			return null;
		}
		if (raw instanceof Number number) {
			return number.longValue();
		}
		if (raw instanceof String str) {
			if (str.isBlank()) {
				return null;
			}
			try {
				return Long.parseLong(str.trim());
			} catch (final NumberFormatException e) {
				Loggers.ERR.warn("Ignoring non-numeric value \"{}\" for metadata key {}", str, key);
				return null;
			}
		}
		Loggers.ERR.debug("Ignoring unsupported metadata value type {} for key {}", raw.getClass(), key);
		return null;
	}
}
