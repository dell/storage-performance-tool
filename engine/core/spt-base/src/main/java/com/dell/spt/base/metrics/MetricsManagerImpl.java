package com.dell.spt.base.metrics;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.base.metrics.MetricsConstants.metricLabelsArray;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.apache.logging.log4j.CloseableThreadContext.Instance;
import static org.apache.logging.log4j.CloseableThreadContext.put;

import com.dell.spt.base.concurrent.TaskBase;
import com.dell.spt.base.concurrent.ThreadTaskExecutor;
import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.logging.MetricsAsciiTableLogMessage;
import com.dell.spt.base.logging.ExtResultsXmlLogMessage;
import com.dell.spt.base.logging.MetricsCsvLogMessage;
import com.dell.spt.base.logging.MetricsTotalCsvLogMessage;
import com.dell.spt.base.logging.StepResultsMetricsLogMessage;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.metrics.util.PrometheusMetricsExporter;
import com.dell.spt.base.metrics.util.PrometheusMetricsExporterImpl;
import com.dell.spt.base.metrics.util.CombinedCompletionCollector;
import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;

/** Created by kurila on 18.05.17. */
public class MetricsManagerImpl extends TaskBase implements MetricsManager {

	private static final String CLS_NAME = MetricsManagerImpl.class.getSimpleName();
	private static final long LOCK_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
	private final Set<MetricsContext> allMetrics = new ConcurrentSkipListSet<>();
	private final Map<DistributedMetricsContext, PrometheusMetricsExporter> distributedMetrics = new ConcurrentHashMap<>();
	private final Set<MetricsContext> selectedMetrics = new TreeSet<>();
	private final ReentrantLock outputLock = new ReentrantLock();

	// Tracks which step IDs have an open <result> wrapper tag in result.xml.
	// Used to emit opening/closing wrapper tags as explicit log events so that both
	// CLI and jar-only users get well-formed XML (log4j2 header/footer had async timing issues).
	private final Set<String> openResultXmlSteps = ConcurrentHashMap.newKeySet();
	private final Set<String> openMetricsTotalCsvSteps = ConcurrentHashMap.newKeySet();

	// Terminal entries retention for /metrics/json when idle
	private final Map<ProgressKey, TerminalStepEntry> terminalByStepId = new ConcurrentHashMap<>();
	private final Map<ProgressKey, TerminalStepEntry> lastProgressByStepId = new ConcurrentHashMap<>();
	private volatile long terminalRetentionMillis = TimeUnit.SECONDS.toMillis(20);
	private static final List<Double> STANDARD_TIMING_QUANTILES = List.of(0.5, 0.9, 0.99, 0.999);

	public MetricsManagerImpl(final ThreadTaskExecutor executor) {
		super(executor);
		// Register a global collector that emits aggregated step completion percent per step id
		try {
			CollectorRegistry.defaultRegistry.register(
							new CombinedCompletionCollector(() -> (Set<DistributedMetricsContext>) (Set) distributedMetrics.keySet()));
		} catch (final IllegalArgumentException alreadyRegistered) {
			Loggers.MSG.debug("CombinedCompletionCollector already registered, skipping duplicate registration");
		} catch (final RuntimeException e) {
			LogUtil.exception(Level.WARN, e, "Failed to register CombinedCompletionCollector");
		}
	}

	/** Preserves the constructor descriptor used by existing extensions. */
	public MetricsManagerImpl(final VirtualThreadExecutor executor) {
		this((ThreadTaskExecutor) executor);
	}

	@Override
	protected void doInit() {
		ThreadContext.put(KEY_CLASS_NAME, CLS_NAME);
	}

	@Override
	protected final void doWork() throws Exception {
		int actualConcurrency = 0;
		int nextConcurrencyThreshold;
		outputLock.lock();
		try {
			for (final MetricsContext metricsCtx : allMetrics) {
				ThreadContext.put(KEY_STEP_ID, metricsCtx.loadStepId());
				// TODO: as a future improvement consider whether throttling is needed here.
				metricsCtx.refreshLastSnapshot();

				final AllMetricsSnapshot snapshot = metricsCtx.lastSnapshot();
				if (null != snapshot) {
					recordProgressSnapshot(metricsCtx, snapshot);
					final ConcurrencyMetricSnapshot concurrencySnapshot = snapshot.concurrencySnapshot();
					if (null != concurrencySnapshot) {
						actualConcurrency = (int) concurrencySnapshot.last();
					}
					// threshold load state checks
					nextConcurrencyThreshold = metricsCtx.concurrencyThreshold();
					if (nextConcurrencyThreshold > 0 && actualConcurrency >= nextConcurrencyThreshold) {
						if (!metricsCtx.thresholdStateEntered() && !metricsCtx.thresholdStateExited()) {
							Loggers.MSG.info(
											"{}: the threshold of {} active load operations count is reached, "
															+ "starting the additional metrics accounting",
											metricsCtx.toString(),
											metricsCtx.concurrencyThreshold());
							metricsCtx.enterThresholdState();
						}
					} else if (metricsCtx.thresholdStateEntered() && !metricsCtx.thresholdStateExited()) {
						exitMetricsThresholdState(metricsCtx);
					}
					// periodic output
					final long outputPeriodMillis = metricsCtx.outputPeriodMillis();
					final long lastOutputTs = metricsCtx.lastOutputTs();
					final long nextOutputTs = System.currentTimeMillis();
					if (outputPeriodMillis > 0 && nextOutputTs - lastOutputTs >= outputPeriodMillis) {
						metricsCtx.lastOutputTs(nextOutputTs);
						selectedMetrics.add(metricsCtx);
						if (metricsCtx.avgPersistEnabled()) {
							Loggers.METRICS_FILE.info(
											new MetricsCsvLogMessage(
															snapshot, metricsCtx.opType(), metricsCtx.concurrencyLimit()));
						}
					}
				}
			}
			// console output
			if (!selectedMetrics.isEmpty()) {
				Loggers.METRICS_STD_OUT.info(new MetricsAsciiTableLogMessage(selectedMetrics));
				selectedMetrics.clear();
			}
		} catch (final ConcurrentModificationException cme) {
			Loggers.MSG.debug("Metrics context set modified during sweep; will retry in next cycle");
		} catch (final Throwable cause) {
			throwUncheckedIfInterrupted(cause);
			LogUtil.exception(Level.DEBUG, cause, "Metrics manager failure");
		} finally {
			outputLock.unlock();
		}
		Thread.sleep(10); // 100 Hz iteration rate
	}

	private void startIfNotStarted() {
		if (!isStarted()) {
			if (isStopped()) {
				restart();
				Loggers.MSG.debug("Restarted the metrics manager task for a new load step");
			} else {
				super.start();
				Loggers.MSG.debug("Started the metrics manager task");
			}
		}
	}

	@Override
	public void register(final MetricsContext metricsCtx) {
		try {
			startIfNotStarted();
			lastProgressByStepId.remove(ProgressKey.of(metricsCtx.loadStepId(), metricsCtx instanceof DistributedMetricsContext));
			allMetrics.add(metricsCtx);
			if (metricsCtx instanceof DistributedMetricsContext<?> distributedMetricsCtx) {
				final String[] labelValues = {
						metricsCtx.loadStepId(),
						metricsCtx.opType().name(),
						String.valueOf(metricsCtx.concurrencyLimit()),
						metricsCtx.itemDataSize().toString(),
						String.valueOf(metricsCtx.startTimeStamp()),
						((DistributedMetricsContext) metricsCtx).nodeAddrs().toString(),
						metricsCtx.comment(),
						String.valueOf(metricsCtx.runId())
				};
				distributedMetrics.put(
								distributedMetricsCtx,
								new PrometheusMetricsExporterImpl(distributedMetricsCtx)
												.labels(metricLabelsArray(), labelValues)
												//.quantiles(distributedMetricsCtx.quantileValues())
												.register());
			}
			Loggers.MSG.debug("Metrics context \"{}\" registered", metricsCtx);
		} catch (final RuntimeException e) {
			throwUncheckedIfInterrupted(e);
			LogUtil.exception(
							Level.WARN,
							e,
							"Failed to register the Prometheus Exporter for the metrics context \"{}\"",
							metricsCtx.toString());
		}
	}

	@Override
	public void unregister(final MetricsContext metricsCtx) {
		try (final Instance logCtx = put(KEY_STEP_ID, metricsCtx.loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			if (allMetrics.remove(metricsCtx)) {
				boolean lockAcquired = false;
				try {
					lockAcquired = outputLock.tryLock(LOCK_TIMEOUT_NANOS, TimeUnit.NANOSECONDS);
					if (!lockAcquired) {
						Loggers.ERR.warn(
										"Acquire lock timeout while unregistering the metrics context \"{}\"", metricsCtx);
					}
					final AllMetricsSnapshot snapshotBefore = metricsCtx.lastSnapshot();
					metricsCtx.refreshLastSnapshot(true); // one last time, including throttled timing snapshots
					final AllMetricsSnapshot snapshotAfter = metricsCtx.lastSnapshot();
					final AllMetricsSnapshot snapshot = preferTerminalSnapshot(snapshotAfter, snapshotBefore);
					// check for the metrics threshold state if entered
					if (metricsCtx.thresholdStateEntered() && !metricsCtx.thresholdStateExited()) {
						exitMetricsThresholdState(metricsCtx);
					}

					final boolean distributedEntry = metricsCtx instanceof DistributedMetricsContext;
					final ProgressKey progressKey = ProgressKey.of(metricsCtx.loadStepId(), distributedEntry);
					recordProgressSnapshot(metricsCtx, snapshot);
					TerminalStepEntry terminalEntry = null;

					if (distributedEntry) {
						final DistributedMetricsContext<?> distributedMetricsCtx = (DistributedMetricsContext<?>) metricsCtx;
						final List<Double> quantileValues = timingQuantiles(distributedMetricsCtx.quantileValues());
						final Map<Double, Long> latencyQuantiles = snapshot == null
										? emptyQuantileValues(quantileValues)
										: quantileValues(snapshot.latencySnapshot(), quantileValues);
						final Map<Double, Long> durationQuantiles = snapshot == null
										? emptyQuantileValues(quantileValues)
										: quantileValues(snapshot.durationSnapshot(), quantileValues);
						final Map<Double, Long> ttfbQuantiles = snapshot == null
										? emptyQuantileValues(quantileValues)
										: quantileValues(snapshot.ttfbSnapshot(), quantileValues);

						if (snapshot instanceof DistributedAllMetricsSnapshot aggregSnapshot) {
							if (hasProgress(snapshot)) {
								final var nodesPresent = distributedMetricsCtx.nodeAddrs();
								final int nodeCount = distributedMetricsCtx.nodeCount();
								final boolean partial = nodesPresent != null && !nodesPresent.isEmpty() && nodeCount > nodesPresent.size();
								terminalEntry = buildEntryFromSnapshot(
												metricsCtx,
												aggregSnapshot,
												true,
												nodeCount,
												nodesPresent,
												partial);
							}
							// file output
							// due to unknown reasons writing to a csv.total is based on a flag and not on a metrics
							// class instance. though this flag is only enabled for distributed context.
							if (metricsCtx.sumPersistEnabled()) {
								final boolean includeHeader = openMetricsTotalCsvSteps.add(metricsCtx.loadStepId());
								final MetricsTotalCsvLogMessage message = new MetricsTotalCsvLogMessage(
												snapshot, metricsCtx.opType(), metricsCtx.concurrencyLimit(), latencyQuantiles,
												durationQuantiles, ttfbQuantiles);
								final StringBuilder csvOutput = new StringBuilder();
								message.formatTo(csvOutput, includeHeader);
								Loggers.METRICS_FILE_TOTAL.info(csvOutput.toString());
							}
						}
						// console output
						Loggers.METRICS_STD_OUT.info(
										new MetricsAsciiTableLogMessage(Collections.singleton(metricsCtx)));
						final DistributedAllMetricsSnapshot aggregSnapshot = (DistributedAllMetricsSnapshot) snapshot;
						if (null != aggregSnapshot) {
							Loggers.METRICS_STD_OUT.info(
											new StepResultsMetricsLogMessage(
															metricsCtx.opType(),
															metricsCtx.loadStepId(),
															metricsCtx.concurrencyLimit(),
															aggregSnapshot,
															latencyQuantiles,
															durationQuantiles,
															ttfbQuantiles));
						} else {
							Loggers.ERR.warn("Metrics snapshot is empty. No metrics were recorded apparently.");
						}

						// XML results file output (result.xml)
						if (null != aggregSnapshot && metricsCtx.sumPersistEnabled()) {
							if (openResultXmlSteps.add(metricsCtx.loadStepId())) {
								Loggers.METRICS_EXT_RESULTS_FILE.info("<result>");
							}
							Loggers.METRICS_EXT_RESULTS_FILE.info(
											new ExtResultsXmlLogMessage(metricsCtx, aggregSnapshot));
						}
						// Close XML wrapper when this is the last context for the step
						if (openResultXmlSteps.contains(metricsCtx.loadStepId())
										&& allMetrics.stream().noneMatch(
														ctx -> metricsCtx.loadStepId().equals(ctx.loadStepId()))) {
							Loggers.METRICS_EXT_RESULTS_FILE.info("</result>");
							openResultXmlSteps.remove(metricsCtx.loadStepId());
							openMetricsTotalCsvSteps.remove(metricsCtx.loadStepId());
						}

						final PrometheusMetricsExporter exporter = distributedMetrics.remove(distributedMetricsCtx);
						if (null != exporter) {
							CollectorRegistry.defaultRegistry.unregister((Collector) exporter);
						}

						// Cache terminal entry for JSON endpoint
						if (terminalEntry != null) {
							terminalByStepId.put(progressKey, terminalEntry);
						}
					} else {
						// Cache terminal entry for JSON endpoint for local (non-distributed) contexts
						if (snapshot != null
										&& snapshot.successSnapshot() != null
										&& snapshot.failsSnapshot() != null
										&& snapshot.byteSnapshot() != null
										&& snapshot.latencySnapshot() != null
										&& snapshot.durationSnapshot() != null
										&& snapshot.concurrencySnapshot() != null
										&& hasProgress(snapshot)) {
							terminalEntry = buildEntryFromSnapshot(
											metricsCtx,
											snapshot,
											false,
											0,
											List.of(),
											false);
							terminalByStepId.put(progressKey, terminalEntry);
						}
					}
					if (terminalEntry == null) {
						final TerminalStepEntry lastProgressEntry = lastProgressByStepId.get(progressKey);
						if (lastProgressEntry != null) {
							terminalByStepId.put(progressKey, lastProgressEntry);
						}
					}
					lastProgressByStepId.remove(progressKey);
				} catch (final InterruptedException e) {
					throwUnchecked(e);
				} finally {
					if (lockAcquired && outputLock.isHeldByCurrentThread()) {
						outputLock.unlock();
					}
				}
			} else {
				Loggers.ERR.debug("Metrics context \"{}\" has not been registered", metricsCtx);
			}
			Loggers.MSG.debug("Metrics context \"{}\" unregistered", metricsCtx);
		} finally {
			if (allMetrics.isEmpty()) {
				stop();
				Loggers.MSG.debug("Stopped the metrics manager task");
			}
		}
	}

	private static void exitMetricsThresholdState(final MetricsContext<?> metricsCtx) {
		Loggers.MSG.info(
						"{}: the active load operations count is below the threshold of {}, stopping the additional metrics "
										+ "accounting",
						metricsCtx.toString(),
						metricsCtx.concurrencyThreshold());
		final MetricsContext lastThresholdMetrics = metricsCtx.thresholdMetrics();
		final AllMetricsSnapshot snapshot = lastThresholdMetrics.lastSnapshot();
		if (lastThresholdMetrics.sumPersistEnabled()) {
			Loggers.METRICS_THRESHOLD_FILE_TOTAL.info(
							new MetricsCsvLogMessage(snapshot, metricsCtx.opType(), metricsCtx.concurrencyLimit()));
			Loggers.METRICS_THRESHOLD_EXT_RESULTS_FILE.info(
							new ExtResultsXmlLogMessage(metricsCtx, snapshot));
		}
		metricsCtx.exitThresholdState();
	}

	private static List<Double> timingQuantiles(final List<Double> configuredQuantiles) {
		final TreeSet<Double> quantiles = new TreeSet<>(STANDARD_TIMING_QUANTILES);
		if (configuredQuantiles != null) {
			for (final Double quantile : configuredQuantiles) {
				if (quantile != null && quantile > 0.0 && quantile <= 1.0) {
					quantiles.add(quantile);
				}
			}
		}
		return new ArrayList<>(quantiles);
	}

	private static Map<Double, Long> quantileValues(
					final TimingMetricSnapshot snapshot, final List<Double> quantiles) {
		final Map<Double, Long> values = new LinkedHashMap<>(quantiles.size());
		for (final Double quantile : quantiles) {
			values.put(quantile, snapshot == null || snapshot.count() == 0 ? null : snapshot.percentile(quantile));
		}
		return values;
	}

	private static Map<Double, Long> emptyQuantileValues(final List<Double> quantiles) {
		final Map<Double, Long> values = new LinkedHashMap<>(quantiles.size());
		for (final Double quantile : quantiles) {
			values.put(quantile, 0L);
		}
		return values;
	}

	@Override
	public Set<DistributedMetricsContext> getDistributedContexts() {
		return distributedMetrics.keySet();
	}

	@Override
	public java.util.Set<com.dell.spt.base.metrics.context.MetricsContext> getAllContexts() {
		return allMetrics;
	}

	@Override
	public Optional<TerminalStepEntry> getLastProgressSnapshot(final String stepId) {
		return getLastProgressSnapshot(stepId, false);
	}

	@Override
	public Optional<TerminalStepEntry> getLastProgressSnapshot(final String stepId, final boolean distributed) {
		if (stepId == null) {
			return Optional.empty();
		}
		return Optional.ofNullable(lastProgressByStepId.get(ProgressKey.of(stepId, distributed)));
	}

	@Override
	public void setTerminalRetentionMillis(final long millis) {
		this.terminalRetentionMillis = Math.max(0L, millis);
	}

	@Override
	public java.util.List<TerminalStepEntry> getTerminalSteps() {
		if (terminalByStepId.isEmpty())
			return List.of();
		final long now = System.currentTimeMillis();
		final var result = new ArrayList<TerminalStepEntry>(terminalByStepId.size());
		final var iterator = terminalByStepId.entrySet().iterator();
		while (iterator.hasNext()) {
			final var entry = iterator.next();
			final TerminalStepEntry v = entry.getValue();
			if (now - v.recordedAtMillis <= terminalRetentionMillis) {
				result.add(v);
			} else {
				iterator.remove();
			}
		}
		return result;
	}

	@Override
	protected final void doClose() {
		// Close any open result.xml wrapper tags (safety net for abnormal shutdown)
		openResultXmlSteps.forEach(stepId -> {
			ThreadContext.put(KEY_STEP_ID, stepId);
			Loggers.METRICS_EXT_RESULTS_FILE.info("</result>");
		});
		openResultXmlSteps.clear();
		allMetrics.forEach(MetricsContext::close);
		allMetrics.clear();
		distributedMetrics.clear();
	}

	private void recordProgressSnapshot(final MetricsContext metricsCtx, final AllMetricsSnapshot snapshot) {
		if (snapshot == null
						|| snapshot.successSnapshot() == null
						|| snapshot.failsSnapshot() == null
						|| snapshot.byteSnapshot() == null
						|| snapshot.latencySnapshot() == null
						|| snapshot.durationSnapshot() == null
						|| snapshot.concurrencySnapshot() == null) {
			return;
		}

		final long successCount = snapshot.successSnapshot().count();
		final long failCount = snapshot.failsSnapshot().count();
		final long bytesTotal = snapshot.byteSnapshot().count();
		if (successCount <= 0 && failCount <= 0 && bytesTotal <= 0) {
			return;
		}

		final boolean distributedEntry = metricsCtx instanceof DistributedMetricsContext && snapshot instanceof DistributedAllMetricsSnapshot;
		final ProgressKey progressKey = ProgressKey.of(metricsCtx.loadStepId(), distributedEntry);
		final TerminalStepEntry existing = lastProgressByStepId.get(progressKey);
		if (existing != null
						&& existing.successCount >= successCount
						&& existing.failedCount >= failCount
						&& existing.bytesTotal >= bytesTotal) {
			return;
		}

		final TerminalStepEntry entry;
		if (distributedEntry) {
			final var distributedMetricsCtx = (DistributedMetricsContext<?>) metricsCtx;
			final var nodesPresent = distributedMetricsCtx.nodeAddrs();
			final int nodeCount = distributedMetricsCtx.nodeCount();
			final boolean partial = nodesPresent != null && !nodesPresent.isEmpty() && nodeCount > nodesPresent.size();
			entry = buildEntryFromSnapshot(
							metricsCtx, snapshot, true, nodeCount, nodesPresent, partial);
		} else {
			entry = buildEntryFromSnapshot(
							metricsCtx, snapshot, false, 0, List.of(), false);
		}

		lastProgressByStepId.put(progressKey, entry);
		if (Loggers.MSG.isDebugEnabled()) {
			Loggers.MSG.debug(
							"{}: cached progress snapshot updated (distributed={}, success={}, fails={}, bytes={})",
							metricsCtx.loadStepId(),
							distributedEntry,
							entry.successCount,
							entry.failedCount,
							entry.bytesTotal);
		}
	}

	private TerminalStepEntry buildEntryFromSnapshot(
					final MetricsContext metricsCtx,
					final AllMetricsSnapshot snapshot,
					final boolean distributed,
					final int nodeCount,
					final List<String> nodesPresent,
					final boolean partial) {
		final long countLimit = extractLongMetadata(
						metricsCtx.metadata(), com.dell.spt.base.metrics.MetricsConstants.METADATA_LIMIT_OP_COUNT);
		final long timeLimitSec = extractLongMetadata(
						metricsCtx.metadata(), com.dell.spt.base.metrics.MetricsConstants.METADATA_LIMIT_TIME_SEC);

		return new TerminalStepEntry(
						metricsCtx.loadStepId(),
						metricsCtx.opType(),
						metricsCtx.runId(),
						System.currentTimeMillis(),
						snapshot.successSnapshot().count(),
						snapshot.failsSnapshot().count(),
						snapshot.byteSnapshot().count(),
						snapshot.latencySnapshot().mean(),
						snapshot.durationSnapshot().mean(),
						snapshot.latencySnapshot(),
						snapshot.durationSnapshot(),
						snapshot.ttfbSnapshot(),
						snapshot.concurrencySnapshot().last(),
						snapshot.concurrencySnapshot().mean(),
						countLimit,
						timeLimitSec,
						snapshot.elapsedTimeMillis(),
						distributed,
						nodeCount,
						nodesPresent,
						partial);
	}

	private long extractLongMetadata(final Map metadata, final String key) {
		if (metadata == null) {
			return 0L;
		}
		final Object value = metadata.get(key);
		if (value == null) {
			return 0L;
		}
		if (value instanceof Number number) {
			return number.longValue();
		}
		if (value instanceof String str) {
			if (str.isBlank()) {
				return 0L;
			}
			try {
				return Long.parseLong(str.trim());
			} catch (final NumberFormatException nfe) {
				Loggers.MSG.debug("Ignoring non-numeric metadata value \"{}\" for key {}", str, key);
				return 0L;
			}
		}
		Loggers.MSG.debug("Ignoring unsupported metadata value type {} for key {}", value.getClass(), key);
		return 0L;
	}

	private static final class ProgressKey {
		private final String stepId;
		private final boolean distributed;

		private ProgressKey(final String stepId, final boolean distributed) {
			this.stepId = stepId;
			this.distributed = distributed;
		}

		static ProgressKey of(final String stepId, final boolean distributed) {
			return new ProgressKey(stepId, distributed);
		}

		@Override
		public boolean equals(final Object o) {
			if (this == o) {
				return true;
			}
			if (!(o instanceof ProgressKey)) {
				return false;
			}
			final ProgressKey that = (ProgressKey) o;
			return distributed == that.distributed && Objects.equals(stepId, that.stepId);
		}

		@Override
		public int hashCode() {
			return Objects.hash(stepId, distributed);
		}
	}

	private static AllMetricsSnapshot preferTerminalSnapshot(
					final AllMetricsSnapshot primary, final AllMetricsSnapshot fallback) {
		if (hasProgress(primary)) {
			return primary;
		}
		if (hasProgress(fallback)) {
			return fallback;
		}
		return primary != null ? primary : fallback;
	}

	private static boolean hasProgress(final AllMetricsSnapshot snapshot) {
		if (snapshot == null) {
			return false;
		}
		try {
			return (snapshot.successSnapshot() != null && snapshot.successSnapshot().count() > 0)
							|| (snapshot.failsSnapshot() != null && snapshot.failsSnapshot().count() > 0)
							|| (snapshot.byteSnapshot() != null && snapshot.byteSnapshot().count() > 0);
		} catch (final RuntimeException e) {
			Loggers.MSG.debug("Unable to determine snapshot progress due to {}", e.toString());
			return false;
		}
	}
}
