package com.dell.spt.base.load.step;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.apache.logging.log4j.CloseableThreadContext.put;

import com.dell.spt.base.concurrent.DaemonBase;
import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.TimeUtil;
import com.dell.spt.base.util.BinarySizeFormat;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityConfig;
import com.dell.spt.base.integrity.IntegrityCsvArtifacts;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.github.akurilov.commons.reflection.TypeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.Level;

public abstract class LoadStepBase extends DaemonBase implements LoadStep, Runnable {

	protected final Config config;
	protected final List<Extension> extensions;
	protected final List<Config> ctxConfigs;
	protected final MetricsManager metricsMgr;
	protected final List<MetricsContext<? extends AllMetricsSnapshot>> metricsContexts = new ArrayList<>();

	private final AtomicLong timeLimitSec = new AtomicLong(Long.MAX_VALUE);
	private final boolean integrityModeEnabled;
	private volatile long startTimeSec = -1;

	protected LoadStepBase(
					final Config config,
					final List<Extension> extensions,
					final List<Config> ctxConfigs,
					final MetricsManager metricsMgr) {
		this.config = new BasicConfig(config);
		this.extensions = extensions;
		this.ctxConfigs = ctxConfigs;
		this.metricsMgr = metricsMgr;
		try {
			this.integrityModeEnabled = IntegrityConfig.validateLoadStep(this.config).enabled();
		} catch (final RuntimeException e) {
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.CONFIGURATION,
							safeStepId(config),
							"invalid storage.integrity configuration",
							e);
		}
		Loggers.CONFIG.info(ConfigUtil.toString(config, ConfigFormat.YAML, resolveStepTypeName()));
	}

	private String resolveStepTypeName() {
		try {
			return getTypeName();
		} catch (final Exception e) {
			return null;
		}
	}

	@Override
	public final String loadStepId() {
		return config.stringVal("load-step-id");
	}

	@Override
	public final long runId() {
		return config.longVal("run-id");
	}

	@Override
	public final List<? extends AllMetricsSnapshot> metricsSnapshots() {
		MetricsContext ctx;
		AllMetricsSnapshot snapshot;
		final var count = metricsContexts.size();
		final List<AllMetricsSnapshot> metricsSnapshots = new ArrayList<>(count);
		for (var i = 0; i < count; i++) {
			ctx = metricsContexts.get(i);
			if (null != ctx) {
				ctx.refreshLastSnapshot();
				snapshot = ctx.lastSnapshot();
				if (null != snapshot) {
					metricsSnapshots.add(snapshot);
				}
			}
		}
		return metricsSnapshots;
	}

	@Override
	public final void run() {
		IntegrityTerminalException terminalCause = null;
		try {
			start();
			try {
				await(timeLimitSec.get(), TimeUnit.SECONDS);
			} catch (final RuntimeException e) {
				if (IntegrityTerminalException.find(e) != null || integrityModeEnabled) {
					terminalCause = terminalFailure(
									IntegrityTerminalException.Category.EXECUTION,
									"failed to await metadata-mode step",
									e);
				} else {
					LogUtil.exception(Level.WARN, e, "Failed to await \"{}\"", toString());
				}
			}
		} catch (final InterruptedException e) {
			throwUnchecked(e);
		} catch (final Throwable cause) {
			throwUncheckedIfInterrupted(cause);
			if (IntegrityTerminalException.find(cause) != null || integrityModeEnabled) {
				terminalCause = terminalFailure(
								IntegrityTerminalException.Category.EXECUTION,
								"metadata-mode step execution failed",
								cause);
			} else if (cause instanceof IllegalStateException) {
				LogUtil.exception(Level.ERROR, cause, "Failed to start \"{}\"", toString());
			} else {
				LogUtil.exception(Level.ERROR, cause, "Load step execution failure \"{}\"", toString());
			}
		} finally {
			try {
				close();
			} catch (final Throwable closeCause) {
				throwUncheckedIfInterrupted(closeCause);
				if (integrityModeEnabled) {
					final var cleanupFailure = terminalFailure(
									IntegrityTerminalException.Category.CLEANUP,
									"metadata-mode step cleanup failed",
									closeCause);
					if (terminalCause == null) {
						terminalCause = cleanupFailure;
					} else {
						terminalCause.addSuppressed(cleanupFailure);
					}
				} else {
					doStop();
					LogUtil.trace(Loggers.ERR, Level.WARN, closeCause, "Failed to close \"{}\"", toString());
				}
			}
		}
		if (terminalCause != null) {
			throw terminalCause;
		}
	}

	@Override
	protected void doStart() throws IllegalStateException {

		init();

		try (final var logCtx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {

			seedIntegrityArtifactHeaders();
			doStartWrapped();

			final long t;
			final var loadStepLimitTimeRaw = config.val("load-step-limit-time");
			if (loadStepLimitTimeRaw instanceof String) {
				t = TimeUtil.getTimeInSeconds((String) loadStepLimitTimeRaw);
			} else {
				t = TypeUtil.typeConvert(loadStepLimitTimeRaw, long.class);
			}
			if (t > 0) {
				timeLimitSec.set(t);
			}
			startTimeSec = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

		} catch (final Throwable cause) {
			throwUncheckedIfInterrupted(cause);
			if (IntegrityTerminalException.find(cause) != null || integrityModeEnabled) {
				throw terminalFailure(
								IntegrityTerminalException.Category.EXECUTION,
								"metadata-mode step failed to start",
								cause);
			}
			LogUtil.exception(Level.WARN, cause, "{} step failed to start", loadStepId());
		}

		metricsContexts.stream().peek(MetricsContext::start).forEach(metricsMgr::register);
	}

	protected abstract void doStartWrapped();

	/**
	 * Initializes the actual configuration and metrics contexts
	 *
	 * @throws IllegalStateException if initialization fails
	 */
	protected abstract void init() throws IllegalStateException;

	protected abstract void initMetrics(
					final int originIndex,
					final OpType opType,
					final int concurrency,
					final Config metricsConfig,
					final SizeInBytes itemDataSize,
					final boolean outputColorFlag);

	protected final boolean effectiveVerifyFlag(
					final boolean verifyFlag, final boolean dedupable, final String stepId) {
		if (verifyFlag && !dedupable) {
			Loggers.MSG.warn(
							"{}: item.data.verify=true is incompatible with item.data.dedupable=false; disabling verification",
							stepId);
			return false;
		}
		return verifyFlag;
	}

	@Override
	protected void doStop() {

		metricsContexts.forEach(metricsMgr::unregister);

		final long t = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis()) - startTimeSec;
		if (t < 0) {
			Loggers.ERR.warn("Stopped earlier than started, won't account the elapsed time");
		} else if (t > timeLimitSec.get()) {
			Loggers.MSG.warn(
							"The elapsed time ({}[s]) is more than the limit ({}[s]), further resuming is not available",
							t,
							timeLimitSec.get());
			timeLimitSec.set(0);
		} else {
			timeLimitSec.addAndGet(-t);
		}
	}

	@Override
	protected void doClose() throws IOException {
		metricsContexts.forEach(MetricsContext::close);
	}

	private void seedIntegrityArtifactHeaders() {
		if (!integrityModeEnabled || !emitsOperationArtifacts()) {
			return;
		}
		final OpType opType = OpType.valueOf(config.stringVal("load-op-type").toUpperCase());
		for (final var artifact : IntegrityCsvArtifacts.applicableHeaders(
						opType, true, multipartEnabled())) {
			switch (artifact.kind()) {
			case FAILURES -> Loggers.INTEGRITY_FAILURES.info(artifact.header());
			case PERFORMANCE -> Loggers.INTEGRITY_PERFORMANCE.info(artifact.header());
			case MULTIPART_LIFECYCLE -> Loggers.MULTIPART_LIFECYCLE.info(artifact.header());
			default -> throw new AssertionError("unsupported integrity artifact kind " + artifact.kind());
			}
		}
	}

	protected boolean emitsOperationArtifacts() {
		return true;
	}

	private boolean multipartEnabled() {
		final var threshold = config.val("item-data-ranges-threshold");
		return threshold instanceof String
						? BinarySizeFormat.parseFixedSize((String) threshold) > 0
						: TypeUtil.typeConvert(threshold, long.class) > 0;
	}

	protected final boolean integrityModeEnabled() {
		return integrityModeEnabled;
	}

	protected final IntegrityTerminalException terminalFailure(
					final IntegrityTerminalException.Category category,
					final String message,
					final Throwable cause) {
		final var existing = IntegrityTerminalException.find(cause);
		return existing == null
						? new IntegrityTerminalException(category, loadStepId(), message, cause)
						: existing.withStepId(loadStepId());
	}

	private static String safeStepId(final Config config) {
		try {
			return config.stringVal("load-step-id");
		} catch (final RuntimeException ignored) {
			return null;
		}
	}

	protected int avgPeriod(final Config metricsConfig) {
		final Object metricsAvgPeriodRaw = metricsConfig.val("average-period");
		if (metricsAvgPeriodRaw instanceof String) {
			return (int) TimeUtil.getTimeInSeconds((String) metricsAvgPeriodRaw);
		} else {
			return TypeUtil.typeConvert(metricsAvgPeriodRaw, int.class);
		}
	}
}
