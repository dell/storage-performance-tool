package com.dell.spt.base.load.step.local;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.apache.logging.log4j.CloseableThreadContext.Instance;
import static org.apache.logging.log4j.CloseableThreadContext.put;

import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.local.context.LoadStepContext;
import com.dell.spt.base.load.step.LoadStepBase;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.fiber4j.Fiber;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;

public abstract class LoadStepLocalBase extends LoadStepBase {

	protected final List<LoadStepContext> stepContexts = new ArrayList<>();

	protected LoadStepLocalBase(
					final Config baseConfig,
					final List<Extension> extensions,
					final List<Config> contextConfigs,
					final MetricsManager metricsManager) {
		super(baseConfig, extensions, contextConfigs, metricsManager);
	}

	@Override
	protected void doStartWrapped() {
		stepContexts.forEach(
						stepCtx -> {
							try {
								stepCtx.start();
							} catch (final RemoteException ignored) {} catch (final IllegalStateException e) {
								LogUtil.exception(
												Level.WARN, e, "{}: failed to start the load step context \"{}\"", loadStepId(), stepCtx);
							}
						});
	}

	@Override
	protected final void initMetrics(
					final int originIndex,
					final OpType opType,
					final int concurrency,
					final Config metricsConfig,
					final SizeInBytes itemDataSize,
					final boolean outputColorFlag) {
		final var index = metricsContexts.size();
		final var metricsCtx = MetricsContextImpl.builder()
						.loadStepId(loadStepId())
						.opType(opType)
						.actualConcurrencyGauge(() -> stepContexts.get(index).activeOpCount())
						.concurrencyLimit(concurrency)
						.concurrencyThreshold((int) (concurrency * metricsConfig.doubleVal("threshold")))
						.itemDataSize(itemDataSize)
						.outputPeriodSec(avgPeriod(metricsConfig))
						.stdOutColorFlag(outputColorFlag)
						.comment(config.stringVal("run-comment"))
						.runId(runId())
						.build();

		// enrich metadata with limits so /metrics/json can compute completion on workers
		try {
			long countLimit = this.config.longVal("load-op-limit-count");
			metricsCtx.metadata().put(MetricsConstants.METADATA_LIMIT_OP_COUNT, countLimit);
		} catch (Exception ignore) {}
		try {
			final Object raw = this.config.val("load-step-limit-time");
			long timeLimitSec = 0L;
			if (raw instanceof String) {
				timeLimitSec = com.dell.spt.base.config.TimeUtil.getTimeInSeconds((String) raw);
			} else if (raw != null) {
				timeLimitSec = com.github.akurilov.commons.reflection.TypeUtil.typeConvert(raw, long.class);
			}
			metricsCtx.metadata().put(MetricsConstants.METADATA_LIMIT_TIME_SEC, timeLimitSec);
		} catch (Exception ignore) {}
		metricsContexts.add(metricsCtx);
	}

	@Override
	protected final void doShutdown() {
		stepContexts.forEach(
						stepCtx -> {
							try (final Instance ctx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
								stepCtx.shutdown();
								Loggers.MSG.debug("{}: load step context shutdown", loadStepId());
							} catch (final RemoteException ignored) {}
						});
	}

	@Override
	public final boolean await(final long timeout, final TimeUnit timeUnit)
					throws IllegalStateException {

		final long timeoutMillis = timeout > 0 ? timeUnit.toMillis(timeout) : Long.MAX_VALUE;
		final long startTimeMillis = System.currentTimeMillis();
		final int stepCtxCount = stepContexts.size();
		final LoadStepContext[] stepContextsCopy = stepContexts.toArray(new LoadStepContext[stepCtxCount]);
		int countDown = stepCtxCount;
		LoadStepContext stepCtx;
		boolean timeIsOut = false;

		while (countDown > 0 && !timeIsOut) {
			for (int i = 0; i < stepCtxCount; i++) {
				if (timeoutMillis <= System.currentTimeMillis() - startTimeMillis) {
					timeIsOut = true;
					break;
				}
				stepCtx = stepContextsCopy[i];
				if (stepCtx != null) {
					try {
						if (stepCtx.isDone()
										|| stepCtx.await(Fiber.SOFT_DURATION_LIMIT_NANOS, TimeUnit.NANOSECONDS)) {
							stepContextsCopy[i] = null; // exclude
							countDown--;
							break;
						}
					} catch (final InterruptedException e) {
						throwUnchecked(e);
					} catch (final RemoteException ignored) {}
				}
			}
		}

		return 0 == countDown;
	}

	@Override
	protected final void doStop() {
		stepContexts.forEach(LoadStepContext::stop);
		super.doStop();
	}

	@Override
	protected final void doClose() throws IOException {
		super.doClose();
		stepContexts
						.parallelStream()
						.filter(Objects::nonNull)
						.forEach(
										stepCtx -> {
											try {
												stepCtx.close();
											} catch (final IOException e) {
												LogUtil.exception(
																Level.ERROR,
																e,
																"Failed to close the load step context \"{}\"",
																stepCtx.toString());
											}
										});
		stepContexts.clear();
	}
}
