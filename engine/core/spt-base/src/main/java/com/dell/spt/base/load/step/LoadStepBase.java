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
import com.dell.spt.base.item.op.deletion.StandaloneDeleteConfig;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.logging.log4j.Level;

public abstract class LoadStepBase extends DaemonBase implements LoadStep, Runnable {
	private static final long DURATION_STOP_PHASE_MIN_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(2);
	private static final int DURATION_RUN_CLOSE_ATTEMPT_LIMIT = 3;

	protected final Config config;
	protected final List<Extension> extensions;
	protected final List<Config> ctxConfigs;
	protected final MetricsManager metricsMgr;
	protected final List<MetricsContext<? extends AllMetricsSnapshot>> metricsContexts = new ArrayList<>();

	private final AtomicLong timeLimitSec = new AtomicLong(Long.MAX_VALUE);
	private final boolean integrityModeEnabled;
	private final boolean standaloneDeleteDurationMode;
	private final boolean standaloneDeleteEnabled;
	private final boolean standaloneDeletePreValidationEnabled;
	private final boolean standaloneDeletePostVerificationEnabled;
	private final Map<String, RetainedDurationPhase<?>> retainedDurationPhases = new HashMap<>();
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
		final StandaloneDeleteConfig standaloneDelete = StandaloneDeleteConfig.from(
						this.config.configVal("load"));
		this.standaloneDeleteEnabled = standaloneDelete.enabled();
		this.standaloneDeleteDurationMode = standaloneDelete.durationMode();
		this.standaloneDeletePreValidationEnabled = standaloneDelete.preValidation();
		this.standaloneDeletePostVerificationEnabled = standaloneDelete.postVerification();
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
				final boolean completedBeforeDeadline = await(timeLimitSec.get(), TimeUnit.SECONDS);
				if (standaloneDeleteDurationMode && completedBeforeDeadline) {
					terminalCause = standaloneDeleteEarlyExhaustionFailure();
				}
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
			final Throwable closeCause = closeAfterRun();
			if (closeCause != null) {
				throwUncheckedIfInterrupted(closeCause);
				if (IntegrityTerminalException.find(closeCause) != null || integrityModeEnabled) {
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

	private Throwable closeAfterRun() {
		final int attemptLimit = standaloneDeleteDurationMode
						? DURATION_RUN_CLOSE_ATTEMPT_LIMIT
						: 1;
		Throwable firstFailure = null;
		for (var attempt = 0; attempt < attemptLimit; attempt++) {
			try {
				close();
				return null;
			} catch (final Throwable closeCause) {
				throwUncheckedIfInterrupted(closeCause);
				if (firstFailure == null) {
					firstFailure = closeCause;
				} else if (closeCause != firstFailure) {
					firstFailure.addSuppressed(closeCause);
				}
			}
		}
		if (standaloneDeleteDurationMode) {
			// A normal run has no caller left to perform another explicit close. Cancel any
			// still-owned lifecycle resources after the bounded retries so they cannot leak.
			try {
				cancelDurationRunCleanupResourcesAfterExhaustion();
			} catch (final Throwable cleanupCause) {
				throwUncheckedIfInterrupted(cleanupCause);
				if (cleanupCause != firstFailure) {
					firstFailure.addSuppressed(cleanupCause);
				}
			} finally {
				closeRetainedDurationPhases();
			}
		} else {
			closeRetainedDurationPhases();
		}
		return firstFailure;
	}

	/** Cancels subclass-owned duration resources after the bounded run cleanup is exhausted. */
	protected void cancelDurationRunCleanupResourcesAfterExhaustion() {}

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
		try {
			closeMetricsContexts();
		} finally {
			closeRetainedDurationPhases();
		}
	}

	protected final void closeMetricsContexts() {
		metricsContexts.forEach(MetricsContext::close);
	}

	/** Cancels phase calls only after terminal cleanup succeeds, preserving sticky retry ownership. */
	protected final void closeRetainedDurationPhases() {
		final List<RetainedDurationPhase<?>> retainedPhases;
		synchronized (this) {
			retainedPhases = List.copyOf(retainedDurationPhases.values());
			retainedDurationPhases.clear();
		}
		retainedPhases.forEach(RetainedDurationPhase::close);
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

	protected final boolean standaloneDeleteDurationMode() {
		return standaloneDeleteDurationMode;
	}

	protected final boolean standaloneDeleteEnabled() {
		return standaloneDeleteEnabled;
	}

	protected final boolean standaloneDeletePreValidationEnabled() {
		return standaloneDeletePreValidationEnabled;
	}

	protected final boolean standaloneDeletePostVerificationEnabled() {
		return standaloneDeletePostVerificationEnabled;
	}

	@FunctionalInterface
	protected interface DurationPhaseAction<T> {
		void execute(T handle) throws Exception;
	}

	protected record DurationPhaseAttempt(
					List<Throwable> failures, boolean interrupted, boolean completedAll) {
		public DurationPhaseAttempt {}

		public boolean succeeded() {
			return completedAll && failures.isEmpty() && !interrupted;
		}
	}

	/**
	 * Offers a duration-stop phase to every frozen handle without multiplying blocked calls on retry.
	 * Completed failures may be retried by the next explicit cleanup attempt; incomplete calls remain
	 * owned by this step and are only observed again. Retried calls for the same phase are serialized,
	 * while a distinct deadline-close phase may proceed when an earlier start call is stuck.
	 */
	protected final <T> DurationPhaseAttempt invokeRetainedDurationPhase(
					final String phaseKey,
					final List<T> handles,
					final DurationPhaseAction<T> action,
					final long deadlineNanos,
					final String threadNamePrefix) {
		final RetainedDurationPhase<T> phase;
		synchronized (this) {
			@SuppressWarnings("unchecked")
			final RetainedDurationPhase<T> retained = (RetainedDurationPhase<T>) retainedDurationPhases.get(phaseKey);
			if (retained == null) {
				phase = new RetainedDurationPhase<>(handles, threadNamePrefix);
				retainedDurationPhases.put(phaseKey, phase);
			} else {
				retained.requireSameHandles(handles);
				phase = retained;
			}
		}
		final DurationPhaseAttempt attempt = phase.attempt(action, deadlineNanos);
		if (attempt.succeeded()) {
			final boolean removed;
			synchronized (this) {
				removed = retainedDurationPhases.remove(phaseKey, phase);
			}
			if (removed) {
				phase.close();
			}
		}
		return attempt;
	}

	private static final class RetainedDurationPhase<T> implements AutoCloseable {
		private final List<T> handles;
		private final boolean[] completed;
		private final List<Future<Integer>> inFlight;
		private final Map<Future<Integer>, Integer> handleIndexes = new HashMap<>();
		private final ExecutorService executor;
		private final ExecutorCompletionService<Integer> completions;

		private RetainedDurationPhase(final List<T> handles, final String threadNamePrefix) {
			this.handles = List.copyOf(handles);
			completed = new boolean[handles.size()];
			inFlight = new ArrayList<>(handles.size());
			for (int i = 0; i < handles.size(); i++) {
				inFlight.add(null);
			}
			// Lifecycle hooks may enter synchronized extension/RMI code and pin a virtual-thread
			// carrier indefinitely. One daemon platform thread per frozen handle preserves fair
			// delivery while keeping the retained phase bound at O(handle count).
			executor = Executors.newThreadPerTaskExecutor(
							Thread.ofPlatform().daemon().name(threadNamePrefix, 0).factory());
			completions = new ExecutorCompletionService<>(executor);
		}

		private void requireSameHandles(final List<T> candidates) {
			if (handles.size() != candidates.size()) {
				throw new IllegalStateException("duration phase handle set changed during cleanup retry");
			}
			for (int i = 0; i < handles.size(); i++) {
				if (handles.get(i) != candidates.get(i)) {
					throw new IllegalStateException("duration phase handle identity changed during cleanup retry");
				}
			}
		}

		private synchronized DurationPhaseAttempt attempt(
						final DurationPhaseAction<T> action, final long deadlineNanos) {
			for (int i = 0; i < handles.size(); i++) {
				if (completed[i] || inFlight.get(i) != null) {
					continue;
				}
				final int handleIndex = i;
				final Future<Integer> future = completions.submit(() -> {
					action.execute(handles.get(handleIndex));
					return handleIndex;
				});
				inFlight.set(i, future);
				handleIndexes.put(future, i);
			}

			final List<Throwable> failures = new ArrayList<>();
			boolean interrupted = false;
			while (hasInFlight()) {
				final Future<Integer> future;
				try {
					final long remainingNanos = DurationTime.remainingNanos(
									deadlineNanos, System.nanoTime());
					if (remainingNanos == 0) {
						failures.add(new TimeoutException("duration stop phase exceeded its deadline"));
						break;
					}
					future = completions.poll(remainingNanos, TimeUnit.NANOSECONDS);
					if (future == null) {
						failures.add(new TimeoutException("duration stop phase exceeded its deadline"));
						break;
					}
				} catch (final InterruptedException e) {
					interrupted = true;
					failures.add(e);
					break;
				}
				final Integer handleIndex = handleIndexes.remove(future);
				if (handleIndex == null) {
					continue;
				}
				inFlight.set(handleIndex, null);
				try {
					future.get();
					completed[handleIndex] = true;
				} catch (final ExecutionException e) {
					failures.add(e.getCause());
				} catch (final InterruptedException e) {
					interrupted = true;
					failures.add(e);
					break;
				}
			}
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
			return new DurationPhaseAttempt(
							List.copyOf(failures), interrupted, allCompleted());
		}

		private boolean hasInFlight() {
			return inFlight.stream().anyMatch(future -> future != null);
		}

		private boolean allCompleted() {
			for (final boolean handleCompleted : completed) {
				if (!handleCompleted) {
					return false;
				}
			}
			return true;
		}

		@Override
		public synchronized void close() {
			inFlight.stream()
							.filter(future -> future != null)
							.forEach(future -> future.cancel(true));
			executor.shutdownNow();
		}
	}

	protected final IntegrityTerminalException standaloneDeleteEarlyExhaustionFailure() {
		return terminalFailure(
						IntegrityTerminalException.Category.EXECUTION,
						"Standalone DELETE inventory slice exhausted before the requested duration; "
										+ "increase --seed-objects for a seeded run or provide/select a larger frozen inventory",
						null);
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

	protected final IntegrityTerminalException appendTerminalFailure(
					final IntegrityTerminalException current,
					final IntegrityTerminalException.Category category,
					final String message,
					final Throwable cause) {
		final var failure = terminalFailure(category, message, cause);
		if (current == null) {
			return failure;
		}
		if (failure != current) {
			current.addSuppressed(failure);
		}
		return current;
	}

	/**
	 * Bounds one lifecycle RPC/context phase independently from the request-drain budget.
	 * A two-second floor lets a zero-drain configuration close and recover admission while
	 * retaining room for the generator's one-second cooperative task-stop wait.
	 */
	protected final long durationStopPhaseDeadlineNanos() {
		final long configuredNanos = TimeUnit.SECONDS.toNanos(
						Math.max(0, config.intVal("load-op-wait-limit")));
		return durationDeadlineNanos(
						Math.max(DURATION_STOP_PHASE_MIN_TIMEOUT_NANOS, configuredNanos));
	}

	/** Returns a monotonic deadline using signed-difference-safe nanoTime arithmetic. */
	protected static long durationDeadlineNanos(final long budgetNanos) {
		return durationDeadlineNanos(System.nanoTime(), budgetNanos);
	}

	/** Returns a deadline relative to an existing monotonic boundary. */
	protected static long durationDeadlineNanos(
					final long boundaryNanos, final long budgetNanos) {
		return DurationTime.deadlineAfter(boundaryNanos, budgetNanos);
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
