package com.dell.spt.base.load.step.local;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.apache.logging.log4j.CloseableThreadContext.Instance;
import static org.apache.logging.log4j.CloseableThreadContext.put;

import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import com.dell.spt.base.load.step.DurationAwaitStatus;
import com.dell.spt.base.load.step.DurationTime;
import com.dell.spt.base.load.step.LoadStepBase;
import com.dell.spt.base.load.step.local.context.LoadStepContext;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.NoSuchElementException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import org.apache.logging.log4j.Level;

public abstract class LoadStepLocalBase extends LoadStepBase {

	private static final long STEP_CONTEXT_POLL_NANOS = 10_000_000L; // 10ms

	protected final List<LoadStepContext> stepContexts = new ArrayList<>();
	private volatile List<LoadStepContext> expectedDeleteObjectLifecycleContexts;
	private volatile IntegrityTerminalException durationStopFailure;
	private volatile IntegrityTerminalException durationValidityFailure;
	private volatile boolean durationAdmissionBarrierSatisfied;
	private boolean durationCleanupRetryPending;
	private boolean durationTerminalValidationCompleted;
	private boolean durationTerminalValidationDelegated;
	private volatile long durationDrainDeadlineNanos = Long.MIN_VALUE;
	private volatile boolean durationDrainDeadlineSet;
	private volatile long durationAwaitDeadlineNanos = Long.MIN_VALUE;
	private volatile boolean durationIntervalArmed;
	private long durationIntervalNanos = Long.MIN_VALUE;
	private volatile DurationAwaitStatus durationAwaitStatus = DurationAwaitStatus.NOT_STARTED;
	private volatile Thread durationDeadlineGuard;
	private volatile Thread durationDrainDeadlineGuard;
	private final Object durationAdmissionClose = new Object();
	private final Object durationDeadlineEnforcement = new Object();
	private volatile boolean durationAdmissionClosedLocally;

	protected IntSupplier actualConcurrencyGauge(final int index, final OpType opType) {
		return () -> stepContexts.get(index).activeOpCount();
	}

	protected LoadStepLocalBase(
					final Config baseConfig,
					final List<Extension> extensions,
					final List<Config> contextConfigs,
					final MetricsManager metricsManager) {
		super(baseConfig, extensions, contextConfigs, metricsManager);
	}

	@Override
	public final DeleteObjectLifecycleSnapshot deleteObjectLifecycle() {
		if (!standaloneDeleteEnabled()) {
			return null;
		}
		final List<LoadStepContext> currentContexts = List.copyOf(stepContexts);
		final List<LoadStepContext> expectedContexts = expectedDeleteObjectLifecycleContexts;
		if (expectedContexts != null && !expectedContexts.equals(currentContexts)) {
			return null;
		}
		long selected = 0;
		long attempted = 0;
		long accepted = 0;
		long failed = 0;
		long unattempted = 0;
		long unresolved = 0;
		long protocolFailed = 0;
		long fullSuccessfulRequests = 0;
		boolean reconciled = true;
		for (final LoadStepContext context : currentContexts) {
			if (context == null) {
				continue;
			}
			final DeleteObjectLifecycleSnapshot snapshot = context.deleteObjectLifecycle();
			if (snapshot == null) {
				return null;
			}
			selected = Math.addExact(selected, snapshot.selected());
			attempted = Math.addExact(attempted, snapshot.attempted());
			accepted = Math.addExact(accepted, snapshot.accepted());
			failed = Math.addExact(failed, snapshot.failed());
			unattempted = Math.addExact(unattempted, snapshot.unattempted());
			unresolved = Math.addExact(unresolved, snapshot.unresolved());
			protocolFailed = Math.addExact(protocolFailed, snapshot.protocolFailed());
			fullSuccessfulRequests = Math.addExact(
							fullSuccessfulRequests, snapshot.fullSuccessfulRequests());
			reconciled &= snapshot.reconciled();
		}
		return new DeleteObjectLifecycleSnapshot(
						selected,
						attempted,
						accepted,
						failed,
						unattempted,
						unresolved,
						protocolFailed,
						fullSuccessfulRequests,
						reconciled);
	}

	@Override
	protected void doStartWrapped() {
		resetDurationLifecycleForStart();
		if (standaloneDeleteEnabled()) {
			expectedDeleteObjectLifecycleContexts = List.copyOf(stepContexts);
		}
		if (standaloneDeleteEnabled() && !standaloneDeleteDurationMode()) {
			for (final LoadStepContext stepContext : List.copyOf(stepContexts)) {
				if (stepContext != null) {
					stepContext.holdObjectFailureBudgetAdmission();
				}
			}
		}
		boolean anyStarted = false;
		final var iterator = stepContexts.iterator();
		while (iterator.hasNext()) {
			final var stepCtx = iterator.next();
			try {
				stepCtx.start();
				anyStarted = true;
			} catch (final IllegalStateException e) {
				final var terminal = IntegrityTerminalException.find(e);
				if (terminal != null) {
					throw terminal;
				}
				LogUtil.exception(
								Level.WARN, e, "{}: failed to start the load step context \"{}\"", loadStepId(), stepCtx);
				iterator.remove();
			} catch (final RemoteException e) {
				LogUtil.exception(
								Level.WARN, e, "{}: failed to start the load step context \"{}\"", loadStepId(), stepCtx);
				iterator.remove();
			}
		}
		if (!anyStarted) {
			throw new IllegalStateException(loadStepId() + ": failed to start any load step contexts");
		}
	}

	@Override
	public final void releaseObjectFailureBudgetAdmission() {
		if (!standaloneDeleteEnabled() || standaloneDeleteDurationMode()) {
			return;
		}
		invokeDurationContextPhase(
						"release object failure-budget admission",
						LoadStepContext::releaseObjectFailureBudgetAdmission,
						true);
	}

	private void resetDurationLifecycleForStart() {
		if (standaloneDeleteDurationMode()) {
			cancelDurationDeadlineGuard();
			cancelDurationDrainDeadlineGuard();
			durationStopFailure = null;
			durationValidityFailure = null;
			durationAdmissionBarrierSatisfied = false;
			durationCleanupRetryPending = false;
			durationTerminalValidationCompleted = false;
			durationTerminalValidationDelegated = false;
			durationDrainDeadlineNanos = Long.MIN_VALUE;
			durationDrainDeadlineSet = false;
			durationAwaitDeadlineNanos = Long.MIN_VALUE;
			durationIntervalArmed = false;
			durationIntervalNanos = Long.MIN_VALUE;
			durationAwaitStatus = DurationAwaitStatus.NOT_STARTED;
			durationAdmissionClosedLocally = false;
		}
	}

	@Override
	public final synchronized void prepareDurationInterval(final long requestedDurationNanos) {
		if (!standaloneDeleteDurationMode()) {
			return;
		}
		if (requestedDurationNanos <= 0) {
			throw new IllegalArgumentException("duration interval must be positive");
		}
		if (durationIntervalNanos != Long.MIN_VALUE
						&& durationIntervalNanos != requestedDurationNanos) {
			throw new IllegalStateException("duration interval was already prepared with a different value");
		}
		durationIntervalNanos = requestedDurationNanos;
	}

	@Override
	public final void startDurationInterval(final long requestedDurationNanos) {
		final long startNanos;
		final long deadlineNanos;
		synchronized (this) {
			if (!standaloneDeleteDurationMode()) {
				return;
			}
			prepareDurationInterval(requestedDurationNanos);
			if (durationIntervalArmed) {
				if (durationIntervalNanos != requestedDurationNanos) {
					throw new IllegalStateException("duration interval was already armed with a different value");
				}
				return;
			}
			startNanos = System.nanoTime();
			deadlineNanos = durationDeadlineNanos(startNanos, requestedDurationNanos);
			durationAwaitDeadlineNanos = deadlineNanos;
			durationDrainDeadlineNanos = durationDeadlineNanos(deadlineNanos, durationDrainBudgetNanos());
			durationDrainDeadlineSet = true;
			durationIntervalArmed = true;
			durationAwaitStatus = DurationAwaitStatus.RUNNING;
			startDurationDeadlineGuard(deadlineNanos);
			startDurationDrainDeadlineGuard(durationDrainDeadlineNanos);
		}
		try {
			invokeDurationContextPhase(
							"start duration interval",
							context -> context.startDurationInterval(startNanos, deadlineNanos));
		} catch (final RuntimeException failure) {
			durationAwaitStatus = DurationAwaitStatus.FAILED;
			throw failure;
		}
	}

	private void startDurationDeadlineGuard(final long deadlineNanos) {
		final Thread deadlineGuard = Thread.ofPlatform()
						.daemon()
						.name("spt-delete-deadline-" + loadStepId())
						.unstarted(() -> {
							try {
								final long remainingNanos = DurationTime.remainingNanos(
												deadlineNanos, System.nanoTime());
								if (remainingNanos > 0) {
									TimeUnit.NANOSECONDS.sleep(remainingNanos);
								}
								closeOperationAdmissionAtWorkerDeadline();
							} catch (final InterruptedException interrupted) {
								Thread.currentThread().interrupt();
							} catch (final RuntimeException failure) {
								durationAwaitStatus = DurationAwaitStatus.FAILED;
								LogUtil.exception(
												Level.ERROR,
												failure,
												"{}: failed to close standalone DELETE admission at the worker deadline",
												loadStepId());
							}
						});
		durationDeadlineGuard = deadlineGuard;
		deadlineGuard.start();
	}

	private void closeOperationAdmissionAtWorkerDeadline() {
		synchronized (this) {
			if (durationDeadlineGuard != Thread.currentThread()) {
				return;
			}
		}
		closeOperationAdmissionForStepStop();
	}

	private void cancelDurationDeadlineGuard() {
		final Thread deadlineGuard = durationDeadlineGuard;
		durationDeadlineGuard = null;
		if (deadlineGuard != null && deadlineGuard != Thread.currentThread()) {
			deadlineGuard.interrupt();
		}
	}

	private synchronized void startDurationDrainDeadlineGuard(final long deadlineNanos) {
		if (durationDrainDeadlineGuard != null) {
			return;
		}
		final Thread deadlineGuard = Thread.ofPlatform()
						.daemon()
						.name("spt-delete-drain-deadline-" + loadStepId())
						.unstarted(() -> {
							try {
								final long remainingNanos = DurationTime.remainingNanos(
												deadlineNanos, System.nanoTime());
								if (remainingNanos > 0) {
									TimeUnit.NANOSECONDS.sleep(remainingNanos);
								}
								expireDispatchedOperationsAtDrainDeadline();
							} catch (final InterruptedException interrupted) {
								Thread.currentThread().interrupt();
							} catch (final RuntimeException failure) {
								durationAwaitStatus = DurationAwaitStatus.FAILED;
								LogUtil.exception(
												Level.ERROR,
												failure,
												"{}: failed to enforce the standalone DELETE drain deadline",
												loadStepId());
							}
						});
		durationDrainDeadlineGuard = deadlineGuard;
		deadlineGuard.start();
	}

	private synchronized void expireDispatchedOperationsAtDrainDeadline() {
		if (durationDrainDeadlineGuard != Thread.currentThread()) {
			return;
		}
		for (final LoadStepContext context : List.copyOf(stepContexts)) {
			if (context != null) {
				context.expireDispatchedOperationsDeadlineForStepStop();
			}
		}
	}

	private void cancelDurationDrainDeadlineGuard() {
		final Thread deadlineGuard = durationDrainDeadlineGuard;
		durationDrainDeadlineGuard = null;
		if (deadlineGuard != null && deadlineGuard != Thread.currentThread()) {
			deadlineGuard.interrupt();
		}
	}

	@Override
	protected final synchronized void cancelDurationRunCleanupResourcesAfterExhaustion() {
		cancelDurationDeadlineGuard();
		cancelDurationDrainDeadlineGuard();
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
						.actualConcurrencyGauge(actualConcurrencyGauge(index, opType))
						.concurrencyLimit(concurrency)
						.concurrencyThreshold((int) (concurrency * metricsConfig.doubleVal("threshold")))
						.itemDataSize(itemDataSize)
						.outputPeriodSec(avgPeriod(metricsConfig))
						.stdOutColorFlag(outputColorFlag)
						.comment(config.stringVal("run-comment"))
						.runId(runId())
						.build();

		// enrich metadata with limits so /metrics/json can compute completion on workers
		boolean countLimitConfigured = false;
		boolean countLimitApplied = false;
		try {
			final long countLimit = this.config.longVal("load-op-limit-count");
			countLimitConfigured = true;
			metricsCtx.metadata().put(MetricsConstants.METADATA_LIMIT_OP_COUNT, countLimit);
			countLimitApplied = true;
		} catch (final NoSuchElementException ignore) {
			// count limit not defined is a valid configuration for unbounded steps
		} catch (final RuntimeException e) {
			countLimitConfigured = true;
			Loggers.MSG.warn(
							"{}: ignoring invalid load-op-limit-count value; treating as unlimited",
							loadStepId(),
							e);
		}
		boolean timeLimitConfigured = false;
		boolean timeLimitApplied = false;
		try {
			final Object raw = this.config.val("load-step-limit-time");
			timeLimitConfigured = true;
			long timeLimitSec = 0L;
			if (raw instanceof String) {
				timeLimitSec = com.dell.spt.base.config.TimeUtil.getTimeInSeconds((String) raw);
			} else if (raw != null) {
				timeLimitSec = com.github.akurilov.commons.reflection.TypeUtil.typeConvert(raw, long.class);
			}
			metricsCtx.metadata().put(MetricsConstants.METADATA_LIMIT_TIME_SEC, timeLimitSec);
			timeLimitApplied = true;
		} catch (final NoSuchElementException ignore) {
			// time limit not defined is a valid configuration for unbounded steps
		} catch (final RuntimeException e) {
			timeLimitConfigured = true;
			Loggers.MSG.warn(
							"{}: ignoring invalid load-step-limit-time value; treating as unlimited",
							loadStepId(),
							e);
		}
		if ((countLimitConfigured || timeLimitConfigured) && !(countLimitApplied || timeLimitApplied)) {
			Loggers.MSG.warn(
							"{}: load-step limits configured but none usable; treating as unlimited",
							loadStepId());
		}
		metricsContexts.add(metricsCtx);
	}

	@Override
	protected final void doShutdown() {
		if (standaloneDeleteDurationMode()) {
			prepareDurationStop();
			if (durationAdmissionBarrierSatisfied && durationStopFailure == null) {
				captureDurationStopFailure(
								"shutdown contexts",
								() -> invokeDurationContextPhase("shutdown", LoadStepContext::shutdown));
			}
			return;
		}
		final var iterator = stepContexts.iterator();
		while (iterator.hasNext()) {
			final var stepCtx = iterator.next();
			try (final Instance ctx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
				stepCtx.shutdown();
				Loggers.MSG.debug("{}: load step context shutdown", loadStepId());
			} catch (final RemoteException e) {
				LogUtil.exception(Level.WARN, e, "{}: failed to shutdown the load step context", loadStepId());
				iterator.remove();
			}
		}
	}

	@Override
	public final boolean await(final long timeout, final TimeUnit timeUnit)
					throws IllegalStateException {
		if (standaloneDeleteDurationMode()) {
			return awaitDurationContexts(timeout, timeUnit);
		}

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
										|| stepCtx.await(STEP_CONTEXT_POLL_NANOS, TimeUnit.NANOSECONDS)) {
							if (standaloneDeleteDurationMode()) {
								throw standaloneDeleteEarlyExhaustionFailure();
							}
							stepContextsCopy[i] = null; // exclude
							countDown--;
							break;
						}
					} catch (final InterruptedException e) {
						throwUnchecked(e);
					} catch (final RemoteException e) {
						if (standaloneDeleteDurationMode()) {
							throw terminalFailure(
											IntegrityTerminalException.Category.EXECUTION,
											"Standalone DELETE duration run lost an input slice before the deadline",
											e);
						}
						stepContextsCopy[i] = null; // exclude failed context
						stepContexts.remove(stepCtx);
						countDown--;
						break;
					}
				}
			}
		}

		return 0 == countDown;
	}

	private boolean awaitDurationContexts(final long timeout, final TimeUnit timeUnit) {
		final boolean pollBounded = timeout > 0;
		final long timeoutNanos = timeout > 0 ? timeUnit.toNanos(timeout) : Long.MAX_VALUE;
		if (!durationIntervalArmed) {
			startDurationInterval(timeoutNanos);
		}
		final long deadlineNanos = durationAwaitDeadlineNanos;
		final long pollDeadlineNanos = !pollBounded
						? Long.MAX_VALUE
						: durationDeadlineNanos(timeoutNanos);
		try {
			while (true) {
				final long observedNanos = System.nanoTime();
				if (DurationTime.deadlineReached(deadlineNanos, observedNanos)) {
					return completeDurationDeadlineAudit();
				}
				if (pollBounded
								&& DurationTime.deadlineReached(pollDeadlineNanos, observedNanos)) {
					return false;
				}
				for (final LoadStepContext context : List.copyOf(stepContexts)) {
					if (context == null) {
						continue;
					}
					final OptionalLong exhaustion = schedulingExhaustionNanos(context);
					final long exhaustedAtNanos;
					if (exhaustion.isEmpty()) {
						if (!context.isDone()) {
							continue;
						}
						exhaustedAtNanos = System.nanoTime();
					} else {
						exhaustedAtNanos = exhaustion.getAsLong();
					}
					if (DurationTime.timestampBeforeDeadline(exhaustedAtNanos, deadlineNanos)) {
						durationAwaitStatus = DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE;
						throw standaloneDeleteEarlyExhaustionFailure();
					}
					durationAwaitStatus = DurationAwaitStatus.REACHED_DEADLINE;
					return false;
				}
				long remainingNanos = DurationTime.remainingNanos(
								deadlineNanos, System.nanoTime());
				if (pollBounded) {
					remainingNanos = Math.min(
									remainingNanos,
									DurationTime.remainingNanos(pollDeadlineNanos, System.nanoTime()));
				}
				if (remainingNanos == 0) {
					return completeDurationDeadlineAudit();
				}
				try {
					TimeUnit.NANOSECONDS.sleep(Math.min(STEP_CONTEXT_POLL_NANOS, remainingNanos));
				} catch (final InterruptedException e) {
					throwUnchecked(e);
				}
			}
		} catch (final RuntimeException failure) {
			if (durationAwaitStatus != DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE) {
				durationAwaitStatus = DurationAwaitStatus.FAILED;
			}
			throw failure;
		}
	}

	private boolean completeDurationDeadlineAudit() {
		final DurationAwaitStatus status = durationAwaitStatus();
		if (status == DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE) {
			throw standaloneDeleteEarlyExhaustionFailure();
		}
		if (status == DurationAwaitStatus.FAILED) {
			throw terminalFailure(
							IntegrityTerminalException.Category.EXECUTION,
							"Standalone DELETE duration validity could not be established on a local input slice",
							null);
		}
		return false;
	}

	@Override
	public final synchronized DurationAwaitStatus durationAwaitStatus() {
		if (durationAwaitStatus == DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE
						|| durationAwaitStatus == DurationAwaitStatus.FAILED) {
			return durationAwaitStatus;
		}
		final long deadlineNanos = durationAwaitDeadlineNanos;
		if (!durationIntervalArmed) {
			return durationAwaitStatus;
		}
		for (final LoadStepContext context : List.copyOf(stepContexts)) {
			if (context == null) {
				continue;
			}
			final OptionalLong exhaustion = schedulingExhaustionNanos(context);
			if (exhaustion.isPresent()
							&& DurationTime.timestampBeforeDeadline(exhaustion.getAsLong(), deadlineNanos)) {
				durationAwaitStatus = DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE;
				return durationAwaitStatus;
			}
		}
		if (DurationTime.deadlineReached(deadlineNanos, System.nanoTime())) {
			durationAwaitStatus = DurationAwaitStatus.REACHED_DEADLINE;
		}
		return durationAwaitStatus;
	}

	private static OptionalLong schedulingExhaustionNanos(final LoadStepContext context) {
		final OptionalLong explicitExhaustion = context.schedulingExhaustionNanos();
		if (explicitExhaustion.isPresent()) {
			return explicitExhaustion;
		}
		final long legacyExhaustion = context.schedulingExhaustedAtNanos();
		return legacyExhaustion == Long.MAX_VALUE
						? OptionalLong.empty()
						: OptionalLong.of(legacyExhaustion);
	}

	@Override
	protected final void doStop() {
		if (standaloneDeleteDurationMode()) {
			if (durationAdmissionBarrierSatisfied && durationStopFailure == null) {
				captureDurationStopFailure(
								"stop contexts",
								() -> invokeDurationContextPhase("stop", context -> context.stop()));
			}
		} else {
			stepContexts.forEach(LoadStepContext::stop);
		}
		super.doStop();
	}

	private void prepareDurationStop() {
		durationAdmissionBarrierSatisfied = captureDurationStopFailure(
						"close operation admission",
						this::closeOperationAdmissionForStepStop);
		if (!durationAdmissionBarrierSatisfied) {
			return;
		}
		if (!durationDrainDeadlineSet) {
			// Compatibility for an explicit stop before a duration interval was armed.
			durationDrainDeadlineNanos = durationDeadlineNanos(durationDrainBudgetNanos());
			durationDrainDeadlineSet = true;
		}
		if (!captureDurationStopFailure(
						"recover queued operations",
						this::recoverQueuedOperationsForStepStop)) {
			return;
		}
		if (!captureDurationStopFailure(
						"drain dispatched operations",
						() -> drainDispatchedOperationsForStepStop(
										DurationTime.remainingNanos(
														durationDrainDeadlineNanos, System.nanoTime())))) {
			return;
		}
		captureDurationTerminalValidation();
	}

	@Override
	public final void closeOperationAdmissionForStepStop() {
		synchronized (durationAdmissionClose) {
			if (durationAdmissionClosedLocally) {
				return;
			}
			invokeDurationContextPhase(
							"close operation admission",
							LoadStepContext::closeOperationAdmissionForStepStop,
							true);
			cancelDurationDeadlineGuard();
			final long drainDeadlineNanos;
			synchronized (this) {
				if (!durationDrainDeadlineSet) {
					durationDrainDeadlineNanos = durationDeadlineNanos(durationDrainBudgetNanos());
					durationDrainDeadlineSet = true;
				}
				drainDeadlineNanos = durationDrainDeadlineNanos;
				startDurationDrainDeadlineGuard(drainDeadlineNanos);
			}
			enforceDispatchedOperationsDeadlineForStepStop(drainDeadlineNanos, false);
			durationAdmissionClosedLocally = true;
		}
	}

	@Override
	public final void enforceDispatchedOperationsDeadlineForStepStop(
					final long remainingNanos) {
		final long selectedDeadlineNanos;
		synchronized (this) {
			final long observedNanos = System.nanoTime();
			final long requestedDeadlineNanos = durationDeadlineNanos(observedNanos, remainingNanos);
			final long previousDeadlineNanos = durationDrainDeadlineNanos;
			selectedDeadlineNanos = !durationDrainDeadlineSet
							? requestedDeadlineNanos
							: DurationTime.earlierDeadline(
											previousDeadlineNanos,
											requestedDeadlineNanos,
											observedNanos);
			if (!durationDrainDeadlineSet || selectedDeadlineNanos != previousDeadlineNanos) {
				cancelDurationDrainDeadlineGuard();
				durationDrainDeadlineNanos = selectedDeadlineNanos;
				durationDrainDeadlineSet = true;
			}
			startDurationDrainDeadlineGuard(durationDrainDeadlineNanos);
		}
		enforceDispatchedOperationsDeadlineForStepStop(selectedDeadlineNanos, true);
	}

	private void enforceDispatchedOperationsDeadlineForStepStop(
					final long deadlineNanos, final boolean controllerSupplied) {
		synchronized (durationDeadlineEnforcement) {
			invokeDurationContextPhase(
							controllerSupplied
											? "tighten dispatched operation deadline"
											: "arm dispatched operation deadline",
							context -> context.enforceDispatchedOperationsDeadlineForStepStop(deadlineNanos));
		}
	}

	@Override
	public final void recoverQueuedOperationsForStepStop() {
		invokeDurationContextPhase(
						"recover queued operations",
						LoadStepContext::recoverQueuedOperationsForStepStop);
	}

	@Override
	public final void drainDispatchedOperationsForStepStop(final long remainingNanos) {
		final long requestedDeadlineNanos = durationDeadlineNanos(remainingNanos);
		final long workerDeadlineNanos = durationDrainDeadlineNanos;
		final long drainDeadlineNanos = !durationDrainDeadlineSet
						? requestedDeadlineNanos
						: DurationTime.earlierDeadline(
										requestedDeadlineNanos,
										workerDeadlineNanos,
										System.nanoTime());
		invokeDurationContextPhase(
						"drain dispatched operations",
						durationStopPhaseDeadlineNanos(),
						context -> context.drainDispatchedOperationsForStepStop(drainDeadlineNanos));
		cancelDurationDrainDeadlineGuard();
	}

	@Override
	public final synchronized void validateTerminalStateForStepStop() {
		durationTerminalValidationDelegated = true;
		invokeDurationContextPhase(
						"validate terminal state", LoadStepContext::validateTerminalState);
	}

	private synchronized void captureDurationTerminalValidation() {
		if (durationTerminalValidationCompleted || durationTerminalValidationDelegated) {
			return;
		}
		try {
			invokeDurationContextPhase(
							"validate terminal state", LoadStepContext::validateTerminalState);
		} catch (final RuntimeException cause) {
			durationValidityFailure = appendTerminalFailure(
							durationValidityFailure,
							IntegrityTerminalException.Category.EXECUTION,
							"Standalone DELETE terminal accounting failed after the local drain",
							cause);
		} finally {
			durationTerminalValidationCompleted = true;
		}
	}

	private void invokeDurationContextPhase(
					final String phase, final DurationContextPhase action) {
		invokeDurationContextPhase(phase, durationStopPhaseDeadlineNanos(), action, false);
	}

	private void invokeDurationContextPhase(
					final String phase,
					final DurationContextPhase action,
					final boolean starvationSafe) {
		invokeDurationContextPhase(phase, durationStopPhaseDeadlineNanos(), action, starvationSafe);
	}

	private void invokeDurationContextPhase(
					final String phase,
					final long deadlineNanos,
					final DurationContextPhase action) {
		invokeDurationContextPhase(phase, deadlineNanos, action, false);
	}

	private void invokeDurationContextPhase(
					final String phase,
					final long deadlineNanos,
					final DurationContextPhase action,
					final boolean starvationSafe) {
		final List<LoadStepContext> activeContexts = stepContexts.stream()
						.filter(Objects::nonNull)
						.toList();
		if (activeContexts.isEmpty()) {
			return;
		}
		final DurationPhaseAttempt result = invokeRetainedDurationPhase(
						"local-" + phase,
						activeContexts,
						action::execute,
						deadlineNanos,
						"spt-delete-context-" + (starvationSafe ? "admission-" : "stop-"));
		IntegrityTerminalException phaseFailure = null;
		for (final Throwable failure : result.failures()) {
			phaseFailure = appendTerminalFailure(
							phaseFailure,
							IntegrityTerminalException.Category.EXECUTION,
							"Standalone DELETE failed to " + phase + " across local input contexts",
							failure);
		}
		if (!result.completedAll() && phaseFailure == null) {
			phaseFailure = appendTerminalFailure(
							phaseFailure,
							IntegrityTerminalException.Category.EXECUTION,
							"Standalone DELETE timed out while awaiting local " + phase,
							new java.util.concurrent.TimeoutException(
											"local duration stop phase exceeded its deadline"));
		}
		if (phaseFailure != null) {
			throw phaseFailure;
		}
	}

	private boolean captureDurationStopFailure(final String phase, final Runnable action) {
		try {
			action.run();
			return true;
		} catch (final RuntimeException cause) {
			recordDurationStopFailure(phase, cause);
			return false;
		}
	}

	private synchronized void recordDurationStopFailure(final String phase, final Throwable cause) {
		durationStopFailure = appendTerminalFailure(
						durationStopFailure,
						IntegrityTerminalException.Category.CLEANUP,
						"Standalone DELETE failed to " + phase + " during local cleanup",
						cause);
	}

	@FunctionalInterface
	private interface DurationContextPhase {
		void execute(LoadStepContext context) throws Exception;
	}

	private long durationDrainBudgetNanos() {
		return TimeUnit.SECONDS.toNanos(Math.max(0, config.intVal("load-op-wait-limit")));
	}

	@Override
	protected final void doClose() throws IOException {
		if (standaloneDeleteDurationMode()) {
			closeDurationContexts();
			return;
		}
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
		RuntimeException terminalFailure = null;
		for (final var stepContext : stepContexts) {
			try {
				stepContext.validateTerminalState();
			} catch (final RuntimeException failure) {
				if (terminalFailure == null) {
					terminalFailure = failure;
				} else {
					terminalFailure.addSuppressed(failure);
				}
			}
		}
		stepContexts.clear();
		if (terminalFailure != null) {
			throw terminalFailure;
		}
	}

	private void closeDurationContexts() {
		boolean durationStopRecoordinated = false;
		if (durationCleanupRetryPending) {
			// The prior close surfaced its sticky failure. Start a new explicit cleanup
			// attempt without duplicating any still-running retained phase invocation.
			durationStopFailure = null;
			durationAdmissionBarrierSatisfied = false;
		} else if (durationStopFailure != null || !durationAdmissionBarrierSatisfied) {
			durationCleanupRetryPending = true;
			if (durationStopFailure == null) {
				recordDurationStopFailure(
								"close operation admission",
								new IllegalStateException("operation admission closure was not confirmed"));
			}
			throw durationStopFailure;
		}
		if (durationCleanupRetryPending) {
			prepareDurationStop();
			if (!durationAdmissionBarrierSatisfied || durationStopFailure != null) {
				durationCleanupRetryPending = true;
				throw durationStopFailure;
			}
			durationStopRecoordinated = true;
		}
		if (durationStopRecoordinated) {
			captureDurationStopFailure(
							"shutdown contexts",
							() -> invokeDurationContextPhase("shutdown", LoadStepContext::shutdown));
			if (durationStopFailure == null) {
				doStop();
			}
			if (durationStopFailure != null) {
				durationCleanupRetryPending = true;
				throw durationStopFailure;
			}
		}
		try {
			closeMetricsContexts();
		} catch (final RuntimeException cause) {
			recordDurationStopFailure("close metrics contexts", cause);
		}
		boolean contextsClosed = true;
		if (durationAdmissionBarrierSatisfied) {
			contextsClosed &= captureDurationStopFailure(
							"close contexts",
							() -> invokeDurationContextPhase("close", LoadStepContext::close));
		}
		if (contextsClosed && durationStopFailure == null) {
			stepContexts.clear();
		}
		final var terminalFailure = durationStopFailure;
		if (terminalFailure != null) {
			durationCleanupRetryPending = true;
			throw terminalFailure;
		}
		durationCleanupRetryPending = false;
		cancelDurationDrainDeadlineGuard();
		closeRetainedDurationPhases();
		if (durationValidityFailure != null) {
			throw durationValidityFailure;
		}
	}
}
