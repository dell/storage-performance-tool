package com.dell.spt.base.load.step.service;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static org.apache.logging.log4j.CloseableThreadContext.Instance;
import static org.apache.logging.log4j.CloseableThreadContext.put;

import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.DurationAwaitStatus;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.PreparationStatus;
import com.dell.spt.base.load.step.RunStepIds;
import com.dell.spt.base.load.step.local.LoadStepLocalBase;
import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.svc.ServiceBase;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.rmi.RemoteException;
import com.dell.spt.base.metrics.bucket.BucketMetricsRange;
import com.dell.spt.base.metrics.bucket.BucketMetricsReply;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import org.apache.logging.log4j.Level;

/**
 * A worker's share of a step. Nothing else on the worker ends a slice whose entry has gone, so a
 * daemon checker stops and closes a started slice after {@value #ENTRY_SILENCE_TIMEOUT_MILLIS} ms
 * with no call from its entry and none in flight.
 *
 * <p>The checker is armed by the first call that begins after the start has returned: the entry
 * starts its slices one at a time and calls none of them until all have started. Every remote
 * method this class overrides counts as a call, except {@link #name()}, which the service's own
 * registration also reads, and {@link #close()}, which stops the slice first. The worker's own
 * reads go through {@link #sliceRunId()} and {@link #sliceStepId()}, so its {@code /run} API does
 * not keep an abandoned slice alive.
 */
public final class LoadStepServiceImpl extends ServiceBase implements LoadStepService {

	private static final long ENTRY_SILENCE_TIMEOUT_MILLIS = 120_000;
	/** For tests only: not a configuration option. */
	static final String ENTRY_SILENCE_TIMEOUT_PROPERTY = "spt.step.entrySilenceTimeoutMillis";
	private static final long ENTRY_CHECK_PERIOD_NANOS = TimeUnit.SECONDS.toNanos(1);

	/** Ends one entry call. A single instance serves every call, so a call allocates nothing. */
	@FunctionalInterface
	private interface EntryCall extends AutoCloseable {
		@Override
		void close();
	}

	private final LoadStep localLoadStep;
	private final long entrySilenceTimeoutNanos;
	private final long entryCheckPeriodNanos;
	private final LongSupplier nanoClock;
	private final AtomicInteger entryCallsInFlight = new AtomicInteger();
	private final EntryCall entryCallEnd = this::endEntryCall;
	private final AtomicBoolean entryGivenUp = new AtomicBoolean();
	private volatile long lastEntryCallNanos;
	private volatile boolean entryWatched;
	private volatile Thread entryChecker;
	private CompletableFuture<Void> durationDrainTask;
	private final Consumer<LoadStepServiceImpl> onRelease;
	private volatile boolean released;
	private volatile boolean running;

	public LoadStepServiceImpl(
					final int port,
					final List<Extension> extensions,
					final String stepType,
					final Config baseConfig,
					final List<Config> ctxConfigs,
					final MetricsManager metricsManager) {
		this(port, extensions, stepType, baseConfig, ctxConfigs, metricsManager, service -> {});
	}

	/** @param onRelease told once, after this service has released its slice and left the registry */
	LoadStepServiceImpl(
					final int port,
					final List<Extension> extensions,
					final String stepType,
					final Config baseConfig,
					final List<Config> ctxConfigs,
					final MetricsManager metricsManager,
					final Consumer<LoadStepServiceImpl> onRelease) {
		this(port, extensions, stepType, baseConfig, ctxConfigs, metricsManager, onRelease, null);
	}

	/**
	 * @param onRelease told once, after this service has released its slice and left the registry
	 * @param runStepIds the worker's record of the step ids each run started there, with which the slice
	 *     moves aside the files an earlier run left under its id; without it, it refuses them
	 */
	LoadStepServiceImpl(
					final int port,
					final List<Extension> extensions,
					final String stepType,
					final Config baseConfig,
					final List<Config> ctxConfigs,
					final MetricsManager metricsManager,
					final Consumer<LoadStepServiceImpl> onRelease,
					final RunStepIds runStepIds) {
		this(
						port, extensions, stepType, baseConfig, ctxConfigs, metricsManager, onRelease, runStepIds,
						TimeUnit.MILLISECONDS.toNanos(entrySilenceTimeoutMillis()), ENTRY_CHECK_PERIOD_NANOS, System::nanoTime);
	}

	LoadStepServiceImpl(
					final int port,
					final List<Extension> extensions,
					final String stepType,
					final Config baseConfig,
					final List<Config> ctxConfigs,
					final MetricsManager metricsManager,
					final Consumer<LoadStepServiceImpl> onRelease,
					final long entrySilenceTimeoutNanos,
					final long entryCheckPeriodNanos,
					final LongSupplier nanoClock) {
		this(
						port, extensions, stepType, baseConfig, ctxConfigs, metricsManager, onRelease, null,
						entrySilenceTimeoutNanos, entryCheckPeriodNanos, nanoClock);
	}

	LoadStepServiceImpl(
					final int port,
					final List<Extension> extensions,
					final String stepType,
					final Config baseConfig,
					final List<Config> ctxConfigs,
					final MetricsManager metricsManager,
					final Consumer<LoadStepServiceImpl> onRelease,
					final RunStepIds runStepIds,
					final long entrySilenceTimeoutNanos,
					final long entryCheckPeriodNanos,
					final LongSupplier nanoClock) {
		super(port);
		this.onRelease = onRelease;
		this.entrySilenceTimeoutNanos = entrySilenceTimeoutNanos;
		this.entryCheckPeriodNanos = entryCheckPeriodNanos;
		this.nanoClock = nanoClock;
		baseConfig.val(
						"load-step-idAutoGenerated",
						false); // don't override the step-id value on the remote node again
		localLoadStep = LoadStepFactory.createLocalLoadStep(
						baseConfig, extensions, ctxConfigs, metricsManager, stepType);
		if (runStepIds != null && localLoadStep instanceof LoadStepLocalBase workerSlice) {
			workerSlice.startOnWorker(runStepIds);
		}
		final String stepId = baseConfig.stringVal("load-step-id");
		try (final Instance logCtx = put(KEY_STEP_ID, stepId).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			Loggers.MSG.info("New step service for type \"{}\"", stepType);
			super.doStart();
		}
	}

	static long entrySilenceTimeoutMillis() {
		final long millis = Long.getLong(ENTRY_SILENCE_TIMEOUT_PROPERTY, ENTRY_SILENCE_TIMEOUT_MILLIS);
		return millis > 0 ? millis : ENTRY_SILENCE_TIMEOUT_MILLIS;
	}

	@Override
	protected final void doStart() {
		try (
						final EntryCall call = entryCall();
						final Instance logCtx = put(KEY_CLASS_NAME, getClass().getSimpleName()).put(KEY_STEP_ID, safeLoadStepId())) {
			localLoadStep.start();
			entryWatched = false;
			entryGivenUp.set(false);
			running = true;
			startEntryChecker();
			Loggers.MSG.info("Step service for \"{}\" is started", safeLoadStepId());
		} catch (final RemoteException e) {
			Loggers.ERR.warn("Step service start failure for {}", safeLoadStepId(), e);
			throw new IllegalStateException("Failed to start load step service " + safeLoadStepId(), e);
		}
	}

	@Override
	protected void doShutdown() {
		try (
						final EntryCall call = entryCall();
						final Instance logCtx = put(KEY_CLASS_NAME, getClass().getSimpleName()).put(KEY_STEP_ID, safeLoadStepId())) {
			localLoadStep.shutdown();
			Loggers.MSG.debug("Step service for \"{}\" is shutdown", safeLoadStepId());
		} catch (final RemoteException e) {
			Loggers.ERR.warn("Step service shutdown failure for {}", safeLoadStepId(), e);
			throw new IllegalStateException("Failed to shutdown load step service " + safeLoadStepId(), e);
		}
	}

	@Override
	protected void doStop() {
		try (
						final EntryCall call = entryCall();
						final Instance logCtx = put(KEY_CLASS_NAME, getClass().getSimpleName()).put(KEY_STEP_ID, safeLoadStepId())) {
			localLoadStep.stop();
			running = false;
			Loggers.MSG.info("Step service for \"{}\" is stopped", safeLoadStepId());
		} catch (final RemoteException e) {
			Loggers.ERR.warn("Step service stop failure for {}", safeLoadStepId(), e);
			throw new IllegalStateException("Failed to stop load step service " + safeLoadStepId(), e);
		} catch (final RuntimeException e) {
			// The slice's contexts have had their stop; a failure to publish its files must not leave it busy.
			running = false;
			throw e;
		}
	}

	/** True from a successful start until the stop, which every close runs first. */
	boolean isRunning() {
		return running;
	}

	/** The run id for this worker's own use: unlike {@link #runId()}, not a call from the entry. */
	long sliceRunId() throws RemoteException {
		return localLoadStep.runId();
	}

	/** The step id for this worker's own use: unlike {@link #loadStepId()}, not a call from the entry. */
	String sliceStepId() {
		return safeLoadStepId();
	}

	private EntryCall entryCall() {
		entryCallsInFlight.incrementAndGet();
		lastEntryCallNanos = nanoClock.getAsLong();
		// The state changes as the start returns, so the start's own call does not arm the checker.
		if (!entryWatched && (isStarted() || isShutdown())) {
			entryWatched = true;
		}
		return entryCallEnd;
	}

	// The time first: a checker that sees no call in flight then sees this call's end.
	private void endEntryCall() {
		lastEntryCallNanos = nanoClock.getAsLong();
		entryCallsInFlight.decrementAndGet();
	}

	/** One checker at a time. After it gave up on an entry a later start needs a new one. */
	private void startEntryChecker() {
		final Thread checker = entryChecker;
		if (checker == null || !checker.isAlive()) {
			entryChecker = Thread.ofPlatform()
							.daemon()
							.name("spt-entry-watchdog-" + safeLoadStepId())
							.start(this::watchEntry);
		}
	}

	private void watchEntry() {
		while (!released) {
			LockSupport.parkNanos(entryCheckPeriodNanos);
			if (!released && stopIfEntryIsSilent()) {
				return;
			}
		}
	}

	/**
	 * Stops and closes a slice whose entry has gone silent, once. The stop comes first and on its own
	 * because a close does not reach it for every slice, and either may fail without ending the worker.
	 * The entry is given up on before they run, so what they record as calls is never read. A slice
	 * whose close will be retried stays exported until the worker's shutdown.
	 *
	 * @return true if this call gave up on the entry
	 */
	boolean stopIfEntryIsSilent() {
		if (!entryWatched || !running || entryCallsInFlight.get() > 0) {
			return false;
		}
		final long silentNanos = nanoClock.getAsLong() - lastEntryCallNanos;
		if (silentNanos <= entrySilenceTimeoutNanos || !entryGivenUp.compareAndSet(false, true)) {
			return false;
		}
		final String stepId = safeLoadStepId();
		try (final Instance logCtx = put(KEY_CLASS_NAME, getClass().getSimpleName()).put(KEY_STEP_ID, stepId)) {
			Loggers.ERR.warn(
							"No call from the entry node for {} s: stopping and closing step \"{}\" on this worker",
							TimeUnit.NANOSECONDS.toSeconds(silentNanos),
							stepId);
			try {
				stop();
			} catch (final RuntimeException e) {
				LogUtil.exception(Level.WARN, e, "Failed to stop step \"{}\" after losing its entry", stepId);
			}
			try {
				close();
			} catch (final IOException | RuntimeException e) {
				LogUtil.exception(Level.WARN, e, "Failed to close step \"{}\" after losing its entry", stepId);
			}
		}
		return true;
	}

	/**
	 * A close with no stop before it, as at worker shutdown or after a lost stop call, stops first, as
	 * every close does. A local slice stops once (see {@code LoadStepLocalBase.doStop}), so after that
	 * stop fails the close's own stop passes and reaches the release below, which throws the stop's
	 * failure. A slice whose close will be retried, and a local step of another kind, close as before.
	 */
	@Override
	public final void close() throws IOException {
		RuntimeException stopFailure = null;
		if (localLoadStep instanceof LoadStepLocalBase local && !local.awaitsCleanupRetry()) {
			try {
				stop();
			} catch (final RuntimeException e) {
				stopFailure = e;
			}
		}
		super.close();
		if (stopFailure != null) {
			// The release did not throw it again; it must not be lost.
			throw stopFailure;
		}
	}

	/**
	 * Closes the local step, then unexports and unbinds this service: always after a successful close,
	 * and after a failed one unless the close will be retried (see {@link #cleanupRetryExpected()}).
	 * A released service releases nothing again.
	 */
	@Override
	protected final void doClose() throws IOException {
		if (released) {
			return;
		}
		try (final Instance logCtx = put(KEY_CLASS_NAME, getClass().getSimpleName()).put(KEY_STEP_ID, safeLoadStepId())) {
			var closed = false;
			try {
				localLoadStep.close();
				closed = true;
			} finally {
				if (closed || !cleanupRetryExpected()) {
					released = true;
					LockSupport.unpark(entryChecker);
					try {
						super.doStop();
					} finally {
						onRelease.accept(this);
					}
				}
			}
			Loggers.MSG.info("Step service for \"{}\" is closed", safeLoadStepId());
		}
	}

	/**
	 * A standalone DELETE in duration mode is closed again after a failed close, until its last retry,
	 * and the retry needs this endpoint. Any other local slice is released at its first close. A local
	 * step of another kind keeps the endpoint after a failed close, as before.
	 */
	private boolean cleanupRetryExpected() {
		return !(localLoadStep instanceof LoadStepLocalBase local) || local.awaitsCleanupRetry();
	}

	@Override
	public String name() {
		return SVC_NAME_PREFIX + hashCode();
	}

	@Override
	public final String loadStepId() throws RemoteException {
		try (final EntryCall call = entryCall()) {
			return localLoadStep.loadStepId();
		}
	}

	@Override
	public long runId()
					throws RemoteException {
		try (final EntryCall call = entryCall()) {
			return localLoadStep.runId();
		}
	}

	@Override
	public final String getTypeName() throws RemoteException {
		try (final EntryCall call = entryCall()) {
			return localLoadStep.getTypeName();
		}
	}

	@Override
	public final List<? extends AllMetricsSnapshot> metricsSnapshots() throws RemoteException {
		try (final EntryCall call = entryCall()) {
			return localLoadStep.metricsSnapshots();
		}
	}

	@Override
	public final DeleteObjectLifecycleSnapshot deleteObjectLifecycle() throws RemoteException {
		try (final EntryCall call = entryCall()) {
			return localLoadStep.deleteObjectLifecycle();
		}
	}

	@Override
	public final void releaseObjectFailureBudgetAdmission() throws RemoteException {
		try (final EntryCall call = entryCall()) {
			localLoadStep.releaseObjectFailureBudgetAdmission();
		}
	}

	@Override
	public final void startPreparation(final long preparationId, final long budgetNanos) throws RemoteException {
		try (final EntryCall call = entryCall()) {
			localLoadStep.startPreparation(preparationId, budgetNanos);
		}
	}

	@Override
	public final PreparationStatus preparationStatus(final long preparationId) throws RemoteException {
		try (final EntryCall call = entryCall()) {
			return localLoadStep.preparationStatus(preparationId);
		}
	}

	@Override
	public final boolean releasePreparedAdmission(final long preparationId) throws RemoteException {
		try (final EntryCall call = entryCall()) {
			return localLoadStep.releasePreparedAdmission(preparationId);
		}
	}

	@Override
	public final void fencePreparation(final long preparationId) throws RemoteException {
		try (final EntryCall call = entryCall()) {
			localLoadStep.fencePreparation(preparationId);
		}
	}

	@Override
	public final void prepareDurationInterval(final long durationNanos) throws RemoteException {
		try (final EntryCall call = entryCall()) {
			localLoadStep.prepareDurationInterval(durationNanos);
		}
	}

	@Override
	public final void startDurationInterval(final long durationNanos) throws RemoteException {
		try (final EntryCall call = entryCall()) {
			localLoadStep.startDurationInterval(durationNanos);
		}
	}

	@Override
	public final void closeOperationAdmissionForStepStop() throws RemoteException {
		try (final EntryCall call = entryCall()) {
			localLoadStep.closeOperationAdmissionForStepStop();
		}
	}

	@Override
	public final void enforceDispatchedOperationsDeadlineForStepStop(final long remainingNanos)
					throws RemoteException {
		try (final EntryCall call = entryCall()) {
			localLoadStep.enforceDispatchedOperationsDeadlineForStepStop(remainingNanos);
		}
	}

	@Override
	public final void recoverQueuedOperationsForStepStop() throws RemoteException {
		try (final EntryCall call = entryCall()) {
			localLoadStep.recoverQueuedOperationsForStepStop();
		}
	}

	@Override
	public final void drainDispatchedOperationsForStepStop(final long remainingNanos)
					throws RemoteException {
		try (final EntryCall call = entryCall()) {
			localLoadStep.drainDispatchedOperationsForStepStop(remainingNanos);
		}
	}

	@Override
	public final synchronized void startDispatchedOperationsDrainForStepStop(
					final long remainingNanos) {
		try (final EntryCall call = entryCall()) {
			if (durationDrainTask != null) {
				if (!durationDrainTask.isDone()) {
					return;
				}
				try {
					durationDrainTask.join();
					return;
				} catch (final CompletionException ignored) {
					// An explicit controller cleanup retry may safely begin a fresh local attempt.
				}
			}
			final CompletableFuture<Void> nextTask = new CompletableFuture<>();
			durationDrainTask = nextTask;
			final Thread drainThread = Thread.ofPlatform()
							.daemon()
							.name("spt-delete-service-drain-" + safeLoadStepId())
							.unstarted(() -> {
								try {
									localLoadStep.drainDispatchedOperationsForStepStop(remainingNanos);
									nextTask.complete(null);
								} catch (final Throwable failure) {
									nextTask.completeExceptionally(failure);
								}
							});
			drainThread.start();
		}
	}

	@Override
	public final synchronized boolean isDispatchedOperationsDrainCompleteForStepStop()
					throws RemoteException {
		try (final EntryCall call = entryCall()) {
			if (durationDrainTask == null || !durationDrainTask.isDone()) {
				return false;
			}
			try {
				durationDrainTask.join();
				return true;
			} catch (final CompletionException failure) {
				final Throwable cause = failure.getCause();
				throw new RemoteException(
								"standalone DELETE dispatched-operation drain failed",
								cause instanceof Exception exception ? exception : new Exception(cause));
			}
		}
	}

	@Override
	public final void validateTerminalStateForStepStop() throws RemoteException {
		try (final EntryCall call = entryCall()) {
			localLoadStep.validateTerminalStateForStepStop();
		}
	}

	@Override
	public final void verifyDeleteInventoryForStepStop() throws RemoteException {
		try (final EntryCall call = entryCall()) {
			localLoadStep.verifyDeleteInventoryForStepStop();
		}
	}

	@Override
	public final void startDeleteInventoryPreValidation() throws RemoteException {
		try (final EntryCall call = entryCall()) {
			localLoadStep.startDeleteInventoryPreValidation();
		}
	}

	@Override
	public final boolean isDeleteInventoryPreValidationComplete() throws RemoteException {
		try (final EntryCall call = entryCall()) {
			return localLoadStep.isDeleteInventoryPreValidationComplete();
		}
	}

	@Override
	public final void skipDeleteInventoryPostVerificationAfterStrictPreValidationFailure()
					throws RemoteException {
		try (final EntryCall call = entryCall()) {
			localLoadStep.skipDeleteInventoryPostVerificationAfterStrictPreValidationFailure();
		}
	}

	@Override
	public final boolean isDeleteInventoryVerificationCompleteForStepStop() throws RemoteException {
		try (final EntryCall call = entryCall()) {
			return localLoadStep.isDeleteInventoryVerificationCompleteForStepStop();
		}
	}

	@Override
	public final BucketMetricsReply bucketMetrics(final BucketMetricsRange range) throws RemoteException {
		try (final EntryCall call = entryCall()) {
			return localLoadStep.bucketMetrics(range);
		}
	}

	@Override
	public final DurationAwaitStatus durationAwaitStatus() throws RemoteException {
		try (final EntryCall call = entryCall()) {
			return localLoadStep.durationAwaitStatus();
		}
	}

	@Override
	public final boolean await(final long timeout, final TimeUnit timeUnit)
					throws IllegalStateException, InterruptedException {
		try (
						final EntryCall call = entryCall();
						final Instance logCtx = put(KEY_CLASS_NAME, getClass().getSimpleName()).put(KEY_STEP_ID, safeLoadStepId())) {
			return localLoadStep.await(timeout, timeUnit);
		} catch (final RemoteException e) {
			Loggers.ERR.warn("Await interrupted by remote failure for {}", safeLoadStepId(), e);
			return false;
		}
	}

	private String safeLoadStepId() {
		try {
			return localLoadStep.loadStepId();
		} catch (final RemoteException e) {
			return "unknown-step";
		}
	}
}
