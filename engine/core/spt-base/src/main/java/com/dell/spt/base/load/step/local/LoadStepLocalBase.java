package com.dell.spt.base.load.step.local;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.apache.logging.log4j.CloseableThreadContext.Instance;
import static org.apache.logging.log4j.CloseableThreadContext.put;

import com.dell.spt.base.metrics.range.RangeReadSnapshot;
import com.dell.spt.base.metrics.range.RangeReadArtifact;
import com.dell.spt.base.nic.EventLoopsArtifact;
import com.dell.spt.base.nic.NicPathsArtifact;
import com.dell.spt.base.nic.NicSetArtifact;
import com.dell.spt.base.nic.pool.PoolPreparation;
import com.dell.spt.base.load.lifecycle.OperationLifecycleCounters;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact;
import com.dell.spt.base.load.step.DurationAwaitStatus;
import com.dell.spt.base.load.step.DurationTime;
import com.dell.spt.base.load.step.LoadStepBase;
import com.dell.spt.base.load.step.PreparationStatus;
import com.dell.spt.base.load.step.StepIdFiles;
import com.dell.spt.base.load.step.StepIdCheckException;
import com.dell.spt.base.load.step.StepIdInUseException;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.storage.driver.AdmissionPhase;
import com.dell.spt.base.load.step.local.context.LoadStepContext;
import com.dell.spt.base.load.step.local.context.LoadStepContextImpl;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.bucket.BucketContextReply;
import com.dell.spt.base.metrics.bucket.BucketMeters;
import com.dell.spt.base.metrics.bucket.BucketMetricsRange;
import com.dell.spt.base.metrics.bucket.BucketMetricsReply;
import com.dell.spt.base.metrics.bucket.BucketSteadyOutcome;
import com.dell.spt.base.metrics.bucket.BucketMetricsBudget;
import com.dell.spt.base.metrics.bucket.BucketRecorder;
import com.dell.spt.base.metrics.bucket.BucketStepFence;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.storage.driver.bucket.TargetBucketSet;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.dell.spt.base.metrics.steady.SteadySettings;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.UUID;
import java.io.IOException;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.NoSuchElementException;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.IntSupplier;
import org.apache.logging.log4j.Level;

public abstract class LoadStepLocalBase extends LoadStepBase {

	private static final String BUCKET_METRICS_ENABLED = "output-metrics-bucket-enabled";
	private static final String METRICS_SUMMARY_PERSIST = "output-metrics-summary-persist";

	private final List<BucketMeters> bucketMeters = new ArrayList<>();
	private final List<BucketContext> bucketContexts = new ArrayList<>();
	private BucketMetricsReply bucketMetricsReply;
	private volatile BucketStepFence bucketFence;
	private long bucketMeterSets;

	private static final long STEP_CONTEXT_POLL_NANOS = 10_000_000L; // 10ms

	protected final List<LoadStepContext> stepContexts = new ArrayList<>();
	private volatile List<LoadStepContext> expectedDeleteObjectLifecycleContexts;
	private List<LoadStepContext> expectedOperationLifecycleContexts = List.of();
	private String operationLifecycleWorkerId;
	private boolean operationLifecyclePublished;
	private boolean nicPathsPublished;
	// Outside DELETE duration mode the stop runs once: the stop that close() repeats after a failed one
	// does nothing, so the close still releases the slice and throws the stop's failure. A standalone
	// DELETE shuts down once too, and a failed shutdown's failure is the stop's.
	private boolean shutdownAttempted;
	private RuntimeException shutdownFailure;
	private boolean stopAttempted;
	private RuntimeException stopFailure;
	// Set once the slice has released its contexts, drivers and metrics; a later close releases nothing.
	private volatile boolean released;
	private int failedDurationCloses;
	// Refused at start: an earlier step with the same id left its files here, and this one publishes none.
	private volatile boolean startRefused;
	private volatile StepPreparation preparation;
	private volatile Thread preparationDeadlineGuard;
	private volatile PoolPreparation.Report poolReport;
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
	/** Set as a step stop starts closing admission, before any context's queue is recovered. */
	private volatile boolean durationStopBegun;
	private volatile CompletableFuture<Void> deletePreValidationTask;
	private volatile CompletableFuture<Void> deletePostVerificationTask;

	protected IntSupplier actualConcurrencyGauge(final int index, final OpType opType) {
		return () -> stepContextActiveOpCount(index);
	}

	/**
	 * The in-flight count of the step context at {@code index}, or 0 when it does not exist.
	 *
	 * <p>The metrics contexts, and so this gauge, exist before {@code init()} adds the step
	 * contexts. A slice that fails in {@code init()} leaves {@link #stepContexts} empty, and the
	 * shutdown that follows still reads the gauge to close the steady-state windows. In a working
	 * run the step context exists for the metrics context's whole life, so the 0 branch is never
	 * taken there.
	 */
	protected final int stepContextActiveOpCount(final int index) {
		final List<LoadStepContext> contexts = stepContexts;
		return index < contexts.size() ? contexts.get(index).activeOpCount() : 0;
	}

	/**
	 * Closes what a failed {@code init()} opened before the context it was building, and returns
	 * {@code failure} to rethrow. That context's own driver and generator are closed by its
	 * {@link ContextInitResources}. When the failure came before {@code init()} added that context
	 * ({@code contextPending}), this closes every context added before it and empties
	 * {@link #stepContexts}. Otherwise the step's close owns them all and nothing is closed here. Any
	 * {@code Throwable} a close throws, an {@code Error} as well, is added to {@code failure} as
	 * suppressed, and the remaining contexts are still closed. An earlier context's close logs an
	 * {@code IOException} from its own driver's close itself and does not throw it, so that failure is
	 * in the log only, not among the suppressed.
	 */
	protected final <T extends Throwable> T closeAfterInitFailure(
					final T failure, final boolean contextPending) {
		if (!contextPending) {
			return failure;
		}
		final List<AutoCloseable> resources = new ArrayList<>(stepContexts);
		stepContexts.clear();
		for (final AutoCloseable resource : resources) {
			try {
				resource.close();
			} catch (final Throwable closeFailure) {
				// An Error from a close is kept as well, so the original failure and the later closes win.
				if (closeFailure instanceof InterruptedException) {
					Thread.currentThread().interrupt();
				}
				failure.addSuppressed(closeFailure);
			}
		}
		return failure;
	}

	protected LoadStepLocalBase(
					final Config baseConfig,
					final List<Extension> extensions,
					final List<Config> contextConfigs,
					final MetricsManager metricsManager) {
		super(baseConfig, extensions, contextConfigs, metricsManager, true);
		// Only now, with this class's fields set, may the exit hook close this step.
		registerForCloseAll();
	}

	@Override
	public final DeleteObjectLifecycleSnapshot deleteObjectLifecycle() {
		// A released slice holds no contexts to report.
		if (!standaloneDeleteEnabled() || released) {
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
		long preValidationFailed = 0;
		long verificationCorrectnessFailed = 0;
		long verificationInconclusive = 0;
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
			preValidationFailed = Math.addExact(preValidationFailed, snapshot.preValidationFailed());
			verificationCorrectnessFailed = Math.addExact(
							verificationCorrectnessFailed, snapshot.verificationCorrectnessFailed());
			verificationInconclusive = Math.addExact(
							verificationInconclusive, snapshot.verificationInconclusive());
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
						preValidationFailed,
						verificationCorrectnessFailed,
						verificationInconclusive,
						reconciled);
	}

	/** A slice refused at JVM exit never ran, so, like one refused for its id, it publishes nothing at close. */
	@Override
	protected final void onStartRefusedAtJvmExit() {
		startRefused = true;
	}

	@Override
	protected void doStartWrapped() {
		// A step that runs again reads its meters afresh.
		synchronized (this) {
			bucketMetricsReply = null;
		}
		resetDurationLifecycleForStart();
		expectedOperationLifecycleContexts = List.copyOf(stepContexts);
		operationLifecycleWorkerId = UUID.randomUUID().toString();
		operationLifecyclePublished = false;
		nicPathsPublished = false;
		shutdownAttempted = false;
		shutdownFailure = null;
		stopAttempted = false;
		stopFailure = null;
		failedDurationCloses = 0;
		discardEarlierEventLoops();
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
		final StepPreparation barrier = preparation;
		if (barrier != null) {
			stepContexts.forEach(LoadStepContext::holdForAdmissionBarrier);
		}
		boolean anyStarted = false;
		final var iterator = stepContexts.iterator();
		while (iterator.hasNext()) {
			final var stepCtx = iterator.next();
			try {
				stepCtx.start();
				anyStarted = true;
			} catch (final IllegalStateException e) {
				final var terminal = RunTerminalException.find(e);
				if (terminal != null) {
					throw terminal;
				}
				throw contextStartFailure(stepCtx, e);
			} catch (final RemoteException e) {
				throw contextStartFailure(stepCtx, e);
			}
		}
		if (!anyStarted) {
			throw new IllegalStateException(loadStepId() + ": failed to start any load step contexts");
		}
		if (barrier != null) {
			startPreparationThread(barrier);
		}
	}

	/** The contexts left would run less of the load than was asked for, under the same numbers. */
	private RunTerminalException contextStartFailure(final LoadStepContext stepCtx, final Exception cause) {
		LogUtil.exception(
						Level.WARN, cause, "{}: failed to start the load step context \"{}\"", loadStepId(), stepCtx);
		return new RunTerminalException(
						IntegrityTerminalException.Category.EXECUTION,
						loadStepId(),
						"failed to start a load step context; see the engine log",
						cause);
	}

	@Override
	protected final boolean loadIntervalDeferred() {
		return preparation != null;
	}

	@Override
	public final synchronized void startPreparation(final long preparationId, final long budgetNanos) {
		final var barrier = new StepPreparation(preparationId, budgetNanos, System.nanoTime());
		preparation = barrier;
		// The deadline runs from here: an Entry Node that dies before start() still leaves it fenced.
		final String stepId = safeStepIdForPreparation();
		preparationDeadlineGuard = Thread.ofPlatform().daemon().name("spt-preparation-deadline-" + stepId).start(() -> {
			try {
				final long remaining = barrier.deadlineNanos() - System.nanoTime();
				if (remaining > 0) {
					TimeUnit.NANOSECONDS.sleep(remaining);
				}
				fence(barrier, PreparationStatus.DEADLINE_MESSAGE);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
	}

	@Override
	public final PreparationStatus preparationStatus(final long preparationId) {
		final StepPreparation barrier = preparation;
		if (barrier == null || barrier.id() != preparationId) {
			return new PreparationStatus(PreparationStatus.State.FENCED, "", "a stale preparation", 0);
		}
		return barrier.status(System.nanoTime());
	}

	@Override
	public final synchronized boolean releasePreparedAdmission(final long preparationId) {
		final StepPreparation barrier = preparation;
		if (barrier == null || barrier.id() != preparationId) {
			return false;
		}
		if (barrier.released()) {
			return true;
		}
		if (!barrier.release(System.nanoTime())) {
			return false;
		}
		preparationDeadlineGuard.interrupt();
		startLoadInterval();
		stepContexts.forEach(LoadStepContext::releaseAdmissionBarrier);
		return true;
	}

	@Override
	public final void fencePreparation(final long preparationId) {
		final StepPreparation barrier = preparation;
		if (barrier != null && barrier.id() == preparationId) {
			// Another slice failed to release, so this step failed even if this slice had opened.
			barrier.markStepFailed();
			fence(barrier, "the entry node stopped the preparation");
		}
	}

	/**
	 * Fences the preparation and stops the contexts on a thread of their own: a driver blocked in its
	 * start, such as in a DNS lookup, holds its lifecycle lock, and the caller may be an RMI call.
	 */
	private void fence(final StepPreparation barrier, final String why) {
		// The monitor makes this atomic with a release: one in progress finishes first, and then this
		// fence does nothing and the caller's own stop handles what the release opened.
		synchronized (this) {
			if (!barrier.markFenced(why, System.nanoTime())) {
				return;
			}
		}
		final String stepId = safeStepIdForPreparation();
		Loggers.MSG.info("{}: preparation fenced: {}", stepId, why);
		final Thread guard = preparationDeadlineGuard;
		if (guard != null && guard != Thread.currentThread()) {
			guard.interrupt();
		}
		stopContextsOfFence(stepId, barrier);
	}

	/** On a thread of its own: neither a driver's fence action nor a stuck context holds up the caller. */
	private void stopContextsOfFence(final String stepId, final StepPreparation barrier) {
		final var contexts = List.copyOf(stepContexts);
		Thread.ofPlatform().daemon().name("spt-preparation-fence-" + stepId).start(() -> {
			try {
				dropBucketMetricsOfFence(contexts);
			} catch (final RuntimeException e) {
				LogUtil.exception(Level.WARN, e, "{}: failed to free a fenced step's bucket meters", stepId);
			}
			barrier.runFenceActions();
			for (final LoadStepContext stepContext : contexts) {
				try {
					stepContext.stop();
				} catch (final Exception e) {
					LogUtil.exception(Level.WARN, e, "{}: failed to stop a fenced context", stepId);
				}
			}
		});
	}

	private String safeStepIdForPreparation() {
		return config.stringVal("load-step-id");
	}

	private void startPreparationThread(final StepPreparation barrier) {
		final String stepId = safeStepIdForPreparation();
		// A fence that came before the contexts existed could not stop them. One that comes after this
		// check sees them, as they were added before it.
		if (barrier.fenced()) {
			stopContextsOfFence(stepId, barrier);
			return;
		}
		final Thread preparing = Thread.ofPlatform().daemon().name("spt-preparation-" + stepId).unstarted(() -> {
			try (final var logCtx = put(KEY_STEP_ID, stepId).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
				AdmissionPhase.current(barrier);
				final var contexts = List.copyOf(stepContexts);
				for (final LoadStepContext stepContext : contexts) {
					if (barrier.fenced()) {
						return;
					}
					stepContext.startDriverForAdmission();
				}
				if (barrier.fenced()) {
					return;
				}
				// Every driver has resolved its NICs, so the concurrent drivers are checked together.
				poolReport = PoolPreparation.run(stepId, poolParticipants(contexts), barrier, PoolPreparation.Environment.system());
				for (final LoadStepContext stepContext : contexts) {
					if (barrier.fenced()) {
						return;
					}
					stepContext.prepareForAdmission(barrier);
				}
				barrier.ready(System.nanoTime());
			} catch (final Throwable failure) {
				final String message = preparationFailureMessage(failure);
				if (barrier.fenced()) {
					// A fence interrupts blocked work; that is not a failure of its own.
					Loggers.MSG.debug("{}: preparation ended by its fence: {}", stepId, message);
				} else {
					Loggers.ERR.error("{}: preparation failed: {}", stepId, message);
					barrier.fail(message, System.nanoTime());
				}
			} finally {
				AdmissionPhase.current(null);
			}
		});
		barrier.onFence(preparing::interrupt);
		preparing.start();
	}

	private List<PoolPreparation.Participant> poolParticipants(final List<LoadStepContext> contexts) {
		final var participants = new ArrayList<PoolPreparation.Participant>();
		for (final var context : contexts) {
			final var driver = context.poolDemandDriver();
			if (driver != null) {
				final int index = expectedOperationLifecycleContexts.indexOf(context);
				participants.add(new PoolPreparation.Participant(index, driver, effectiveConfig(index), mixesOperations()));
			}
		}
		return participants;
	}

	/** Whether this step's one driver mixes operations by weight, which only the mixed step type does. */
	protected boolean mixesOperations() {
		return false;
	}

	/**
	 * The effective configuration of the context at {@code contextIndex}, which Pool Demand reads. A
	 * step with one context per sub-load merges each sub-load's configuration over the base.
	 */
	protected Config effectiveConfig(final int contextIndex) {
		return config;
	}

	private static String preparationFailureMessage(final Throwable failure) {
		final RunTerminalException terminal = RunTerminalException.find(failure);
		final Throwable source = terminal != null ? terminal : failure;
		return source.getMessage() != null ? source.getMessage() : source.toString();
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
		deletePreValidationTask = null;
		deletePostVerificationTask = null;
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
			durationStopBegun = false;
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

	/**
	 * Builds the bucket meters of one step context, in one block before its load starts: for its
	 * primary metrics context and, in a mixed step, each per-type context.
	 *
	 * @param config the context's configuration
	 * @param primary the context's primary metrics context
	 * @param byOpType its per-type metrics contexts, or null when it has none
	 * @return the recorder for the context's results, or null when bucket metrics are off for it: a
	 *     step with one target bucket, the off switch, or a step already fenced
	 */
	protected final synchronized BucketRecorder initBucketMetrics(
					final Config config,
					final MetricsContext primary,
					final Map<OpType, MetricsContext> byOpType) {
		// Under the monitor, either this sees the fence or the fence's drop sees this step's lists. A
		// context not yet in stepContexts keeps its recorder until the second drop in doStartWrapped.
		final StepPreparation barrier = preparation;
		if (barrier != null && barrier.fenced()) {
			return null;
		}
		// A phase that persists no totals, such as a seed, has no files for these to go beside.
		if (TargetBucketSet.count(config) <= 1
						|| !config.boolVal(BUCKET_METRICS_ENABLED)
						|| !config.boolVal(METRICS_SUMMARY_PERSIST)) {
			return null;
		}
		final List<String> members = TargetBucketSet.memberNames(config);
		final int contexts = byOpType == null || byOpType.isEmpty() ? 1 : byOpType.size();
		bucketMeterSets += contexts * (members.size() + 1L);
		BucketMetricsBudget.check(bucketMeterSets, Runtime.getRuntime().maxMemory());
		final IdentityHashMap<MetricsContext, BucketMeters> metersOfContext = new IdentityHashMap<>();
		final BucketMeters primaryMeters = new BucketMeters(primary.opType(), members);
		metersOfContext.put(primary, primaryMeters);
		final Map<OpType, BucketMeters> metersOfType = new EnumMap<>(OpType.class);
		if (byOpType != null) {
			byOpType.forEach((type, context) -> metersOfType.put(
							type, metersOfContext.computeIfAbsent(context, c -> new BucketMeters(c.opType(), members))));
		}
		bucketMeters.addAll(metersOfContext.values());
		final List<BucketStepFence.ContextCuts> ownCuts = new ArrayList<>(metersOfContext.size());
		metersOfContext.forEach((context, meters) -> {
			if (bucketFence == null) {
				bucketFence = new BucketStepFence(loadStepId());
			}
			final BucketStepFence.ContextCuts cuts = bucketFence.register(meters);
			final boolean measuresWindow = context.steadyWindowListener(cuts);
			if (measuresWindow) {
				ownCuts.add(cuts);
			}
			bucketContexts.add(new BucketContext(context, meters, cuts, measuresWindow, members));
		});
		final BucketStepFence fence = bucketFence;
		return new BucketRecorder(members, primaryMeters, metersOfType, () -> fence.await(ownCuts));
	}

	private record BucketContext(
					MetricsContext context,
					BucketMeters meters,
					BucketStepFence.ContextCuts cuts,
					boolean measuresWindow,
					List<String> members) {}

	/**
	 * Reads the quiescent meters once, encoding each set before reading the next, and holds the
	 * result until this step closes.
	 */
	@Override
	public final synchronized BucketMetricsReply bucketMetrics(final BucketMetricsRange range) {
		if (bucketContexts.isEmpty()) {
			return null;
		}
		if (bucketMetricsReply == null) {
			if (bucketFence != null) {
				bucketFence.await();
			}
			final List<BucketContextReply> replies = new ArrayList<>(bucketContexts.size());
			for (final BucketContext context : bucketContexts) {
				final BucketSteadyOutcome outcome = context.measuresWindow() ? context.cuts().outcome() : null;
				final SteadyResult aggregateWindow = context.context().steadyResult();
				final AllMetricsSnapshot lastSnapshot = context.context().lastSnapshot();
				replies.add(new BucketContextReply(
								metricsContexts.indexOf(context.context()),
								context.meters().opType(),
								lastSnapshot == null ? 0L : lastSnapshot.elapsedTimeMillis(),
								aggregateWindow == null ? SteadyResult.NONE : aggregateWindow.startOffsetMillis(),
								context.members(),
								context.meters().terminalRecords(),
								context.measuresWindow() && outcome == null
												? BucketSteadyOutcome.unmeasured("the bucket window never closed")
												: outcome));
			}
			bucketMetricsReply = new BucketMetricsReply(loadStepId(), UUID.randomUUID().toString(), replies);
		}
		return range == null || BucketMetricsRange.ALL.equals(range)
						? bucketMetricsReply
						: bucketMetricsReply.within(range);
	}

	/**
	 * A fence means nothing was admitted, so the meters hold nothing, and the close that would free them
	 * may never come: a worker fences itself when its entry node is gone, and the next step replaces this
	 * one without closing it. First, so that a stuck fence action or context doesn't keep them.
	 */
	private synchronized void dropBucketMetricsOfFence(final List<LoadStepContext> contexts) {
		for (final BucketContext context : bucketContexts) {
			context.context().steadyWindowListener(null);
		}
		for (final LoadStepContext context : contexts) {
			if (context instanceof LoadStepContextImpl<?, ?> impl) {
				impl.bucketRecorder(null);
			}
		}
		releaseBucketMetrics();
	}

	/** A worker holds its bucket data until its step closes: a closed service can outlive its step. */
	private synchronized void releaseBucketMetrics() {
		if (bucketFence != null) {
			bucketFence.close();
			bucketFence = null;
		}
		bucketContexts.clear();
		bucketMeters.clear();
		bucketMetricsReply = null;
	}

	@Override
	protected final void afterSteadyWindowsClosed() {
		final BucketStepFence fence = bucketFence;
		if (fence != null) {
			fence.await();
		}
	}

	/** The bucket meters this step built, which are empty when bucket metrics are off. */
	protected final List<BucketMeters> bucketMeters() {
		return List.copyOf(bucketMeters);
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
						.steadySettings(SteadySettings.fromMetricsConfig(metricsConfig))
						.build();

		// enrich metadata with limits so /metrics/json can compute completion on workers
		boolean countLimitConfigured = false;
		boolean countLimitApplied = false;
		try {
			// A mixed step counts against every sub-load over the base; other step types keep the base's
			// count here, which the CLI's completion detection reads.
			final Config countConfig = mixesOperations() ? effectiveConfig(originIndex) : this.config;
			final long countLimit = countConfig.longVal("load-op-limit-count");
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
		final StepPreparation barrier = preparation;
		if (barrier != null) {
			fence(barrier, "the step stopped during preparation");
		}
		closeSteadyWindows("shutdown");
		if (standaloneDeleteDurationMode()) {
			prepareDurationStop();
			if (durationAdmissionBarrierSatisfied && durationStopFailure == null) {
				captureDurationStopFailure(
								"shutdown contexts",
								() -> invokeDurationContextPhase("shutdown", LoadStepContext::shutdown));
			}
			return;
		}
		// A standalone DELETE shuts every context down once; another workload as before.
		final boolean once = standaloneDeleteEnabled();
		if (once && shutdownAttempted) {
			// The stop or close repeats a shutdown that failed: its failure was thrown then, and the stop
			// and the close throw it again.
			return;
		}
		RuntimeException failure = null;
		final var iterator = stepContexts.iterator();
		while (iterator.hasNext()) {
			final var stepCtx = iterator.next();
			try (final Instance ctx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
				stepCtx.shutdown();
				Loggers.MSG.debug("{}: load step context shutdown", loadStepId());
			} catch (final RemoteException e) {
				LogUtil.exception(Level.WARN, e, "{}: failed to shutdown the load step context", loadStepId());
				iterator.remove();
			} catch (final RuntimeException e) {
				if (!once) {
					throw e;
				}
				// The other contexts still shut down; the stop and the close release them all.
				failure = withFailure(failure, e);
			}
		}
		if (!once) {
			return;
		}
		// Only now: a shutdown cut short by anything else, an interrupt for one, runs again when repeated.
		shutdownAttempted = true;
		if (failure != null) {
			shutdownFailure = failure;
			throw failure;
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
							contextDone(stepCtx);
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
		if (timeIsOut) {
			countDown -= settleDoneContexts(stepContextsCopy);
		}

		return 0 == countDown;
	}

	/**
	 * Once the time is up, looks at each remaining context once more, without waiting, so a context's
	 * terminal failure is reported even when the time ran out before the loop above came to it.
	 * Contexts are completed in their order only: a context reaches {@link #contextDone} here when it
	 * is done and every context before it is settled. A context after one still running only has its
	 * terminal failure checked, because a pipeline stage still reads what the stage before it writes.
	 * Returns how many contexts it settled.
	 */
	private int settleDoneContexts(final LoadStepContext[] stepContextsCopy) {
		int settled = 0;
		boolean earlierSettled = true;
		for (int i = 0; i < stepContextsCopy.length; i++) {
			final LoadStepContext stepCtx = stepContextsCopy[i];
			if (stepCtx == null) {
				continue;
			}
			if (earlierSettled && stepCtx.isDone()) {
				contextDone(stepCtx);
				stepContextsCopy[i] = null;
				settled++;
			} else if (earlierSettled) {
				earlierSettled = false;
			} else {
				stepCtx.checkTerminalFailure();
			}
		}
		return settled;
	}

	/**
	 * Called by {@link #await(long, TimeUnit)} each time it finds {@code context} done, before the
	 * step stops its contexts. Does nothing by default.
	 */
	protected void contextDone(final LoadStepContext context) {}

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
				boolean stoppedBeforeDeadline = false;
				for (final LoadStepContext context : List.copyOf(stepContexts)) {
					if (context == null) {
						continue;
					}
					final OptionalLong exhaustion = schedulingExhaustionNanos(context);
					long exhaustedAtNanos;
					if (exhaustion.isEmpty()) {
						if (!context.isDone()) {
							continue;
						}
						exhaustedAtNanos = System.nanoTime();
						if (durationStopBegun
										&& DurationTime.timestampBeforeDeadline(exhaustedAtNanos, deadlineNanos)) {
							// The last request may have dispatched between the read above and isDone:
							// read the record again, so a real exhaustion is not reported as a stop.
							final OptionalLong lateExhaustion = schedulingExhaustionNanos(context);
							if (lateExhaustion.isEmpty()) {
								// A stop emptied the queue; the slice did not run out. Another
								// context's recorded exhaustion still wins, and the final counts still
								// decide exhaustion at validation.
								stoppedBeforeDeadline = true;
								continue;
							}
							exhaustedAtNanos = lateExhaustion.getAsLong();
						}
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
				if (stoppedBeforeDeadline) {
					break;
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
		// Thrown where the exhaustion failure was, so the await ends as promptly. The status stays
		// RUNNING, as for a slice stopped with no await live.
		throw terminalFailure(
						IntegrityTerminalException.Category.EXECUTION,
						"Standalone DELETE input slice was stopped before the requested duration",
						null);
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
			final var contextsStopped = durationAdmissionBarrierSatisfied && durationStopFailure == null;
			if (contextsStopped) {
				captureDurationStopFailure(
								"stop contexts",
								() -> invokeDurationContextPhase("stop", context -> context.stop()));
				// Otherwise close stops the contexts and publishes.
				publishNicPaths();
			}
			publishOperationLifecycle();
			super.doStop();
			return;
		}
		if (stopAttempted) {
			// The close repeats a stop that failed: its failure was thrown then, and the close throws it again.
			return;
		}
		RuntimeException failure = shutdownFailure;
		for (final var context : List.copyOf(stepContexts)) {
			if (context == null) {
				continue;
			}
			try {
				context.stop();
			} catch (final RuntimeException e) {
				failure = withFailure(failure, e);
			}
		}
		publishNicPaths();
		try {
			publishOperationLifecycle();
		} catch (final RuntimeException e) {
			failure = withFailure(failure, e);
		}
		try {
			super.doStop();
		} catch (final RuntimeException e) {
			failure = withFailure(failure, e);
		}
		// Only now: a stop cut short by anything else, an interrupt for one, runs again when repeated.
		stopAttempted = true;
		if (failure != null) {
			stopFailure = failure;
			throw failure;
		}
	}

	/** A failed stop ends a standalone run with its own failure: the stop runs once (see doStop). */
	@Override
	protected final void stopAfterFailedClose() {
		final RuntimeException failure = stopFailure;
		if (failure != null && !standaloneDeleteDurationMode()) {
			throw failure;
		}
		doStop();
	}

	/**
	 * Whether this slice keeps its resources after a failed close because the close will be retried:
	 * a standalone DELETE in duration mode until its last retry. Its step service then stays exported.
	 */
	public final boolean awaitsCleanupRetry() {
		return standaloneDeleteDurationMode() && !released;
	}

	private static <T extends Throwable> T withFailure(final T first, final T next) {
		if (first == null) {
			return next;
		}
		if (next != first) {
			first.addSuppressed(next);
		}
		return first;
	}

	/**
	 * Writes the step's event-loop report once, and its per-path CSV and NIC-set report when an
	 * expected context has a NIC set: after the contexts stopped, or at close for a step whose start
	 * failed. A context without its rows is named missing in the CSV. A failure to write any of them
	 * warns; they are evidence, not a step outcome.
	 */
	private void publishNicPaths() {
		if (nicPathsPublished || startRefused) {
			return;
		}
		nicPathsPublished = true;
		publishEventLoops();
		final var paths = new ArrayList<NicPathsArtifact.Context>();
		final var sets = new ArrayList<NicSetArtifact.Context>();
		var contextsWithoutNicSet = 0;
		for (var i = 0; i < expectedOperationLifecycleContexts.size(); i++) {
			final var context = expectedOperationLifecycleContexts.get(i);
			if (context != null && context.hasNicSet()) {
				paths.add(new NicPathsArtifact.Context(i, context.nicPathRows()));
				sets.add(new NicSetArtifact.Context(i, context.nicSetResources()));
			} else if (context != null) {
				contextsWithoutNicSet++;
			}
		}
		if (paths.isEmpty()) {
			return;
		}
		try (final var logContext = put(KEY_STEP_ID, loadStepId())) {
			final StepPreparation barrier = preparation;
			NicPathsArtifact.publish(
							runId(), loadStepId(), operationLifecycleWorkerId, paths, barrier == null || barrier.admitted());
		} catch (final RuntimeException e) {
			LogUtil.exception(Level.WARN, e, "{}: failed to write the NIC paths file", loadStepId());
		}
		try (final var logContext = put(KEY_STEP_ID, loadStepId())) {
			NicSetArtifact.publish(runId(), loadStepId(), operationLifecycleWorkerId, sets, contextsWithoutNicSet, poolReport);
		} catch (final RuntimeException e) {
			LogUtil.exception(Level.WARN, e, "{}: failed to write the NIC set file", loadStepId());
		}
	}

	/**
	 * Refuses this slice, before it seeds or writes any of them, when an earlier step with the same id
	 * left one of the {@link StepIdFiles} this slice would publish: the same files the Entry Node
	 * tests before it starts any slice. A refused slice publishes nothing at close, so the earlier
	 * step's files, its event-loop report among them, stay as they were.
	 */
	@Override
	protected final void refuseStepIdInUse(final boolean nodes) {
		startRefused = false;
		final var paths = new ArrayList<Path>();
		for (final var loggerName : StepIdFiles.sliceLoggerNames(
						StepIdFiles.merged(config, ctxConfigs), StepIdFiles.nicSet(config, ctxConfigs))) {
			try {
				paths.add(Path.of(FileManager.INSTANCE.logFileName(loggerName, loadStepId())));
			} catch (final IOException | RuntimeException unknown) {
				throwUncheckedIfInterrupted(unknown);
				startRefused = true;
				throw StepIdCheckException.here(loadStepId(), "the " + loggerName + " file", unknown);
			}
		}
		try {
			StepIdInUseException.refuseExisting(loadStepId(), paths);
		} catch (final StepIdInUseException | StepIdCheckException refused) {
			startRefused = true;
			throw refused;
		}
	}

	private boolean publishesOperationLifecycle() {
		final String operation = config.stringVal("load-op-type");
		return "create".equalsIgnoreCase(operation) || "read".equalsIgnoreCase(operation);
	}

	/** A report an earlier step with this id left must not pass for this one's (see publishEventLoops). */
	private void discardEarlierEventLoops() {
		try (final var logContext = put(KEY_STEP_ID, loadStepId())) {
			EventLoopsArtifact.discardEarlier(loadStepId());
		} catch (final RuntimeException e) {
			LogUtil.exception(Level.WARN, e, "{}: failed to remove an earlier event loops file", loadStepId());
		}
	}

	/** Each expected context's driver loops, in context order; contexts that own none are left out. */
	private void publishEventLoops() {
		try (final var logContext = put(KEY_STEP_ID, loadStepId())) {
			final var loops = new ArrayList<EventLoopsArtifact.Context>();
			for (var i = 0; i < expectedOperationLifecycleContexts.size(); i++) {
				final var context = expectedOperationLifecycleContexts.get(i);
				final var contextLoops = context == null ? null : context.eventLoops();
				if (contextLoops != null) {
					loops.add(new EventLoopsArtifact.Context(i, contextLoops));
				}
			}
			EventLoopsArtifact.publish(runId(), loadStepId(), operationLifecycleWorkerId, loops);
		} catch (final RuntimeException e) {
			LogUtil.exception(Level.WARN, e, "{}: failed to write the event loops file", loadStepId());
		}
	}

	private void publishOperationLifecycle() {
		if (operationLifecyclePublished || startRefused || !publishesOperationLifecycle()) {
			return;
		}
		final var counters = new ArrayList<OperationLifecycleCounters>();
		for (final var context : expectedOperationLifecycleContexts) {
			counters.add(context.terminalOperationCounters());
		}
		try (final var logContext = put(KEY_STEP_ID, loadStepId())) {
			OperationLifecycleArtifact.publish(runId(), loadStepId(),
							operationLifecycleWorkerId, counters, expectedOperationLifecycleContexts.equals(stepContexts));
		}
		// Published: a failed range-read file below must not have it published again.
		operationLifecyclePublished = true;
		final var ranges = new ArrayList<RangeReadSnapshot>();
		boolean complete = expectedOperationLifecycleContexts.equals(stepContexts);
		for (final var context : expectedOperationLifecycleContexts) {
			ranges.add(context.rangeReadSnapshot());
			complete &= context.terminalOperationCounters() != null;
		}
		if (ranges.stream().anyMatch(java.util.Objects::nonNull)) {
			try (final var logContext = put(KEY_STEP_ID, loadStepId())) {
				RangeReadArtifact.publish(runId(), loadStepId(), operationLifecycleWorkerId, ranges, complete);
			}
		}
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
		closeSteadyWindows("close operation admission");
		durationStopBegun = true;
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
		closeSteadyWindows("dispatched operations deadline");
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
		closeSteadyWindows("recover queued operations");
		invokeDurationContextPhase(
						"recover queued operations",
						LoadStepContext::recoverQueuedOperationsForStepStop);
	}

	@Override
	public final void drainDispatchedOperationsForStepStop(final long remainingNanos) {
		closeSteadyWindows("drain dispatched operations");
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
		closeSteadyWindows("validate terminal state");
		durationTerminalValidationDelegated = true;
		invokeDurationContextPhase(
						"validate terminal state", LoadStepContext::validateTerminalState);
		if (durationInventoryRanOut()) {
			// Even when a poll found it: a controller can hold a verdict read before that.
			durationAwaitStatus = DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE;
			throw standaloneDeleteEarlyExhaustionFailure();
		}
	}

	private boolean durationInventoryRanOut() {
		for (final LoadStepContext context : List.copyOf(stepContexts)) {
			if (context != null && context.durationInventoryRanOut()) {
				return true;
			}
		}
		return false;
	}

	@Override
	public final synchronized void startDeleteInventoryPreValidation() {
		if (deletePreValidationTask == null) {
			deletePreValidationTask = standaloneDeletePreValidationEnabled()
							? startDeleteInventoryPhase(
											"pre-validation", LoadStepContext::validateDeleteInventoryBeforeAdmission)
							: CompletableFuture.completedFuture(null);
		}
	}

	@Override
	public final boolean isDeleteInventoryPreValidationComplete() {
		return deleteInventoryPhaseComplete(
						deletePreValidationTask, "pre-validation was not started");
	}

	@Override
	public final synchronized void skipDeleteInventoryPostVerificationAfterStrictPreValidationFailure() {
		invokeDurationContextPhase(
						"record distributed strict pre-validation abort",
						LoadStepContext::skipDeleteInventoryPostVerificationAfterStrictPreValidationFailure,
						true);
	}

	@Override
	public final synchronized void verifyDeleteInventoryForStepStop() {
		closeSteadyWindows("verify delete inventory");
		if (deletePostVerificationTask == null) {
			deletePostVerificationTask = startDeleteInventoryPhase(
							"post-verification", LoadStepContext::verifyDeleteInventoryAfterDrain);
		}
	}

	@Override
	public final boolean isDeleteInventoryVerificationCompleteForStepStop() {
		final CompletableFuture<Void> task = deletePostVerificationTask;
		try {
			return deleteInventoryPhaseComplete(task, "post-verification was not started");
		} catch (final RuntimeException failure) {
			synchronized (this) {
				if (deletePostVerificationTask == task) {
					deletePostVerificationTask = null;
				}
			}
			throw failure;
		}
	}

	private CompletableFuture<Void> startDeleteInventoryPhase(
					final String phase, final DurationContextPhase action) {
		final CompletableFuture<Void> task = new CompletableFuture<>();
		Thread.ofPlatform()
						.daemon(true)
						.name("spt-delete-" + phase + "-" + loadStepId())
						.start(() -> {
							try {
								invokeUnboundedDeleteInventoryContextPhase(phase, action);
								task.complete(null);
							} catch (final Throwable failure) {
								task.completeExceptionally(failure);
							}
						});
		return task;
	}

	private void invokeUnboundedDeleteInventoryContextPhase(
					final String phase, final DurationContextPhase action) {
		final List<LoadStepContext> activeContexts = stepContexts.stream()
						.filter(Objects::nonNull)
						.toList();
		if (activeContexts.isEmpty()) {
			return;
		}
		IntegrityTerminalException phaseFailure = null;
		try (ExecutorService executor = Executors.newThreadPerTaskExecutor(
						Thread.ofPlatform().daemon().name("spt-delete-context-" + phase + "-", 0).factory())) {
			final List<? extends Future<?>> tasks = activeContexts.stream()
							.map(context -> executor.submit(() -> {
								action.execute(context);
								return null;
							}))
							.toList();
			for (final Future<?> task : tasks) {
				try {
					task.get();
				} catch (final ExecutionException failure) {
					phaseFailure = appendTerminalFailure(
									phaseFailure,
									IntegrityTerminalException.Category.EXECUTION,
									"Standalone DELETE failed " + phase + " across local input contexts",
									failure.getCause());
				} catch (final InterruptedException interrupted) {
					Thread.currentThread().interrupt();
					throw terminalFailure(
									IntegrityTerminalException.Category.EXECUTION,
									"Standalone DELETE " + phase + " was interrupted",
									interrupted);
				}
			}
		}
		if (phaseFailure != null) {
			throw phaseFailure;
		}
	}

	private boolean deleteInventoryPhaseComplete(
					final CompletableFuture<Void> task, final String missingMessage) {
		if (task == null) {
			throw new IllegalStateException(missingMessage);
		}
		if (!task.isDone()) {
			return false;
		}
		try {
			task.join();
			return true;
		} catch (final CompletionException failure) {
			final Throwable cause = failure.getCause();
			if (cause instanceof RuntimeException runtimeFailure) {
				throw runtimeFailure;
			}
			throw new IllegalStateException("DELETE inventory phase failed", cause);
		}
	}

	private synchronized void captureDurationTerminalValidation() {
		if (durationTerminalValidationCompleted || durationTerminalValidationDelegated) {
			return;
		}
		try {
			invokeDurationContextPhase(
							"validate terminal state", LoadStepContext::validateTerminalState);
			// await has already reported an exhaustion a poll found.
			if (durationAwaitStatus != DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE
							&& durationInventoryRanOut()) {
				durationAwaitStatus = DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE;
				durationValidityFailure = standaloneDeleteEarlyExhaustionFailure();
			}
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

	/**
	 * Releases the slice: its metrics, contexts and drivers, whatever its stop or any part of the close
	 * throws, and throws the stop's failure first and the close's after it, suppressed. A standalone
	 * DELETE in duration mode keeps them after a failed close until its last retry. A released slice
	 * releases nothing again.
	 */
	@Override
	protected final void doClose() throws IOException {
		if (released) {
			return;
		}
		releaseBucketMetrics();
		if (standaloneDeleteDurationMode()) {
			try {
				closeDurationContexts();
			} catch (final RuntimeException failure) {
				if (++failedDurationCloses >= DURATION_RUN_CLOSE_ATTEMPT_LIMIT) {
					releaseAfterLastFailedDurationClose(failure);
				}
				throw failure;
			} finally {
				publishNicPaths();
			}
			releaseContextCopies();
			released = true;
			return;
		}
		// The stop, which always runs before the close, has unregistered the metrics.
		Exception failure = stopFailure;
		try {
			super.doClose();
		} catch (final IOException | RuntimeException e) {
			failure = withFailure(failure, e);
		}
		final List<RuntimeException> contextFailures = stepContexts
						.parallelStream()
						.filter(Objects::nonNull)
						.map(
										stepCtx -> {
											try {
												stepCtx.close();
											} catch (final IOException e) {
												LogUtil.exception(
																Level.ERROR,
																e,
																"Failed to close the load step context \"{}\"",
																stepCtx.toString());
											} catch (final RuntimeException e) {
												return e;
											}
											return null;
										})
						.filter(Objects::nonNull)
						.toList();
		for (final RuntimeException e : contextFailures) {
			failure = withFailure(failure, e);
		}
		publishNicPaths();
		for (final var stepContext : stepContexts) {
			try {
				stepContext.validateTerminalState();
			} catch (final RuntimeException e) {
				failure = withFailure(failure, e);
			}
		}
		stepContexts.clear();
		releaseContextCopies();
		released = true;
		if (failure instanceof IOException e) {
			throw e;
		}
		if (failure instanceof RuntimeException e) {
			throw e;
		}
	}

	/** The publishes' copies of the contexts would keep their queues and drivers reachable. */
	private void releaseContextCopies() {
		expectedOperationLifecycleContexts = List.of();
		expectedDeleteObjectLifecycleContexts = List.of();
	}

	/**
	 * After the last close a standalone DELETE in duration mode is retried for, nothing closes the
	 * slice again. Close its contexts without retrying their stops, so their generators and drivers are
	 * closed, then, as a standalone run does after its retries, cancel its lifecycle work. Its stop has
	 * unregistered its metrics, and its step service then unexports it.
	 */
	private void releaseAfterLastFailedDurationClose(final RuntimeException failure) {
		released = true;
		try {
			invokeDurationContextPhase("release", LoadStepContext::releaseAfterFailedStop);
		} catch (final RuntimeException e) {
			withFailure(failure, e);
		}
		try {
			cancelDurationRunCleanupResourcesAfterExhaustion();
		} catch (final RuntimeException e) {
			withFailure(failure, e);
		}
		try {
			closeRetainedDurationPhases();
		} catch (final RuntimeException e) {
			withFailure(failure, e);
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
