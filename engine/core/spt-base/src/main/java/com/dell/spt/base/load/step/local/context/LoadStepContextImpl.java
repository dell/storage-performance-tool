package com.dell.spt.base.load.step.local.context;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.dell.spt.base.Constants.LIFECYCLE_POLL_INTERVAL_MILLIS;
import static com.dell.spt.base.Constants.TASK_STOP_WAIT_SECONDS;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.base.concurrent.AsyncRunnable.State.SHUTDOWN;
import static com.dell.spt.base.concurrent.AsyncRunnable.State.STARTED;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_IDENTITY_MODE_BATCH;
import static com.dell.spt.base.metrics.MetricsConstants.DELETE_IDENTITY_MODE_SINGLE;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.apache.logging.log4j.CloseableThreadContext.Instance;

import com.dell.spt.base.load.lifecycle.OperationLifecycleCounters;
import com.dell.spt.base.concurrent.DaemonBase;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.integrity.IntegrityCsvArtifacts;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.deletion.DeleteArtifactRecorder;
import com.dell.spt.base.item.op.deletion.DeleteInventoryVerifier;
import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import com.dell.spt.base.item.op.deletion.DeleteOperationalOutcomeLedger;
import com.dell.spt.base.item.op.deletion.DeletePhaseTimingSnapshot;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteTargetOutcome;
import com.dell.spt.base.item.op.deletion.DeleteVerificationPhase;
import com.dell.spt.base.item.op.deletion.DeleteVerificationProbe;
import com.dell.spt.base.item.op.deletion.DeleteVerificationReport;
import com.dell.spt.base.item.op.deletion.DeleteVerificationSummary;
import com.dell.spt.base.item.op.deletion.StandaloneDeleteConfig;
import com.dell.spt.base.item.op.list.ListOperation;
import com.dell.spt.base.item.op.list.shard.ListShard;
import com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorder;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.item.op.path.PathOperation;
import com.dell.spt.base.load.failure.ObjectFailureBudgetConfig;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.lifecycle.OperationLifecycleSnapshot;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.load.step.DurationTime;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.logging.OperationTraceCsvBatchLogMessage;
import com.dell.spt.base.logging.OperationTraceCsvLogMessage;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.dell.spt.base.storage.driver.ListDiscoveryProbe;
import com.dell.spt.base.storage.driver.ListOptions;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StandaloneDeletePreparable;
import com.dell.spt.base.util.BinarySizeFormat;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.reflection.TypeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalLong;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;

/** Created by kurila on 12.07.16. */
public class LoadStepContextImpl<I extends Item, O extends Operation<I>> extends DaemonBase
				implements LoadStepContext<I, O> {

	private final String id;
	private final LoadGenerator<I, O> generator;
	private final StorageDriver<I, O> driver;
	private final long countLimit;
	private final long sizeLimit;
	private final long failCountLimit;
	private final boolean failRateLimitFlag;
	private final ConcurrentMap<I, O> latestSuccOpResultByItem;
	private final boolean recycleFlag;
	private final boolean listPathWorkload;
	private final boolean retryFlag;
	private final int retryLimit;
	private final MetricsContext metricsCtx;
	private final Map<OpType, MetricsContext> metricsCtxByOpType;
	private final LongAdder counterResults = new LongAdder();
	private final boolean tracePersistFlag;
	private volatile Output<O> opsResultsOutput;
	private volatile Output<O> opsMetricsOutput;
	private final boolean waitOpFinishBeforeStop;
	private final int waitOpFinishLimit;
	private final boolean outputDuplicates;
	private final boolean updateContents;
	private final ListShardMetricsRecorder listShardMetricsRecorder;
	private final String immutableListRootPrefix;
	// delimiter-first runtime split state (per-shard prefix)
	private final java.util.concurrent.ConcurrentMap<String, Window> splitWindows = new java.util.concurrent.ConcurrentHashMap<>();

	private static final class Window {
		String firstKey;
		String lastKey;
		int pages;
	}

	private final ThreadLocal<SplittableRandom> rand = ThreadLocal.withInitial(SplittableRandom::new);

	// Full-jitter exponential backoff before a load-op-retry re-dispatch, mirroring the
	// shape of common S3-client retry defaults (e.g. minio-go's own 200ms/1s retry timer)
	// rather than immediately re-hitting a target that just failed the operation.
	private static final long RETRY_BACKOFF_BASE_MILLIS = 200L;
	private static final long RETRY_BACKOFF_CAP_MILLIS = 1000L;

	// Tracks load-op-retry redispatches currently sitting in their backoff delay, keyed by
	// the operation awaiting redispatch, so a stop/shutdown can cancel them and resolve
	// them to a definite terminal outcome immediately instead of leaving them to fire later
	// into a driver/generator that's already gone. Value is a handle (not the Future
	// directly) inserted *before* scheduling: see scheduleRetry()'s javadoc for why.
	private final ConcurrentMap<O, RetryHandle> pendingRetries = new ConcurrentHashMap<>();
	// Counts retry tasks that have started running (i.e. already past cancellation via
	// pendingRetries) but haven't yet finished deciding retry() vs markOpFailed(). See
	// awaitRetryTasksSettled()'s javadoc for the shutdown-ordering race this closes.
	private final AtomicInteger activeRetryTasks = new AtomicInteger();
	// Set before admission closes so a scheduled retry cannot enter generator circulation
	// behind the recovery snapshot.
	private final AtomicBoolean stepShuttingDown = new AtomicBoolean(false);
	private final AtomicBoolean retryTrackingWarningLogged = new AtomicBoolean(false);
	private final AtomicBoolean operationAdmissionClosed = new AtomicBoolean(false);
	private final ReentrantLock operationAdmissionCloseLock = new ReentrantLock();
	private final AtomicBoolean generatorQueueRecovered = new AtomicBoolean(false);
	private final AtomicBoolean driverQueueRecovered = new AtomicBoolean(false);
	private List<O> generatorRecoveryPending;
	private List<O> driverRecoveryPending;
	private final AtomicBoolean operationDrainComplete = new AtomicBoolean(false);
	private final AtomicBoolean startedOnce = new AtomicBoolean(false);
	private final AtomicBoolean durationIntervalStarted = new AtomicBoolean(false);
	private final AtomicBoolean objectFailureBudgetAdmissionHeld = new AtomicBoolean(false);
	private final AtomicBoolean objectFailureBudgetAdmissionReleased = new AtomicBoolean(false);
	private final OperationLifecycleTracker<O> operationLifecycle;
	private final boolean standaloneDeleteEnabled;
	private final boolean standaloneDeleteDurationMode;
	private final StandaloneDeleteConfig standaloneDeleteConfig;
	private final ObjectFailureBudgetConfig deleteFailurePolicy;
	private final AtomicLong deleteScheduledStartedNanos = new AtomicLong();
	private final AtomicLong deleteScheduledDeadlineNanos = new AtomicLong();
	private final AtomicLong deleteAdmissionClosedNanos = new AtomicLong();
	private final AtomicLong deleteDrainCompletedNanos = new AtomicLong();
	private final AtomicLong deleteWorkflowStartedEpochNanos = new AtomicLong();
	private final AtomicLong deleteWorkflowCompletedEpochNanos = new AtomicLong();
	private final AtomicBoolean deleteDrainTimestampRecorded = new AtomicBoolean();
	private final DeleteObjectLifecycleCounters deleteObjectLifecycleCounters;
	private final DeleteArtifactRecorder deleteArtifactRecorder;
	private final Path deleteSelectionManifest;
	private final DeleteOperationalOutcomeLedger deleteTargetOutcomes;
	private final AtomicReference<DeleteVerificationReport> deletePreValidationReport = new AtomicReference<>();
	private final AtomicReference<DeleteVerificationReport> deletePostVerificationReport = new AtomicReference<>();
	private final AtomicReference<DeleteVerificationSummary> deleteFinalVerificationSummary = new AtomicReference<>();
	private final AtomicBoolean deletePostVerificationStarted = new AtomicBoolean();
	private final AtomicBoolean deletePostVerificationSkipped = new AtomicBoolean();
	private final Object deletePostVerificationPhaseLock = new Object();

	/**
	 * Per-scheduled-retry state, guaranteeing that exactly one of {the scheduled task's
	 * own body} or {@code LoadStepContextImpl#cancelPendingRetries()} ever resolves a
	 * given retry - never both, and never neither. This is needed because {@link
	 * CompletableFuture#cancel} does not reliably mean "the task body never ran": for a
	 * future produced by {@code runAsync()}, a successful {@code cancel(false)} can still
	 * race with a task that has already started running on its own executor thread
	 * (cancelling the future does not interrupt or otherwise stop that thread), so
	 * treating a successful {@code cancel()} as proof the task body will never execute -
	 * and therefore that it's safe to terminal-fail the operation ourselves - is not safe
	 * on its own.
	 */
	private static final class RetryHandle {
		private enum State {
			SCHEDULED, CLAIMED, CANCELLED
		}

		private final AtomicReference<State> state = new AtomicReference<>(State.SCHEDULED);
		private volatile Future<?> future;

		/** Called from {@link #cancelPendingRetries()}; wins iff the task hasn't already claimed this retry. */
		boolean tryCancel() {
			return state.compareAndSet(State.SCHEDULED, State.CANCELLED);
		}

		/** Called from the scheduled task's own body; wins iff not already cancelled. */
		boolean tryClaim() {
			return state.compareAndSet(State.SCHEDULED, State.CLAIMED);
		}
	}

	/** Schedules a retry redispatch after a backoff delay; overridable for tests so they don't have to wait on real timers. */
	@FunctionalInterface
	interface RetryScheduler {
		Future<?> schedule(long delayMillis, Runnable task);
	}

	@FunctionalInterface
	interface DeleteArtifactRecorderFactory {
		DeleteArtifactRecorder create(String stepId, Path selectionManifest, long selectedCount);
	}

	// Not final: package-private test seam (see setRetryScheduler) swaps this for a
	// synchronous stand-in so retry tests run fast and deterministically. Production always
	// uses this real, delayed default.
	private RetryScheduler retryScheduler = (delayMillis, task) -> {
		if (delayMillis <= 0) {
			task.run();
			return CompletableFuture.completedFuture(null);
		}
		return CompletableFuture.runAsync(task, CompletableFuture.delayedExecutor(delayMillis, TimeUnit.MILLISECONDS));
	};

	/** Test-only seam: replace the real delayed scheduler with e.g. a synchronous one. */
	void setRetryScheduler(final RetryScheduler retryScheduler) {
		this.retryScheduler = retryScheduler;
	}

	/** @param id test step id */
	public LoadStepContextImpl(
					final String id,
					final LoadGenerator<I, O> generator,
					final StorageDriver<I, O> driver,
					final MetricsContext metricsCtx,
					final Config loadConfig,
					final boolean tracePersistFlag) {
		this(
						id, generator, driver, metricsCtx, null, loadConfig, tracePersistFlag, ListShardMetricsRecorder.NO_OP);
	}

	public LoadStepContextImpl(
					final String id,
					final LoadGenerator<I, O> generator,
					final StorageDriver<I, O> driver,
					final MetricsContext metricsCtx,
					final Config loadConfig,
					final boolean tracePersistFlag,
					final ListShardMetricsRecorder shardMetricsRecorder) {
		this(
						id, generator, driver, metricsCtx, null, loadConfig, tracePersistFlag, shardMetricsRecorder, null);
	}

	public LoadStepContextImpl(
					final String id,
					final LoadGenerator<I, O> generator,
					final StorageDriver<I, O> driver,
					final MetricsContext metricsCtx,
					final Config loadConfig,
					final boolean tracePersistFlag,
					final ListShardMetricsRecorder shardMetricsRecorder,
					final String immutableListRootPrefix) {
		this(
						id, generator, driver, metricsCtx, null, loadConfig, tracePersistFlag,
						shardMetricsRecorder, immutableListRootPrefix, null);
	}

	/**
	 * Constructor for mixed-mode workloads where per-op-type metrics routing is needed.
	 *
	 * @param metricsCtxByOpType per-op-type metrics contexts; when non-null, {@code markSucc}/{@code markFail}
	 *                           calls are routed to the MetricsContext for the operation's type
	 */
	public LoadStepContextImpl(
					final String id,
					final LoadGenerator<I, O> generator,
					final StorageDriver<I, O> driver,
					final MetricsContext metricsCtx,
					final Map<OpType, MetricsContext> metricsCtxByOpType,
					final Config loadConfig,
					final boolean tracePersistFlag,
					final ListShardMetricsRecorder shardMetricsRecorder) {
		this(
						id, generator, driver, metricsCtx, metricsCtxByOpType, loadConfig, tracePersistFlag,
						shardMetricsRecorder, null);
	}

	public LoadStepContextImpl(
					final String id,
					final LoadGenerator<I, O> generator,
					final StorageDriver<I, O> driver,
					final MetricsContext metricsCtx,
					final Map<OpType, MetricsContext> metricsCtxByOpType,
					final Config loadConfig,
					final boolean tracePersistFlag,
					final ListShardMetricsRecorder shardMetricsRecorder,
					final String immutableListRootPrefix) {
		this(
						id, generator, driver, metricsCtx, metricsCtxByOpType, loadConfig, tracePersistFlag,
						shardMetricsRecorder, immutableListRootPrefix, null);
	}

	public LoadStepContextImpl(
					final String id,
					final LoadGenerator<I, O> generator,
					final StorageDriver<I, O> driver,
					final MetricsContext metricsCtx,
					final Map<OpType, MetricsContext> metricsCtxByOpType,
					final Config loadConfig,
					final boolean tracePersistFlag,
					final ListShardMetricsRecorder shardMetricsRecorder,
					final String immutableListRootPrefix,
					final Config explicitItemConfig) {
		this(
						id, generator, driver, metricsCtx, metricsCtxByOpType, loadConfig, tracePersistFlag,
						shardMetricsRecorder, immutableListRootPrefix, explicitItemConfig,
						DeleteArtifactRecorder::new);
	}

	LoadStepContextImpl(
					final String id,
					final LoadGenerator<I, O> generator,
					final StorageDriver<I, O> driver,
					final MetricsContext metricsCtx,
					final Map<OpType, MetricsContext> metricsCtxByOpType,
					final Config loadConfig,
					final boolean tracePersistFlag,
					final ListShardMetricsRecorder shardMetricsRecorder,
					final String immutableListRootPrefix,
					final Config explicitItemConfig,
					final DeleteArtifactRecorderFactory deleteArtifactRecorderFactory) {
		this.id = id;
		this.generator = generator;
		this.driver = driver;
		final var driverLifecycle = driver.operationLifecycle();
		this.operationLifecycle = driverLifecycle == null
						? OperationLifecycleTracker.disabled()
						: driverLifecycle;
		this.metricsCtx = metricsCtx;
		this.metricsCtxByOpType = metricsCtxByOpType;
		this.tracePersistFlag = tracePersistFlag;
		this.listShardMetricsRecorder = shardMetricsRecorder == null ? ListShardMetricsRecorder.NO_OP : shardMetricsRecorder;
		this.immutableListRootPrefix = canonicalListPrefix(immutableListRootPrefix);
		final Config opConfig = loadConfig.configVal("op");
		final Config itemConfig = explicitItemConfig != null
						? explicitItemConfig
						: loadConfig.configVal("item");
		final Config recycleConfig = opConfig.configVal("recycle");
		final var itemTypeStr = itemConfig != null ? itemConfig.stringVal("type") : null;
		final ItemType itemType = itemTypeStr != null ? ItemType.valueOf(itemTypeStr.toUpperCase(Locale.ROOT)) : null;
		final var opTypeStr = opConfig.stringVal("type");
		final OpType opType = opTypeStr != null ? OpType.valueOf(opTypeStr.toUpperCase(Locale.ROOT)) : OpType.CREATE;
		final boolean listPathWorkload = OpType.LIST.equals(opType) && (itemType == null || ItemType.PATH.equals(itemType));
		this.listPathWorkload = listPathWorkload;
		this.recycleFlag = listPathWorkload || recycleConfig.boolVal("mode");
		this.updateContents = recycleConfig.boolVal("content-update");
		this.retryFlag = opConfig.boolVal("retry");
		this.retryLimit = opConfig.intVal("retryLimit");
		final var standaloneDelete = StandaloneDeleteConfig.from(loadConfig);
		this.standaloneDeleteConfig = standaloneDelete;
		this.deleteObjectLifecycleCounters = new DeleteObjectLifecycleCounters(
						standaloneDelete.selectedBuckets().keySet());
		this.deleteFailurePolicy = standaloneDelete.enabled()
						? ObjectFailureBudgetConfig.from(loadConfig)
						: null;
		this.standaloneDeleteEnabled = standaloneDelete.enabled();
		this.standaloneDeleteDurationMode = standaloneDelete.durationMode();
		final String deleteSelectionManifestValue;
		if (standaloneDelete.enabled() && itemConfig != null && standaloneDelete.selected() >= 0) {
			final Config inputConfig = itemConfig.configVal("input");
			deleteSelectionManifestValue = inputConfig == null ? null : inputConfig.stringVal("file");
		} else {
			deleteSelectionManifestValue = null;
		}
		this.deleteSelectionManifest = deleteSelectionManifestValue == null
						|| deleteSelectionManifestValue.isBlank()
										? null
										: Path.of(deleteSelectionManifestValue).toAbsolutePath().normalize();
		if ((standaloneDelete.preValidation() || standaloneDelete.postVerification())
						&& (standaloneDelete.selected() < 0
										|| this.deleteSelectionManifest == null)) {
			throw new IllegalConfigurationException(
							"Standalone DELETE verification requires a frozen manifest and exact count");
		}
		standaloneDelete.validateSettings(opType, itemType, this.recycleFlag, this.retryFlag);
		if (standaloneDelete.enabled() && !operationLifecycle.isEnabled()) {
			throw new IllegalConfigurationException(
							"Standalone DELETE requires a driver with operation lifecycle accounting");
		}
		if (standaloneDelete.enabled() && tracePersistFlag) {
			throw new IllegalConfigurationException(
							"Standalone DELETE cannot use generic single-item operation tracing");
		}
		if (standaloneDelete.enabled() && !driver.supportsStandaloneDeleteRequests()) {
			throw new IllegalConfigurationException(
							"Configured storage driver does not support standalone DELETE requests");
		}
		if ((standaloneDelete.preValidation() || standaloneDelete.postVerification())
						&& !(driver instanceof DeleteVerificationProbe)) {
			throw new IllegalConfigurationException(
							"Configured storage driver does not support standalone DELETE verification");
		}
		if (this.retryFlag) {
			if (this.retryLimit < 0) {
				throw new IllegalConfigurationException(
								"load-op-retryLimit must be >= 0 (0 disables retry even though load-op-retry "
												+ "is true), got " + this.retryLimit);
			}
			if (!generator.supportsRetry()) {
				throw new IllegalConfigurationException(
								"load-op-retry is enabled, but the configured load generator ("
												+ generator.getClass().getSimpleName()
												+ ") does not support requeueing failed operations for retry");
			}
		}
		final Config opLimitConfig = opConfig.configVal("limit");
		final int recycleLimit = opLimitConfig.intVal("recycle");
		if (recycleFlag || retryFlag) {
			latestSuccOpResultByItem = new ConcurrentHashMap<>(recycleLimit);
		} else {
			latestSuccOpResultByItem = null;
		}
		final long configCountLimit = opLimitConfig.longVal("count");
		this.countLimit = configCountLimit > 0 ? configCountLimit : Long.MAX_VALUE;
		final SizeInBytes configSizeLimit;
		final Config stepLimitConfig = loadConfig.configVal("step-limit");
		final Object configSizeLimitRaw = stepLimitConfig.val("size");
		if (configSizeLimitRaw instanceof String) {
			configSizeLimit = BinarySizeFormat.parseSize((String) configSizeLimitRaw);
		} else {
			configSizeLimit = new SizeInBytes(TypeUtil.typeConvert(configSizeLimitRaw, long.class));
		}
		this.sizeLimit = configSizeLimit.get() > 0 ? configSizeLimit.get() : Long.MAX_VALUE;
		final Config failConfig = opLimitConfig.configVal("fail");
		final long configFailCount = failConfig.longVal("count");
		this.failCountLimit = standaloneDelete.enabled()
						? Long.MAX_VALUE
						: configFailCount > 0 ? configFailCount : Long.MAX_VALUE;
		this.failRateLimitFlag = !standaloneDelete.enabled() && failConfig.boolVal("rate");
		this.waitOpFinishBeforeStop = opConfig.boolVal("wait-finish");
		this.waitOpFinishLimit = opConfig.intVal("wait-limit");
		this.outputDuplicates = opConfig.boolVal("output-duplicates");
		this.deleteArtifactRecorder = this.deleteSelectionManifest == null
						? null
						: deleteArtifactRecorderFactory.create(
										id, this.deleteSelectionManifest, standaloneDelete.selected());
		try {
			this.deleteTargetOutcomes = standaloneDelete.postVerification()
							? DeleteOperationalOutcomeLedger.create(standaloneDelete.selected())
							: null;
		} catch (final IOException failure) {
			if (deleteArtifactRecorder != null) {
				deleteArtifactRecorder.close();
			}
			throw new IllegalConfigurationException(
							"Unable to allocate bounded DELETE operational outcome storage", failure);
		}
		try {
			if (this.listShardMetricsRecorder != ListShardMetricsRecorder.NO_OP) {
				this.metricsCtx.metadata().put(
								com.dell.spt.base.metrics.MetricsConstants.METADATA_LIST_SHARD_METRICS,
								this.listShardMetricsRecorder);
			}
			if (this.generator != null) {
				this.generator.operationLifecycle(this.operationLifecycle);
			}
			if (standaloneDelete.enabled()) {
				this.metricsCtx.metadata().put(
								com.dell.spt.base.metrics.MetricsConstants.METADATA_DELETE_METRICS,
								(java.util.function.Supplier<DeleteMetricsSnapshot>) this::deleteMetricsSnapshot);
				operationLifecycle.dispatchObserver(this::recordStandaloneDeleteDispatch);
				operationLifecycle.terminalObserver(this::recordStandaloneDeleteTerminal);
			}
			this.driver.operationResultOutput(this);
		} catch (final RuntimeException | Error failure) {
			if (deleteArtifactRecorder != null) {
				deleteArtifactRecorder.close();
			}
			closeDeleteVerificationStorage();
			throw failure;
		}
	}

	/** Resolve the MetricsContext for a given operation type (mixed-mode routing). */
	private MetricsContext resolveMetrics(final OpType opType) {
		if (metricsCtxByOpType != null) {
			final MetricsContext mc = metricsCtxByOpType.get(opType);
			if (mc != null) {
				return mc;
			}
		}
		return metricsCtx;
	}

	@Override
	public boolean isDone() {
		final var generatorFailure = generator.terminalFailure();
		if (generatorFailure != null) {
			throw generatorFailure;
		}
		final var driverFailure = driver.terminalFailure();
		if (driverFailure != null) {
			throw driverFailure;
		}
		if (!STARTED.equals(state()) && !SHUTDOWN.equals(state())) {
			Loggers.MSG.debug("{}: done due to {} state", id, state());
			return true;
		}
		if (isDoneCountLimit()) {
			Loggers.MSG.debug("{}: done due to max count ({}) done state", id, countLimit);
			return true;
		}
		if (isDoneSizeLimit()) {
			Loggers.MSG.debug("{}: done due to max size done state", id);
			return true;
		}
		if (isFailThresholdReached()) {
			Loggers.ERR.warn("{}: done due to \"BAD\" state", id);
			return true;
		}
		if (listPathWorkload && isListNamespaceExhausted()) {
			Loggers.MSG.debug("{}: done after exhausting LIST namespace", id);
			return true;
		}
		if (standaloneDeleteDurationMode && generator.isStopped()) {
			Loggers.MSG.debug(
							"{}: done because the standalone DELETE inventory can no longer schedule requests",
							id);
			return true;
		}
		if (!recycleFlag && allOperationsCompleted()) {
			Loggers.MSG.debug(
							"{}: done due to all {} load operations have been completed",
							id,
							generator.generatedOpCount());
			return true;
		}
		// issue SLTM-938 fix: only bail out when we've actually produced work
		if (recycleFlag && counterResults.sum() > 0 && isNothingToRecycle()) {
			Loggers.ERR.warn("{}: no load operations to recycle (all failed?)", id);
			return true;
		}
		// retry-only (not true recycle-mode) finite workloads: allOperationsCompleted()
		// above needs generator.isStopped(), but a generator with load-op-retry enabled
		// deliberately keeps running past item-input exhaustion (and even past its own
		// countLimit) for as long as a dispatch already in flight could still turn into a
		// retry - see LoadGenerator#retry's javadoc. So it may never self-stop here even
		// once every operation has genuinely resolved. Detect completion directly instead:
		// every operation ever generated (fresh + true-recycle; retries are deliberately
		// *not* separately counted here, see LoadGenerator#retry) has reached a terminal
		// outcome in counterResults, and item generation itself is finished. This
		// deliberately doesn't check activeOpCount()/pendingRetries/isNothingToRecycle():
		// none of those can be trusted to reflect "no outstanding work" on their own (a
		// cooperative driver can hold a queued-but-not-yet-active op invisible to
		// activeOpCount(), for example) the way this direct comparison can, since
		// counterResults is only ever incremented once an operation has truly reached SUCC
		// or a terminal (retries-exhausted-or-not-retryable) failure.
		if (!recycleFlag && retryFlag
						&& generator.isItemInputFinished()
						&& counterResults.longValue() >= generator.generatedOpCount()) {
			Loggers.MSG.debug(
							"{}: done, retry-only workload drained (all {} generated operations resolved)",
							id,
							generator.generatedOpCount());
			return true;
		}
		return false;
	}

	@Override
	public long schedulingExhaustedAtNanos() {
		return generator.schedulingExhaustedAtNanos();
	}

	@Override
	public OptionalLong schedulingExhaustionNanos() {
		return generator.schedulingExhaustionNanos();
	}

	private boolean isDoneCountLimit() {
		if (countLimit > 0) {
			if (counterResults.sum() >= countLimit) {
				Loggers.MSG.debug(
								"{}: count limit reached, {} results >= {} limit",
								id,
								counterResults.sum(),
								countLimit);
				return true;
			}
			final AllMetricsSnapshot lastStats = metricsCtx.lastSnapshot();
			final long succCountSum = lastStats.successSnapshot().count();
			final long failCountSum = lastStats.failsSnapshot().count();
			if (succCountSum + failCountSum >= countLimit) {
				Loggers.MSG.debug(
								"{}: count limit reached, {} successful + {} failed >= {} limit",
								id,
								succCountSum,
								failCountSum,
								countLimit);
				return true;
			}
		}
		return false;
	}

	private boolean isDoneSizeLimit() {
		if (sizeLimit > 0) {
			final long sizeSum = metricsCtx.lastSnapshot().byteSnapshot().count();
			if (sizeSum >= sizeLimit) {
				Loggers.MSG.debug(
								"{}: size limit reached, done {} >= {} limit",
								id,
								BinarySizeFormat.formatFixedSize(sizeSum),
								BinarySizeFormat.formatFixedSize(sizeLimit));
				return true;
			}
		}
		return false;
	}

	private boolean allOperationsCompleted() {
		if (generator.isStopped()) {
			return counterResults.longValue() >= generator.generatedOpCount();
		}
		return false;
	}

	// issue SLTM-938 fix
	private boolean isNothingToRecycle() {
		final long resultCount = counterResults.sum();
		return recycleFlag
						&& generator.isNothingToRecycle()
						&&
						// all generated ops executed at least once
						resultCount > 0
						&& resultCount >= generator.generatedOpCount()
						&&
						// Metadata LIST publishes pages immediately and never populates this map,
						// so this condition is meaningful only for other recycled workloads.
						latestSuccOpResultByItem.size() == 0;
	}

	private boolean isListNamespaceExhausted() {
		if (!listPathWorkload) {
			return false;
		}
		if (counterResults.sum() == 0) {
			return false;
		}
		if (!generator.isItemInputFinished()) {
			return false;
		}
		if (!generator.isNothingToRecycle()) {
			return false;
		}
		if (driver.activeOpCount() > 0) {
			return false;
		}
		return counterResults.sum() >= generator.generatedOpCount();
	}

	/**
	 * @return true if the configured failures threshold is reached and the step should be stopped,
	 *     false otherwise
	 */
	private boolean isFailThresholdReached() {
		final AllMetricsSnapshot allMetricsSnapshot = metricsCtx.lastSnapshot();
		final long failCountSum = allMetricsSnapshot.failsSnapshot().count();
		final double failRateLast = allMetricsSnapshot.failsSnapshot().last();
		final double succRateLast = allMetricsSnapshot.successSnapshot().last();
		if (failCountSum > failCountLimit) {
			Loggers.ERR.warn(
							"{}: failure count ({}) is more than the configured limit ({}), stopping the step",
							id,
							failCountSum,
							failCountLimit);
			return true;
		}
		if (failRateLimitFlag && failRateLast > succRateLast) {
			Loggers.ERR.warn(
							"{}: failures rate ({} failures/sec) is more than success rate ({} op/sec), stopping the step",
							id,
							failRateLast,
							succRateLast);
			return true;
		}
		return false;
	}

	@Override
	public final void operationsResultsOutput(final Output<O> opsResultsOutput) {
		if (standaloneDeleteEnabled && opsResultsOutput != null) {
			throw new IllegalConfigurationException(
							"Standalone DELETE cannot use ordinary successful-item output");
		}
		this.opsResultsOutput = opsResultsOutput;
	}

	@Override
	public final void operationsMetricsOutput(final Output<O> opsMetricsOutput) {
		if (standaloneDeleteEnabled && opsMetricsOutput != null) {
			throw new IllegalConfigurationException(
							"Standalone DELETE cannot use generic single-item timing output");
		}
		this.opsMetricsOutput = opsMetricsOutput;
	}

	@Override
	public final int activeOpCount() {
		return driver.activeOpCount();
	}

	@Override
	public final boolean put(final O opResult) {
		ThreadContext.put(KEY_STEP_ID, id);
		if (standaloneDeleteEnabled) {
			return acceptStandaloneDeleteResult(opResult);
		}
		// I/O trace logging
		if (tracePersistFlag) {
			Loggers.OP_TRACES.info(new OperationTraceCsvLogMessage<>(opResult));
		}
		// account the completed composite ops only
		if (opResult instanceof CompositeOperation
						&& !((CompositeOperation) opResult).allSubOperationsDone()) {
			return true;
		}
		final Status status = opResult.status();
		if (Status.SUCC.equals(status)) {
			// A terminal success ends this operation's current retry episode - clear the
			// counter now so a recycled reuse of this object (read-recycle mode) starts its
			// next attempt with a clean budget instead of accumulating retry counts across
			// unrelated cycles.
			opResult.resetOpRetryCount();
			final long reqDuration = opResult.duration();
			final long respLatency = opResult.latency();
			final long timeToFirstByte = timeToFirstByte(opResult);
			final long countBytesDone;
			if (opResult instanceof DataOperation) {
				countBytesDone = ((DataOperation) opResult).countBytesDone();
			} else if (opResult instanceof ListOperation) {
				countBytesDone = ((ListOperation<?>) opResult).bytesListed();
			} else if (opResult instanceof PathOperation) {
				countBytesDone = ((PathOperation) opResult).countBytesDone();
			} else {
				countBytesDone = 0;
			}
			final boolean recycleEligible;
			if (opResult instanceof ListOperation) {
				recycleEligible = recycleFlag && ((ListOperation<?>) opResult).truncated();
			} else {
				recycleEligible = recycleFlag;
			}
			if (opResult instanceof PartialOperation) {
				resolveMetrics(opResult.type()).markPartSucc(countBytesDone, reqDuration, respLatency, timeToFirstByte);
			} else {
				if (!recycleEligible) {
					// recycled ops should only appear in output.csv only once unless
					// outputDuplicates flag is specified
					outputResults(opResult);
					if (recycleFlag && opResult instanceof ListOperation) {
						latestSuccOpResultByItem.remove(opResult.item());
					}
				} else {
					// for recycled ops we might want to print them once or every time
					// Metadata discovery must publish every page before the mutable LIST op is recycled.
					if (outputDuplicates || (opResult instanceof ListOperation && driver.metadataIntegrityEnabled())) {
						outputResults(opResult);
					} else {
						// this way we only add duplicate items once to the output list
						latestSuccOpResultByItem.put(opResult.item(), opResult);
					}

					// for recycled ops we might also want to update contents before recycling
					if (updateContents) {
						//if (recycleFlag && updateContents) {
						final var dataItem = (DataItem) opResult.item();
						// TODO: possible change: remove dataItem.offset() to improve perf and increase variability
						dataItem.offset(dataItem.offset() + rand.get().nextLong());
					}
					generator.recycle(opResult);
				}

				// each recycled op's lat and dur should be written to file each time
				// just like regular op
				outputTimingMetrics(opResult);
				if (opResult instanceof ListOperation) {
					markListSuccess((ListOperation<?>) opResult, reqDuration, respLatency, timeToFirstByte);
				} else {
					resolveMetrics(opResult.type()).markSucc(countBytesDone, reqDuration, respLatency, timeToFirstByte);
				}
				counterResults.increment();
			}
		} else if (Status.PENDING.equals(status)) {
			// in case driver cannot finish operation due to storage API issues or of some other sort, we need
			// to set the operation status to Pending, so that we don't count it in the metrics and recycle the operation
			if (opResult instanceof ListOperation<?>) {
				final ListShard shardRef = ((ListOperation<?>) opResult).shard();
				if (shardRef != null) {
					listShardMetricsRecorder.onRetry(shardRef, status);
				}
			}
			recyclePendingOp(opResult);
		} else if (Status.OMIT.equals(status)) {
			// operation status is set to Omit in case we want an operation to complete, but not to register
			// in the metrics in any way
			outputResults(opResult);
		} else {
			if (recycleFlag) {
				latestSuccOpResultByItem.remove(opResult.item());
			}
			if (!Status.INTERRUPTED.equals(status)) {
				handleFailedOp(opResult, status);
			}
		}
		return true;
	}

	@Override
	public final int put(final List<O> opResults, final int from, final int to) {
		ThreadContext.put(KEY_STEP_ID, id);
		if (standaloneDeleteEnabled) {
			for (var i = from; i < to; i++) {
				acceptStandaloneDeleteResult(opResults.get(i));
			}
			return to - from;
		}
		// I/O trace logging
		if (tracePersistFlag) {
			Loggers.OP_TRACES.info(new OperationTraceCsvBatchLogMessage<>(opResults, from, to));
		}
		O opResult;
		Status status;
		long reqDuration;
		long respLatency;
		long timeToFirstByte;
		long countBytesDone = 0;
		ListOperation<?> listOpResult = null;
		int i;
		for (i = from; i < to; i++) {
			opResult = opResults.get(i);
			// account the completed composite ops only
			if (opResult instanceof CompositeOperation
							&& !((CompositeOperation) opResult).allSubOperationsDone()) {
				continue;
			}
			status = opResult.status();
			reqDuration = opResult.duration();
			respLatency = opResult.latency();
			timeToFirstByte = timeToFirstByte(opResult);
			countBytesDone = 0;
			listOpResult = null;
			if (opResult instanceof DataOperation) {
				countBytesDone = ((DataOperation) opResult).countBytesDone();
			} else if (opResult instanceof ListOperation) {
				listOpResult = (ListOperation<?>) opResult;
				countBytesDone = listOpResult.bytesListed();
			} else if (opResult instanceof PathOperation) {
				countBytesDone = ((PathOperation) opResult).countBytesDone();
			}
			final boolean recycleEligible;
			if (opResult instanceof ListOperation) {
				recycleEligible = recycleFlag && ((ListOperation<?>) opResult).truncated();
			} else {
				recycleEligible = recycleFlag;
			}
			if (Status.SUCC.equals(status)) {
				// see the single-op put(O) above for why this is reset unconditionally on
				// every terminal success, not just recycle-eligible ones
				opResult.resetOpRetryCount();
				if (opResult instanceof PartialOperation) {
					resolveMetrics(opResult.type()).markPartSucc(countBytesDone, reqDuration, respLatency, timeToFirstByte);
				} else {
					if (!recycleEligible) {
						// recycled ops should only appear in output.csv only once unless
						// outputDuplicates flag is specified
						outputResults(opResult);
						if (recycleFlag && opResult instanceof ListOperation) {
							latestSuccOpResultByItem.remove(opResult.item());
						}
					} else {
						// for recycled ops we might want to print them once or every time
						// Metadata discovery must publish every page before the mutable LIST op is recycled.
						if (outputDuplicates || (opResult instanceof ListOperation && driver.metadataIntegrityEnabled())) {
							outputResults(opResult);
						} else {
							// this way we only add duplicate items once to the output list
							latestSuccOpResultByItem.put(opResult.item(), opResult);
						}

						// for recycled ops we might also want to update contents before recycling
						if (updateContents) {
							//if (recycleFlag && updateContents) {
							final var dataItem = (DataItem) opResult.item();
							// TODO: possible change: remove dataItem.offset() to improve perf and increase variability
							dataItem.offset(dataItem.offset() + rand.get().nextLong());
						}
						if (opResult instanceof ListOperation) {
							final var shardRef = ((ListOperation<?>) opResult).shard();
							if (shardRef != null) {
								listShardMetricsRecorder.onRequeue(shardRef);
							}
						}
						generator.recycle(opResult);
					}

					// each recycled op's lat and dur should be written to file each time
					// just like regular op
					outputTimingMetrics(opResult);
					if (listOpResult != null) {
						markListSuccess(listOpResult, reqDuration, respLatency, timeToFirstByte);
					} else {
						resolveMetrics(opResult.type()).markSucc(countBytesDone, reqDuration, respLatency, timeToFirstByte);
					}
					counterResults.increment();
				}
			} else if (Status.PENDING.equals(status)) {
				// in case driver cannot finish operation due to storage API issues or of some other sort, we need
				// to set the operation status to Pending, so that we don't count it in the metrics and recycle the operation
				if (opResult instanceof ListOperation) {
					final var shardRef = ((ListOperation<?>) opResult).shard();
					if (shardRef != null) {
						listShardMetricsRecorder.onRequeue(shardRef);
					}
				}
				recyclePendingOp(opResult);
			} else if (Status.OMIT.equals(status)) {
				// operation status is set to Omit in case we want an operation to complete, but not to register
				// in the metrics in any way
				outputResults(opResult);
			} else {
				if (recycleFlag) {
					latestSuccOpResultByItem.remove(opResult.item());
				}
				if (!Status.INTERRUPTED.equals(status)) {
					handleFailedOp(opResult, status);
				}
			}
		}
		return i - from;
	}

	@Override
	public final int put(final List<O> opsResults) {
		return put(opsResults, 0, opsResults.size());
	}

	/**
	 * Handles a {@code Status.PENDING} result: the driver couldn't finish the operation (a
	 * storage API issue or similar) and wants it attempted again, without counting it as a
	 * success or failure in the reported metrics. For a true recycle-mode workload, "try
	 * again" naturally happens via the same recycle queue duration-based recycling already
	 * drains, so it's safe to advance {@link #counterResults} immediately, same as before -
	 * a future cycle supersedes it either way. For a non-recycle-mode workload with {@code
	 * load-op-retry} enabled, recycling it would enqueue into a queue {@code
	 * LoadGeneratorImpl} only drains when *its own* {@code recycleFlag} is set (i.e. never,
	 * for this case) - silently stranding the operation forever while {@link
	 * #counterResults} advances as though it had actually resolved. Route through the
	 * dedicated, always-drained {@link LoadGenerator#retry} path instead in that case, and
	 * don't count it until it actually reaches a terminal outcome, mirroring how a
	 * retryable failure is handled. (A non-recycle, non-retry workload's PENDING handling
	 * is intentionally unchanged here - actually retrying it ourselves in that case would
	 * be a separately-scoped behavior change for a longstanding gap that predates {@code
	 * load-op-retry} entirely.)
	 *
	 * <p><strong>Note this deliberately differs from {@link #handleFailedOp}'s retry
	 * behavior</strong>: a {@code PENDING} redispatch here is immediate (no {@link
	 * #retryBackoffMillis} backoff) and unbounded (not counted against {@code
	 * opRetryCount()}/{@code load-op-retryLimit} at all - {@code incrementOpRetryCount()}
	 * is never called for it). {@code PENDING} represents the driver's own signal that the
	 * operation isn't actually finished yet (as opposed to a definite failure worth
	 * backing off from and eventually giving up on), so it's treated the same way true
	 * recycle-mode already treats it: an unlimited, immediate retry loop is the existing,
	 * intentional contract for this status, unrelated to {@code load-op-retry}'s own
	 * bounded-attempts-at-a-genuine-failure semantics.
	 */
	private void recyclePendingOp(final O opResult) {
		if (!recycleFlag && retryFlag) {
			generator.retry(opResult);
		} else {
			generator.recycle(opResult);
			counterResults.increment();
		}
	}

	/**
	 * Whether a failed operation's status is worth retrying at all when {@code
	 * load-op-retry} is on. Matches common S3-client SDK retry policy (minio-go/aws-sdk-cpp
	 * both retry transient I/O/timeout/server-side failures but not auth, not-found, client
	 * request errors, corruption, or out-of-space - see SPT_PERFORMANCE_INVESTIGATION_PLAN_
	 * 202607.md H8 for the comparison this was modeled on). Retrying a permanent failure
	 * only delays an actionable result and wastes the retry budget on something that will
	 * never succeed.
	 */
	private static boolean isRetryableStatus(final Status status) {
		return switch (status) {
		case FAIL_IO, FAIL_TIMEOUT, FAIL_UNKNOWN, RESP_FAIL_UNKNOWN, RESP_FAIL_SVC -> true;
		default -> false;
		};
	}

	/**
	 * Handles a failed (non-{@code INTERRUPTED}, non-{@code PENDING}, non-{@code OMIT})
	 * operation result: either schedules it for retry, or marks it as a terminal failure -
	 * always exactly one of the two. Shared by the single-op and batch {@code put()} paths
	 * so their retry semantics (including the list-shard retry callback) can't drift apart.
	 */
	private void handleFailedOp(final O opResult, final Status status) {
		if (retryFlag && isRetryableStatus(status) && opResult.opRetryCount() < retryLimit) {
			if (!opResult.supportsOpRetryTracking()) {
				warnRetryTrackingUnsupportedOnce(opResult);
			} else {
				if (opResult instanceof ListOperation<?> listOp) {
					final ListShard shardRef = listOp.shard();
					if (shardRef != null) {
						listShardMetricsRecorder.onRetry(shardRef, status);
					}
				}
				opResult.incrementOpRetryCount();
				scheduleRetry(opResult);
				return;
			}
		}
		final String reason;
		if (!retryFlag) {
			reason = null; // preserve the exact original (pre-retry-feature) log format
		} else if (!isRetryableStatus(status)) {
			reason = "status is not retryable";
		} else if (!opResult.supportsOpRetryTracking()) {
			reason = "operation does not support retry tracking";
		} else {
			reason = "retry limit (" + retryLimit + ") reached";
		}
		markOpFailed(opResult, status, reason);
	}

	private void markOpFailed(final O opResult, final Status status, final String reason) {
		if (reason != null) {
			Loggers.ERR.debug("{}: {}, {}", opResult.toString(), status.toString(), reason);
		} else {
			Loggers.ERR.debug("{}: {}", opResult.toString(), status.toString());
		}
		final var metrics = resolveMetrics(opResult.type());
		if (Operation.Status.RESP_FAIL_CORRUPT.equals(status)) {
			metrics.markCorrupt();
			if (driver.metadataIntegrityEnabled() && opResult.integrityVerificationResult() != null) {
				Loggers.INTEGRITY_FAILURES.info(
								IntegrityCsvArtifacts.failureRecord(
												IntegrityCsvArtifacts.nodeIdentity(), id, driver.driverType(), opResult));
			}
		} else {
			metrics.markFail();
		}
		counterResults.increment();
	}

	private void warnRetryTrackingUnsupportedOnce(final O opResult) {
		if (retryTrackingWarningLogged.compareAndSet(false, true)) {
			Loggers.ERR.warn(
							"{}: load-op-retry is enabled but {} does not override Operation's retry-tracking "
											+ "methods (supportsOpRetryTracking() returned false) - failed "
											+ "operations of this type will be counted as failures immediately "
											+ "instead of retried",
							id,
							opResult.getClass().getName());
		}
	}

	/**
	 * Recycles a load-op-retry-eligible failed operation for another attempt after a
	 * full-jitter exponential backoff (see {@link #retryBackoffMillis(int)}), rather than
	 * immediately. The delay is scheduled off the calling thread rather than blocking it:
	 * {@code put()} is invoked from an I/O-completion callback (e.g. a Netty event-loop
	 * thread), and blocking that thread to sleep would stall every other in-flight
	 * operation sharing it, not just the retried one.
	 *
	 * <p>The scheduled task itself re-checks whether the step is shutting down or the
	 * generator has otherwise already stopped right before redispatching: calling {@link
	 * LoadGenerator#retry} on a generator that's stopped, or about to be stopped as part
	 * of the same shutdown this task is racing against, would leave the operation stuck
	 * forever instead of resolving to a definite outcome. ({@code load-op-limit-count}
	 * reached is deliberately *not* one of the reasons the generator would already be
	 * stopped here - see {@code LoadGeneratorImpl}'s own {@code retryFlag} constructor
	 * javadoc.)
	 *
	 * <p>The handle is inserted into {@link #pendingRetries} <em>before</em> scheduling,
	 * and the task removes it by identity (not just by key) once it starts running: this
	 * closes a race where a very-short (possibly zero-jitter) delay lets the task run and
	 * try to remove itself before this method would otherwise have inserted it, which
	 * could otherwise leave a stale, already-resolved entry permanently stuck in the map.
	 * {@link RetryHandle#tryClaim()} - not the removal, and not {@link Future#cancel} - is
	 * what actually guarantees this task and a concurrent {@link #cancelPendingRetries()}
	 * can't both resolve the same operation; see that class's own javadoc for why.
	 */
	private void scheduleRetry(final O opResult) {
		final long delayMillis = retryBackoffMillis(opResult.opRetryCount());
		final RetryHandle handle = new RetryHandle();
		pendingRetries.put(opResult, handle);
		handle.future = retryScheduler.schedule(delayMillis, () -> {
			// Incremented as the very first thing the task body does (not at schedule
			// time): a task successfully cancelled by cancelPendingRetries() before it ever
			// starts running must never have touched this counter, or it would stay
			// elevated forever - awaitRetryTasksSettled() would then wait out its full
			// bound and warn on every stop for no reason.
			activeRetryTasks.incrementAndGet();
			try {
				pendingRetries.remove(opResult, handle);
				if (!handle.tryClaim()) {
					// cancelPendingRetries() already won the race on this exact operation
					// and has resolved (or is about to resolve) it itself - touching it
					// again here would double-resolve it (e.g. markFail() twice, or a
					// markFail() racing a retry that goes on to succeed).
					return;
				}
				if (stepShuttingDown.get() || generator.isStopped()) {
					markOpFailed(opResult, opResult.status(), "step stopping or generator already stopped, cannot retry");
				} else {
					generator.retry(opResult);
				}
			} finally {
				activeRetryTasks.decrementAndGet();
			}
		});
	}

	/**
	 * Cancels every load-op-retry redispatch still sitting in its backoff delay, resolving
	 * each to a definite terminal failure immediately rather than leaving it to fire (or
	 * not) on its own timer after the step has already moved on to shutdown. Called at the
	 * start of {@link #doShutdown()}, <em>before</em> it stops the generator - the public
	 * {@code stop()} lifecycle runs {@code shutdown()} (i.e. {@code doShutdown()}) before
	 * {@link #doStop()}, so this has to happen here to actually run before the generator is
	 * torn down; a previous version of this code ran it from {@link #doStop()} instead,
	 * which is too late (see that method's own comment).
	 *
	 * <p>This alone cannot close every race with a concurrent stop: a task can already be
	 * past the point of no return (removed itself from {@link #pendingRetries}, about to
	 * call {@link LoadGenerator#retry}) at the exact moment this runs. {@link
	 * #awaitRetryTasksSettled()}, called right after this, closes that remaining window,
	 * together with the {@code stepShuttingDown} check the task itself performs.
	 */
	private void cancelPendingRetries() {
		if (pendingRetries.isEmpty()) {
			return;
		}
		for (final var entry : List.copyOf(pendingRetries.entrySet())) {
			final O op = entry.getKey();
			final RetryHandle handle = entry.getValue();
			if (handle.tryCancel()) {
				// Won the race against the task's own tryClaim() (see RetryHandle's
				// javadoc for why this - not Future#cancel's return value - is what
				// actually guarantees exclusivity): the task, whenever it runs (including
				// if it's already running but hasn't reached tryClaim() yet), is now
				// guaranteed to see CANCELLED there and return without touching this
				// operation, so it's safe to give it its definite terminal outcome here.
				pendingRetries.remove(op, handle);
				final Future<?> future = handle.future;
				if (future != null) {
					// Best-effort only, to free the underlying scheduled timer/thread
					// promptly - correctness no longer depends on what this returns.
					future.cancel(false);
				}
				markOpFailed(op, op.status(), "step stopping, cancelled pending retry");
			}
			// else: the task already claimed this one first - its own body resolves it
			// (or already has); nothing more to do here.
		}
	}

	/**
	 * Waits briefly for a retry task which claimed its operation before cancellation to
	 * observe the closed admission gate and choose its terminal or unattempted outcome.
	 */
	private void awaitRetryTasksSettled() {
		// These tasks run near-instantaneously once started (a single generator.isStopped()
		// check plus either markOpFailed() or an enqueue) - this closes a narrow scheduling
		// race, it is not a real drain wait, so a short bound is enough.
		final long deadline = System.currentTimeMillis()
						+ TimeUnit.SECONDS.toMillis(TASK_STOP_WAIT_SECONDS);
		while (activeRetryTasks.get() > 0 && System.currentTimeMillis() < deadline) {
			try {
				Thread.sleep(LIFECYCLE_POLL_INTERVAL_MILLIS);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
		if (activeRetryTasks.get() > 0) {
			Loggers.ERR.warn(
							"{}: {} retry task(s) still in flight after the settle wait; proceeding with shutdown anyway",
							id,
							activeRetryTasks.get());
		}
	}

	/**
	 * Compatibility fallback for custom generators whose admission/recovery methods do not
	 * expose their legacy retry queue. The built-in generator is already empty after
	 * admission closure and recovery; legacy implementations retain the former bounded
	 * drain and terminal-failure behavior rather than silently dropping retry work.
	 */
	private void awaitRetryQueueDrained() {
		final long deadline = System.currentTimeMillis()
						+ TimeUnit.SECONDS.toMillis(TASK_STOP_WAIT_SECONDS);
		while (!generator.isNothingPendingRetry() && System.currentTimeMillis() < deadline) {
			try {
				Thread.sleep(LIFECYCLE_POLL_INTERVAL_MILLIS);
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				return;
			}
		}
		if (!generator.isNothingPendingRetry()) {
			final List<O> strandedRetries = generator.drainPendingRetries();
			if (!strandedRetries.isEmpty()) {
				Loggers.ERR.warn(
								"{}: generator still had {} pending retry redispatch(es) after the drain "
												+ "wait; terminal-failing them instead of leaving them stranded",
								id,
								strandedRetries.size());
				for (final O op : strandedRetries) {
					markOpFailed(op, op.status(), "step stopping, retry queue drain timed out");
				}
			}
		}
	}

	/**
	 * Full-jitter exponential backoff delay (milliseconds) before the {@code attempt}-th
	 * load-op-retry re-dispatch (1-based: {@code attempt == opResult.opRetryCount()} right
	 * after it was incremented for this try). Mirrors the shape of common S3-client retry
	 * defaults (e.g. minio-go's own retry timer: 200ms base, 1s cap, full jitter) rather than
	 * inventing a new backoff policy from scratch. Package-private for direct unit testing.
	 */
	static long retryBackoffMillis(final int attempt) {
		final int shift = Math.min(Math.max(attempt - 1, 0), 16); // guard against overflow
		final long exp = RETRY_BACKOFF_BASE_MILLIS << shift;
		final long capped = Math.min(exp, RETRY_BACKOFF_CAP_MILLIS);
		return capped <= 0 ? 0 : ThreadLocalRandom.current().nextLong(capped + 1);
	}

	@Override
	protected void doStart() throws IllegalStateException {
		final boolean standaloneDeletePreparationRequired = standaloneDeleteEnabled && driver instanceof StandaloneDeletePreparable;
		if (!startedOnce.compareAndSet(false, true)) {
			throw new IllegalStateException(
							id + ": load-step context instances cannot be restarted; create a new context");
		}
		stepShuttingDown.set(false);
		operationAdmissionClosed.set(false);
		generatorQueueRecovered.set(false);
		driverQueueRecovered.set(false);
		generatorRecoveryPending = null;
		driverRecoveryPending = null;
		operationDrainComplete.set(false);
		durationIntervalStarted.set(false);
		objectFailureBudgetAdmissionReleased.set(false);
		deletePreValidationReport.set(null);
		deletePostVerificationReport.set(null);
		deletePostVerificationStarted.set(false);
		deletePostVerificationSkipped.set(false);
		operationLifecycle.reset();
		if (standaloneDeleteEnabled) {
			final long deleteStepStartedEpochNanos = DurationTime.monotonicEpochNanos();
			final long configuredWorkflowStartedEpochNanos = standaloneDeleteConfig.workflowStartedEpochNanos();
			deleteWorkflowStartedEpochNanos.set(
							configuredWorkflowStartedEpochNanos != -1
											&& configuredWorkflowStartedEpochNanos <= deleteStepStartedEpochNanos
															? configuredWorkflowStartedEpochNanos
															: deleteStepStartedEpochNanos);
			deleteScheduledStartedNanos.set(
							standaloneDeleteDurationMode
											|| objectFailureBudgetAdmissionHeld.get()
											|| standaloneDeletePreparationRequired
															? 0
															: System.nanoTime());
			deleteScheduledDeadlineNanos.set(0);
			deleteAdmissionClosedNanos.set(0);
			deleteDrainCompletedNanos.set(0);
			deleteWorkflowCompletedEpochNanos.set(0);
			deleteDrainTimestampRecorded.set(false);
		}
		if (standaloneDeleteDurationMode
						|| objectFailureBudgetAdmissionHeld.get()
						|| standaloneDeletePreparationRequired) {
			generator.holdAdmission();
		} else {
			generator.openAdmission();
		}
		try {
			driver.start();
		} catch (final RemoteException e) {
			throw new IllegalStateException(
							String.format("%s: failed to start the storage driver \"%s\"", id, driver),
							e);
		} catch (final IllegalStateException e) {
			LogUtil.exception(Level.WARN, e, "{}: failed to start the storage driver \"{}\"", id, driver);
		}
		prepareStandaloneDelete();
		if (!standaloneDeleteDurationMode && !objectFailureBudgetAdmissionHeld.get()) {
			try {
				if (standaloneDeletePreparationRequired) {
					deleteScheduledStartedNanos.set(System.nanoTime());
					generator.openAdmission();
				}
				generator.start();
			} catch (final IllegalStateException e) {
				LogUtil.exception(
								Level.WARN, e, "{}: failed to start the load generator \"{}\"", id, generator);
			}
		}
	}

	private void prepareStandaloneDelete() {
		if (!standaloneDeleteEnabled || !(driver instanceof StandaloneDeletePreparable preparable)) {
			return;
		}
		try {
			preparable.prepareStandaloneDelete();
		} catch (final Throwable preparationFailure) {
			throwUncheckedIfInterrupted(preparationFailure);
			try {
				driver.close();
			} catch (final Throwable closeFailure) {
				throwUncheckedIfInterrupted(closeFailure);
				if (closeFailure != preparationFailure) {
					preparationFailure.addSuppressed(closeFailure);
				}
			}
			throwUnchecked(preparationFailure);
		}
	}

	@Override
	public final void holdObjectFailureBudgetAdmission() {
		if (!standaloneDeleteEnabled || standaloneDeleteDurationMode) {
			return;
		}
		if (startedOnce.get()) {
			throw new IllegalStateException(id + ": object failure-budget admission must be held before start");
		}
		objectFailureBudgetAdmissionHeld.set(true);
	}

	@Override
	public final void validateDeleteInventoryBeforeAdmission() {
		if (!standaloneDeleteConfig.preValidation()) {
			return;
		}
		final DeleteVerificationReport report;
		try {
			report = DeleteInventoryVerifier.verify(
							deleteSelectionManifest,
							standaloneDeleteConfig.selected(),
							DeleteVerificationPhase.PRE_DELETE,
							Duration.ofMillis(standaloneDeleteConfig.verificationTimeoutMillis()),
							(DeleteVerificationProbe) driver);
		} catch (final Exception failure) {
			throwUncheckedIfInterrupted(failure);
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.INPUT,
							id,
							"Standalone DELETE pre-validation could not complete its full inventory pass",
							failure);
		}
		deletePreValidationReport.set(report);
		if (!report.successful()) {
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.INPUT,
							id,
							"Standalone DELETE strict pre-validation failed for "
											+ report.failureCount() + " selected identity/identities before timing",
							null);
		}
	}

	@Override
	public final void verifyDeleteInventoryAfterDrain() {
		final DeleteVerificationReport preValidation = deletePreValidationReport.get();
		if (!claimDeletePostVerification(preValidation)) {
			return;
		}
		final DeleteVerificationReport report;
		try {
			report = DeleteInventoryVerifier.verify(
							deleteSelectionManifest,
							standaloneDeleteConfig.selected(),
							DeleteVerificationPhase.POST_DELETE,
							Duration.ofMillis(standaloneDeleteConfig.verificationTimeoutMillis()),
							(DeleteVerificationProbe) driver);
		} catch (final Exception failure) {
			throwUncheckedIfInterrupted(failure);
			deletePostVerificationStarted.set(false);
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.EXECUTION,
							id,
							"Standalone DELETE post-verification could not complete its full inventory pass",
							failure);
		}
		deletePostVerificationReport.set(report);
		deleteWorkflowCompletedEpochNanos.set(DurationTime.monotonicEpochNanos());
	}

	private boolean claimDeletePostVerification(final DeleteVerificationReport preValidation) {
		synchronized (deletePostVerificationPhaseLock) {
			return standaloneDeleteConfig.postVerification()
							&& !deletePostVerificationSkipped.get()
							&& (!standaloneDeleteConfig.preValidation()
											|| (preValidation != null && preValidation.successful()))
							&& deletePostVerificationReport.get() == null
							&& deletePostVerificationStarted.compareAndSet(false, true);
		}
	}

	@Override
	public final void skipDeleteInventoryPostVerificationAfterStrictPreValidationFailure() {
		if (!standaloneDeleteConfig.preValidation() || !standaloneDeleteConfig.postVerification()) {
			return;
		}
		synchronized (deletePostVerificationPhaseLock) {
			if (deletePostVerificationStarted.get() || deletePostVerificationReport.get() != null
							|| deleteFinalVerificationSummary.get() != null) {
				throw new IllegalStateException(
								id + ": distributed strict pre-validation abort arrived after post-verification started");
			}
			deletePostVerificationSkipped.set(true);
		}
	}

	private DeleteVerificationSummary deleteVerificationSummary() {
		final var finalSummary = deleteFinalVerificationSummary.get();
		if (finalSummary != null) {
			return finalSummary;
		}
		if (!standaloneDeleteConfig.preValidation() && !standaloneDeleteConfig.postVerification()) {
			return DeleteVerificationSummary.disabled();
		}
		final var summary = DeleteVerificationSummary.classify(
						standaloneDeleteConfig.preValidation(),
						standaloneDeleteConfig.postVerification(),
						standaloneDeleteConfig.verificationTimeoutMillis(),
						deletePreValidationReport.get(),
						deletePostVerificationReport.get(),
						standaloneDeleteConfig.selected(),
						deleteTargetOutcomes);
		return deletePostVerificationSkipped.get()
						? summary.withPostVerificationSkipped()
						: summary;
	}

	private DeleteVerificationSummary finalizeDeleteVerificationSummary() {
		synchronized (deleteFinalVerificationSummary) {
			var finalSummary = deleteFinalVerificationSummary.get();
			if (finalSummary == null) {
				finalSummary = deleteVerificationSummary();
				deleteFinalVerificationSummary.set(finalSummary);
			}
			return finalSummary;
		}
	}

	@Override
	public final void releaseObjectFailureBudgetAdmission() {
		if (!standaloneDeleteEnabled || standaloneDeleteDurationMode) {
			return;
		}
		operationAdmissionCloseLock.lock();
		try {
			if (!objectFailureBudgetAdmissionHeld.get()) {
				return;
			}
			if (objectFailureBudgetAdmissionReleased.get()) {
				return;
			}
			if (!startedOnce.get() || stepShuttingDown.get() || operationAdmissionClosed.get()) {
				throw new IllegalStateException(
								id + ": object failure-budget admission closed before the controller release");
			}
			final long releasedNanos = System.nanoTime();
			deleteScheduledStartedNanos.set(releasedNanos);
			generator.openAdmission();
			generator.start();
			objectFailureBudgetAdmissionReleased.set(true);
		} finally {
			operationAdmissionCloseLock.unlock();
		}
	}

	@Override
	public final void startDurationInterval(
					final long startNanos, final long deadlineNanos) {
		if (!DurationTime.isPositiveRange(startNanos, deadlineNanos)) {
			throw new IllegalArgumentException("duration interval requires a positive monotonic range");
		}
		operationAdmissionCloseLock.lock();
		try {
			if (!standaloneDeleteDurationMode || durationIntervalStarted.get()) {
				return;
			}
			final long releasedNanos = System.nanoTime();
			if (stepShuttingDown.get()
							|| operationAdmissionClosed.get()
							|| DurationTime.deadlineReached(deadlineNanos, releasedNanos)) {
				throw new IllegalStateException(
								id + ": duration scheduling release missed its absolute worker deadline");
			}
			deleteScheduledStartedNanos.set(releasedNanos);
			deleteScheduledDeadlineNanos.set(deadlineNanos);
			operationLifecycle.enforceDispatchDeadline(deadlineNanos);
			enforceDispatchedOperationsDeadlineForStepStop(DurationTime.deadlineAfter(
							deadlineNanos,
							TimeUnit.SECONDS.toNanos(Math.max(0, waitOpFinishLimit))));
			generator.openAdmissionUntil(deadlineNanos);
			if (DurationTime.deadlineReached(deadlineNanos, System.nanoTime())) {
				generator.closeAdmission();
				throw new IllegalStateException(
								id + ": duration scheduling release crossed its absolute worker deadline");
			}
			generator.start();
			durationIntervalStarted.set(true);
		} finally {
			operationAdmissionCloseLock.unlock();
		}
	}

	private void outputResults(final O opResult) {
		final var opsResultsOutput = this.opsResultsOutput;
		if (opsResultsOutput != null) {
			try {
				if (!opsResultsOutput.put(opResult)) {
					if (driver.metadataIntegrityEnabled()) {
						throw new IntegrityTerminalException(
										IntegrityTerminalException.Category.PUBLICATION,
										id,
										"integrity manifest output rejected a successful operation",
										null);
					}
					Loggers.ERR.warn("Failed to output the I/O result");
				}
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				if (driver.metadataIntegrityEnabled() && e instanceof IOException) {
					throw new IntegrityTerminalException(
									IntegrityTerminalException.Category.PUBLICATION,
									id,
									"failed to publish a successful operation to the integrity manifest",
									e);
				}
				if (e instanceof EOFException) {
					LogUtil.exception(Level.DEBUG, e, "Load operations results destination end of input");
				} else if (e instanceof IOException) {
					LogUtil.exception(
									Level.WARN, e, "Failed to put the load operation to the destination");
				} else {
					throw e;
				}
			}
		}
	}

	private void outputTimingMetrics(final O opResult) {
		final var opsMetricsOutput = this.opsMetricsOutput;
		if (opsMetricsOutput != null) {
			try {
				if (!opsMetricsOutput.put(opResult)) {
					Loggers.ERR.warn("Failed to output the operation metrics");
				}
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				if (e instanceof EOFException) {
					LogUtil.exception(Level.DEBUG, e, "Load operations metrics result destination end of input");
				} else if (e instanceof IOException) {
					LogUtil.exception(
									Level.WARN, e, "Failed to put the load operation metrics to the destination");
				} else {
					throw e;
				}
			}
		}
	}

	@Override
	public final OperationLifecycleSnapshot<O> operationLifecycle() {
		return operationLifecycle.snapshot();
	}

	@Override
	public final OperationLifecycleCounters terminalOperationCounters() {
		return isStopped() && operationDrainComplete.get() ? operationLifecycle.counters() : null;
	}

	@Override
	public final DeleteObjectLifecycleSnapshot deleteObjectLifecycle() {
		if (!standaloneDeleteEnabled) {
			return DeleteObjectLifecycleSnapshot.empty();
		}
		final var operationSnapshot = operationLifecycle.snapshot();
		final var selected = Math.addExact(
						generator.consumedItemCount(), generator.aggregateUnattemptedItemCount());
		final var terminalCounters = deleteObjectLifecycleCounters.snapshot();
		final var accepted = terminalCounters.accepted();
		final var failed = terminalCounters.failed();
		final var unattempted = Math.addExact(
						deleteTargetCount(operationSnapshot.unattemptedOperations()),
						generator.aggregateUnattemptedItemCount());
		final var unresolved = deleteTargetCount(operationSnapshot.unresolvedOperations());
		final var attempted = accepted + failed + unresolved;
		final var reconciled = selected == accepted + failed + unattempted + unresolved
						&& attempted == accepted + failed + unresolved;
		final var verification = deleteVerificationSummary();
		return new DeleteObjectLifecycleSnapshot(
						selected,
						attempted,
						accepted,
						failed,
						unattempted,
						unresolved,
						terminalCounters.protocolFailed(),
						terminalCounters.fullSuccessfulRequests(),
						verification.preValidationFailures(),
						verification.correctnessFailures(),
						verification.inconclusiveFailures(),
						reconciled);
	}

	@Override
	public final DeletePhaseTimingSnapshot deletePhaseTiming() {
		if (!standaloneDeleteEnabled) {
			return DeletePhaseTimingSnapshot.empty();
		}
		final long started = deleteScheduledStartedNanos.get();
		final long admissionClosed = deleteAdmissionClosedNanos.get();
		final long drainCompleted = deleteDrainCompletedNanos.get();
		final boolean schedulingStarted = standaloneDeleteDurationMode
						? durationIntervalStarted.get()
						: startedOnce.get();
		if (!schedulingStarted || !operationAdmissionClosed.get()) {
			return DeletePhaseTimingSnapshot.empty();
		}
		final long scheduledBoundary = scheduledDeleteBoundaryNanos(started, admissionClosed);
		final long drainBoundary = deleteDrainTimestampRecorded.get()
						? drainCompleted
						: scheduledBoundary;
		return new DeletePhaseTimingSnapshot(
						DurationTime.elapsedNanos(started, scheduledBoundary),
						DurationTime.elapsedNanos(scheduledBoundary, drainBoundary));
	}

	private long currentScheduledDeleteNanos() {
		return currentScheduledDeleteNanos(System.nanoTime());
	}

	long currentScheduledDeleteNanos(final long currentNanos) {
		final long started = deleteScheduledStartedNanos.get();
		final boolean schedulingStarted = standaloneDeleteDurationMode
						? durationIntervalStarted.get()
						: startedOnce.get();
		if (!schedulingStarted) {
			return 0;
		}
		final long admissionClosed = deleteAdmissionClosedNanos.get();
		final long observedBoundary;
		if (operationAdmissionClosed.get()) {
			observedBoundary = admissionClosed;
		} else {
			observedBoundary = currentNanos;
		}
		final long scheduledBoundary = scheduledDeleteBoundaryNanos(started, observedBoundary);
		return DurationTime.elapsedNanos(started, scheduledBoundary);
	}

	private long scheduledDeleteBoundaryNanos(
					final long started, final long observedBoundary) {
		if (standaloneDeleteDurationMode) {
			final long scheduledDeadline = deleteScheduledDeadlineNanos.get();
			return DurationTime.deadlineReached(scheduledDeadline, observedBoundary)
							? scheduledDeadline
							: observedBoundary;
		}
		final var schedulingExhaustion = generator.schedulingExhaustionNanos();
		if (schedulingExhaustion.isPresent()) {
			final long exhaustedAt = schedulingExhaustion.getAsLong();
			if (DurationTime.deadlineReached(started, exhaustedAt)
							&& DurationTime.deadlineReached(exhaustedAt, observedBoundary)) {
				return exhaustedAt;
			}
		}
		return observedBoundary;
	}

	@Override
	public final void validateTerminalState() {
		if (!standaloneDeleteEnabled) {
			return;
		}
		final var verification = deleteVerificationSummary();
		if (standaloneDeleteConfig.preValidation()
						&& !verification.preValidationComplete()) {
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.EXECUTION,
							id,
							"Standalone DELETE pre-validation evidence is incomplete",
							null);
		}
		if (standaloneDeleteConfig.postVerification()
						&& !verification.postVerificationSkipped()
						&& (!standaloneDeleteConfig.preValidation()
										|| verification.preValidationFailures() == 0)
						&& !verification.postVerificationComplete()) {
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.EXECUTION,
							id,
							"Standalone DELETE post-verification evidence is incomplete",
							null);
		}
		if (verification.requiresFailedTerminalOutcome()) {
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.EXECUTION,
							id,
							"Standalone DELETE post-verification reported correctness or inconclusive failures",
							null);
		}
		final var objects = deleteObjectLifecycle();
		if (objects.unresolved() > 0) {
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.EXECUTION,
							id,
							"Standalone DELETE has " + objects.unresolved()
											+ " unresolved object identity/identities after the configured "
											+ waitOpFinishLimit + "-second drain bound",
							null);
		}
		if (!objects.reconciled()) {
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.EXECUTION,
							id,
							"Standalone DELETE terminal object lifecycle does not reconcile",
							null);
		}
	}

	private boolean acceptStandaloneDeleteResult(final O operation) {
		if (!(operation instanceof DeleteRequestOperation)) {
			throw new IllegalStateException(
							"Standalone DELETE result output received a non-DELETE-request operation");
		}
		return true;
	}

	private void recordStandaloneDeleteTerminal(final O operation) {
		if (!(operation instanceof DeleteRequestOperation deleteOperation)) {
			throw new IllegalStateException(
							"Standalone DELETE terminal lifecycle received a non-DELETE-request operation");
		}
		if (deleteOperation.deleteResult() == null) {
			deleteOperation.completeDelete(null);
		}
		final var result = deleteOperation.deleteResult();
		deleteObjectLifecycleCounters.recordTerminal(result);
		if (deleteTargetOutcomes != null) {
			for (final var targetResult : result.targetResults()) {
				final long selectionIndex = targetResult.target().selectionIndex();
				if (selectionIndex >= 0 && selectionIndex < standaloneDeleteConfig.selected()) {
					deleteTargetOutcomes.markTerminal(
									selectionIndex,
									targetResult.outcome() == DeleteTargetOutcome.ACCEPTED);
				}
			}
		}
		final var metrics = resolveMetrics(operation.type());
		final long requestDuration = standaloneDeleteRequestDuration(deleteOperation);
		final long requestLatency = standaloneDeleteRequestLatency(deleteOperation);
		if (Status.SUCC.equals(result.operationStatus())) {
			metrics.markSucc(
							0,
							requestDuration,
							requestLatency,
							timeToFirstByte(operation));
		} else {
			metrics.markFail(requestDuration, requestLatency);
		}
		counterResults.increment();
		if (deleteArtifactRecorder != null) {
			deleteArtifactRecorder.recordTerminal(deleteOperation);
		}
	}

	private static long standaloneDeleteRequestLatency(final DeleteRequestOperation operation) {
		final long requestLatency = operation.transportRequestLatency();
		if (requestLatency <= 0) {
			return 0;
		}
		return standaloneDeleteRequestDuration(operation) >= requestLatency
						? requestLatency
						: 0;
	}

	private static long standaloneDeleteRequestDuration(final DeleteRequestOperation operation) {
		final long requestStart = operation.reqTimeStart();
		final long responseDone = operation.respTimeDone();
		return requestStart > 0 && responseDone > requestStart ? responseDone - requestStart : 0;
	}

	private void recordStandaloneDeleteDispatch(final O operation) {
		if (!(operation instanceof DeleteRequestOperation deleteOperation)) {
			throw new IllegalStateException(
							"Standalone DELETE dispatch lifecycle received a non-DELETE-request operation");
		}
		deleteObjectLifecycleCounters.recordDispatch(
						deleteOperation.deleteRequest(), standaloneDeleteConfig.batchSize());
		if (deleteTargetOutcomes != null) {
			for (final var target : deleteOperation.deleteRequest().targets()) {
				final long selectionIndex = target.selectionIndex();
				if (selectionIndex >= 0 && selectionIndex < standaloneDeleteConfig.selected()) {
					deleteTargetOutcomes.markDispatched(selectionIndex);
				}
			}
		}
	}

	private DeleteMetricsSnapshot deleteMetricsSnapshot() {
		if (!standaloneDeleteEnabled) {
			return null;
		}
		final var objects = deleteObjectLifecycle();
		final var requests = operationLifecycle.snapshot();
		final var counters = deleteObjectLifecycleCounters.snapshot();
		final boolean frozenSelection = standaloneDeleteConfig.frozenSelectionAvailable();
		if (!frozenSelection && counters.attemptedObjects() != objects.selected()) {
			// Dispatch counters cannot classify the version or bucket identity of an unread
			// manifest suffix. Omit detail until every selected identity is represented instead
			// of fabricating current/exact-version or overflow counts.
			return null;
		}
		final var phaseTiming = deletePhaseTiming();
		final long selectedObjects = standaloneDeleteConfig.selected() >= 0
						? standaloneDeleteConfig.selected()
						: objects.selected();
		final double elapsedSeconds = currentScheduledDeleteNanos()
						/ (double) TimeUnit.SECONDS.toNanos(1);
		final double requestsPerSecond = elapsedSeconds > 0
						? counters.attemptedRequests() / elapsedSeconds
						: 0.0;
		final double objectsPerSecond = elapsedSeconds > 0
						? counters.attemptedObjects() / elapsedSeconds
						: 0.0;
		final long operationalFailedObjects = Math.subtractExact(
						objects.failed(), objects.protocolFailed());
		final var verification = deleteVerificationSummary();
		final var preReport = deletePreValidationReport.get();
		final var postReport = deletePostVerificationReport.get();
		final long preValidationNanos = preReport == null ? -1 : preReport.elapsedNanos();
		final long postVerificationNanos = postReport == null ? -1 : postReport.elapsedNanos();
		final long seedNanos = optionalPhaseNanos(standaloneDeleteConfig.seedMillis());
		final long discoveryNanos = optionalPhaseNanos(standaloneDeleteConfig.discoveryMillis());
		final long totalWallNanos = currentDeleteWorkflowNanos();
		final boolean scheduledPhaseMeasured = operationAdmissionClosed.get();
		final boolean drainAndTotalMeasured = deleteDrainTimestampRecorded.get();
		final var builder = DeleteMetricsSnapshot.builder(standaloneDeleteConfig.batchSize())
						.identity(
										standaloneDeleteConfig.batchSize() == 1
														? DELETE_IDENTITY_MODE_SINGLE
														: DELETE_IDENTITY_MODE_BATCH,
										standaloneDeleteConfig.selectionOrder())
						.requests(
										counters.attemptedRequests(),
										counters.fullSuccessfulRequests(),
										counters.partialRequests(),
										counters.failedRequests(),
										requests.unresolved(),
										requestsPerSecond)
						.objects(
										selectedObjects,
										counters.attemptedObjects(),
										objects.accepted(),
										objects.failed(),
										objects.unattempted(),
										objects.unresolved(),
										objectsPerSecond)
						.batches(
										counters.attemptedRequests(),
										counters.attemptedObjects(),
										counters.fullBatchRequests(),
										counters.partialBatchRequests())
						.versions(
										frozenSelection
														? standaloneDeleteConfig.selectedCurrentKey()
														: counters.currentKeyTargets(),
										frozenSelection
														? standaloneDeleteConfig.selectedExactVersion()
														: counters.exactVersionTargets())
						.phases(
										seedNanos,
										discoveryNanos,
										preValidationNanos,
										scheduledPhaseMeasured ? phaseTiming.scheduledNanos() : -1,
										drainAndTotalMeasured ? phaseTiming.drainNanos() : -1,
										postVerificationNanos,
										-1,
										drainAndTotalMeasured ? totalWallNanos : -1)
						.failurePolicy(
										deleteFailurePolicy.mode().wireValue(),
										deleteFailurePolicy.maxFailedObjects(),
										deleteFailurePolicy.maxFailurePercent(),
										deleteFailurePolicy.grace().toSeconds(),
										operationalFailedObjects,
										objects.protocolFailed())
						.verification(verification)
						.reconciled(objects.reconciled());

		final var bucketMetrics = new TreeMap<String, long[]>();
		standaloneDeleteConfig.selectedBuckets().forEach(
						(name, selected) -> bucketMetrics.put(name, new long[]{selected, 0, 0, 0
						}));
		counters.buckets().forEach((name, counts) -> {
			final long[] values = bucketMetrics.computeIfAbsent(
							name, ignored -> new long[]{
									standaloneDeleteConfig.selectedBuckets().isEmpty() ? counts.attempted() : 0,
									0, 0, 0
			});
			values[1] = counts.attempted();
			values[2] = counts.accepted();
			values[3] = counts.failed();
		});
		long selectedByBucket = bucketMetrics.values().stream().mapToLong(values -> values[0]).sum();
		if (selectedByBucket < selectedObjects) {
			final long[] overflow = bucketMetrics.computeIfAbsent(
							DeleteMetricsSnapshot.OVERFLOW_BUCKET, ignored -> new long[4]);
			overflow[0] = Math.addExact(overflow[0], selectedObjects - selectedByBucket);
		}
		bucketMetrics.forEach((name, values) -> builder.bucket(
						name, values[0], values[1], values[2], values[3]));
		return builder.build();
	}

	private static long optionalPhaseNanos(final long millis) {
		return millis < 0 ? -1 : TimeUnit.MILLISECONDS.toNanos(millis);
	}

	private long currentDeleteWorkflowNanos() {
		if (!startedOnce.get()) {
			return 0;
		}
		final long boundary = deleteDrainTimestampRecorded.get()
						? deleteWorkflowCompletedEpochNanos.get()
						: DurationTime.monotonicEpochNanos();
		return DurationTime.elapsedNanos(deleteWorkflowStartedEpochNanos.get(), boundary);
	}

	private static long deleteTargetCount(final List<? extends Operation<?>> operations) {
		return operations.stream()
						.filter(DeleteRequestOperation.class::isInstance)
						.map(DeleteRequestOperation.class::cast)
						.mapToLong(operation -> operation.deleteRequest().targets().size())
						.sum();
	}

	/** Closes both admission gates without recovering or draining queued work. */
	private void closeOperationAdmission() {
		operationAdmissionCloseLock.lock();
		try {
			if (operationAdmissionClosed.get()) {
				return;
			}
			try {
				// Close the downstream gate first. A generator handoff which already started
				// may be blocked in an extension's Output implementation; closing the driver's
				// atomic gate first prevents that handoff from crossing dispatch while allowing
				// generator admission closure and recovery to remain bounded.
				driver.closeAdmission();
			} finally {
				try {
					generator.closeAdmission();
				} finally {
					// Compatibility backstop for generators whose closeAdmission() predates the
					// default implementation or is supplied by a mocking/proxy framework.
					generator.stop();
				}
			}
			if (standaloneDeleteEnabled) {
				deleteAdmissionClosedNanos.set(System.nanoTime());
			}
			operationAdmissionClosed.set(true);
		} finally {
			operationAdmissionCloseLock.unlock();
		}
	}

	/** Recovers work which remained before the actual driver-dispatch boundary. */
	private synchronized void recoverQueuedOperations() {
		RuntimeException recoveryFailure = null;
		if (!generatorQueueRecovered.get()) {
			try {
				if (generatorRecoveryPending == null) {
					generatorRecoveryPending = recoveryBatch(generator.recoverBufferedOperations());
				}
				recoveryFailure = markPendingUnattempted(generatorRecoveryPending);
				if (generatorRecoveryPending.isEmpty()) {
					generatorRecoveryPending = null;
					generatorQueueRecovered.set(true);
				}
			} catch (final RuntimeException failure) {
				recoveryFailure = appendRecoveryFailure(recoveryFailure, failure);
			}
		}
		if (!driverQueueRecovered.get()) {
			try {
				if (driverRecoveryPending == null) {
					driverRecoveryPending = recoveryBatch(driver.recoverQueuedOperations());
				}
				recoveryFailure = appendRecoveryFailure(
								recoveryFailure, markPendingUnattempted(driverRecoveryPending));
				if (driverRecoveryPending.isEmpty()) {
					driverRecoveryPending = null;
					driverQueueRecovered.set(true);
				}
			} catch (final RuntimeException failure) {
				recoveryFailure = appendRecoveryFailure(recoveryFailure, failure);
			}
		}
		if (recoveryFailure != null) {
			throw recoveryFailure;
		}
	}

	private void prepareOperationDrain() {
		stepShuttingDown.set(true);
		closeOperationAdmission();
		cancelPendingRetries();
		awaitRetryTasksSettled();
		recoverQueuedOperations();
		awaitRetryQueueDrained();
	}

	@Override
	public final void closeOperationAdmissionForStepStop() {
		stepShuttingDown.set(true);
		closeOperationAdmission();
	}

	@Override
	public final void recoverQueuedOperationsForStepStop() {
		prepareOperationDrain();
	}

	@Override
	public final void enforceDispatchedOperationsDeadlineForStepStop(
					final long deadlineNanos) {
		operationLifecycle.enforceTerminalDeadline(deadlineNanos);
	}

	@Override
	public final void expireDispatchedOperationsDeadlineForStepStop() {
		operationLifecycle.expireTerminalDeadline();
	}

	@Override
	public final void drainDispatchedOperationsForStepStop(final long deadlineNanos) {
		prepareOperationDrain();
		drainDispatchedOperations(deadlineNanos);
	}

	private void drainDispatchedOperations() {
		final long startedAt = System.nanoTime();
		final long waitNanos = TimeUnit.SECONDS.toNanos(Math.max(0, waitOpFinishLimit));
		drainDispatchedOperations(DurationTime.deadlineAfter(startedAt, waitNanos));
	}

	private synchronized void drainDispatchedOperations(final long deadlineNanos) {
		if (operationDrainComplete.get()) {
			return;
		}
		final long startedAt = System.nanoTime();
		try {
			if (operationLifecycle.isEnabled()) {
				try {
					if (waitOpFinishBeforeStop) {
						awaitOutstanding(() -> operationLifecycle.inFlightCount() > 0, deadlineNanos);
					}
				} finally {
					operationLifecycle.resolveOutstandingAsUnresolved();
				}
			} else if (waitOpFinishBeforeStop) {
				// Third-party drivers which do not expose lifecycle information retain the
				// historical active-concurrency wait behavior.
				awaitOutstanding(() -> activeOpCount() != 0, deadlineNanos);
			}
			operationDrainComplete.set(true);
		} finally {
			if (standaloneDeleteEnabled && !deleteDrainTimestampRecorded.get()) {
				deleteDrainCompletedNanos.set(System.nanoTime());
				deleteWorkflowCompletedEpochNanos.set(DurationTime.monotonicEpochNanos());
				deleteDrainTimestampRecorded.set(true);
			}
			final var snapshot = operationLifecycle.snapshot();
			Loggers.MSG.debug(
							"{}: lifecycle stop complete after {} ms: unattempted={}, terminal={}, unresolved={}",
							id,
							TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt),
							snapshot.unattempted(),
							snapshot.terminal(),
							snapshot.unresolved());
			if (standaloneDeleteDurationMode) {
				final var phaseTiming = deletePhaseTiming();
				Loggers.MSG.info(
								"{}: standalone DELETE phase timing: scheduled={} ms, drain={} ms",
								id,
								TimeUnit.NANOSECONDS.toMillis(phaseTiming.scheduledNanos()),
								TimeUnit.NANOSECONDS.toMillis(phaseTiming.drainNanos()));
			}
		}
	}

	private void awaitOutstanding(final BooleanSupplier outstanding, final long deadlineNanos) {
		while (outstanding.getAsBoolean()
						&& !DurationTime.deadlineReached(deadlineNanos, System.nanoTime())) {
			try {
				Thread.sleep(LIFECYCLE_POLL_INTERVAL_MILLIS);
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			}
		}
	}

	private List<O> recoveryBatch(final List<O> recovered) {
		return recovered == null ? new ArrayList<>() : new ArrayList<>(recovered);
	}

	private RuntimeException markPendingUnattempted(final List<O> pending) {
		RuntimeException recoveryFailure = null;
		int retainedCount = 0;
		// Compact failures into the prefix, then clear the processed tail once. Removing
		// each successful ArrayList element while scanning would make large queue recovery quadratic.
		for (int readIndex = 0; readIndex < pending.size(); readIndex++) {
			final O operation = pending.get(readIndex);
			try {
				operationLifecycle.unattempted(operation);
			} catch (final RuntimeException failure) {
				pending.set(retainedCount++, operation);
				recoveryFailure = appendRecoveryFailure(recoveryFailure, failure);
			}
		}
		pending.subList(retainedCount, pending.size()).clear();
		return recoveryFailure;
	}

	private RuntimeException appendRecoveryFailure(
					final RuntimeException currentFailure, final RuntimeException nextFailure) {
		if (nextFailure == null) {
			return currentFailure;
		}
		if (currentFailure == null) {
			return nextFailure;
		}
		if (currentFailure != nextFailure) {
			currentFailure.addSuppressed(nextFailure);
		}
		return currentFailure;
	}

	private void verifyDeleteInventoryForDirectLifecycle() {
		// Pre-validation is a distributed barrier. Its post phase is therefore started
		// only by the controller after every slice has passed; a worker-local shutdown
		// must not race a global strict-pre abort into issuing HEAD requests.
		if (!standaloneDeleteConfig.preValidation()) {
			verifyDeleteInventoryAfterDrain();
		}
	}

	@Override
	protected final void doShutdown() {
		// Close both admission gates before retry settlement or recovery. Only operations
		// already past actual driver dispatch remain drain-eligible.
		prepareOperationDrain();
		drainDispatchedOperations();
		verifyDeleteInventoryForDirectLifecycle();
		try (final Instance ctx = CloseableThreadContext.put(KEY_STEP_ID, id)
						.put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			driver.shutdown();
			Loggers.MSG.debug("{}: storage driver {} shutdown", id, driver.toString());
		} catch (final RemoteException e) {
			LogUtil.exception(Level.WARN, e, "{}: failed to shutdown the storage driver cleanly", id);
		}
	}

	@Override
	protected final void doStop() throws IllegalStateException {
		// Defensive backstop for direct doStop() lifecycles which bypass doShutdown().
		prepareOperationDrain();
		drainDispatchedOperations();
		verifyDeleteInventoryForDirectLifecycle();
		finalizeDeleteVerificationSummary();

		RuntimeException artifactFailure = null;
		if (deleteArtifactRecorder != null) {
			try {
				deleteArtifactRecorder.finish(
								operationLifecycle.snapshot(),
								deleteMetricsSnapshot(),
								deletePreValidationReport.get(),
								deletePostVerificationReport.get());
			} catch (final RuntimeException failure) {
				artifactFailure = failure;
			}
		}
		artifactFailure = appendRecoveryFailure(artifactFailure, closeDeleteVerificationStorage());

		driver.stop();
		if (artifactFailure != null) {
			throw artifactFailure;
		}

		if (latestSuccOpResultByItem != null && opsResultsOutput != null) {
			try {
				final var ioResultCount = latestSuccOpResultByItem.size();
				Loggers.MSG.info(
								"{}: please wait while performing {} I/O results output...", id, ioResultCount);
				for (final var latestOpResult : latestSuccOpResultByItem.values()) {
					try {
						if (!opsResultsOutput.put(latestOpResult)) {
							Loggers.ERR.debug(
											"{}: item info output fails to ingest, blocking the closing method", id);
							while (!opsResultsOutput.put(latestOpResult)) {
								Thread.sleep(1);
							}
							Loggers.MSG.debug("{}: closing method unblocked", id);
						}
					} catch (final Exception e) {
						if (e instanceof IOException) {
							LogUtil.exception(Level.WARN, e, "{}: failed to output the latest results", id);
						} else {
							throw e;
						}
					}
				}
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			} finally {
				Loggers.MSG.info("{}: I/O results output done", id);
			}
			latestSuccOpResultByItem.clear();
		}

		if (opsResultsOutput != null) {
			try {
				opsResultsOutput.put((O) null);
				Loggers.MSG.debug("{}: poisoned the items output", id);
			} catch (final NullPointerException e) {
				LogUtil.exception(
								Level.ERROR,
								e,
								"{}: results output \"{}\" failed to eat the poison",
								id,
								opsResultsOutput);
			} catch (final Exception e) {
				if (e instanceof IOException) {
					LogUtil.exception(Level.WARN, e, "{}: failed to poison the results output", id);
				} else {
					throw e;
				}
			}
		}

		if (opsMetricsOutput != null) {
			try {
				opsMetricsOutput.put((O) null);
				Loggers.MSG.debug("{}: poisoned the items timing metrics output", id);
			} catch (final NullPointerException e) {
				LogUtil.exception(
								Level.ERROR,
								e,
								"{}: timing metrics results output \"{}\" failed to eat the poison",
								id,
								opsMetricsOutput);
			} catch (final Exception e) {
				if (e instanceof IOException) {
					LogUtil.exception(Level.WARN, e, "{}: failed to poison the timing metrics results output", id);
				} else {
					throw e;
				}
			}
		}

		Loggers.MSG.debug("{}: interrupted the load step context", id);
	}

	private RuntimeException closeDeleteVerificationStorage() {
		RuntimeException failure = null;
		for (final DeleteVerificationReport report : new DeleteVerificationReport[]{
				deletePreValidationReport.get(), deletePostVerificationReport.get()
		}) {
			if (report == null) {
				continue;
			}
			try {
				report.close();
			} catch (final IOException closeFailure) {
				failure = appendRecoveryFailure(
								failure,
								new IllegalStateException("Failed to release DELETE verification evidence", closeFailure));
			}
		}
		if (deleteTargetOutcomes != null) {
			try {
				deleteTargetOutcomes.close();
			} catch (final IOException closeFailure) {
				failure = appendRecoveryFailure(
								failure,
								new IllegalStateException("Failed to release DELETE operational outcome storage", closeFailure));
			}
		}
		return failure;
	}

	private void markListSuccess(
					final ListOperation<?> listOp,
					final long reqDuration,
					final long respLatency,
					final long timeToFirstByte) {
		final int objectsListed = listOp.objectsListed();
		final ListShard shard = listOp.shard();
		if (shard != null) {
			final long durationMicros = Math.max(0L, reqDuration);
			final ListOptions listOptions = listOp.options();
			final int maxKeys = listOptions == null ? 0 : listOptions.maxKeys();
			listShardMetricsRecorder.onPageResult(
							shard,
							objectsListed,
							maxKeys,
							TimeUnit.MICROSECONDS.toNanos(durationMicros));
		}
		if (objectsListed <= 0) {
			Loggers.MSG.trace("{}: LIST op completed with zero objects", id);
			if (shard != null && !listOp.truncated()) {
				listShardMetricsRecorder.onComplete(shard);
			}
			return;
		}
		final long bytesListed = listOp.bytesListed();
		final boolean truncated = listOp.truncated();
		// Attempt delimiter-based split when hot
		boolean splitPerformed = false;
		if (shard != null) {
			splitPerformed = tryDelimiterSplit(shard, listOp);
			if (splitPerformed) {
				// parent retired by split; recorder will see children as they get leased
				return;
			}
		}
		if (truncated) {
			Loggers.MSG.trace(
							"{}: LIST op listed {} objects ({} bytes), truncated=true token={}",
							id,
							objectsListed,
							bytesListed,
							listOp.continuationToken());
		} else {
			Loggers.MSG.trace(
							"{}: LIST op finished after {} objects ({} bytes)",
							id,
							objectsListed,
							bytesListed);
			if (shard != null) {
				listShardMetricsRecorder.onComplete(shard);
			}
		}
		// bytesListed is the logical size of the objects described by the LIST response, not
		// payload transferred by this operation. Keep it for discovery diagnostics, but do not
		// feed it into the generic data-transfer and bandwidth meters.
		metricsCtx.markSucc(
						objectsListed,
						0L,
						new long[]{reqDuration
						},
						new long[]{respLatency
						},
						new long[]{timeToFirstByte
						});
	}

	private long timeToFirstByte(final O opResult) {
		if (opResult.type() == OpType.READ && opResult instanceof DataOperation<?> dataOperation) {
			final long dataLatency = dataOperation.dataLatency();
			return dataLatency > 0 && dataLatency <= opResult.duration() ? dataLatency : 0L;
		}
		if (opResult.type() == OpType.LIST && opResult instanceof PathOperation<?> pathOperation) {
			final long reqDone = opResult.reqTimeDone();
			final long dataStart = pathOperation.respDataTimeStart();
			if (reqDone > 0 && dataStart > reqDone && dataStart - reqDone <= opResult.duration()) {
				return dataStart - reqDone;
			}
		}
		return 0L;
	}

	// Returns true if a split has been performed and parent should not be recycled
	private boolean tryDelimiterSplit(final ListShard shard, final ListOperation<?> listOp) {
		final ListOptions options = listOp.options();
		if (options != null && options.includeVersions()) {
			return false;
		}
		// Integrity discovery is a destructive-operation safety boundary. Keep its verified
		// startup partition immutable instead of issuing blocking adaptive probes from this
		// completion callback or accepting a later server-provided namespace partition.
		if (immutableListRootPrefix != null) {
			return false;
		}
		// Only consider ENUMERATE shards for splitting
		final ListShard.Kind kind = shard.kind();
		if (kind != null && kind != ListShard.Kind.ENUMERATE) {
			return false;
		}
		final String prefix = shard.prefix();
		final Window w = splitWindows.computeIfAbsent(prefix, k -> new Window());
		final String pageFirst = listOp.pageFirstKey();
		final String pageLast = listOp.startAfter();
		if (pageFirst == null || pageLast == null || pageFirst.isEmpty() || pageLast.isEmpty()) {
			return false;
		}
		if (w.pages == 0) {
			w.firstKey = pageFirst;
		}
		w.lastKey = pageLast;
		w.pages++;
		int threshold = 50;
		try {
			if (listShardMetricsRecorder instanceof com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorderImpl rec) {
				threshold = rec.splitPages();
			}
		} catch (final Exception e) {
			LogUtil.exception(Level.DEBUG, e, "{}: failed to resolve split page threshold", id);
		}
		if (w.pages < threshold) {
			return false;
		}
		// Compute LCP
		final String lcp = longestCommonPrefix(w.firstKey, w.lastKey);
		if (lcp == null || lcp.isEmpty()) {
			splitWindows.remove(prefix);
			return false;
		}
		// Idle/backlog gate: only attempt a split probe when we appear to need more work.
		// Heuristic: if current concurrency is at the configured limit, workers are saturated;
		// skip probing to avoid stealing cycles from productive listing.
		try {
			final var snapshot = metricsCtx.lastSnapshot();
			final int concLimit = metricsCtx.concurrencyLimit();
			final int concCurr = (int) snapshot.concurrencySnapshot().last();
			if (concCurr >= concLimit) {
				if (Loggers.MSG.isDebugEnabled()) {
					Loggers.MSG.debug(
									"split.skip prefix={} lcp={} pages={} reason=no_idle concCurr={} concLimit={}",
									prefix,
									lcp,
									w.pages,
									concCurr,
									concLimit);
				}
				splitWindows.remove(prefix);
				return false;
			}
		} catch (final Exception e) {
			// If metrics are unavailable, fall through and attempt the probe below, but surface detail.
			LogUtil.exception(Level.DEBUG, e, "{}: metrics unavailable during delimiter split probe", id);
		}
		// Probe delimiters
		String delims = "/-_.";
		try {
			if (listShardMetricsRecorder instanceof com.dell.spt.base.item.op.list.shard.ListShardMetricsRecorderImpl rec) {
				delims = rec.delimiters();
			}
		} catch (final Exception e) {
			LogUtil.exception(Level.DEBUG, e, "{}: failed to resolve delimiter candidates", id);
		}
		if (!(driver instanceof ListDiscoveryProbe probe)) {
			splitWindows.remove(prefix);
			return false;
		}
		final String bucketPath = listOp.srcPath();
		java.util.List<String> best = java.util.Collections.emptyList();
		String usedDelim = null;
		if (Loggers.MSG.isDebugEnabled()) {
			Loggers.MSG.debug(
							"split.probe prefix={} lcp={} pages={} delimiters={} reason=pending",
							prefix,
							lcp,
							w.pages,
							delims);
		}
		for (int i = 0; i < delims.length(); i++) {
			final String d = String.valueOf(delims.charAt(i));
			try {
				final var r = probe.probeCommonPrefixes(bucketPath, lcp, d, 1000);
				if (!r.truncated()
								&& r.commonPrefixes() != null
								&& r.commonPrefixes().size() > (best == null ? 0 : best.size())) {
					best = r.commonPrefixes();
					usedDelim = d;
				}
			} catch (final IOException e) {
				LogUtil.exception(Level.WARN, e, "Delimiter probe failure for '{}' at LCP '{}'", d, lcp);
			}
		}
		if (best == null || best.size() < 2) {
			// no good split; reset window and continue parent
			if (Loggers.MSG.isDebugEnabled()) {
				Loggers.MSG.debug(
								"split.skip prefix={} lcp={} pages={} reason=lt2children children={}",
								prefix,
								lcp,
								w.pages,
								(best == null ? 0 : best.size()));
			}
			splitWindows.remove(prefix);
			return false;
		}
		// Enqueue children and a flat-children sweep
		final java.util.ArrayList<ListShard> childShards = new java.util.ArrayList<>(best.size());
		for (final String p : best) {
			final var childOp = ((com.dell.spt.base.item.op.list.ListOperationImpl<?>) listOp).result();
			childOp.reset();
			childOp.item().name(p);
			childOp.options(childOp.options().toBuilder().delimiter(null).startAfter(w.lastKey).continuationToken(null).build());
			final var childShard = new ListShard(p, null, null, w.lastKey);
			childShards.add(childShard);
			childOp.shard(childShard);
			generator.recycle((O) childOp);
		}
		if (usedDelim != null) {
			final var flatOp = ((com.dell.spt.base.item.op.list.ListOperationImpl<?>) listOp).result();
			flatOp.reset();
			flatOp.item().name(lcp);
			flatOp.options(flatOp.options().toBuilder().delimiter(usedDelim).startAfter(w.lastKey).continuationToken(null).build());
			final var flatShard = new ListShard(lcp, null, null, w.lastKey).withKind(ListShard.Kind.FLAT_CHILDREN).withDelimiter(usedDelim);
			flatOp.shard(flatShard);
			generator.recycle((O) flatOp);
		}
		// notify recorder and clear window
		try {
			listShardMetricsRecorder.onSplit(shard, null, childShards);
		} catch (final Exception e) {
			LogUtil.exception(Level.WARN, e, "{}: shard metrics recorder failed during split commit", id);
		}
		if (Loggers.MSG.isDebugEnabled()) {
			Loggers.MSG.debug(
							"split.commit prefix={} lcp={} children={} flat_children={} startAfter={}",
							prefix,
							lcp,
							childShards.size(),
							(usedDelim == null ? 0 : 1),
							w.lastKey);
		}
		splitWindows.remove(prefix);
		return true;
	}

	private static String canonicalListPrefix(final String prefix) {
		if (prefix == null) {
			return null;
		}
		if (prefix.isEmpty() || "/".equals(prefix)) {
			return "";
		}
		return prefix.startsWith("/") ? prefix.substring(1) : prefix;
	}

	private static String longestCommonPrefix(final String a, final String b) {
		if (a == null || b == null) {
			return "";
		}
		final int len = Math.min(a.length(), b.length());
		int i = 0;
		while (i < len && a.charAt(i) == b.charAt(i)) {
			i++;
		}
		return a.substring(0, i);
	}

	@Override
	protected final void doClose() {
		try (final Instance logCtx = CloseableThreadContext.put(KEY_STEP_ID, id)
						.put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			if (listShardMetricsRecorder != null) {
				listShardMetricsRecorder.emitSummary(id);
			}
			generator.close();
			if (deleteArtifactRecorder != null) {
				deleteArtifactRecorder.close();
			}
			try {
				driver.close();
			} catch (final IOException e) {
				LogUtil.exception(
								Level.ERROR, e, "Failed to close the storage driver \"{}\"", driver.toString());
			}
			Loggers.MSG.debug("{}: closed the load step context", id);
		}
	}
}
