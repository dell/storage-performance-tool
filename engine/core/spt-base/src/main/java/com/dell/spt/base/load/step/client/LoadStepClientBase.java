package com.dell.spt.base.load.step.client;

import com.dell.spt.base.metrics.bucket.BucketMetricsRange;
import com.dell.spt.base.metrics.bucket.BucketMetricsReply;
import com.dell.spt.base.metrics.bucket.BucketRows;
import com.dell.spt.base.metrics.context.BucketMetricsSource;
import com.dell.spt.base.metrics.steady.SteadySettings;
import com.dell.spt.base.storage.driver.bucket.TargetBucketSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.base.config.ConfigUtil.validateBooleanValues;
import static com.dell.spt.base.config.ConfigUtil.validateListValues;
import static com.dell.spt.base.config.ConfigUtil.flatten;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.apache.logging.log4j.CloseableThreadContext.put;

import com.github.akurilov.commons.collection.TreeUtil;
import com.dell.spt.base.buildinfo.EngineBuildInfoProvider;
import com.dell.spt.base.concurrent.JvmExit;
import com.dell.spt.base.config.AliasingUtil;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.NicSetConfig;
import com.dell.spt.base.integrity.IntegrityConfig;
import com.dell.spt.base.integrity.IntegrityInputProvenance;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.config.ScenarioConfigurationException;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.io.CsvItemInput;
import com.dell.spt.base.item.io.ItemInputFactory;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import com.dell.spt.base.load.failure.ObjectFailureBudgetConfig;
import com.dell.spt.base.load.failure.ObjectFailureBudgetController;
import com.dell.spt.base.load.failure.ObjectFailureBudgetDecision;
import com.dell.spt.base.load.failure.ObjectFailureBudgetOutcome;
import com.dell.spt.base.load.step.DurationAwaitStatus;
import com.dell.spt.base.load.step.DurationTime;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepBase;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.load.step.RunStepIds;
import com.dell.spt.base.load.step.StepIdCheckException;
import com.dell.spt.base.load.step.StepIdFiles;
import com.dell.spt.base.load.step.StepIdInUseException;
import com.dell.spt.base.load.step.client.metrics.MetricsAggregator;
import com.dell.spt.base.load.step.client.metrics.MetricsAggregatorImpl;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.nic.pool.ItemSizeRecording;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.DistributedMetricsContextImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.storage.driver.CompositeOperationSupport;
import com.dell.spt.base.storage.driver.CompositeOperationsUnsupported;
import com.dell.spt.base.item.op.data.DataOperationsBuilderImpl;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.svc.ServiceUtil;
import com.dell.spt.base.storage.driver.bucket.TargetBucketProvisioning;
import com.dell.spt.base.storage.driver.bucket.TargetBucketStep;
import com.dell.spt.base.util.BinarySizeFormat;
import com.github.akurilov.commons.net.NetUtil;
import com.dell.spt.base.config.TimeUtil;
import com.github.akurilov.commons.reflection.TypeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValueTypeException;
import com.github.akurilov.confuse.impl.BasicConfig;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.rmi.ConnectException;
import java.rmi.NoSuchObjectException;
import java.rmi.RemoteException;
import java.rmi.UnmarshalException;
import java.rmi.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReentrantLock;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;

public abstract class LoadStepClientBase<T extends LoadStepClient<T>>
				extends LoadStepBase
				implements LoadStepClient<T> {

	/** Proposed in the spec, until the payload's measurement says otherwise. */
	private static final long BUCKET_METRICS_FETCH_SECONDS = 30;
	/**
	 * The ids of the steps with {@link StepIdFiles} each run started on this Entry Node, which tell a
	 * worker's earlier-run file from this run's.
	 */
	private static final RunStepIds RUN_STEP_IDS = new RunStepIds();

	private static final long DURATION_DRAIN_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
	private static final long DURATION_REMOTE_AWAIT_POLL_NANOS = TimeUnit.SECONDS.toNanos(1);
	private static final long FAILURE_BUDGET_POLL_MILLIS = 100;
	private static final long FAILURE_BUDGET_LIVE_SNAPSHOT_TIMEOUT_NANOS = TimeUnit.MILLISECONDS.toNanos(100);
	private static final int FAILURE_BUDGET_MAX_RETAINED_SNAPSHOT_FLIGHTS = 1;
	private static final int FAILURE_BUDGET_STOP_PHASE_COUNT = 6;
	private static final long FAILURE_BUDGET_MONITOR_JOIN_BUFFER_MILLIS = 250;
	// An arbitrary RMI invocation cannot be force-stopped safely. Admit one JVM-wide
	// virtual probe flight so an interruption-resistant peer cannot accumulate retained
	// tasks across runs; the shipped RMI response timeout bounds the real transport call.
	private static final Semaphore FAILURE_BUDGET_SNAPSHOT_FLIGHT_ADMISSION = new Semaphore(FAILURE_BUDGET_MAX_RETAINED_SNAPSHOT_FLIGHTS);
	private static final AtomicInteger ACTIVE_FAILURE_BUDGET_SNAPSHOT_PROBE_TASKS = new AtomicInteger();

	// Copy-on-write: a signal's exit hook clears it in the close while the scenario thread's await reads it.
	private final List<LoadStep> stepSlices = new CopyOnWriteArrayList<>();
	/** Each slice's node address, in slice order; null for the entry's own slice. */
	private final List<String> stepSliceNodeAddrs = new CopyOnWriteArrayList<>();
	// Held for the whole of doStop() and doClose(). A signal runs both on the exit hook while the scenario
	// thread runs await()'s trailing doStop(), outside the state lock. Order: state lock, then this lock,
	// then the base class's metrics unregister lock.
	private final ReentrantLock stopCloseLock = new ReentrantLock();
	// Set when doClose() returns: a later doStop() has nothing left to stop.
	private volatile boolean stopCloseDone = false;
	// Set before closing slices so a concurrent await seeing a self-closed slice is not marked lost.
	private volatile boolean slicesClosing;
	private final Object lostSliceLock = new Object();
	/** The remote slices this entry lost contact with, created at the first loss. Guarded by lostSliceLock. */
	private Set<LoadStep> lostSlices;
	/** The step's failure for its lost slices, one suppressed failure per slice after the first. Guarded by lostSliceLock. */
	private RunTerminalException lostSliceFailure;
	/** The slices whose latest stop got no response within the RMI response timeout. Guarded by lostSliceLock. */
	private Set<LoadStep> slowStopSlices;
	/** Set at the first lost slice: the await's poll loops end, and the await stops and fails the step. */
	private volatile boolean sliceLost;
	/** Whether a failed metrics fetch can lose a slice: from the aggregator's start to the step's stop. */
	private volatile boolean metricsFetchLosesSlices;
	/**
	 * Whether a lost slice stops every started slice at once, each from a thread of its own: from the
	 * start of an ordinary distributed step's await to its stop. Guarded by lostSliceLock.
	 */
	private boolean lostSliceStopsAtOnce;
	/**
	 * The stop at once of each started slice at the first loss in the await, the entry's own among
	 * them and the lost ones not, by slice: set once, then read only. The step's own stop stops these slices
	 * again only when their stop at once failed before it was sent. Guarded by lostSliceLock.
	 */
	private IdentityHashMap<LoadStep, Thread> stopsAtOnce;
	/** The slices whose stop at once ended without the stop being sent. Guarded by lostSliceLock. */
	private Set<LoadStep> unsentStopsAtOnce;
	/**
	 * The one close each lost slice gets, by slice, sent from a thread of its own as the slice is
	 * lost; see {@link #closeLostSliceOnce}. Guarded by lostSliceLock.
	 */
	private IdentityHashMap<LoadStep, LostSliceClose> lostSliceCloses;
	/**
	 * The slice index of each lost slice, which its node's file manager reads to stop calling the
	 * worker; kept until the step's next start, as the step's close collects files after it has let
	 * go of its slices.
	 */
	private final Set<Integer> lostNodeIndexes = ConcurrentHashMap.newKeySet();
	private List<BucketRows.Contribution> bucketContributions;
	private final List<FileManager> fileMgrs = new ArrayList<>();
	// for the core configuration options which are using the files
	private final List<AutoCloseable> itemDataInputFileSlicers = new ArrayList<>();
	private final List<AutoCloseable> itemInputFileSlicers = new ArrayList<>();
	private final List<AutoCloseable> itemOutputFileAggregators = new ArrayList<>();
	private final List<AutoCloseable> operationLifecycleAggregators = new ArrayList<>();
	private NicPathsCollector nicPathsCollector;
	private BucketVersioningRelay bucketVersioningRelay;
	private final List<AutoCloseable> integrityLogFileAggregators = new ArrayList<>();
	private final List<AutoCloseable> itemTimingMetricsOutputFileAggregators = new ArrayList<>();
	private final List<AutoCloseable> opTraceLogFileAggregators = new ArrayList<>();
	// The step ids that already have a timing or a trace aggregator: each engine writes one file per id.
	private final Set<String> timingMetricsStepIds = new HashSet<>();
	private final Set<String> opTraceStepIds = new HashSet<>();
	private final List<AutoCloseable> deleteArtifactAggregators = new ArrayList<>();
	private final List<AutoCloseable> storageAuthFileSlicers = new ArrayList<>();
	private final static int MAX_SLEEP_TIME_MILLIS = 120_000; // 2min.
	private volatile IntegrityTerminalException durationStopFailure;
	private volatile IntegrityTerminalException durationValidityFailure;
	private volatile boolean durationAdmissionBarrierSatisfied;
	private boolean durationCleanupRetryPending;
	private volatile long durationDrainDeadlineNanos = Long.MIN_VALUE;
	private volatile boolean durationDrainDeadlineSet;
	private final Map<LoadStep, DurationAwaitStatus> durationAwaitStatuses = synchronizedIdentityMap();
	private final Set<LoadStep> durationAwaitStatusProbes = Collections.synchronizedSet(
					Collections.newSetFromMap(new IdentityHashMap<>()));
	private final ObjectFailureBudgetController failureBudgetController;
	private volatile IntegrityTerminalException failureBudgetFailure;
	private volatile String failureBudgetFailureReason;
	private volatile long failureBudgetStartedNanos = Long.MIN_VALUE;
	private volatile Thread failureBudgetMonitor;
	private FailureBudgetSnapshotCollector failureBudgetSnapshotCollector;
	private final Object standaloneDeleteStopCoordination = new Object();
	private volatile boolean standaloneDeleteStopInProgress;
	private volatile boolean standaloneDeleteStopCoordinated;
	private volatile boolean deletePostVerificationGloballySkipped;
	private boolean failureBudgetFinalized;
	private IntegrityTerminalException failureBudgetTerminalFailure;
	private volatile boolean failureBudgetMetricsOutputDeferred;
	// The failed outcome an incomplete duration stop wrote: a close after the one that threw its stop
	// failure cleans up and closes with it, as a run's close retry does.
	private IntegrityTerminalException incompleteDurationStopFailure;
	// The thread running the await's own stop, which the run's close follows; null otherwise.
	private volatile Thread awaitStopThread;
	// Set by a duration slice stop that a stop request interrupted, which leaves the timing files open.
	private boolean durationSliceStopInterrupted;
	// Of this start: how many slices, in slice order, returned from their start() (used for stopped-start
	// artifact publishing and lost-slice gating), and whether the start ended as a stop.
	private volatile int startedSliceCount;
	private volatile boolean startStopped;
	// Whether this start created a slice and never called its start(): it ended between the two.
	private volatile boolean createdSliceUnreached;
	// Whether a slice's start() failed as a refusal for its step-id files: the close then merges the
	// slices whose start returned only.
	private volatile boolean sliceRefusedForStepIdFiles;
	// Whether a start ever reached doStartWrapped(); a start the base class refused never did.
	private volatile boolean startEntered;

	static int activeFailureBudgetSnapshotFlightCount() {
		return FAILURE_BUDGET_MAX_RETAINED_SNAPSHOT_FLIGHTS
						- FAILURE_BUDGET_SNAPSHOT_FLIGHT_ADMISSION.availablePermits();
	}

	static int activeFailureBudgetSnapshotProbeTaskCount() {
		return ACTIVE_FAILURE_BUDGET_SNAPSHOT_PROBE_TASKS.get();
	}

	final long failureBudgetEpochNanos() {
		return failureBudgetStartedNanos;
	}

	public LoadStepClientBase(
					final Config config, final List<Extension> extensions, final List<Config> ctxConfigs,
					final MetricsManager metricsMgr) {
		super(config, extensions, ctxConfigs, metricsMgr, true);
		try {
			this.failureBudgetController = standaloneDeleteEnabled()
							? new ObjectFailureBudgetController(
											ObjectFailureBudgetConfig.from(this.config.configVal("load")))
							: null;
		} catch (final RuntimeException | Error e) {
			abandonRegistration();
			throw e;
		}
		// Only now, with this class's fields set, may the exit hook close this client.
		registerForCloseAll();
	}

	private static <K, V> Map<K, V> synchronizedIdentityMap() {
		return Collections.synchronizedMap(new IdentityHashMap<>());
	}

	// The start writes a generated id into the configuration as if it were set: a copy must not keep it.
	private boolean stepIdGenerated = false;
	private MetricsAggregator metricsAggregator = null;
	// Set for a step with a NIC set on any engine, before its items are distributed.
	private ItemSizeRecording itemSizeRecording;

	@Override
	protected final void provisionTargetBuckets() {
		final String stepId = config.boolVal("load-step-idAutoGenerated") ? null : loadStepId();
		TargetBucketProvisioning.provision(stepId, config, ctxConfigs, extensions, targetBucketStep());
	}

	/**
	 * Slices any items file of the step's own, beyond the item input, for the base or appended
	 * configuration being distributed. A file every engine read whole would have each of them process
	 * every row. The step closes what this returns with the other slicers.
	 */
	protected List<AutoCloseable> sliceOwnItemFiles(
					final Config config,
					final List<FileManager> fileMgrs,
					final List<Config> configSlices,
					final int batchSize) {
		return List.of();
	}

	/**
	 * What target-bucket setup must know about this step beyond its configuration. A step whose
	 * operation type does not tell whether it writes new objects overrides this.
	 */
	protected TargetBucketStep targetBucketStep() {
		return TargetBucketStep.ORDINARY;
	}

	/** What {@link #controlsOperations} and {@link #suppliesItems} take for the base configuration. */
	public static final int BASE_CONFIG = -1;

	/**
	 * Whether this step's operations take their count limit from the configuration of context
	 * {@code contextIndex}, or from the base configuration at {@link #BASE_CONFIG}. Only such a count
	 * must give every engine at least 1, and is split evenly where the usual split would leave an
	 * engine at 0; any other is sliced as it always was, since the step never uses it. A linear step
	 * runs on its base configuration and ignores its contexts.
	 */
	protected boolean controlsOperations(final int contextIndex) {
		return contextIndex == BASE_CONFIG;
	}

	/**
	 * Whether this step's operations read the item input of the configuration of context
	 * {@code contextIndex}, or of the base configuration at {@link #BASE_CONFIG}. Only such an input
	 * must give every engine an item in a count-based recycle step. By default, the input of each
	 * configuration that {@link #controlsOperations controls the operations}.
	 */
	protected boolean suppliesItems(final int contextIndex) {
		return controlsOperations(contextIndex);
	}

	/**
	 * Refuses a stopped step before anything of a new start happens: a restart would run the whole
	 * step again beside its stopped slices. Local slices keep their restart. While the JVM exits, the
	 * base class refuses the start as a stop instead.
	 */
	@Override
	protected void doStart() throws IllegalStateException {
		if (isStopped() && !JvmExit.begun()) {
			throw new RunTerminalException(
							IntegrityTerminalException.Category.EXECUTION,
							loadStepId(),
							"this step object has been stopped and cannot be started again; a step object runs once. "
											+ "Build a new step from a prototype with config() and run that.",
							null);
		}
		try {
			super.doStart();
		} catch (final Throwable cause) {
			// The base class ends a start that a stop cut short, or refused at JVM exit, with this.
			if (cause instanceof InterruptedException) {
				startStopped = true;
			}
			throw cause;
		}
	}

	@Override
	protected final void doStartWrapped()
					throws IllegalArgumentException {
		// Reset where a start really begins: a restart the base class refuses (at JVM exit) keeps the
		// count of the start before it, so its close still publishes that start's slices.
		startEntered = true;
		startedSliceCount = 0;
		startStopped = false;
		createdSliceUnreached = false;
		sliceRefusedForStepIdFiles = false;
		stepSliceNodeAddrs.clear();
		slicesClosing = false;
		resetDurationLifecycleForStart();
		try (final var logCtx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			// need to set the once generated step id
			stepIdGenerated |= config.boolVal("load-step-idAutoGenerated");
			config.val("load-step-id", loadStepId());
			config.val("load-step-idAutoGenerated", false);
			final var configuredNodeAddrs = remoteNodeAddrs(config);
			final var nodeAddrs = participatingRemoteNodeAddrs(config, configuredNodeAddrs);
			if (nodeAddrs.size() != configuredNodeAddrs.size()) {
				Loggers.MSG.info(
								"{}: exact-output count {} activates {} of {} load step slices",
								loadStepId(),
								config.longVal("load-op-limit-count"),
								1 + nodeAddrs.size(),
								1 + configuredNodeAddrs.size());
			}
			// The start check resolved them, for the same nodes.
			if (fileMgrs.size() != 1 + nodeAddrs.size()) {
				throw new IllegalStateException(loadStepId() + ": the file managers do not match the step's nodes");
			}
			initDeleteArtifactAggregator(nodeAddrs);
			// Before any slice starts, so the relay notes where this run's notices begin.
			initBucketVersioningRelay(nodeAddrs);
			final var sliceCount = 1 + nodeAddrs.size();
			// init the base/shared config slices
			final var configSlices = sliceConfig(
							config,
							sliceCount,
							nodeAddrs,
							IntegrityConfig.requiresExactOutputCount(config.configVal("storage")),
							controlsOperations(BASE_CONFIG));
			// init the config slices for each of the load step context configs
			final var ctxConfigsSlices = (List<List<Config>>) new ArrayList<List<Config>>(sliceCount);
			for (var i = 0; i < sliceCount; i++) {
				ctxConfigsSlices.add(new ArrayList<>());
			}
			final var slicesByCtxConfig = new ArrayList<List<Config>>();
			if (null != ctxConfigs) {
				for (var ctxIndex = 0; ctxIndex < ctxConfigs.size(); ctxIndex++) {
					final var ctxConfigSlices = sliceConfig(
									ctxConfigs.get(ctxIndex), sliceCount, nodeAddrs, false, controlsOperations(ctxIndex));
					slicesByCtxConfig.add(ctxConfigSlices);
					for (var i = 0; i < sliceCount; i++) {
						ctxConfigsSlices.get(i).add(ctxConfigSlices.get(i));
					}
				}
			}
			final var nicSetExpected = nicSetExpected(configSlices, ctxConfigsSlices);
			// Before the base or any context lists an item input.
			refuseCompositeInAppendedContexts();
			final Runnable distribution = () -> {
				addFileClients(
								config, configSlices, suppliesItems(BASE_CONFIG), controlsOperations(BASE_CONFIG), loadStepId());
				for (var i = 0; i < slicesByCtxConfig.size(); i++) {
					addFileClients(
									ctxConfigs.get(i), slicesByCtxConfig.get(i), suppliesItems(i), controlsOperations(i),
									contextOutputStepId(i));
				}
			};
			// The barrier's deadline starts before the items are distributed (D54-R2).
			final AdmissionBarrier barrier = nicSetExpected.contains(true) ? newAdmissionBarrier() : null;
			itemSizeRecording = stepItemSizeRecording(barrier == null ? null : barrier.itemSizeRecording());
			try {
				if (barrier == null) {
					distribution.run();
				} else {
					barrier.distribute(() -> {
						try (final var distributionCtx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
							distribution.run();
						}
					});
				}
				initAndStartStepSlices(nodeAddrs, configSlices, ctxConfigsSlices, metricsMgr, barrier);
				initNicPathsCollector(nodeAddrs, nicSetExpected);
				if (barrier != null) {
					openAdmissionBarrier(barrier);
				}
			} catch (final RuntimeException | Error | InterruptedException failure) {
				if (barrier != null) {
					barrier.fenceAll();
				}
				throwUnchecked(failure);
			}
			validateDeleteInventoriesBeforeAdmission();
			initAndStartMetricsAggregator(config.configVal("output-metrics"));
			if (standaloneDeleteEnabled() && !standaloneDeleteDurationMode()) {
				prepareCountFailureBudgetMonitoring();
				failureBudgetStartedNanos = System.nanoTime();
				releaseCountFailureBudgetAdmission();
			}
			Loggers.MSG.info(
							"{}: load step client started, additional nodes: {}", loadStepId(),
							Arrays.toString(nodeAddrs.toArray()));
		}
	}

	/** One per slice: whether that slice requested a NIC set, in its base or any context configuration. */
	private static List<Boolean> nicSetExpected(final List<Config> configSlices, final List<List<Config>> ctxConfigsSlices) {
		final var expected = new ArrayList<Boolean>(configSlices.size());
		for (var i = 0; i < configSlices.size(); i++) {
			expected.add(NicSetConfig.isSet(configSlices.get(i).configVal("storage"))
							|| ctxConfigsSlices.get(i).stream().anyMatch(ctx -> NicSetConfig.isSet(ctx.configVal("storage"))));
		}
		return expected;
	}

	/** One per slice, as {@link #sliceConfig} gives each its node's interfaces: whether it requested a NIC set. */
	private List<Boolean> nicSetExpected(final List<String> nodeAddrs) {
		final int sliceCount = 1 + nodeAddrs.size();
		final var configSlices = nodeInterfaceSlices(config, sliceCount, nodeAddrs);
		final var ctxConfigsSlices = new ArrayList<List<Config>>(sliceCount);
		for (var i = 0; i < sliceCount; i++) {
			ctxConfigsSlices.add(new ArrayList<>());
		}
		if (ctxConfigs != null) {
			for (final var ctxConfig : ctxConfigs) {
				final var slices = nodeInterfaceSlices(ctxConfig, sliceCount, nodeAddrs);
				for (var i = 0; i < sliceCount; i++) {
					ctxConfigsSlices.get(i).add(slices.get(i));
				}
			}
		}
		return nicSetExpected(configSlices, ctxConfigsSlices);
	}

	private static List<Config> nodeInterfaceSlices(final Config config, final int sliceCount, final List<String> nodeAddrs) {
		final var slices = new ArrayList<Config>(sliceCount);
		for (var i = 0; i < sliceCount; i++) {
			slices.add(ConfigSliceUtil.initSlice(config));
		}
		// The step start slices the configuration again, and logs each slice's interfaces then.
		ConfigSliceUtil.sliceNodeInterfaces(config, slices, nodeAddrs, false);
		return slices;
	}

	/**
	 * Refuses the step before any load when an earlier step with the same id left a file that this
	 * step would refuse to replace at its close, on the Entry Node or on any other node of the step:
	 * each slice's {@link StepIdFiles}, which {@link #addFileClients} and {@link #initNicPathsCollector}
	 * aggregate, and the Entry Node's copies of them. The Entry Node's own files are tested before any
	 * bucket is provisioned when the id is configured; with {@code nodes}, after the target checks and
	 * before any item is distributed or any slice started, it resolves the nodes' file managers for the
	 * step and tests theirs too. Another node's file is refused only when an earlier step of this run
	 * had the id; for the first, it can only be an earlier run's, which that worker is asked to move
	 * aside there and then, before any slice of the step exists, and a worker that cannot refuses the
	 * step. Each slice tests its own files again when it starts. A metadata-mode step's manifest is refused the
	 * same way when it was already published. A step that publishes none of these files runs as
	 * before.
	 */
	@Override
	protected final void refuseStepIdInUse(final boolean nodes) {
		final var nodeAddrs = participatingRemoteNodeAddrs(config, remoteNodeAddrs(config));
		final int sliceCount = 1 + nodeAddrs.size();
		final var configs = new ArrayList<Config>();
		configs.add(config);
		if (ctxConfigs != null) {
			configs.addAll(ctxConfigs);
		}
		for (final var stepConfig : configs) {
			final var integrityConfig = IntegrityConfig.fromStorage(stepConfig.configVal("storage"));
			final var itemOutputFile = stepConfig.stringVal("item-output-file");
			if (integrityConfig.enabled() && itemOutputFile != null && !itemOutputFile.isEmpty()) {
				for (final var path : CsvArtifactAggregator.publishedPaths(
								itemOutputFile, sliceCount, StepIdFiles.opType(stepConfig))) {
					final boolean published;
					try {
						published = StepIdFiles.exists(path);
					} catch (final IOException unknown) {
						throw StepIdCheckException.here(loadStepId(), path.toString(), unknown);
					}
					if (published) {
						throw terminalFailure(
										IntegrityTerminalException.Category.PUBLICATION,
										"item output file is already published by an earlier step (" + path
														+ " exists); give each step its own item.output.file",
										null);
					}
				}
			}
		}
		final var merged = StepIdFiles.merged(config, ctxConfigs);
		final var nicSetExpected = nicSetExpected(nodeAddrs);
		final var paths = new ArrayList<java.nio.file.Path>();
		for (final var artifact : merged) {
			try {
				paths.addAll(CsvLoggerArtifactAggregator.publishedPaths(
								FileManager.INSTANCE, artifact.loggerName(), loadStepId(), sliceCount));
			} catch (final IOException | RuntimeException unknown) {
				throwUncheckedIfInterrupted(unknown);
				throw StepIdCheckException.here(loadStepId(), "the " + artifact.loggerName() + " file", unknown);
			}
		}
		if (nicSetExpected.contains(true)) {
			try {
				paths.addAll(NicPathsCollector.publishedPaths(FileManager.INSTANCE, loadStepId(), nicSetExpected));
			} catch (final IOException | RuntimeException unknown) {
				throwUncheckedIfInterrupted(unknown);
				throw StepIdCheckException.here(loadStepId(), "the NIC files", unknown);
			}
		}
		StepIdInUseException.refuseExisting(loadStepId(), paths);
		if (!nodes) {
			return;
		}
		fileMgrs.clear();
		initFileManagers(nodeAddrs, fileMgrs);
		// A step that publishes none of the files claims no id: a later one of the run may be the first.
		final boolean firstInRun = !nodeAddrs.isEmpty() && !paths.isEmpty()
						&& RUN_STEP_IDS.firstInRun(runId(), loadStepId());
		refuseNodeFiles(
						loadStepId(), fileMgrs, nodeAddrs, merged, nicSetExpected, runId(),
						firstInRun ? this::resolveStepManager : null);
	}

	/** Resolves a node's step manager service, which prepares its files for a step. */
	@FunctionalInterface
	interface StepManagers {
		LoadStepManagerService resolve(String nodeAddr) throws Exception;
	}

	LoadStepManagerService resolveStepManager(final String nodeAddr) throws Exception {
		return ServiceUtil.resolveWithConnectTimeout(nodeAddr, LoadStepManagerService.SVC_NAME, LoadStepManagerService.class);
	}

	/**
	 * Refuses step {@code stepId} when an earlier step with its id left, on another node of the step,
	 * a file that node's slice would publish, or when the node cannot tell whether one is there: a
	 * slice must not start over a file nobody could test. With {@code managers}, the step is the first
	 * with its id in its run on this Entry Node, so only an earlier run can have left such a file:
	 * once every node is tested, each node whose slice publishes such files is asked to prepare them
	 * (see {@link LoadStepManagerService#prepareStepIdFiles}), which moves an earlier run's aside. The
	 * step is refused when a node cannot, when a node had the id in this run already and holds such a
	 * file, or when an older node, which does not know the call, holds one. All of it comes before any
	 * slice of the step is created on any node.
	 *
	 * @param fileMgrs one per slice, the Entry Node's first
	 * @param nodeAddrs one per other node, in slice order
	 * @param nicSetExpected one per slice: whether that slice requested a NIC set
	 * @param managers the nodes' step managers when the step is the first of its id in its run; else
	 *     {@code null}, and every such file is refused
	 */
	static void refuseNodeFiles(
					final String stepId,
					final List<FileManager> fileMgrs,
					final List<String> nodeAddrs,
					final List<StepIdFiles.LoggerArtifact> merged,
					final List<Boolean> nicSetExpected,
					final long runId,
					final StepManagers managers) {
		final int sliceCount = fileMgrs.size();
		// The first file found on each node, or null.
		final var found = new ArrayList<String>(sliceCount);
		found.add(null);
		for (var i = 1; i < sliceCount; i++) {
			final var fileMgr = fileMgrs.get(i);
			String first = null;
			for (final var loggerName : StepIdFiles.sliceLoggerNames(merged, nicSetExpected.get(i))) {
				String file = "the " + loggerName + " file";
				final boolean exists;
				try {
					file = fileMgr.logFileName(loggerName, stepId);
					exists = StepIdFiles.exists(fileMgr, file);
				} catch (final IOException | RuntimeException unknown) {
					throwUncheckedIfInterrupted(unknown);
					throw StepIdCheckException.onNode(stepId, i, sliceCount, nodeAddrs.get(i - 1), file, unknown);
				}
				if (exists && managers == null) {
					throw StepIdInUseException.onNode(stepId, i, sliceCount, nodeAddrs.get(i - 1), file);
				}
				if (exists && first == null) {
					first = file;
				}
			}
			found.add(first);
		}
		if (managers == null) {
			return;
		}
		for (var i = 1; i < sliceCount; i++) {
			final var loggerNames = StepIdFiles.sliceLoggerNames(merged, nicSetExpected.get(i));
			if (loggerNames.isEmpty()) {
				continue;
			}
			final var node = nodeAddrs.get(i - 1);
			final var earlier = found.get(i);
			throwIfStartStopped(stepId, node);
			final boolean firstHere;
			try {
				final var manager = managers.resolve(node);
				if (manager == null) {
					throw new IllegalStateException("no step manager service @ " + node);
				}
				firstHere = manager.prepareStepIdFiles(runId, stepId, loggerNames);
			} catch (final Exception failure) {
				throwUncheckedIfInterrupted(failure);
				// A stop or signal during the call, whose transport it failed: the start ends as a stop.
				throwIfStartStopped(stepId, node);
				if (lacksStepIdFilesPreparation(failure)) {
					// An older node: its files are refused, as by an Entry Node that never asks.
					if (earlier != null) {
						throw StepIdInUseException.onNode(stepId, i, sliceCount, node, earlier);
					}
					Loggers.MSG.debug("{}: node {} does not prepare a step's files, and holds none of this step's", stepId, node);
					continue;
				}
				// Only a file this Entry Node saw there can have failed to move; else the node could not check.
				if (earlier != null && RunTerminalException.find(failure) instanceof StepIdCheckException) {
					throw StepIdCheckException.notMovedAsideOnNode(stepId, i, sliceCount, node, failure);
				}
				throw StepIdCheckException.onNode(stepId, i, sliceCount, node, "the step's files", failure);
			}
			throwIfStartStopped(stepId, node);
			if (!firstHere && earlier != null) {
				// That node had the id in this run already: a repeated run id, for example.
				throw StepIdInUseException.onNode(stepId, i, sliceCount, node, earlier);
			}
			if (earlier != null) {
				Loggers.MSG.info("{}: an earlier run left {} on node {}, which moved it aside", stepId, earlier, node);
			}
		}
	}

	/** Ends the start as a stop when this thread was interrupted or the JVM is exiting. */
	private static void throwIfStartStopped(final String stepId, final String node) {
		if (Thread.currentThread().isInterrupted() || JvmExit.begun()) {
			throwUnchecked(new InterruptedException(stepId + ": stopped while preparing the step's files on node " + node));
		}
	}

	/**
	 * Whether a node failed {@link LoadStepManagerService#prepareStepIdFiles} for not knowing it: its
	 * server wraps an unknown method as a {@code ServerException} around an {@code UnmarshalException}.
	 */
	static boolean lacksStepIdFilesPreparation(final Throwable failure) {
		for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
			final String message = cause.getMessage();
			if (message != null && message.contains(LoadStepManagerService.NO_STEP_ID_FILES_PREPARATION)) {
				return true;
			}
			if (cause instanceof UnmarshalException && message != null && message.contains("unrecognized method hash")) {
				return true;
			}
		}
		return false;
	}

	private AdmissionBarrier newAdmissionBarrier() {
		final Object raw = config.val("load-step-preparation-timeout");
		final long seconds = raw instanceof String text
						? TimeUtil.getTimeInSeconds(text)
						: TypeUtil.typeConvert(raw, long.class);
		if (seconds <= 0) {
			throw new IllegalConfigurationException(
							AdmissionBarrier.TIMEOUT_SETTING + " must be greater than 0, not " + raw);
		}
		final String timeoutText = raw instanceof Number || String.valueOf(raw).matches("\\d+") ? raw + " s" : String.valueOf(raw);
		return new AdmissionBarrier(loadStepId(), TimeUnit.SECONDS.toNanos(seconds), timeoutText);
	}

	private void openAdmissionBarrier(final AdmissionBarrier barrier) throws InterruptedException {
		final Map<String, Long> engineMillis = barrier.awaitAndRelease();
		metricsMgr.recordPreparation(runId(), loadStepId(), barrier.distributionMillis(), engineMillis);
		final var slowest = engineMillis.entrySet().stream().max(Map.Entry.comparingByValue()).orElseThrow();
		Loggers.MSG.info(
						"Step {}: admission opened after {} s of preparation (item distribution {} s; slowest {}, {} s)",
						loadStepId(),
						seconds(barrier.elapsedMillis()),
						seconds(barrier.distributionMillis()),
						slowest.getKey(),
						seconds(slowest.getValue()));
	}

	private static String seconds(final long millis) {
		return String.format(Locale.ROOT, "%.1f", millis / 1000.0);
	}

	private void initNicPathsCollector(final List<String> nodeAddrs, final List<Boolean> nicSetExpected) {
		if (fileMgrs.size() < 2 || nicPathsCollector != null) {
			return;
		}
		// Every slice writes its event-loop report, so a distributed step always collects.
		nicPathsCollector = new NicPathsCollector(
						loadStepId(), fileMgrs, NicPathsCollector.nodeNames(nodeAddrs), nicSetExpected);
	}

	/** On a distributed step with bucket versioning on, relays the other nodes' first-use notices. */
	private void initBucketVersioningRelay(final List<String> nodeAddrs) {
		// A relay left by an earlier start of this client would relay from that run's offsets.
		bucketVersioningRelay = fileMgrs.size() < 2 || !BucketVersioningRelay.versioningRequested(config, ctxConfigs)
						? null
						: new BucketVersioningRelay(loadStepId(), fileMgrs, NicPathsCollector.nodeNames(nodeAddrs));
	}

	/**
	 * Relays the other nodes' bucket versioning notices once the slices have stopped and before the
	 * step publishes its total metrics, after which the CLI may fetch the messages and errors.
	 */
	private void relayBucketVersioningNoticesAtStop() {
		if (bucketVersioningRelay != null) {
			try (final var logCtx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
				bucketVersioningRelay.relayAtStop();
			}
		}
	}

	/** Relays the nodes' notices written so far when a close fails, keeping the close's failure. */
	private void relayBucketVersioningNoticesAtClose(final Throwable failure) {
		if (bucketVersioningRelay != null) {
			try (final var logCtx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
				bucketVersioningRelay.close();
			} catch (final RuntimeException e) {
				failure.addSuppressed(e);
			}
		}
	}

	private void initDeleteArtifactAggregator(final List<String> nodeAddrs) {
		if (!supportsStandaloneDeleteArtifactAggregation()
						|| !standaloneDeleteEnabled()
						|| !deleteArtifactAggregators.isEmpty()) {
			return;
		}
		final String itemInputFile = config.configVal("item").configVal("input").stringVal("file");
		if (itemInputFile == null || itemInputFile.isBlank()) {
			throw terminalFailure(
							IntegrityTerminalException.Category.INPUT,
							"Standalone DELETE artifact finalization requires a frozen selection manifest",
							null);
		}
		final java.nio.file.Path selection = java.nio.file.Path.of(itemInputFile).toAbsolutePath().normalize();
		try {
			deleteArtifactAggregators.add(new DeleteArtifactAggregator(
							loadStepId(),
							fileMgrs,
							distributedContributorIds(config, nodeAddrs),
							selection,
							IntegrityManifestCompletion.completionPath(selection)));
		} catch (final IOException e) {
			throw terminalFailure(
							IntegrityTerminalException.Category.CONFIGURATION,
							"failed to initialize DELETE artifact aggregation",
							e);
		}
	}

	/** Enables standalone DELETE finalization only for a load-step shape that owns this contract. */
	protected boolean supportsStandaloneDeleteArtifactAggregation() {
		return false;
	}

	private void resetDurationLifecycleForStart() {
		if (standaloneDeleteEnabled()) {
			stopFailureBudgetMonitor();
			closeFailureBudgetSnapshotCollector();
			failureBudgetFailure = null;
			failureBudgetFailureReason = null;
			failureBudgetStartedNanos = Long.MIN_VALUE;
			standaloneDeleteStopInProgress = false;
			standaloneDeleteStopCoordinated = false;
			deletePostVerificationGloballySkipped = false;
			failureBudgetFinalized = false;
			failureBudgetTerminalFailure = null;
			incompleteDurationStopFailure = null;
			failureBudgetMetricsOutputDeferred = false;
			durationSliceStopInterrupted = false;
			durationStopFailure = null;
			durationAdmissionBarrierSatisfied = false;
			durationCleanupRetryPending = false;
			durationDrainDeadlineNanos = Long.MIN_VALUE;
			durationDrainDeadlineSet = false;
			durationValidityFailure = null;
			durationAwaitStatuses.clear();
		}
	}

	// determine the additional/remote full node addresses
	private static List<String> remoteNodeAddrs(final Config config) {
		final var nodeConfig = config.configVal("load-step-node");
		final var nodePort = nodeConfig.intVal("port");
		final var nodeAddrs = nodeConfig.<String> listVal("addrs");
		return nodeAddrs == null || nodeAddrs.isEmpty() ? Collections.EMPTY_LIST : nodeAddrs.stream().map(addr -> NetUtil.addPortIfMissing(addr, nodePort)).collect(Collectors.toList());
	}

	static List<String> participatingRemoteNodeAddrs(
					final Config config, final List<String> configuredRemoteNodeAddrs) {
		if (!IntegrityConfig.requiresExactOutputCount(config.configVal("storage"))) {
			return configuredRemoteNodeAddrs;
		}
		final long countLimit = config.longVal("load-op-limit-count");
		final long configuredSliceCount = 1L + configuredRemoteNodeAddrs.size();
		if (countLimit <= 0 || countLimit >= configuredSliceCount) {
			return configuredRemoteNodeAddrs;
		}
		final int activeRemoteCount = (int) countLimit - 1;
		return List.copyOf(configuredRemoteNodeAddrs.subList(0, activeRemoteCount));
	}

	static List<String> distributedContributorIds(
					final Config config, final List<String> configuredRemoteNodeAddrs) {
		final List<String> participatingRemotes = participatingRemoteNodeAddrs(
						config, configuredRemoteNodeAddrs);
		final List<String> contributors = new ArrayList<>(1 + participatingRemotes.size());
		contributors.add(MetricsConstants.FLEET_LOCAL_CONTRIBUTOR_ID);
		contributors.addAll(participatingRemotes);
		return List.copyOf(contributors);
	}

	private void initFileManagers(final List<String> nodeAddrs, final List<FileManager> fileMgrsDst) {
		lostNodeIndexes.clear();
		// local file manager
		fileMgrsDst.add(FileManager.INSTANCE);
		// remote file managers
		for (final var nodeAddr : nodeAddrs) {
			final var fileMgr = resolveFileManager(nodeAddr);
			if (null == fileMgr) {
				if (Thread.currentThread().isInterrupted()) {
					// A stop ended the retries, and the node may yet have been reachable.
					throwUnchecked(new InterruptedException(loadStepId() + ": stopped while resolving the file manager service @ " + nodeAddr));
				}
				throw new IllegalStateException(
								"Failed to resolve the file manager service @ " + nodeAddr
												+ ". Ensure the remote node is running with --run-node");
			}
			// A lost slice's node gets no further file call: its files are not collected.
			final int sliceIndex = fileMgrsDst.size();
			fileMgrsDst.add(LostNodeFileManager.guard(
							fileMgr,
							() -> lostNodeIndexes.contains(sliceIndex),
							() -> Loggers.ERR.warn(
											"{}: the entry collects none of the files of node {}, whose step slice it lost: the item output and the other results leave that node out, and an artifact that needs every node's file, such as operation.lifecycle.csv, is not published",
											loadStepId(),
											nodeAddr)));
		}
	}

	/** The file manager of the node {@code nodeAddr}, or null when it cannot be resolved; tests supply their own. */
	FileManager resolveFileManager(final String nodeAddr) {
		return resolveFileManagerWithRetries(nodeAddr, 10);
	}

	private static FileManagerService resolveFileManagerWithRetries(final String nodeAddrWithPort, final int maxRetries) {
		return resolveFileManagerWithRetries(nodeAddrWithPort, maxRetries, FileManagerClient::resolve, Thread::sleep);
	}

	/** One try to resolve a node's file manager service; tests supply their own. */
	@FunctionalInterface
	interface FileManagerResolver {
		FileManagerService resolve(String nodeAddrWithPort) throws Exception;
	}

	/** The sleep between two tries; it throws on interrupt as {@link Thread#sleep(long)} does. */
	@FunctionalInterface
	interface RetrySleeper {
		void sleep(long millis) throws InterruptedException;
	}

	/**
	 * Tries {@code maxRetries} times to resolve the file manager service of {@code nodeAddrWithPort}.
	 * Only a {@link ConnectException} (connect refused or timed out) is retried, after a sleep of
	 * {@code min(2^n s, 2 min)} following try {@code n}; there is no sleep after the last try.
	 *
	 * @return the service, or null when every try failed, a failure is not retried, or the thread is
	 *     interrupted (the interrupt flag is then left set)
	 */
	static FileManagerService resolveFileManagerWithRetries(
					final String nodeAddrWithPort, final int maxRetries, final FileManagerResolver resolver,
					final RetrySleeper sleeper) {
		final long startNanos = System.nanoTime();
		FileManagerService fms = null;
		int retryCount = 0;
		while (null == fms && retryCount < maxRetries && !Thread.currentThread().isInterrupted()) {
			try {
				retryCount++;
				fms = resolver.resolve(nodeAddrWithPort);
			} catch (final ConnectException e) {
				if (retryCount >= maxRetries) {
					LogUtil.exception(
									Level.ERROR, e, "Failed to resolve the file manager service @ {}. Giving up after {} "
													+ "tries in {} s",
									nodeAddrWithPort, retryCount,
									TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startNanos));
					break;
				}
				LogUtil.exception(
								Level.ERROR, e, "Failed to resolve the file manager service @ {}. Will try to " +
												"reconnect. {} retry out of {}",
								nodeAddrWithPort, retryCount, maxRetries);
				try {
					final int sleepTime = 1000 * (int) Math.pow(2, retryCount);
					sleeper.sleep(Math.min(sleepTime, MAX_SLEEP_TIME_MILLIS));
				} catch (final InterruptedException ie) {
					Thread.currentThread().interrupt();
					break;
				}
			} catch (final UnknownHostException e) {
				LogUtil.exception(
								Level.ERROR, e, "Failed to resolve the hostname @ {}. Stopping workload",
								nodeAddrWithPort);
				break;
			} catch (final Exception e) {
				LogUtil.exception(
								Level.ERROR, e, "Failed to resolve the file manager service @ {}. No action " +
												"taken. Report to devs. Stopping workload.",
								nodeAddrWithPort);
				break;
			}
		}
		return fms;
	}

	/** The data input the entry's driver for {@code config} reads its data from. */
	private static DataInput entryDataInput(final Config config) throws IOException {
		return entryDataInput(config, entryDataLayerSize(config));
	}

	/**
	 * The data input layer size of {@code config}. The entry parses it before it builds its driver, so
	 * a malformed size fails the step before any slice starts, as in 5.17.0.
	 */
	private static SizeInBytes entryDataLayerSize(final Config config) {
		final var itemDataInputLayerSizeRaw = config.configVal("item").configVal("data").configVal("input").configVal("layer").val("size");
		if (itemDataInputLayerSizeRaw instanceof String) {
			return BinarySizeFormat.parseSize((String) itemDataInputLayerSizeRaw);
		}
		return new SizeInBytes(TypeUtil.typeConvert(itemDataInputLayerSizeRaw, int.class));
	}

	/** The data input the entry's driver for {@code config} reads its data from, at {@code itemDataLayerSize}. */
	private static DataInput entryDataInput(final Config config, final SizeInBytes itemDataLayerSize)
					throws IOException {
		final var itemDataConfig = config.configVal("item").configVal("data");
		final var itemDataInputConfig = itemDataConfig.configVal("input");
		final var itemDataInputLayerConfig = itemDataInputConfig.configVal("layer");
		return DataInput.instance(
						itemDataInputConfig.stringVal("file"),
						itemDataInputConfig.stringVal("seed"),
						itemDataLayerSize,
						itemDataInputLayerConfig.intVal("cache"),
						itemDataInputLayerConfig.boolVal("heap"),
						itemDataInputConfig.doubleVal("compressibility"),
						itemDataConfig.boolVal("dedupable"));
	}

	/** The verify flag the entry's driver for {@code config} is built with, without its warning. */
	private static boolean entryVerifyFlag(final Config config) {
		final var itemDataConfig = config.configVal("item").configVal("data");
		return itemDataConfig.boolVal("verify") && itemDataConfig.boolVal("dedupable");
	}

	private void addFileClients(
					final Config config, final List<Config> configSlices, final boolean suppliesItems,
					final boolean controlsOperations, final String outputStepId) {
		// Before the input path is listed into the engines' items files, while it is still a path. A
		// mixed step keeps the default create type, but its listed items are only the GET and STAT
		// pool, and its PUTs name new items, so it never copies a listed item.
		if (suppliesItems && !mixesOperations()) {
			CopyIntoItsSource.check(loadStepId(), config);
		}
		final var loadConfig = config.configVal("load");
		final var batchSize = loadConfig.intVal("batch-size");
		final var storageConfig = config.configVal("storage");
		final var itemConfig = config.configVal("item");
		final var itemDataConfig = itemConfig.configVal("data");
		final var verifyFlag = itemDataConfig.boolVal("verify");
		final var itemDataInputConfig = itemDataConfig.configVal("input");
		final var itemDataInputFile = itemDataInputConfig.stringVal("file");
		final var isDedupable = itemDataConfig.boolVal("dedupable");
		final var effectiveVerifyFlag = effectiveVerifyFlag(verifyFlag, isDedupable, loadStepId());
		final var opConfig = loadConfig.configVal("op");
		final var opType = OpType.valueOf(opConfig.stringVal("type").toUpperCase(Locale.ROOT));
		final var integrityConfig = IntegrityConfig.fromStorage(storageConfig);
		if (integrityConfig.enabled()
						&& (OpType.READ.equals(opType) || OpType.DELETE.equals(opType))) {
			integrityConfig.requireInputProvenance(opType.name());
			if (OpType.READ.equals(opType) && itemDataInputFile != null && !itemDataInputFile.isEmpty()) {
				throw terminalFailure(
								IntegrityTerminalException.Category.CONFIGURATION,
								"metadata verification does not support item.data.input.file",
								null);
			}
			final var itemInputFile = itemConfig.configVal("input").stringVal("file");
			if (itemInputFile == null || itemInputFile.isEmpty()) {
				throw terminalFailure(
								IntegrityTerminalException.Category.INPUT,
								"metadata-mode " + opType + " requires a finite item input file",
								null);
			}
			if (IntegrityInputProvenance.ENGINE_STEP.equals(integrityConfig.inputProvenance())
							|| IntegrityInputProvenance.CLI_STAGER.equals(integrityConfig.inputProvenance())) {
				try {
					IntegrityManifestCompletion.validate(
									java.nio.file.Path.of(itemInputFile),
									config.longVal("run-id"),
									integrityConfig.inputProvenance(),
									integrityConfig.expectedProducerId());
				} catch (final IOException e) {
					throw terminalFailure(
									IntegrityTerminalException.Category.INPUT,
									"integrity input is not committed by the expected producer",
									e);
				}
			}
		}

		initIntegrityLogFileAggregators(integrityConfig, opType, itemConfig);
		if (operationLifecycleAggregators.isEmpty()) {
			try {
				for (final var artifact : StepIdFiles.operationLifecycle(opType, StepIdFiles.rangeRead(config, ctxConfigs))) {
					operationLifecycleAggregators.add(new CsvLoggerArtifactAggregator(loadStepId(), fileMgrs,
									artifact.loggerName(), artifact.fileName(), artifact.header()));
				}
			} catch (final IOException e) {
				throw new IllegalStateException("failed to initialize operation lifecycle aggregation", e);
			}
		}
		final var itemType = ItemType.valueOf(itemConfig.stringVal("type").toUpperCase(Locale.ROOT));
		final boolean skipScatter = ItemType.PATH.equals(itemType) && OpType.LIST.equals(opType);
		if (skipScatter) {
			Loggers.MSG.info("{}: skipping item input scatter for LIST workload", loadStepId());
		}

		final var itemDataLayerSize = entryDataLayerSize(config);
		try (
						final var dataInput = entryDataInput(config, itemDataLayerSize);
						final var storageDriver = StorageDriver.instance(
										extensions, storageConfig, dataInput, effectiveVerifyFlag, batchSize, loadStepId())) {
			// The driver is owned from here, so a refusal or any other failure below closes it.
			if (controlsOperations) {
				refuseCompositeWithoutSupport(storageDriver, config);
			}
			try (final var itemInput = skipScatter ? null : ItemInputFactory.createItemInput(itemConfig, batchSize, storageDriver)) {
				if (null != itemDataInputFile && !itemDataInputFile.isEmpty()) {
					itemDataInputFileSlicers.add(
									new ItemDataInputFileSlicer(loadStepId(), fileMgrs, configSlices, itemDataInputFile, batchSize));
					Loggers.MSG.debug("{}: item data input file slicer initialized", loadStepId());
				}
				if (!skipScatter && itemInput != null) {
					final var slicer = new ItemInputFileSlicer(
									loadStepId(), fileMgrs, configSlices, itemInput, batchSize, integrityConfig.enabled(), itemSizeRecording);
					itemInputFileSlicers.add(slicer);
					Loggers.MSG.debug("{}: item input file slicer initialized", loadStepId());
					if (!integrityConfig.enabled() && slicer.distributedItems() == 0) {
						requireAnItemFromAFile(itemConfig, opType);
					}
					if (suppliesItems) {
						requireAnItemPerEngine(config, slicer.distributedItems(), configSlices.size());
					}
				} else if (!skipScatter && !integrityConfig.enabled()) {
					distributeNoItemsIfNothingToDelete(itemConfig, opType, itemType, configSlices, batchSize);
				}
				itemInputFileSlicers.addAll(sliceOwnItemFiles(config, fileMgrs, configSlices, batchSize));
			}
		} catch (final RunTerminalException e) {
			if (integrityConfig.enabled()) {
				// This message is already public, and names what's wrong with the input.
				throw terminalFailure(IntegrityTerminalException.Category.INPUT, e.getMessage(), e);
			}
			throw e;
		} catch (final IllegalConfigurationException e) {
			if (integrityConfig.enabled()) {
				throw terminalFailure(
								IntegrityTerminalException.Category.CONFIGURATION,
								"failed to initialize metadata verification storage driver",
								e);
			}
			LogUtil.exception(Level.ERROR, e, "{}: failed to init the storage driver", loadStepId());
		} catch (final InterruptedException e) {
			throwUnchecked(e);
		} catch (final Exception e) {
			if (integrityConfig.enabled()) {
				throw terminalFailure(
								IntegrityTerminalException.Category.INPUT,
								"failed to validate or distribute integrity input",
								e);
			}
			LogUtil.exception(Level.WARN, e, "{}: failed to close the item input", loadStepId());
		}
		final var itemOutputFile = config.stringVal("item-output-file");
		if (itemOutputFile != null && !itemOutputFile.isEmpty()) {
			if (integrityConfig.enabled()) {
				final boolean requireExactOutputCount = IntegrityConfig.requiresExactOutputCount(storageConfig);
				final boolean requireNonEmptySelection = IntegrityConfig.requiresNonEmptySelection(storageConfig);
				final long recordLimit;
				if (OpType.LIST.equals(opType)) {
					recordLimit = storageConfig.configVal("integrity").configVal("selection").longVal("maxCount");
				} else if (OpType.CREATE.equals(opType) && requireExactOutputCount) {
					recordLimit = config.longVal("load-op-limit-count");
				} else {
					recordLimit = 0;
				}
				itemOutputFileAggregators.add(
								new CsvArtifactAggregator(
												loadStepId(), fileMgrs, configSlices, itemOutputFile, config.longVal("run-id"), recordLimit,
												opType, requireExactOutputCount, requireNonEmptySelection));
			} else {
				itemOutputFileAggregators.add(
								new ItemOutputFileAggregator(loadStepId(), fileMgrs, configSlices, itemOutputFile));
			}
			Loggers.MSG.debug("{}: item output file aggregator initialized", loadStepId());
		}

		// One aggregator per step id: the first collects and deletes each engine's file of that id.
		if (config.boolVal("output-metrics-timing-persist") && timingMetricsStepIds.add(outputStepId)) {
			itemTimingMetricsOutputFileAggregators.add(
							new ItemTimingMetricOutputFileAggregator(outputStepId, fileMgrs));
			Loggers.MSG.debug("{}: item metrics output file aggregator initialized", outputStepId);
		}

		if (config.boolVal("output-metrics-trace-persist") && opTraceStepIds.add(outputStepId)) {
			opTraceLogFileAggregators.add(new OpTraceLogFileAggregator(outputStepId, fileMgrs));
			Loggers.MSG.debug("{}: operation traces log file aggregator initialized", outputStepId);
		}
		final var storageAuthFile = storageConfig.stringVal("auth-file");
		if (storageAuthFile != null && !storageAuthFile.isEmpty()) {
			storageAuthFileSlicers.add(
							new TempInputTextFileSlicer(
											loadStepId(), storageAuthFile, fileMgrs, "storage-auth-file", configSlices, batchSize));
			Loggers.MSG.debug("{}: storage auth file slicer initialized", loadStepId());
		}
	}

	private void initIntegrityLogFileAggregators(
					final IntegrityConfig integrityConfig, final OpType opType, final Config itemConfig) {
		if (!integrityConfig.enabled() || !integrityLogFileAggregators.isEmpty()) {
			return;
		}
		try {
			for (final var artifact : StepIdFiles.integrity(integrityConfig, opType, itemConfig)) {
				integrityLogFileAggregators.add(new CsvLoggerArtifactAggregator(
								loadStepId(), fileMgrs, artifact.loggerName(), artifact.fileName(), artifact.header()));
			}
		} catch (final IOException e) {
			throw terminalFailure(
							IntegrityTerminalException.Category.CONFIGURATION,
							"failed to initialize integrity logger artifact aggregation",
							e);
		}
	}

	@Override
	protected boolean emitsOperationArtifacts() {
		return false;
	}

	/**
	 * Called at close, after every step slice is closed and the item output file has received the
	 * records of every slice. The default does nothing.
	 *
	 * @param itemOutputFile the step's item output file
	 * @param successCounts the step's final count of successful operations by type, without the
	 *                      types whose final metrics were unavailable
	 */
	protected void itemOutputCollected(final String itemOutputFile, final Map<OpType, Long> successCounts)
					throws IOException {}

	static Map<OpType, Long> successCounts(
					final List<? extends MetricsContext<? extends AllMetricsSnapshot>> contexts) {
		final Map<OpType, Long> counts = new EnumMap<>(OpType.class);
		for (final var context : contexts) {
			final AllMetricsSnapshot snapshot = context.lastSnapshot();
			if (snapshot != null && snapshot.successSnapshot() != null) {
				counts.merge(context.opType(), snapshot.successSnapshot().count(), Math::addExact);
			}
		}
		return counts;
	}

	private void initAndStartMetricsAggregator(Config config) {
		try (final var logCtx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			// The standalone DELETE duration route has its own loss handling.
			metricsFetchLosesSlices = !standaloneDeleteDurationMode();
			metricsAggregator = new MetricsAggregatorImpl(loadStepId(), stepSlices, config, new MetricsAggregatorImpl.SliceLoss() {
				@Override
				public boolean isLost(final LoadStep slice) {
					return sliceLost && isLostSlice(slice);
				}

				@Override
				public boolean fetchFailed(final LoadStep slice, final RemoteException failure) {
					return recordLostSliceAtMetricsFetch(slice, failure);
				}
			});
			metricsAggregator.start();
		} catch (final Exception e) {
			LogUtil.exception(Level.ERROR, e, "{}: failed to start the metrics aggregator", loadStepId());
		}
	}

	/**
	 * An empty items file ends the run, except for a DELETE: the cleanup of a step that created
	 * nothing has nothing to do. An empty listing isn't a file, and stays a run with no items.
	 */
	void requireAnItemFromAFile(final Config itemConfig, final OpType opType) {
		if (itemInputFile(itemConfig) != null && !OpType.DELETE.equals(opType)) {
			throw new RunTerminalException(
							IntegrityTerminalException.Category.INPUT, loadStepId(), "the items file has no items", null);
		}
	}

	/**
	 * A recycle step limited by count ends when each engine reaches its share, which an engine with
	 * no items never does, so fewer items than engines end the run. The step's time limit ends the
	 * step anyway, so a duration run with an idle engine runs as before. Only the base configuration's
	 * time limit ends a step: a context's is never enforced. No items at all is for
	 * {@link #requireAnItemFromAFile}.
	 */
	void requireAnItemPerEngine(final Config config, final long items, final int engines) {
		if (items == 0
						|| items >= engines
						|| !config.boolVal("load-op-recycle-mode")
						|| config.longVal("load-op-limit-count") <= 0
						|| !withoutTimeLimit()) {
			return;
		}
		final String input = itemInputFile(config.configVal("item")) != null ? "the items file" : "the item input";
		throw new RunTerminalException(
						IntegrityTerminalException.Category.INPUT,
						loadStepId(),
						input + " has " + items + (items == 1 ? " item" : " items") + " for " + engines
										+ " engines; each engine needs at least one in a step limited by count: add items, or pass"
										+ " fewer --test-hosts",
						null);
	}

	/**
	 * Whether the step's time limit, which {@link LoadStepBase} enforces, is 0. One that can't be read
	 * counts as a limit, so the step fails where it is read, as it always has.
	 */
	private boolean withoutTimeLimit() {
		final Object raw = config.val("load-step-limit-time");
		try {
			final long seconds = raw instanceof String text
							? TimeUtil.getTimeInSeconds(text)
							: TypeUtil.typeConvert(raw, long.class);
			return seconds <= 0;
		} catch (final RuntimeException e) {
			return false;
		}
	}

	/** A slice reads a count of 0 as no limit, so each engine needs a count of at least 1. */
	private RunTerminalException countBelowEngineCount(final long countLimit, final int engines) {
		return new RunTerminalException(
						IntegrityTerminalException.Category.CONFIGURATION,
						loadStepId(),
						"the count limit (" + countLimit + ") is below the number of engines (" + engines
										+ "), and each engine needs a count of at least 1: raise the count limit to at least "
										+ engines + " (--object-count, or --seed-objects for a seed step), or pass fewer --test-hosts",
						null);
	}

	/**
	 * Configured items that couldn't be opened end the run. A DELETE whose file doesn't exist is the
	 * cleanup of a step that wrote nothing, so its slices get no items and it finishes at once.
	 */
	void distributeNoItemsIfNothingToDelete(
					final Config itemConfig,
					final OpType opType,
					final ItemType itemType,
					final List<Config> configSlices,
					final int batchSize) {
		final String file = itemInputFile(itemConfig);
		final String path = itemConfig.configVal("input").stringVal("path");
		if (file == null && (path == null || path.isEmpty())) {
			return;
		}
		if (file == null || !OpType.DELETE.equals(opType) || !Files.notExists(Path.of(file))) {
			throw new RunTerminalException(
							IntegrityTerminalException.Category.INPUT,
							loadStepId(),
							file == null
											? "the items input path could not be used"
											: "the items file \"" + Path.of(file).getFileName() + "\" could not be opened",
							null);
		}
		@SuppressWarnings("unchecked")
		final ItemFactory<Item> factory = (ItemFactory<Item>) ItemType.getItemFactory(itemType);
		itemInputFileSlicers.add(new ItemInputFileSlicer(
						loadStepId(),
						fileMgrs,
						configSlices,
						new CsvItemInput<>(InputStream.nullInputStream(), factory),
						batchSize));
	}

	private static String itemInputFile(final Config itemConfig) {
		final String file = itemConfig.configVal("input").stringVal("file");
		return file == null || file.isEmpty() ? null : file;
	}

	/**
	 * The op types with a composite implementation (CREATE or READ) that {@code config} makes: its
	 * {@code load-op-type} for a DATA item, else none, or what a {@link CompositeCheckedOperations}
	 * client returns.
	 */
	private Set<OpType> compositeCheckedOpTypes(final Config config) {
		final Set<OpType> opTypes;
		if (this instanceof CompositeCheckedOperations client) {
			opTypes = client.compositeCheckedOpTypes(config);
		} else if (ItemType.DATA.equals(ItemType.valueOf(config.stringVal("item-type").toUpperCase(Locale.ROOT)))) {
			opTypes = Set.of(OpType.valueOf(config.stringVal("load-op-type").toUpperCase(Locale.ROOT)));
		} else {
			opTypes = Set.of();
		}
		final Set<OpType> composite = EnumSet.noneOf(OpType.class);
		for (final OpType opType : opTypes) {
			if (DataOperationsBuilderImpl.buildsCompositeOperations(opType)) {
				composite.add(opType);
			}
		}
		return composite;
	}

	/**
	 * Refuses, on this engine and before the item input is created, a composite configuration of
	 * {@code config} on {@code driver} when its class declares {@link CompositeOperationsUnsupported}:
	 * the input of a READ from a storage path lists that path. The same check runs again in each step
	 * slice. The caller owns and closes the driver, and the refusal ends the run with its own message.
	 *
	 * <p>The threshold is read only for a CREATE or READ on such a driver. A threshold that does not
	 * parse is left to the step slice, whose own parse fails the run as it did before this check.
	 */
	private void refuseCompositeWithoutSupport(final StorageDriver<?, ?> driver, final Config config) {
		final var opTypes = compositeCheckedOpTypes(config);
		if (opTypes.isEmpty() || !CompositeOperationSupport.declaresNoCompositeSupport(driver)) {
			return;
		}
		final long sizeThreshold;
		try {
			sizeThreshold = CompositeOperationSupport.sizeThreshold(config.configVal("item-data-ranges"));
		} catch (final RuntimeException malformed) {
			return;
		}
		try {
			for (final OpType opType : opTypes) {
				CompositeOperationSupport.requireSupport(driver, opType, sizeThreshold);
			}
		} catch (final IllegalConfigurationException refusal) {
			LogUtil.exception(Level.ERROR, refusal, "{}: refused at step start", loadStepId());
			throw new RunTerminalException(
							IntegrityTerminalException.Category.CONFIGURATION, loadStepId(), refusal.getMessage(), refusal);
		}
	}

	/**
	 * Refuses, before the entry builds or lists any item input, a composite configuration of any
	 * appended context that controls the operations. The entry distributes the base first and each
	 * context after it, so the check on a context's own driver would come after the base's input was
	 * read. The base needs no check here: it is distributed first, and its own driver is checked
	 * before its input is created.
	 *
	 * <p>Whether a driver declares {@link CompositeOperationsUnsupported} is known only from the class
	 * its factory builds, so this builds the context's driver once, checks it and closes it. It does so
	 * only for a context that makes CREATE or READ operations with a threshold above 0. A scenario's
	 * threshold arrives as text, so it is parsed first; one that does not parse is left to the step
	 * slice. A driver that fails to build here is left to the distribution, which reports it as before.
	 */
	private void refuseCompositeInAppendedContexts() {
		if (ctxConfigs == null) {
			return;
		}
		for (var ctxIndex = 0; ctxIndex < ctxConfigs.size(); ctxIndex++) {
			final var ctxConfig = ctxConfigs.get(ctxIndex);
			if (!controlsOperations(ctxIndex) || compositeCheckedOpTypes(ctxConfig).isEmpty()) {
				continue;
			}
			final long threshold;
			try {
				threshold = CompositeOperationSupport.sizeThreshold(ctxConfig.configVal("item-data-ranges"));
			} catch (final RuntimeException malformed) {
				// Left to the step slice, whose own parse fails the run, as in 5.17.0.
				continue;
			}
			if (threshold <= 0) {
				continue;
			}
			try (
							final var dataInput = entryDataInput(ctxConfig);
							final var driver = StorageDriver.instance(
											extensions,
											ctxConfig.configVal("storage"),
											dataInput,
											entryVerifyFlag(ctxConfig),
											ctxConfig.intVal("load-batch-size"),
											loadStepId())) {
				refuseCompositeWithoutSupport(driver, ctxConfig);
			} catch (final RunTerminalException refusal) {
				throw refusal;
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			} catch (final Exception e) {
				LogUtil.exception(
								Level.DEBUG, e, "{}: no composite-support check before the distribution", loadStepId());
			}
		}
	}

	/**
	 * This message is public, so it counts the engine and leaves its address and the cause to the
	 * log. A refused composite configuration is the exception: its message names only the driver
	 * type, the op type and the setting, and is kept.
	 */
	RunTerminalException sliceStartFailure(final Exception cause, final int slice, final int sliceCount) {
		final var engine = "engine " + (slice + 1) + " of " + sliceCount;
		final var typed = RunTerminalException.find(cause);
		final var refusal = typed == null ? CompositeOperationSupport.find(cause) : null;
		if (refusal != null) {
			return new RunTerminalException(
							IntegrityTerminalException.Category.CONFIGURATION,
							loadStepId(),
							engine + " did not start: " + refusal.getMessage(),
							cause);
		}
		if (typed == null) {
			return new RunTerminalException(
							IntegrityTerminalException.Category.EXECUTION,
							loadStepId(),
							engine + " did not start; see the engine log",
							cause);
		}
		final var nodeRefusal = nodeStepIdRefusal(typed, cause, slice, sliceCount);
		if (nodeRefusal != null) {
			return nodeRefusal;
		}
		return sliceCount == 1
						? typed.withStepId(loadStepId())
						: new RunTerminalException(typed.category(), loadStepId(), engine + ": " + typed.getMessage(), cause);
	}

	/**
	 * Another node's refusal of its slice at the start for its step-id files, as the Entry Node words
	 * it for another node, the engine counted: the node's own message names its file, so it goes to
	 * the log. Otherwise {@code null}.
	 */
	private RunTerminalException nodeStepIdRefusal(
					final RunTerminalException typed, final Exception cause, final int slice, final int sliceCount) {
		if (slice < 1 || !(typed instanceof StepIdInUseException || typed instanceof StepIdCheckException)) {
			return null;
		}
		final String node = slice < stepSliceNodeAddrs.size() && stepSliceNodeAddrs.get(slice) != null
						? stepSliceNodeAddrs.get(slice)
						: "of engine " + (slice + 1);
		return typed instanceof StepIdInUseException
						? StepIdInUseException.fromNode(loadStepId(), slice, sliceCount, node, typed)
						: StepIdCheckException.fromNode(loadStepId(), slice, sliceCount, node, cause);
	}

	/**
	 * A metadata-mode slice that did not start: an integrity failure, unless its cause has a category
	 * of its own, as a refused step id has, which it keeps with its message.
	 */
	RunTerminalException metadataSliceStartFailure(final Exception cause, final int slice, final int sliceCount) {
		final var typed = RunTerminalException.find(cause);
		if (typed != null && !(typed instanceof IntegrityTerminalException)) {
			return sliceStartFailure(cause, slice, sliceCount);
		}
		return terminalFailure(
						IntegrityTerminalException.Category.EXECUTION, "failed to start metadata-mode step slice " + slice, cause);
	}

	/**
	 * A slice that didn't start under the admission barrier: its engine named, and the category of a
	 * cause that has one, such as a bad items line. Another node's refusal for its step-id files reads
	 * as {@link #sliceStartFailure} words it.
	 */
	RunTerminalException barrierStartFailure(
					final Exception cause, final String engine, final int slice, final int sliceCount) {
		final var typed = RunTerminalException.find(cause);
		final var nodeRefusal = nodeStepIdRefusal(typed, cause, slice, sliceCount);
		if (nodeRefusal != null) {
			return nodeRefusal;
		}
		final var refusal = typed == null ? CompositeOperationSupport.find(cause) : null;
		if (refusal != null) {
			return new RunTerminalException(
							IntegrityTerminalException.Category.CONFIGURATION,
							loadStepId(),
							"Step " + loadStepId() + " did not start: " + engine + ": " + refusal.getMessage(),
							cause);
		}
		return new RunTerminalException(
						typed != null ? typed.category() : IntegrityTerminalException.Category.EXECUTION,
						loadStepId(),
						"Step " + loadStepId() + " did not start: " + engine + ": " + cause.getMessage(),
						cause);
	}

	private void initAndStartStepSlices(
					final List<String> nodeAddrs, final List<Config> configSlices, final List<List<Config>> ctxConfigsSlices,
					final MetricsManager metricsManager, final AdmissionBarrier barrier) {
		final String stepTypeName;
		try {
			stepTypeName = getTypeName();
		} catch (final RemoteException e) {
			throw new AssertionError(e);
		}
		final var sliceCount = configSlices.size();
		final boolean validateRunId = IntegrityConfig.fromStorage(config.configVal("storage")).enabled();
		final long expectedRunId = runId();
		for (var i = 0; i < sliceCount; i++) {
			checkStartStopped("stopped before starting step slice " + i);
			final var configSlice = configSlices.get(i);
			final LoadStep stepSlice;
			if (i == 0) {
				stepSlice = LoadStepFactory.createLocalLoadStep(
								configSlice, extensions, ctxConfigsSlices.get(i), metricsManager, stepTypeName);
			} else {
				final var nodeAddrWithPort = nodeAddrs.get(i - 1);
				stepSlice = LoadStepSliceUtil.resolveRemote(
								configSlice, ctxConfigsSlices.get(i), stepTypeName, nodeAddrWithPort);
				if (stepSlice == null) {
					throw new IllegalStateException(
									"Failed to resolve the load step service @ " + nodeAddrWithPort
													+ ". Ensure the remote node is running with --run-node");
				}
			}
			stepSliceNodeAddrs.add(i == 0 ? null : nodeAddrs.get(i - 1));
			stepSlices.add(stepSlice);
			createdSliceUnreached = true;
			checkStartStopped("stopped after resolving step slice " + i);
			// A stop during either node call: the slice is closed unstarted, and an armed one fenced.
			if (validateRunId) {
				requireMatchingRunId(stepSlice, expectedRunId);
				checkStartStopped("stopped while checking the run id of step slice " + i);
			}
			if (barrier != null) {
				barrier.arm(i == 0 ? NicSetConfig.ENTRY_KEY : nodeAddrs.get(i - 1), stepSlice);
				checkStartStopped("stopped while preparing step slice " + i);
			}
			try {
				createdSliceUnreached = false;
				stepSlice.start();
				startedSliceCount = i + 1;
			} catch (final Exception e) {
				// A slice refused for its step-id files found a file in its folder that is not this step's.
				final var refusal = RunTerminalException.find(e);
				sliceRefusedForStepIdFiles = refusal instanceof StepIdInUseException || refusal instanceof StepIdCheckException;
				if (e instanceof InterruptedException) {
					throwUnchecked(e);
				}
				if (barrier == null && Thread.currentThread().isInterrupted()) {
					// A stop interrupts this thread, and a start it cut short is that stop, not a failure.
					throwUnchecked(new InterruptedException(loadStepId() + ": stopped while starting a step slice"));
				}
				if (barrier == null && validateRunId) {
					throw metadataSliceStartFailure(e, i, sliceCount);
				}
				LogUtil.exception(
								Level.ERROR, e, "{}: failed to start the step slice \"{}\"", loadStepId(), stepSlice);
				if (barrier != null) {
					throw barrierStartFailure(e, i == 0 ? NicSetConfig.ENTRY_KEY : nodeAddrs.get(i - 1), i, sliceCount);
				}
				// One engine missing would leave a lower load reported as the one asked for.
				throw sliceStartFailure(e, i, sliceCount);
			}
			checkStartStopped("stopped while starting step slice " + i);
		}
	}

	private void checkStartStopped(final String detail) {
		if (Thread.currentThread().isInterrupted() || JvmExit.begun()) {
			throwUnchecked(new InterruptedException(loadStepId() + ": " + detail));
		}
	}

	/**
	 * Records that this entry lost a started remote slice: its await, stop, close or metrics fetch
	 * failed in RMI, or the worker no longer exports it. The entry no longer fetches that worker's
	 * metrics, so the step's totals can leave out all of its operations, and the step fails. In
	 * the await of an ordinary distributed step the first loss stops the whole step at once: every
	 * started slice that is not lost, the entry's own included, is stopped there and then, each from a
	 * thread of its own, since a worker's await answers only once its stop has drained, and the
	 * entry's own slice may be draining inside its await; a lost slice gets its one close instead. The await's stop then waits for those stops
	 * and fails. A slice whose contact comes back stays lost. Each slice is recorded and logged once.
	 * <p>Once a slice is lost the entry stops calling it, as a frozen worker would hold each call for
	 * its connection's handshake: the slice gets no stop, shutdown, await poll, metrics fetch or file
	 * call, and no remote {@code loadStepId()} for a log context. It gets one close instead, at once,
	 * since a lost worker may still be alive and generating, and its worker stops on that close; see
	 * {@link #closeLostSliceOnce}. A slice lost at its close gets no other one. A standalone DELETE
	 * keeps its own coordination of its slices' stops and closes, and its slices get them as before.
	 *
	 * @return whether the slice is a lost one, now or before; false for a slice that never started,
	 *     whose failure to start already ends the step, and for the entry's own slice
	 */
	private boolean recordLostSlice(final LoadStep slice, final String call, final RemoteException cause) {
		final LoadStep[] slices = stepSlices.toArray(LoadStep[]::new);
		final int index = sliceIndex(slices, slice);
		if (index < 1 || index >= startedSliceCount) {
			return false;
		}
		synchronized (lostSliceLock) {
			if (lostSlices == null) {
				lostSlices = Collections.newSetFromMap(new IdentityHashMap<>());
			}
			if (!lostSlices.add(slice)) {
				return true;
			}
			sliceLost = true;
			lostNodeIndexes.add(index);
			final var sliceCount = slices.length;
			final String nodeAddr = index < stepSliceNodeAddrs.size()
							? stepSliceNodeAddrs.get(index)
							: String.valueOf(slice);
			Loggers.ERR.error(
							"{}: lost the step slice on node {} at its {} ({}); the step's totals can leave out all of that node's operations, so the step fails",
							loadStepId(),
							nodeAddr,
							call,
							oneLine(cause));
			// The message is public, so it counts the engine and leaves the node's address to the log.
			final var failure = new RunTerminalException(
							IntegrityTerminalException.Category.EXECUTION,
							loadStepId(),
							"engine " + (index + 1) + " of " + sliceCount
											+ ": the entry lost this node's step slice, so the step's totals can leave out all of its operations; see the entry's log",
							cause);
			if (lostSliceFailure == null) {
				lostSliceFailure = failure;
			} else {
				lostSliceFailure.addSuppressed(failure);
			}
			if (!"close".equals(call) && !standaloneDeleteEnabled()) {
				closeLostSliceOnce(slice, index, nodeAddr);
			}
			if (lostSliceStopsAtOnce && stopsAtOnce == null) {
				stopsAtOnce = stopStartedSlicesAtOnce(slices);
			}
		}
		return true;
	}

	/**
	 * Stops every started slice that is not lost, the entry's own included, each from a thread of
	 * its own, so that no engine goes on generating load while another one's stop drains: a worker's
	 * await, which the entry's poll of it is in, waits for that worker's stop, and the entry's own
	 * slice may drain inside its await. A lost slice has its close instead, which stops it first.
	 * Called holding lostSliceLock, at the first loss in the await, so the entry's own slice has
	 * started.
	 *
	 * @return the thread of each slice's stop, by slice
	 */
	private IdentityHashMap<LoadStep, Thread> stopStartedSlicesAtOnce(final LoadStep[] slices) {
		final IdentityHashMap<LoadStep, Thread> stops = new IdentityHashMap<>();
		var remoteStops = 0;
		for (var i = 0; i < startedSliceCount && i < slices.length; i++) {
			final var slice = slices[i];
			if (slice != null && !lostSlices.contains(slice)) {
				if (i > 0) {
					remoteStops++;
				}
				stops.put(slice, Thread.ofPlatform()
								.daemon(true)
								.name("spt-lost-slice-stop-" + loadStepId() + "-" + i)
								.unstarted(() -> stopSliceAtOnce(slice)));
			}
		}
		if (remoteStops == 1) {
			Loggers.MSG.info("{}: lost a step slice, so the entry stops the other remote step slice now", loadStepId());
		} else if (remoteStops > 1) {
			Loggers.MSG.info(
							"{}: lost a step slice, so the entry stops the other {} remote step slices now", loadStepId(),
							remoteStops);
		}
		stops.values().forEach(Thread::start);
		return stops;
	}

	/** A lost slice's one close: its thread, and when the step's close stops waiting for it. */
	private record LostSliceClose(Thread thread, long deadlineNanos) {}

	/**
	 * Sends a lost slice its one close, at once, from a virtual thread of its own: a worker that is
	 * still alive stops and closes its slice on it, and an interrupt ends the call where the worker
	 * does not answer. The step's close waits for it until {@link #lostSliceCloseBoundNanos} after
	 * it began, and then interrupts it. The close is best effort: its failure is no new loss and
	 * logs at DEBUG. A worker that never gets it, as one frozen until then, stops and closes the
	 * slice itself 120 s after the entry's last call. Called holding lostSliceLock.
	 */
	private void closeLostSliceOnce(final LoadStep slice, final int index, final String node) {
		if (lostSliceCloses == null) {
			lostSliceCloses = new IdentityHashMap<>();
		}
		final long deadlineNanos = System.nanoTime() + lostSliceCloseBoundNanos();
		final var stepId = loadStepId();
		final Thread close = Thread.ofVirtual()
						.name("spt-lost-slice-close-" + stepId + "-" + index)
						.unstarted(() -> {
							try (final var logCtx = put(KEY_STEP_ID, stepId).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
								slice.close();
								Loggers.MSG.debug("{}: closed the lost step slice on node {}", stepId, node);
							} catch (final Exception e) {
								LogUtil.trace(Loggers.ERR, Level.DEBUG, e, "{}: the close of the lost step slice on node {} failed",
												stepId, node);
							}
						});
		lostSliceCloses.put(slice, new LostSliceClose(close, deadlineNanos));
		close.start();
	}

	/**
	 * How long a lost slice's one close may take: the RMI response timeout when it is set above 0,
	 * otherwise 10 s, the shipped one.
	 */
	long lostSliceCloseBoundNanos() {
		final long responseTimeoutMillis = Long.getLong("sun.rmi.transport.tcp.responseTimeout", 0);
		return TimeUnit.MILLISECONDS.toNanos(responseTimeoutMillis > 0 ? responseTimeoutMillis : 10_000);
	}

	/**
	 * Waits for a lost slice's one close until its deadline, then interrupts it, so that the step's
	 * close goes on: the worker, if it ever gets the close, or else its own 120 s watchdog, closes
	 * the slice.
	 */
	private void awaitLostSliceClose(final LoadStep slice) {
		final LostSliceClose close;
		synchronized (lostSliceLock) {
			close = lostSliceCloses == null ? null : lostSliceCloses.remove(slice);
		}
		if (close == null) {
			return;
		}
		final long remainingNanos = close.deadlineNanos() - System.nanoTime();
		try {
			if (remainingNanos > 0) {
				close.thread().join(Duration.ofNanos(remainingNanos));
			}
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		if (close.thread().isAlive()) {
			close.thread().interrupt();
			Loggers.MSG.info(
							"{}: the lost step slice \"{}\" did not answer its close in time; its worker stops and closes it itself 120 s after the entry's last call",
							loadStepId(), slice);
		}
	}

	/**
	 * Whether the entry makes no further call to the slice: it is lost, and the step is not a
	 * standalone DELETE, which coordinates its slices' stops and closes itself.
	 */
	private boolean callsNoMore(final LoadStep slice) {
		return !standaloneDeleteEnabled() && isLostSlice(slice);
	}

	/** Stops the slice at once, and records a stop that ended before it was sent. */
	private void stopSliceAtOnce(final LoadStep slice) {
		var sent = false;
		try {
			sent = stopSlice(slice);
		} finally {
			if (!sent) {
				synchronized (lostSliceLock) {
					if (unsentStopsAtOnce == null) {
						unsentStopsAtOnce = Collections.newSetFromMap(new IdentityHashMap<>());
					}
					unsentStopsAtOnce.add(slice);
				}
			}
		}
	}

	/**
	 * Takes the slices whose stop at once ended before it was sent, apart from lost ones, whose
	 * stop would only fail again; each is returned once.
	 */
	private List<LoadStep> takeUnsentStopsAtOnce() {
		synchronized (lostSliceLock) {
			if (unsentStopsAtOnce == null || unsentStopsAtOnce.isEmpty()) {
				return List.of();
			}
			final List<LoadStep> unsent = unsentStopsAtOnce.stream()
							.filter(slice -> lostSlices == null || !lostSlices.contains(slice))
							.toList();
			unsentStopsAtOnce.clear();
			return unsent;
		}
	}

	/** Whether the step's first loss in the await stopped this slice at once. */
	private boolean stoppedAtOnce(final LoadStep slice) {
		synchronized (lostSliceLock) {
			return stopsAtOnce != null && stopsAtOnce.containsKey(slice);
		}
	}

	/** Arms the stop at once for the first loss in the await; see {@link #recordLostSlice}. */
	private void armLostSliceStop() {
		synchronized (lostSliceLock) {
			lostSliceStopsAtOnce = true;
		}
	}

	/**
	 * Disarms the stop at once, so that a later loss starts no stop of its own.
	 *
	 * @return the stops at once already started, by slice; empty when none
	 */
	private IdentityHashMap<LoadStep, Thread> disarmLostSliceStop() {
		synchronized (lostSliceLock) {
			lostSliceStopsAtOnce = false;
			return stopsAtOnce == null ? new IdentityHashMap<>() : stopsAtOnce;
		}
	}

	/**
	 * Waits for the stops at once, after the step's own stop has stopped each slice that no stop at
	 * once reached. An interrupt ends the wait, and a stop still in flight then overlaps none of the
	 * step's: its slice gets no other stop here, and the slice runs its close only after its stop,
	 * under its state lock, on the worker for a remote slice and on the entry for its own.
	 *
	 * @return whether every stop at once has ended
	 */
	private static boolean awaitStopsAtOnce(final Collection<Thread> stops) {
		try {
			for (final var stop : stops) {
				stop.join();
			}
			return true;
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			return false;
		}
	}

	/**
	 * A failed await call of a slice, other than a {@linkplain #isResponseReadTimeout response
	 * timeout}, loses the slice (see {@link #recordLostSlice}). An await whose response timed out is
	 * the worker still working inside the call, as a pipeline stage that drains its operations in
	 * flight when it ends does, for up to {@code load.op.wait.limit}: as in 5.17.0 the entry stops
	 * awaiting that slice and goes on to its normal stop and close of it, where a worker that has
	 * died or hung is still lost, unless a {@linkplain #recordLostSliceAtMetricsFetch metrics fetch}
	 * has lost it first. Unlike 5.17.0, it logs a warning, except for a slice the step has
	 * already stopped at once: its await waits for that stop, whose own warning names a slow drain.
	 */
	private void recordLostSliceAtAwait(final LoadStep slice, final RemoteException cause) {
		if (slicesClosing || closedAtJvmExit(stepSlices.size())) {
			return;
		}
		if (isResponseReadTimeout(cause)) {
			if (stoppedAtOnce(slice)) {
				LogUtil.trace(
								Loggers.ERR,
								Level.DEBUG,
								cause,
								"{}: the step slice \"{}\", stopped at once, did not answer its await within the RMI response timeout",
								loadStepId(),
								slice);
				return;
			}
			LogUtil.trace(
							Loggers.ERR,
							Level.WARN,
							cause,
							"{}: the step slice \"{}\" did not answer its await within the RMI response timeout; the entry no longer awaits it, and stops and closes it later, or only closes it once it has lost it",
							loadStepId(),
							slice);
			return;
		}
		recordLostSlice(slice, "await", cause);
	}

	/**
	 * A metrics fetch from a slice that failed in RMI loses it, whatever the failure, a
	 * {@linkplain #isResponseReadTimeout response timeout} included, unlike the await: a healthy
	 * worker answers the fetch also while its slice drains, inside its stop or inside its await, so
	 * a fetch that gets no answer is a worker that has stopped answering, as a frozen one. The
	 * entry's metrics fetch each aggregation period reaches every slice, so this also finds a lost
	 * slice whose await already answered done, as a worker that stops its own slice at its exit
	 * does, a slice that no await poller has reached, and a frozen worker whose await read timed
	 * out. Only until the step stops: a fetch still in flight when its slice closes at the normal
	 * end fails, and is no loss.
	 *
	 * @return whether the slice is a lost one, so it gets no further fetch
	 */
	private boolean recordLostSliceAtMetricsFetch(final LoadStep slice, final RemoteException cause) {
		if (!metricsFetchLosesSlices || slicesClosing || closedAtJvmExit(stepSlices.size())) {
			return false;
		}
		return recordLostSlice(slice, "metrics fetch", cause);
	}

	/**
	 * A failed stop of a slice, other than a {@linkplain #isResponseReadTimeout response timeout},
	 * loses the slice (see {@link #recordLostSlice}). A stop whose response timed out is the worker
	 * still draining its operations in flight, for up to {@code load.op.wait.limit}, past the RMI
	 * response timeout: it keeps 5.17.0's warning and does not fail the step, although that worker's
	 * operations that end after the timeout are missing from the totals.
	 *
	 * @param stopSent whether the failed call was the stop itself, rather than a call before it
	 * @return whether the slice is a lost one, so the caller logs nothing more
	 */
	private boolean recordLostSliceAtStop(final LoadStep slice, final RemoteException cause, final boolean stopSent) {
		if (isResponseReadTimeout(cause)) {
			synchronized (lostSliceLock) {
				if (stopSent) {
					if (slowStopSlices == null) {
						slowStopSlices = Collections.newSetFromMap(new IdentityHashMap<>());
					}
					slowStopSlices.add(slice);
				}
				// A slice already lost, at its await, keeps its one error line.
				return lostSlices != null && lostSlices.contains(slice);
			}
		}
		return recordLostSlice(slice, "stop", cause);
	}

	/** A stop of the slice answered, so a later close of it no longer waits behind a slow stop. */
	private void stopAnswered(final LoadStep slice) {
		synchronized (lostSliceLock) {
			if (slowStopSlices != null) {
				slowStopSlices.remove(slice);
			}
		}
	}

	/**
	 * A failed close of a slice loses it, like a failed stop. The one exception is a response timeout
	 * at the close of a slice whose latest stop timed out the same way: the worker's close waits for
	 * that stop, which is still draining, so it is the same slow stop and keeps the same warning. A
	 * close that times out after a stop that answered loses the slice.
	 *
	 * @return whether the slice is a lost one, so the caller logs nothing more
	 */
	private boolean recordLostSliceAtClose(final LoadStep slice, final RemoteException cause) {
		if (isResponseReadTimeout(cause)) {
			synchronized (lostSliceLock) {
				if (slowStopSlices != null && slowStopSlices.contains(slice)) {
					// A slice already lost keeps its one error line.
					return lostSlices != null && lostSlices.contains(slice);
				}
			}
		}
		return recordLostSlice(slice, "close", cause);
	}

	/**
	 * Whether a remote call failed only because the worker's response did not arrive within the RMI
	 * response timeout ({@code sun.rmi.transport.tcp.responseTimeout}, 10 s in the shipped engine).
	 * RMI reports a read of the response that timed out as an {@link UnmarshalException} with a
	 * {@link SocketTimeoutException} in its cause chain. A connect that timed out is a
	 * {@link java.rmi.ConnectException} or {@link java.rmi.ConnectIOException}, also when its cause is
	 * a {@code SocketTimeoutException}; a refused connect, a reset connection, a worker that died
	 * during the call and a released slice ({@link NoSuchObjectException}) fail in other ways. None of
	 * those is a response timeout.
	 */
	static boolean isResponseReadTimeout(final RemoteException failure) {
		if (!(failure instanceof UnmarshalException)) {
			return false;
		}
		Throwable cause = failure.getCause();
		for (var depth = 0; cause != null && depth < 16; depth++) {
			if (cause instanceof SocketTimeoutException) {
				return true;
			}
			cause = cause.getCause();
		}
		return false;
	}

	/** The failure's text on one line, as RMI exceptions put their nested exception on a new one. */
	private static String oneLine(final Throwable failure) {
		return failure.toString().replaceAll("\\s*\\R\\s*", " ").strip();
	}

	private boolean isLostSlice(final LoadStep slice) {
		synchronized (lostSliceLock) {
			return lostSlices != null && lostSlices.contains(slice);
		}
	}

	private RunTerminalException takeLostSliceFailure() {
		synchronized (lostSliceLock) {
			final var failure = lostSliceFailure;
			lostSliceFailure = null;
			return failure;
		}
	}

	private static int sliceIndex(final LoadStep[] slices, final LoadStep slice) {
		for (var i = 0; i < slices.length; i++) {
			if (slices[i] == slice) {
				return i;
			}
		}
		return -1;
	}

	void validateDeleteInventoriesBeforeAdmission() {
		if (!standaloneDeletePreValidationEnabled()) {
			return;
		}
		// Post-verification remains globally closed until every pre-validation slice
		// has completed successfully. Any unexpected coordinator failure therefore
		// fails closed even when abort propagation cannot reach a peer.
		deletePostVerificationGloballySkipped = standaloneDeletePostVerificationEnabled();
		try {
			coordinateDeleteInventoryPhase(
							"pre-validation",
							LoadStep::startDeleteInventoryPreValidation,
							LoadStep::isDeleteInventoryPreValidationComplete,
							IntegrityTerminalException.Category.INPUT);
		} catch (final IntegrityTerminalException failure) {
			propagateStrictPreValidationAbort(failure);
			throw failure;
		}
		deletePostVerificationGloballySkipped = false;
	}

	private void propagateStrictPreValidationAbort(final IntegrityTerminalException phaseFailure) {
		if (!standaloneDeletePostVerificationEnabled()) {
			return;
		}
		final List<LoadStep> activeSlices = stepSlices.stream()
						.filter(Objects::nonNull)
						.toList();
		final DurationPhaseAttempt result = invokeStepSlicePhase(
						activeSlices,
						LoadStep::skipDeleteInventoryPostVerificationAfterStrictPreValidationFailure,
						durationStopPhaseDeadlineNanos(),
						"spt-delete-pre-abort-");
		for (final Throwable propagationFailure : result.failures()) {
			phaseFailure.addSuppressed(terminalFailure(
							IntegrityTerminalException.Category.EXECUTION,
							"failed to propagate strict pre-validation abort to every distributed slice",
							propagationFailure));
		}
		if (!result.completedAll() && result.failures().isEmpty()) {
			phaseFailure.addSuppressed(terminalFailure(
							IntegrityTerminalException.Category.EXECUTION,
							"timed out propagating strict pre-validation abort to every distributed slice",
							new java.util.concurrent.TimeoutException(
											"distributed strict pre-validation abort propagation exceeded its deadline")));
		}
		if (result.interrupted()) {
			Thread.currentThread().interrupt();
		}
	}

	private void coordinateDeleteInventoryPhase(
					final String phase,
					final StepSlicePhase starter,
					final DeleteInventoryPhaseCompletion completion,
					final IntegrityTerminalException.Category category) {
		final List<LoadStep> activeSlices = stepSlices.stream()
						.filter(Objects::nonNull)
						.toList();
		if (activeSlices.isEmpty()) {
			return;
		}
		final String threadNamePrefix = "spt-delete-" + phase + "-";
		// A terminal result from one slice must not release the distributed barrier
		// while another slice is still completing its mandatory inventory pass.
		final Map<LoadStep, Throwable> terminalSliceFailures = Collections.synchronizedMap(
						new IdentityHashMap<>());
		final DurationPhaseAttempt startResult = invokeRetainedDurationPhase(
						"distributed-delete-" + phase + "-start",
						activeSlices,
						slice -> {
							try {
								starter.execute(slice);
							} catch (final InterruptedException interrupted) {
								Thread.currentThread().interrupt();
								throw interrupted;
							} catch (final Exception failure) {
								terminalSliceFailures.putIfAbsent(slice, failure);
							}
						},
						durationStopPhaseDeadlineNanos(),
						threadNamePrefix);
		throwDeleteInventoryPhaseFailure(phase, category, startResult, false);
		while (true) {
			final DurationPhaseAttempt completionResult = invokeRetainedDurationPhase(
							"distributed-delete-" + phase + "-completion",
							activeSlices,
							slice -> {
								if (terminalSliceFailures.containsKey(slice)) {
									return;
								}
								try {
									if (!completion.complete(slice)) {
										throw new DeleteInventoryPhasePending();
									}
								} catch (final DeleteInventoryPhasePending pending) {
									throw pending;
								} catch (final InterruptedException interrupted) {
									Thread.currentThread().interrupt();
									throw interrupted;
								} catch (final Exception failure) {
									terminalSliceFailures.putIfAbsent(slice, failure);
								}
							},
							durationStopPhaseDeadlineNanos(),
							threadNamePrefix);
			throwDeleteInventoryPhaseFailure(phase, category, completionResult, true);
			if (completionResult.completedAll()) {
				throwDeleteInventoryTerminalFailures(phase, category, terminalSliceFailures);
				return;
			}
			try {
				TimeUnit.NANOSECONDS.sleep(DURATION_DRAIN_POLL_NANOS);
			} catch (final InterruptedException interrupted) {
				Thread.currentThread().interrupt();
				throw terminalFailure(
								category,
								"Standalone DELETE " + phase + " was interrupted",
								interrupted);
			}
		}
	}

	private void throwDeleteInventoryTerminalFailures(
					final String phase,
					final IntegrityTerminalException.Category category,
					final Map<LoadStep, Throwable> terminalSliceFailures) {
		final List<Throwable> failures;
		synchronized (terminalSliceFailures) {
			failures = List.copyOf(terminalSliceFailures.values());
		}
		IntegrityTerminalException phaseFailure = null;
		for (final Throwable failure : failures) {
			phaseFailure = appendTerminalFailure(
							phaseFailure,
							category,
							"Standalone DELETE " + phase + " failed across distributed slices",
							failure);
		}
		if (phaseFailure != null) {
			throw phaseFailure;
		}
	}

	private void throwDeleteInventoryPhaseFailure(
					final String phase,
					final IntegrityTerminalException.Category category,
					final DurationPhaseAttempt result,
					final boolean allowPending) {
		IntegrityTerminalException phaseFailure = null;
		for (final Throwable failure : result.failures()) {
			if (allowPending && failure instanceof DeleteInventoryPhasePending) {
				continue;
			}
			phaseFailure = appendTerminalFailure(
							phaseFailure,
							category,
							"Standalone DELETE " + phase + " failed across distributed slices",
							failure);
		}
		if (!result.completedAll() && phaseFailure == null
						&& result.failures().stream().noneMatch(failure -> failure instanceof DeleteInventoryPhasePending)) {
			phaseFailure = terminalFailure(
							category,
							"Standalone DELETE " + phase + " did not complete across distributed slices",
							new TimeoutException("DELETE verification control-plane call exceeded its deadline"));
		}
		if (phaseFailure != null) {
			throw phaseFailure;
		}
	}

	private static final class DeleteInventoryPhasePending extends Exception {
		private static final long serialVersionUID = 1L;

		private DeleteInventoryPhasePending() {
			super("DELETE inventory phase remains active", null, false, false);
		}
	}

	static void requireMatchingRunId(final LoadStep stepSlice, final long expectedRunId) {
		if (expectedRunId <= 0L) {
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.CONFIGURATION,
							"metadata-mode step requires a positive run.id");
		}
		final long actualRunId;
		try {
			actualRunId = stepSlice.runId();
		} catch (final RemoteException e) {
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.CONFIGURATION,
							"failed to validate metadata-mode worker run.id",
							e);
		}
		if (actualRunId != expectedRunId) {
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.CONFIGURATION,
							"metadata-mode worker run.id mismatch: expected "
											+ expectedRunId + ", actual " + actualRunId);
		}
	}

	/**
	 * @param controlsOperations whether the step's operations take their count limit from this
	 *                           configuration ({@link #controlsOperations}); a count the step never
	 *                           uses is sliced as it always was, and never refused
	 */
	List<Config> sliceConfig(
					final Config config,
					final int sliceCount,
					final List<String> nodeAddrs,
					final boolean balanceCountLimit,
					final boolean controlsOperations) {
		final var configSlices = (List<Config>) new ArrayList<Config>(sliceCount);
		for (var i = 0; i < sliceCount; i++) {
			final var configSlice = ConfigSliceUtil.initSlice(config);
			if (i == 0) {
				// local step slice: disable the average metrics output
				configSlice.val("output-metrics-average-period", "0s");
			}
			configSlices.add(configSlice);
		}
		ConfigSliceUtil.sliceNodeInterfaces(config, configSlices, nodeAddrs);
		if (sliceCount > 1) { // distributed mode
			//
			final var countLimit = config.longVal("load-op-limit-count");
			if (countLimit > 0 && !controlsOperations) {
				if (balanceCountLimit) {
					ConfigSliceUtil.sliceLongValueBalanced(
									countLimit, configSlices, "load-op-limit-count");
				} else {
					ConfigSliceUtil.sliceLongValue(
									countLimit, configSlices, "load-op-limit-count");
				}
			} else if (countLimit > 0) {
				// A slice reads a count of 0 as no limit.
				if (countLimit < sliceCount) {
					throw countBelowEngineCount(countLimit, sliceCount);
				}
				if (balanceCountLimit) {
					ConfigSliceUtil.sliceLongValueBalanced(
									countLimit, configSlices, "load-op-limit-count");
				} else {
					ConfigSliceUtil.sliceCountLimit(
									countLimit, configSlices, "load-op-limit-count");
				}
				for (var i = 0; i < sliceCount; i++) {
					if (configSlices.get(i).longVal("load-op-limit-count") <= 0) {
						throw new IllegalStateException(
										loadStepId() + ": the count limit (" + countLimit + ") left load step slice #" + i + " of "
														+ sliceCount + " without a count");
					}
				}
			}
			//
			final var countFailLimit = config.longVal("load-op-limit-fail-count");
			if (!standaloneDeleteEnabled() && countFailLimit > 0) {
				ConfigSliceUtil.sliceLongValue(countFailLimit, configSlices, "load-op-limit-fail-count");
				configSlices
								.stream()
								.mapToLong(configSlice -> configSlice.longVal("load-op-limit-fail-count"))
								.filter(failCountLimitSlice -> failCountLimitSlice == 0)
								.findAny()
								.ifPresent(
												failCountLimitSlice -> Loggers.MSG.error(
																"{}: the failures count limit ({}) is too small to be sliced among the {} nodes, the load " +
																				"step may not work correctly",
																loadStepId(), countFailLimit, sliceCount));
			}
			//
			final var rateLimit = config.doubleVal("load-op-limit-rate");
			if (rateLimit > 0) {
				ConfigSliceUtil.sliceDoubleValue(rateLimit, configSlices, "load-op-limit-rate");
			}
			//
			final long sizeLimit;
			final var sizeLimitRaw = config.val("load-step-limit-size");
			if (sizeLimitRaw instanceof String) {
				sizeLimit = BinarySizeFormat.parseFixedSize((String) sizeLimitRaw);
			} else {
				sizeLimit = TypeUtil.typeConvert(sizeLimitRaw, long.class);
			}
			if (sizeLimit > 0) {
				ConfigSliceUtil.sliceLongValue(sizeLimit, configSlices, "load-step-limit-size");
			}
			//
			try {
				final var storageNetNodeConfig = config.configVal("storage-net-node");
				final var sliceStorageNodesFlag = storageNetNodeConfig.boolVal("slice");
				if (sliceStorageNodesFlag) {
					final var storageNodeAddrs = storageNetNodeConfig.<String> listVal("addrs");
					ConfigSliceUtil.sliceStorageNodeAddrs(configSlices, storageNodeAddrs);
				}
			} catch (final NoSuchElementException e) {
				Loggers.MSG.debug(
								"{}: storage-net-node configuration missing; skipping endpoint slicing",
								loadStepId());
			} catch (final InvalidValueTypeException e) {
				if (null != e.actualType()) {
					LogUtil.exception(Level.ERROR, e, "Failed to assign the storage endpoints to the nodes");
				}
			}
			//
			ConfigSliceUtil.sliceItemNaming(configSlices);
		}
		return configSlices;
	}

	private int sliceCount() {
		return stepSlices.size();
	}

	/**
	 * The configuration the operations of metrics row {@code originIndex} run on, which gives the row
	 * its count limit and bucket source. By default the base with context {@code originIndex} over
	 * it, or the base when there is no such context. A step type whose rows are not its contexts
	 * overrides this. A {@code null} result, like a thrown exception, stands for the base.
	 */
	protected Config effectiveConfig(final int originIndex) {
		if (ctxConfigs == null || originIndex < 0 || originIndex >= ctxConfigs.size()) {
			return config;
		}
		final var merged = TreeUtil.reduceForest(
						java.util.Arrays.asList(
										com.github.akurilov.confuse.Config.deepToMap(this.config),
										com.github.akurilov.confuse.Config.deepToMap(ctxConfigs.get(originIndex))));
		return new BasicConfig(this.config.pathSep(), this.config.schema(), merged);
	}

	@Override
	protected final void initMetrics(
					final int originIndex, final OpType opType, final int concurrencyLimit, final Config metricsConfig,
					final SizeInBytes itemDataSize, final boolean outputColorFlag) {

		// The configuration this row's operations run on, as the step type says.
		Config effectiveConfig;
		try {
			effectiveConfig = effectiveConfig(originIndex);
		} catch (final Exception e) {
			// Fall back to base config if resolving it fails for any reason
			Loggers.MSG.debug(
							"{}: failed to resolve the configuration of metrics row {}; using base config",
							loadStepId(),
							originIndex,
							e);
			effectiveConfig = this.config;
		}
		if (effectiveConfig == null) {
			effectiveConfig = this.config;
		}

		// Extract limits for completion percentage
		long opCountLimit = 0L;
		long timeLimitSec = 0L;
		try {
			opCountLimit = effectiveConfig.longVal("load-op-limit-count");
		} catch (final Exception e) {
			Loggers.MSG.debug(
							"{}: load-op-limit-count unavailable or invalid; proceeding without override",
							loadStepId(),
							e);
		}
		try {
			// The step stops on the base's time limit, whatever a context says.
			final Object raw = this.config.val("load-step-limit-time");
			if (raw instanceof String) {
				timeLimitSec = com.dell.spt.base.config.TimeUtil.getTimeInSeconds((String) raw);
			} else if (raw != null) {
				timeLimitSec = com.github.akurilov.commons.reflection.TypeUtil.typeConvert(raw, long.class);
			}
		} catch (final Exception e) {
			Loggers.MSG.debug(
							"{}: load-step-limit-time unavailable or invalid; proceeding without override",
							loadStepId(),
							e);
		}
		final var concurrencyThreshold = (int) (concurrencyLimit * metricsConfig.doubleVal("threshold"));
		final var metricsAvgPersistFlag = metricsConfig.boolVal("average-persist");
		final var metricsSumPersistFlag = metricsConfig.boolVal("summary-persist");
		final var metricsTimingPersistFlag = metricsConfig.boolVal("timing-persist");
		// it's not known yet how many nodes are involved, so passing the function "this::sliceCount"
		// reference for
		// further usage
		final var metricsCtx = (DistributedMetricsContext) DistributedMetricsContextImpl
						.builder()
						.loadStepId(loadStepId())
						.opType(opType)
						.nodeCountSupplier(this::sliceCount)
						.concurrencyLimit(concurrencyLimit)
						.concurrencyThreshold(concurrencyThreshold)
						.itemDataSize(itemDataSize)
						.outputPeriodSec(avgPeriod(metricsConfig))
						.stdOutColorFlag(outputColorFlag)
						.avgPersistFlag(metricsAvgPersistFlag)
						.sumPersistFlag(metricsSumPersistFlag)
						.timingPersistFlag(metricsTimingPersistFlag)
						.snapshotsSupplier(() -> metricsSnapshotsByIndex(originIndex))
						.quantileValues(quantiles(metricsConfig))
						.bucketMetricsSource(bucketMetricsSource(effectiveConfig, metricsConfig, originIndex))
						.nodeAddrs(remoteNodeAddrs(config))
						.contributorIds(distributedContributorIds(config, remoteNodeAddrs(config)))
						.deleteDetailsExpected(standaloneDeleteEnabled())
						.comment(config.stringVal("run-comment"))
						.runId(runId())
						.opCountLimit(opCountLimit)
						.timeLimitSec(timeLimitSec)
						.build();
		metricsContexts.add(metricsCtx);
	}

	private BucketMetricsSource bucketMetricsSource(
					final Config effectiveConfig, final Config metricsConfig, final int contextIndex) {
		// As a worker decides in initBucketMetrics, so that a file exists exactly when workers record.
		if (TargetBucketSet.count(effectiveConfig) <= 1
						|| !effectiveConfig.boolVal("output-metrics-bucket-enabled")
						|| !effectiveConfig.boolVal("output-metrics-summary-persist")) {
			return null;
		}
		final SteadySettings steadySettings = SteadySettings.fromMetricsConfig(metricsConfig);
		return new BucketMetricsSource() {
			@Override
			public List<BucketRows.Contribution> contributions() {
				return fetchBucketMetrics();
			}

			@Override
			public SteadySettings steadySettings() {
				return steadySettings;
			}

			@Override
			public int contextIndex() {
				return contextIndex;
			}

			@Override
			public void release() {
				releaseBucketMetrics();
			}
		};
	}

	/**
	 * Fetches every worker's bucket records in parallel, once, separately from the periodic
	 * snapshots, with a bounded deadline. A worker that does not answer leaves a labeled subtotal.
	 */
	private synchronized List<BucketRows.Contribution> fetchBucketMetrics() {
		if (bucketContributions != null) {
			return bucketContributions;
		}
		final List<LoadStep> slices = new ArrayList<>(stepSlices);
		final List<String> contributorIds = distributedContributorIds(config, remoteNodeAddrs(config));
		final List<BucketRows.Contribution> contributions = new ArrayList<>(slices.size());
		if (slices.isEmpty()) {
			bucketContributions = contributions;
			return contributions;
		}
		final ExecutorService pool = Executors.newFixedThreadPool(slices.size(), runnable -> {
			final Thread thread = new Thread(runnable, "bucket-metrics-fetch-" + loadStepId());
			thread.setDaemon(true);
			return thread;
		});
		try {
			final List<Callable<BucketMetricsReply>> fetches = new ArrayList<>(slices.size());
			for (final LoadStep slice : slices) {
				fetches.add(() -> {
					if (slice == null) {
						throw new IllegalStateException("this node's step slice never started");
					}
					if (isLostSlice(slice)) {
						throw new IllegalStateException("the entry lost this node's step slice");
					}
					return slice.bucketMetrics(BucketMetricsRange.ALL);
				});
			}
			final List<Future<BucketMetricsReply>> results = pool.invokeAll(fetches, BUCKET_METRICS_FETCH_SECONDS, TimeUnit.SECONDS);
			for (int i = 0; i < results.size(); i++) {
				final String node = i < contributorIds.size() && contributorIds.size() == slices.size()
								? contributorIds.get(i)
								: "slice-" + i;
				contributions.add(bucketContribution(node, results.get(i)));
			}
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			contributions.clear();
			for (int i = 0; i < slices.size(); i++) {
				contributions.add(new BucketRows.Contribution("slice-" + i, null, "interrupted"));
			}
		} finally {
			pool.shutdownNow();
		}
		bucketContributions = List.copyOf(contributions);
		return bucketContributions;
	}

	/** Lets go of every worker's payload. A step that runs again fetches afresh. */
	private synchronized void releaseBucketMetrics() {
		bucketContributions = null;
	}

	private static BucketRows.Contribution bucketContribution(
					final String node, final Future<BucketMetricsReply> result) throws InterruptedException {
		if (result.isCancelled()) {
			return new BucketRows.Contribution(node, null, "timeout");
		}
		try {
			final BucketMetricsReply reply = result.get();
			return new BucketRows.Contribution(node, reply, reply == null ? "unsupported call" : null);
		} catch (final ExecutionException e) {
			return new BucketRows.Contribution(node, null, "no reply: " + e.getCause());
		}
	}

	private List<Double> quantiles(final Config metricsConfig) {
		List<Double> quantileValues = metricsConfig
						.listVal("quantiles")
						.stream()
						.map(v -> {
							Double val = Double.valueOf(v.toString());
							if ((val < 0) || (val >= 1)) {
								throw new IllegalArgumentException("Quantile values must be in range [0,1), but found" + val);
							}
							return val;
						})
						.collect(Collectors.toList());
		if (quantileValues.size() == 0) {
			throw new IllegalArgumentException("Quantile values list cannot be empty");
		}
		return quantileValues;
	}

	private List<AllMetricsSnapshot> metricsSnapshotsByIndex(final int originIndex) {
		return metricsAggregator == null ? Collections.emptyList() : metricsAggregator.metricsSnapshotsByIndex(originIndex);
	}

	static Map<OpType, Long> terminalFailureCounts(
					final List<? extends MetricsContext<? extends AllMetricsSnapshot>> contexts) {
		final Map<OpType, Long> counts = new HashMap<>();
		for (final var context : contexts) {
			final AllMetricsSnapshot snapshot = context.lastSnapshot();
			if (snapshot == null || snapshot.failsSnapshot() == null) {
				throw new IllegalStateException(
								"terminal failure metrics are unavailable for " + context.opType());
			}
			final long failureCount = snapshot.failsSnapshot().count();
			if (failureCount < 0) {
				throw new IllegalStateException(
								"terminal failure metrics are negative for " + context.opType());
			}
			counts.merge(context.opType(), failureCount, Math::addExact);
		}
		return counts;
	}

	@Override
	protected final void doShutdown() {
		if (failureBudgetFailure != null) {
			if (!durationAdmissionBarrierSatisfied || durationStopFailure != null) {
				coordinateFailureBudgetStop();
			}
			return;
		}
		if (standaloneDeleteEnabled()) {
			coordinateDurationStop();
			return;
		}
		// A lost slice has had its one close.
		stepSlices.stream().filter(s -> s != null && !isLostSlice(s)).parallel().forEach(stepSlice -> {
			try (final var logCtx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
				stepSlice.shutdown();
			} catch (final RemoteException | RuntimeException e) {
				// A local slice rethrows its context's shutdown failure, as a drain or driver shutdown that throws:
				// the other slices still shut down, and the step's stop and close still run.
				if (!isLostSlice(stepSlice)) {
					LogUtil.exception(Level.WARN, e, "{}: failed to shutdown the step service {}", loadStepId(), stepSlice);
				}
			}
		});
	}

	private void coordinateDurationStop() {
		synchronized (standaloneDeleteStopCoordination) {
			coordinateStandaloneDeleteStopOnce(
							standaloneDeleteDurationMode() && failureBudgetFailure == null);
		}
	}

	private void coordinateStandaloneDeleteStopOnce(final boolean requireDurationVerdict) {
		if (standaloneDeleteStopCoordinated) {
			return;
		}
		standaloneDeleteStopInProgress = true;
		try {
			coordinateStandaloneDeleteStop(requireDurationVerdict);
			standaloneDeleteStopCoordinated = durationAdmissionBarrierSatisfied
							&& durationStopFailure == null;
		} finally {
			standaloneDeleteStopInProgress = false;
		}
	}

	private void coordinateStandaloneDeleteStop(final boolean requireDurationVerdict) {
		final List<LoadStep> activeSlices = stepSlices.stream()
						.filter(slice -> slice != null)
						.collect(Collectors.toList());
		if (activeSlices.isEmpty() || !hasStartedStandaloneDeleteWork()) {
			durationAdmissionBarrierSatisfied = true;
			return;
		}
		durationAdmissionBarrierSatisfied = false;
		setDurationDrainDeadlineIfAbsent(durationDrainDeadlineNanos(System.nanoTime()));
		boolean interrupted = false;
		final DurationPhaseAttempt admissionResult = invokeRetainedDurationPhase(
						"distributed-admission",
						activeSlices,
						slice -> {
							slice.enforceDispatchedOperationsDeadlineForStepStop(
											DurationTime.remainingNanos(
															durationDrainDeadlineNanos, System.nanoTime()));
							slice.closeOperationAdmissionForStepStop();
						},
						durationStopPhaseDeadlineNanos(),
						"spt-delete-admission-close-");
		interrupted |= recordDurationPhaseResult("close operation admission", admissionResult);
		if (!admissionResult.succeeded()) {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
			return;
		}
		durationAdmissionBarrierSatisfied = true;
		if (requireDurationVerdict) {
			final DurationPhaseAttempt verdictResult = invokeRetainedDurationPhase(
							"distributed-duration-verdict",
							activeSlices,
							this::probeDurationAwaitStatus,
							durationStopPhaseDeadlineNanos(),
							"spt-delete-duration-verdict-");
			interrupted |= verdictResult.interrupted();
			if (!verdictResult.succeeded()) {
				if (verdictResult.failures().isEmpty()) {
					recordDurationValidityFailure(
									"Standalone DELETE could not confirm duration validity for every distributed input slice",
									null);
				} else {
					for (final Throwable failure : verdictResult.failures()) {
						recordDurationValidityFailure(
										"Standalone DELETE could not confirm duration validity for every distributed input slice",
										failure);
					}
				}
			}
			for (final LoadStep slice : activeSlices) {
				final DurationAwaitStatus status = durationAwaitStatuses.get(slice);
				if (status == DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE) {
					recordDurationValidityFailure(
									"Standalone DELETE inventory slice exhausted before the requested duration; "
													+ "increase --seed-objects for a seeded run or provide/select a larger frozen inventory",
									null);
				} else if (status != DurationAwaitStatus.REACHED_DEADLINE) {
					recordDurationValidityFailure(
									"Standalone DELETE could not confirm that every distributed input slice reached its duration deadline",
									null);
				}
			}
		}
		final DurationPhaseAttempt recoveryResult = invokeRetainedDurationPhase(
						"distributed-recovery",
						activeSlices,
						LoadStep::recoverQueuedOperationsForStepStop,
						durationStopPhaseDeadlineNanos(),
						"spt-delete-recovery-");
		interrupted |= recordDurationPhaseResult("recover queued operations", recoveryResult);
		if (!recoveryResult.succeeded()) {
			restoreDurationStopInterrupt(interrupted);
			return;
		}
		final long drainPhaseDeadlineNanos = durationStopPhaseDeadlineNanos();
		final DurationPhaseAttempt drainResult = invokeRetainedDurationPhase(
						"distributed-drain",
						activeSlices,
						slice -> drainDispatchedOperations(
										slice,
										DurationTime.remainingNanos(
														durationDrainDeadlineNanos, System.nanoTime()),
										drainPhaseDeadlineNanos),
						drainPhaseDeadlineNanos,
						"spt-delete-drain-");
		interrupted |= recordDurationPhaseResult("drain dispatched operations", drainResult);
		if (!drainResult.succeeded()) {
			restoreDurationStopInterrupt(interrupted);
			return;
		}
		if (standaloneDeletePostVerificationEnabled() && !deletePostVerificationGloballySkipped) {
			try {
				coordinateDeleteInventoryPhase(
								"post-verification",
								LoadStep::verifyDeleteInventoryForStepStop,
								LoadStep::isDeleteInventoryVerificationCompleteForStepStop,
								IntegrityTerminalException.Category.EXECUTION);
			} catch (final RuntimeException failure) {
				recordDurationStopFailure("verify DELETE inventory", failure);
				restoreDurationStopInterrupt(interrupted);
				return;
			}
		}
		final DurationPhaseAttempt validationResult = invokeRetainedDurationPhase(
						"distributed-terminal-validation",
						activeSlices,
						LoadStep::validateTerminalStateForStepStop,
						durationStopPhaseDeadlineNanos(),
						"spt-delete-terminal-validation-");
		interrupted |= validationResult.interrupted();
		if (!validationResult.succeeded()) {
			if (validationResult.failures().isEmpty()) {
				recordDurationValidityFailure(
								"Standalone DELETE terminal accounting could not be validated after the drain",
								null);
			} else {
				for (final Throwable failure : validationResult.failures()) {
					recordDurationValidityFailure(
									"Standalone DELETE terminal accounting failed after the drain",
									failure);
				}
			}
		}
		final DurationPhaseAttempt shutdownResult = invokeRetainedDurationPhase(
						"distributed-shutdown",
						activeSlices,
						LoadStep::shutdown,
						durationStopPhaseDeadlineNanos(),
						"spt-delete-shutdown-");
		interrupted |= recordDurationPhaseResult("shutdown", shutdownResult);
		restoreDurationStopInterrupt(interrupted);
	}

	private static void drainDispatchedOperations(
					final LoadStep slice,
					final long remainingNanos,
					final long pollDeadlineNanos) throws Exception {
		slice.startDispatchedOperationsDrainForStepStop(remainingNanos);
		while (!slice.isDispatchedOperationsDrainCompleteForStepStop()) {
			final long pollRemainingNanos = DurationTime.remainingNanos(
							pollDeadlineNanos, System.nanoTime());
			if (pollRemainingNanos == 0) {
				throw new java.util.concurrent.TimeoutException(
								"distributed dispatched-operation drain did not complete before its phase deadline");
			}
			TimeUnit.NANOSECONDS.sleep(Math.min(DURATION_DRAIN_POLL_NANOS, pollRemainingNanos));
		}
	}

	private static void restoreDurationStopInterrupt(final boolean interrupted) {
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private long durationDrainDeadlineNanos(final long scheduledDeadlineNanos) {
		final long waitNanos = TimeUnit.SECONDS.toNanos(
						Math.max(0, config.intVal("load-op-wait-limit")));
		return durationDeadlineNanos(scheduledDeadlineNanos, waitNanos);
	}

	private boolean recordDurationPhaseResult(
					final String phase, final DurationPhaseAttempt result) {
		for (final Throwable failure : result.failures()) {
			recordDurationStopFailure(phase, failure);
		}
		return result.interrupted();
	}

	private DurationPhaseAttempt invokeStepSlicePhase(
					final List<LoadStep> activeSlices,
					final StepSlicePhase action,
					final long deadlineNanos,
					final String threadNamePrefix) {
		if (activeSlices.isEmpty()) {
			return new DurationPhaseAttempt(List.of(), false, true);
		}
		return invokeRetainedDurationPhase(
						"distributed-" + threadNamePrefix,
						activeSlices,
						action::execute,
						deadlineNanos,
						threadNamePrefix);
	}

	private synchronized void recordDurationStopFailure(final String phase, final Throwable cause) {
		durationStopFailure = appendTerminalFailure(
						durationStopFailure,
						IntegrityTerminalException.Category.EXECUTION,
						"Standalone DELETE failed to " + phase + " across distributed slices",
						cause);
	}

	private synchronized void recordDurationValidityFailure(final String message, final Throwable cause) {
		durationValidityFailure = appendTerminalFailure(
						durationValidityFailure,
						IntegrityTerminalException.Category.EXECUTION,
						message,
						cause);
	}

	private synchronized IntegrityTerminalException recordDurationValidityFailure(
					final IntegrityTerminalException failure) {
		durationValidityFailure = appendExistingFailure(durationValidityFailure, failure);
		return failure;
	}

	@FunctionalInterface
	private interface StepSlicePhase {
		void execute(LoadStep slice) throws Exception;
	}

	@FunctionalInterface
	private interface DeleteInventoryPhaseCompletion {
		boolean complete(LoadStep slice) throws Exception;
	}

	@Override
	public final boolean await(final long timeout, final TimeUnit timeUnit)
					throws IllegalStateException, InterruptedException {
		final var stepSliceCount = stepSlices.size();
		try (final var logCtx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			if (closedAtJvmExit(stepSliceCount)) {
				throw stoppedAtJvmExit();
			}
			if (0 == stepSliceCount) {
				throw new IllegalStateException("No step slices are available");
			}
			Loggers.MSG.debug(
							"{}: await for {} step slices for at most {} {}...", loadStepId(), stepSliceCount,
							timeout, timeUnit.name().toLowerCase(Locale.ROOT));
			if (standaloneDeleteEnabled()) {
				if (!standaloneDeleteDurationMode()
								&& failureBudgetStartedNanos == Long.MIN_VALUE) {
					failureBudgetStartedNanos = System.nanoTime();
				}
				startFailureBudgetMonitor();
			}
			if (standaloneDeleteDurationMode()) {
				try {
					return awaitDurationSlicesAndStop(timeout, timeUnit, stepSliceCount);
				} finally {
					stopFailureBudgetMonitor();
				}
			}
			final boolean done;
			// A standalone DELETE coordinates its slices' stops in its own shutdown.
			if (!standaloneDeleteEnabled()) {
				armLostSliceStop();
			}
			try {
				done = awaitSlices(timeout, timeUnit);
				if (closedAtJvmExit(stepSlices.size())) {
					throw stoppedAtJvmExit();
				}
			} catch (final Throwable failure) {
				// As before: a failure of this stop replaces the await's own.
				stopAfterAwait(stepSliceCount);
				throw failure;
			}
			// A slice lost mid-step stops the whole step at once: the failure is recorded, the stop runs,
			// and then the failure ends the await, also when the stop itself fails.
			if (!sliceLost) {
				stopAfterAwait(stepSliceCount);
				return done;
			}
			try {
				stopAfterAwait(stepSliceCount);
			} catch (final RuntimeException | Error stopFailure) {
				throwUncheckedIfInterrupted(stopFailure);
				final var lost = takeLostSliceFailure();
				if (lost == null) {
					throw stopFailure;
				}
				lost.addSuppressed(stopFailure);
				throw lost;
			}
			final var lost = takeLostSliceFailure();
			if (lost != null) {
				throw lost;
			}
			return done;
		}
	}

	/**
	 * Polls each slice until it is done, the time limit passes or a slice is lost. A lost slice, at
	 * its own await or found by the {@linkplain #recordLostSliceAtMetricsFetch metrics fetch}, ends
	 * every poll loop at its next poll, and a slice still queued for a poller is not polled.
	 */
	private boolean awaitSlices(final long timeout, final TimeUnit timeUnit) {
		// One start time for all slices: a slice queued for a free poller must not restart the limit.
		// Compared by subtraction, since toMillis saturates for a step without a time limit.
		final var invokeTimeMillis = System.currentTimeMillis();
		final var timeOutMillis = timeUnit.toMillis(timeout);
		return stepSlices.stream().filter(s -> s != null).parallel().map(stepSlice -> {
			try {
				var awaitResult = false;
				// A slice still queued at the limit gets one poll: only a poll reports its terminal failure.
				var polled = timeOutMillis <= 0;
				while (!sliceLost && (!polled || timeOutMillis > System.currentTimeMillis() - invokeTimeMillis)) {
					if (Thread.currentThread().isInterrupted()) {
						throwUnchecked(new InterruptedException());
					}
					polled = true;
					awaitResult = stepSlice.await(1, TimeUnit.MILLISECONDS);
					if (awaitResult) { // awaitResult = (0 == countDown)
						break;
					}
				}
				return awaitResult;
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			} catch (final RemoteException e) {
				recordLostSliceAtAwait(stepSlice, e);
				return false;
			}
			return false;
		}).reduce((flag1, flag2) -> flag1 && flag2).orElse(false);
	}

	private void stopAfterAwait(final int stepSliceCount) {
		stopFailureBudgetMonitor();
		Loggers.MSG.info("{}: await for {} step slices done", loadStepId(), stepSliceCount);
		// After pre-validation, post-verification is this client's to start, in its shutdown. A
		// slice stopped before that has already closed its verification evidence.
		if (standaloneDeletePreValidationEnabled() && standaloneDeletePostVerificationEnabled()) {
			shutdown();
		}
		stopBeforeClose(this::doStop);
	}

	/**
	 * Whether the exit hook has stopped and closed this step, or is closing it: the JVM exits, and
	 * the step is closed, is closing its slices, or has no slices left after its start. A step still in its initial state is not; one the exit closed before it started is treated as stopped.
	 */
	private boolean closedAtJvmExit(final int stepSliceCount) {
		return JvmExit.begun()
						&& (slicesClosing || stopCloseDone || isClosed() || (0 == stepSliceCount && !isInitial()));
	}

	private InterruptedException stoppedAtJvmExit() {
		return new InterruptedException(loadStepId() + ": stopped, the engine is exiting");
	}

	private synchronized void startFailureBudgetMonitor() {
		if (failureBudgetController == null || failureBudgetMonitor != null) {
			return;
		}
		final Thread monitor = Thread.ofPlatform()
						.daemon(true)
						.name("spt-delete-failure-budget-" + loadStepId())
						.unstarted(this::monitorFailureBudget);
		failureBudgetMonitor = monitor;
		monitor.start();
	}

	private void prepareCountFailureBudgetMonitoring() {
		try {
			final ObjectFailureBudgetDecision decision = evaluateFailureBudgetWithRequiredCounters();
			if (decision.stopScheduling()) {
				recordFailureBudgetFailure(failureBudgetException(decision), decision.reason());
			}
		} catch (final Throwable failure) {
			throwUncheckedIfInterrupted(failure);
			publishFailureBudgetOutcome(ObjectFailureBudgetOutcome.FAILED);
			recordFailureBudgetFailure(
							terminalFailure(
											IntegrityTerminalException.Category.EXECUTION,
											"Standalone DELETE failed-object counter monitoring is not ready before count admission",
											failure),
							"failed-object counter monitoring was not ready before count admission");
		}
		if (failureBudgetFailure != null) {
			coordinateFailureBudgetStop();
			throw failureBudgetFailure;
		}
		startFailureBudgetMonitor();
	}

	private void releaseCountFailureBudgetAdmission() {
		final List<LoadStep> activeSlices = stepSlices.stream()
						.filter(slice -> slice != null)
						.collect(Collectors.toList());
		final DurationPhaseAttempt result = invokeRetainedDurationPhase(
						"distributed-failure-budget-admission",
						activeSlices,
						LoadStep::releaseObjectFailureBudgetAdmission,
						durationStopPhaseDeadlineNanos(),
						"spt-delete-failure-budget-admission-");
		if (result.succeeded()) {
			return;
		}
		IntegrityTerminalException failure = null;
		for (final Throwable cause : result.failures()) {
			failure = appendTerminalFailure(
							failure,
							IntegrityTerminalException.Category.EXECUTION,
							"Standalone DELETE failed to release the object failure-budget admission barrier",
							cause);
		}
		if (!result.completedAll() && failure == null) {
			failure = terminalFailure(
							IntegrityTerminalException.Category.EXECUTION,
							"Standalone DELETE timed out while releasing the object failure-budget admission barrier",
							new java.util.concurrent.TimeoutException(
											"distributed failure-budget admission phase exceeded its deadline"));
		}
		recordFailureBudgetFailure(
						failure, "object failure-budget admission barrier failed");
		coordinateFailureBudgetStop();
		if (result.interrupted()) {
			Thread.currentThread().interrupt();
		}
		throw failure;
	}

	private void monitorFailureBudget() {
		try {
			while (!Thread.currentThread().isInterrupted() && failureBudgetFailure == null) {
				if (failureBudgetStartedNanos != Long.MIN_VALUE) {
					final ObjectFailureBudgetDecision decision = evaluateFailureBudget(false);
					if (decision.stopScheduling()) {
						recordFailureBudgetFailure(failureBudgetException(decision), decision.reason());
						coordinateFailureBudgetStop();
						return;
					}
				}
				Thread.sleep(FAILURE_BUDGET_POLL_MILLIS);
			}
		} catch (final InterruptedException interrupted) {
			Thread.currentThread().interrupt();
		} catch (final Throwable failure) {
			throwUncheckedIfInterrupted(failure);
			recordFailureBudgetFailure(
							terminalFailure(
											IntegrityTerminalException.Category.EXECUTION,
											"Standalone DELETE failed to evaluate the failed-object budget",
											failure),
							"failed-object budget evaluation failed");
			coordinateFailureBudgetStop();
		}
	}

	private synchronized void recordFailureBudgetFailure(
					final IntegrityTerminalException failure, final String reason) {
		if (failureBudgetFailure == null) {
			failureBudgetFailureReason = reason;
			failureBudgetFailure = failure;
		} else if (failure != null && failure != failureBudgetFailure) {
			failureBudgetFailure.addSuppressed(failure);
		}
	}

	private synchronized void attachToFailureBudgetFailure(
					final IntegrityTerminalException budgetFailure, final Throwable failure) {
		if (failure == null || failure == budgetFailure) {
			return;
		}
		for (final Throwable attached : budgetFailure.getSuppressed()) {
			if (attached == failure) {
				return;
			}
		}
		budgetFailure.addSuppressed(failure);
	}

	private void coordinateFailureBudgetStop() {
		if (standaloneDeleteStopCoordinated) {
			return;
		}
		final long breachObservedNanos = System.nanoTime();
		final long breachDrainDeadlineNanos = durationDrainDeadlineNanos(breachObservedNanos);
		synchronized (this) {
			if (!durationDrainDeadlineSet) {
				durationDrainDeadlineNanos = breachDrainDeadlineNanos;
				durationDrainDeadlineSet = true;
			} else {
				durationDrainDeadlineNanos = DurationTime.earlierDeadline(
								durationDrainDeadlineNanos,
								breachDrainDeadlineNanos,
								breachObservedNanos);
			}
		}
		if (standaloneDeleteStopInProgress) {
			propagateFailureBudgetDrainDeadline();
		}
		synchronized (standaloneDeleteStopCoordination) {
			coordinateStandaloneDeleteStopOnce(false);
		}
	}

	private void propagateFailureBudgetDrainDeadline() {
		final List<LoadStep> activeSlices = stepSlices.stream()
						.filter(slice -> slice != null)
						.collect(Collectors.toList());
		final DurationPhaseAttempt result = invokeRetainedDurationPhase(
						"distributed-budget-deadline",
						activeSlices,
						slice -> slice.enforceDispatchedOperationsDeadlineForStepStop(
										DurationTime.remainingNanos(
														durationDrainDeadlineNanos, System.nanoTime())),
						durationStopPhaseDeadlineNanos(),
						"spt-delete-budget-deadline-");
		if (recordDurationPhaseResult("propagate failed-object drain deadline", result)) {
			Thread.currentThread().interrupt();
		}
	}

	private void stopFailureBudgetMonitor() {
		final Thread monitor;
		synchronized (this) {
			monitor = failureBudgetMonitor;
			if (monitor == null || monitor == Thread.currentThread()) {
				return;
			}
			if (failureBudgetFailure == null) {
				monitor.interrupt();
			}
		}
		try {
			monitor.join(failureBudgetMonitorJoinMillis());
		} catch (final InterruptedException interrupted) {
			Thread.currentThread().interrupt();
		}
		if (monitor.isAlive()) {
			monitor.interrupt();
			try {
				monitor.join(FAILURE_BUDGET_MONITOR_JOIN_BUFFER_MILLIS);
			} catch (final InterruptedException interrupted) {
				Thread.currentThread().interrupt();
			}
		}
		synchronized (this) {
			if (failureBudgetMonitor == monitor && !monitor.isAlive()) {
				failureBudgetMonitor = null;
			}
		}
		closeFailureBudgetSnapshotCollectorIfIdle();
	}

	private long failureBudgetMonitorJoinMillis() {
		final long phaseMillis = Math.max(
						1,
						TimeUnit.NANOSECONDS.toMillis(DurationTime.remainingNanos(
										durationStopPhaseDeadlineNanos(), System.nanoTime())));
		return Math.addExact(
						Math.multiplyExact(phaseMillis, FAILURE_BUDGET_STOP_PHASE_COUNT),
						FAILURE_BUDGET_MONITOR_JOIN_BUFFER_MILLIS);
	}

	private ObjectFailureBudgetDecision evaluateFailureBudget(final boolean completion)
					throws InterruptedException {
		final List<DeleteObjectLifecycleSnapshot> counters = collectFailureBudgetSnapshots(completion);
		return evaluateFailureBudget(counters, completion);
	}

	private ObjectFailureBudgetDecision evaluateFailureBudgetWithRequiredCounters()
					throws InterruptedException {
		final List<DeleteObjectLifecycleSnapshot> counters = collectFailureBudgetSnapshots(false);
		if (counters.isEmpty() || counters.stream().anyMatch(snapshot -> snapshot == null)) {
			throw new IllegalStateException("failed-object counters are unavailable from a participant");
		}
		return evaluateFailureBudget(counters, false);
	}

	private ObjectFailureBudgetDecision evaluateFailureBudget(
					final List<DeleteObjectLifecycleSnapshot> counters, final boolean completion) {
		final long startedNanos = failureBudgetStartedNanos;
		final Duration elapsed = startedNanos == Long.MIN_VALUE
						? Duration.ZERO
						: Duration.ofNanos(Math.max(0, System.nanoTime() - startedNanos));
		return failureBudgetController.evaluate(counters, elapsed, completion);
	}

	private List<DeleteObjectLifecycleSnapshot> collectFailureBudgetSnapshots(
					final boolean completion) throws InterruptedException {
		final List<LoadStep> activeSlices = stepSlices.stream()
						.filter(slice -> slice != null)
						.collect(Collectors.toList());
		final FailureBudgetSnapshotCollector collector;
		synchronized (this) {
			if (failureBudgetSnapshotCollector == null) {
				failureBudgetSnapshotCollector = new FailureBudgetSnapshotCollector(activeSlices);
			} else {
				failureBudgetSnapshotCollector.requireSameSlices(activeSlices);
			}
			collector = failureBudgetSnapshotCollector;
		}
		final long deadlineNanos = completion
						? durationStopPhaseDeadlineNanos()
						: durationDeadlineNanos(FAILURE_BUDGET_LIVE_SNAPSHOT_TIMEOUT_NANOS);
		return collector.collect(deadlineNanos, completion);
	}

	private void closeFailureBudgetSnapshotCollector() {
		final FailureBudgetSnapshotCollector collector;
		synchronized (this) {
			collector = failureBudgetSnapshotCollector;
			failureBudgetSnapshotCollector = null;
		}
		if (collector != null) {
			collector.close();
		}
	}

	private void closeFailureBudgetSnapshotCollectorIfIdle() {
		final FailureBudgetSnapshotCollector collector;
		synchronized (this) {
			collector = failureBudgetSnapshotCollector;
			if (collector == null || collector.hasActiveProbe()) {
				return;
			}
			failureBudgetSnapshotCollector = null;
		}
		collector.close();
	}

	/**
	 * Whether this client has standalone DELETE work to judge: a slice started, or a failure is held.
	 * A client that never started a slice, as an engine prototype built from a base configuration with
	 * {@code load-op-delete-standalone} or a step refused, stopped or failed before its first slice
	 * started, has none, and its stop and close give no failure-budget verdict. Slices held without a
	 * start, as tests add them, are judged.
	 */
	private boolean hasStartedStandaloneDeleteWork() {
		if (failureBudgetFailure != null || durationValidityFailure != null || startedSliceCount > 0) {
			return true;
		}
		return !startEntered && !startStopped && !stepSlices.isEmpty();
	}

	private synchronized IntegrityTerminalException finalizeFailureBudget() {
		if (failureBudgetController == null) {
			return null;
		}
		if (failureBudgetFinalized) {
			return failureBudgetTerminalFailure;
		}
		// After the finalized check, so a failure already given stays sticky at a later close.
		if (!hasStartedStandaloneDeleteWork()) {
			return null;
		}
		final ObjectFailureBudgetDecision terminalDecision;
		try {
			terminalDecision = evaluateFailureBudget(true);
		} catch (final Throwable failure) {
			throwUncheckedIfInterrupted(failure);
			publishFailureBudgetOutcome(ObjectFailureBudgetOutcome.FAILED);
			failureBudgetTerminalFailure = appendTerminalFailure(
							appendExistingFailure(failureBudgetFailure, durationValidityFailure),
							IntegrityTerminalException.Category.EXECUTION,
							"Standalone DELETE failed to capture terminal failed-object counters",
							failure);
			failureBudgetFinalized = true;
			return failureBudgetTerminalFailure;
		}
		publishFailureBudgetOutcome(
						durationValidityFailure != null || failureBudgetFailure != null
										? ObjectFailureBudgetOutcome.FAILED
										: terminalDecision.outcome());
		if (durationValidityFailure != null
						&& failureBudgetFailure == null
						&& terminalDecision.outcome() != ObjectFailureBudgetOutcome.FAILED) {
			failureBudgetFinalized = true;
			Loggers.MSG.error(
							"{}: Standalone DELETE overall run already failed duration validity; "
											+ "failure-budget status only: {}",
							loadStepId(),
							failureBudgetSummary(terminalDecision));
			return null;
		}
		final boolean priorFailure = failureBudgetFailure != null;
		final boolean stickyFailure = priorFailure
						&& terminalDecision.outcome() != ObjectFailureBudgetOutcome.FAILED;
		final String summary = stickyFailure
						? failureBudgetSummary(
										terminalDecision,
										ObjectFailureBudgetOutcome.FAILED,
										(failureBudgetFailure != null
														? (failureBudgetFailureReason == null
																		? "failed-object budget enforcement failed earlier"
																		: failureBudgetFailureReason)
														: "duration validity failed; operational failure-budget room cannot validate the run")
														+ "; the failure remains sticky; "
														+ "final counters were captured after the coordinated drain")
						: failureBudgetSummary(terminalDecision);
		if (priorFailure || terminalDecision.outcome() == ObjectFailureBudgetOutcome.FAILED) {
			failureBudgetTerminalFailure = terminalFailure(
							IntegrityTerminalException.Category.EXECUTION,
							"Standalone DELETE failed-object budget: " + summary,
							null);
			if (failureBudgetFailure != null && failureBudgetFailure != failureBudgetTerminalFailure) {
				failureBudgetTerminalFailure.addSuppressed(failureBudgetFailure);
			}
			if (durationValidityFailure != null
							&& durationValidityFailure != failureBudgetTerminalFailure
							&& durationValidityFailure != failureBudgetFailure) {
				failureBudgetTerminalFailure.addSuppressed(durationValidityFailure);
			}
			failureBudgetFinalized = true;
			Loggers.MSG.error("{}: Standalone DELETE failed: {}", loadStepId(), summary);
			return failureBudgetTerminalFailure;
		}
		failureBudgetFinalized = true;
		if (terminalDecision.outcome() == ObjectFailureBudgetOutcome.COMPLETED_WITHIN_BUDGET) {
			Loggers.MSG.warn("{}: Standalone DELETE completed within failure budget: {}", loadStepId(), summary);
		} else {
			Loggers.MSG.info("{}: Standalone DELETE completed cleanly: {}", loadStepId(), summary);
		}
		return null;
	}

	private void publishFailureBudgetOutcome(final ObjectFailureBudgetOutcome outcome) {
		final String published = switch (outcome) {
		case RUNNING -> MetricsConstants.DELETE_FAILURE_OUTCOME_RUNNING;
		case COMPLETED_CLEANLY -> MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY;
		case COMPLETED_WITHIN_BUDGET -> MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_WITHIN_BUDGET;
		case FAILED -> MetricsConstants.DELETE_FAILURE_OUTCOME_FAILED;
		};
		metricsContexts.forEach(context -> context.metadata().put(
						MetricsConstants.METADATA_DELETE_FAILURE_OUTCOME, published));
		metricsMgr.updateTerminalDeleteFailureOutcome(loadStepId(), published);
	}

	private IntegrityTerminalException failureBudgetException(
					final ObjectFailureBudgetDecision decision) {
		return terminalFailure(
						IntegrityTerminalException.Category.EXECUTION,
						"Standalone DELETE failed-object budget: " + failureBudgetSummary(decision),
						null);
	}

	private static String failureBudgetSummary(final ObjectFailureBudgetDecision decision) {
		return failureBudgetSummary(decision, decision.outcome(), decision.reason());
	}

	private static String failureBudgetSummary(
					final ObjectFailureBudgetDecision decision,
					final ObjectFailureBudgetOutcome outcome,
					final String reason) {
		final String hardCapNote = reason.contains("not a hard cap")
						? ""
						: "; the threshold is a stop trigger, not a hard cap";
		return decision.policy().description()
						+ ", operational failed objects=" + decision.counters().operationalFailedObjects()
						+ ", accepted objects=" + decision.counters().acceptedObjects()
						+ ", observed failure percent=" + decision.observedFailurePercent()
						+ "%, outcome=" + outcome
						+ ", reason=" + reason
						+ hardCapNote;
	}

	private static IntegrityTerminalException appendExistingFailure(
					final IntegrityTerminalException current,
					final IntegrityTerminalException additional) {
		if (current == null) {
			return additional;
		}
		if (additional != null && additional != current) {
			current.addSuppressed(additional);
		}
		return current;
	}

	private boolean awaitDurationSlicesAndStop(
					final long timeout, final TimeUnit timeUnit, final int stepSliceCount)
					throws InterruptedException {
		boolean awaitResult = false;
		RuntimeException awaitFailure = null;
		try {
			awaitResult = awaitDurationSlices(timeout, timeUnit);
		} catch (final RuntimeException failure) {
			awaitFailure = failure;
		} finally {
			Loggers.MSG.info("{}: await for {} step slices done", loadStepId(), stepSliceCount);
			stopBeforeClose(this::stop);
		}
		if (awaitFailure != null) {
			if (durationValidityFailure != null && durationValidityFailure != awaitFailure) {
				awaitFailure.addSuppressed(durationValidityFailure);
			}
			throw awaitFailure;
		}
		if (durationValidityFailure != null) {
			throw durationValidityFailure;
		}
		return awaitResult;
	}

	private boolean awaitDurationSlices(final long timeout, final TimeUnit timeUnit)
					throws InterruptedException {
		final List<LoadStep> activeSlices = stepSlices.stream()
						.filter(slice -> slice != null)
						.collect(Collectors.toList());
		final long timeoutNanos = Math.max(0, timeUnit.toNanos(timeout));
		if (activeSlices.isEmpty()) {
			throw new IllegalStateException("No active step slices are available");
		}
		final DurationPhaseAttempt prepareResult = invokeRetainedDurationPhase(
						"distributed-duration-prepare",
						activeSlices,
						slice -> slice.prepareDurationInterval(timeoutNanos),
						durationStopPhaseDeadlineNanos(),
						"spt-delete-duration-prepare-");
		failDurationStartPhase(
						prepareResult,
						"Standalone DELETE could not prepare the requested duration on every distributed input slice");
		return startAndAwaitDurationSlices(activeSlices, timeoutNanos);
	}

	private boolean startAndAwaitDurationSlices(
					final List<LoadStep> activeSlices, final long timeoutNanos)
					throws InterruptedException {
		failureBudgetStartedNanos = System.nanoTime();
		final DurationPhaseAttempt startResult = invokeRetainedDurationPhase(
						"distributed-duration-start",
						activeSlices,
						slice -> slice.startDurationInterval(timeoutNanos),
						durationStopPhaseDeadlineNanos(),
						"spt-delete-duration-start-");
		failDurationStartPhase(
						startResult,
						"Standalone DELETE could not arm the requested duration on every distributed input slice");
		// Worker-private monotonic clocks cannot be compared across RMI. Establish the one
		// controller deadline only after every worker acknowledged its own duration arm, so the
		// controller never closes admission before a successfully armed worker reaches its boundary.
		final long deadlineNanos = durationDeadlineNanos(timeoutNanos);
		setDurationDrainDeadlineIfAbsent(durationDrainDeadlineNanos(deadlineNanos));
		if (failureBudgetFailure != null) {
			throw failureBudgetFailure;
		}
		return awaitDurationSlices(activeSlices, deadlineNanos);
	}

	private synchronized void setDurationDrainDeadlineIfAbsent(final long deadlineNanos) {
		if (!durationDrainDeadlineSet) {
			durationDrainDeadlineNanos = deadlineNanos;
			durationDrainDeadlineSet = true;
		}
	}

	private void failDurationStartPhase(
					final DurationPhaseAttempt result, final String message) throws InterruptedException {
		if (result.succeeded()) {
			return;
		}
		if (result.failures().isEmpty()) {
			recordDurationValidityFailure(message, null);
		} else {
			for (final Throwable failure : result.failures()) {
				recordDurationValidityFailure(message, failure);
			}
		}
		if (result.interrupted()) {
			Thread.currentThread().interrupt();
			throw new InterruptedException("interrupted while preparing distributed duration interval");
		}
		throw durationValidityFailure;
	}

	private boolean awaitDurationSlices(
					final List<LoadStep> activeSlices, final long deadlineNanos)
					throws InterruptedException {
		if (activeSlices.isEmpty()) {
			throw new IllegalStateException("No active step slices are available");
		}
		// A remote call may ignore interruption; do not dedicate a platform thread to it.
		final ExecutorService executor = Executors.newThreadPerTaskExecutor(
						Thread.ofVirtual().name("spt-delete-await-", 0).factory());
		final var completions = new ExecutorCompletionService<DurationSliceProbe>(executor);
		final List<Future<DurationSliceProbe>> outstanding = new ArrayList<>(activeSlices.size());
		try {
			for (final LoadStep slice : activeSlices) {
				outstanding.add(completions.submit(() -> awaitDurationSlice(slice, deadlineNanos)));
			}
			int completed = 0;
			while (completed < activeSlices.size()) {
				final long remainingNanos = DurationTime.remainingNanos(
								deadlineNanos, System.nanoTime());
				if (remainingNanos == 0) {
					return false;
				}
				final Future<DurationSliceProbe> completedProbe = completions.poll(remainingNanos, TimeUnit.NANOSECONDS);
				if (completedProbe == null) {
					return false;
				}
				completed++;
				try {
					final DurationSliceProbe probe = completedProbe.get();
					if (DurationTime.deadlineReached(
									deadlineNanos, probe.observedAtNanos())) {
						return false;
					}
					if (probe.failure() != null || probe.exhausted()) {
						// The monitor records the budget failure before it closes admission, so a
						// slice that failed or ran dry because of that stop finds it set here. The
						// slice's own failure, such as a lost engine, stays on it as evidence.
						final IntegrityTerminalException budgetFailure = failureBudgetFailure;
						if (budgetFailure != null) {
							attachToFailureBudgetFailure(budgetFailure, probe.failure());
							throw budgetFailure;
						}
					}
					if (probe.failure() != null) {
						throwUnchecked(probe.failure());
					}
					if (probe.exhausted()) {
						throw recordDurationValidityFailure(
										standaloneDeleteEarlyExhaustionFailure());
					}
				} catch (final ExecutionException e) {
					if (DurationTime.deadlineReached(deadlineNanos, System.nanoTime())) {
						return false;
					}
					throwUnchecked(e.getCause());
				}
			}
			return false;
		} finally {
			outstanding.forEach(future -> future.cancel(true));
			executor.shutdownNow();
		}
	}

	private DurationSliceProbe awaitDurationSlice(final LoadStep stepSlice, final long deadlineNanos)
					throws InterruptedException {
		while (!DurationTime.deadlineReached(deadlineNanos, System.nanoTime())) {
			if (Thread.currentThread().isInterrupted()) {
				throw new InterruptedException();
			}
			final long remainingNanos = Math.max(
							1, DurationTime.remainingNanos(deadlineNanos, System.nanoTime()));
			try {
				final long pollNanos = Math.min(DURATION_REMOTE_AWAIT_POLL_NANOS, remainingNanos);
				if (stepSlice.await(pollNanos, TimeUnit.NANOSECONDS)) {
					return new DurationSliceProbe(true, System.nanoTime(), null);
				}
				final DurationAwaitStatus status = probeDurationAwaitStatus(stepSlice);
				if (status == null) {
					continue;
				}
				if (status == DurationAwaitStatus.REACHED_DEADLINE) {
					return new DurationSliceProbe(false, System.nanoTime(), null);
				}
				if (status == DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE) {
					return new DurationSliceProbe(true, System.nanoTime(), null);
				}
				if (status == DurationAwaitStatus.FAILED) {
					return new DurationSliceProbe(
									false,
									System.nanoTime(),
									terminalFailure(
													IntegrityTerminalException.Category.EXECUTION,
													"Standalone DELETE worker could not establish duration validity",
													null));
				}
			} catch (final RemoteException e) {
				return new DurationSliceProbe(
								false,
								System.nanoTime(),
								terminalFailure(
												IntegrityTerminalException.Category.EXECUTION,
												"Standalone DELETE duration run lost a remote input slice before the deadline",
												e));
			} catch (final RuntimeException failure) {
				return new DurationSliceProbe(false, System.nanoTime(), failure);
			}
		}
		return new DurationSliceProbe(false, System.nanoTime(), null);
	}

	private DurationAwaitStatus probeDurationAwaitStatus(final LoadStep stepSlice)
					throws RemoteException {
		if (!durationAwaitStatusProbes.add(stepSlice)) {
			return null;
		}
		try {
			final DurationAwaitStatus status = stepSlice.durationAwaitStatus();
			durationAwaitStatuses.put(stepSlice, status);
			return status;
		} finally {
			durationAwaitStatusProbes.remove(stepSlice);
		}
	}

	private record DurationSliceProbe(boolean exhausted, long observedAtNanos, Throwable failure) {}

	@Override
	protected final void doStop() {
		stopCloseLock.lock();
		try {
			if (stopCloseDone) {
				// await()'s trailing stop after the exit hook's close: everything is stopped and closed.
				Loggers.MSG.debug("{}: already closed, nothing to stop", loadStepId());
				return;
			}
			stopUnderLock();
		} finally {
			stopCloseLock.unlock();
		}
	}

	private void stopUnderLock() {
		metricsFetchLosesSlices = false;
		final var stoppedAtOnce = disarmLostSliceStop();
		// Each stage runs whatever an earlier one threw, so the stop's metrics output, its last stage, is
		// written; the first failure is thrown at the end, with any later ones suppressed under it.
		final var failures = new StopFailures();
		// The await's stop has the run's close after it, which writes the output with the final outcome;
		// a stop that throws skips the close it runs in, so its output cannot wait for that close.
		final boolean closeFollows = awaitStopThread == Thread.currentThread();
		if (standaloneDeleteDurationMode()) {
			if (!durationAdmissionBarrierSatisfied || durationStopFailure != null) {
				stopIncompleteDuration(failures);
				return;
			}
			failures.run(this::stopDurationSlices);
			if (durationStopFailure != null && !(failures.first() instanceof InterruptedException)) {
				// A slice failed to stop: the stop is incomplete as when its coordination fails, and the
				// close fails with that failure, so the outcome cannot be a clean one.
				stopIncompleteDuration(failures);
				return;
			}
		} else {
			failures.run(() -> stopSlicesAtStop(stoppedAtOnce));
		}
		if (failures.first() instanceof InterruptedException) {
			// An interrupted slice stop ends the stop here, as before: the stages below expect stopped
			// slices, and the close's stop runs them once the slices have stopped.
			failures.throwFirst();
		}
		failures.run(this::relayBucketVersioningNoticesAtStop);
		failures.run(this::stopMetricsAggregator);
		failures.run(this::closeItemTimingMetricsOutputFileAggregators);
		if (deferMetricsOutputForActiveFailureBudgetProbe()) {
			if (failures.first() == null || closeFollows) {
				// The close finalizes the outcome once the live probe ends, and writes the output then.
				failureBudgetMetricsOutputDeferred = true;
				failures.throwFirst();
				return;
			}
			// The outcome cannot wait for the live probe: it is final as failed, so a later close keeps it.
			final Throwable cause = failures.first();
			failures.run(() -> finalizeFailedFailureBudgetAtStop(cause));
			failureBudgetMetricsOutputDeferred = false;
			failures.run(super::doStop);
			failures.throwFirst();
			return;
		}
		failures.run(this::finalizeFailureBudgetBeforeMetricsOutput);
		if (!failureBudgetFinalizedOrAbsent()) {
			// The finalize failed, as when a stop request interrupts its counter capture: the outcome is
			// still running, and the output never carries that.
			if (closeFollows) {
				// The close's stop finalizes the outcome and writes the output.
				failures.throwFirst();
				return;
			}
			final Throwable cause = failures.first();
			failures.run(() -> finalizeFailedFailureBudgetAtStop(cause));
		}
		// Written here, the output is not written again by a close that an earlier stop deferred it to.
		failureBudgetMetricsOutputDeferred = false;
		failures.run(super::doStop);
		failures.throwFirst();
	}

	/**
	 * Ends a duration stop that is incomplete: its coordination or a slice's stop failed, so the close
	 * that follows fails with that failure and a later close retries the cleanup.
	 */
	private void stopIncompleteDuration(final StopFailures failures) {
		// The relay reads the nodes' files only: it needs neither the failed slice nor its barrier.
		failures.run(this::relayBucketVersioningNoticesAtStop);
		if (runClosesAgain() && !JvmExit.begun()) {
			// The run closes again: the close that settles the cleanup writes the output, with its
			// outcome and the counts captured after the drain, or the run writes it failed at its end.
			failureBudgetMetricsOutputDeferred = true;
			failures.throwFirst();
			return;
		}
		writeIncompleteDurationStopOutput(failures);
		failures.throwFirst();
	}

	/**
	 * Runs the await's own stop, with the run's close after it: a stop that cannot finalize the
	 * failure-budget outcome then leaves the outcome and the output to that close.
	 */
	private void stopBeforeClose(final Runnable stop) {
		awaitStopThread = Thread.currentThread();
		try {
			stop.run();
		} finally {
			awaitStopThread = null;
		}
	}

	private synchronized boolean failureBudgetFinalizedOrAbsent() {
		return failureBudgetController == null || failureBudgetFinalized || !hasStartedStandaloneDeleteWork();
	}

	/**
	 * Makes the outcome final as failed at a stop that writes the output without the terminal
	 * counters, so that a later close reports that outcome and fails the run, not a clean one.
	 */
	private synchronized void finalizeFailedFailureBudgetAtStop(final Throwable cause) {
		if (failureBudgetController == null || failureBudgetFinalized || !hasStartedStandaloneDeleteWork()) {
			return;
		}
		failureBudgetTerminalFailure = appendTerminalFailure(
						appendExistingFailure(failureBudgetFailure, durationValidityFailure),
						IntegrityTerminalException.Category.EXECUTION,
						"Standalone DELETE stop failed before its terminal failed-object counters were captured",
						cause instanceof InterruptedException ? null : cause);
		failureBudgetFinalized = true;
		publishFailureBudgetOutcome(ObjectFailureBudgetOutcome.FAILED);
	}

	private void stopDurationSlices() {
		final List<LoadStep> activeSlices = stepSlices.stream()
						.filter(slice -> slice != null)
						.collect(Collectors.toList());
		final var result = invokeStepSlicePhase(
						activeSlices,
						stepSlice -> {
							try (final var logCtx = put(KEY_STEP_ID, stepSlice.loadStepId()).put(
											KEY_CLASS_NAME, getClass().getSimpleName())) {
								stepSlice.stop();
							}
						},
						durationStopPhaseDeadlineNanos(),
						"spt-delete-step-stop-");
		if (recordDurationPhaseResult("stop", result)) {
			// As a slice stop that throws it: the stop ends before its output.
			durationSliceStopInterrupted = true;
			// The exception carries the stop request, so the flag the phase restored is cleared, as in normal mode.
			Thread.interrupted();
			throwUnchecked(new InterruptedException(loadStepId() + ": stopped while stopping the step slices"));
		}
	}

	private void stopSlicesAtStop(final IdentityHashMap<LoadStep, Thread> stoppedAtOnce) {
		// Each slice that no stop at once reached stops now, without waiting for a stop at once that a
		// slow drain holds up; a slice stopped at once, the entry's own among them, is not stopped again.
		stepSlices.stream()
						.filter(s -> s != null && !stoppedAtOnce.containsKey(s))
						.parallel()
						.forEach(this::stopSlice);
		// A stop at once that failed before it was sent, as at a slow loadStepId call, is sent now, once
		// its thread has ended, so the two never overlap. After an interrupt, the close's stop does it.
		if (awaitStopsAtOnce(stoppedAtOnce.values())) {
			takeUnsentStopsAtOnce().parallelStream().forEach(this::stopSlice);
		}
	}

	private void stopMetricsAggregator() {
		// Read once: the run's exhaustion cleanup calls this without stopCloseLock, while a close nulls it.
		final var aggregator = metricsAggregator;
		if (null != aggregator) {
			try {
				aggregator.stop();
			} catch (final RemoteException e) {
				LogUtil.trace(
								Loggers.ERR,
								Level.DEBUG,
								e,
								"{}: metrics aggregator stop failed; continuing shutdown",
								loadStepId());
			}
		}
	}

	private void closeMetricsAggregatorAfterTheRun() {
		if (null != metricsAggregator) {
			try {
				metricsAggregator.close();
			} catch (final Exception e) {
				LogUtil.exception(Level.WARN, e, "{}: failed to close the metrics aggregator", loadStepId());
			}
			metricsAggregator = null;
		}
	}

	private void closeItemTimingMetricsOutputFileAggregators() {
		final int timingSlicesStarted = startedSliceCount;
		itemTimingMetricsOutputFileAggregators.parallelStream().forEach(itemMetricsOutputFileAggregator -> {
			try {
				if (closesAsStoppedStart(timingSlicesStarted)
								&& itemMetricsOutputFileAggregator instanceof ItemTimingMetricOutputFileAggregator timingAggregator) {
					timingAggregator.closeAfterStoppedStart(timingSlicesStarted);
				} else {
					itemMetricsOutputFileAggregator.close();
				}
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				LogUtil.exception(Level.WARN, e, "{}: failed to close the item metrics output file aggregator \"{}\"",
								loadStepId(), itemMetricsOutputFileAggregator);
			}
		});
		itemTimingMetricsOutputFileAggregators.clear();
		timingMetricsStepIds.clear();
	}

	/**
	 * The failures of one stop: the first is thrown at its end, with each later one suppressed under it,
	 * never under itself. A stop request suppressed under another failure sets the interrupt flag again,
	 * so the caller still sees it.
	 */
	static final class StopFailures {

		private Throwable first;
		private boolean interrupted;

		/** Runs one stage of the stop, whatever an earlier stage threw. */
		void run(final Runnable stage) {
			try {
				stage.run();
			} catch (final Throwable e) {
				interrupted |= e instanceof InterruptedException;
				if (first == null) {
					first = e;
				} else if (first != e) {
					first.addSuppressed(e);
				}
			}
		}

		Throwable first() {
			return first;
		}

		void throwFirst() {
			if (first == null) {
				return;
			}
			if (interrupted && !(first instanceof InterruptedException)) {
				Thread.currentThread().interrupt();
			}
			throwUnchecked(first);
		}
	}

	/**
	 * Stops a slice, unless it is lost: a lost slice has its one close instead, which stops it, and
	 * gets no remote {@code loadStepId()} for the log context either.
	 *
	 * @return whether the stop was sent, whatever its outcome; true for a lost slice, which needs none
	 */
	private boolean stopSlice(final LoadStep stepSlice) {
		if (callsNoMore(stepSlice)) {
			return true;
		}
		var stopSent = false;
		try (
						final var logCtx = put(KEY_STEP_ID, stepSlice.loadStepId()).put(
										KEY_CLASS_NAME, getClass().getSimpleName())) {
			stopSent = true;
			stepSlice.stop();
			stopAnswered(stepSlice);
		} catch (final Exception e) {
			throwUncheckedIfInterrupted(e);
			if (!(e instanceof RemoteException remote && recordLostSliceAtStop(stepSlice, remote, stopSent))) {
				LogUtil.trace(Loggers.ERR, Level.WARN, e, "{}: failed to stop the step slice \"{}\"", loadStepId(),
								stepSlice);
			}
		}
		return stopSent;
	}

	private boolean deferMetricsOutputForActiveFailureBudgetProbe() {
		if (failureBudgetController == null) {
			return false;
		}
		synchronized (this) {
			return failureBudgetSnapshotCollector != null
							&& failureBudgetSnapshotCollector.hasActiveProbe();
		}
	}

	/**
	 * Writes the metrics output of an incomplete duration stop, or the output a stop left to a later
	 * close, with the outcome final as failed: no close can settle the cleanup first. A later close
	 * still cleans up, and keeps that outcome. Each stage runs whatever an earlier one threw.
	 */
	private void writeIncompleteDurationStopOutput(final StopFailures failures) {
		if (durationSliceStopInterrupted) {
			// The interrupted stop left them open: they are written with the rest of the output.
			durationSliceStopInterrupted = false;
			failures.run(this::closeItemTimingMetricsOutputFileAggregators);
		}
		final Throwable cause = durationStopFailure;
		failures.run(() -> finalizeFailedFailureBudgetAtIncompleteDurationStop(cause));
		failureBudgetMetricsOutputDeferred = false;
		failures.run(super::doStop);
	}

	private synchronized void finalizeFailedFailureBudgetAtIncompleteDurationStop(final Throwable cause) {
		if (failureBudgetFinalizedOrAbsent()) {
			return;
		}
		// The failure policy's own failure fails every close; without one, a close whose cleanup
		// completes closes the step and keeps this outcome.
		final boolean noPolicyFailure = failureBudgetFailure == null;
		finalizeFailedFailureBudgetAtStop(cause);
		if (noPolicyFailure) {
			incompleteDurationStopFailure = failureBudgetTerminalFailure;
		}
	}

	/** The failed outcome an incomplete duration stop wrote, once a later close has closed the step. */
	@Override
	protected final synchronized Throwable failureKeptByClose() {
		return isClosed() ? incompleteDurationStopFailure : null;
	}

	/** A stop left the output to a close that did not come: it is written now, failed. */
	private void writeOutputLeftToALaterClose(final Throwable closeFailure) {
		if (!failureBudgetMetricsOutputDeferred) {
			return;
		}
		final var failures = new StopFailures();
		writeIncompleteDurationStopOutput(failures);
		final Throwable writeFailure = failures.first();
		if (writeFailure != null && writeFailure != closeFailure) {
			closeFailure.addSuppressed(writeFailure);
		}
	}

	@Override
	protected final void writeOutputLeftToTheRunsClose() {
		// The run's last close may have ended on a stop request: the output is still written.
		final boolean interrupted = Thread.interrupted();
		stopCloseLock.lock();
		try {
			if (failureBudgetMetricsOutputDeferred) {
				final var failures = new StopFailures();
				writeIncompleteDurationStopOutput(failures);
				if (failures.first() != null) {
					LogUtil.exception(
									Level.WARN,
									failures.first(),
									"{}: failed to write the metrics output after the run's last close",
									loadStepId());
				}
			}
			// No close of the run closed the aggregator, and none will: it stops polling the slices now.
			closeMetricsAggregatorAfterTheRun();
		} finally {
			stopCloseLock.unlock();
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	private void finalizeFailureBudgetBeforeMetricsOutput() {
		if (failureBudgetController == null) {
			return;
		}
		stopFailureBudgetMonitor();
		finalizeFailureBudget();
	}

	@Override
	protected final void doClose()
					throws IOException {
		stopCloseLock.lock();
		try {
			closeUnderLock();
			stopCloseDone = true;
		} finally {
			stopCloseLock.unlock();
		}
	}

	private void closeUnderLock() throws IOException {
		releaseBucketMetrics();
		stopFailureBudgetMonitor();
		final boolean durationMode = standaloneDeleteDurationMode();
		if (durationMode) {
			try {
				closeForStandaloneDeleteMode(true);
			} catch (final IOException | RuntimeException | Error e) {
				// A close that fails keeps the relay for its retry, but relays what the nodes wrote so far.
				relayBucketVersioningNoticesAtClose(e);
				if (!runClosesAgain() || JvmExit.begun()) {
					// No close of the run follows this one to settle the cleanup.
					writeOutputLeftToALaterClose(e);
				}
				throw e;
			}
			return;
		}
		try {
			closeForStandaloneDeleteMode(false);
		} catch (final IOException | RuntimeException | Error e) {
			final var lost = takeLostSliceFailure();
			if (lost == null) {
				throw e;
			}
			// The lost slice is the step's failure: a close failure, such as that node's missing files, follows from it.
			lost.addSuppressed(e);
			throw lost;
		} finally {
			closeRetainedDurationPhases();
		}
	}

	/**
	 * Fails the closed step when it lost a slice: the close has run in full, but the totals can
	 * leave out all of that worker's operations.
	 */
	@Override
	protected final void afterCloseTransition() {
		final var lost = takeLostSliceFailure();
		if (lost != null) {
			throw lost;
		}
	}

	/**
	 * The run has given up its bounded cleanup, often before it reached the slice close. Each worker
	 * slice counts its own failed closes to the same limit before it releases itself and unexports, so
	 * deliver those closes now: up to the limit, each within the stop phase deadline. A slice that
	 * closes or is already released ({@link NoSuchObjectException}) gets no further close. A close
	 * still running on the worker at the deadline is not sent again, so a slice whose admission close
	 * stays blocked can miss its last close and stay exported, as before. A subclass that overrides this
	 * hook calls it through {@code super}.
	 */
	@Override
	protected void cancelDurationRunCleanupResourcesAfterExhaustion() {
		// The slices are released below: the aggregator stops polling them first.
		stopMetricsAggregator();
		final List<LoadStep> slices = stepSlices.stream().filter(Objects::nonNull).toList();
		DurationPhaseAttempt result = null;
		for (var attempt = 0; attempt < DURATION_RUN_CLOSE_ATTEMPT_LIMIT; attempt++) {
			result = invokeStepSlicePhase(
							slices,
							stepSlice -> {
								try {
									stepSlice.close();
								} catch (final NoSuchObjectException released) {
									// The worker released the slice and unexported it.
								}
							},
							durationStopPhaseDeadlineNanos(),
							"spt-delete-step-release-");
			if (result.interrupted()) {
				Thread.currentThread().interrupt();
				break;
			}
			if (result.succeeded()) {
				return;
			}
		}
		if (result != null && !result.succeeded()) {
			Loggers.ERR.warn(
							"{}: {} step slice close failure(s) after the cleanup retries; a worker releases a slice after its last failed close, and a slice whose close is still running may stay on its worker",
							loadStepId(),
							result.failures().size());
		}
	}

	private void closeForStandaloneDeleteMode(final boolean durationMode) throws IOException {
		boolean durationStopRecoordinated = false;
		if (durationMode) {
			if (durationCleanupRetryPending) {
				// The prior close surfaced its sticky failure. Start a new explicit cleanup
				// attempt without duplicating any still-running retained phase invocation.
				durationStopFailure = null;
				durationAdmissionBarrierSatisfied = false;
				standaloneDeleteStopCoordinated = false;
			} else if (!hasStartedStandaloneDeleteWork()) {
				// No slice started, so no operation admission is open to close.
				durationAdmissionBarrierSatisfied = true;
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
				coordinateDurationStop();
				if (!durationAdmissionBarrierSatisfied || durationStopFailure != null) {
					durationCleanupRetryPending = true;
					throw durationStopFailure;
				}
				durationStopRecoordinated = true;
			}
			if (durationStopRecoordinated) {
				doStop();
				if (durationStopFailure != null) {
					durationCleanupRetryPending = true;
					throw durationStopFailure;
				}
			}
		}
		IntegrityTerminalException budgetTerminal;
		try {
			budgetTerminal = finalizeFailureBudget();
		} finally {
			closeFailureBudgetSnapshotCollector();
		}
		if (budgetTerminal != null && budgetTerminal == incompleteDurationStopFailure) {
			// An earlier close threw the stop failure behind this outcome, and the output carries it: this
			// close, whose stop has now completed, cleans up and closes. A run still ends with that failure
			// (failureKeptByClose), so it ends failed, as its output says.
			budgetTerminal = null;
		}
		emitDeferredFailureBudgetMetricsOutput();
		try (final var logCtx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			IntegrityTerminalException terminalCause = appendExistingFailure(
							durationStopFailure, budgetTerminal);
			durationStopFailure = null;
			boolean durationSlicesClosed = true;
			final int slicesStarted = startedSliceCount;
			Map<OpType, Long> terminalFailureCountsByOpType = Map.of();
			if (integrityModeEnabled() && !noSliceStarted(slicesStarted)) {
				try {
					terminalFailureCountsByOpType = terminalFailureCounts(metricsContexts);
				} catch (final Throwable cause) {
					throwUncheckedIfInterrupted(cause);
					terminalCause = appendTerminalFailure(
									terminalCause,
									IntegrityTerminalException.Category.EXECUTION,
									"failed to capture terminal metadata-mode operation failure counts",
									cause);
				}
			}
			// Closing the metrics contexts drops their final snapshots.
			final Map<OpType, Long> successCounts = itemOutputFileAggregators.isEmpty()
							? Map.of()
							: successCounts(metricsContexts);
			try {
				closeMetricsContexts();
			} catch (final Throwable cause) {
				throwUncheckedIfInterrupted(cause);
				if (!integrityModeEnabled()) {
					rethrowCloseFailure(cause);
				}
				terminalCause = appendTerminalFailure(
								terminalCause,
								IntegrityTerminalException.Category.CLEANUP,
								"failed to close metadata-mode metrics contexts",
								cause);
			}
			if (null != metricsAggregator) {
				try {
					metricsAggregator.close();
				} catch (final Throwable cause) {
					throwUncheckedIfInterrupted(cause);
					if (!integrityModeEnabled()) {
						rethrowCloseFailure(cause);
					}
					terminalCause = appendTerminalFailure(
									terminalCause,
									IntegrityTerminalException.Category.CLEANUP,
									"failed to close metadata-mode metrics aggregator",
									cause);
				}
				metricsAggregator = null;
			}
			slicesClosing = true;
			if (durationMode && durationAdmissionBarrierSatisfied) {
				final List<LoadStep> activeSlices = stepSlices.stream()
								.filter(slice -> slice != null)
								.collect(Collectors.toList());
				final var result = invokeStepSlicePhase(
								activeSlices,
								stepSlice -> {
									stepSlice.close();
									Loggers.MSG.debug("{}: step slice \"{}\" closed", loadStepId(), stepSlice);
								},
								durationStopPhaseDeadlineNanos(),
								"spt-delete-step-close-");
				for (final Throwable failure : result.failures()) {
					terminalCause = appendTerminalFailure(
									terminalCause,
									IntegrityTerminalException.Category.CLEANUP,
									"failed to close a step slice with terminal accounting",
									failure);
				}
				durationSlicesClosed = result.succeeded();
				if (result.interrupted()) {
					Thread.currentThread().interrupt();
				}
			} else if (!durationMode) {
				for (final var stepSlice : stepSlices) {
					if (stepSlice == null) {
						continue;
					}
					if (callsNoMore(stepSlice)) {
						// Its one close was sent as it was lost.
						awaitLostSliceClose(stepSlice);
						continue;
					}
					try {
						stepSlice.close();
						Loggers.MSG.debug("{}: step slice \"{}\" closed", loadStepId(), stepSlice);
					} catch (final Exception e) {
						throwUncheckedIfInterrupted(e);
						if (e instanceof RemoteException remote && recordLostSliceAtClose(stepSlice, remote)) {
							// The step's failure for its lost slices names this one.
							continue;
						}
						LogUtil.exception(Level.WARN, e, "{}: failed to close the step service \"{}\"", loadStepId(),
										stepSlice);
						if (integrityModeEnabled()) {
							terminalCause = appendTerminalFailure(
											terminalCause,
											IntegrityTerminalException.Category.CLEANUP,
											"failed to close a step slice with terminal accounting",
											e);
						}
					}
				}
			}
			Loggers.MSG.debug("{}: closed all {} step slices", loadStepId(), stepSlices.size());
			if (!durationMode || (durationSlicesClosed && terminalCause == null)) {
				stepSlices.clear();
				sliceLost = false;
			}
			final List<AutoCloseable> failedDeleteArtifactAggregators = new ArrayList<>();
			for (final var deleteArtifactAggregator : deleteArtifactAggregators) {
				try {
					if (closesAsStoppedStart(slicesStarted)
									&& deleteArtifactAggregator instanceof DeleteArtifactAggregator aggregator) {
						aggregator.closeAfterStoppedStart(slicesStarted);
					} else {
						deleteArtifactAggregator.close();
					}
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					failedDeleteArtifactAggregators.add(deleteArtifactAggregator);
					terminalCause = appendTerminalFailure(
									terminalCause,
									IntegrityTerminalException.Category.AGGREGATION,
									"failed to collect and publish DELETE artifacts",
									e);
				}
			}
			deleteArtifactAggregators.clear();
			deleteArtifactAggregators.addAll(failedDeleteArtifactAggregators);
			for (final var aggregator : operationLifecycleAggregators) {
				try {
					closeOperationLifecycleArtifact(aggregator, slicesStarted);
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					terminalCause = appendTerminalFailure(terminalCause,
									IntegrityTerminalException.Category.AGGREGATION,
									"failed to collect operation lifecycle evidence", e);
				}
			}
			operationLifecycleAggregators.clear();
			if (nicPathsCollector != null && (!durationMode || (durationAdmissionBarrierSatisfied && durationSlicesClosed))) {
				nicPathsCollector.close();
				nicPathsCollector = null;
			}
			if (bucketVersioningRelay != null && (!durationMode || (durationAdmissionBarrierSatisfied && durationSlicesClosed))) {
				bucketVersioningRelay.close();
				bucketVersioningRelay = null;
			}
			for (final var integrityLogFileAggregator : integrityLogFileAggregators) {
				try {
					closeSliceLoggerArtifact(integrityLogFileAggregator, slicesStarted);
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					terminalCause = appendTerminalFailure(
									terminalCause,
									IntegrityTerminalException.Category.AGGREGATION,
									"failed to collect and publish integrity logger artifacts",
									e);
				}
			}
			integrityLogFileAggregators.clear();
			for (final var itemDataInputFileSlicer : itemDataInputFileSlicers) {
				try {
					itemDataInputFileSlicer.close();
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					LogUtil.exception(Level.WARN, e, "{}: failed to close the item data input file slicer \"{}\"",
									loadStepId(), itemDataInputFileSlicer);
					if (integrityModeEnabled()) {
						terminalCause = appendTerminalFailure(
										terminalCause,
										IntegrityTerminalException.Category.CLEANUP,
										"failed to close metadata-mode item data input slicer",
										e);
					}
				}
			}
			itemDataInputFileSlicers.clear();
			for (final var itemInputFileSlicer : itemInputFileSlicers) {
				try {
					itemInputFileSlicer.close();
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					LogUtil.exception(Level.WARN, e, "{}: failed to close the item input file slicer \"{}\"",
									loadStepId(), itemInputFileSlicer);
					if (integrityModeEnabled()) {
						terminalCause = appendTerminalFailure(
										terminalCause,
										IntegrityTerminalException.Category.CLEANUP,
										"failed to close metadata-mode item input slicer",
										e);
					}
				}
			}
			itemInputFileSlicers.clear();
			final boolean itemOutputCollected = !itemOutputFileAggregators.isEmpty();
			for (final var itemOutputFileAggregator : itemOutputFileAggregators) {
				try {
					if (itemOutputFileAggregator instanceof CsvArtifactAggregator csvAggregator) {
						final long failedOperationCount = terminalFailureCountsByOpType.getOrDefault(
										csvAggregator.artifactOpType(), -1L);
						if (closesAsStoppedStart(slicesStarted)) {
							csvAggregator.closeAfterStoppedStart(failedOperationCount, slicesStarted);
						} else {
							csvAggregator.close(failedOperationCount);
						}
					} else if (closesAsStoppedStart(slicesStarted)
									&& itemOutputFileAggregator instanceof ItemOutputFileAggregator itemAggregator) {
						itemAggregator.closeAfterStoppedStart(slicesStarted);
					} else {
						itemOutputFileAggregator.close();
					}
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					if (integrityModeEnabled()) {
						terminalCause = appendTerminalFailure(
										terminalCause,
										IntegrityTerminalException.Category.AGGREGATION,
										"failed to close and publish canonical integrity manifest",
										e);
					} else {
						LogUtil.exception(
										Level.WARN,
										e,
										"{}: failed to close the item output file aggregator \"{}\"",
										loadStepId(),
										itemOutputFileAggregator);
					}
				}
			}
			itemOutputFileAggregators.clear();
			if (itemOutputCollected && !noSliceStarted(slicesStarted)) {
				try {
					itemOutputCollected(config.stringVal("item-output-file"), successCounts);
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					LogUtil.exception(Level.WARN, e, "{}: failed to check the item output file", loadStepId());
				}
			}
			for (final var opTraceLogFileAggregator : opTraceLogFileAggregators) {
				try {
					if (closesAsStoppedStart(slicesStarted)
									&& opTraceLogFileAggregator instanceof OpTraceLogFileAggregator traceAggregator) {
						traceAggregator.closeAfterStoppedStart(slicesStarted);
					} else {
						opTraceLogFileAggregator.close();
					}
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					LogUtil.exception(Level.WARN, e,
									"{}: failed to close the operation traces log file aggregator \"{}\"", loadStepId(),
									opTraceLogFileAggregator);
					if (integrityModeEnabled()) {
						terminalCause = appendTerminalFailure(
										terminalCause,
										IntegrityTerminalException.Category.CLEANUP,
										"failed to close metadata-mode operation trace aggregator",
										e);
					}
				}
			}
			opTraceLogFileAggregators.clear();
			opTraceStepIds.clear();
			for (final var storageAuthFileSlicer : storageAuthFileSlicers) {
				try {
					storageAuthFileSlicer.close();
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					LogUtil.exception(Level.WARN, e, "{}: failed to close the storage auth file slicer \"{}\"",
									loadStepId(), storageAuthFileSlicer);
					if (integrityModeEnabled()) {
						terminalCause = appendTerminalFailure(
										terminalCause,
										IntegrityTerminalException.Category.CLEANUP,
										"failed to close metadata-mode storage auth slicer",
										e);
					}
				}
			}
			storageAuthFileSlicers.clear();
			if (terminalCause != null) {
				if (durationMode) {
					durationStopFailure = terminalCause;
					durationCleanupRetryPending = true;
				}
				throw terminalCause;
			}
			if (durationMode) {
				durationCleanupRetryPending = false;
			}
			closeRetainedDurationPhases();
		}
	}

	/**
	 * Whether the step's start ended before any slice's start returned: a start stopped before its
	 * first slice started, or one that failed from {@link #init()} on before that, a slice whose own
	 * start threw among them. No slice ran, so the close captures no terminal counts and publishes
	 * nothing. A step never started is judged on the slices it holds instead.
	 */
	private boolean noSliceStarted(final int slicesStarted) {
		return slicesStarted == 0 && (startStopped || startStepEntered());
	}

	/**
	 * Whether the close publishes the slice artifacts from the slices whose start returned only: after
	 * a start that ended as a stop, or that ended before any slice started.
	 */
	private boolean closesAsStoppedStart(final int slicesStarted) {
		return startStopped || noSliceStarted(slicesStarted);
	}

	/**
	 * Publishes a logger-backed slice artifact: from every slice, or after a start that ended as a
	 * stop or at a slice refused for its step-id files, from the slices whose start returned only. A
	 * slice refused at JVM exit or never reached wrote nothing, and a slice cut partway through its
	 * start, or refused at it for its step-id files, is left out with them: its folder holds, or may
	 * hold, another step's or run's file. A slice whose start failed otherwise may have written its
	 * file, so it is collected. With no slice started, as after a start that failed before any slice
	 * started, nothing is published.
	 */
	private void closeSliceLoggerArtifact(final AutoCloseable aggregator, final int slicesStarted) throws Exception {
		if ((closesAsStoppedStart(slicesStarted) || sliceRefusedForStepIdFiles)
						&& aggregator instanceof CsvLoggerArtifactAggregator sliceArtifact) {
			sliceArtifact.closeAfterStoppedStart(slicesStarted);
		} else {
			aggregator.close();
		}
	}

	/**
	 * Publishes the operation lifecycle file as {@link #closeSliceLoggerArtifact} does. A slice that no
	 * start reached writes none, so after a start that failed between creating a slice and starting
	 * it, the file is collected from the slices started before it, as after a stopped start.
	 */
	private void closeOperationLifecycleArtifact(final AutoCloseable aggregator, final int slicesStarted)
					throws Exception {
		if (!startStopped && createdSliceUnreached && aggregator instanceof CsvLoggerArtifactAggregator sliceArtifact) {
			sliceArtifact.closeAfterStoppedStart(slicesStarted);
		} else {
			closeSliceLoggerArtifact(aggregator, slicesStarted);
		}
	}

	private void emitDeferredFailureBudgetMetricsOutput() {
		if (!failureBudgetMetricsOutputDeferred) {
			return;
		}
		failureBudgetMetricsOutputDeferred = false;
		super.doStop();
	}

	private static void rethrowCloseFailure(final Throwable cause) throws IOException {
		if (cause instanceof IOException) {
			throw (IOException) cause;
		}
		if (cause instanceof RuntimeException) {
			throw (RuntimeException) cause;
		}
		if (cause instanceof Error) {
			throw (Error) cause;
		}
		throw new IOException(cause);
	}

	private static final class FailureBudgetSnapshotCollector implements AutoCloseable {
		private final List<LoadStep> slices;
		private final ExecutorService executor;
		private final List<Future<DeleteObjectLifecycleSnapshot>> inFlight;
		private final List<DeleteObjectLifecycleSnapshot> completedSnapshots;
		private boolean terminalCollectionStarted;

		private FailureBudgetSnapshotCollector(final List<LoadStep> slices) {
			this.slices = List.copyOf(slices);
			this.inFlight = new ArrayList<>(Collections.nCopies(slices.size(), null));
			this.completedSnapshots = new ArrayList<>(Collections.nCopies(slices.size(), null));
			executor = Executors.newThreadPerTaskExecutor(
							Thread.ofVirtual()
											.name("spt-delete-failure-budget-snapshot-", 0)
											.factory());
		}

		private void requireSameSlices(final List<LoadStep> candidates) {
			if (slices.size() != candidates.size()) {
				throw new IllegalStateException("failure-budget worker set changed during collection");
			}
			for (int i = 0; i < slices.size(); i++) {
				if (slices.get(i) != candidates.get(i)) {
					throw new IllegalStateException("failure-budget worker identity changed during collection");
				}
			}
		}

		private synchronized List<DeleteObjectLifecycleSnapshot> collect(
						final long deadlineNanos, final boolean completion) throws InterruptedException {
			if (completion && !terminalCollectionStarted) {
				terminalCollectionStarted = true;
				resetForTerminalCollection();
			}
			if (inFlight.stream().allMatch(probe -> probe == null)
							&& awaitProbeFlightAdmission(deadlineNanos)) {
				startProbeFlight();
			}
			final List<DeleteObjectLifecycleSnapshot> snapshots = new ArrayList<>(slices.size());
			for (int i = 0; i < slices.size(); i++) {
				final Future<DeleteObjectLifecycleSnapshot> probe = inFlight.get(i);
				if (probe == null) {
					snapshots.add(completedSnapshots.get(i));
					continue;
				}
				try {
					final long remainingNanos = DurationTime.remainingNanos(
									deadlineNanos, System.nanoTime());
					if (remainingNanos == 0 && !probe.isDone()) {
						snapshots.add(null);
						continue;
					}
					final DeleteObjectLifecycleSnapshot snapshot = probe.get(
									remainingNanos, TimeUnit.NANOSECONDS);
					completedSnapshots.set(i, snapshot);
					snapshots.add(snapshot);
					inFlight.set(i, null);
				} catch (final TimeoutException unavailable) {
					snapshots.add(null);
				} catch (final ExecutionException | CancellationException unavailable) {
					inFlight.set(i, null);
					snapshots.add(null);
				}
			}
			return Collections.unmodifiableList(snapshots);
		}

		private void resetForTerminalCollection() {
			inFlight.stream()
							.filter(probe -> probe != null)
							.forEach(probe -> probe.cancel(true));
			Collections.fill(inFlight, null);
			Collections.fill(completedSnapshots, null);
		}

		private static boolean awaitProbeFlightAdmission(final long deadlineNanos)
						throws InterruptedException {
			final long remainingNanos = DurationTime.remainingNanos(
							deadlineNanos, System.nanoTime());
			return remainingNanos > 0
							&& FAILURE_BUDGET_SNAPSHOT_FLIGHT_ADMISSION.tryAcquire(
											remainingNanos, TimeUnit.NANOSECONDS);
		}

		private void startProbeFlight() {
			if (slices.isEmpty()) {
				FAILURE_BUDGET_SNAPSHOT_FLIGHT_ADMISSION.release();
				return;
			}
			Collections.fill(completedSnapshots, null);
			final SnapshotProbeFlight flight = new SnapshotProbeFlight(slices.size());
			for (int i = 0; i < slices.size(); i++) {
				final SnapshotProbeTask task = new SnapshotProbeTask(
								slices.get(i)::deleteObjectLifecycle, flight);
				inFlight.set(i, task);
				try {
					executor.execute(task);
				} catch (final RuntimeException submissionFailure) {
					task.cancel(false);
				}
			}
		}

		private synchronized boolean hasActiveProbe() {
			return inFlight.stream().anyMatch(probe -> probe != null && !probe.isDone());
		}

		@Override
		public synchronized void close() {
			inFlight.stream()
							.filter(probe -> probe != null)
							.forEach(probe -> probe.cancel(true));
			executor.shutdownNow();
		}
	}

	private static final class SnapshotProbeFlight {
		private final AtomicInteger unfinishedTasks;

		private SnapshotProbeFlight(final int taskCount) {
			unfinishedTasks = new AtomicInteger(taskCount);
		}

		private void taskFinished() {
			if (unfinishedTasks.decrementAndGet() == 0) {
				FAILURE_BUDGET_SNAPSHOT_FLIGHT_ADMISSION.release();
			}
		}
	}

	private static final class SnapshotProbeTask extends FutureTask<DeleteObjectLifecycleSnapshot> {
		private final SnapshotProbeExecution execution;

		private SnapshotProbeTask(
						final Callable<DeleteObjectLifecycleSnapshot> callable,
						final SnapshotProbeFlight flight) {
			this(new SnapshotProbeExecution(callable, flight));
		}

		private SnapshotProbeTask(final SnapshotProbeExecution execution) {
			super(execution);
			this.execution = execution;
		}

		@Override
		public boolean cancel(final boolean mayInterruptIfRunning) {
			final boolean cancelled = super.cancel(mayInterruptIfRunning);
			if (cancelled) {
				execution.finishIfNotStarted();
			}
			return cancelled;
		}
	}

	private static final class SnapshotProbeExecution implements Callable<DeleteObjectLifecycleSnapshot> {
		private final Callable<DeleteObjectLifecycleSnapshot> probe;
		private final SnapshotProbeFlight flight;
		private final AtomicBoolean executionClaimed = new AtomicBoolean();

		private SnapshotProbeExecution(
						final Callable<DeleteObjectLifecycleSnapshot> probe,
						final SnapshotProbeFlight flight) {
			this.probe = probe;
			this.flight = flight;
		}

		@Override
		public DeleteObjectLifecycleSnapshot call() throws Exception {
			if (!executionClaimed.compareAndSet(false, true)) {
				throw new IllegalStateException("snapshot probe execution was already claimed");
			}
			ACTIVE_FAILURE_BUDGET_SNAPSHOT_PROBE_TASKS.incrementAndGet();
			try {
				return probe.call();
			} finally {
				ACTIVE_FAILURE_BUDGET_SNAPSHOT_PROBE_TASKS.decrementAndGet();
				flight.taskFinished();
			}
		}

		private void finishIfNotStarted() {
			if (executionClaimed.compareAndSet(false, true)) {
				flight.taskFinished();
			}
		}
	}

	@Override
	public final T config(final Map<String, Object> configMap) {
		if (ctxConfigs != null) {
			throw new IllegalStateException("config(...) should be invoked before any append(...) call");
		}
		final var configCopy = (Config) new BasicConfig(config);
		final var argValPairs = (Map<String, String>) new HashMap<String, String>();
		final var listVals = (Map<String, List<String>>) new HashMap<String, List<String>>();
		flatten(configMap, argValPairs, listVals, config.pathSep(), null);
		final var aliasingConfig = config.<Map<String, Object>> listVal("aliasing");
		try {
			validateListValues(configCopy, listVals, aliasingConfig);
			final var aliasedArgs = AliasingUtil.apply(argValPairs, aliasingConfig);
			validateBooleanValues(configCopy, aliasedArgs);
			if (stepIdGenerated || config.boolVal("load-step-idAutoGenerated")) {
				configCopy.val("load-step-idAutoGenerated", aliasedArgs.get("load-step-id") == null);
			}
			aliasedArgs.forEach(configCopy::val); // merge
			EngineBuildInfoProvider.global().projectVersion(configCopy, aliasedArgs.containsKey("run-version"));
		} catch (final Exception e) {
			LogUtil.exception(Level.FATAL, e, "Scenario syntax error");
			throw new ScenarioConfigurationException(e, config);
		}
		return copyInstance(configCopy, null);
	}

	@Override
	public final T append(final Map<String, Object> context) {
		final List<Config> ctxConfigsCopy;
		if (ctxConfigs == null) {
			ctxConfigsCopy = new ArrayList<>(1);
		} else {
			ctxConfigsCopy = ctxConfigs.stream().map(BasicConfig::new).collect(Collectors.toList());
		}
		final var argValPairs = (Map<String, String>) new HashMap<String, String>();
		final var listVals = (Map<String, List<String>>) new HashMap<String, List<String>>();
		flatten(context, argValPairs, listVals, config.pathSep(), null);
		final var aliasingConfig = config.<Map<String, Object>> listVal("aliasing");
		final var baseConfig = appendCopyBaseConfig();
		final var ctxConfig = (Config) new BasicConfig(baseConfig);
		try {
			validateListValues(ctxConfig, listVals, aliasingConfig);
			final var aliasedArgs = AliasingUtil.apply(argValPairs, aliasingConfig);
			validateBooleanValues(ctxConfig, aliasedArgs);
			aliasedArgs.forEach(ctxConfig::val); // merge
			if (marksSetContextStepIds() && aliasedArgs.get("load-step-id") != null) {
				// after the merge: a map that sets an id has its id set, whatever flag it also gives
				ctxConfig.val("load-step-idAutoGenerated", false);
			}
			EngineBuildInfoProvider.global().projectVersion(ctxConfig, aliasedArgs.containsKey("run-version"));
		} catch (final Exception e) {
			LogUtil.exception(Level.FATAL, e, "Scenario syntax error");
			throw new ScenarioConfigurationException(e, config);
		}
		ctxConfigsCopy.add(ctxConfig);
		return copyInstance(baseConfig, ctxConfigsCopy);
	}

	/**
	 * The base configuration of an {@link #append} copy. An id in the appended map belongs to that
	 * context, so it does not name the copy: a copy of a step with a generated id generates its own.
	 */
	private Config appendCopyBaseConfig() {
		if (!stepIdGenerated) {
			return config;
		}
		final var baseConfig = (Config) new BasicConfig(config);
		baseConfig.val("load-step-idAutoGenerated", true);
		return baseConfig;
	}

	/**
	 * Whether {@link #append} marks a context whose map sets {@code load.step.id} as having its id set,
	 * with {@code load-step-idAutoGenerated} false, so the step can tell it from an id the context only
	 * copied from the base. The default, since 5.17.1, is {@code true}: the weighted step reads the flag
	 * per context, and the linear and pipeline steps never read a context's flag. A step type may
	 * return {@code false}: append then merges the map as before 5.17.1, so a context's flag is the one
	 * it copied from the base, or the one its map sets. The mixed step does, though its slice takes
	 * the step id from the base and reads no context's flag.
	 */
	protected boolean marksSetContextStepIds() {
		return true;
	}

	/**
	 * The step id that every engine writes the operation traces and timing metrics of context
	 * {@code ctxIndex} under, so the step collects each engine's file of that id once. The default is
	 * the step's id: a linear, mixed or pipeline slice writes all its records under it. A step type
	 * whose contexts run under their own ids, as the weighted step's do, returns the context's id.
	 * Called once per context at the step's start, after {@link #init()}; the step's other collected
	 * files are collected under {@link #loadStepId()} whatever this returns.
	 */
	protected String contextOutputStepId(final int ctxIndex) {
		return loadStepId();
	}

	/** Whether this step's one driver mixes operations by weight, which only the mixed step type does. */
	protected boolean mixesOperations() {
		return false;
	}

	private ItemSizeRecording stepItemSizeRecording(final ItemSizeRecording recording) {
		return recording != null && mixesOperations() ? recording.asMixed() : recording;
	}

	protected abstract T copyInstance(final Config config, final List<Config> ctxConfigs);
}
