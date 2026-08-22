package com.dell.spt.base.load.step.client;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.dell.spt.base.config.ConfigUtil.flatten;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;
import static org.apache.logging.log4j.CloseableThreadContext.put;

import com.dell.spt.base.config.AliasingUtil;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.integrity.IntegrityConfig;
import com.dell.spt.base.integrity.IntegrityCsvArtifacts;
import com.dell.spt.base.integrity.IntegrityInputProvenance;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.io.ItemInputFactory;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.DurationAwaitStatus;
import com.dell.spt.base.load.step.DurationTime;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepBase;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.client.metrics.MetricsAggregator;
import com.dell.spt.base.load.step.client.metrics.MetricsAggregatorImpl;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.DistributedMetricsContextImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.util.BinarySizeFormat;
import com.github.akurilov.commons.net.NetUtil;
import com.github.akurilov.commons.reflection.TypeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValueTypeException;
import com.github.akurilov.confuse.impl.BasicConfig;

import java.io.IOException;
import java.rmi.ConnectException;
import java.rmi.RemoteException;
import java.rmi.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.logging.log4j.Level;

public abstract class LoadStepClientBase<T extends LoadStepClient<T>>
				extends LoadStepBase
				implements LoadStepClient<T> {
	private static final long DURATION_DRAIN_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(10);
	private static final long DURATION_REMOTE_AWAIT_POLL_NANOS = TimeUnit.SECONDS.toNanos(1);

	private final List<LoadStep> stepSlices = new ArrayList<>();
	private final List<FileManager> fileMgrs = new ArrayList<>();
	// for the core configuration options which are using the files
	private final List<AutoCloseable> itemDataInputFileSlicers = new ArrayList<>();
	private final List<AutoCloseable> itemInputFileSlicers = new ArrayList<>();
	private final List<AutoCloseable> itemOutputFileAggregators = new ArrayList<>();
	private final List<AutoCloseable> integrityLogFileAggregators = new ArrayList<>();
	private final List<AutoCloseable> itemTimingMetricsOutputFileAggregators = new ArrayList<>();
	private final List<AutoCloseable> opTraceLogFileAggregators = new ArrayList<>();
	private final List<AutoCloseable> storageAuthFileSlicers = new ArrayList<>();
	private final static int MAX_SLEEP_TIME_MILLIS = 120_000; // 2min.
	private volatile IntegrityTerminalException durationStopFailure;
	private volatile IntegrityTerminalException durationValidityFailure;
	private volatile boolean durationAdmissionBarrierSatisfied;
	private boolean durationCleanupRetryPending;
	private long durationDrainDeadlineNanos = Long.MIN_VALUE;
	private boolean durationDrainDeadlineSet;
	private final Map<LoadStep, DurationAwaitStatus> durationAwaitStatuses = synchronizedIdentityMap();
	private final Set<LoadStep> durationAwaitStatusProbes = Collections.synchronizedSet(
					Collections.newSetFromMap(new IdentityHashMap<>()));

	public LoadStepClientBase(
					final Config config, final List<Extension> extensions, final List<Config> ctxConfigs,
					final MetricsManager metricsMgr) {
		super(config, extensions, ctxConfigs, metricsMgr);
	}

	private static <K, V> Map<K, V> synchronizedIdentityMap() {
		return Collections.synchronizedMap(new IdentityHashMap<>());
	}

	private MetricsAggregator metricsAggregator = null;

	@Override
	protected final void doStartWrapped()
					throws IllegalArgumentException {
		resetDurationLifecycleForStart();
		try (final var logCtx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			// need to set the once generated step id
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
			initFileManagers(nodeAddrs, fileMgrs);
			final var sliceCount = 1 + nodeAddrs.size();
			// init the base/shared config slices
			final var configSlices = sliceConfig(
							config,
							sliceCount,
							IntegrityConfig.requiresExactOutputCount(config.configVal("storage")));
			addFileClients(config, configSlices);
			// init the config slices for each of the load step context configs
			final var ctxConfigsSlices = (List<List<Config>>) new ArrayList<List<Config>>(sliceCount);
			for (var i = 0; i < sliceCount; i++) {
				ctxConfigsSlices.add(new ArrayList<>());
			}
			if (null != ctxConfigs) {
				for (final var ctxConfig : ctxConfigs) {
					final var ctxConfigSlices = sliceConfig(ctxConfig, sliceCount, false);
					addFileClients(ctxConfig, ctxConfigSlices);
					for (var i = 0; i < sliceCount; i++) {
						ctxConfigsSlices.get(i).add(ctxConfigSlices.get(i));
					}
				}
			}
			initAndStartStepSlices(nodeAddrs, configSlices, ctxConfigsSlices, metricsMgr);
			initAndStartMetricsAggregator(config.configVal("output-metrics"));
			Loggers.MSG.info(
							"{}: load step client started, additional nodes: {}", loadStepId(),
							Arrays.toString(nodeAddrs.toArray()));
		}
	}

	private void resetDurationLifecycleForStart() {
		if (standaloneDeleteDurationMode()) {
			durationStopFailure = null;
			durationValidityFailure = null;
			durationAdmissionBarrierSatisfied = false;
			durationCleanupRetryPending = false;
			durationDrainDeadlineNanos = Long.MIN_VALUE;
			durationDrainDeadlineSet = false;
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

	private static void initFileManagers(final List<String> nodeAddrs, final List<FileManager> fileMgrsDst) {
		// local file manager
		fileMgrsDst.add(FileManager.INSTANCE);
		// remote file managers
		for (final var nodeAddr : nodeAddrs) {
			final var fileMgr = resolveFileManagerWithRetries(nodeAddr, 10);
			if (null == fileMgr) {
				throw new IllegalStateException(
								"Failed to resolve the file manager service @ " + nodeAddr
												+ ". Ensure the remote node is running with --run-node");
			}
			fileMgrsDst.add(fileMgr);
		}
	}

	private static FileManagerService resolveFileManagerWithRetries(final String nodeAddrWithPort, final int maxRetries) {
		FileManagerService fms = null;
		int retryCount = 0;
		while (null == fms && retryCount < maxRetries && !Thread.currentThread().isInterrupted()) {
			try {
				retryCount++;
				fms = FileManagerClient.resolve(nodeAddrWithPort);
			} catch (final ConnectException e) {
				LogUtil.exception(
								Level.ERROR, e, "Failed to resolve the file manager service @ {}. Will try to " +
												"reconnect. {} retry out of {}",
								nodeAddrWithPort, retryCount, maxRetries);
				try {
					final int sleepTime = 1000 * (int) Math.pow(2, retryCount);
					Thread.sleep(Math.min(sleepTime, MAX_SLEEP_TIME_MILLIS));
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

	private void addFileClients(final Config config, final List<Config> configSlices) {
		final var loadConfig = config.configVal("load");
		final var batchSize = loadConfig.intVal("batch-size");
		final var storageConfig = config.configVal("storage");
		final var itemConfig = config.configVal("item");
		final var itemDataConfig = itemConfig.configVal("data");
		final var verifyFlag = itemDataConfig.boolVal("verify");
		final var itemDataInputConfig = itemDataConfig.configVal("input");
		final var itemDataInputLayerConfig = itemDataInputConfig.configVal("layer");
		final var itemDataInputLayerSizeRaw = itemDataInputLayerConfig.val("size");
		final SizeInBytes itemDataLayerSize;
		if (itemDataInputLayerSizeRaw instanceof String) {
			itemDataLayerSize = BinarySizeFormat.parseSize((String) itemDataInputLayerSizeRaw);
		} else {
			itemDataLayerSize = new SizeInBytes(TypeUtil.typeConvert(itemDataInputLayerSizeRaw, int.class));
		}
		final var itemDataInputFile = itemDataInputConfig.stringVal("file");
		final var itemDataInputSeed = itemDataInputConfig.stringVal("seed");
		final var itemDataInputLayerCacheSize = itemDataInputLayerConfig.intVal("cache");
		final var isInHeapMem = itemDataInputLayerConfig.boolVal("heap");
		final var itemDataCompressibility = itemDataInputConfig.doubleVal("compressibility");
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
		final var itemType = ItemType.valueOf(itemConfig.stringVal("type").toUpperCase(Locale.ROOT));
		final boolean skipScatter = ItemType.PATH.equals(itemType) && OpType.LIST.equals(opType);
		if (skipScatter) {
			Loggers.MSG.info("{}: skipping item input scatter for LIST workload", loadStepId());
		}

		try (
						final var dataInput = DataInput.instance(
										itemDataInputFile, itemDataInputSeed, itemDataLayerSize, itemDataInputLayerCacheSize, isInHeapMem,
										itemDataCompressibility, isDedupable);
						final var storageDriver = StorageDriver.instance(
										extensions, storageConfig, dataInput, effectiveVerifyFlag, batchSize, loadStepId());
						final var itemInput = skipScatter
										? null
										: ItemInputFactory.createItemInput(itemConfig, batchSize, storageDriver)) {
			if (null != itemDataInputFile && !itemDataInputFile.isEmpty()) {
				itemDataInputFileSlicers.add(
								new ItemDataInputFileSlicer(loadStepId(), fileMgrs, configSlices, itemDataInputFile, batchSize));
				Loggers.MSG.debug("{}: item data input file slicer initialized", loadStepId());
			}
			if (!skipScatter && itemInput != null) {
				itemInputFileSlicers.add(
								new ItemInputFileSlicer(
												loadStepId(), fileMgrs, configSlices, itemInput, batchSize, integrityConfig.enabled()));
				Loggers.MSG.debug("{}: item input file slicer initialized", loadStepId());
			}
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

		if (config.boolVal("output-metrics-timing-persist")) {
			itemTimingMetricsOutputFileAggregators.add(
							new ItemTimingMetricOutputFileAggregator(loadStepId(), fileMgrs));
			Loggers.MSG.debug("{}: item metrics output file aggregator initialized", loadStepId());
		}

		if (config.boolVal("output-metrics-trace-persist")) {
			opTraceLogFileAggregators.add(new OpTraceLogFileAggregator(loadStepId(), fileMgrs));
			Loggers.MSG.debug("{}: operation traces log file aggregator initialized", loadStepId());
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
			if (OpType.READ.equals(opType)) {
				integrityLogFileAggregators.add(new CsvLoggerArtifactAggregator(
								loadStepId(), fileMgrs, Loggers.INTEGRITY_FAILURES.getName(),
								IntegrityCsvArtifacts.FAILURES_FILE_NAME, IntegrityCsvArtifacts.FAILURES_HEADER));
				integrityLogFileAggregators.add(new CsvLoggerArtifactAggregator(
								loadStepId(), fileMgrs, Loggers.INTEGRITY_PERFORMANCE.getName(),
								IntegrityCsvArtifacts.PERFORMANCE_FILE_NAME, IntegrityCsvArtifacts.PERFORMANCE_HEADER));
			} else if (OpType.CREATE.equals(opType)) {
				integrityLogFileAggregators.add(new CsvLoggerArtifactAggregator(
								loadStepId(), fileMgrs, Loggers.INTEGRITY_PERFORMANCE.getName(),
								IntegrityCsvArtifacts.PERFORMANCE_FILE_NAME, IntegrityCsvArtifacts.PERFORMANCE_HEADER));
				final var threshold = itemConfig.configVal("data").configVal("ranges").val("threshold");
				final long thresholdBytes = threshold instanceof String
								? BinarySizeFormat.parseFixedSize((String) threshold)
								: TypeUtil.typeConvert(threshold, long.class);
				if (thresholdBytes > 0) {
					integrityLogFileAggregators.add(new CsvLoggerArtifactAggregator(
									loadStepId(), fileMgrs, Loggers.MULTIPART_LIFECYCLE.getName(),
									IntegrityCsvArtifacts.MULTIPART_LIFECYCLE_FILE_NAME,
									IntegrityCsvArtifacts.MULTIPART_LIFECYCLE_HEADER));
				}
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

	private void initAndStartMetricsAggregator(Config config) {
		try (final var logCtx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			metricsAggregator = new MetricsAggregatorImpl(loadStepId(), stepSlices, config);
			metricsAggregator.start();
		} catch (final Exception e) {
			LogUtil.exception(Level.ERROR, e, "{}: failed to start the metrics aggregator", loadStepId());
		}
	}

	private void initAndStartStepSlices(
					final List<String> nodeAddrs, final List<Config> configSlices, final List<List<Config>> ctxConfigsSlices,
					final MetricsManager metricsManager) {
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
			stepSlices.add(stepSlice);
			if (validateRunId) {
				requireMatchingRunId(stepSlice, expectedRunId);
			}
			try {
				stepSlice.start();
			} catch (final Exception e) {
				if (e instanceof InterruptedException) {
					throwUnchecked(e);
				}
				if (validateRunId) {
					throw terminalFailure(
									IntegrityTerminalException.Category.EXECUTION,
									"failed to start metadata-mode step slice " + i,
									e);
				}
				LogUtil.exception(
								Level.ERROR, e, "{}: failed to start the step slice \"{}\"", loadStepId(), stepSlice);
			}
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

	private List<Config> sliceConfig(
					final Config config, final int sliceCount, final boolean balanceCountLimit) {
		final var configSlices = (List<Config>) new ArrayList<Config>(sliceCount);
		for (var i = 0; i < sliceCount; i++) {
			final var configSlice = ConfigSliceUtil.initSlice(config);
			if (i == 0) {
				// local step slice: disable the average metrics output
				configSlice.val("output-metrics-average-period", "0s");
			}
			configSlices.add(configSlice);
		}
		if (sliceCount > 1) { // distributed mode
			//
			final var countLimit = config.longVal("load-op-limit-count");
			if (countLimit > 0) {
				if (balanceCountLimit) {
					ConfigSliceUtil.sliceLongValueBalanced(
									countLimit, configSlices, "load-op-limit-count");
				} else {
					ConfigSliceUtil.sliceLongValue(
									countLimit, configSlices, "load-op-limit-count");
				}
				configSlices
								.stream()
								.mapToLong(configSlice -> configSlice.longVal("load-op-limit-count"))
								.filter(countLimitSlice -> countLimitSlice == 0)
								.findAny()
								.ifPresent(
												countLimitSlice -> Loggers.MSG.fatal(
																"{}: the count limit ({}) is too small to be sliced among the {} nodes, the load step " +
																				"won't work correctly",
																loadStepId(), countLimit, sliceCount));
			}
			//
			final var countFailLimit = config.longVal("load-op-limit-fail-count");
			if (countFailLimit > 0) {
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
																loadStepId(), countLimit, sliceCount));
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

	@Override
	protected final void initMetrics(
					final int originIndex, final OpType opType, final int concurrencyLimit, final Config metricsConfig,
					final SizeInBytes itemDataSize, final boolean outputColorFlag) {

		// Determine effective config for this origin index (merge base + context slice if present)
		Config effectiveConfig = this.config;
		try {
			if (ctxConfigs != null && originIndex >= 0 && originIndex < ctxConfigs.size()) {
				final var merged = com.github.akurilov.commons.collection.TreeUtil.reduceForest(
								java.util.Arrays.asList(
												com.github.akurilov.confuse.Config.deepToMap(this.config),
												com.github.akurilov.confuse.Config.deepToMap(ctxConfigs.get(originIndex))));
				effectiveConfig = new com.github.akurilov.confuse.impl.BasicConfig(
								this.config.pathSep(), this.config.schema(), merged);
			}
		} catch (final Exception e) {
			// Fall back to base config if merging fails for any reason
			Loggers.MSG.debug(
							"{}: failed to merge context config for origin index {}; using base config",
							loadStepId(),
							originIndex,
							e);
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
			final Object raw = effectiveConfig.val("load-step-limit-time");
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
						.nodeAddrs(remoteNodeAddrs(config))
						.comment(config.stringVal("run-comment"))
						.runId(runId())
						.opCountLimit(opCountLimit)
						.timeLimitSec(timeLimitSec)
						.build();
		metricsContexts.add(metricsCtx);
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
		if (standaloneDeleteDurationMode()) {
			coordinateDurationStop();
			return;
		}
		stepSlices.stream().filter(s -> s != null).parallel().forEach(stepSlice -> {
			try (final var logCtx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
				stepSlice.shutdown();
			} catch (final RemoteException e) {
				LogUtil.exception(Level.WARN, e, "{}: failed to shutdown the step service {}", loadStepId(), stepSlice);
			}
		});
	}

	private void coordinateDurationStop() {
		final List<LoadStep> activeSlices = stepSlices.stream()
						.filter(slice -> slice != null)
						.collect(Collectors.toList());
		if (activeSlices.isEmpty()) {
			durationAdmissionBarrierSatisfied = true;
			return;
		}
		durationAdmissionBarrierSatisfied = false;
		boolean interrupted = false;
		final DurationPhaseAttempt admissionResult = invokeRetainedDurationPhase(
						"distributed-admission",
						activeSlices,
						LoadStep::closeOperationAdmissionForStepStop,
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
		if (!durationDrainDeadlineSet) {
			// Compatibility for an explicit stop before await() established a scheduled boundary.
			durationDrainDeadlineNanos = durationDrainDeadlineNanos(System.nanoTime());
			durationDrainDeadlineSet = true;
		}
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

	@FunctionalInterface
	private interface StepSlicePhase {
		void execute(LoadStep slice) throws Exception;
	}

	@Override
	public final boolean await(final long timeout, final TimeUnit timeUnit)
					throws IllegalStateException, InterruptedException {
		final var stepSliceCount = stepSlices.size();
		try (final var logCtx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			if (0 == stepSliceCount) {
				throw new IllegalStateException("No step slices are available");
			}
			Loggers.MSG.debug(
							"{}: await for {} step slices for at most {} {}...", loadStepId(), stepSliceCount,
							timeout, timeUnit.name().toLowerCase(Locale.ROOT));
			if (standaloneDeleteDurationMode()) {
				return awaitDurationSlicesAndStop(timeout, timeUnit, stepSliceCount);
			}
			try {
				return stepSlices.stream().filter(s -> s != null).parallel().map(stepSlice -> {
					try {
						final var invokeTimeMillis = System.currentTimeMillis();
						final var timeOutMillis = timeUnit.toMillis(timeout);
						var awaitResult = false;
						while (timeOutMillis > System.currentTimeMillis() - invokeTimeMillis) {
							if (Thread.currentThread().isInterrupted()) {
								throwUnchecked(new InterruptedException());
							}
							awaitResult = stepSlice.await(1, TimeUnit.MILLISECONDS);
							if (awaitResult) { // awaitResult = (0 == countDown)
								break;
							}
						}
						return awaitResult;
					} catch (final InterruptedException e) {
						throwUnchecked(e);
					} catch (final RemoteException e) {
						return false;
					}
					return false;
				}).reduce((flag1, flag2) -> flag1 && flag2).orElse(false);
			} finally {
				Loggers.MSG.info("{}: await for {} step slices done", loadStepId(), stepSliceCount);
				doStop();
			}
		}
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
			stop();
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
		durationDrainDeadlineNanos = durationDrainDeadlineNanos(deadlineNanos);
		durationDrainDeadlineSet = true;
		return awaitDurationSlices(activeSlices, deadlineNanos);
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
					if (probe.failure() != null) {
						throwUnchecked(probe.failure());
					}
					if (probe.exhausted()) {
						throw standaloneDeleteEarlyExhaustionFailure();
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
		if (standaloneDeleteDurationMode()) {
			if (!durationAdmissionBarrierSatisfied || durationStopFailure != null) {
				super.doStop();
				return;
			}
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
				Thread.currentThread().interrupt();
			}
		} else {
			stepSlices.stream().filter(s -> s != null).parallel().forEach(stepSlice -> {
				try (
								final var logCtx = put(KEY_STEP_ID, stepSlice.loadStepId()).put(
												KEY_CLASS_NAME, getClass().getSimpleName())) {
					stepSlice.stop();
				} catch (final Exception e) {
					throwUncheckedIfInterrupted(e);
					LogUtil.trace(Loggers.ERR, Level.WARN, e, "{}: failed to stop the step slice \"{}\"", loadStepId(),
									stepSlice);
				}
			});
		}
		if (null != metricsAggregator) {
			try {
				metricsAggregator.stop();
			} catch (final RemoteException e) {
				LogUtil.trace(
								Loggers.ERR,
								Level.DEBUG,
								e,
								"{}: metrics aggregator stop failed; continuing shutdown",
								loadStepId());
			}
		}
		itemTimingMetricsOutputFileAggregators.parallelStream().forEach(itemMetricsOutputFileAggregator -> {
			try {
				itemMetricsOutputFileAggregator.close();
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				LogUtil.exception(Level.WARN, e, "{}: failed to close the item metrics output file aggregator \"{}\"",
								loadStepId(), itemMetricsOutputFileAggregator);
			}
		});
		itemTimingMetricsOutputFileAggregators.clear();
		super.doStop();
	}

	@Override
	protected final void doClose()
					throws IOException {
		final boolean durationMode = standaloneDeleteDurationMode();
		boolean durationStopRecoordinated = false;
		if (durationMode) {
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
		try (final var logCtx = put(KEY_STEP_ID, loadStepId()).put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			IntegrityTerminalException terminalCause = durationStopFailure;
			durationStopFailure = null;
			boolean durationSlicesClosed = true;
			Map<OpType, Long> terminalFailureCountsByOpType = Map.of();
			if (integrityModeEnabled()) {
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
					try {
						stepSlice.close();
						Loggers.MSG.debug("{}: step slice \"{}\" closed", loadStepId(), stepSlice);
					} catch (final Exception e) {
						throwUncheckedIfInterrupted(e);
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
			}
			for (final var integrityLogFileAggregator : integrityLogFileAggregators) {
				try {
					integrityLogFileAggregator.close();
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
			for (final var itemOutputFileAggregator : itemOutputFileAggregators) {
				try {
					if (itemOutputFileAggregator instanceof CsvArtifactAggregator csvAggregator) {
						csvAggregator.close(terminalFailureCountsByOpType.getOrDefault(
										csvAggregator.artifactOpType(), -1L));
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
			for (final var opTraceLogFileAggregator : opTraceLogFileAggregators) {
				try {
					opTraceLogFileAggregator.close();
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

	@Override
	public final T config(final Map<String, Object> configMap) {
		if (ctxConfigs != null) {
			throw new IllegalStateException("config(...) should be invoked before any append(...) call");
		}
		final var configCopy = (Config) new BasicConfig(config);
		final var argValPairs = (Map<String, String>) new HashMap<String, String>();
		flatten(configMap, argValPairs, config.pathSep(), null);
		final var aliasingConfig = config.<Map<String, Object>> listVal("aliasing");
		try {
			final var aliasedArgs = AliasingUtil.apply(argValPairs, aliasingConfig);
			if (config.boolVal("load-step-idAutoGenerated")) {
				if (aliasedArgs.get("load-step-id") != null) {
					configCopy.val("load-step-idAutoGenerated", false);
				}
			}
			aliasedArgs.forEach(configCopy::val); // merge
		} catch (final Exception e) {
			LogUtil.exception(Level.FATAL, e, "Scenario syntax error");
			throwUnchecked(e);
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
		flatten(context, argValPairs, config.pathSep(), null);
		final var aliasingConfig = config.<Map<String, Object>> listVal("aliasing");
		final var ctxConfig = (Config) new BasicConfig(config);
		try {
			final var aliasedArgs = AliasingUtil.apply(argValPairs, aliasingConfig);
			aliasedArgs.forEach(ctxConfig::val); // merge
		} catch (final Exception e) {
			LogUtil.exception(Level.FATAL, e, "Scenario syntax error");
			throwUnchecked(e);
		}
		ctxConfigsCopy.add(ctxConfig);
		return copyInstance(config, ctxConfigsCopy);
	}

	protected abstract T copyInstance(final Config config, final List<Config> ctxConfigs);
}
