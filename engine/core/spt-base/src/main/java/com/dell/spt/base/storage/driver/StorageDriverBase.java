package com.dell.spt.base.storage.driver;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;

import com.dell.spt.base.concurrent.DaemonBase;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.integrity.DigestResult;
import com.dell.spt.base.integrity.IntegrityConfig;
import com.dell.spt.base.integrity.IntegrityCsvArtifacts;
import com.dell.spt.base.integrity.IntegrityPerformanceAccumulator;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.integrity.IntegrityVerificationResult;
import com.dell.spt.base.integrity.StreamingSha256;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.concurrent.ThreadUtil;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.logging.log4j.CloseableThreadContext;

/** Created by kurila on 11.07.16. */
public abstract class StorageDriverBase<I extends Item, O extends Operation<I>> extends DaemonBase
				implements StorageDriver<I, O> {

	private final DataInput itemDataInput;
	protected final String stepId;
	private Output<O> opResultOut = null;
	protected final int concurrencyLimit;
	protected final int ioWorkerCount;
	protected final String namespace;
	private final String driverType;
	protected final Credential credential;
	protected final boolean verifyFlag;
	protected final IntegrityConfig integrityConfig;
	protected final IntegrityPerformanceAccumulator integrityPerformance;
	private final StreamingSha256 integrityHasher;
	private final AtomicReference<String> integrityPhase = new AtomicReference<>();
	private final AtomicReference<IntegrityTerminalException> terminalFailure = new AtomicReference<>();
	private OperationLifecycleTracker<O> operationLifecycle = OperationLifecycleTracker.disabled();

	protected final ConcurrentMap<String, Credential> pathToCredMap = new ConcurrentHashMap<>(1);

	private final ConcurrentMap<String, String> pathMap = new ConcurrentHashMap<>(1);
	protected Function<String, String> requestNewPathFunc = this::requestNewPath;

	protected final ConcurrentMap<Credential, String> authTokens = new ConcurrentHashMap<>(1);
	protected Function<Credential, String> requestAuthTokenFunc = this::requestNewAuthToken;

	protected StorageDriverBase(
					final String stepId,
					final DataInput itemDataInput,
					final Config storageConfig,
					final boolean verifyFlag)
					throws IllegalConfigurationException {

		this.itemDataInput = itemDataInput;
		final var driverConfig = storageConfig.configVal("driver");
		final var limitConfig = driverConfig.configVal("limit");
		this.stepId = stepId;
		this.namespace = storageConfig.stringVal("namespace");
		this.driverType = storageConfig.stringVal("driver-type");
		final var authConfig = storageConfig.configVal("auth");
		this.credential = Credential.getInstance(authConfig.stringVal("uid"), authConfig.stringVal("secret"));
		final var authToken = authConfig.stringVal("token");
		if (authToken != null) {
			if (this.credential == null) {
				this.authTokens.put(Credential.NONE, authToken);
			} else {
				this.authTokens.put(credential, authToken);
			}
		}
		this.concurrencyLimit = limitConfig.intVal("concurrency");
		this.verifyFlag = verifyFlag;
		this.integrityConfig = IntegrityConfig.fromStorage(storageConfig);
		if (integrityConfig.enabled()) {
			IntegrityConfig.requireSupportedDriver(driverType);
		}

		final var confWorkerCount = driverConfig.intVal("threads");
		if (confWorkerCount > 0) {
			ioWorkerCount = confWorkerCount;
		} else if (concurrencyLimit > 0) {
			ioWorkerCount = Math.min(concurrencyLimit, ThreadUtil.getHardwareThreadCount());
		} else {
			ioWorkerCount = ThreadUtil.getHardwareThreadCount();
		}
		if (integrityConfig.enabled()) {
			integrityPerformance = new IntegrityPerformanceAccumulator();
			integrityHasher = new StreamingSha256(Math.max(1, ioWorkerCount));
		} else {
			integrityPerformance = null;
			integrityHasher = null;
		}
	}

	@Override
	public final void operationResultOutput(final Output<O> opResultOut) {
		this.opResultOut = opResultOut;
	}

	protected abstract String requestNewPath(final String path);

	/**
	 * Return the resource identity used to cache path initialization.
	 *
	 * <p>Drivers whose initialization operates on a parent resource may normalize the operation
	 * destination path. The default preserves the historical full-path behavior.
	 */
	protected String requestNewPathCacheKey(final String path) {
		return path;
	}

	protected abstract String requestNewAuthToken(final Credential credential);

	protected boolean prepare(final O op) {
		op.reset();
		if (op instanceof DataOperation<?> dataOperation) {
			dataOperation.item().dataInput(itemDataInput);
			prepareIntegrity(op, dataOperation);
		} else if (integrityMetadataEnabled() && op.type() == OpType.UPDATE) {
			throw unsupportedIntegrityCombination("UPDATE operations are outside integrity metadata v1");
		}
		final String dstPath = op.dstPath();
		final String pathCacheKey = dstPath == null || dstPath.isEmpty()
						? ""
						: requestNewPathCacheKey(dstPath);
		final Credential credential = op.credential();
		if (credential != null) {
			pathToCredMap.putIfAbsent(pathCacheKey, credential);
			if (requestAuthTokenFunc != null) {
				authTokens.computeIfAbsent(credential, requestAuthTokenFunc);
			}
		}
		if (requestNewPathFunc != null) {
			// NOTE: in the distributed mode null dstPath becomes empty one
			if (dstPath != null && !dstPath.isEmpty()) {
				if (null == pathMap.computeIfAbsent(pathCacheKey, requestNewPathFunc)) {
					Loggers.ERR.debug("Failed to compute the destination path for the operation: {}", op);
					op.status(Operation.Status.FAIL_UNKNOWN);
					// return false;
				}
			}
		}
		return true;
	}

	private void prepareIntegrity(final O op, final DataOperation<?> dataOperation) {
		if (!integrityMetadataEnabled()) {
			return;
		}
		// Multipart parts are transport fragments. V1 metadata and performance accounting describe
		// the complete logical object and are prepared once on the parent operation.
		if (op instanceof PartialDataOperation<?>) {
			return;
		}
		switch (op.type()) {
		case CREATE:
			selectIntegrityPhase("write_prehash");
			if (op.srcPath() != null && !op.srcPath().isEmpty()) {
				throw unsupportedIntegrityCombination("copy CREATE operations are outside integrity metadata v1");
			}
			if (dataOperation.srcItemsToConcat() != null
							&& !dataOperation.srcItemsToConcat().isEmpty()) {
				throw unsupportedIntegrityCombination("concatenated CREATE operations are outside integrity metadata v1");
			}
			if (op.integrityMetadata() == null) {
				integrityPerformance.markPrehashStarted(System.nanoTime());
				try {
					final DigestResult result = integrityHasher.hash(dataOperation.item());
					op.integrityMetadata(result.metadata());
					integrityPerformance.recordDigest(result.metadata().size(), result.workerNanos());
					if (integrityAdditionalPayloadPassRequired(op)) {
						integrityPerformance.recordAdditionalPayloadPass();
					}
				} catch (final IOException e) {
					if (e instanceof StreamingSha256.DigestFailureException digestFailure) {
						integrityPerformance.recordDigest(
										digestFailure.processedBytes(), digestFailure.workerNanos());
					} else {
						integrityPerformance.recordDigest(0, 0);
					}
					throw new IntegrityTerminalException(
									IntegrityTerminalException.Category.EXECUTION,
									"failed to pre-hash CREATE object " + dataOperation.item().name(),
									e);
				}
			}
			break;
		case READ:
			if (dataOperation instanceof CompositeDataOperation<?>) {
				throw unsupportedIntegrityCombination(
								"parallel/composite READ cannot use whole-object integrity metadata v1");
			}
			try {
				integrityConfig.requireReadProvenance();
			} catch (final IllegalConfigurationException e) {
				throw new IntegrityTerminalException(
								IntegrityTerminalException.Category.CONFIGURATION, e.getMessage(), e);
			}
			if (dataOperation.randomRangesCount() > 0
							|| (dataOperation.fixedRanges() != null && !dataOperation.fixedRanges().isEmpty())) {
				throw unsupportedIntegrityCombination("range READ cannot use whole-object integrity metadata v1");
			}
			break;
		case UPDATE:
			throw unsupportedIntegrityCombination("UPDATE operations are outside integrity metadata v1");
		default:
			break;
		}
	}

	/**
	 * Reports whether transport processing performs one additional logical-object payload pass
	 * beyond the metadata prehash. Called only for the first parent CREATE preparation.
	 */
	protected boolean integrityAdditionalPayloadPassRequired(final O op) {
		return false;
	}

	private static IntegrityTerminalException unsupportedIntegrityCombination(final String message) {
		return new IntegrityTerminalException(
						IntegrityTerminalException.Category.CONFIGURATION, message);
	}

	protected final boolean integrityMetadataEnabled() {
		return integrityConfig != null && integrityConfig.enabled();
	}

	protected final void markIntegrityRequestDispatched() {
		if (integrityPerformance != null) {
			integrityPerformance.markFirstRequestDispatched(System.nanoTime());
		}
	}

	private void selectIntegrityPhase(final String phase) {
		final String selected = integrityPhase.updateAndGet(current -> current == null ? phase : current);
		if (!phase.equals(selected)) {
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.CONFIGURATION,
							"one integrity-enabled driver instance cannot mix " + selected + " and " + phase);
		}
	}

	protected final void recordIntegrityReadResult(final IntegrityVerificationResult result) {
		if (integrityPerformance != null && result != null) {
			selectIntegrityPhase("read_verify");
			integrityPerformance.recordDigest(result.actualSize(), result.workerNanos());
		}
	}

	protected final IntegrityPerformanceAccumulator.Snapshot integrityPerformanceSnapshot() {
		return integrityPerformance == null ? null : integrityPerformance.snapshot();
	}

	@Override
	public final OperationLifecycleTracker<O> operationLifecycle() {
		return operationLifecycle == null ? OperationLifecycleTracker.disabled() : operationLifecycle;
	}

	/** Activates lifecycle-only draining for a driver base which publishes every ownership edge. */
	protected final void enableOperationLifecycle() {
		if (operationLifecycle == null || !operationLifecycle.isEnabled()) {
			operationLifecycle = new OperationLifecycleTracker<>();
		}
	}

	/** Records the exact boundary at which transport execution begins. */
	protected final boolean markOperationDispatched(final O op) {
		return operationLifecycle().explicitlyDispatched(op);
	}

	@Override
	public final String driverType() {
		return driverType;
	}

	@Override
	public final boolean metadataIntegrityEnabled() {
		return integrityMetadataEnabled();
	}

	@Override
	public final IntegrityTerminalException terminalFailure() {
		return terminalFailure.get();
	}

	/** Publishes a failure detected by an asynchronous response handler to the step lifecycle. */
	protected final void recordTerminalFailure(final IntegrityTerminalException failure) {
		if (failure != null) {
			terminalFailure.compareAndSet(null, failure.withStepId(stepId));
		}
	}

	protected final int integrityDigestWorkerCount() {
		return integrityHasher == null ? 0 : integrityHasher.workerCount();
	}

	protected boolean handleCompleted(final O op) {
		// Synchronous/custom drivers may complete inside submit(). If they did not use the
		// explicit dispatch hook, completion itself proves transport execution began.
		final var lifecycle = operationLifecycle();
		lifecycle.dispatched(op);
		if (isStopped() || !lifecycle.completionStarted(op)) {
			return false;
		}
		var compatibilityTerminalCommitted = false;
		try {
			if (Loggers.MSG.isTraceEnabled()) {
				Loggers.MSG.trace("{}: Load operation completed", op);
			}
			final var terminalStatus = op.status();
			@SuppressWarnings("unchecked")
			final O opResult = (O) op.result();
			if (!lifecycle.isOperationLifecycleTracked(op)) {
				// Legacy operation implementations may return and synchronously recycle the
				// same instance. Commit that compatibility circulation before output so its
				// weak sidecar can advance independently; tracked operations retain the
				// stronger output-before-terminal ordering below.
				compatibilityTerminalCommitted = lifecycle.terminal(op, terminalStatus);
				if (!compatibilityTerminalCommitted) {
					return false;
				}
			}
			if (opResultOut.put(opResult)) {
				// Output acceptance linearizes before the terminal transition. The operation
				// remains in flight while arbitrary output code runs, so a bounded-drain
				// deadline may still classify it unresolved; a late output return then cannot
				// mutate either that outcome or a reset run's counters.
				return compatibilityTerminalCommitted || lifecycle.terminal(op, terminalStatus);
			}
			Loggers.ERR.debug(
							"{}: Load operations results output rejected a terminal snapshot",
							toString());
			if (!compatibilityTerminalCommitted) {
				lifecycle.unresolved(op);
			}
			return false;
		} catch (final RuntimeException e) {
			if (!compatibilityTerminalCommitted) {
				lifecycle.unresolved(op);
			}
			final var terminal = IntegrityTerminalException.find(e);
			if (terminal != null) {
				recordTerminalFailure(terminal);
				throw terminal;
			}
			throw e;
		}
	}

	@Override
	public final int concurrencyLimit() {
		return concurrencyLimit;
	}

	@Override
	public Input<O> getInput() {
		throw new AssertionError("Shouldn't be invoked");
	}

	@Override
	protected void doClose() throws IOException, IllegalStateException {
		try (final CloseableThreadContext.Instance logCtx = CloseableThreadContext.put(KEY_STEP_ID, stepId)
						.put(KEY_CLASS_NAME, StorageDriverBase.class.getSimpleName())) {
			emitIntegrityPerformance();
			if (integrityHasher != null) {
				integrityHasher.close();
			}
			itemDataInput.close();
			authTokens.clear();
			pathToCredMap.clear();
			pathMap.clear();
			super.doClose();
			Loggers.MSG.debug("{}: closed", toString());
		}
		opResultOut = null;
	}

	private void emitIntegrityPerformance() {
		if (integrityPerformance == null) {
			return;
		}
		final var snapshot = integrityPerformance.snapshot();
		final String phase = integrityPhase.get();
		if (phase != null && snapshot.objects() > 0) {
			Loggers.INTEGRITY_PERFORMANCE.info(
							IntegrityCsvArtifacts.performanceRecord(
											IntegrityCsvArtifacts.nodeIdentity(), stepId, driverType, phase, snapshot));
		}
	}

	@Override
	public String toString() {
		return "storage/driver/" + concurrencyLimit + "/%s/" + hashCode();
	}
}
