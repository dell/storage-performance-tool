package com.dell.spt.base.storage.driver;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.KEY_STEP_ID;

import com.dell.spt.base.concurrent.DaemonBase;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.concurrent.ThreadUtil;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
	protected final Credential credential;
	protected final boolean verifyFlag;

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

		final var confWorkerCount = driverConfig.intVal("threads");
		if (confWorkerCount > 0) {
			ioWorkerCount = confWorkerCount;
		} else if (concurrencyLimit > 0) {
			ioWorkerCount = Math.min(concurrencyLimit, ThreadUtil.getHardwareThreadCount());
		} else {
			ioWorkerCount = ThreadUtil.getHardwareThreadCount();
		}
	}

	public final void operationResultOutput(final Output<O> opResultOut) {
		this.opResultOut = opResultOut;
	}

	protected abstract String requestNewPath(final String path);

	protected abstract String requestNewAuthToken(final Credential credential);

	protected boolean prepare(final O op) {
		op.reset();
		if (op instanceof DataOperation) {
			((DataOperation) op).item().dataInput(itemDataInput);
		}
		final String dstPath = op.dstPath();
		final Credential credential = op.credential();
		if (credential != null) {
			pathToCredMap.putIfAbsent(dstPath == null ? "" : dstPath, credential);
			if (requestAuthTokenFunc != null) {
				authTokens.computeIfAbsent(credential, requestAuthTokenFunc);
			}
		}
		if (requestNewPathFunc != null) {
			// NOTE: in the distributed mode null dstPath becomes empty one
			if (dstPath != null && !dstPath.isEmpty()) {
				if (null == pathMap.computeIfAbsent(dstPath, requestNewPathFunc)) {
					Loggers.ERR.debug("Failed to compute the destination path for the operation: {}", op);
					op.status(Operation.Status.FAIL_UNKNOWN);
					// return false;
				}
			}
		}
		return true;
	}

	protected boolean handleCompleted(final O op) {
		if (isStopped()) {
			return false;
		} else {
			if (Loggers.MSG.isTraceEnabled()) {
				Loggers.MSG.trace("{}: Load operation completed", op);
			}
			final O opResult = op.result();
			if (opResultOut.put(opResult)) {
				return true;
			} else {
				Loggers.ERR.error(
								"{}: Load operations results queue overflow, dropping the result", toString());
				return false;
			}
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
			itemDataInput.close();
			authTokens.clear();
			pathToCredMap.clear();
			pathMap.clear();
			super.doClose();
			Loggers.MSG.debug("{}: closed", toString());
		}
		opResultOut = null;
	}

	@Override
	public String toString() {
		return "storage/driver/" + concurrencyLimit + "/%s/" + hashCode();
	}
}
