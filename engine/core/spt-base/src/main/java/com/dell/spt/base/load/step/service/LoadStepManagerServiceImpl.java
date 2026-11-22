package com.dell.spt.base.load.step.service;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static org.apache.logging.log4j.CloseableThreadContext.Instance;

import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.svc.ServiceBase;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.Level;

public final class LoadStepManagerServiceImpl extends ServiceBase
				implements LoadStepManagerService {

	private final List<Extension> extensions;
	private final MetricsManager metricsMgr;
	/** Created and not released, oldest first. Several are normal: a scenario can run steps concurrently. */
	private final List<LoadStepServiceImpl> openStepSvcs = new CopyOnWriteArrayList<>();
	private volatile LoadStepService stepSvc = null;
	private boolean acceptingSteps = true;

	public LoadStepManagerServiceImpl(
					final int port, final List<Extension> extensions, final MetricsManager metricsMgr) {
		super(port);
		this.extensions = extensions;
		this.metricsMgr = metricsMgr;
	}

	@Override
	public final String name() {
		return SVC_NAME;
	}

	@Override
	protected final void doStart() {
		try (final Instance logCtx = CloseableThreadContext.put(KEY_CLASS_NAME, getClass().getSimpleName())) {
			super.doStart();
		}
	}

	/** Closes every open step service, also after another one's close failed, and throws the first failure. */
	@Override
	protected final synchronized void doClose() throws IOException {
		acceptingSteps = false;
		Exception failure = null;
		for (final var openStepSvc : List.copyOf(openStepSvcs)) {
			try {
				openStepSvc.close();
				openStepSvcs.remove(openStepSvc);
			} catch (final IOException | RuntimeException e) {
				if (failure == null) {
					failure = e;
				} else {
					failure.addSuppressed(e);
				}
			}
		}
		if (openStepSvcs.isEmpty()) {
			stepSvc = null;
		}
		if (failure instanceof IOException e) {
			throw e;
		}
		if (failure instanceof RuntimeException e) {
			throw e;
		}
		Loggers.MSG.info("Service \"{}\" closed", SVC_NAME);
	}

	@Override
	public final LoadStepService getStepService() {
		return stepSvc;
	}

	@Override
	public final List<Long> busySliceRunIds() throws RemoteException {
		final List<Long> runIds = new ArrayList<>();
		for (final var openStepSvc : openStepSvcs.reversed()) {
			if (openStepSvc.isRunning()) {
				runIds.add(openStepSvc.sliceRunId());
			}
		}
		return runIds;
	}

	@Override
	public final boolean hasOpenSlices() {
		return !openStepSvcs.isEmpty();
	}

	@Override
	public final boolean stopSlicesOfRun(final long runId) throws RemoteException {
		var matched = false;
		final List<String> stoppedStepIds = new ArrayList<>();
		for (final var openStepSvc : openStepSvcs) {
			if (openStepSvc.sliceRunId() != runId) {
				continue;
			}
			matched = true;
			if (openStepSvc.isRunning()) {
				final var stepId = openStepSvc.sliceStepId();
				try {
					openStepSvc.stop();
					stoppedStepIds.add(stepId);
				} catch (final RuntimeException e) {
					LogUtil.exception(Level.WARN, e, "Failed to stop step \"{}\" of run {}", stepId, runId);
				}
			}
		}
		if (!stoppedStepIds.isEmpty()) {
			Loggers.ERR.warn(
							"DELETE /run stopped this node's share of run {}, step(s) {}; the run itself is stopped on its entry",
							runId,
							stoppedStepIds);
		}
		return matched;
	}

	@Override
	public final synchronized String newStepService(
					final String stepType, final Config config, final List<Config> ctxConfigs)
					throws RemoteException {
		if (!acceptingSteps) {
			throw new RemoteException("Load step manager is closing");
		}
		final var newStepSvc = new LoadStepServiceImpl(
						port, extensions, stepType, config, ctxConfigs, metricsMgr, openStepSvcs::remove);
		openStepSvcs.add(newStepSvc);
		stepSvc = newStepSvc;
		Loggers.MSG.info("New step service started @ port #{}: {}", port, newStepSvc.name());
		return newStepSvc.name();
	}
}
