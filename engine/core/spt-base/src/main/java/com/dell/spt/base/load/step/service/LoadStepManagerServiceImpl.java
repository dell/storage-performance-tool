package com.dell.spt.base.load.step.service;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static org.apache.logging.log4j.CloseableThreadContext.Instance;

import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.svc.ServiceBase;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;
import org.apache.logging.log4j.CloseableThreadContext;

public final class LoadStepManagerServiceImpl extends ServiceBase
				implements LoadStepManagerService {

	private final List<Extension> extensions;
	private final MetricsManager metricsMgr;
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

	@Override
	protected final synchronized void doClose() throws IOException {
		acceptingSteps = false;
		final var activeStepSvc = stepSvc;
		if (activeStepSvc != null) {
			activeStepSvc.close();
			stepSvc = null;
		}
		Loggers.MSG.info("Service \"{}\" closed", SVC_NAME);
	}

	@Override
	public final LoadStepService getStepService() {
		return stepSvc;
	}

	@Override
	public final synchronized String newStepService(
					final String stepType, final Config config, final List<Config> ctxConfigs)
					throws RemoteException {
		if (!acceptingSteps) {
			throw new RemoteException("Load step manager is closing");
		}
		stepSvc = new LoadStepServiceImpl(port, extensions, stepType, config, ctxConfigs, metricsMgr);
		Loggers.MSG.info("New step service started @ port #{}: {}", port, stepSvc.name());
		return stepSvc.name();
	}
}
