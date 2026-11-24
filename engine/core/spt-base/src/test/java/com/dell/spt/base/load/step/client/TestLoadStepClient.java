package com.dell.spt.base.load.step.client;

import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.LoadStepManagerService;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.github.akurilov.confuse.Config;

import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;

/**
 * Concrete test implementation of LoadStepClientBase for testing purposes.
 * 
 * This provides minimal implementations of abstract methods to enable testing
 * of the base class functionality without complex setup.
 */
public class TestLoadStepClient extends LoadStepClientBase<TestLoadStepClient> {

	private boolean initCalled = false;
	private int initCallCount = 0;
	private boolean copyInstanceCalled = false;
	private int copyInstanceCallCount = 0;
	private Map<String, FileManager> nodeFileManagers = null;
	private Map<String, LoadStepManagerService> nodeStepManagers = null;

	public TestLoadStepClient(
					Config config,
					List<Extension> extensions,
					List<Config> ctxConfigs,
					MetricsManager metricsMgr) {
		super(config, extensions, ctxConfigs, metricsMgr);
	}

	@Override
	protected TestLoadStepClient copyInstance(
					Config config,
					List<Config> ctxConfigs) {
		copyInstanceCalled = true;
		copyInstanceCallCount++;

		// Return a new instance with the updated config
		return new TestLoadStepClient(config, extensions, ctxConfigs, metricsMgr);
	}

	/** Gives the remote nodes these file managers, by node address, instead of resolving them. */
	public TestLoadStepClient withNodeFileManagers(final Map<String, FileManager> fileManagers) {
		nodeFileManagers = Map.copyOf(fileManagers);
		return this;
	}

	@Override
	FileManager resolveFileManager(final String nodeAddr) {
		return nodeFileManagers == null ? super.resolveFileManager(nodeAddr) : nodeFileManagers.get(nodeAddr);
	}

	/** Gives the remote nodes these step managers, by node address, instead of resolving them. */
	public TestLoadStepClient withNodeStepManagers(final Map<String, LoadStepManagerService> stepManagers) {
		nodeStepManagers = Map.copyOf(stepManagers);
		return this;
	}

	/**
	 * The given step manager; with given file managers only, one of an older node, which does not
	 * prepare a step's files, so that its files are refused as before.
	 */
	@Override
	LoadStepManagerService resolveStepManager(final String nodeAddr) throws Exception {
		if (nodeStepManagers != null) {
			return nodeStepManagers.get(nodeAddr);
		}
		if (nodeFileManagers != null) {
			return Mockito.mock(LoadStepManagerService.class, Mockito.CALLS_REAL_METHODS);
		}
		return super.resolveStepManager(nodeAddr);
	}

	@Override
	protected void init() throws IllegalStateException {
		initCalled = true;
		initCallCount++;
		// Minimal initialization for testing - no actual initialization needed
	}

	@Override
	public String getTypeName() throws RemoteException {
		return "test-load-step";
	}

	// Test helper methods to verify behavior

	public boolean wasInitCalled() {
		return initCalled;
	}

	public int getInitCallCount() {
		return initCallCount;
	}

	public boolean wasCopyInstanceCalled() {
		return copyInstanceCalled;
	}

	public int getCopyInstanceCallCount() {
		return copyInstanceCallCount;
	}

	public void resetCallTracking() {
		initCalled = false;
		copyInstanceCalled = false;
		copyInstanceCallCount = 0;
	}

	void addMetricsContextForTest(final MetricsContext<? extends AllMetricsSnapshot> context) {
		metricsContexts.add(context);
	}

	// Expose effective config and ctx config count for targeted assertions
	public Config getEffectiveConfig() {
		return this.config;
	}

	public int getCtxConfigsCount() {
		return this.ctxConfigs == null ? 0 : this.ctxConfigs.size();
	}

	public List<Config> getCtxConfigs() {
		return this.ctxConfigs;
	}
}
