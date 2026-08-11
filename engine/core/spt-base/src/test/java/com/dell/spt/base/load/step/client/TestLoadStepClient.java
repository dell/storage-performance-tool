package com.dell.spt.base.load.step.client;

import com.dell.spt.base.env.Extension;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.github.akurilov.confuse.Config;

import java.rmi.RemoteException;
import java.util.List;

/**
 * Concrete test implementation of LoadStepClientBase for testing purposes.
 * 
 * This provides minimal implementations of abstract methods to enable testing
 * of the base class functionality without complex setup.
 */
public class TestLoadStepClient extends LoadStepClientBase<TestLoadStepClient> {

	private boolean initCalled = false;
	private boolean copyInstanceCalled = false;
	private int copyInstanceCallCount = 0;

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

	@Override
	protected void init() throws IllegalStateException {
		initCalled = true;
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
}
