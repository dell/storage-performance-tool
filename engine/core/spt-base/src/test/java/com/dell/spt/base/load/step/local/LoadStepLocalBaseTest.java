package com.dell.spt.base.load.step.local;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.local.context.LoadStepContext;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.List;
import java.util.Map;
import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Focused tests for LoadStepLocalBase.initMetrics to ensure we surface configuration issues without
 * flagging intentionally unlimited steps.
 */
class LoadStepLocalBaseTest {

	@Test
	void initMetricsStoresValidCountLimitWithoutWarnings() {
		final Config config = baseConfig();
		config.val("load-step-id", "step-valid");
		config.val("run-id", 42L);
		config.val("run-comment", "healthy");
		config.val("load-op-limit-count", 500L);
		config.val("load-step-limit-time", "60s");

		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(config, mockMetricsManager(), mockContext());

		loadStep.initMetricsForTest(defaultMetricsConfig(config));

		final Map<String, Object> metadata = loadStep.latestMetadata();
		assertEquals(500L, metadata.get(MetricsConstants.METADATA_LIMIT_OP_COUNT));
		assertEquals(60L, metadata.get(MetricsConstants.METADATA_LIMIT_TIME_SEC));
	}

	@Test
	void initMetricsWarnsWhenLimitsConfiguredButInvalid() {
		final Config config = baseConfig();
		config.val("load-step-id", "step-invalid");
		config.val("run-id", 99L);
		config.val("run-comment", "bad-limits");

		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(config, mockMetricsManager(), mockContext());
		loadStep.setRawValue("load-op-limit-count", "not-a-number");
		loadStep.setRawValue("load-step-limit-time", "bogus");

		loadStep.initMetricsForTest(defaultMetricsConfig(config));

		final Map<String, Object> metadata = loadStep.latestMetadata();
		assertFalse(metadata.containsKey(MetricsConstants.METADATA_LIMIT_OP_COUNT));
		assertFalse(metadata.containsKey(MetricsConstants.METADATA_LIMIT_TIME_SEC));
	}

	private static Config baseConfig() {
		return TestConfigBuilder.config();
	}

	private static Config defaultMetricsConfig(final Config config) {
		return config.configVal("output").configVal("metrics");
	}

	private static MetricsManager mockMetricsManager() {
		return mock(MetricsManager.class);
	}

	@SuppressWarnings({"unchecked", "rawtypes"
	})
	private static LoadStepContext mockContext() {
		final LoadStepContext ctx = mock(LoadStepContext.class);
		when(ctx.activeOpCount()).thenReturn(0);
		return ctx;
	}

	@Test
	void doStartWrappedRemovesFailingContext() throws RemoteException {
		final Config config = baseConfig();
		config.val("load-step-id", "step-start");

		final LoadStepContext failing = mock(LoadStepContext.class);
		final LoadStepContext healthy = mock(LoadStepContext.class);
		when(failing.start()).thenThrow(new RemoteException("boom"));

		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(config, mockMetricsManager(), failing, healthy);

		assertDoesNotThrow(loadStep::startContextsForTest);
		assertEquals(1, loadStep.contextCount());
		assertSame(healthy, loadStep.contextAt(0));
		verify(failing).start();
		verify(healthy).start();
	}

	@Test
	void doStartWrappedThrowsWhenAllContextsFail() throws RemoteException {
		final Config config = baseConfig();
		config.val("load-step-id", "step-none");

		final LoadStepContext failing = mock(LoadStepContext.class);
		when(failing.start()).thenThrow(new RemoteException("boom"));

		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(config, mockMetricsManager(), failing);

		assertThrows(IllegalStateException.class, loadStep::startContextsForTest);
		assertEquals(0, loadStep.contextCount());
		verify(failing).start();
	}

	@Test
	void doShutdownRemovesFailingContext() throws RemoteException {
		final Config config = baseConfig();
		config.val("load-step-id", "step-shutdown");

		final LoadStepContext failing = mock(LoadStepContext.class);
		final LoadStepContext healthy = mock(LoadStepContext.class);
		doThrow(new RemoteException("boom")).when(failing).shutdown();

		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(config, mockMetricsManager(), failing, healthy);

		loadStep.shutdownContextsForTest();
		assertEquals(1, loadStep.contextCount());
		assertSame(healthy, loadStep.contextAt(0));
		verify(failing).shutdown();
		verify(healthy).shutdown();
	}

	@Test
	void awaitRemovesContextOnRemoteFailure() throws Exception {
		final Config config = baseConfig();
		config.val("load-step-id", "step-await");

		final LoadStepContext failing = mock(LoadStepContext.class);
		when(failing.isDone()).thenReturn(false);
		when(failing.await(anyLong(), any(TimeUnit.class))).thenThrow(new RemoteException("boom"));

		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(config, mockMetricsManager(), failing);

		assertTrue(loadStep.awaitContextsForTest(5, TimeUnit.SECONDS));
		assertEquals(0, loadStep.contextCount());
		verify(failing).await(anyLong(), eq(TimeUnit.NANOSECONDS));
	}

	private static final class TestLoadStepLocalBase extends LoadStepLocalBase {

		TestLoadStepLocalBase(
						final Config baseConfig,
						final MetricsManager metricsManager,
						final LoadStepContext... stepContexts) {
			super(baseConfig, List.<Extension> of(), List.<Config> of(), metricsManager);
			this.stepContexts.addAll(List.of(stepContexts));
		}

		@Override
		public String getTypeName() {
			return "test-load-step";
		}

		@Override
		protected void init() {
			// not needed for these tests
		}

		void initMetricsForTest(final Config metricsConfig) {
			initMetrics(0, OpType.CREATE, 1, metricsConfig, new SizeInBytes(1L), false);
		}

		void setRawValue(final String path, final Object value) {
			final int sep = path.lastIndexOf('-');
			if (sep < 0) {
				config.mapVal(com.github.akurilov.confuse.Config.ROOT_PATH).put(path, value);
				return;
			}
			final String parentPath = path.substring(0, sep);
			final String leafKey = path.substring(sep + 1);
			config.mapVal(parentPath).put(leafKey, value);
		}

		Map<String, Object> latestMetadata() {
			return metricsContexts.get(metricsContexts.size() - 1).metadata();
		}

		void startContextsForTest() {
			doStartWrapped();
		}

		void shutdownContextsForTest() {
			doShutdown();
		}

		boolean awaitContextsForTest(final long timeout, final TimeUnit unit) {
			return await(timeout, unit);
		}

		int contextCount() {
			return stepContexts.size();
		}

		LoadStepContext contextAt(final int index) {
			return stepContexts.get(index);
		}
	}

}
