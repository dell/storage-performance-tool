package com.dell.spt.base.load.step.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.ArgumentMatchers.any;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.bucket.BucketRecorder;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.storage.driver.bucket.TargetBucketSet;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class LoadStepBucketMetricsSetupTest {

	private static final class SetupStep extends LoadStepLocalBase {
		SetupStep(final Config config, final MetricsManager metricsManager) {
			super(config, List.<Extension> of(), List.<Config> of(), metricsManager);
		}

		@Override
		public String getTypeName() {
			return "bucket-metrics-setup";
		}

		@Override
		protected void init() {}

		BucketRecorder recorder(final Config config, final Map<OpType, MetricsContext> byType) {
			return initBucketMetrics(config, context(OpType.CREATE), byType);
		}

		int metersBuilt() {
			return bucketMeters().size();
		}
	}

	private static MetricsContext context(final OpType type) {
		return MetricsContextImpl.builder()
						.loadStepId("setup")
						.runId(1L)
						.opType(type)
						.concurrencyLimit(1)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(1))
						.stdOutColorFlag(false)
						.outputPeriodSec(0)
						.comment("")
						.actualConcurrencyGauge(() -> 0)
						.build();
	}

	private static Config config(final int count) {
		final Config config = TestConfigBuilder.config();
		config.val("item-output-path", "/bench");
		config.val(TargetBucketSet.COUNT, count);
		return config;
	}

	@Test
	void shippedDefaultIsOnAndBothSpellingsReachOneSetting() {
		final Config config = TestConfigBuilder.config();
		assertTrue(config.boolVal("output-metrics-bucket-enabled"));
		config.val("output-metrics-bucket-enabled", false);
		assertEquals(false, config.configVal("output").configVal("metrics").configVal("bucket").boolVal("enabled"));
	}

	@Test
	void aSingleBucketAllocatesNothing() {
		final var step = new SetupStep(config(1), mock(MetricsManager.class));
		assertNull(step.recorder(config(1), null));
		assertEquals(0, step.metersBuilt());
	}

	@Test
	void aOneNameListAllocatesNothing() {
		final Config config = config(1);
		config.val(TargetBucketSet.NAMES, List.of("only"));
		final var step = new SetupStep(config, mock(MetricsManager.class));
		assertNull(step.recorder(config, null));
		assertEquals(0, step.metersBuilt());
	}

	@Test
	void theOffSwitchAllocatesNothing() {
		final Config config = config(4);
		config.val("output-metrics-bucket-enabled", false);
		final var step = new SetupStep(config, mock(MetricsManager.class));
		assertNull(step.recorder(config, null));
		assertEquals(0, step.metersBuilt());
	}

	@Test
	void aSetBuildsOneBlockForEachContext() {
		final Config config = config(4);
		final var step = new SetupStep(config, mock(MetricsManager.class));
		assertNotNull(step.recorder(config, null));
		assertEquals(1, step.metersBuilt());
		assertEquals(4, step.bucketMeters().get(0).memberCount());
	}

	@Test
	void aMixedStepBuildsOneBlockForEachOperationType() {
		final Config config = config(4);
		final var step = new SetupStep(config, mock(MetricsManager.class));
		final Map<OpType, MetricsContext> byType = new EnumMap<>(OpType.class);
		byType.put(OpType.READ, context(OpType.READ));
		byType.put(OpType.DELETE, context(OpType.DELETE));
		assertNotNull(step.recorder(config, byType));
		// The primary and the two types: a step's primary is normally also one of its types.
		assertEquals(3, step.metersBuilt());
	}

	@Test
	void bucketMetersNeverReachTheMetricsManager() {
		final Config config = config(1000);
		final MetricsManager manager = mock(MetricsManager.class);
		final var step = new SetupStep(config, manager);
		assertNotNull(step.recorder(config, null));
		assertEquals(1000, step.bucketMeters().get(0).memberCount());
		verify(manager, never()).register(any());
	}
}
