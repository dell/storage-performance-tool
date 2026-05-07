package com.dell.spt.base.load.step.linear;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension;
import com.github.akurilov.confuse.Config;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.jupiter.api.Test;

class LinearLoadStepLocalTimingPersistTest {

	@Test
	void initDoesNotCreateTimingMetricsFileWhenTimingPersistDisabled() throws Exception {
		final String stepId = "timing-disabled-" + System.nanoTime();
		final Path timingPath = Paths.get(System.getProperty("java.io.tmpdir"), "spt", "timingMetrics_" + stepId);
		Files.deleteIfExists(timingPath);
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", stepId);
		config.val("output-metrics-timing-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-data-ranges-concat", null);
		final Path itemInput = Files.createTempFile("spt-items-", ".csv");
		config.val("item-input-file", itemInput.toString());
		final ProbeLinearLoadStepLocal loadStep = new ProbeLinearLoadStepLocal(
						config,
						List.of(new DummyStorageDriverMockExtension<>()),
						mock(MetricsManager.class));

		try {
			assertDoesNotThrow(loadStep::initForTest);
			assertFalse(Files.exists(timingPath));
		} finally {
			assertDoesNotThrow(loadStep::close);
			Files.deleteIfExists(itemInput);
			Files.deleteIfExists(timingPath);
		}
	}

	private static final class ProbeLinearLoadStepLocal extends LinearLoadStepLocal {
		ProbeLinearLoadStepLocal(
						final Config baseConfig,
						final List<Extension> extensions,
						final MetricsManager metricsManager) {
			super(baseConfig, extensions, List.of(), metricsManager);
		}

		void initForTest() {
			init();
		}
	}
}
