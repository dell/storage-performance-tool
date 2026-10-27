package com.dell.spt.base.load.step.linear;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.generator.PacedRateThrottle;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;

/** A rate-limited linear step must pace with {@link PacedRateThrottle}, never the bursting library throttle. */
class LinearLoadStepLocalRateLimitTest {

	@Test
	void rateLimitedStepPacesWithPacedRateThrottle() throws Exception {
		final Object[] throttles = throttlesOfStepWithRate(100);
		assertEquals(1, throttles.length);
		assertInstanceOf(PacedRateThrottle.class, throttles[0]);
	}

	@Test
	void unlimitedStepConstructsNoThrottle() throws Exception {
		assertEquals(0, throttlesOfStepWithRate(0).length);
	}

	private static Object[] throttlesOfStepWithRate(final double rate) throws Exception {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "rate-limit-" + System.nanoTime());
		config.val("load-op-limit-rate", rate);
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
			return loadStep.throttles();
		} finally {
			assertDoesNotThrow(loadStep::close);
			Files.deleteIfExists(itemInput);
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

		Object[] throttles() throws ReflectiveOperationException {
			final Object generator = field(stepContexts.get(0), "generator");
			return (Object[]) field(generator, "throttles");
		}

		private static Object field(final Object owner, final String name) throws ReflectiveOperationException {
			final Field field = owner.getClass().getDeclaredField(name);
			field.setAccessible(true);
			return field.get(owner);
		}
	}
}
