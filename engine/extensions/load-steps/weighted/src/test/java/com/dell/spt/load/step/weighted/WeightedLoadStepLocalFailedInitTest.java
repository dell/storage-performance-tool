package com.dell.spt.load.step.weighted;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.Constants;
import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMock;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * A context whose init fails closes its own driver, and the contexts built before it, before the
 * step itself is closed; the step's close then finds them closed.
 */
@SuppressWarnings({"rawtypes", "unchecked"
})
class WeightedLoadStepLocalFailedInitTest {

	private final List<DummyStorageDriverMock> drivers = new ArrayList<>();

	private StorageDriverFactory factory() throws Exception {
		final StorageDriverFactory factory = mock(StorageDriverFactory.class);
		when(factory.id()).thenReturn("stub");
		when(factory.create(anyString(), any(), any(), anyBoolean(), anyInt())).thenAnswer(call -> {
			final var driver = new DummyStorageDriverMock((Config) call.getArgument(2));
			drivers.add(driver);
			return driver;
		});
		return factory;
	}

	@Test
	void theContextThatFailsClosesItsDriverAndTheEarlierOne() throws Exception {
		final var baseSchema = SchemaProvider.resolveAndReduce(Constants.APP_NAME, getClass().getClassLoader());
		final var schema = TreeUtil.reduceForest(List.of(baseSchema, new WeightedLoadStepExtension().schemaProvider().schema()));
		final Config config;
		try (var defaults = getClass().getResourceAsStream("/config/defaults.yaml")) {
			config = ConfigUtil.loadConfig(new String(defaults.readAllBytes(), StandardCharsets.UTF_8), ConfigFormat.YAML, schema);
		}
		config.val("load-step-id", "failed-init-" + UUID.randomUUID());
		config.val("storage-driver-type", "stub");
		config.val("load-op-weight", 1);
		final Config good = new BasicConfig(config.pathSep(), schema, Map.of());
		final Config refused = new BasicConfig(config.pathSep(), schema, Map.of(
						"load", Map.of("op", Map.of("retry", true, "retryLimit", -1))));
		final var step = new WeightedLoadStepLocal(config, List.of(factory()), List.of(good, refused), mock(MetricsManager.class));
		try {
			final var failure = assertThrows(IllegalStateException.class, step::init);

			assertInstanceOf(IllegalConfigurationException.class, failure.getCause());
			assertTrue(drivers.size() == 2, () -> "drivers built: " + drivers.size());
			assertTrue(drivers.get(0).isClosed(), "a failed init also closes the earlier context");
			assertTrue(drivers.get(1).isClosed(), "nothing else would close this driver");
		} finally {
			step.close();
		}
		assertTrue(drivers.get(0).isClosed(), "the step's close leaves it closed");
	}
}
