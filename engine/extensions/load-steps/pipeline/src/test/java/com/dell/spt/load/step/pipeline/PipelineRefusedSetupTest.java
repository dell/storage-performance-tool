package com.dell.spt.load.step.pipeline;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import com.dell.spt.base.storage.driver.CompositeOperationsUnsupported;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMock;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * A pipeline step whose second context is refused at setup closes the driver it created for that
 * context, and the driver and generator of the first context, before the step itself is closed.
 */
@SuppressWarnings({"rawtypes", "unchecked"
})
class PipelineRefusedSetupTest {

	private final List<StorageDriver> created = Collections.synchronizedList(new ArrayList<>());
	private final Set<StorageDriver> closed = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));
	/** Thrown by the close of the second driver built, the refused context's, when set. */
	private volatile Error closeError;

	@Test
	void aRefusalInTheSecondContextClosesBothContextsDrivers() throws Exception {
		final var step = refusedStep();
		try {
			final var failure = assertThrows(IllegalStateException.class, step::init);
			assertInstanceOf(IllegalConfigurationException.class, failure.getCause());
			assertTrue(failure.getCause().getMessage().contains("composite CREATE"), failure.getCause().getMessage());
			assertEquals(2, created.size(), "one driver per context");
			assertTrue(closed.contains(created.get(0)), "the first context's driver is closed before the step is");
			assertTrue(closed.contains(created.get(1)), "the refused context's driver is closed before the step is");
		} finally {
			assertDoesNotThrow(step::close);
		}
	}

	/**
	 * An {@code Error} from the refused context's driver close neither replaces the refusal nor stops
	 * the close of the first context: {@code ContextInitResources} logs it.
	 */
	@Test
	void anErrorFromADriversCloseKeepsTheRefusalAndClosesTheFirstContext() throws Exception {
		closeError = new AssertionError("close failed");
		final var step = refusedStep();
		try {
			final var failure = assertThrows(IllegalStateException.class, step::init);
			assertInstanceOf(IllegalConfigurationException.class, failure.getCause());
			assertFalse(List.of(failure.getSuppressed()).contains(closeError), "the close's Error is logged, not suppressed");
			assertEquals(2, created.size());
			assertTrue(closed.contains(created.get(1)), "the refused context's driver close was called");
			assertTrue(closed.contains(created.get(0)), "the first context's driver is closed after that Error");
		} finally {
			closeError = null;
			step.close();
		}
	}

	private PipelineLoadStepLocal refusedStep() throws Exception {
		final var baseSchema = SchemaProvider.resolveAndReduce(Constants.APP_NAME, getClass().getClassLoader());
		final var schema = TreeUtil.reduceForest(List.of(baseSchema, new PipelineLoadStepExtension().schemaProvider().schema()));
		final Config baseConfig;
		try (var defaults = getClass().getResourceAsStream("/config/defaults.yaml")) {
			baseConfig = ConfigUtil.loadConfig(new String(defaults.readAllBytes(), StandardCharsets.UTF_8), ConfigFormat.YAML, schema);
		}
		baseConfig.val("load-step-id", "refused-setup-" + java.util.UUID.randomUUID());
		baseConfig.val("load-op-type", "create");
		baseConfig.val("storage-driver-type", "dummy-mock");
		baseConfig.val("item-data-ranges-threshold", 0);
		baseConfig.val("output-metrics-timing-persist", false);
		baseConfig.val("item-output-delay", 0);
		baseConfig.val("item-output-queue-limit", 1000);
		final var first = new BasicConfig(baseConfig.pathSep(), schema, Map.of(
						"item", Map.of("data", Map.of("ranges", Map.of("threshold", 0)))));
		final var second = new BasicConfig(baseConfig.pathSep(), schema, Map.of(
						"item", Map.of("data", Map.of("ranges", Map.of("threshold", "1MB")))));
		final StorageDriverFactory factory = mock(StorageDriverFactory.class);
		when(factory.id()).thenReturn("dummy-mock");
		when(factory.create(anyString(), any(), any(), anyBoolean(), anyInt()))
						.thenAnswer(invocation -> closeCounting(new DummyStorageDriverMock(invocation.getArgument(2))));
		return new PipelineLoadStepLocal(baseConfig, List.of(factory), List.of(first, second), mock(MetricsManager.class));
	}

	/** A driver without composite support whose {@code close} is recorded. */
	private StorageDriver closeCounting(final StorageDriver target) {
		final StorageDriver driver = (StorageDriver) Proxy.newProxyInstance(
						StorageDriver.class.getClassLoader(),
						new Class<?>[]{StorageDriver.class, CompositeOperationsUnsupported.class
						},
						(proxy, method, args) -> {
							if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
								closed.add((StorageDriver) proxy);
								if (closeError != null && created.size() > 1 && created.get(1) == proxy) {
									try {
										method.invoke(target, args);
									} finally {
										throw closeError;
									}
								}
							}
							try {
								return method.invoke(target, args);
							} catch (final InvocationTargetException e) {
								throw e.getCause();
							}
						});
		created.add(driver);
		return driver;
	}
}
