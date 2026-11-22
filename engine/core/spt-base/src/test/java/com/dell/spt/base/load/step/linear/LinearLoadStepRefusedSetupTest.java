package com.dell.spt.base.load.step.linear;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.storage.driver.CompositeOperationsUnsupported;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMock;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** A linear step whose setup is refused closes the storage driver it created for it. */
@SuppressWarnings({"rawtypes", "unchecked"
})
class LinearLoadStepRefusedSetupTest {

	private final List<StorageDriver> created = Collections.synchronizedList(new ArrayList<>());
	private final Set<StorageDriver> closed = Collections.synchronizedSet(Collections.newSetFromMap(new IdentityHashMap<>()));

	@Test
	void aRefusedSetupClosesItsDriver() throws Exception {
		for (final String opType : List.of("create", "read")) {
			created.clear();
			closed.clear();
			final Config config = TestConfigBuilder.config();
			config.val("load-step-id", "refused-setup-" + System.nanoTime());
			config.val("load-op-type", opType);
			config.val("output-metrics-timing-persist", false);
			config.val("item-data-input-compressibility", 0.0);
			config.val("item-data-dedupable", false);
			config.val("item-data-verify", false);
			config.val("item-data-ranges-concat", null);
			config.val("item-data-ranges-threshold", "1MB");
			final var step = new ProbeStep(config, factory(config.stringVal("storage-driver-type")));
			try {
				final var failure = assertThrows(IllegalStateException.class, step::initForTest, opType);
				assertInstanceOf(IllegalConfigurationException.class, failure.getCause(), opType);
				assertTrue(failure.getCause().getMessage().contains("composite"), failure.getCause().getMessage());
				assertEquals(1, created.size(), opType);
				assertTrue(closed.contains(created.get(0)), opType + ": closed before the step is");
			} finally {
				assertDoesNotThrow(step::close);
			}
		}
	}

	private StorageDriverFactory factory(final String id) throws Exception {
		final StorageDriverFactory factory = mock(StorageDriverFactory.class);
		when(factory.id()).thenReturn(id);
		when(factory.create(anyString(), any(), any(), anyBoolean(), anyInt()))
						.thenAnswer(invocation -> closeCounting(new DummyStorageDriverMock(invocation.getArgument(2))));
		return factory;
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

	private static final class ProbeStep extends LinearLoadStepLocal {
		ProbeStep(final Config config, final StorageDriverFactory factory) {
			super(config, List.of(factory), List.of(), mock(MetricsManager.class));
		}

		void initForTest() {
			init();
		}
	}
}
