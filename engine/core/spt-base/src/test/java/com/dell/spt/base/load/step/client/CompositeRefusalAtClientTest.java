package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.storage.driver.CompositeOperationSupport;
import com.dell.spt.base.storage.driver.CompositeOperationsUnsupported;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMock;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * A composite configuration on a driver without composite support is refused by the entry engine
 * before it reads the item input, so a READ from a storage path sends no listing request, and the
 * refusal's own text, not a pointer to the log, is the run's failure.
 */
@SuppressWarnings({"rawtypes", "unchecked"
})
class CompositeRefusalAtClientTest {

	private static final String REFUSAL = "The storage driver type \"dummy-mock\" does not support composite"
					+ " (multipart) operations, but item.data.ranges.threshold=1048576 would make composite READ"
					+ " operations; set item.data.ranges.threshold to 0 for this driver";

	private final AtomicInteger listings = new AtomicInteger();
	private final AtomicInteger created = new AtomicInteger();
	private final AtomicInteger closed = new AtomicInteger();

	@Test
	void aRefusedReadFromAStoragePathListsNothingAndFailsWithTheRefusal() throws Exception {
		final LoadStep slice = mock(LoadStep.class);
		final LoadStepFactory<LoadStep, ?> stepFactory = stepFactory(slice);
		final var client = client("1MB", stepFactory);
		final var failure = assertThrows(RunTerminalException.class, client::run);
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, failure.category());
		// The dummy driver's type is its class name.
		assertEquals(REFUSAL.replace("dummy-mock", "DummyStorageDriverMock"), failure.getMessage());
		assertEquals(0, listings.get(), "no listing request before the refusal");
		assertEquals(1, created.get());
		assertEquals(1, closed.get(), "the item input driver is closed");
		verify(stepFactory, never()).createLocal(any(), anyList(), anyList(), any());
	}

	@Test
	void theSameReadWithTheThresholdAtZeroListsItsPath() throws Exception {
		final LoadStep slice = mock(LoadStep.class);
		when(slice.start()).thenThrow(new IllegalStateException("No item input available"));
		final var client = client(0, stepFactory(slice));
		assertThrows(RunTerminalException.class, client::run);
		assertTrue(listings.get() > 0, "the probe sees the listing a refused READ must not send");
	}

	@Test
	void aSliceThatRefusesTheConfigurationReportsTheRefusal() throws Exception {
		final var client = new TestLoadStepClient(TestConfigBuilder.config(), List.of(), List.of(), mock(MetricsManager.class));
		final var cause = new IllegalStateException("Failed to initialize the load generator", refusal());

		final var entry = client.sliceStartFailure(cause, 0, 2);
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, entry.category());
		assertEquals("engine 1 of 2 did not start: " + REFUSAL, entry.getMessage());
		assertEquals(client.loadStepId(), entry.stepId());
		assertSame(cause, entry.getCause());

		final var worker = client.sliceStartFailure(new RuntimeException("remote", cause), 1, 2);
		assertEquals("engine 2 of 2 did not start: " + REFUSAL, worker.getMessage());

		final var barrier = client.barrierStartFailure(cause, "entry");
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, barrier.category());
		assertEquals("Step " + client.loadStepId() + " did not start: entry: " + REFUSAL, barrier.getMessage());

		// Any other failure to start keeps its message.
		final var other = client.sliceStartFailure(
						new IllegalStateException("Failed", new IllegalConfigurationException("No item input available")), 1, 2);
		assertEquals(IntegrityTerminalException.Category.EXECUTION, other.category());
		assertEquals("engine 2 of 2 did not start; see the engine log", other.getMessage());
	}

	private static IllegalConfigurationException refusal() {
		final StorageDriver driver = mock(
						StorageDriver.class, withSettings().extraInterfaces(CompositeOperationsUnsupported.class));
		when(driver.driverType()).thenReturn("dummy-mock");
		return assertThrows(
						IllegalConfigurationException.class,
						() -> CompositeOperationSupport.requireSupport(driver, OpType.READ, 1048576));
	}

	private TestLoadStepClient client(final Object threshold, final LoadStepFactory<LoadStep, ?> stepFactory)
					throws Exception {
		final Config config = TestConfigBuilder.config();
		config.val("load-op-type", "read");
		config.val("item-input-path", "/bucket");
		config.val("item-input-file", null);
		config.val("item-data-ranges-threshold", threshold);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		final StorageDriverFactory driverFactory = mock(StorageDriverFactory.class);
		when(driverFactory.id()).thenReturn(config.stringVal("storage-driver-type"));
		when(driverFactory.create(anyString(), any(), any(), anyBoolean(), anyInt()))
						.thenAnswer(invocation -> probe(new DummyStorageDriverMock(invocation.getArgument(2))));
		return new TestLoadStepClient(config, List.of(stepFactory, driverFactory), List.of(), mock(MetricsManager.class));
	}

	private static LoadStepFactory<LoadStep, ?> stepFactory(final LoadStep slice) {
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(slice);
		return factory;
	}

	/** A driver without composite support that records each listing and each close. */
	private StorageDriver probe(final StorageDriver target) {
		created.incrementAndGet();
		return (StorageDriver) Proxy.newProxyInstance(
						StorageDriver.class.getClassLoader(),
						new Class<?>[]{StorageDriver.class, CompositeOperationsUnsupported.class
						},
						(proxy, method, args) -> {
							if ("list".equals(method.getName())) {
								listings.incrementAndGet();
								return List.of();
							}
							if ("close".equals(method.getName()) && method.getParameterCount() == 0) {
								closed.incrementAndGet();
							}
							try {
								return method.invoke(target, args);
							} catch (final InvocationTargetException e) {
								throw e.getCause();
							}
						});
	}
}
