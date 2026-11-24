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
import com.dell.spt.base.env.Extension;
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
import java.util.Set;
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
	private boolean marked = true;
	private List<Extension> extensions;

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

	/**
	 * A malformed threshold on a driver without composite support is left to the step slice, whose
	 * own parse fails the run, as in 5.17.0. The entry's driver is closed all the same.
	 */
	@Test
	void aMalformedThresholdIsLeftToTheSliceAndTheEntryClosesItsDriver() throws Exception {
		final LoadStep slice = mock(LoadStep.class);
		when(slice.start()).thenThrow(new IllegalStateException("Invalid size: bogus"));
		final var client = client("bogus", stepFactory(slice));
		final var failure = assertThrows(RunTerminalException.class, client::run);
		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertEquals("engine 1 of 1 did not start; see the engine log", failure.getMessage());
		assertEquals(1, created.get());
		assertEquals(1, closed.get(), "the entry's item input driver is closed");
	}

	/**
	 * A malformed data input layer size fails the step before any slice starts, as in 5.17.0: the
	 * entry parses it before it builds its driver, so the failure is not taken for a failed close.
	 */
	@Test
	void aMalformedLayerSizeFailsBeforeAnySliceStarts() throws Exception {
		final LoadStep slice = mock(LoadStep.class);
		final LoadStepFactory<LoadStep, ?> stepFactory = stepFactory(slice);
		final var config = config(0, stepFactory);
		config.val("item-data-input-layer-size", "4XB");
		final var client = new TestLoadStepClient(config, extensions, List.of(), mock(MetricsManager.class));
		final var failure = assertThrows(RunTerminalException.class, client::run);
		assertEquals("step failed to start; see the engine log", failure.getMessage());
		assertEquals(0, created.get(), "no driver is built");
		verify(stepFactory, never()).createLocal(any(), anyList(), anyList(), any());
	}

	/** A PATH LIST never reads the DATA threshold, so a malformed one changes nothing, as in 5.17.0. */
	@Test
	void aListWithAMalformedThresholdOnADriverWithSupportClosesItsDriver() throws Exception {
		final LoadStep slice = mock(LoadStep.class);
		when(slice.start()).thenThrow(new IllegalStateException("stop after the entry's setup"));
		marked = false;
		final var config = config("bogus", stepFactory(slice));
		config.val("item-type", "path");
		config.val("load-op-type", "list");
		final var client = new TestLoadStepClient(config, extensions, List.of(), mock(MetricsManager.class));
		final var failure = assertThrows(RunTerminalException.class, client::run);
		assertEquals("engine 1 of 1 did not start; see the engine log", failure.getMessage());
		assertEquals(1, created.get());
		assertEquals(1, closed.get(), "the entry's driver is closed");
	}

	/**
	 * A client built outside the engine may already declare a method of the hook's name, private or
	 * public. It compiles as before, and the entry never calls it: the READ is still refused.
	 */
	@Test
	void aClientsOwnMethodOfTheHooksNameIsNeitherACollisionNorTheHook() throws Exception {
		final LoadStep slice = mock(LoadStep.class);
		final var config = config("1MB", stepFactory(slice));
		for (final TestLoadStepClient client : List.of(
						new PrivateHookClient(config, extensions), new PublicHookClient(config, extensions))) {
			final var failure = assertThrows(RunTerminalException.class, client::run);
			assertEquals(IntegrityTerminalException.Category.CONFIGURATION, failure.category());
			assertEquals(REFUSAL.replace("dummy-mock", "DummyStorageDriverMock"), failure.getMessage());
		}
		assertEquals(0, listings.get());
	}

	/** The shape of an external client with a private method of the hook's name. */
	static final class PrivateHookClient extends TestLoadStepClient {

		PrivateHookClient(final Config config, final List<Extension> extensions) {
			super(config, extensions, List.of(), mock(MetricsManager.class));
		}

		@SuppressWarnings({"UnusedMethod", "UnusedVariable"
		})
		private int compositeCheckedOpTypes(final Config config) {
			return 0;
		}
	}

	/** An external client with a public method of the hook's name and signature, which checks nothing. */
	static final class PublicHookClient extends TestLoadStepClient {

		PublicHookClient(final Config config, final List<Extension> extensions) {
			super(config, extensions, List.of(), mock(MetricsManager.class));
		}

		public Set<OpType> compositeCheckedOpTypes(final Config config) {
			return Set.of();
		}
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

		final var barrier = client.barrierStartFailure(cause, "entry", 0, 2);
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
		return new TestLoadStepClient(config(threshold, stepFactory), extensions, List.of(), mock(MetricsManager.class));
	}

	/** A READ from {@code /bucket} at {@code threshold}, and the {@link #extensions} that run it. */
	private Config config(final Object threshold, final LoadStepFactory<LoadStep, ?> stepFactory) throws Exception {
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
		extensions = List.of(stepFactory, driverFactory);
		return config;
	}

	private static LoadStepFactory<LoadStep, ?> stepFactory(final LoadStep slice) {
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(slice);
		return factory;
	}

	/** A driver without composite support, unless {@link #marked} is off, that records each listing and close. */
	private StorageDriver probe(final StorageDriver target) {
		created.incrementAndGet();
		return (StorageDriver) Proxy.newProxyInstance(
						StorageDriver.class.getClassLoader(),
						marked
										? new Class<?>[]{StorageDriver.class, CompositeOperationsUnsupported.class
										}
										: new Class<?>[]{StorageDriver.class
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
