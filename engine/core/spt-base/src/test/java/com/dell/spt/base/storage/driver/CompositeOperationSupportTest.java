package com.dell.spt.base.storage.driver;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

/** The step-start check that refuses a ranges threshold on a driver without composite support. */
class CompositeOperationSupportTest {

	@Test
	void theDriverInterfaceGainsNoMethod() {
		assertFalse(
						Arrays.stream(StorageDriver.class.getDeclaredMethods())
										.map(Method::getName)
										.anyMatch(name -> name.toLowerCase(java.util.Locale.ROOT).contains("composite")
														&& !name.equals("compositeResults")),
						"the opt-out is a marker interface, not a StorageDriver method");
		assertEquals(0, CompositeOperationsUnsupported.class.getDeclaredMethods().length);
	}

	@Test
	void aThresholdOnADriverWithoutSupportIsRefusedForCreateAndRead() {
		final StorageDriver<?, ?> driver = driver(false);
		for (final OpType opType : new OpType[]{OpType.CREATE, OpType.READ
		}) {
			final var failure = assertThrows(
							IllegalConfigurationException.class,
							() -> CompositeOperationSupport.requireSupport(driver, opType, 1048576),
							opType.name());
			assertEquals(
							"The storage driver type \"fs\" does not support composite (multipart) operations, but"
											+ " item.data.ranges.threshold=1048576 would make composite " + opType
											+ " operations; set item.data.ranges.threshold to 0 for this driver",
							failure.getMessage());
		}
	}

	@Test
	void aThresholdOfZeroOrAnOpTypeWithoutCompositesOrADriverWithSupportPasses() {
		final StorageDriver<?, ?> unsupported = driver(false);
		for (final OpType opType : OpType.values()) {
			assertDoesNotThrow(() -> CompositeOperationSupport.requireSupport(unsupported, opType, 0), opType.name());
			assertDoesNotThrow(() -> CompositeOperationSupport.requireSupport(unsupported, opType, -1), opType.name());
			assertDoesNotThrow(() -> CompositeOperationSupport.requireSupport(driver(true), opType, 1048576), opType.name());
		}
		for (final OpType opType : new OpType[]{OpType.NOOP, OpType.UPDATE, OpType.DELETE, OpType.STAT, OpType.LIST
		}) {
			assertDoesNotThrow(() -> CompositeOperationSupport.requireSupport(unsupported, opType, 1048576), opType.name());
		}
	}

	/**
	 * An external driver that already declares a method named {@code supportsCompositeOperations},
	 * in any form, behaves as it did before the check existed: the check does not call it, so the
	 * driver is accepted, and its own method keeps its answer and its access.
	 */
	@Test
	void anExternalDriverWithASameNamedMethodIsAcceptedAsBefore() throws Exception {
		final PublicFalseDriver<?, ?> publicFalse = mock(PublicFalseDriver.class, CALLS_REAL_METHODS);
		final PrivateMethodDriver<?, ?> privateMethod = mock(PrivateMethodDriver.class, CALLS_REAL_METHODS);
		final UnrelatedDefaultDriver<?, ?> unrelatedDefault = mock(UnrelatedDefaultDriver.class, CALLS_REAL_METHODS);
		for (final StorageDriver<?, ?> driver : new StorageDriver<?, ?>[]{publicFalse, privateMethod, unrelatedDefault
		}) {
			for (final OpType opType : new OpType[]{OpType.CREATE, OpType.READ
			}) {
				assertDoesNotThrow(
								() -> CompositeOperationSupport.requireSupport(driver, opType, 1048576),
								driver.getClass().getSuperclass().getSimpleName() + " " + opType);
			}
		}
		assertFalse(publicFalse.supportsCompositeOperations());
		assertFalse(privateMethod.answer());
		final Method privateDeclared = PrivateMethodDriver.class.getDeclaredMethod("supportsCompositeOperations");
		assertTrue(Modifier.isPrivate(privateDeclared.getModifiers()));
		assertEquals(UnrelatedCapability.class, UnrelatedDefaultDriver.class
						.getMethod("supportsCompositeOperations")
						.getDeclaringClass());
		assertFalse(unrelatedDefault.supportsCompositeOperations());
	}

	/**
	 * Only the runtime class's own {@code implements} clause counts: a driver built outside the
	 * engine on a refusing driver, or through an interface that extends the marker, runs as before.
	 */
	@Test
	void onlyAMarkerTheDriversOwnClassDeclaresIsRefused() {
		final StorageDriver<?, ?> declared = proxy(StorageDriver.class, CompositeOperationsUnsupported.class);
		assertTrue(CompositeOperationSupport.declaresNoCompositeSupport(declared));
		assertThrows(
						IllegalConfigurationException.class,
						() -> CompositeOperationSupport.requireSupport(declared, OpType.CREATE, 1048576));

		final StorageDriver<?, ?> subclass = mock(MarkedDriver.class, CALLS_REAL_METHODS);
		assertTrue(subclass instanceof CompositeOperationsUnsupported, "the marker is inherited");
		assertFalse(CompositeOperationSupport.declaresNoCompositeSupport(subclass));
		assertDoesNotThrow(() -> CompositeOperationSupport.requireSupport(subclass, OpType.CREATE, 1048576));
		assertDoesNotThrow(() -> CompositeOperationSupport.requireSupport(subclass, OpType.READ, 1048576));

		final StorageDriver<?, ?> viaInterface = proxy(MarkedDriverInterface.class);
		assertTrue(viaInterface instanceof CompositeOperationsUnsupported);
		assertFalse(CompositeOperationSupport.declaresNoCompositeSupport(viaInterface));
		assertDoesNotThrow(() -> CompositeOperationSupport.requireSupport(viaInterface, OpType.READ, 1048576));
	}

	private static StorageDriver<?, ?> proxy(final Class<?>... interfaces) {
		return (StorageDriver<?, ?>) Proxy.newProxyInstance(
						CompositeOperationSupportTest.class.getClassLoader(),
						interfaces,
						(proxy, method, args) -> "driverType".equals(method.getName()) ? "proxy" : null);
	}

	/** A driver class that declares the marker, as a shipped refusing driver does. */
	abstract static class MarkedDriver<I extends Item, O extends Operation<I>>
					implements StorageDriver<I, O>, CompositeOperationsUnsupported {}

	/** An interface that extends the marker. */
	interface MarkedDriverInterface extends StorageDriver<Item, Operation<Item>>, CompositeOperationsUnsupported {}

	private static StorageDriver<?, ?> driver(final boolean supported) {
		final StorageDriver<?, ?> driver = supported
						? mock(StorageDriver.class)
						: mock(StorageDriver.class, withSettings().extraInterfaces(CompositeOperationsUnsupported.class));
		when(driver.driverType()).thenReturn("fs");
		return driver;
	}

	/** An external driver with a public method of the same name that returns {@code false}. */
	abstract static class PublicFalseDriver<I extends Item, O extends Operation<I>> implements StorageDriver<I, O> {
		public boolean supportsCompositeOperations() {
			return false;
		}
	}

	/** An external driver with a private method of the same name. */
	abstract static class PrivateMethodDriver<I extends Item, O extends Operation<I>> implements StorageDriver<I, O> {
		@SuppressWarnings("UnusedMethod")
		private boolean supportsCompositeOperations() {
			return false;
		}

		boolean answer() {
			return supportsCompositeOperations();
		}
	}

	/** An unrelated interface whose default method has the same signature. */
	interface UnrelatedCapability {
		default boolean supportsCompositeOperations() {
			return false;
		}
	}

	/** An external driver that inherits that unrelated default. */
	abstract static class UnrelatedDefaultDriver<I extends Item, O extends Operation<I>>
					implements StorageDriver<I, O>, UnrelatedCapability {}
}
