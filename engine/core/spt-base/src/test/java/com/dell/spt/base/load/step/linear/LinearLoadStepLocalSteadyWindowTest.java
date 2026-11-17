package com.dell.spt.base.load.step.linear;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.dell.spt.base.metrics.steady.SteadyStatus;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMockExtension;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Every stop-path entry point closes the step's Steady-State Windows before it acts (D1, F1). */
class LinearLoadStepLocalSteadyWindowTest {

	@Test
	void everyStopEntryPointOfLoadStepClosesTheWindowsFirst() throws Exception {
		final List<Method> entryPoints = Arrays.stream(LoadStep.class.getMethods())
						.filter(method -> method.getName().endsWith("ForStepStop"))
						// Queries observe progress; they are not stop-path actions.
						.filter(method -> !method.getName().startsWith("is"))
						.toList();
		assertTrue(entryPoints.size() >= 7, "entry points found: " + entryPoints);
		for (final Method entryPoint : entryPoints) {
			final ProbeStep step = probeStep();
			try {
				assertFalse(windowsClosed(step), entryPoint.getName() + ": windows open before the call");
				final Object[] arguments = Arrays.stream(entryPoint.getParameterTypes())
								.map(type -> type == long.class ? (Object) 0L : null)
								.toArray();
				try {
					entryPoint.invoke(step, arguments);
				} catch (final ReflectiveOperationException ignored) {
					// The action itself may fail on an idle step; the close comes first regardless.
				}
				assertTrue(windowsClosed(step), entryPoint.getName() + " must close the steady-state windows first");
			} finally {
				step.closeQuietly();
			}
		}
	}

	@Test
	void shutdownClosesTheWindowsBeforeAnyContextShutsDown() throws Exception {
		final ProbeStep step = probeStep();
		try {
			step.shutdownForTest();
			assertTrue(windowsClosed(step));
		} finally {
			step.closeQuietly();
		}
	}

	@Test
	void aStartThatFailsAfterTheMetricsExistClosesTheWindowsWithoutAMeasurementFailure() throws Exception {
		final Path itemInput = Files.createTempFile("spt-items-", ".csv");
		final ProbeStep step = new ProbeStep(stepConfig(itemInput, "nosuch"), List.of(), itemInput);
		try {
			assertThrows(RuntimeException.class, step::initForTest, "no extension provides the storage driver");
			assertFalse(step.contexts().isEmpty(), "the metrics contexts exist before the driver is built");
			step.shutdownForTest();
			for (final MetricsContext<?> context : step.contexts()) {
				final SteadyResult result = context.steadyResult();
				assertNotNull(result, "shutdown closes every window");
				assertFalse(result.cause().contains("measurement failed"), "cause: " + result.cause());
				assertEquals(SteadyStatus.NOT_REACHED, result.status());
				assertEquals("shutdown", result.cause());
			}
		} finally {
			step.closeQuietly();
		}
	}

	@Test
	void theConcurrencyGaugeOfAStepWithNoStepContextReadsZero() throws Exception {
		final Path itemInput = Files.createTempFile("spt-items-", ".csv");
		final ProbeStep step = new ProbeStep(stepConfig(itemInput, null), List.of(), itemInput);
		try {
			for (final OpType opType : OpType.values()) {
				assertEquals(0, assertDoesNotThrow(() -> step.concurrencyGauge(0, opType)), opType.name());
			}
		} finally {
			step.closeQuietly();
		}
	}

	private static boolean windowsClosed(final ProbeStep step) {
		final List<MetricsContext<?>> contexts = step.contexts();
		assertFalse(contexts.isEmpty());
		return contexts.stream().allMatch(ctx -> ctx.steadyResult() != null);
	}

	private static ProbeStep probeStep() throws Exception {
		final Path itemInput = Files.createTempFile("spt-items-", ".csv");
		final ProbeStep step = new ProbeStep(
						stepConfig(itemInput, null), List.of(new DummyStorageDriverMockExtension<>()), itemInput);
		assertDoesNotThrow(step::initForTest);
		return step;
	}

	/** A step config reading {@code itemInput}; a non-null {@code driverType} overrides the default. */
	private static Config stepConfig(final Path itemInput, final String driverType) {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "steady-close-" + System.nanoTime());
		config.val("output-metrics-timing-persist", false);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		config.val("item-data-ranges-concat", null);
		config.val("item-input-file", itemInput.toString());
		if (driverType != null) {
			config.val("storage-driver-type", driverType);
		}
		return config;
	}

	private static final class ProbeStep extends LinearLoadStepLocal {
		private final Path itemInput;

		ProbeStep(final Config config, final List<Extension> extensions, final Path itemInput) {
			super(config, extensions, List.of(), mock(MetricsManager.class));
			this.itemInput = itemInput;
		}

		void initForTest() {
			init();
		}

		int concurrencyGauge(final int index, final OpType opType) {
			return actualConcurrencyGauge(index, opType).getAsInt();
		}

		void shutdownForTest() {
			doShutdownForTest();
		}

		private void doShutdownForTest() {
			try {
				final Method doShutdown = com.dell.spt.base.load.step.local.LoadStepLocalBase.class
								.getDeclaredMethod("doShutdown");
				doShutdown.setAccessible(true);
				doShutdown.invoke(this);
			} catch (final ReflectiveOperationException ignored) {
				// The shutdown itself may fail on an idle step; the close comes first regardless.
			}
		}

		@SuppressWarnings({"unchecked", "rawtypes"
		})
		List<MetricsContext<?>> contexts() {
			return (List) metricsContexts;
		}

		void closeQuietly() {
			try {
				close();
			} catch (final Exception ignored) {
				// best-effort cleanup of an idle probe step
			}
			try {
				Files.deleteIfExists(itemInput);
			} catch (final Exception ignored) {
				// best-effort cleanup
			}
		}
	}
}
