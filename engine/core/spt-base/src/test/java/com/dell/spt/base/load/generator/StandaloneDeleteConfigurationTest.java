package com.dell.spt.base.load.generator;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.CliArgUtil;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.TransferConvertBuffer;
import com.dell.spt.base.item.io.RemainingItemCountInput;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.StandaloneDeleteConfig;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.load.step.local.context.LoadStepContextImpl;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class StandaloneDeleteConfigurationTest {

	@Test
	void builderInstallsStandaloneAssemblerOnlyForExplicitValidConfiguration() {
		final var config = standaloneConfig();
		final var driver = supportedDriver();

		final var generator = assertDoesNotThrow(() -> builder(config, driver, emptyInput()).build());
		try {
			assertInstanceOf(LoadGeneratorImpl.class, generator);
		} finally {
			generator.close();
		}
	}

	@Test
	void engineCliOverridesResolveThroughTheShippedStandaloneDeleteSchema() {
		final var config = TestConfigBuilder.config();
		final var parsed = CliArgUtil.parseArgs(
						"--load-op-delete-standalone", "--load-op-delete-batchSize=17",
						"--load-op-delete-duration");
		parsed.forEach(config::val);

		final var standalone = StandaloneDeleteConfig.from(config.configVal("load"));

		assertTrue(standalone.enabled());
		assertEquals(17, standalone.batchSize());
		assertTrue(standalone.durationMode());
	}

	@Test
	void durationModeRejectsMissingDeadlineAndRequestCountWhileCountModeRemainsCompatible() {
		final var missingDeadline = standaloneConfig();
		missingDeadline.val("load-op-delete-duration", true);
		missingDeadline.val("load-step-limit-time", 0);
		assertThrows(
						IllegalConfigurationException.class,
						() -> StandaloneDeleteConfig.from(missingDeadline.configVal("load")));

		final var requestCount = standaloneConfig();
		requestCount.val("load-op-delete-duration", true);
		requestCount.val("load-step-limit-time", "10s");
		requestCount.val("load-op-limit-count", 1L);
		assertThrows(
						IllegalConfigurationException.class,
						() -> StandaloneDeleteConfig.from(requestCount.configVal("load")));

		final var countMode = standaloneConfig();
		countMode.val("load-step-limit-time", "0s");
		countMode.val("load-op-limit-count", 1L);
		assertFalse(StandaloneDeleteConfig.from(countMode.configVal("load")).durationMode());

		final var mismatchedDurationFlag = standaloneConfig();
		mismatchedDurationFlag.val("load-op-delete-duration", false);
		mismatchedDurationFlag.val("load-step-limit-time", "10s");
		final var mismatch = assertThrows(
						IllegalConfigurationException.class,
						() -> StandaloneDeleteConfig.from(mismatchedDurationFlag.configVal("load")));
		assertTrue(mismatch.getMessage().contains("load-op-delete-duration"));
	}

	@Test
	void builderFailsClosedForUnsupportedDriverAndSingleItemOutputTopologies() {
		final var unsupportedDriver = supportedDriver();
		when(unsupportedDriver.supportsStandaloneDeleteRequests()).thenReturn(false);
		assertThrows(
						IllegalConfigurationException.class,
						() -> builder(standaloneConfig(), unsupportedDriver, emptyInput()).build());

		final var outputConfig = standaloneConfig();
		outputConfig.val("item-output-file", "/tmp/ordinary-success-items.csv");
		assertThrows(
						IllegalConfigurationException.class,
						() -> builder(outputConfig, supportedDriver(), emptyInput()).build());

		@SuppressWarnings("unchecked")
		final var pipeline = (TransferConvertBuffer<IntegrityManifestDataItem, DeleteRequestOperation>) mock(TransferConvertBuffer.class);
		assertThrows(
						IllegalConfigurationException.class,
						() -> builder(standaloneConfig(), supportedDriver(), pipeline).build());

		@SuppressWarnings("unchecked")
		final var uncountedInput = (Input<IntegrityManifestDataItem>) mock(Input.class);
		assertThrows(
						IllegalConfigurationException.class,
						() -> builder(standaloneConfig(), supportedDriver(), uncountedInput).build());
	}

	@Test
	void loadStepRejectsOverrideInjectedRecycleRetryAndUnsupportedDriver() {
		final var recycle = standaloneConfig();
		recycle.val("load-op-recycle-mode", true);
		assertContextRejected(recycle, supportedDriver());

		final var retry = standaloneConfig();
		retry.val("load-op-retry", true);
		assertContextRejected(retry, supportedDriver());

		final var unsupported = supportedDriver();
		when(unsupported.supportsStandaloneDeleteRequests()).thenReturn(false);
		assertContextRejected(standaloneConfig(), unsupported);

		final LoadGenerator generator = mock(LoadGenerator.class);
		final MetricsContext metrics = mock(MetricsContext.class);
		assertThrows(
						IllegalConfigurationException.class,
						() -> new LoadStepContextImpl(
										"standalone-delete-trace",
										generator,
										supportedDriver(),
										metrics,
										standaloneConfig().configVal("load"),
										true));
		final var context = new LoadStepContextImpl(
						"standalone-delete-output",
						generator,
						supportedDriver(),
						metrics,
						standaloneConfig().configVal("load"),
						false);
		assertThrows(
						IllegalConfigurationException.class,
						() -> context.operationsResultsOutput(mock(Output.class)));
		assertThrows(
						IllegalConfigurationException.class,
						() -> context.operationsMetricsOutput(mock(Output.class)));

		final var lifecycleDisabled = supportedDriver();
		when(lifecycleDisabled.operationLifecycle()).thenReturn(OperationLifecycleTracker.disabled());
		assertContextRejected(standaloneConfig(), lifecycleDisabled);
	}

	@Test
	void standaloneBatchSizeIsStrictlyBounded() {
		for (final var invalid : List.of(0, 1001)) {
			final var config = standaloneConfig();
			config.val("load-op-delete-batchSize", invalid);
			assertThrows(
							IllegalConfigurationException.class,
							() -> builder(config, supportedDriver(), emptyInput()).build());
		}
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void legacyCleanupDeleteStillBuildsOneDataOperationPerItem() throws Exception {
		final var config = standaloneConfig();
		config.val("load-op-delete-standalone", false);
		final var reads = new AtomicInteger();
		final Input<IntegrityManifestDataItem> input = mock(Input.class);
		when(input.toString()).thenReturn("legacy-cleanup-items");
		org.mockito.Mockito.doAnswer(invocation -> {
			if (reads.getAndIncrement() == 0) {
				invocation.<List<IntegrityManifestDataItem>> getArgument(0)
								.add(new IntegrityManifestDataItem("bucket", "legacy-key", 19, null));
				return 1;
			}
			throw new EOFException();
		}).when(input).get(org.mockito.ArgumentMatchers.anyList(), org.mockito.ArgumentMatchers.anyInt());
		final var output = new LegacyCollectingOutput();
		final LoadGeneratorImpl<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>> generator = new LoadGeneratorBuilderImpl<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>, LoadGeneratorImpl<IntegrityManifestDataItem, Operation<IntegrityManifestDataItem>>>()
						.authConfig(config.configVal("storage-auth"))
						.itemConfig(config.configVal("item"))
						.itemFactory((com.dell.spt.base.item.ItemFactory) new DataItemFactoryImpl<>())
						.itemType(ItemType.DATA)
						.loadConfig(config.configVal("load"))
						.itemInput(input)
						.loadOperationsOutput(output)
						.originIndex(0)
						.build();

		try {
			generator.doWork();

			assertEquals(1, output.operations.size());
			assertInstanceOf(DataOperation.class, output.operations.get(0));
			assertFalse(output.operations.get(0) instanceof DeleteRequestOperation);
			assertEquals("legacy-key", output.operations.get(0).item().name());
		} finally {
			generator.close();
		}
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private static LoadGeneratorBuilderImpl builder(
					final Config config,
					final StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> driver,
					final Input<IntegrityManifestDataItem> input) {
		return new LoadGeneratorBuilderImpl()
						.authConfig(config.configVal("storage-auth"))
						.itemConfig(config.configVal("item"))
						.itemFactory(new DataItemFactoryImpl<>())
						.itemType(ItemType.DATA)
						.loadConfig(config.configVal("load"))
						.itemInput(input)
						.loadOperationsOutput(driver)
						.originIndex(0);
	}

	private static Config standaloneConfig() {
		final var config = TestConfigBuilder.config();
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-batchSize", 100);
		config.val("load-op-recycle-mode", false);
		config.val("load-op-retry", false);
		config.val("load-step-limit-time", "0s");
		config.val("item-output-file", "");
		return config;
	}

	@SuppressWarnings("unchecked")
	private static StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> supportedDriver() {
		final var driver = (StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation>) mock(StorageDriver.class);
		when(driver.supportsStandaloneDeleteRequests()).thenReturn(true);
		when(driver.operationLifecycle()).thenReturn(new OperationLifecycleTracker<>());
		return driver;
	}

	@SuppressWarnings("unchecked")
	private static Input<IntegrityManifestDataItem> emptyInput() {
		final var input = (RemainingItemCountInput<IntegrityManifestDataItem>) mock(RemainingItemCountInput.class);
		when(input.toString()).thenReturn("canonical-manifest");
		return input;
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private static void assertContextRejected(
					final Config config, final StorageDriver driver) {
		final LoadGenerator generator = mock(LoadGenerator.class);
		final MetricsContext metrics = mock(MetricsContext.class);
		assertThrows(
						IllegalConfigurationException.class,
						() -> new LoadStepContextImpl(
										"standalone-delete", generator, driver, metrics, config.configVal("load"), false));
	}

	private static final class LegacyCollectingOutput
					implements Output<Operation<IntegrityManifestDataItem>> {
		private final List<Operation<IntegrityManifestDataItem>> operations = new ArrayList<>();

		@Override
		public boolean put(final Operation<IntegrityManifestDataItem> operation) {
			operations.add(operation);
			return true;
		}

		@Override
		public int put(
						final List<Operation<IntegrityManifestDataItem>> buffer,
						final int from,
						final int to) {
			operations.addAll(buffer.subList(from, to));
			return to - from;
		}

		@Override
		public int put(final List<Operation<IntegrityManifestDataItem>> buffer) {
			return put(buffer, 0, buffer.size());
		}

		@Override
		public Input<Operation<IntegrityManifestDataItem>> getInput() {
			return null;
		}

		@Override
		public void close() {}
	}
}
