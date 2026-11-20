package com.dell.spt.base.load.generator;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.storage.driver.CompositeOperationsUnsupported;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.commons.io.collection.ListInput;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.Test;

/**
 * A linear, pipeline or weighted step builds its load generator at start: on a driver without
 * composite support, the builder refuses a ranges threshold that would make composite operations.
 */
@SuppressWarnings("unchecked")
class CompositeSupportGeneratorBuilderTest {

	private final StorageDriver<DataItem, DataOperation<DataItem>> unsupported = mock(StorageDriver.class, withSettings().extraInterfaces(CompositeOperationsUnsupported.class));
	private final StorageDriver<DataItem, DataOperation<DataItem>> supported = mock(StorageDriver.class);

	@Test
	void aThresholdOnADriverWithoutSupportFailsTheBuildForCreateAndRead() {
		when(unsupported.driverType()).thenReturn("fs");
		for (final String opType : List.of("create", "read")) {
			for (final Object threshold : List.of("1MB", 1048576)) {
				final Config config = config(opType, threshold);
				final var failure = assertThrows(
								IllegalConfigurationException.class, () -> builder(unsupported, config).build(), opType + " " + threshold);
				assertTrue(failure.getMessage().contains("\"fs\""), failure.getMessage());
				assertTrue(failure.getMessage().contains("item.data.ranges.threshold=1048576"), failure.getMessage());
				assertTrue(failure.getMessage().contains("composite " + opType.toUpperCase(Locale.ROOT)), failure.getMessage());
			}
		}
		verify(unsupported, never()).put(any(DataOperation.class));
	}

	@Test
	void theThresholdAtZeroOrADriverWithSupportBuilds() throws Exception {
		for (final String opType : List.of("create", "read")) {
			try (var generator = builder(unsupported, config(opType, 0)).build()) {
				assertNotNull(generator, opType);
			}
		}
		for (final String opType : List.of("create", "read")) {
			try (var generator = builder(supported, config(opType, "1MB")).build()) {
				assertNotNull(generator, opType);
			}
		}
	}

	private static Config config(final String opType, final Object threshold) {
		final Config config = TestConfigBuilder.config();
		config.val("load-op-type", opType);
		config.val("load-batch-size", 1);
		config.val("load-op-limit-count", 1);
		config.val("load-op-recycle-mode", false);
		config.val("load-op-retry", false);
		config.val("item-data-ranges-threshold", threshold);
		config.val("item-data-ranges-concat", null);
		return config;
	}

	private LoadGeneratorBuilderImpl<DataItem, DataOperation<DataItem>, LoadGeneratorImpl<DataItem, DataOperation<DataItem>>> builder(
					final StorageDriver<DataItem, DataOperation<DataItem>> driver, final Config config) {
		return new LoadGeneratorBuilderImpl<DataItem, DataOperation<DataItem>, LoadGeneratorImpl<DataItem, DataOperation<DataItem>>>()
						.itemConfig(config.configVal("item"))
						.loadConfig(config.configVal("load"))
						.itemType(ItemType.DATA)
						.itemFactory(new DataItemFactoryImpl())
						.authConfig(config.configVal("storage-auth"))
						.originIndex(0)
						.itemInput(new ListInput<>(new ArrayList<>()))
						.loadOperationsOutput(driver);
	}
}
