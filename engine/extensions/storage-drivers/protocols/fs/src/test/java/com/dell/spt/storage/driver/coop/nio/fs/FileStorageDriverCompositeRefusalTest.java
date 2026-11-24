package com.dell.spt.storage.driver.coop.nio.fs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.storage.driver.CompositeOperationSupport;
import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.load.generator.LoadGeneratorBuilderImpl;
import com.dell.spt.base.load.generator.LoadGeneratorImpl;
import com.github.akurilov.commons.io.collection.ListInput;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The file system driver has no composite (multipart) support, so a step refuses at start a
 * ranges threshold above 0 for a CREATE or a READ, the check a linear, pipeline or weighted step
 * runs when it builds its load generator. With the threshold at 0 the step starts as before.
 */
class FileStorageDriverCompositeRefusalTest {

	private DataInput dataInput;
	private FileStorageDriver<DataItem, DataOperation<DataItem>> driver;

	@BeforeEach
	void setUp() throws Exception {
		final Config driverConfig = TestConfigBuilder.config();
		driverConfig.val("storage-driver-type", "fs");
		driverConfig.val("load-batch-size", 16);
		driverConfig.val("storage-driver-limit-concurrency", 4);
		driverConfig.val("storage-driver-threads", 0);
		driverConfig.val("storage-driver-limit-queue-input", 128);
		dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false);
		driver = new FileStorageDriver<>(
						"test-fs", dataInput, driverConfig.configVal("storage"), false, driverConfig.intVal("load-batch-size"));
	}

	@AfterEach
	void tearDown() throws Exception {
		driver.close();
		dataInput.close();
	}

	@Test
	void theDriverHasNoCompositeSupport() {
		assertTrue(CompositeOperationSupport.declaresNoCompositeSupport(driver));
	}

	@Test
	void aCreateStepWithAThresholdFailsAtStart() throws Exception {
		assertRefused("create");
	}

	@Test
	void aReadStepWithAThresholdFailsAtStart() throws Exception {
		assertRefused("read");
	}

	@Test
	void createAndReadStepsWithTheThresholdAtZeroStart() throws Exception {
		for (final String opType : List.of("create", "read")) {
			try (var generator = builder(stepConfig(opType, 0)).build()) {
				assertNotNull(generator, opType);
			}
		}
	}

	private void assertRefused(final String opType) throws Exception {
		final var failure = assertThrows(
						IllegalConfigurationException.class, () -> builder(stepConfig(opType, "1MB")).build());
		assertEquals(
						"The storage driver type \"fs\" does not support composite (multipart) operations, but"
										+ " item.data.ranges.threshold=1048576 would make composite " + opType.toUpperCase(Locale.ROOT)
										+ " operations; set item.data.ranges.threshold to 0 for this driver",
						failure.getMessage());
	}

	private LoadGeneratorBuilderImpl<DataItem, DataOperation<DataItem>, LoadGeneratorImpl<DataItem, DataOperation<DataItem>>> builder(
					final Config config) {
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

	/** The engine's shipped defaults, with the op type and the ranges threshold set. */
	private static Config stepConfig(final String opType, final Object threshold) throws Exception {
		final String defaults;
		try (InputStream stream = InitialConfigSchemaProvider.class.getResourceAsStream("/config/defaults.yaml")) {
			defaults = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
		}
		final Config config = ConfigUtil.loadConfig(
						defaults, ConfigFormat.YAML, InitialConfigSchemaProvider.provider().schema());
		config.val("load-op-type", opType);
		config.val("item-data-ranges-threshold", threshold);
		config.val("item-data-ranges-concat", null);
		return config;
	}
}
