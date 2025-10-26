package com.dell.spt.base.item.io;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.logging.LogUtil;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.file.BinFileInput;
import com.github.akurilov.confuse.Config;

import org.apache.logging.log4j.Level;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.file.Path;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMock;
import org.apache.logging.log4j.core.config.Configurator;

public class ItemInputFactoryTest {
	private static Path TMP_DIR_PATH = null;
	private int batchSize = 4096; // default value given in the schema
	private Input<Item> itemInput = null;
	Config testConfig = TestConfigBuilder.config();

	@BeforeEach
	void setUp() {
		initDirectory();
		testConfig.val("item-type", "data");
		testConfig.val("item-input-file", TMP_DIR_PATH.toString());
		testConfig.val("item-input-path", TMP_DIR_PATH.toString());
	}

	/*
	 	Description: When no file or path exists at the path given, it should throw IOException and return null
		Steps:
			1. Ensure basic values are assigned to item-input-file and item-input-path
			2. CreateItemInput should see that no file or path exists and return null
			3. Assert that it is actually null
	 */
	@Test
	void testCreateEmptyItemInput() throws InterruptedException {
		// Suppress expected WARN from ItemInputFactory when pointing at a directory
		Configurator.setLevel("com.dell.spt.base.logging.Errors", Level.OFF);
		try {
			var mockDriver = DummyStorageDriverMock.create();
			itemInput = ItemInputFactory.createItemInput(testConfig.configVal("item"), batchSize, mockDriver);
			assertNull(itemInput);
		} finally {
			Configurator.setLevel("com.dell.spt.base.logging.Errors", Level.ERROR);
		}
	}

	/*
	 	Description: When a csv file is given, it will create a CsvFileItemInput
		Steps:
			1. Create a csv file
			2. Let ItemInputFactory create a CsvFileItemInput
			3. Assert that it is an actual CsvFileItemInput
	 */
	@Test
	void testCreateCSVItemInput() throws InterruptedException {
		File csvFile = new File(TMP_DIR_PATH.toString(), "test.csv");

		try {
			csvFile.createNewFile();
		} catch (IOException e) {
			LogUtil.exception(Level.WARN, e, "Failed to create the item input file \"{}\"", csvFile);
		}

		testConfig.val("item-input-file", TMP_DIR_PATH.toString() + "/test.csv");
		var mockDriver = DummyStorageDriverMock.create();
		itemInput = ItemInputFactory.createItemInput(testConfig.configVal("item"), batchSize, mockDriver);
		assertInstanceOf(CsvFileItemInput.class, itemInput);
	}

	/*
	 	Description: When a non-csv file is given, it will create a BinFileInput
		Steps:
			1. Create a non-csv file
			2. Serialize data to the file
			3. Let ItemInputFactory create a BinFileInput
			4. Assert that it is an actual BinFileInput given
	 */
	@Test
	void testCreateBinaryItemInput() throws InterruptedException {
		File binFile = new File(TMP_DIR_PATH.toString(), "test");

		try {
			binFile.createNewFile();
		} catch (IOException e) {
			LogUtil.exception(Level.WARN, e, "Failed to create the item input file \"{}\"", binFile);
		}

		// Create some data to serialize
		String data = "Hello, World!";
		try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(binFile))) {
			oos.writeObject(data);
		} catch (IOException e) {
			LogUtil.exception(Level.WARN, e, "Failed to write to the item input file \"{}\"", binFile);
		}

		testConfig.val("item-input-file", TMP_DIR_PATH.toString() + "/test");
		var mockDriver = DummyStorageDriverMock.create();
		itemInput = ItemInputFactory.createItemInput(testConfig.configVal("item"), batchSize, mockDriver);
		assertInstanceOf(BinFileInput.class, itemInput);
	}

	/*
	 	Description: When a path directory is given, it will create a StorageItemInput
		Steps:
			1. Ensure item-input-file is null
			2. Set some basic values for item-naming-prefix and item-naming-radix
			3. Let ItemInputFactory create a StorageItemInput
			4. Assert that it is an actual StorageItemInput
	 */
	@Test
	void testCreatePathItemInput() throws InterruptedException {
		testConfig.val("item-input-file", null); // We need a path, not file
		testConfig.val("item-naming-prefix", null);
		testConfig.val("item-naming-radix", 36);

		var mockDriver = DummyStorageDriverMock.create();
		itemInput = ItemInputFactory.createItemInput(testConfig.configVal("item"), batchSize, mockDriver);
		assertInstanceOf(StorageItemInput.class, itemInput);
	}

	@AfterAll
	static void cleanup() {
		if (TMP_DIR_PATH != null) {
			try (var paths = java.nio.file.Files.walk(TMP_DIR_PATH)) {
				paths.sorted((o1, o2) -> o2.compareTo(o1))
								.forEach(path -> {
									try {
										if (java.nio.file.Files.deleteIfExists(path)) {
											System.out.println("Deleted file: " + path);
										}
									} catch (IOException e) {
										LogUtil.exception(Level.WARN, e, "Failed to Delete temp files");
									}
								});
			} catch (IOException e) {
				LogUtil.exception(Level.WARN, e, "Failed to delete ", TMP_DIR_PATH.toString());
			}
		}
	}

	/* Utility Methods */
	void initDirectory() {
		TMP_DIR_PATH = Path.of(System.getProperty("java.io.tmpdir"), "test_item_input_factory");
		TMP_DIR_PATH.toFile().mkdirs();
	}
}
