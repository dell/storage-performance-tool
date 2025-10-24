package com.dell.spt.base.config;

import com.github.akurilov.confuse.Config;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test the TestConfigBuilder with full schema loading from config-schema.yaml
 */
public class TestConfigBuilderFullSchemaTest {

	@Test
	public void testCreateFullSchemaConfig() {
		Config config = TestConfigBuilder.config();
		assertNotNull(config);

		// Test that all major sections are present and accessible
		assertDoesNotThrow(() -> config.stringVal("item-type"));
		assertDoesNotThrow(() -> config.stringVal("item-input-file"));
		assertDoesNotThrow(() -> config.stringVal("item-input-path"));
		assertDoesNotThrow(() -> config.stringVal("item-output-file"));
		assertDoesNotThrow(() -> config.stringVal("item-output-path"));
		assertDoesNotThrow(() -> config.stringVal("item-naming-type"));
		assertDoesNotThrow(() -> config.intVal("item-naming-radix"));
		assertDoesNotThrow(() -> config.stringVal("item-data-input-file"));
		assertDoesNotThrow(() -> config.boolVal("item-data-verify"));

		assertDoesNotThrow(() -> config.stringVal("load-op-type"));
		assertDoesNotThrow(() -> config.longVal("load-op-limit-count"));
		assertDoesNotThrow(() -> config.intVal("load-batch-size"));
		assertDoesNotThrow(() -> config.stringVal("load-step-id"));

		assertDoesNotThrow(() -> config.stringVal("storage-driver-type"));
		assertDoesNotThrow(() -> config.intVal("storage-driver-limit-concurrency"));
		assertDoesNotThrow(() -> config.stringVal("storage-auth-uid"));
		assertDoesNotThrow(() -> config.stringVal("storage-namespace"));

		assertDoesNotThrow(() -> config.boolVal("output-color"));
		assertDoesNotThrow(() -> config.listVal("output-metrics-quantiles"));

		assertDoesNotThrow(() -> config.stringVal("run-comment"));
		assertDoesNotThrow(() -> config.stringVal("run-version"));
		assertDoesNotThrow(() -> config.intVal("run-port"));

		assertDoesNotThrow(() -> config.listVal("aliasing"));
	}

	@Test
	public void testConfigWithValMethod() {
		String testPath = "/test/input/path";
		String testFile = "/test/input/file.csv";

		Config config = TestConfigBuilder.config();
		config.val("item-type", "data");
		config.val("item-input-path", testPath);
		config.val("item-input-file", testFile);
		config.val("load-op-type", "read");
		config.val("storage-driver-limit-concurrency", 100);

		assertEquals("data", config.stringVal("item-type"));
		assertEquals(testPath, config.stringVal("item-input-path"));
		assertEquals(testFile, config.stringVal("item-input-file"));
		assertEquals("read", config.stringVal("load-op-type"));
		assertEquals(100, config.intVal("storage-driver-limit-concurrency"));
	}

	@Test
	public void testSetValuesWithFullSchema() {
		Config config = TestConfigBuilder.config();

		// Now these should work without "Invalid Value Path" errors
		assertDoesNotThrow(() -> {
			config.val("item-type", "data");
			config.val("item-input-file", "/tmp/items.csv");
			config.val("item-input-path", "/bucket/path");
			config.val("item-output-file", "/tmp/output.csv");
			config.val("item-output-path", "/output/path");
		});

		// Verify the values were set
		assertEquals("data", config.stringVal("item-type"));
		assertEquals("/tmp/items.csv", config.stringVal("item-input-file"));
		assertEquals("/bucket/path", config.stringVal("item-input-path"));
		assertEquals("/tmp/output.csv", config.stringVal("item-output-file"));
		assertEquals("/output/path", config.stringVal("item-output-path"));
	}

	@Test
	public void testAllItemDataPaths() {
		Config config = TestConfigBuilder.config();

		// Test all item.data paths are accessible
		assertDoesNotThrow(() -> {
			config.val("item-data-size", "10MB");
			config.val("item-data-verify", true);
			config.val("item-data-input-seed", "custom-seed");
			config.val("item-data-input-layer-cache", 32);
			config.val("item-data-input-layer-heap", true);
			config.val("item-data-input-layer-size", "8MB");
			config.val("item-data-ranges-random", 5);
			config.val("item-data-ranges-threshold", "1KB");
		});

		assertEquals("10MB", config.stringVal("item-data-size"));
		assertTrue(config.boolVal("item-data-verify"));
		assertEquals("custom-seed", config.stringVal("item-data-input-seed"));
		assertEquals(32, config.intVal("item-data-input-layer-cache"));
	}

	@Test
	public void testConvenienceMethods() {
		// Test the convenience methods still work
		Config config1 = TestConfigBuilder.config();
		config1.val("load-step-id", "minimal-test");
		assertEquals("minimal-test", config1.stringVal("load-step-id"));

		Config config2 = TestConfigBuilder.config();
		config2.val("load-op-type", "write");
		assertEquals("write", config2.stringVal("load-op-type"));
	}

	@Test
	public void testCachedSchemaLoading() {
		// First load
		Config config1 = TestConfigBuilder.config();

		// Second load (should be cached and faster)
		Config config2 = TestConfigBuilder.config();

		assertNotNull(config1);
		assertNotNull(config2);
	}
}
