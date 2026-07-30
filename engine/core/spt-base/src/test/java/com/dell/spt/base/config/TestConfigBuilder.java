package com.dell.spt.base.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Test configuration builder that creates valid Config instances with full schema
 * loaded from config-schema.yaml resource.
 * 
 * Usage:
 * Config config = TestConfigBuilder.config();
 * config.val("item-input-file", "/test/path");
 * config.val("load-op-type", "read");
 * 
 * This uses the same API as production code.
 */
public class TestConfigBuilder {

	/**
	 * Path separator used by confuse library for hyphenated path translation
	 */
	private static final String PATH_SEPARATOR = "-";

	/**
	 * Cache for the full schema loaded from YAML
	 */
	private static Map<String, Object> cachedFullSchema = null;

	/**
	 * Create test configuration with full schema loaded from config-schema.yaml.
	 * All configuration paths are valid when using this method.
	 */
	public static Config config() {
		Map<String, Object> schema = loadFullSchema();
		Map<String, Object> values = createFullValues();
		return new BasicConfig(PATH_SEPARATOR, schema, values);
	}

	/**
	 * Load and parse the full schema from config-schema.yaml resource.
	 * The schema is cached after first load for performance.
	 */
	@SuppressWarnings("unchecked")
	private static Map<String, Object> loadFullSchema() {
		if (cachedFullSchema != null) {
			return cachedFullSchema;
		}

		try (InputStream schemaStream = TestConfigBuilder.class.getResourceAsStream("/config-schema.yaml")) {
			if (schemaStream == null) {
				throw new RuntimeException("config-schema.yaml not found in resources");
			}

			ObjectMapper yamlMapper = new YAMLMapper();
			Map<String, Object> yamlSchema = yamlMapper.readValue(schemaStream, Map.class);

			// Convert the YAML schema (with string type names) to Java Class objects
			Map<String, Object> schema = convertSchemaTypes(yamlSchema);
			cachedFullSchema = schema;
			return schema;
		} catch (IOException e) {
			throw new RuntimeException("Failed to load config-schema.yaml", e);
		}
	}

	/**
	 * Convert YAML schema type strings to Java Class objects.
	 * Recursively processes the schema tree, replacing type strings with Class references.
	 */
	@SuppressWarnings("unchecked")
	private static Map<String, Object> convertSchemaTypes(Map<String, Object> yamlSchema) {
		Map<String, Object> converted = new HashMap<>();

		for (Map.Entry<String, Object> entry : yamlSchema.entrySet()) {
			Object value = entry.getValue();
			if (value instanceof Map) {
				// Recursively convert nested maps
				converted.put(entry.getKey(), convertSchemaTypes((Map<String, Object>) value));
			} else if (value instanceof String) {
				// Convert type string to Class object
				converted.put(entry.getKey(), stringToClass((String) value));
			} else {
				// Keep other types as-is (e.g., lists)
				converted.put(entry.getKey(), value);
			}
		}

		return converted;
	}

	/**
	 * Map YAML type strings to Java Class objects.
	 * This mapping matches what the confuse library expects.
	 */
	private static Class<?> stringToClass(String typeName) {
		switch (typeName) {
		case "string":
			return String.class;
		case "int":
			return Integer.class;
		case "long":
			return Long.class;
		case "double":
			return Double.class;
		case "boolean":
			return Boolean.class;
		case "list":
			return List.class;
		case "any":
			return Object.class;
		default:
			throw new IllegalArgumentException("Unknown type in schema: " + typeName);
		}
	}

	/**
	 * Create full default values matching the complete schema structure.
	 * This provides sensible defaults for all configuration paths.
	 */
	private static Map<String, Object> createFullValues() {
		Map<String, Object> values = new HashMap<>();

		// item section
		Map<String, Object> item = new HashMap<>();
		item.put("type", "data");

		Map<String, Object> itemData = new HashMap<>();
		Map<String, Object> itemDataInput = new HashMap<>();
		itemDataInput.put("file", "");
		itemDataInput.put("seed", "7a42d9c483244167");
		Map<String, Object> itemDataInputLayer = new HashMap<>();
		itemDataInputLayer.put("cache", 16);
		itemDataInputLayer.put("heap", false);
		itemDataInputLayer.put("size", "4MB");
		itemDataInput.put("layer", itemDataInputLayer);
		itemData.put("input", itemDataInput);

		Map<String, Object> itemDataRanges = new HashMap<>();
		itemDataRanges.put("concat", "");
		itemDataRanges.put("fixed", List.of());
		itemDataRanges.put("random", 0);
		itemDataRanges.put("threshold", 0);
		itemData.put("ranges", itemDataRanges);

		itemData.put("size", "1MB");
		itemData.put("verify", false);
		item.put("data", itemData);

		Map<String, Object> itemInput = new HashMap<>();
		itemInput.put("file", "");
		itemInput.put("path", "");
		item.put("input", itemInput);

		Map<String, Object> itemNaming = new HashMap<>();
		itemNaming.put("type", "random");
		itemNaming.put("prefix", "");
		itemNaming.put("shards", 0);
		itemNaming.put("radix", 36);
		itemNaming.put("step", 1);
		itemNaming.put("seed", 0L);
		itemNaming.put("length", 12);
		item.put("naming", itemNaming);

		Map<String, Object> itemOutput = new HashMap<>();
		itemOutput.put("file", "");
		itemOutput.put("path", "");
		item.put("output", itemOutput);

		values.put("item", item);

		// load section (reuse existing minimal values and extend)
		Map<String, Object> load = new HashMap<>();
		Map<String, Object> batch = new HashMap<>();
		batch.put("size", 100);
		load.put("batch", batch);

		Map<String, Object> step = new HashMap<>();
		step.put("id", "test-step-001");
		step.put("idAutoGenerated", false);
		Map<String, Object> stepLimit = new HashMap<>();
		stepLimit.put("size", "100MB");
		stepLimit.put("time", "60s");
		step.put("limit", stepLimit);
		Map<String, Object> stepNode = new HashMap<>();
		stepNode.put("addrs", List.of());
		stepNode.put("port", 9020);
		step.put("node", stepNode);
		load.put("step", step);

		Map<String, Object> service = new HashMap<>();
		service.put("threads", 0);
		load.put("service", service);

		Map<String, Object> op = new HashMap<>();
		Map<String, Object> opLimit = new HashMap<>();
		opLimit.put("count", 0L);
		opLimit.put("rate", 0.0);
		opLimit.put("recycle", 1000000);
		Map<String, Object> opLimitFail = new HashMap<>();
		opLimitFail.put("count", 100000L);
		opLimitFail.put("rate", false);
		opLimit.put("fail", opLimitFail);
		op.put("limit", opLimit);

		Map<String, Object> opOutput = new HashMap<>();
		opOutput.put("duplicates", false);
		op.put("output", opOutput);

		Map<String, Object> opRecycle = new HashMap<>();
		opRecycle.put("mode", false);
		Map<String, Object> opRecycleContent = new HashMap<>();
		opRecycleContent.put("update", false);
		opRecycle.put("content", opRecycleContent);
		op.put("recycle", opRecycle);

		op.put("retry", false);
		op.put("retryLimit", 10);
		op.put("shuffle", false);
		op.put("type", "create");

		Map<String, Object> opWait = new HashMap<>();
		opWait.put("finish", false);
		opWait.put("limit", 0);
		op.put("wait", opWait);

		load.put("op", op);
		values.put("load", load);

		// storage section
		Map<String, Object> storage = new HashMap<>();
		Map<String, Object> auth = new HashMap<>();
		auth.put("file", "");
		auth.put("secret", "");
		auth.put("token", "");
		auth.put("uid", "");
		storage.put("auth", auth);

		Map<String, Object> driver = new HashMap<>();
		driver.put("type", "dummy-mock");
		driver.put("threads", 0);
		Map<String, Object> driverLimit = new HashMap<>();
		driverLimit.put("concurrency", 10);
		Map<String, Object> driverLimitQueue = new HashMap<>();
		driverLimitQueue.put("input", 1000000);
		driverLimit.put("queue", driverLimitQueue);
		driver.put("limit", driverLimit);
		storage.put("driver", driver);

		Map<String, Object> integrity = new HashMap<>();
		integrity.put("mode", "none");
		integrity.put("algorithm", "sha256");
		Map<String, Object> integrityInput = new HashMap<>();
		integrityInput.put("provenance", "none");
		integrityInput.put("expectedProducerId", "");
		integrity.put("input", integrityInput);
		storage.put("integrity", integrity);

		storage.put("namespace", "");
		Map<String, Object> storageNet = new HashMap<>();
		Map<String, Object> storageNetNode = new HashMap<>();
		storageNetNode.put("slice", false);
		storageNet.put("node", storageNetNode);
		storage.put("net", storageNet);

		values.put("storage", storage);

		// output section
		Map<String, Object> outputSection = new HashMap<>();
		outputSection.put("color", true);

		Map<String, Object> metrics = new HashMap<>();
		Map<String, Object> metricsAverage = new HashMap<>();
		Map<String, Object> metricsAverageAggregation = new HashMap<>();
		metricsAverageAggregation.put("period", 10);
		metricsAverage.put("aggregation", metricsAverageAggregation);
		metricsAverage.put("period", "10s");
		metricsAverage.put("persist", true);
		Map<String, Object> metricsAverageTable = new HashMap<>();
		Map<String, Object> metricsAverageTableHeader = new HashMap<>();
		metricsAverageTableHeader.put("period", 20);
		metricsAverageTable.put("header", metricsAverageTableHeader);
		metricsAverage.put("table", metricsAverageTable);
		metrics.put("average", metricsAverage);

		Map<String, Object> metricsSummary = new HashMap<>();
		metricsSummary.put("persist", true);
		metrics.put("summary", metricsSummary);

		Map<String, Object> metricsTrace = new HashMap<>();
		metricsTrace.put("persist", true);
		metrics.put("trace", metricsTrace);

		metrics.put("threshold", 0.0);

		Map<String, Object> metricsTiming = new HashMap<>();
		metricsTiming.put("persist", false);
		metrics.put("timing", metricsTiming);

		metrics.put("quantiles", List.of(0.25, 0.5, 0.75));
		outputSection.put("metrics", metrics);

		values.put("output", outputSection);

		// run section
		Map<String, Object> run = new HashMap<>();
		run.put("id", 0L);
		run.put("comment", "");
		run.put("node", false);
		run.put("port", 9999);
		run.put("scenario", "");
		run.put("version", "test-version");
		values.put("run", run);

		Map<String, Object> server = new HashMap<>();
		Map<String, Object> serverMetrics = new HashMap<>();
		serverMetrics.put("expose_fleet", true);
		server.put("metrics", serverMetrics);
		values.put("server", server);

		// aliasing section
		values.put("aliasing", List.of());

		return values;
	}

}
