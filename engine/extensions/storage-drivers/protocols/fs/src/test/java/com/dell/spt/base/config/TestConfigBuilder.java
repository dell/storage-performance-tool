package com.dell.spt.base.config;

import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Minimal test configuration builder for FS driver tests.
 * Provides only the paths used by the tests and FS driver:
 * - load-batch-size
 * - storage-driver-limit-concurrency
 * - storage-driver-threads
 * - storage-driver-limit-queue-input
 * - storage (namespace, auth, driver with limit and queue)
 */
public final class TestConfigBuilder {

	private static final String PATH_SEPARATOR = "-";

	public static Config config() {
		Map<String, Object> schema = createSchema();
		Map<String, Object> values = createDefaults();
		return new BasicConfig(PATH_SEPARATOR, schema, values);
	}

	private static Map<String, Object> createSchema() {
		Map<String, Object> schema = new HashMap<>();

		// load.batch.size
		Map<String, Object> load = new HashMap<>();
		Map<String, Object> batch = new HashMap<>();
		batch.put("size", Integer.class);
		load.put("batch", batch);
		schema.put("load", load);

		// storage subtree
		Map<String, Object> storage = new HashMap<>();
		storage.put("namespace", String.class);

		Map<String, Object> auth = new HashMap<>();
		auth.put("uid", String.class);
		auth.put("secret", String.class);
		auth.put("token", String.class);
		storage.put("auth", auth);

		Map<String, Object> limitQueue = new HashMap<>();
		limitQueue.put("input", Integer.class);

		Map<String, Object> limit = new HashMap<>();
		limit.put("concurrency", Integer.class);
		limit.put("queue", limitQueue);

		Map<String, Object> driver = new HashMap<>();
		driver.put("type", String.class);
		driver.put("threads", Integer.class);
		driver.put("limit", limit);

		storage.put("driver", driver);

		// net.node.slice is referenced in some code paths; keep minimal
		Map<String, Object> netNode = new HashMap<>();
		netNode.put("slice", Boolean.class);
		Map<String, Object> net = new HashMap<>();
		net.put("node", netNode);
		storage.put("net", net);

		schema.put("storage", storage);

		return schema;
	}

	private static Map<String, Object> createDefaults() {
		Map<String, Object> values = new HashMap<>();

		Map<String, Object> load = new HashMap<>();
		Map<String, Object> batch = new HashMap<>();
		batch.put("size", 16); // may be overridden in tests
		load.put("batch", batch);
		values.put("load", load);

		Map<String, Object> storage = new HashMap<>();
		storage.put("namespace", "");

		Map<String, Object> auth = new HashMap<>();
		auth.put("uid", "");
		auth.put("secret", "");
		auth.put("token", "");
		storage.put("auth", auth);

		Map<String, Object> limitQueue = new HashMap<>();
		limitQueue.put("input", 128);

		Map<String, Object> limit = new HashMap<>();
		limit.put("concurrency", 4);
		limit.put("queue", limitQueue);

		Map<String, Object> driver = new HashMap<>();
		driver.put("type", "dummy-mock");
		driver.put("threads", 0);
		driver.put("limit", limit);

		storage.put("driver", driver);

		Map<String, Object> netNode = new HashMap<>();
		netNode.put("slice", false);
		Map<String, Object> net = new HashMap<>();
		net.put("node", netNode);
		storage.put("net", net);

		values.put("storage", storage);

		return values;
	}
}
