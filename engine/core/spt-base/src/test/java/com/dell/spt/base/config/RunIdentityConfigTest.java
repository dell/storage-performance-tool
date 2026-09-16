package com.dell.spt.base.config;

import static org.junit.jupiter.api.Assertions.*;

import com.github.akurilov.confuse.impl.BasicConfig;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RunIdentityConfigTest {
	@Test
	void readsNestedAndLegacyClusterIdentity() {
		assertEquals("nested", resolve(Map.of("run", Map.of("cluster", Map.of("id", "nested")))));
		assertEquals("legacy", resolve(Map.of("run", Map.of("cluster", "legacy"))));
	}

	@Test
	void toleratesMissingAndInvalidPaths() {
		for (final Map<String, Object> values : java.util.List.<Map<String, Object>> of(
						Map.of(), Map.of("run", "scalar"), Map.of("run", Map.of()),
						Map.of("run", Map.of("cluster", Map.of())),
						Map.of("run", Map.of("cluster", 42)),
						Map.of("run", Map.of("cluster", "  ")),
						Map.of("run", Map.of("cluster", Map.of("id", "  "))))) {
			assertNull(resolve(values), values.toString());
		}
	}

	private static String resolve(final Map<String, Object> values) {
		return RunIdentityConfig.clusterId(new BasicConfig("-", schemaFor(values), values));
	}

	private static Map<String, Object> schemaFor(final Map<?, ?> values) {
		final Map<String, Object> schema = new java.util.HashMap<>();
		values.forEach((key, value) -> schema.put((String) key,
						value instanceof Map<?, ?> nested ? schemaFor(nested) : value.getClass()));
		return schema;
	}
}
