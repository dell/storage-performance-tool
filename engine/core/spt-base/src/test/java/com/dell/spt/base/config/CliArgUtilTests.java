// File and class name changed from "CliArgUtilTest" to "CliArgUtilTests" because of a conflict with an exclude in the build.gradle file
package com.dell.spt.base.config;

import static org.junit.jupiter.api.Assertions.*;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class CliArgUtilTests {

	/**
	 * Tests that CliArgUtil.parseArgs can correctly parse command line arguments that include key-value pairs, boolean flags, and empty values.
	 *
	 * @param args	a list of command line arguments
	 * @return         	a map of parsed key-value pairs
	 */
	@Test
	void parseArgs_parsesKeyValueBooleanAndEmpty_andLastWins() {
		// --a=1, --b (boolean), --c= (empty value), then override a w/ 2
		Map<String, String> result = CliArgUtil.parseArgs(
						"--a=1",
						"--b",
						"--c=",
						"--a=2");

		assertEquals("2", result.get("a"), "last wins for duplicate keys");
		assertEquals("true", result.get("b"), "boolean shortcut should map to true");
		assertEquals("", result.get("c"), "explicit empty value should be preserved");
		assertEquals(3, result.size(), "only 3 distinct keys should remain");
	}

	/**
	 * Tests that CliArgUtil.parseArgs can correctly parse command line arguments that include key-value pairs, boolean flags, and empty values.
	 *
	 * @param args	a list of command line arguments
	 * @return         	a map of parsed key-value pairs
	 */
	@Test
	void parseArgs_allowsHyphenatedKeys() {
		Map<String, String> result = CliArgUtil.parseArgs(
						"--feature-enabled",
						"--log-level=debug",
						"--max-retry-count=5");

		assertEquals("true", result.get("feature-enabled"));
		assertEquals("debug", result.get("log-level"));
		assertEquals("5", result.get("max-retry-count"));
		assertEquals(3, result.size());
	}

	/**
	 * Tests that CliArgUtil.parseArgs throws an exception when the argument prefix is missing.
	 *
	 * @param args	a list of command line arguments
	 * @return         	a map of parsed key-value pairs
	 */
	@Test
	void parseArgs_throwsOnBadPrefix() {
		// Missing the required "--" prefix
		IllegalArgumentNameException ex = assertThrows(
						IllegalArgumentNameException.class,
						() -> CliArgUtil.parseArgs("-a=1"));
		assertTrue(ex.getMessage() == null || ex.getMessage().contains("-a=1"),
						"Exception message should reference the offending arg");
	}

	/**
	 * Tests that CliArgUtil.allCliArgs() correctly flattens nested schemas into command line arguments.
	 *
	 * @param  schema	a nested schema with hyphenated path separators
	 * @return         	a list of command line arguments
	 */
	@Test
	void allCliArgs_flattensNestedSchema_withDefaultDashSeparator() {
		Map<String, Object> leafs = new LinkedHashMap<>();
		leafs.put("child", "VAL2");
		leafs.put("leaf", 123);

		Map<String, Object> schema = new LinkedHashMap<>();
		schema.put("flat", "VAL");
		schema.put("top", leafs);

		List<String> args = CliArgUtil.allCliArgs(schema, CliArgUtil.ARG_PATH_SEP);

		var expected = new HashSet<>(List.of(
						"--flat=<VAL>",
						"--top-child=<VAL2>",
						"--top-leaf=<123>"));
		assertEquals(expected, new HashSet<>(args));
	}

	/**
	 * Tests that CliArgUtil.argsFromSchemaEntry supports custom separators and recursion.
	 *
	 * @param  paramName	a schema entry
	 * @return         	a list of flattened arguments
	 */
	@Test
	void argsFromSchemaEntry_supportsCustomSeparatorAndRecursion() {
		Map<String, Object> nestedLevel2 = Map.of("c", "X");
		Map<String, Object> nestedLevel1 = Map.of("b", nestedLevel2);

		var entry = new AbstractMap.SimpleEntry<String, Object>("a", nestedLevel1);

		// Use custom separator "_"
		List<String> flattened = CliArgUtil.argsFromSchemaEntry(
						CliArgUtil.ARG_PREFIX,
						"_",
						entry);

		assertEquals(1, flattened.size());
		assertEquals("--a_b_c=<X>", flattened.get(0));
	}

	/**
	 * Tests that CliArgUtil.parseArgs can correctly parse command line arguments that include key-value pairs, boolean flags, and empty values.
	 *
	 * @param args	a list of command line arguments
	 * @return         	a map of parsed key-value pairs
	 */
	@Test
	void parseArgs_handlesSingleEqualsOnlyOnce() {
		// Ensure only the first '=' splits key/value, leaving the rest intact
		Map<String, String> result = CliArgUtil.parseArgs("--connection=host=localhost:5432");
		assertEquals("host=localhost:5432", result.get("connection"));
	}
}
