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
	 * Tests that CliArgUtil.parseArgs only splits on the first equals sign.
	 */
	@Test
	void parseArgs_handlesSingleEqualsOnlyOnce() {
		// Ensure only the first '=' splits key/value, leaving the rest intact
		Map<String, String> result = CliArgUtil.parseArgs("--connection=host=localhost:5432");
		assertEquals("host=localhost:5432", result.get("connection"));
	}

	@Test
	void usage_listsSortedOptionsWithReadableTypesAndTheAliases() {
		final Map<String, Object> schema = new LinkedHashMap<>();
		final Map<String, Object> run = new LinkedHashMap<>();
		run.put("node", boolean.class);
		run.put("port", int.class);
		schema.put("run", run);
		final Map<String, Object> net = new LinkedHashMap<>();
		net.put("nodeInterfaces", Map.class);
		net.put("addrs", List.class);
		schema.put("net", net);
		schema.put("count", long.class);
		schema.put("ratio", double.class);
		schema.put("name", String.class);
		schema.put("anything", Object.class);
		final List<Map<String, Object>> aliasing = List.of(
						Map.of(AliasingUtil.NAME, "read", AliasingUtil.TARGET, "name=read"),
						Map.of(AliasingUtil.NAME, "old-count", AliasingUtil.TARGET, "count", AliasingUtil.DEPRECATED, true),
						Map.of(AliasingUtil.NAME, "lo", AliasingUtil.TARGET, "net-nodeInterfaces-host=lo"),
						Map.of(AliasingUtil.NAME, "old-loop", AliasingUtil.TARGET, "loop", AliasingUtil.DEPRECATED, true),
						Map.of(AliasingUtil.NAME, "gone", AliasingUtil.DEPRECATED, true),
						Map.of(AliasingUtil.NAME, "remote", AliasingUtil.DEPRECATED, true, AliasingUtil.REASON, "no such model."));

		assertEquals(
						"Options:\n"
										+ "\t--anything=<value>\n"
										+ "\t--count=<long>\n"
										+ "\t--name=<string>\n"
										+ "\t--net-addrs=<list>\n"
										+ "\t--net-nodeInterfaces-<key>=<value>\n"
										+ "\t--ratio=<double>\n"
										+ "\t--run-node=<true|false>\n"
										+ "\t--run-port=<int>\n"
										+ "\nAliases:\n"
										+ "\t--read  same as --name=read\n"
										+ "\t--old-count  same as --count (deprecated)\n"
										+ "\t--lo  same as --net-nodeInterfaces-host=lo\n"
										+ "\t--old-loop  no longer supported: --loop is not an option\n"
										+ "\t--gone  no longer supported\n"
										+ "\t--remote  not supported: no such model.\n",
						CliArgUtil.usage(schema, aliasing));
	}

	@Test
	void usage_withoutAliasesListsOnlyTheOptions() {
		assertEquals("Options:\n\t--a=<int>\n", CliArgUtil.usage(Map.of("a", Integer.class), List.of()));
	}
}
