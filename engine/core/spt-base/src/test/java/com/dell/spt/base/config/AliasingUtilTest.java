package com.dell.spt.base.config;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class AliasingUtilTest {

	private static Map<String, Object> aliasEntry(String name, String target, boolean deprecated) {
		Map<String, Object> m = new LinkedHashMap<>();
		m.put(AliasingUtil.NAME, name);
		m.put(AliasingUtil.TARGET, target);
		if (deprecated) {
			m.put(AliasingUtil.DEPRECATED, true);
		}
		return m;
	}

	/**
	 * Verifies that the AliasingUtil.apply function leaves the arguments unchanged when there is no matching alias.
	 */
	@Test
	void noMatchLeavesArgsAlone() {
		Map<String, String> args = new LinkedHashMap<>();
		args.put("foo", "1");

		List<Map<String, Object>> cfg = List.of(aliasEntry("bar", "baz", false));

		Map<String, String> out = AliasingUtil.apply(args, cfg);
		assertEquals(Map.of("foo", "1"), out);
	}

	/**
	 * Verifies that the AliasingUtil.apply function leaves the arguments unchanged when there is no matching alias.
	 */
	@Test
	void renameKeepsValue() {
		Map<String, String> args = Map.of("old", "123");
		List<Map<String, Object>> cfg = List.of(aliasEntry("old", "new", false));

		Map<String, String> out = AliasingUtil.apply(args, cfg);
		assertEquals("123", out.get("new"));
		assertFalse(out.containsKey("old"));
	}

	/**
	 * Verifies that the AliasingUtil.apply function leaves the arguments unchanged when there is no matching alias.
	 */
	@Test
	void renameOverridesValue() {
		Map<String, String> args = Map.of("old", "123");
		List<Map<String, Object>> cfg = List.of(aliasEntry("old", "new=456", false));

		Map<String, String> out = AliasingUtil.apply(args, cfg);
		assertEquals("456", out.get("new"));
	}

	/**
	 * Verifies that the AliasingUtil.apply function splits the first equals sign in the target value.
	 */
	@Test
	void onlyFirstEqualsSplits() {
		Map<String, String> args = Map.of("old", "orig");
		List<Map<String, Object>> cfg = List.of(aliasEntry("old", "new=left=right", false));

		Map<String, String> out = AliasingUtil.apply(args, cfg);
		assertEquals("left=right", out.get("new"));
	}

	/**
	 * Verifies that the AliasingUtil.apply function leaves the arguments unchanged when there is no matching alias.
	 */
	@Test
	void emptyValueOverride() {
		Map<String, String> args = Map.of("old", "orig");
		List<Map<String, Object>> cfg = List.of(aliasEntry("old", "new=", false));

		Map<String, String> out = AliasingUtil.apply(args, cfg);
		assertEquals("", out.get("new"));
	}

	/**
	 * Verifies that the AliasingUtil.apply function throws an IllegalArgumentException when a deprecated alias has a null target.
	 */
	@Test
	void deprecatedWithNullTargetThrows() {
		Map<String, String> args = Map.of("old", "v");
		List<Map<String, Object>> cfg = List.of(aliasEntry("old", null, false));

		IllegalArgumentException ex = assertThrows(
						IllegalArgumentException.class,
						() -> AliasingUtil.apply(args, cfg));
		assertTrue(ex.getMessage().contains("\"old\""));
		assertTrue(ex.getMessage().toLowerCase(Locale.ROOT).contains("deprecated"));
	}

	/**
	 * Verifies that the AliasingUtil.apply function leaves the arguments unchanged when there is no matching alias.
	 */
	@Test
	void onlyMatchingArgsTransformed() {
		Map<String, String> args = new LinkedHashMap<>();
		args.put("keep", "A");
		args.put("old", "B");

		List<Map<String, Object>> cfg = List.of(
						aliasEntry("old", "new", false),
						aliasEntry("nope", "ignored", false));

		Map<String, String> out = AliasingUtil.apply(args, cfg);
		assertEquals("A", out.get("keep"));
		assertEquals("B", out.get("new"));
		assertFalse(out.containsKey("old"));
	}

	@Test
	void splitLogRateAliasTargetsSnakeCaseKey() {
		Map<String, String> args = Map.of("load-op-list-sharding-split-log-rate", "7");
		List<Map<String, Object>> cfg = List.of(aliasEntry("load-op-list-sharding-split-log-rate", "load-op-list-sharding-split_log_rate", false));

		Map<String, String> out = AliasingUtil.apply(args, cfg);
		assertEquals("7", out.get("load-op-list-sharding-split_log_rate"));
	}

	@Test
	void listAccelAliasTargetsModeKey() {
		Map<String, String> args = Map.of("list-accel", "auto");
		List<Map<String, Object>> cfg = List.of(aliasEntry("list-accel", "load-op-list-sharding-mode", false));

		Map<String, String> out = AliasingUtil.apply(args, cfg);
		assertEquals("auto", out.get("load-op-list-sharding-mode"));
	}

	@Test
	void deprecatedSplitLogSampleAliasStillMaps() {
		Map<String, String> args = Map.of("load-op-list-sharding-split-log-sample", "9");
		List<Map<String, Object>> cfg = List.of(aliasEntry("load-op-list-sharding-split-log-sample", "load-op-list-sharding-split_log_rate", true));

		Map<String, String> out = AliasingUtil.apply(args, cfg);
		assertEquals("9", out.get("load-op-list-sharding-split_log_rate"));
	}
}
