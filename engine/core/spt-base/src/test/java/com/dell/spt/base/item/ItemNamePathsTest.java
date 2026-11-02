package com.dell.spt.base.item;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ItemNamePathsTest {

	@ParameterizedTest
	@CsvSource({
			"/abc/k, /abc, k",
			"/abc/s0/k, /abc, s0/k",
			"/abc/abc/k, /abc, abc/k",
			"/abc/k, /abc/, k",
			"abc/k, abc, k",
			"abc/k, abc/, k",
			"/abc/k, abc, k",
			"/abc/k, abc/, k",
			"/abc/pre/k, /abc/pre, k",
			"/k, /, k",
	})
	void holdsPath_whenSameFormAndAtABoundary(
					final String name, final String path, final String relative) {
		assertTrue(ItemNamePaths.holdsPath(name, path));
		assertEquals(relative, ItemNamePaths.relativeToPath(name, path));
	}

	@ParameterizedTest
	@CsvSource({
			// A relative name is a key: it does not hold a path written with a leading slash.
			"abc/k, /abc",
			"abc, /abc",
			// No path boundary.
			"abcdef/k, abc",
			"/abcdef/k, /abc",
			"/abcdef/k, abc",
			"/abcdef, /abc",
			"s0000001/k, s00",
			// The name equals the path, so it names no object under it.
			"/abc, /abc",
			"abc, abc",
			"/abc, abc",
			// Other paths.
			"/zzz/k, /abc",
			"zzz/k, abc",
			"k, /abc",
			"k, ''",
			"/k, ''",
			"'', /abc",
			"'', abc",
	})
	void doesNotHoldPath(final String name, final String path) {
		assertFalse(ItemNamePaths.holdsPath(name, path));
	}

	@ParameterizedTest
	@CsvSource({
			"/abc/k, /abc, true",
			"/abc/k, /abc/, true",
			"abc/k, abc, true",
			// Exactly as written: a relative and an absolute path are different places.
			"/abc/k, abc, false",
			"abc/k, /abc, false",
			"/abcdef/k, /abc, false",
			"abcdef/k, abc, false",
			"/abc, /abc, false",
			"k, '', false",
	})
	void startsWithPath_comparesExactlyAsWritten(
					final String name, final String path, final boolean expected) {
		assertEquals(expected, ItemNamePaths.startsWithPath(name, path));
	}

	@ParameterizedTest
	@CsvSource({
			"/abc, k, /abc/k",
			"/abc/, k, /abc/k",
			"/abc, /k, /abc/k",
			"/abc/, /k, /abc/k",
			"abc, k, abc/k",
			"abc, /k, abc/k",
			"/abc, abc/k, /abc/abc/k",
	})
	void join_usesExactlyOneSeparator(final String path, final String name, final String expected) {
		assertEquals(expected, ItemNamePaths.join(path, name));
	}

	@Test
	void holdsPath_isCaseSensitive() {
		assertFalse(ItemNamePaths.holdsPath("/ABC/k", "/abc"));
	}
}
