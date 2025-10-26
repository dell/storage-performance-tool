package com.dell.spt.base.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.HashMap;
import java.util.Map;

public class ConfigUtilTest {

	@Test
	public void testFlatten() throws Exception {

		final Map<String, Object> srcMap = new HashMap<>();
		final Map<String, Object> aMap = new HashMap<>();
		aMap.put("aa", null);
		aMap.put("bb", 123);
		srcMap.put("a", aMap);
		final Map<String, Object> bMap = new HashMap<>();
		bMap.put("aa", "yohoho");
		bMap.put("bb", true);
		srcMap.put("b", bMap);

		final String sep = "-";
		final Map<String, String> dstMap = new HashMap<>();
		ConfigUtil.flatten(srcMap, dstMap, sep, null);

		assertNull(dstMap.get("a-aa"));
		assertEquals("123", dstMap.get("a-bb"));
		assertEquals("yohoho", dstMap.get("b-aa"));
		assertEquals("true", dstMap.get("b-bb"));
	}
}
