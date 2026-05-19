package com.dell.spt.base.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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

	@Test
	public void testTrimLoadOpNoiseMixedRemovesScalarWeightOnly() {
		final Map<String, Object> configTree = new HashMap<>();
		final Map<String, Object> load = new HashMap<>();
		final Map<String, Object> op = new HashMap<>();
		final Map<String, Object> weights = new HashMap<>();
		weights.put("get", 50);
		weights.put("put", 50);
		weights.put("delete", 0);
		weights.put("stat", 0);
		op.put("weight", 1);
		op.put("weights", weights);
		load.put("op", op);
		configTree.put("load", load);

		ConfigUtil.trimLoadOpNoise(configTree, ConfigUtil.MIXED_LOAD_STEP_TYPE);

		assertNull(op.get("weight"));
		assertNotNull(op.get("weights"));
	}

	@Test
	public void testTrimLoadOpNoiseNonMixedRemovesWeightsAndLegacyWeightMap() {
		final Map<String, Object> configTree = new HashMap<>();
		final Map<String, Object> load = new HashMap<>();
		final Map<String, Object> op = new HashMap<>();
		final Map<String, Object> legacyWeightMap = new HashMap<>();
		legacyWeightMap.put("get", 45);
		legacyWeightMap.put("put", 15);
		legacyWeightMap.put("delete", 10);
		legacyWeightMap.put("stat", 30);
		final Map<String, Object> weights = new HashMap<>();
		weights.put("get", 45);
		weights.put("put", 15);
		weights.put("delete", 10);
		weights.put("stat", 30);
		op.put("weight", legacyWeightMap);
		op.put("weights", weights);
		load.put("op", op);
		configTree.put("load", load);

		ConfigUtil.trimLoadOpNoise(configTree, "Load");

		assertNull(op.get("weight"));
		assertNull(op.get("weights"));
	}

	@Test
	public void testTrimLoadOpNoiseNonMixedKeepsScalarWeight() {
		final Map<String, Object> configTree = new HashMap<>();
		final Map<String, Object> load = new HashMap<>();
		final Map<String, Object> op = new HashMap<>();
		final Map<String, Object> weights = new HashMap<>();
		weights.put("get", 45);
		weights.put("put", 15);
		weights.put("delete", 10);
		weights.put("stat", 30);
		op.put("weight", 1);
		op.put("weights", weights);
		load.put("op", op);
		configTree.put("load", load);

		ConfigUtil.trimLoadOpNoise(configTree, "Load");

		assertEquals(1, op.get("weight"));
		assertNull(op.get("weights"));
	}

	@Test
	public void testTrimLoadOpNoiseGuardReturns() {
		assertDoesNotThrow(() -> ConfigUtil.trimLoadOpNoise(null, "Load"));
		assertDoesNotThrow(() -> ConfigUtil.trimLoadOpNoise(new HashMap<>(), null));
		assertDoesNotThrow(() -> ConfigUtil.trimLoadOpNoise(new HashMap<>(), ""));

		final Map<String, Object> configTreeWithNonMapLoad = new HashMap<>();
		configTreeWithNonMapLoad.put("load", 1);
		assertDoesNotThrow(() -> ConfigUtil.trimLoadOpNoise(configTreeWithNonMapLoad, "Load"));

		final Map<String, Object> configTreeWithoutOp = new HashMap<>();
		configTreeWithoutOp.put("load", new HashMap<>());
		assertDoesNotThrow(() -> ConfigUtil.trimLoadOpNoise(configTreeWithoutOp, "Load"));

		final Map<String, Object> configTreeWithNonMapOp = new HashMap<>();
		final Map<String, Object> load = new HashMap<>();
		load.put("op", 1);
		configTreeWithNonMapOp.put("load", load);
		assertDoesNotThrow(() -> ConfigUtil.trimLoadOpNoise(configTreeWithNonMapOp, "Load"));
	}
}
