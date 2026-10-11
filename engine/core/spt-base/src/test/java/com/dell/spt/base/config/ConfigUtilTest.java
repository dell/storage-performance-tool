package com.dell.spt.base.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
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

	@Test
	public void maskedStringHidesCredentialsWithoutChangingLiveConfig() throws Exception {
		final var config = TestConfigBuilder.config();
		config.val("storage-auth-uid", "uid-value-1");
		config.val("storage-auth-secret", "secret-value-1");
		config.val("storage-auth-token", "token-value-1");

		final var masked = ConfigUtil.toMaskedString(config, ConfigFormat.YAML, "Load");

		for (final var value : List.of("uid-value-1", "secret-value-1", "token-value-1")) {
			assertFalse(masked.contains(value), value);
		}
		final var auth = authOf(parse(masked));
		for (final var key : ConfigUtil.MASKED_AUTH_KEYS) {
			assertEquals(ConfigUtil.MASKED_VALUE, auth.get(key), key);
		}
		assertEquals("secret-value-1", config.stringVal("storage-auth-secret"));
		assertTrue(ConfigUtil.toString(config, ConfigFormat.YAML, "Load").contains("secret-value-1"));
	}

	@Test
	public void maskedStringChangesOnlyCredentialValues() throws Exception {
		for (final var stepType : Arrays.asList(null, "Load", ConfigUtil.MIXED_LOAD_STEP_TYPE)) {
			final var config = TestConfigBuilder.config();
			config.val("storage-auth-uid", "uid-value-2");
			config.val("storage-auth-secret", "secret-value-2");
			final var expected = parse(ConfigUtil.toString(config, ConfigFormat.YAML, stepType));
			authOf(expected).put("uid", ConfigUtil.MASKED_VALUE);
			authOf(expected).put("secret", ConfigUtil.MASKED_VALUE);

			assertEquals(expected, parse(ConfigUtil.toMaskedString(config, ConfigFormat.YAML, stepType)), stepType);
		}
	}

	@Test
	public void maskedStringKeepsUnsetAndEmptyCredentials() throws Exception {
		final var config = TestConfigBuilder.config();
		config.val("storage-auth-uid", "");
		config.val("storage-auth-secret", null);

		final var auth = authOf(parse(ConfigUtil.toMaskedString(config, ConfigFormat.YAML, null)));

		assertEquals("", auth.get("uid"));
		assertTrue(auth.containsKey("secret"));
		assertNull(auth.get("secret"));
		assertEquals("", auth.get("token"));
	}

	@Test
	public void maskCredentialsCopiesInsteadOfModifyingSharedMaps() {
		final Map<String, Object> auth = Map.of("uid", "u", "secret", "s", "file", "/creds.csv");
		final Map<String, Object> storage = Map.of("auth", auth, "driver", Map.of("type", "s3"));
		final Map<String, Object> configTree = new HashMap<>(Map.of("storage", storage));

		ConfigUtil.maskCredentials(configTree);

		assertEquals(
						Map.of("auth", Map.of("uid", "***", "secret", "***", "file", "/creds.csv"), "driver", Map.of("type", "s3")),
						configTree.get("storage"));
		assertEquals("s", auth.get("secret"));
	}

	@Test
	public void maskCredentialsIgnoresTreesWithoutAuth() {
		final Map<String, Object> noStorage = new HashMap<>(Map.of("load", Map.of()));
		final Map<String, Object> noAuth = new HashMap<>(Map.of("storage", Map.of("driver", Map.of())));
		final Map<String, Object> scalarAuth = new HashMap<>(Map.of("storage", Map.of("auth", "x")));

		ConfigUtil.maskCredentials(noStorage);
		ConfigUtil.maskCredentials(noAuth);
		ConfigUtil.maskCredentials(scalarAuth);

		assertEquals(Map.of("load", Map.of()), noStorage);
		assertEquals(Map.of("storage", Map.of("driver", Map.of())), noAuth);
		assertEquals(Map.of("storage", Map.of("auth", "x")), scalarAuth);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> parse(final String yaml) throws Exception {
		return new YAMLMapper().readValue(yaml, Map.class);
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> authOf(final Map<String, Object> configTree) {
		return (Map<String, Object>) ((Map<String, Object>) configTree.get("storage")).get("auth");
	}
}
