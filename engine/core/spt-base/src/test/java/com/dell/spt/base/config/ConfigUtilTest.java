package com.dell.spt.base.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ConfigUtilTest {

	@Test
	public void testMergeSharesNoMapWithItsInputs() {
		final Map<String, Object> schema = new HashMap<>(Map.of("headers", Map.class));
		final Config first = new BasicConfig(
						"-", schema, new HashMap<>(Map.of("headers", new HashMap<>(Map.of("X-First", "1")))));
		final Config second = new BasicConfig(
						"-", schema, new HashMap<>(Map.of("headers", new HashMap<>(Map.of("X-Second", "2")))));

		final Config merged = ConfigUtil.merge("-", List.of(first, second));

		assertEquals(Map.of("X-First", "1", "X-Second", "2"), merged.mapVal("headers"));
		assertEquals(Map.of("X-First", "1"), first.mapVal("headers"));
		assertEquals(Map.of("X-Second", "2"), second.mapVal("headers"));
		merged.mapVal("headers").put("X-Later", "3");
		assertEquals(Map.of("X-First", "1"), first.mapVal("headers"));
		assertEquals(Map.of("X-Second", "2"), second.mapVal("headers"));
	}

	@Test
	public void testMergeKeepsTheOrderOfAMapValue() {
		final Map<String, Object> schema = new HashMap<>(Map.of("headers", Map.class));
		final Map<String, Object> written = new LinkedHashMap<>();
		for (final String name : List.of("X-Zulu", "X-Alpha", "X-Mike", "X-Bravo", "X-Yankee", "X-Charlie")) {
			written.put(name, "1");
		}
		final Map<String, Object> added = new LinkedHashMap<>();
		added.put("X-Whiskey", "2");
		added.put("X-Delta", "2");
		final Config first = new BasicConfig("-", schema, new HashMap<>(Map.of("headers", written)));
		final Config second = new BasicConfig("-", schema, new HashMap<>(Map.of("headers", added)));

		final Config merged = ConfigUtil.merge("-", List.of(first, second));

		assertEquals(
						List.of("X-Zulu", "X-Alpha", "X-Mike", "X-Bravo", "X-Yankee", "X-Charlie", "X-Whiskey", "X-Delta"),
						List.copyOf(merged.mapVal("headers").keySet()));
	}

	@Test
	public void testMergeSharesNoListWithItsInputs() {
		final Map<String, Object> schema = new HashMap<>(Map.of("names", List.class));
		final List<Object> names = new ArrayList<>(List.of("a"));
		final Config first = new BasicConfig("-", schema, new HashMap<>(Map.of("names", names)));

		final Config merged = ConfigUtil.merge("-", List.of(first));

		assertEquals(names, merged.listVal("names"));
		assertNotSame(names, merged.listVal("names"));
	}

	@Test
	public void testCopyOfSharesNoMapOrListAndKeepsEveryValue() {
		final Map<String, Object> schema = new HashMap<>(Map.of(
						"branch", new HashMap<>(Map.of("headers", Map.class, "names", List.class, "count", int.class)),
						"title", String.class));
		final Map<String, Object> nestedInMap = new HashMap<>(Map.of("inner", "1"));
		final Map<String, Object> headers = new HashMap<>(Map.of("X-Base", "1", "nested", nestedInMap));
		final Map<String, Object> nestedInList = new HashMap<>(Map.of("inner", "2"));
		final List<Object> innerList = new ArrayList<>(List.of("x"));
		final List<Object> names = new ArrayList<>(List.of("a", nestedInList, innerList));
		final Config source = new BasicConfig("-", schema, new HashMap<>(Map.of(
						"branch", new HashMap<>(Map.of("headers", headers, "names", names, "count", 7)),
						"title", "base")));

		final Config copy = ConfigUtil.copyOf(source);

		assertEquals(Config.deepToMap(source), Config.deepToMap(copy));
		assertEquals(7, copy.intVal("branch-count"));
		assertEquals("base", copy.stringVal("title"));
		assertEquals("-", copy.pathSep());
		final Map<String, Object> copiedHeaders = copy.mapVal("branch-headers");
		final List<Object> copiedNames = copy.listVal("branch-names");
		assertNotSame(headers, copiedHeaders);
		assertNotSame(nestedInMap, copiedHeaders.get("nested"));
		assertNotSame(names, copiedNames);
		assertNotSame(nestedInList, copiedNames.get(1));
		assertNotSame(innerList, copiedNames.get(2));
	}

	@Test
	public void testCopyOfTakesAWriteThatTheOriginalDoesNotSee() {
		final Config source = TestConfigBuilder.configWithHttpHeaders(Map.of("X-Base", "1"));

		final Config copy = ConfigUtil.copyOf(source);
		copy.val("storage-net-http-headers-X", "run");

		assertEquals(Map.of("X-Base", "1", "X", "run"), copy.mapVal("storage-net-http-headers"));
		assertEquals(Map.of("X-Base", "1"), source.mapVal("storage-net-http-headers"));
	}

	@Test
	public void testCopyOfKeepsTheOrderOfAMapValue() {
		final Map<String, Object> schema = new HashMap<>(Map.of("headers", Map.class));
		final Map<String, Object> written = new LinkedHashMap<>();
		final List<String> names = List.of("X-Zulu", "X-Alpha", "X-Mike", "X-Bravo", "X-Yankee", "X-Charlie");
		names.forEach(name -> written.put(name, "1"));
		final Config source = new BasicConfig("-", schema, new HashMap<>(Map.of("headers", written)));

		assertEquals(names, List.copyOf(ConfigUtil.copyOf(source).mapVal("headers").keySet()));
	}

	@Test
	@SuppressWarnings("JdkObsolete") // an extension's schema may name LinkedList
	public void testMergeAndCopyOfKeepTheClassOfAListOrMapThatTheSchemaNames() {
		// An extension's defaults may name a concrete class: the configuration refuses any other one.
		final Map<String, Object> schema = new HashMap<>(Map.of("names", LinkedList.class, "sorted", TreeMap.class));
		final List<Object> names = new LinkedList<>(List.of("a", "b"));
		final Map<String, Object> sorted = new TreeMap<>(Comparator.reverseOrder());
		sorted.put("a", "1");
		sorted.put("c", "3");
		sorted.put("b", "2");
		final Config source = new BasicConfig("-", schema, new HashMap<>(Map.of("names", names, "sorted", sorted)));

		final Config merged = assertDoesNotThrow(() -> ConfigUtil.merge("-", List.of(source)));
		final Config copy = assertDoesNotThrow(() -> ConfigUtil.copyOf(source));

		for (final Config config : List.of(merged, copy)) {
			final List<Object> copiedNames = config.listVal("names");
			final Map<String, Object> copiedSorted = config.mapVal("sorted");
			assertEquals(LinkedList.class, copiedNames.getClass());
			assertEquals(names, copiedNames);
			assertNotSame(names, copiedNames);
			assertEquals(TreeMap.class, copiedSorted.getClass());
			assertNotSame(sorted, copiedSorted);
			copiedSorted.put("d", "4");
			assertEquals(List.of("d", "c", "b", "a"), List.copyOf(copiedSorted.keySet()));
		}
		assertEquals(List.of("c", "b", "a"), List.copyOf(sorted.keySet()));
	}

	@Test
	public void testCopyOfKeepsAnyOtherListAndTheOrderOfAnyOtherMap() {
		final Map<String, Object> schema = new HashMap<>(Map.of("names", List.class, "headers", Map.class));
		final List<Object> names = Collections.unmodifiableList(new ArrayList<>(List.of("a")));
		final Map<String, Object> written = new LinkedHashMap<>();
		final List<String> headerNames = List.of("X-Zulu", "X-Alpha", "X-Mike", "X-Bravo", "X-Yankee", "X-Charlie");
		headerNames.forEach(name -> written.put(name, "1"));
		final Config source = new BasicConfig("-", schema, new HashMap<>(Map.of(
						"names", names, "headers", Collections.unmodifiableMap(written))));

		final Config copy = ConfigUtil.copyOf(source);

		assertSame(names, copy.listVal("names"));
		assertEquals(headerNames, List.copyOf(copy.mapVal("headers").keySet()));
		copy.mapVal("headers").put("X-Later", "2");
		assertEquals(headerNames, List.copyOf(written.keySet()));
	}

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

	@Test
	public void maskedStringHidesListedHeadersInAnyCaseWithoutChangingLiveConfig() throws Exception {
		final var headers = new HashMap<String, Object>();
		headers.put("Authorization", "AWS fake-auth-header-1");
		headers.put("x-auth-token", "fake-swift-token-1");
		headers.put("COOKIE", "session=fake-cookie-1");
		headers.put("Proxy-Authorization", "Basic ZmFrZTpmYWtl");
		headers.put("X-Amz-Security-Token", "fake-session-token-1");
		headers.put("X-Custom", "custom-value-1");
		headers.put("Cookie2", "unlisted-cookie-1");
		headers.put("x-amz-date", "");
		final var config = TestConfigBuilder.configWithHttpHeaders(headers);

		final var masked = ConfigUtil.toMaskedString(config, ConfigFormat.YAML, "Load");

		for (final var value : List.of("fake-auth-header-1", "fake-swift-token-1", "fake-cookie-1", "ZmFrZTpmYWtl",
						"fake-session-token-1")) {
			assertFalse(masked.contains(value), value);
		}
		final var maskedHeaders = headersOf(parse(masked));
		for (final var name : List.of("Authorization", "x-auth-token", "COOKIE", "Proxy-Authorization",
						"X-Amz-Security-Token")) {
			assertEquals(ConfigUtil.MASKED_VALUE, maskedHeaders.get(name), name);
		}
		assertEquals("custom-value-1", maskedHeaders.get("X-Custom"));
		assertEquals("unlisted-cookie-1", maskedHeaders.get("Cookie2"));
		assertEquals("", maskedHeaders.get("x-amz-date"));
		assertEquals("AWS fake-auth-header-1", config.mapVal("storage-net-http-headers").get("Authorization"));
		assertEquals("fake-swift-token-1", config.mapVal("storage-net-http-headers").get("x-auth-token"));
	}

	@Test
	public void maskedStringChangesOnlyCredentialAndListedHeaderValues() throws Exception {
		final var headers = new HashMap<String, Object>();
		headers.put("authorization", "fake-auth-header-2");
		headers.put("X-Custom", "custom-value-2");
		final var config = TestConfigBuilder.configWithHttpHeaders(headers);
		config.val("storage-auth-secret", "secret-value-3");
		final var expected = parse(ConfigUtil.toString(config, ConfigFormat.YAML, "Load"));
		authOf(expected).put("secret", ConfigUtil.MASKED_VALUE);
		headersOf(expected).put("authorization", ConfigUtil.MASKED_VALUE);

		assertEquals(expected, parse(ConfigUtil.toMaskedString(config, ConfigFormat.YAML, "Load")));
	}

	@Test
	public void maskCredentialsKeepsUnsetAndEmptyHeadersAndCopiesSharedMaps() {
		final var headers = new HashMap<String, Object>();
		headers.put("Authorization", null);
		headers.put("Cookie", "");
		headers.put("X-Auth-Token", "fake-swift-token-2");
		final Map<String, Object> http = Map.of("headers", headers, "uri", Map.of("args", Map.of()));
		final Map<String, Object> net = Map.of("http", http, "node", Map.of("port", 9020));
		final Map<String, Object> storage = Map.of("net", net, "driver", Map.of("type", "s3"));
		final Map<String, Object> configTree = new HashMap<>(Map.of("storage", storage));

		ConfigUtil.maskCredentials(configTree);

		final var maskedHeaders = headersOf(configTree);
		assertTrue(maskedHeaders.containsKey("Authorization"));
		assertNull(maskedHeaders.get("Authorization"));
		assertEquals("", maskedHeaders.get("Cookie"));
		assertEquals(ConfigUtil.MASKED_VALUE, maskedHeaders.get("X-Auth-Token"));
		assertEquals("fake-swift-token-2", headers.get("X-Auth-Token"));
		final var maskedStorage = (Map<?, ?>) configTree.get("storage");
		assertEquals(Map.of("type", "s3"), maskedStorage.get("driver"));
		assertEquals(Map.of("port", 9020), ((Map<?, ?>) maskedStorage.get("net")).get("node"));
		assertFalse(maskedStorage.containsKey("auth"));
	}

	@Test
	public void maskedArgPathsAreTheAuthCredentialsAndListedHeaders() {
		for (final var path : List.of("storage-auth-uid", "storage-auth-secret", "storage-auth-token",
						"storage-net-http-headers-Authorization", "storage-net-http-headers-proxy-authorization",
						"storage-net-http-headers-cookie", "storage-net-http-headers-X-Amz-Security-Token",
						"storage-net-http-headers-x-auth-token")) {
			assertTrue(ConfigUtil.isMaskedArgPath(path), path);
		}
		for (final var path : List.of("storage-auth-file", "storage-auth-uid-x", "storage-auth", "storage-driver-type",
						"storage-net-http-headers-X-Custom", "storage-net-http-headers-", "storage-net-http-uri-args-token",
						"item-output-path")) {
			assertFalse(ConfigUtil.isMaskedArgPath(path), path);
		}
	}

	@Test
	public void maskedArgFollowsTheFirstAliasingEntryAndKeepsTheGivenName() {
		final List<Map<String, Object>> aliasing = List.of(
						Map.of(AliasingUtil.NAME, "atmos-subtenant", AliasingUtil.TARGET, "storage-auth-token"),
						Map.of(AliasingUtil.NAME, "fixed-secret", AliasingUtil.TARGET, "storage-auth-secret=x"),
						Map.of(AliasingUtil.NAME, "s3-output-bucket", AliasingUtil.TARGET, "item-output-path"),
						Map.of(AliasingUtil.NAME, "s3-output-bucket", AliasingUtil.TARGET, "storage-auth-secret"),
						Map.of(AliasingUtil.NAME, "storage-auth-file", AliasingUtil.TARGET, "storage-auth-secret"));

		assertEquals("--atmos-subtenant=***", ConfigUtil.maskedArg("--atmos-subtenant=fake-token-3", aliasing));
		assertEquals("--fixed-secret=***", ConfigUtil.maskedArg("--fixed-secret=fake-secret-3", aliasing));
		assertEquals("--s3-output-bucket=/b", ConfigUtil.maskedArg("--s3-output-bucket=/b", aliasing));
		assertEquals("--storage-auth-file=***", ConfigUtil.maskedArg("--storage-auth-file=/creds.csv", aliasing));
		assertEquals("--storage-auth-uid=***", ConfigUtil.maskedArg("--storage-auth-uid=fake-uid-3", null));
		assertEquals("--storage-auth-secret=***", ConfigUtil.maskedArg("--storage-auth-secret==a=b=", List.of()));
		assertEquals("--storage-auth-secret=", ConfigUtil.maskedArg("--storage-auth-secret=", aliasing));
		assertEquals("--storage-auth-secret", ConfigUtil.maskedArg("--storage-auth-secret", aliasing));
		assertEquals("--storage-auth-file=/c", ConfigUtil.maskedArg("--storage-auth-file=/c", List.of()));
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> headersOf(final Map<String, Object> configTree) {
		final var storage = (Map<String, Object>) configTree.get("storage");
		final var net = (Map<String, Object>) storage.get("net");
		return (Map<String, Object>) ((Map<String, Object>) net.get("http")).get("headers");
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
