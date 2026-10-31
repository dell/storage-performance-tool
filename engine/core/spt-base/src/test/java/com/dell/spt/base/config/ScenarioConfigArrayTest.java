package com.dell.spt.base.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.load.step.client.TestLoadStepClient;
import com.dell.spt.base.storage.driver.bucket.TargetBucketSet;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.openjdk.nashorn.api.scripting.JSObject;
import org.junit.jupiter.api.Test;

/** JavaScript arrays in a scenario's step configuration, evaluated by the scenarios' script engine. */
class ScenarioConfigArrayTest {

	private static final ScriptEngine ENGINE = new ScriptEngineManager().getEngineByName("js");

	@SuppressWarnings("unchecked")
	private static Map<String, Object> js(final String object) throws Exception {
		return (Map<String, Object>) ENGINE.eval("(" + object + ")");
	}

	private static TestLoadStepClient client() {
		return new TestLoadStepClient(TestConfigBuilder.config(), List.of(), null, null);
	}

	private static List<String> addrs(final Map<String, Object> config) {
		return client().config(config).getEffectiveConfig().listVal("load-step-node-addrs");
	}

	@Test
	void anArrayIsAMirrorThatIsAMap() throws Exception {
		final var array = js("{\"addrs\": [\"a\"]}").get("addrs");

		assertInstanceOf(Map.class, array, "the reason flatten must recognize it");
		assertInstanceOf(JSObject.class, array);
	}

	@Test
	void anArrayInConfigIsItsList() throws Exception {
		assertEquals(List.of("a", "b:9000"), addrs(js("{\"load\": {\"step\": {\"node\": {\"addrs\": [\"a\", \"b:9000\"]}}}}")));
	}

	@Test
	void anArrayInAppendIsItsList() throws Exception {
		final var appended = client().append(js("{\"load\": {\"step\": {\"node\": {\"addrs\": [\"a\", \"b\"]}}}}"));

		assertEquals(List.of("a", "b"), appended.getCtxConfigs().get(0).listVal("load-step-node-addrs"));
	}

	@Test
	void anArrayGivesWhatACommaStringAndAJavaListGive() throws Exception {
		final var commaString = addrs(js("{\"load\": {\"step\": {\"node\": {\"addrs\": \"a,b\"}}}}"));
		final var javaList = addrs(Map.of("load", Map.of("step", Map.of("node", Map.of("addrs", List.of("a", "b"))))));

		assertEquals(commaString, addrs(js("{\"load\": {\"step\": {\"node\": {\"addrs\": [\"a\", \"b\"]}}}}")));
		assertEquals(javaList, commaString);
	}

	@Test
	void numbersAndASingleElementKeepTheirText() throws Exception {
		final var configured = client().config(js("{\"output\": {\"metrics\": {\"quantiles\": [0.5, 0.99]}}, "
						+ "\"load\": {\"step\": {\"node\": {\"addrs\": [\"only\"]}}}}")).getEffectiveConfig();

		assertEquals(List.of("0.5", "0.99"), configured.listVal("output-metrics-quantiles"));
		assertEquals(List.of("only"), configured.listVal("load-step-node-addrs"));
	}

	@Test
	void anEmptyArrayGivesWhatAnEmptyJavaListGives() throws Exception {
		final var javaEmpty = new HashMap<String, Object>(Map.of("load", Map.of("step", Map.of("node", Map.of("addrs", List.of())))));

		assertEquals(addrs(javaEmpty), addrs(js("{\"load\": {\"step\": {\"node\": {\"addrs\": []}}}}")));
	}

	// An array passes as its comma string, so an element that string cannot carry, empty or holding
	// a comma, fails the scenario instead of being split or dropped.
	@Test
	void aListElementTheCommaStringCannotCarryFailsTheScenario() throws Exception {
		for (final var names : List.of("[\"alpha,beta\"]", "[\"alpha\", \"\"]", "[\"\"]")) {
			assertThrows(Exception.class, () -> client().config(targetBucket(names)), "config " + names);
			assertThrows(Exception.class, () -> client().append(targetBucket(names)), "append " + names);
		}
		assertEquals(List.of("ps-eu", "PS_US"), names(client().config(targetBucket("[\"ps-eu\", \"PS_US\"]")).getEffectiveConfig()));
		assertEquals(List.of("ps-eu", "PS_US"),
						names(client().append(targetBucket("[\"ps-eu\", \"PS_US\"]")).getCtxConfigs().get(0)));
	}

	@Test
	void aCommaStringStillSetsItsElements() throws Exception {
		assertEquals(List.of("ps-eu", "PS_US"),
						names(client().config(js("{\"storage\": {\"targetBucket\": {\"names\": \"ps-eu,PS_US\"}}}")).getEffectiveConfig()));
	}

	@Test
	void aSettingThatIsNotAListIsNotChecked() throws Exception {
		final var configured = client().config(js("{\"item\": {\"output\": {\"path\": [\"a,b\", \"\"]}}}")).getEffectiveConfig();

		assertEquals("a,b,", configured.stringVal("item-output-path"));
	}

	@Test
	void theSchemaDecidesWhichSettingsAreLists() {
		final var schema = TestConfigBuilder.config().schema();

		assertTrue(ConfigUtil.isListSetting(schema, "storage-targetBucket-names", "-"));
		assertTrue(ConfigUtil.isListSetting(schema, "load-step-node-addrs", "-"));
		assertFalse(ConfigUtil.isListSetting(schema, "storage-targetBucket-count", "-"));
		assertFalse(ConfigUtil.isListSetting(schema, "storage-targetBucket", "-"));
		assertFalse(ConfigUtil.isListSetting(schema, "storage-targetBucket-names-x", "-"));
		assertFalse(ConfigUtil.isListSetting(schema, "no-such-setting", "-"));
	}

	// A later scalar at the same path replaces an array, so the array is not checked.
	@Test
	void aLaterScalarAtTheSamePathReplacesTheArray() throws Exception {
		final var configured = client().config(js("{\"storage-targetBucket-names\": [\"alpha,beta\"], "
						+ "\"storage\": {\"targetBucket\": {\"names\": \"gamma,delta\"}}}")).getEffectiveConfig();

		assertEquals(List.of("gamma", "delta"), names(configured));
	}

	// The target bucket set reads what the scenario wrote: an empty array falls back to the count.
	@Test
	void aScenarioBucketListReachesTheTargetBucketSet() throws Exception {
		final var emptyList = client().config(js("{\"storage\": {\"targetBucket\": {\"count\": 4, \"names\": []}}}")).getEffectiveConfig();
		assertEquals(4, TargetBucketSet.count(emptyList));
		assertEquals(List.of(), TargetBucketSet.names(emptyList));

		final var listed = client().config(targetBucket("[\"ps-eu\", \"PS_US\"]")).getEffectiveConfig();
		assertEquals(2, TargetBucketSet.count(listed));
		assertEquals(List.of("ps-eu", "PS_US"), TargetBucketSet.names(listed));
	}

	// A value is checked at the setting aliasing takes it to: a list renamed onto a list setting is
	// checked there, one renamed away from it or replaced by an alias's own value is not.
	@Test
	void aListIsCheckedWhereAliasingTakesIt() throws Exception {
		final var config = TestConfigBuilder.config();
		final var aliasing = new ArrayList<Map<String, Object>>(config.<Map<String, Object>> listVal("aliasing"));
		aliasing.add(Map.of("name", "old-names", "target", "storage-targetBucket-names"));
		aliasing.add(Map.of("name", "storage-targetBucket-names", "target", "item-output-path"));
		aliasing.add(Map.of("name", "fixed-names", "target", "storage-targetBucket-names=ps-eu,PS_US"));
		config.val("aliasing", aliasing);
		final var client = new TestLoadStepClient(config, List.of(), null, null);

		assertThrows(Exception.class, () -> client.config(js("{\"old-names\": [\"alpha,beta\"]}")), "renamed onto a list");
		assertThrows(Exception.class, () -> client.append(js("{\"old-names\": [\"alpha\", \"\"]}")), "renamed onto a list");
		client.config(js("{\"storage-targetBucket-names\": [\"a,b\"]}"));
		assertEquals(List.of("ps-eu", "PS_US"), names(client.config(js("{\"fixed-names\": [\"x,y\"]}")).getEffectiveConfig()));
	}

	// Aliasing works on the strings, as it always has; an alias never meets a restored list.
	@Test
	void aliasingStillDecidesWhatItWrites() throws Exception {
		final var input = js("{\"storage-targetBucket-names\": [\"alpha\", \"beta\"], \"x\": \"unused\"}");
		final var config = TestConfigBuilder.config();
		final var aliasing = new ArrayList<Map<String, Object>>(config.<Map<String, Object>> listVal("aliasing"));
		aliasing.add(Map.of("name", "x", "target", "storage-targetBucket-names=gamma,delta"));
		config.val("aliasing", aliasing);
		final var flat = new HashMap<String, String>();
		ConfigUtil.flatten(input, flat, "-", null);
		final var aliasedOnly = List.of(AliasingUtil.apply(flat, aliasing).get("storage-targetBucket-names").split(","));

		final var client = new TestLoadStepClient(config, List.of(), null, null);
		assertEquals(aliasedOnly, names(client.config(input).getEffectiveConfig()), "config");
		assertEquals(aliasedOnly, names(client.append(input).getCtxConfigs().get(0)), "append");
	}

	private static Map<String, Object> targetBucket(final String namesArray) throws Exception {
		return js("{\"storage\": {\"targetBucket\": {\"names\": " + namesArray + "}}}");
	}

	private static List<String> names(final Config config) {
		return config.listVal("storage-targetBucket-names");
	}

	@Test
	void anObjectStillNestsItsKeys() throws Exception {
		final var flat = new HashMap<String, String>();
		ConfigUtil.flatten(js("{\"a\": {\"b\": 1, \"c\": [\"x\", \"y\"]}}"), flat, "-", null);

		assertEquals(Map.of("a-b", "1", "a-c", "x,y"), flat);
	}
}
