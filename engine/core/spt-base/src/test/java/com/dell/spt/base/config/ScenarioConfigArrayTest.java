package com.dell.spt.base.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.control.run.RunTerminalException;
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

	// A list setting keeps exactly the elements of its array, which a comma string cannot carry: an
	// empty array, an element with a comma and a trailing empty element.
	@Test
	void aListSettingKeepsExactlyTheElementsOfItsArray() throws Exception {
		assertEquals(List.of(), names(client().config(targetBucket("[]")).getEffectiveConfig()));
		assertEquals(List.of("alpha,beta"), names(client().config(targetBucket("[\"alpha,beta\"]")).getEffectiveConfig()));
		assertEquals(List.of("alpha", ""), names(client().config(targetBucket("[\"alpha\", \"\"]")).getEffectiveConfig()));
		assertEquals(List.of("ps-eu", "PS_US"),
						names(client().append(targetBucket("[\"ps-eu\", \"PS_US\"]")).getCtxConfigs().get(0)));
		assertEquals(List.of(), names(client().append(targetBucket("[]")).getCtxConfigs().get(0)));
	}

	@Test
	void aCommaStringStillSetsItsElements() throws Exception {
		assertEquals(List.of("ps-eu", "PS_US"),
						names(client().config(js("{\"storage\": {\"targetBucket\": {\"names\": \"ps-eu,PS_US\"}}}")).getEffectiveConfig()));
	}

	@Test
	void aSettingThatIsNotAListStillGetsTheCommaString() throws Exception {
		final var configured = client().config(js("{\"item\": {\"output\": {\"path\": [\"a\", \"b\"]}}}")).getEffectiveConfig();

		assertEquals("a,b", configured.stringVal("item-output-path"));
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

	// The target bucket set reads what the scenario wrote: an empty list falls back to the count, and
	// names the engine cannot use fail instead of being split or dropped.
	@Test
	void aScenarioBucketListReachesTheTargetBucketSetExactly() throws Exception {
		final var emptyList = client().config(js("{\"storage\": {\"targetBucket\": {\"count\": 4, \"names\": []}}}")).getEffectiveConfig();
		assertEquals(4, TargetBucketSet.count(emptyList));

		final var listed = client().config(targetBucket("[\"ps-eu\", \"PS_US\"]")).getEffectiveConfig();
		assertEquals(2, TargetBucketSet.count(listed));
		assertEquals(List.of("ps-eu", "PS_US"), TargetBucketSet.names(listed));

		for (final var malformed : List.of("[\"alpha,beta\"]", "[\"alpha\", \"\"]")) {
			final var configured = client().config(targetBucket(malformed)).getEffectiveConfig();
			assertThrows(RunTerminalException.class, () -> TargetBucketSet.count(configured), malformed);
		}
	}

	// Aliasing's value stands over the array it replaced, even when its text equals the array's
	// comma string.
	@Test
	void anAliasingReplacementKeepsItsValue() throws Exception {
		for (final var appendStep : List.of(false, true)) {
			final var config = TestConfigBuilder.config();
			final var aliasing = new ArrayList<Map<String, Object>>(config.<Map<String, Object>> listVal("aliasing"));
			aliasing.add(Map.of("name", "storage-targetBucket-names", "target", "storage-targetBucket-names=alpha,beta"));
			config.val("aliasing", aliasing);
			final var client = new TestLoadStepClient(config, List.of(), null, null);
			final var configured = appendStep
							? client.append(targetBucket("[\"alpha,beta\"]")).getCtxConfigs().get(0)
							: client.config(targetBucket("[\"alpha,beta\"]")).getEffectiveConfig();

			assertEquals(List.of("alpha", "beta"), names(configured), appendStep ? "append" : "config");
		}
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
