package com.dell.spt.base.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.dell.spt.base.load.step.client.TestLoadStepClient;
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

	@Test
	void anObjectStillNestsItsKeys() throws Exception {
		final var flat = new HashMap<String, String>();
		ConfigUtil.flatten(js("{\"a\": {\"b\": 1, \"c\": [\"x\", \"y\"]}}"), flat, "-", null);

		assertEquals(Map.of("a-b", "1", "a-c", "x,y"), flat);
	}
}
