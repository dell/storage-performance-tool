package com.dell.spt.base.config;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.load.step.client.TestLoadStepClient;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.exceptions.InvalidValueTypeException;
import java.util.List;
import java.util.Map;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A boolean setting given as a string takes {@code true} or {@code false}, in any case, on every
 * path that reads strings: a scenario's {@code config} and {@code append}, a defaults file and a run
 * request's defaults. The configuration reads any other string as false, so these paths refuse it.
 * The command line is covered by {@code MainArgumentErrorTest}.
 */
class BooleanValueTest {

	private static final ScriptEngine ENGINE = new ScriptEngineManager().getEngineByName("js");

	@SuppressWarnings("unchecked")
	private static Map<String, Object> js(final String object) throws Exception {
		return (Map<String, Object>) ENGINE.eval("(" + object + ")");
	}

	private static TestLoadStepClient client() {
		return new TestLoadStepClient(TestConfigBuilder.config(), List.of(), null, null);
	}

	private static Map<String, Object> schema() throws Exception {
		return SchemaProvider.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
						.stream()
						.findFirst()
						.orElseThrow();
	}

	@Test
	void theSchemaTypesOfTheSettingsAreRead() throws Exception {
		final var schema = schema();
		assertTrue(ConfigUtil.isBooleanSetting(schema, "run-node", "-"));
		assertTrue(ConfigUtil.isBooleanSetting(schema, "load-op-recycle-mode", "-"));
		assertFalse(ConfigUtil.isBooleanSetting(schema, "load-op-recycle", "-"));
		assertFalse(ConfigUtil.isBooleanSetting(schema, "load-op-limit-count", "-"));
		assertFalse(ConfigUtil.isBooleanSetting(schema, "no-such-setting", "-"));
		assertFalse(ConfigUtil.isBooleanSetting(schema, "run-node-deeper", "-"));
	}

	@ParameterizedTest
	@ValueSource(strings = {"true", "TRUE", "True", "false", "FALSE"
	})
	void aBooleanSettingAcceptsTrueOrFalse(final String value) throws Exception {
		assertTrue(ConfigUtil.isAcceptedBooleanValue(schema(), "run-node", "-", value));
	}

	@ParameterizedTest
	@ValueSource(strings = {"yes", "no", "1", "0", "", " true", "on"
	})
	void aBooleanSettingRefusesAnyOtherString(final String value) throws Exception {
		assertFalse(ConfigUtil.isAcceptedBooleanValue(schema(), "run-node", "-", value));
	}

	@Test
	void anotherSettingTakesAnyStringAndANullValuePasses() throws Exception {
		assertTrue(ConfigUtil.isAcceptedBooleanValue(schema(), "storage-driver-type", "-", "yes"));
		assertTrue(ConfigUtil.isAcceptedBooleanValue(schema(), "no-such-setting", "-", "yes"));
		assertTrue(ConfigUtil.isAcceptedBooleanValue(schema(), "run-node", "-", null));
	}

	@ParameterizedTest
	@ValueSource(strings = {"\"yes\"", "\"1\"", "1", "\"\""
	})
	void aScenarioConfigRefusesANonBooleanValue(final String value) throws Exception {
		final var config = js("{\"load\": {\"op\": {\"shuffle\": " + value + "}}}");
		final var failure = assertThrows(IllegalArgumentException.class, () -> client().config(config));
		assertTrue(failure.getMessage().startsWith("load-op-shuffle: "), failure.getMessage());
		assertTrue(failure.getMessage().endsWith(" is not true or false"), failure.getMessage());
	}

	@Test
	void aScenarioAppendRefusesANonBooleanValue() throws Exception {
		final var context = js("{\"load\": {\"op\": {\"recycle\": {\"mode\": \"yes\"}}}}");
		final var failure = assertThrows(IllegalArgumentException.class, () -> client().append(context));
		assertEquals("load-op-recycle-mode: \"yes\" is not true or false", failure.getMessage());
	}

	@Test
	void aScenarioValueThroughAnAliasIsCheckedAtItsTarget() throws Exception {
		final var client = new TestLoadStepClient(aliasedConfig(), List.of(), null, null);
		final var failure = assertThrows(
						IllegalArgumentException.class, () -> client.config(js("{\"load\": {\"generator\": {\"shuffle\": \"yes\"}}}")));
		assertEquals("load-op-shuffle: \"yes\" is not true or false", failure.getMessage());
	}

	private static com.github.akurilov.confuse.Config aliasedConfig() {
		final var config = TestConfigBuilder.config();
		config.val("aliasing", List.of(Map.of("name", "load-generator-shuffle", "target", "load-op-shuffle")));
		return config;
	}

	@Test
	void aScenarioTakesBooleansAndTrueOrFalseStrings() throws Exception {
		assertTrue(client().config(js("{\"load\": {\"op\": {\"shuffle\": true}}}")).getEffectiveConfig().boolVal("load-op-shuffle"));
		assertFalse(client().config(js("{\"load\": {\"op\": {\"shuffle\": false}}}")).getEffectiveConfig().boolVal("load-op-shuffle"));
		assertTrue(client().config(js("{\"load\": {\"op\": {\"shuffle\": \"TRUE\"}}}")).getEffectiveConfig().boolVal("load-op-shuffle"));
		assertFalse(client().append(js("{\"load\": {\"op\": {\"shuffle\": \"false\"}}}")).getCtxConfigs().get(0).boolVal("load-op-shuffle"));
	}

	@ParameterizedTest
	@ValueSource(strings = {"\"yes\"", "\"1\"", "\"\"", "\"on\"", "maybe"
	})
	void aDefaultsDocumentRefusesANonBooleanString(final String value) throws Exception {
		final var yaml = "run:\n  node: " + value + "\n";
		final var failure = assertThrows(
						IllegalArgumentException.class, () -> ConfigUtil.loadConfig(yaml, ConfigFormat.YAML, schema()));
		assertTrue(failure.getMessage().startsWith("run-node: "), failure.getMessage());
	}

	@Test
	void aDefaultsDocumentRefusesANumberForABoolean() throws Exception {
		// Not a string: the configuration itself refuses it, as it did before
		assertThrows(
						InvalidValueTypeException.class,
						() -> ConfigUtil.loadConfig("run:\n  node: 1\n", ConfigFormat.YAML, schema()));
	}

	@Test
	void aDefaultsDocumentTakesBooleansAndTrueOrFalseStrings() throws Exception {
		assertTrue(ConfigUtil.loadConfig("run:\n  node: true\n", ConfigFormat.YAML, schema()).boolVal("run-node"));
		assertFalse(ConfigUtil.loadConfig("run:\n  node: false\n", ConfigFormat.YAML, schema()).boolVal("run-node"));
		assertTrue(ConfigUtil.loadConfig("run:\n  node: \"True\"\n", ConfigFormat.YAML, schema()).boolVal("run-node"));
		assertFalse(ConfigUtil.loadConfig("{\"run\": {\"node\": false}}", ConfigFormat.JSON, schema()).boolVal("run-node"));
		assertTrue(ConfigUtil.loadConfig("load:\n  op:\n    recycle:\n      mode: true\n", ConfigFormat.YAML, schema())
						.boolVal("load-op-recycle-mode"));
	}

	@Test
	void aJsonRunRequestRefusesANonBooleanString() throws Exception {
		final var failure = assertThrows(
						IllegalArgumentException.class,
						() -> ConfigUtil.loadConfig("{\"load\": {\"op\": {\"recycle\": {\"mode\": \"yes\"}}}}", ConfigFormat.JSON, schema()));
		assertEquals("load-op-recycle-mode: \"yes\" is not true or false", failure.getMessage());
	}

	@Test
	void aDefaultsDocumentReadsUnquotedYaml11WordsAsBooleans() throws Exception {
		// The YAML parser reads unquoted yes, no, on and off as booleans, so they never reach the check
		assertTrue(ConfigUtil.loadConfig("run:\n  node: yes\n", ConfigFormat.YAML, schema()).boolVal("run-node"));
		assertFalse(ConfigUtil.loadConfig("run:\n  node: off\n", ConfigFormat.YAML, schema()).boolVal("run-node"));
	}

	@Test
	void theTreeCheckSkipsAMissingSchemaOrTree() {
		ConfigUtil.validateBooleanValues(null, Map.of("run", Map.of("node", "yes")), "-");
		ConfigUtil.validateBooleanValues(Map.of("run", Map.of("node", boolean.class)), null, "-");
	}
}
