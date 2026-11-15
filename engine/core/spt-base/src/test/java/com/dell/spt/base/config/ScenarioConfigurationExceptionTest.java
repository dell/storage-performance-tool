package com.dell.spt.base.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import org.junit.jupiter.api.Test;

/**
 * A refused scenario setting is named in public by the schema's own keys only, never by a value or a
 * key the scenario wrote.
 */
class ScenarioConfigurationExceptionTest {

	private static final String MARKER = "AKIAFAKEMARKER0123456789";

	private static String publicMessage(final Throwable cause) {
		return new ScenarioConfigurationException(cause, TestConfigBuilder.config()).publicMessage();
	}

	@Test
	void theDefinedPrefixIsMadeOfSchemaKeys() {
		final var schema = TestConfigBuilder.config().schema();
		assertEquals("load-op-shuffle", ConfigUtil.definedPrefix(schema, "load-op-shuffle", "-"));
		assertEquals("load-op", ConfigUtil.definedPrefix(schema, "load-op-" + MARKER, "-"));
		assertEquals("load-op", ConfigUtil.definedPrefix(schema, "load-op-" + MARKER + "-shuffle", "-"));
		assertEquals("load-op-shuffle", ConfigUtil.definedPrefix(schema, "load-op-shuffle-" + MARKER, "-"));
		assertNull(ConfigUtil.definedPrefix(schema, MARKER + "-op", "-"));
		assertNull(ConfigUtil.definedPrefix(schema, "", "-"));
		assertNull(ConfigUtil.definedPrefix(schema, null, "-"));
	}

	@Test
	void aRefusedBooleanIsNamedOnlyWhenTheSchemaDefinesIt() {
		assertEquals(
						"invalid boolean value for \"run-node\"; give true or false",
						publicMessage(new InvalidBooleanValueException("run-node", "run-node: \"" + MARKER + "\"")));
		assertEquals(
						"invalid boolean value; give true or false",
						publicMessage(new InvalidBooleanValueException(MARKER, MARKER)));
		assertEquals(
						"invalid boolean value; give true or false",
						publicMessage(new InvalidBooleanValueException("load-op-limit-count", MARKER)));
		assertEquals("invalid boolean value; give true or false", publicMessage(new InvalidBooleanValueException(MARKER)));
	}

	@Test
	void anInvalidPathIsNamedByItsDefinedPrefix() {
		assertEquals("invalid configuration option under \"load-op\"", publicMessage(new InvalidValuePathException("load-op-" + MARKER)));
		assertEquals("invalid value for configuration option \"load-op\"", publicMessage(new InvalidValuePathException("load-op")));
		assertEquals("invalid configuration option", publicMessage(new InvalidValuePathException(MARKER)));
	}

	@Test
	void anotherFailureHasNoPublicMessageAndKeepsItsCause() {
		final var cause = new NumberFormatException("For input string: \"" + MARKER + "\"");
		final var refused = new ScenarioConfigurationException(cause, TestConfigBuilder.config());
		assertNull(refused.publicMessage());
		assertSame(cause, refused.getCause());
		assertEquals(cause.getMessage(), refused.getMessage());
		assertNull(new ScenarioConfigurationException(new InvalidValuePathException("load-op"), null).publicMessage());
	}
}
