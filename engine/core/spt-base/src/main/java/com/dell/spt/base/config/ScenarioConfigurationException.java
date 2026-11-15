package com.dell.spt.base.config;

import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * A setting a scenario gave to {@code config(...)} or {@code append(...)} that the engine refused: an
 * unknown or malformed option, a value of the wrong type, a bad list or a boolean other than
 * {@code true} or {@code false}. A node reports it in {@code /status} as a {@code configuration}
 * failure.
 *
 * <p>It keeps the refused setting's message, and the refused exception as its cause, for the engine
 * log and for a scenario that catches it; that message may quote a value or a key from the scenario.
 * {@link #publicMessage()} is built from the schema's own keys instead, and is what {@code /status}
 * publishes.
 */
public final class ScenarioConfigurationException extends IllegalArgumentException {

	private final String publicMessage;

	/**
	 * @param cause the refused setting's failure
	 * @param config the configuration whose schema names the settings, or {@code null}
	 */
	public ScenarioConfigurationException(final Throwable cause, final Config config) {
		super(cause.getMessage(), cause);
		this.publicMessage = config == null ? null : publicMessage(cause, config);
	}

	/**
	 * A message naming the refused setting by the schema's keys only, or {@code null} when the failure
	 * names no setting; it never quotes a value or a key from the scenario.
	 */
	public String publicMessage() {
		return publicMessage;
	}

	private static String publicMessage(final Throwable failure, final Config config) {
		final var schema = config.schema();
		final var sep = config.pathSep();
		final Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
		for (var cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
			if (cause instanceof InvalidBooleanValueException refused) {
				final var path = refused.path();
				// The refusal names a schema boolean setting; check it against the schema rather than trust it
				return path != null && schema != null && ConfigUtil.isBooleanSetting(schema, path, sep)
								? "invalid boolean value for \"" + path + "\"; give true or false"
								: "invalid boolean value; give true or false";
			}
			if (cause instanceof InvalidValuePathException invalidPath) {
				final var path = invalidPath.path();
				final var prefix = ConfigUtil.definedPrefix(schema, path, sep);
				if (prefix == null) {
					return "invalid configuration option";
				}
				return prefix.equals(path)
								? "invalid value for configuration option \"" + prefix + "\""
								: "invalid configuration option under \"" + prefix + "\"";
			}
		}
		return null;
	}
}
