package com.dell.spt.base.control.run;

import com.dell.spt.base.config.ScenarioConfigurationException;
import com.dell.spt.base.integrity.IntegrityTerminalException.Category;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import javax.script.ScriptException;

/**
 * The category and the public message a node's {@code /status} gives for a scenario that failed
 * without a typed terminal failure.
 *
 * <p>The message is built here and never copied from the failure or the scenario: a script engine's
 * message quotes the scenario line, a refused value is echoed by its parser, and an unknown key is
 * the scenario's own text, so any of them could carry a credential written in the scenario. A refused
 * setting is named only by the schema's keys ({@link ScenarioConfigurationException#publicMessage()}).
 * The engine log keeps the failure itself.
 */
final class ScenarioFailureStatus {

	/** The script engine's parse error, which Nashorn sets as the cause of a syntax error. */
	private static final String PARSER_EXCEPTION_SUFFIX = ".nashorn.internal.runtime.ParserException";

	private ScenarioFailureStatus() {}

	/** {@code CONFIGURATION} for a refused scenario setting or a syntax error, else {@code EXECUTION}. */
	static Category category(final Throwable failure) {
		for (final var cause : causes(failure)) {
			if (cause instanceof ScenarioConfigurationException) {
				return Category.CONFIGURATION;
			}
		}
		return syntaxError(failure) != null ? Category.CONFIGURATION : Category.EXECUTION;
	}

	/** A message naming the cause, with no text taken from the scenario. */
	static String message(final Throwable failure) {
		final var syntaxError = syntaxError(failure);
		if (syntaxError != null) {
			return "scenario failed" + position(syntaxError) + "; see the node's log";
		}
		final var chain = causes(failure);
		for (final var cause : chain) {
			if (cause instanceof ScenarioConfigurationException refused && refused.publicMessage() != null) {
				return refused.publicMessage();
			}
		}
		return "scenario failed: " + className(chain.get(chain.size() - 1)) + "; see the node's log";
	}

	private static ScriptException syntaxError(final Throwable failure) {
		for (final var cause : causes(failure)) {
			if (cause instanceof ScriptException scriptFailure
							&& cause.getCause() != null
							&& cause.getCause().getClass().getName().endsWith(PARSER_EXCEPTION_SUFFIX)) {
				return scriptFailure;
			}
		}
		return null;
	}

	private static String position(final ScriptException syntaxError) {
		final int line = syntaxError.getLineNumber();
		if (line < 0) {
			return "";
		}
		final int column = syntaxError.getColumnNumber();
		return column < 0 ? " at line " + line : " at line " + line + ", column " + column;
	}

	private static String className(final Throwable cause) {
		final var simpleName = cause.getClass().getSimpleName();
		return simpleName.isEmpty() ? cause.getClass().getName() : simpleName;
	}

	/** The failure and its causes, outermost first, each once. */
	private static List<Throwable> causes(final Throwable failure) {
		final Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
		final List<Throwable> chain = new ArrayList<>();
		for (var cause = failure; cause != null && visited.add(cause); cause = cause.getCause()) {
			chain.add(cause);
		}
		return chain;
	}
}
