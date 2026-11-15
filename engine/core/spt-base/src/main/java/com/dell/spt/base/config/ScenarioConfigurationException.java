package com.dell.spt.base.config;

/**
 * A setting a scenario gave to {@code config(...)} or {@code append(...)} that the engine refused: an
 * unknown option, a value of the wrong type, a bad list or a boolean other than {@code true} or
 * {@code false}. A node reports it in {@code /status} as a {@code configuration} failure.
 *
 * <p>It keeps the refused setting's message for the engine log and for a scenario that catches it;
 * that message may quote a value from the scenario, so {@code /status} never publishes it.
 */
public final class ScenarioConfigurationException extends IllegalArgumentException {

	public ScenarioConfigurationException(final Throwable cause) {
		super(cause.getMessage(), cause);
	}
}
