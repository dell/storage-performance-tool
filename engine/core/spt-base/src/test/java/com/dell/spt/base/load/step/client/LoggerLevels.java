package com.dell.spt.base.load.step.client;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configurator;

/**
 * Lets a test's records through loggers that earlier tests in the same JVM may have left at a higher level, and
 * puts back the levels they had.
 */
final class LoggerLevels implements AutoCloseable {

	private final Map<String, Level> previous = new LinkedHashMap<>();

	private LoggerLevels() {}

	/** Lowers each named logger to {@code level} where it is higher now. */
	static LoggerLevels atMost(final Level level, final String... names) {
		final var levels = new LoggerLevels();
		final var context = LoggerContext.getContext(false);
		for (final var name : names) {
			final var current = context.getLogger(name).getLevel();
			if (current == null || (current.isMoreSpecificThan(level) && !current.equals(level))) {
				levels.previous.put(name, current);
				Configurator.setLevel(name, level);
			}
		}
		return levels;
	}

	@Override
	public void close() {
		previous.forEach(Configurator::setLevel);
	}
}
