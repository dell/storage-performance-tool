package com.dell.spt.base.load.step.client;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;

/**
 * Changes Log4j logger levels for a test's window and, on close, puts back what it found, so that a later test in
 * the same JVM sees the configured levels. Use it with try-with-resources, so the levels come back when the test
 * fails too.
 *
 * <p>It works on the configuration directly rather than through {@code Configurator.setLevel}, which leaves three
 * traps: it skips {@code updateLoggers()} when the configured level already matches, so a per-Logger level an
 * earlier test set survives the window; on a logger with no configuration of its own it adds one, and restoring a
 * level through it pins that logger, which then no longer follows its parent; and saving by name lets a repeated
 * name overwrite the level to restore.
 */
public final class LoggerLevels implements AutoCloseable {

	/**
	 * What a named logger had before the window: whether the configuration had a logger entry of its own for it,
	 * that entry's explicit level ({@code null} when it inherits) and the Logger's level, which may be a per-Logger
	 * level an earlier test set.
	 */
	private record Saved(boolean ownConfig, Level configLevel, Level loggerLevel) {}

	private final LoggerContext context;
	private final Map<String, Saved> previous = new LinkedHashMap<>();

	private LoggerLevels(final LoggerContext context) {
		this.context = context;
	}

	/** Lowers each named logger to {@code level} where it is higher now, to let a test's records through. */
	public static LoggerLevels atMost(final Level level, final String... names) {
		final var levels = new LoggerLevels(LoggerContext.getContext(false));
		for (final var name : names) {
			final var current = levels.context.getLogger(name).getLevel();
			if (current == null || (current.isMoreSpecificThan(level) && !current.equals(level))) {
				levels.apply(name, level);
			}
		}
		levels.context.updateLoggers();
		return levels;
	}

	/** Sets each named logger to its level, for example to silence the noise a test expects. */
	public static LoggerLevels set(final Map<String, Level> levelsByName) {
		final var levels = new LoggerLevels(LoggerContext.getContext(false));
		levelsByName.forEach(levels::apply);
		levels.context.updateLoggers();
		return levels;
	}

	private void apply(final String name, final Level level) {
		final var configuration = context.getConfiguration();
		final var loggerConfig = configuration.getLoggerConfig(name);
		final boolean ownConfig = name.equals(loggerConfig.getName());
		// The first save wins: a name given twice must still get back what it had before the window.
		previous.putIfAbsent(name, new Saved(
						ownConfig, ownConfig ? loggerConfig.getExplicitLevel() : null, context.getLogger(name).getLevel()));
		if (ownConfig) {
			loggerConfig.setLevel(level);
		} else {
			configuration.addLogger(name, new LoggerConfig(name, level, true));
		}
	}

	@Override
	public void close() {
		if (previous.isEmpty()) {
			return;
		}
		final var configuration = context.getConfiguration();
		previous.forEach((name, saved) -> {
			final var loggerConfig = configuration.getLoggerConfig(name);
			if (!name.equals(loggerConfig.getName())) {
				return; // the window's entry is gone already
			}
			if (saved.ownConfig()) {
				loggerConfig.setLevel(saved.configLevel());
			} else if (loggerConfig.getAppenders().isEmpty()) {
				configuration.removeLogger(name);
			} else {
				// The test attached an appender to the entry this window added: keep it, but inherit the level again.
				loggerConfig.setLevel(null);
			}
		});
		// Resets every Logger from the configuration, which drops per-Logger levels; put back the ones found.
		context.updateLoggers();
		previous.forEach((name, saved) -> {
			final var logger = context.getLogger(name);
			if (saved.loggerLevel() != null && !saved.loggerLevel().equals(logger.getLevel())) {
				logger.setLevel(saved.loggerLevel());
			}
		});
		previous.clear();
	}
}
