package com.dell.spt.base.load.step.client;

import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;

/**
 * Changes Log4j logger levels for a test's window and, on close, puts back what it found, so that a later test in
 * the same JVM sees the configured levels. Use it with try-with-resources, so the levels come back when the test
 * fails too. Windows that name the same logger must close in the reverse order of opening, as nested
 * try-with-resources blocks do. Levels set on a LoggerConfig without updateLoggers(), and other threads logging
 * while a window opens or closes, are not covered.
 *
 * <p>It works on the configuration directly rather than through {@code Configurator.setLevel}, which leaves three
 * traps: it skips {@code updateLoggers()} when the configured level already matches, so a per-Logger level an
 * earlier test set survives the window; on a logger with no configuration of its own it adds one, and restoring a
 * level through it pins that logger, which then no longer follows its parent; and saving by name lets a repeated
 * name overwrite the level to restore.
 *
 * <p>A window touches only the loggers it changes. One that changes nothing does nothing, and the per-Logger levels
 * of other loggers, which {@code updateLoggers()} would drop, are put back each time it runs.
 */
public final class LoggerLevels implements AutoCloseable {

	/**
	 * What a named logger had before the window: whether the configuration had a logger entry of its own for it,
	 * that entry's explicit level ({@code null} when it inherits) and the per-Logger level an earlier test set
	 * ({@code null} when the Logger follows its configuration).
	 */
	private record Saved(boolean ownConfig, Level configLevel, Level loggerOverride) {}

	private final LoggerContext context;
	private final Map<String, Saved> previous = new LinkedHashMap<>();

	private LoggerLevels(final LoggerContext context) {
		this.context = context;
	}

	/** Lowers each named logger to {@code level} where it is higher now, to let a test's records through. */
	public static LoggerLevels atMost(final Level level, final String... names) {
		final var context = LoggerContext.getContext(false);
		final var changes = new LinkedHashMap<String, Level>();
		for (final var name : names) {
			final var current = context.getLogger(name).getLevel();
			if (current == null || (current.isMoreSpecificThan(level) && !current.equals(level))) {
				changes.putIfAbsent(name, level);
			}
		}
		return new LoggerLevels(context).open(changes);
	}

	/** Sets each named logger to its level, for example to silence the noise a test expects. */
	public static LoggerLevels set(final Map<String, Level> levelsByName) {
		return new LoggerLevels(LoggerContext.getContext(false)).open(levelsByName);
	}

	private LoggerLevels open(final Map<String, Level> changes) {
		if (changes.isEmpty()) {
			return this;
		}
		final var configuration = context.getConfiguration();
		// Save every named logger before changing any, so that a change to a parent does not show in a child's save.
		changes.keySet().forEach(name -> {
			final var loggerConfig = configuration.getLoggerConfig(name);
			final boolean ownConfig = name.equals(loggerConfig.getName());
			previous.put(name, new Saved(
							ownConfig, ownConfig ? loggerConfig.getExplicitLevel() : null, override(context.getLogger(name))));
		});
		final var others = overridesExcept(changes.keySet());
		changes.forEach((name, level) -> {
			final var loggerConfig = configuration.getLoggerConfig(name);
			if (name.equals(loggerConfig.getName())) {
				loggerConfig.setLevel(level);
			} else {
				configuration.addLogger(name, new LoggerConfig(name, level, true));
			}
		});
		updateLoggersKeeping(others);
		return this;
	}

	@Override
	public void close() {
		if (previous.isEmpty()) {
			return;
		}
		final var configuration = context.getConfiguration();
		final var others = overridesExcept(previous.keySet());
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
		updateLoggersKeeping(others);
		previous.forEach((name, saved) -> {
			if (saved.loggerOverride() != null) {
				context.getLogger(name).setLevel(saved.loggerOverride());
			}
		});
		previous.clear();
	}

	/** The per-Logger level of a Logger, or {@code null} when it has the level its configuration gives it. */
	private Level override(final Logger logger) {
		final var level = logger.getLevel();
		return Objects.equals(level, context.getConfiguration().getLoggerConfig(logger.getName()).getLevel())
						? null
						: level;
	}

	/** The per-Logger levels of every Logger not named, which {@code updateLoggers()} would drop. */
	private Map<Logger, Level> overridesExcept(final Set<String> names) {
		final var overrides = new IdentityHashMap<Logger, Level>();
		for (final var logger : context.getLoggers()) {
			final var level = names.contains(logger.getName()) ? null : override(logger);
			if (level != null) {
				overrides.put(logger, level);
			}
		}
		return overrides;
	}

	/** Resets every Logger from the configuration, then puts back the per-Logger levels of the others. */
	private void updateLoggersKeeping(final Map<Logger, Level> overrides) {
		context.updateLoggers();
		overrides.forEach(Logger::setLevel);
	}
}
