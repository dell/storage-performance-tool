package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dell.spt.base.logging.Loggers;
import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.NullAppender;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** A {@link LoggerLevels} window gives later tests in the JVM back the levels it found, whatever they were. */
class LoggerLevelsTest {

	private static final String ERR = Loggers.ERR.getName();

	/** A logger with no entry of its own in the configuration, so that it follows its parent. */
	private static final String UNCONFIGURED = LoggerLevelsTest.class.getName() + ".unconfigured";

	private static final LoggerContext CONTEXT = LoggerContext.getContext(false);

	/** What the Errors logger had before each test, to check against, and to put back if a test fails. */
	private Level errConfigLevel;
	private Level errLoggerLevel;

	@BeforeEach
	void recordErrors() {
		errConfigLevel = CONTEXT.getConfiguration().getLoggerConfig(ERR).getExplicitLevel();
		errLoggerLevel = CONTEXT.getLogger(ERR).getLevel();
	}

	@AfterEach
	void putErrorsBack() {
		CONTEXT.getConfiguration().getLoggerConfig(ERR).setLevel(errConfigLevel);
		CONTEXT.getConfiguration().removeLogger(UNCONFIGURED);
		CONTEXT.updateLoggers();
		if (!errLoggerLevel.equals(CONTEXT.getLogger(ERR).getLevel())) {
			CONTEXT.getLogger(ERR).setLevel(errLoggerLevel);
		}
	}

	/** A per-Logger level that differs from the configured one, as an earlier test may have left. */
	private static Level overrideErrorsAboveItsConfiguredLevel() {
		final var configured = CONTEXT.getConfiguration().getLoggerConfig(ERR).getLevel();
		final var override = Level.OFF.equals(configured) ? Level.ERROR : Level.OFF;
		CONTEXT.getLogger(ERR).setLevel(override);
		return configured;
	}

	@Test
	void aWindowAtTheConfiguredLevelOverridesAPerLoggerLevelAndPutsItBack() {
		final var configured = overrideErrorsAboveItsConfiguredLevel();
		final var override = CONTEXT.getLogger(ERR).getLevel();

		try (var levels = LoggerLevels.set(Map.of(ERR, configured))) {
			assertEquals(configured, CONTEXT.getLogger(ERR).getLevel());
		}

		assertEquals(override, CONTEXT.getLogger(ERR).getLevel());
		assertEquals(errConfigLevel, CONTEXT.getConfiguration().getLoggerConfig(ERR).getExplicitLevel());
	}

	@Test
	void atMostTheConfiguredLevelOverridesAPerLoggerLevelAndPutsItBack() {
		final var configured = overrideErrorsAboveItsConfiguredLevel();
		final var override = CONTEXT.getLogger(ERR).getLevel();

		try (var levels = LoggerLevels.atMost(configured, ERR)) {
			assertEquals(configured, CONTEXT.getLogger(ERR).getLevel());
		}

		assertEquals(override, CONTEXT.getLogger(ERR).getLevel());
	}

	@Test
	void aLoggerWithNoEntryOfItsOwnFollowsItsParentAgainAfterTheWindow() {
		final var parentLevel = CONTEXT.getLogger(UNCONFIGURED).getLevel();
		final var windowLevel = Level.OFF.equals(parentLevel) ? Level.ERROR : Level.OFF;

		try (var levels = LoggerLevels.set(Map.of(UNCONFIGURED, windowLevel))) {
			assertEquals(windowLevel, CONTEXT.getLogger(UNCONFIGURED).getLevel());
		}

		assertNotEquals(UNCONFIGURED, CONTEXT.getConfiguration().getLoggerConfig(UNCONFIGURED).getName());
		assertEquals(parentLevel, CONTEXT.getLogger(UNCONFIGURED).getLevel());
		final var rootLevel = LogManager.getRootLogger().getLevel();
		final var otherRootLevel = Level.FATAL.equals(rootLevel) ? Level.ERROR : Level.FATAL;
		try (var levels = LoggerLevels.set(Map.of(LogManager.ROOT_LOGGER_NAME, otherRootLevel))) {
			assertEquals(otherRootLevel, CONTEXT.getLogger(UNCONFIGURED).getLevel(), "it no longer follows the root");
		}
		assertEquals(rootLevel, LogManager.getRootLogger().getLevel());
	}

	@Test
	void anAppenderAddedInTheWindowStaysAndTheLoggerFollowsItsParentAgain() {
		final var parentLevel = CONTEXT.getLogger(UNCONFIGURED).getLevel();
		final var windowLevel = Level.OFF.equals(parentLevel) ? Level.ERROR : Level.OFF;
		final var appender = NullAppender.createAppender(LoggerLevelsTest.class.getSimpleName());
		appender.start();

		try (var levels = LoggerLevels.set(Map.of(UNCONFIGURED, windowLevel))) {
			CONTEXT.getLogger(UNCONFIGURED).addAppender(appender);
		}

		final var config = CONTEXT.getConfiguration().getLoggerConfig(UNCONFIGURED);
		assertEquals(UNCONFIGURED, config.getName());
		assertNull(config.getExplicitLevel());
		assertEquals(parentLevel, CONTEXT.getLogger(UNCONFIGURED).getLevel());
		CONTEXT.getLogger(UNCONFIGURED).removeAppender(appender);
		appender.stop();
	}

	@Test
	void aNameGivenTwiceGetsBackTheLevelItHadBeforeTheWindow() {
		final var before = CONTEXT.getLogger(ERR).getLevel();
		final var windowLevel = Level.OFF.equals(before) ? Level.ERROR : Level.OFF;
		try (var outer = LoggerLevels.set(Map.of(ERR, windowLevel))) {
			try (var levels = LoggerLevels.atMost(Level.WARN, ERR, ERR)) {
				assertEquals(Level.WARN, CONTEXT.getLogger(ERR).getLevel());
			}

			assertEquals(windowLevel, CONTEXT.getConfiguration().getLoggerConfig(ERR).getExplicitLevel());
			assertEquals(windowLevel, CONTEXT.getLogger(ERR).getLevel());
		}
		assertEquals(before, CONTEXT.getLogger(ERR).getLevel());
	}

	@Test
	void aTestThatFailsInsideASilencedWindowLeavesTheLevelsItFound() {
		final var silencing = Level.OFF.equals(errLoggerLevel) ? Level.ERROR : Level.OFF;

		assertThrows(AssertionError.class, () -> {
			try (var levels = LoggerLevels.set(Map.of(ERR, silencing))) {
				assertEquals(silencing, CONTEXT.getLogger(ERR).getLevel());
				throw new AssertionError("a failure inside the window");
			}
		});

		assertEquals(errConfigLevel, CONTEXT.getConfiguration().getLoggerConfig(ERR).getExplicitLevel());
		assertEquals(errLoggerLevel, CONTEXT.getLogger(ERR).getLevel());
	}
}
