package com.dell.spt.base;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.akurilov.confuse.impl.BasicConfig;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.dell.spt.base.logging.Loggers;

class MainTest {

	private Level originalLevel;

	@BeforeEach
	void rememberOriginalLevel() {
		originalLevel = LogManager.getRootLogger().getLevel();
		setRootLevel(Level.INFO);
	}

	@AfterEach
	void restoreLevel() {
		setRootLevel(originalLevel);
	}

	@Test
	void testResolveLogPath() {
		// Test that SPT_LOG_DIR property overrides the default
		final String originalProp = System.getProperty("spt.log.dir");
		try {
			System.setProperty("spt.log.dir", "/custom/log/path");
			assertEquals("/custom/log/path", Main.resolveLogPath(), "spt.log.dir property should override default log path");

			System.clearProperty("spt.log.dir");

			// Testing env var is tricky, but we can verify it falls back to user.dir
			// if neither the env var nor property are set (assuming env var is not set in test env)
			if (System.getenv("SPT_LOG_DIR") == null || System.getenv("SPT_LOG_DIR").isEmpty()) {
				assertEquals(System.getProperty("user.dir"), Main.resolveLogPath(), "Should fallback to user.dir");
			}
		} finally {
			if (originalProp != null) {
				System.setProperty("spt.log.dir", originalProp);
			} else {
				System.clearProperty("spt.log.dir");
			}
		}
	}

	@Test
	void applyLogLevelHonorsDebugSetting() throws Exception {
		final var schema = Map.<String, Object> of("log", Map.of("level", String.class));
		final var values = Map.<String, Object> of("log", Map.of("level", "debug"));
		final var config = new BasicConfig("-", schema, values);
		assertEquals(false, Loggers.MSG.isEnabled(Level.DEBUG), "baseline should be INFO before applying level");
		invokeApplyLogLevel(config);
		assertEquals(true, Loggers.MSG.isEnabled(Level.DEBUG));
	}

	@Test
	void applyLogLevelIgnoresInvalidValue() throws Exception {
		final var schema = Map.<String, Object> of("log", Map.of("level", String.class));
		final var values = Map.<String, Object> of("log", Map.of("level", "not-a-level"));
		final var config = new BasicConfig("-", schema, values);
		assertEquals(false, Loggers.MSG.isEnabled(Level.DEBUG), "baseline should be INFO before applying level");
		invokeApplyLogLevel(config);
		assertEquals(false, Loggers.MSG.isEnabled(Level.DEBUG));
	}

	private static void invokeApplyLogLevel(final com.github.akurilov.confuse.Config config)
					throws Exception {
		final Method method = Main.class.getDeclaredMethod("applyLogLevel", com.github.akurilov.confuse.Config.class);
		method.setAccessible(true);
		method.invoke(null, config);
	}

	private static void setRootLevel(final Level level) {
		final LoggerContext ctx = LoggerContext.getContext(false);
		final var configuration = ctx.getConfiguration();
		final LoggerConfig rootConfig = configuration.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
		rootConfig.setLevel(level);
		final LoggerConfig msgConfig = configuration.getLoggerConfig(Loggers.MSG.getName());
		if (msgConfig != null) {
			msgConfig.setLevel(level);
		}
		final org.apache.logging.log4j.core.Logger msgLogger = ctx.getLogger(Loggers.MSG.getName());
		if (msgLogger != null) {
			msgLogger.setLevel(level);
		}
		ctx.updateLoggers();
	}

}
