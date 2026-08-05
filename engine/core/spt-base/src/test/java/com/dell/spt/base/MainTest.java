package com.dell.spt.base;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

	@Test
	void testApplyVtParallelismSetsSystemProperty() throws Exception {
		final var schema = Map.<String, Object> of("load", Map.of("service", Map.of("threads", Integer.class)));
		final var values = Map.<String, Object> of("load", Map.of("service", Map.of("threads", 16)));
		final var config = new BasicConfig("-", schema, values);

		final String originalProp = System.getProperty("jdk.virtualThreadScheduler.parallelism");
		try {
			Main.applyVtParallelism(config);
			assertEquals("16", System.getProperty("jdk.virtualThreadScheduler.parallelism"));
		} finally {
			if (originalProp != null) {
				System.setProperty("jdk.virtualThreadScheduler.parallelism", originalProp);
			} else {
				System.clearProperty("jdk.virtualThreadScheduler.parallelism");
			}
		}
	}

	@Test
	void testApplyVtParallelismIgnoresZeroOrNegative() throws Exception {
		final var schema = Map.<String, Object> of("load", Map.of("service", Map.of("threads", Integer.class)));
		final var values = Map.<String, Object> of("load", Map.of("service", Map.of("threads", 0)));
		final var config = new BasicConfig("-", schema, values);

		final String originalProp = System.getProperty("jdk.virtualThreadScheduler.parallelism");
		try {
			System.clearProperty("jdk.virtualThreadScheduler.parallelism");
			Main.applyVtParallelism(config);
			assertEquals(null, System.getProperty("jdk.virtualThreadScheduler.parallelism"));

			final var configNeg = new BasicConfig("-", schema, Map.of("load", Map.of("service", Map.of("threads", -1))));
			Main.applyVtParallelism(configNeg);
			assertEquals(null, System.getProperty("jdk.virtualThreadScheduler.parallelism"));
		} finally {
			if (originalProp != null) {
				System.setProperty("jdk.virtualThreadScheduler.parallelism", originalProp);
			} else {
				System.clearProperty("jdk.virtualThreadScheduler.parallelism");
			}
		}
	}

	@Test
	void initializeRunIdReplacesZeroOnce() {
		final var config = runIdConfig(0L);
		Main.initializeRunId(config);
		final long generated = config.longVal("run-id");
		assertTrue(generated > 0L);

		Main.initializeRunId(config);
		assertEquals(generated, config.longVal("run-id"));
	}

	@Test
	void initializeRunIdPreservesExplicitValue() {
		final var config = runIdConfig(123456789L);
		Main.initializeRunId(config);
		assertEquals(123456789L, config.longVal("run-id"));
	}

	private static BasicConfig runIdConfig(final long runId) {
		return new BasicConfig(
						"-", Map.of("run", Map.of("id", Long.class)), Map.of("run", Map.of("id", runId)));
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
