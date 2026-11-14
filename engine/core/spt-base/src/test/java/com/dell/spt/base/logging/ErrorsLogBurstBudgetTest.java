package com.dell.spt.base.logging;

import static com.dell.spt.base.Constants.KEY_HOME_DIR;
import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The rate limits of {@code errors.log}, checked against the shipped {@code log4j2.yaml}. Each
 * test starts a private logger context from that file, logs through the Errors logger, and counts
 * the lines written to the step's {@code errors.log}.
 */
final class ErrorsLogBurstBudgetTest {

	/** The maxBurst of each errFile budget in log4j2.yaml. */
	private static final int BUDGET = 1000;

	private static final int FLOOD = BUDGET + 500;

	/** maxBurst / rate in log4j2.yaml: a used slot comes back after this long. */
	private static final Duration SLOT_RETURN = Duration.ofSeconds(10);

	@TempDir
	Path homeDir;

	@Test
	void debugFloodDoesNotDropAFollowingWarning() throws Exception {
		final var lines = logToErrorsLog(errors -> {
			flood(errors, Level.DEBUG, FLOOD);
			errors.warn("warning after the debug flood");
		});

		assertEquals(BUDGET, count(lines, Level.DEBUG));
		assertEquals(List.of("warning after the debug flood"), messages(lines, Level.WARN));
	}

	@Test
	void warningFloodIsCutOffWithoutUsingTheDebugBudget() throws Exception {
		final var lines = logToErrorsLog(errors -> {
			flood(errors, Level.WARN, FLOOD);
			errors.debug("debug line after the warning flood");
		});

		assertEquals(BUDGET, count(lines, Level.WARN));
		assertEquals(List.of("debug line after the warning flood"), messages(lines, Level.DEBUG));
	}

	@Test
	void infoAndDebugShareOneBudget() throws Exception {
		final var lines = logToErrorsLog(errors -> {
			for (int i = 0; i < FLOOD; i++) {
				errors.log(i % 2 == 0 ? Level.INFO : Level.DEBUG, "info or debug line {}", i);
			}
		});

		assertEquals(BUDGET, count(lines, Level.INFO) + count(lines, Level.DEBUG));
		assertEquals(BUDGET / 2, count(lines, Level.INFO));
	}

	@Test
	void errorsAreNotLimited() throws Exception {
		final var lines = logToErrorsLog(errors -> {
			flood(errors, Level.DEBUG, FLOOD);
			flood(errors, Level.WARN, FLOOD);
			flood(errors, Level.ERROR, FLOOD);
			errors.fatal("fatal line after every flood");
		});

		assertAll(
						() -> assertEquals(FLOOD, count(lines, Level.ERROR)),
						() -> assertEquals(List.of("fatal line after every flood"), messages(lines, Level.FATAL)),
						() -> assertEquals(BUDGET, count(lines, Level.DEBUG)),
						() -> assertEquals(BUDGET, count(lines, Level.WARN)));
	}

	/**
	 * Logs through the Errors logger of a logger context started from the shipped log4j2.yaml, with
	 * its step files under {@link #homeDir}, and returns the lines of the step's errors.log.
	 */
	private List<String> logToErrorsLog(final Consumer<Logger> body) throws Exception {
		final String stepId = "errors-budget-" + System.nanoTime();
		// The first use of Loggers starts the JVM's global logger context. Use it before home_dir points
		// at this test's temp dir, so the global context's files don't go there.
		final String errorsLogger = Loggers.ERR.getName();
		final var ctx = new LoggerContext(stepId);
		final long elapsedNanos;
		try (final var ignored = CloseableThreadContext.put(KEY_HOME_DIR, homeDir.toString())
						.put(KEY_STEP_ID, stepId)) {
			final var source = ConfigurationSource.fromUri(shippedConfig().toURI());
			try {
				ctx.start(ConfigurationFactory.getInstance().getConfiguration(ctx, source));
				// The Errors logger also writes INFO and above to the console. Keep the floods out of
				// the build output: this test reads errors.log only.
				ctx.getConfiguration().getLoggerConfig(errorsLogger).removeAppender("stdout");
				final long start = System.nanoTime();
				body.accept(ctx.getLogger(errorsLogger));
				elapsedNanos = System.nanoTime() - start;
			} finally {
				ctx.stop();
			}
		}
		// The exact counts hold only while no used slot has come back.
		assumeTrue(
						elapsedNanos < SLOT_RETURN.toNanos(),
						"logging took " + Duration.ofNanos(elapsedNanos) + ", so used slots may have come back");
		return Files.readAllLines(homeDir.resolve("log").resolve(stepId).resolve("errors.log"));
	}

	/** The log4j2.yaml the engine loads: the only one on the classpath, from src/main/resources. */
	private static URL shippedConfig() throws Exception {
		final var found = Collections.list(
						ErrorsLogBurstBudgetTest.class.getClassLoader().getResources("log4j2.yaml"));
		assertEquals(1, found.size(), "log4j2.yaml resources on the test classpath: " + found);
		return found.get(0);
	}

	private static void flood(final Logger errors, final Level level, final int count) {
		for (int i = 0; i < count; i++) {
			errors.log(level, "{} flood line {}", level, i);
		}
	}

	private static long count(final List<String> lines, final Level level) {
		return lines.stream().filter(line -> level.name().equals(field(line, 1))).count();
	}

	private static List<String> messages(final List<String> lines, final Level level) {
		return lines.stream().filter(line -> level.name().equals(field(line, 1))).map(line -> field(line, 4)).toList();
	}

	/** A field of the errors.log pattern "date | level | class | thread | message". */
	private static String field(final String line, final int index) {
		return line.split(" \\| ", 5)[index];
	}
}
