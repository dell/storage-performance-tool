package com.dell.spt.base;

import static com.dell.spt.base.Constants.APP_NAME;
import static com.dell.spt.base.Constants.PATH_DEFAULTS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.load.step.client.LoggerLevels;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The engine's {@code cli.args.log}, written by {@link Main#applyArgsToConfig}: credential values and
 * the values of the listed request headers are logged as {@code ***}, in command-line order, while the
 * configuration keeps the real values. All credentials here are made up.
 */
class MainCliArgsLogMaskingTest {

	private CapturingAppender appender;
	private Level previousLevel;

	/**
	 * The root and Messages loggers at INFO for each test, and their own levels back after it: applying the
	 * arguments sets both to the shipped log level.
	 */
	private LoggerLevels levels;

	@BeforeEach
	void attach() {
		levels = LoggerLevels.set(Map.of(LogManager.ROOT_LOGGER_NAME, Level.INFO, Loggers.MSG.getName(), Level.INFO));
		appender = new CapturingAppender();
		appender.start();
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.CLI.getName());
		previousLevel = logger.getLevel();
		logger.addAppender(appender);
		logger.setLevel(Level.INFO);
	}

	@AfterEach
	void detach() {
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.CLI.getName());
		try {
			logger.setLevel(previousLevel);
			logger.removeAppender(appender);
			appender.stop();
		} finally {
			if (levels != null) {
				levels.close();
			}
		}
	}

	/**
	 * The shipped base schema and defaults, as {@code Main} loads them, with the HTTP extension's
	 * {@code storage.net.http.headers} map and its default value, which a node whose classpath holds
	 * that extension has.
	 */
	@SuppressWarnings("unchecked")
	private static Config shippedDefaultsWithHeaders() throws Exception {
		final var schema = deepCopy(SchemaProvider.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
						.stream()
						.findFirst()
						.orElseThrow());
		final var storage = (Map<String, Object>) schema.get("storage");
		final var net = (Map<String, Object>) storage.get("net");
		((Map<String, Object>) net.computeIfAbsent("http", k -> new HashMap<String, Object>())).put("headers", Map.class);
		final var defaultsFile = Paths.get(Main.class.getResource("/" + PATH_DEFAULTS).toURI()).toFile();
		final var config = ConfigUtil.loadConfig(defaultsFile, schema);
		// As the HTTP extension's defaults set it; the command line adds to this map.
		config.val("storage-net-http-headers", new HashMap<>(Map.of("Connection", "Keep-Alive")));
		return config;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> deepCopy(final Map<String, Object> tree) {
		final Map<String, Object> copy = new HashMap<>();
		for (final var entry : tree.entrySet()) {
			final var value = entry.getValue();
			copy.put(entry.getKey(), value instanceof Map ? deepCopy((Map<String, Object>) value) : value);
		}
		return copy;
	}

	/** Applies the arguments and returns the configuration and the lines logged for them, in order. */
	private Config apply(final String[] args, final List<String> logged) throws Exception {
		// The last argument is unique to the call, so its line marks the end of the call's lines.
		final var config = Main.applyArgsToConfig(args, shippedDefaultsWithHeaders(), "initial-step");
		logged.addAll(appender.linesEndingWith(args[args.length - 1], args.length));
		return config;
	}

	@Test
	void credentialAndListedHeaderValuesAreMaskedAndTheConfigurationKeepsThem() throws Exception {
		final var end = "--item-output-path=/bucket-" + System.nanoTime();
		final String[] args = {
				"--storage-driver-type=s3",
				"--storage-auth-uid=FAKE-UID-MARKER",
				"--storage-auth-secret=FAKE-SECRET-MARKER",
				"--storage-auth-token=FAKE-TOKEN-MARKER",
				"--storage-net-http-headers-Authorization=AWS FAKE-AUTH-MARKER",
				"--storage-net-http-headers-x-auth-token=FAKE-XAUTH-MARKER",
				"--storage-net-http-headers-X-Custom=custom-value",
				"--load-op-limit-count=5",
				end
		};
		final var logged = new ArrayList<String>();
		final var config = apply(args, logged);

		assertEquals(
						List.of(
										"--storage-driver-type=s3",
										"--storage-auth-uid=***",
										"--storage-auth-secret=***",
										"--storage-auth-token=***",
										"--storage-net-http-headers-Authorization=***",
										"--storage-net-http-headers-x-auth-token=***",
										"--storage-net-http-headers-X-Custom=custom-value",
										"--load-op-limit-count=5",
										end),
						logged);
		assertFalse(String.join("\n", logged).contains("MARKER"), String.join("\n", logged));
		assertEquals("FAKE-UID-MARKER", config.stringVal("storage-auth-uid"));
		assertEquals("FAKE-SECRET-MARKER", config.stringVal("storage-auth-secret"));
		assertEquals("FAKE-TOKEN-MARKER", config.stringVal("storage-auth-token"));
		final var headers = config.mapVal("storage-net-http-headers");
		assertEquals("AWS FAKE-AUTH-MARKER", headers.get("Authorization"));
		assertEquals("FAKE-XAUTH-MARKER", headers.get("x-auth-token"));
		assertEquals("custom-value", headers.get("X-Custom"));
		assertEquals("Keep-Alive", headers.get("Connection"));
	}

	@Test
	void theAliasOfTheTokenIsMaskedUnderItsOwnName() throws Exception {
		final var end = "--item-output-path=/bucket-" + System.nanoTime();
		final String[] args = {"--atmos-subtenant=FAKE-SUBTENANT-MARKER", end
		};
		final var logged = new ArrayList<String>();
		final var config = apply(args, logged);

		assertEquals(List.of("--atmos-subtenant=***", end), logged);
		assertEquals("FAKE-SUBTENANT-MARKER", config.stringVal("storage-auth-token"));
	}

	@Test
	void anEmptyValueAndAnArgumentWithoutAValueAreLoggedAsGiven() throws Exception {
		final var end = "--item-output-path=/bucket-" + System.nanoTime();
		final String[] args = {"--storage-auth-secret=", "--storage-auth-uid", end
		};
		final var logged = new ArrayList<String>();
		final var config = apply(args, logged);

		assertEquals(List.of("--storage-auth-secret=", "--storage-auth-uid", end), logged);
		assertEquals("", config.stringVal("storage-auth-secret"));
		assertEquals("true", config.stringVal("storage-auth-uid"));
	}

	private static final class CapturingAppender extends AbstractAppender {
		private final List<String> captured = new ArrayList<>();

		CapturingAppender() {
			super("testCliArgsCapture", null, null, true, Property.EMPTY_ARRAY);
		}

		@Override
		public void append(final LogEvent event) {
			synchronized (captured) {
				captured.add(event.getMessage().getFormattedMessage());
				captured.notifyAll();
			}
		}

		/**
		 * Waits for the line {@code last} and returns it with the {@code count - 1} lines before it.
		 * Engine loggers are asynchronous but keep the order of one thread's lines.
		 */
		List<String> linesEndingWith(final String last, final int count) throws InterruptedException {
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			synchronized (captured) {
				int at;
				while ((at = captured.lastIndexOf(last)) < 0) {
					final long left = deadline - System.nanoTime();
					assertTrue(left > 0, "no line " + last + " in " + captured);
					TimeUnit.NANOSECONDS.timedWait(captured, left);
				}
				assertTrue(at + 1 >= count, "too few lines before " + last + " in " + captured);
				return List.copyOf(captured.subList(at + 1 - count, at + 1));
			}
		}
	}
}
