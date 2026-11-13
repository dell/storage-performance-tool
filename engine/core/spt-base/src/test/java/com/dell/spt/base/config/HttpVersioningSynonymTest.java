package com.dell.spt.base.config;

import static com.dell.spt.base.config.HttpVersioningSynonym.SYNONYM_PATH;
import static com.dell.spt.base.config.HttpVersioningSynonym.TARGET_PATH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/** The Mongoose synonym {@code storage.net.http.versioning} for {@code storage.object.versioning}. */
class HttpVersioningSynonymTest {

	/**
	 * The base schema plus the two keys, as the HTTP protocol and S3 driver schemas add them to the merged schema,
	 * with both at their shipped default {@code false}.
	 */
	static Config configWithVersioningKeys(final String driverType) {
		final var base = TestConfigBuilder.config();
		final Map<String, Object> keys = Map.of(
						"storage", Map.of(
										"object", Map.of("versioning", Boolean.class),
										"net", Map.of("http", Map.of("versioning", Boolean.class))));
		final var schema = TreeUtil.reduceForest(List.of(TreeUtil.copyTree(base.schema()), keys));
		final Config config = new BasicConfig(base.pathSep(), schema, Config.deepToMap(base));
		config.val("storage-driver-type", driverType);
		config.val(TARGET_PATH, false);
		config.val(SYNONYM_PATH, false);
		return config;
	}

	private static void resolve(final Config config) {
		HttpVersioningSynonym.resolve(config, null, new AtomicBoolean());
	}

	@ParameterizedTest
	@ValueSource(strings = {"s3", "s3-rdma", "s3-tables", "s3-aws"
	})
	void theSynonymTurnsOnVersioningForEachS3DriverType(final String driverType) {
		final var config = configWithVersioningKeys(driverType);
		config.val(SYNONYM_PATH, true);

		resolve(config);

		assertTrue(config.boolVal(TARGET_PATH));
		assertTrue(config.boolVal(SYNONYM_PATH), "the synonym itself is left as given");
	}

	@ParameterizedTest
	@ValueSource(strings = {"s3", "s3-rdma", "s3-tables", "s3-aws"
	})
	void aCommandLineStringIsReadAsTheBoolean(final String driverType) {
		final var on = configWithVersioningKeys(driverType);
		on.val(SYNONYM_PATH, "true");
		final var off = configWithVersioningKeys(driverType);
		off.val(SYNONYM_PATH, "false");

		resolve(on);
		resolve(off);

		assertTrue(on.boolVal(TARGET_PATH));
		assertFalse(off.boolVal(TARGET_PATH));
	}

	@ParameterizedTest
	@ValueSource(strings = {"s3", "s3-rdma", "s3-tables", "s3-aws"
	})
	void theSynonymFalseChangesNothing(final String driverType) {
		final var off = configWithVersioningKeys(driverType);
		final var on = configWithVersioningKeys(driverType);
		on.val(TARGET_PATH, true);

		resolve(off);
		resolve(on);

		assertFalse(off.boolVal(TARGET_PATH));
		assertTrue(on.boolVal(TARGET_PATH), "the synonym false never turns storage.object.versioning off");
	}

	@Test
	void theSynonymUnsetChangesNothing() {
		final var off = configWithVersioningKeys("s3");
		off.val(SYNONYM_PATH, null);
		final var on = configWithVersioningKeys("s3");
		on.val(SYNONYM_PATH, null);
		on.val(TARGET_PATH, true);

		resolve(off);
		resolve(on);

		assertFalse(off.boolVal(TARGET_PATH));
		assertTrue(on.boolVal(TARGET_PATH));
	}

	@Test
	void eitherKeyTrueTurnsVersioningOn() {
		final var synonymOnly = configWithVersioningKeys("s3");
		synonymOnly.val(SYNONYM_PATH, true);
		synonymOnly.val(TARGET_PATH, false);
		final var targetOnly = configWithVersioningKeys("s3");
		targetOnly.val(TARGET_PATH, true);
		final var both = configWithVersioningKeys("s3");
		both.val(SYNONYM_PATH, true);
		both.val(TARGET_PATH, true);

		resolve(synonymOnly);
		resolve(targetOnly);
		resolve(both);

		assertTrue(synonymOnly.boolVal(TARGET_PATH), "storage.object.versioning=false with the synonym true is on");
		assertTrue(targetOnly.boolVal(TARGET_PATH));
		assertTrue(both.boolVal(TARGET_PATH));
	}

	@ParameterizedTest
	@ValueSource(strings = {"swift", "atmos", "fs", "dummy-mock", "S3", " s3"
	})
	void otherDriverTypesAreLeftAlone(final String driverType) {
		final var config = configWithVersioningKeys(driverType);
		config.val(SYNONYM_PATH, true);

		resolve(config);

		assertFalse(config.boolVal(TARGET_PATH));
		assertTrue(config.boolVal(SYNONYM_PATH), "Swift keeps reading its own key");
	}

	@Test
	void missingSchemaKeysAreHarmless() {
		// The base schema alone declares neither key, as with no HTTP protocol or S3 extension on the classpath.
		final var bare = TestConfigBuilder.config();
		bare.val("storage-driver-type", "s3");
		resolve(bare);

		// The synonym is declared but storage.object.versioning isn't: no S3 driver schema, so no S3 reader.
		final var base = TestConfigBuilder.config();
		final Map<String, Object> synonymOnly = Map.of(
						"storage", Map.of("net", Map.of("http", Map.of("versioning", Boolean.class))));
		final Config config = new BasicConfig(
						base.pathSep(),
						TreeUtil.reduceForest(List.of(TreeUtil.copyTree(base.schema()), synonymOnly)),
						Config.deepToMap(base));
		config.val("storage-driver-type", "s3");
		config.val(SYNONYM_PATH, true);
		final var logged = new AtomicBoolean();
		HttpVersioningSynonym.resolve(config, null, logged);

		assertFalse(logged.get(), "nothing was applied, so nothing is logged");

		// No driver type at all.
		final var noDriverType = configWithVersioningKeys("s3");
		noDriverType.val("storage-driver-type", null);
		noDriverType.val(SYNONYM_PATH, true);
		resolve(noDriverType);
		assertFalse(noDriverType.boolVal(TARGET_PATH));
	}

	@Test
	void eachContextIsResolvedOnItsOwnValues() {
		final var step = configWithVersioningKeys("s3");
		final var synonymContext = new BasicConfig(step);
		synonymContext.val(SYNONYM_PATH, true);
		final var plainContext = new BasicConfig(step);
		final var swiftContext = new BasicConfig(step);
		swiftContext.val("storage-driver-type", "swift");
		swiftContext.val(SYNONYM_PATH, true);

		HttpVersioningSynonym.resolve(step, List.of(synonymContext, plainContext, swiftContext), new AtomicBoolean());

		assertFalse(step.boolVal(TARGET_PATH));
		assertTrue(synonymContext.boolVal(TARGET_PATH));
		assertFalse(plainContext.boolVal(TARGET_PATH));
		assertFalse(swiftContext.boolVal(TARGET_PATH));
	}

	@Test
	void aContextOmittingAKeyInheritsItFromTheStep() {
		final var step = configWithVersioningKeys("s3-aws");
		step.val(SYNONYM_PATH, true);
		// A partial context: only storage.object.versioning, explicitly false.
		final var schema = step.schema();
		final Map<String, Object> storage = new HashMap<>();
		storage.put("object", new HashMap<>(Map.of("versioning", false)));
		final Config partial = new BasicConfig(step.pathSep(), schema, new HashMap<>(Map.of("storage", storage)));

		HttpVersioningSynonym.resolve(step, new ArrayList<>(List.of(partial)), new AtomicBoolean());

		assertTrue(step.boolVal(TARGET_PATH));
		assertTrue(partial.boolVal(TARGET_PATH), "the merged context has the step's driver type and synonym");
		assertEquals(
						Map.of("object", Map.of("versioning", true)),
						Config.deepToMap(partial).get("storage"),
						"the context is not filled in beyond the target");
	}

	@Test
	void theNoticeIsLoggedOnceInTheAliasStyle() throws InterruptedException {
		final var appender = new CapturingAppender();
		appender.start();
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.ERR.getName());
		logger.addAppender(appender);
		try {
			final var guard = new AtomicBoolean();
			for (int i = 0; i < 3; i++) {
				final var config = configWithVersioningKeys("s3");
				config.val(SYNONYM_PATH, true);
				HttpVersioningSynonym.resolve(config, null, guard);
			}
			final var unused = configWithVersioningKeys("s3");
			HttpVersioningSynonym.resolve(unused, null, new AtomicBoolean());

			// Engine loggers are asynchronous; wait for the one record, then for any stray second one.
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (appender.count() == 0 && System.nanoTime() < deadline) {
				Thread.sleep(10);
			}
			Thread.sleep(200);
			assertEquals(1, appender.count(), appender.messages::toString);
			assertEquals(
							"The argument \"storage-net-http-versioning\" is the Mongoose name for S3 versioning, "
											+ "use \"storage-object-versioning\" instead",
							appender.messages.get(0));
			assertTrue(appender.warn, "logged at WARN, as the alias table logs a deprecated name");
		} finally {
			logger.removeAppender(appender);
			appender.stop();
		}
	}

	private static final class CapturingAppender extends AbstractAppender {
		final List<String> messages = Collections.synchronizedList(new ArrayList<>());
		volatile boolean warn = true;

		CapturingAppender() {
			super("testHttpVersioningSynonymNotice", null, null, true, Property.EMPTY_ARRAY);
		}

		@Override
		public void append(final LogEvent event) {
			final String message = event.getMessage().getFormattedMessage();
			if (message.contains(SYNONYM_PATH)) {
				messages.add(message);
				warn &= org.apache.logging.log4j.Level.WARN.equals(event.getLevel());
			}
		}

		int count() {
			return messages.size();
		}
	}
}
