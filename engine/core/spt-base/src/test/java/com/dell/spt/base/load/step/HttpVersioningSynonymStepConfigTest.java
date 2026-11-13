package com.dell.spt.base.load.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.load.step.linear.LinearLoadStepClient;
import com.dell.spt.base.load.step.linear.LinearLoadStepLocal;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The Mongoose synonym {@code storage.net.http.versioning} is resolved on the step configuration, so the recorded
 * {@code config.yaml}, the drivers and the worker slices all see {@code storage.object.versioning: true}.
 */
class HttpVersioningSynonymStepConfigTest {
	private static final String SYNONYM = "storage-net-http-versioning";
	private static final String TARGET = "storage-object-versioning";
	private static final ScriptEngine SCENARIO_ENGINE = new ScriptEngineManager().getEngineByName("js");

	private final String stepId = "http-versioning-synonym-" + System.nanoTime();
	private CapturingAppender appender;
	private Level previousLevel;

	@BeforeEach
	void attach() {
		appender = new CapturingAppender(stepId);
		appender.start();
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.CONFIG.getName());
		previousLevel = logger.getLevel();
		logger.addAppender(appender);
		logger.setLevel(Level.INFO);
	}

	@AfterEach
	void detach() {
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.CONFIG.getName());
		logger.setLevel(previousLevel);
		logger.removeAppender(appender);
		appender.stop();
	}

	/** The base schema plus the HTTP protocol's synonym and the S3 drivers' key, both at their shipped false. */
	private Config config(final String driverType) {
		final var base = TestConfigBuilder.config();
		final Map<String, Object> keys = Map.of(
						"storage", Map.of(
										"object", Map.of("versioning", Boolean.class),
										"net", Map.of("http", Map.of("versioning", Boolean.class))));
		final Config config = new BasicConfig(
						base.pathSep(),
						TreeUtil.reduceForest(List.of(TreeUtil.copyTree(base.schema()), keys)),
						Config.deepToMap(base));
		config.val("load-step-id", stepId);
		config.val("storage-driver-type", driverType);
		config.val(TARGET, false);
		config.val(SYNONYM, false);
		return config;
	}

	@SuppressWarnings("unchecked")
	private static Map<String, Object> scenarioObject(final String object) throws Exception {
		return (Map<String, Object>) SCENARIO_ENGINE.eval("(" + object + ")");
	}

	@SuppressWarnings("unchecked")
	private Map<String, Object> recordedStorage() throws Exception {
		assertTrue(appender.awaitFirst(5, TimeUnit.SECONDS), "no step configuration was recorded");
		final Map<String, Object> recorded = new YAMLMapper().readValue(appender.messages().get(0), Map.class);
		return (Map<String, Object>) recorded.get("storage");
	}

	@SuppressWarnings("unchecked")
	private static Object at(final Map<String, Object> tree, final String... path) {
		Object node = tree;
		for (final var key : path) {
			node = ((Map<String, Object>) node).get(key);
		}
		return node;
	}

	@Test
	void theRecordedStepConfigurationShowsVersioningInEffect() throws Exception {
		final var config = config("s3");
		config.val(SYNONYM, true);
		final var context = config("s3");
		context.val(SYNONYM, true);

		final LoadStepBase step = new LinearLoadStepLocal(
						config, List.of(), List.of(context), mock(MetricsManager.class));

		assertTrue(step.config.boolVal(TARGET));
		assertTrue(step.ctxConfigs.get(0).boolVal(TARGET));
		assertFalse(config.boolVal(TARGET), "the caller's configuration is not changed");
		assertFalse(context.boolVal(TARGET));
		final var storage = recordedStorage();
		assertEquals(Boolean.TRUE, at(storage, "object", "versioning"), storage::toString);
		assertEquals(Boolean.TRUE, at(storage, "net", "http", "versioning"), storage::toString);
	}

	@Test
	void theShippedDefaultsRecordNeitherKeyOn() throws Exception {
		final LoadStepBase step = new LinearLoadStepLocal(config("s3"), List.of(), null, mock(MetricsManager.class));

		assertFalse(step.config.boolVal(TARGET));
		final var storage = recordedStorage();
		assertEquals(Boolean.FALSE, at(storage, "object", "versioning"), storage::toString);
		assertEquals(Boolean.FALSE, at(storage, "net", "http", "versioning"), storage::toString);
	}

	@Test
	void aSwiftStepKeepsTheKeyToItself() throws Exception {
		final var config = config("swift");
		config.val(SYNONYM, true);

		final LoadStepBase step = new LinearLoadStepLocal(config, List.of(), null, mock(MetricsManager.class));

		assertFalse(step.config.boolVal(TARGET));
		assertTrue(step.config.boolVal(SYNONYM));
		assertEquals(Boolean.FALSE, at(recordedStorage(), "object", "versioning"));
	}

	@Test
	void aScenarioConfigCallSetsTheSynonym() throws Exception {
		final var prototype = new LinearLoadStepClient(config("s3-aws"), List.of(), null, mock(MetricsManager.class));

		final LoadStepBase configured = prototype.config(
						scenarioObject("{storage: {net: {http: {versioning: true}}}}"));

		assertTrue(configured.config.boolVal(TARGET));
		assertFalse(((LoadStepBase) prototype).config.boolVal(TARGET), "the prototype is not changed");
	}

	@Test
	void aScenarioConfigFalseKeepsTheVersioningAnEngineSynonymTurnedOn() throws Exception {
		final var engineConfig = config("s3");
		engineConfig.val(SYNONYM, true);
		final var prototype = new LinearLoadStepClient(engineConfig, List.of(), null, mock(MetricsManager.class));

		final LoadStepBase synonymOff = prototype.config(
						scenarioObject("{storage: {net: {http: {versioning: false}}}}"));
		final LoadStepBase bothOff = prototype.config(
						scenarioObject("{storage: {object: {versioning: false}, net: {http: {versioning: false}}}}"));

		assertTrue(
						synonymOff.config.boolVal(TARGET),
						"the prototype resolved the engine's synonym, and false never turns versioning off");
		assertFalse(bothOff.config.boolVal(TARGET), "setting both keys false turns it off for the step");
	}

	@Test
	void aScenarioAppendCallSetsTheSynonymForThatContext() throws Exception {
		final var prototype = new LinearLoadStepClient(config("s3-tables"), List.of(), null, mock(MetricsManager.class));

		final LoadStepBase appended = prototype
						.append(scenarioObject("{storage: {net: {http: {versioning: true}}}}"))
						.append(scenarioObject("{load: {op: {limit: {count: 1}}}}"));

		assertFalse(appended.config.boolVal(TARGET), "the step's own configuration has no synonym");
		assertTrue(appended.ctxConfigs.get(0).boolVal(TARGET));
		assertFalse(appended.ctxConfigs.get(1).boolVal(TARGET));
	}

	private static final class CapturingAppender extends AbstractAppender {
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());
		private final CountDownLatch first = new CountDownLatch(1);
		private final String marker;

		CapturingAppender(final String marker) {
			super("testHttpVersioningSynonymStepConfig", null, null, true, Property.EMPTY_ARRAY);
			this.marker = marker;
		}

		@Override
		public void append(final LogEvent event) {
			final String message = event.getMessage().getFormattedMessage();
			if (message.contains(marker)) {
				captured.add(message);
				first.countDown();
			}
		}

		boolean awaitFirst(final long timeout, final TimeUnit unit) throws InterruptedException {
			return first.await(timeout, unit);
		}

		List<String> messages() {
			return List.copyOf(captured);
		}
	}
}
