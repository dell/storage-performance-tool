package com.dell.spt.base.load.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.dell.spt.base.Constants;
import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.linear.LinearLoadStepExtension;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.script.ScriptEngine;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

/**
 * With {@code load-op-delete-standalone} in the engine's base configuration, every step-type
 * prototype the scenario engine registers is a standalone DELETE client. None of them runs, and
 * each closes at exit without a failure-budget verdict.
 */
class StandaloneDeletePrototypeCloseTest {

	@Test
	void theScenarioPrototypesCloseWithoutAVerdict() throws Exception {
		final Config config;
		try (var defaults = getClass().getResourceAsStream("/config/defaults.yaml")) {
			config = ConfigUtil.loadConfig(new String(defaults.readAllBytes(), StandardCharsets.UTF_8), ConfigFormat.YAML,
							SchemaProvider.resolveAndReduce(Constants.APP_NAME, getClass().getClassLoader()));
		}
		final String stepId = "delete-prototypes-" + UUID.randomUUID();
		config.val("load-step-id", stepId);
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		final MetricsManager metrics = mock(MetricsManager.class);
		final Map<String, Object> bindings = new LinkedHashMap<>();
		final ScriptEngine engine = mock(ScriptEngine.class);
		doAnswer(call -> {
			bindings.put(call.getArgument(0), call.getArgument(1));
			return null;
		}).when(engine).put(anyString(), any());
		final List<String> messages = Collections.synchronizedList(new ArrayList<>());
		final AbstractAppender appender = new AbstractAppender(
						"deletePrototypeCapture", null, null, true, Property.EMPTY_ARRAY) {
			@Override
			public void append(final LogEvent event) {
				messages.add(event.getMessage().getFormattedMessage());
			}
		};
		appender.start();
		final Logger logger = LoggerContext.getContext(false).getLogger(Loggers.MSG.getName());
		logger.addAppender(appender);
		final Map<String, Throwable> closeFailures = new LinkedHashMap<>();
		try {
			final List<Extension> extensions = List.of(new LinearLoadStepExtension<>());
			ScenarioUtil.registerStepTypes(engine, extensions, config, metrics);
			assertFalse(bindings.isEmpty());
			for (final var binding : bindings.entrySet()) {
				final LoadStep prototype = (LoadStep) binding.getValue();
				try {
					prototype.close();
				} catch (final Throwable failure) {
					closeFailures.put(binding.getKey(), failure);
				}
				assertTrue(prototype.isClosed(), binding.getKey() + " did not close");
			}
		} finally {
			logger.removeAppender(appender);
			appender.stop();
		}

		assertEquals(Map.of(), closeFailures);
		synchronized (messages) {
			assertEquals(List.of(), messages.stream().filter(message -> message.contains("Standalone DELETE")).toList());
		}
		verify(metrics, never()).updateTerminalDeleteFailureOutcome(anyString(), anyString());
	}
}
