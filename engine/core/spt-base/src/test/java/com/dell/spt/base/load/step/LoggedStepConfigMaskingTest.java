package com.dell.spt.base.load.step;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.load.step.linear.LinearLoadStepLocal;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LoggedStepConfigMaskingTest {
	// Engine loggers are asynchronous, so a record from a step built by an earlier test can arrive
	// after this test attaches its appender. Only this test's step id identifies its own record.
	private final String stepId = "masking-" + System.nanoTime();
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

	@Test
	void stepConfigLogMasksCredentialsWhileStepKeepsThem() throws InterruptedException {
		final var config = TestConfigBuilder.config();
		config.val("load-step-id", stepId);
		config.val("storage-auth-uid", "step-uid-value");
		config.val("storage-auth-secret", "step-secret-value");

		final LoadStepBase step = new LinearLoadStepLocal(
						config, List.of(), List.of(TestConfigBuilder.config()), mock(MetricsManager.class));

		// Engine loggers are asynchronous; wait for the step's configuration record.
		assertTrue(appender.awaitFirst(5, TimeUnit.SECONDS), "no configuration was logged");
		final var logged = String.join("\n", appender.messages());
		assertTrue(logged.contains(ConfigUtil.MASKED_VALUE), logged);
		assertFalse(logged.contains("step-uid-value"));
		assertFalse(logged.contains("step-secret-value"));
		assertEquals("step-uid-value", step.config.stringVal("storage-auth-uid"));
		assertEquals("step-secret-value", step.config.stringVal("storage-auth-secret"));
	}

	private static final class CapturingAppender extends AbstractAppender {
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());
		private final CountDownLatch first = new CountDownLatch(1);
		private final String marker;

		CapturingAppender(final String marker) {
			super("testStepConfigCapture", null, null, true, Property.EMPTY_ARRAY);
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
