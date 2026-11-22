package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.client.LoadStepClientBase;
import com.dell.spt.base.load.step.client.LoggerLevels;
import com.dell.spt.base.load.step.linear.LinearLoadStepClient;
import com.dell.spt.base.load.step.linear.LinearLoadStepLocal;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

/** A step whose constructor refuses its configuration leaves nothing for the exit hook to close. */
class RefusedStepRegistrationTest {

	private static final class Client extends LinearLoadStepClient {
		Client(final Config config, final List<Config> ctxConfigs) {
			super(config, List.of(), ctxConfigs, mock(MetricsManager.class));
		}

		@Override
		protected LinearLoadStepClient copyInstance(final Config config, final List<Config> ctxConfigs) {
			return new Client(config, ctxConfigs);
		}
	}

	private static final class Local extends LinearLoadStepLocal {
		Local(final Config config) {
			super(config, List.of(), null, mock(MetricsManager.class));
		}
	}

	private static boolean stepRegistered() {
		return DaemonBase.isRegistered(daemon -> daemon instanceof Client || daemon instanceof Local);
	}

	private static Config exactCountWithoutMode() {
		final var config = TestConfigBuilder.config();
		config.val("storage-integrity-output-requireExactCount", true);
		return config;
	}

	private static Config standaloneDelete() {
		final var config = TestConfigBuilder.config();
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-step-limit-time", 0);
		return config;
	}

	/**
	 * Runs closeAll and returns the failures it logged about a step, leaving other daemons' records out. closeAll
	 * logs them as warnings on the Errors logger, which lets them through meanwhile, whatever level an earlier test
	 * in this JVM left it at.
	 */
	private static List<Throwable> stepCloseFailures() throws InterruptedException {
		final var sentinel = new Throwable("closeAll test sentinel", null, false, false) {};
		final var appender = new CloseFailureAppender(sentinel);
		appender.start();
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.ERR.getName());
		try (var levels = LoggerLevels.atMost(Level.WARN, Loggers.ERR.getName())) {
			logger.addAppender(appender);
			try {
				DaemonBase.closeAll();
				// The engine's loggers are asynchronous but keep their order: what closeAll logged arrives first.
				Loggers.ERR.warn("closeAll test sentinel", sentinel);
				assertTrue(appender.awaitSentinel(), "the sentinel record never arrived");
			} finally {
				logger.removeAppender(appender);
				appender.stop();
			}
		}
		return appender.failures().stream()
						.filter(RefusedStepRegistrationTest::comesFromAStep)
						.toList();
	}

	/** Collects the throwables of the Errors logger's warnings until the sentinel's record arrives. */
	private static final class CloseFailureAppender extends AbstractAppender {
		private final List<Throwable> failures = Collections.synchronizedList(new ArrayList<>());
		private final CountDownLatch sentinelSeen = new CountDownLatch(1);
		private final Throwable sentinel;

		CloseFailureAppender(final Throwable sentinel) {
			super("refused-step-close-capture", null, null, true, Property.EMPTY_ARRAY);
			this.sentinel = sentinel;
		}

		@Override
		public void append(final LogEvent event) {
			final var thrown = event.getThrown();
			if (thrown == sentinel) {
				sentinelSeen.countDown();
			} else if (thrown != null && sentinelSeen.getCount() > 0 && event.getLevel().isMoreSpecificThan(Level.WARN)) {
				failures.add(thrown);
			}
		}

		boolean awaitSentinel() throws InterruptedException {
			return sentinelSeen.await(10, TimeUnit.SECONDS);
		}

		List<Throwable> failures() {
			return List.copyOf(failures);
		}
	}

	/**
	 * Whether a failure comes from closing a step, not another kind of daemon an earlier test class
	 * left registered. A step another class left open, whose close throws, would still show here.
	 */
	private static boolean comesFromAStep(final Throwable thrown) {
		final var stepPackage = LoadStepClientBase.class.getPackageName().replaceFirst("\\.client$", ".");
		return Arrays.stream(thrown.getStackTrace())
						.anyMatch(frame -> frame.getClassName().startsWith(stepPackage));
	}

	@Test
	void aClientRefusedForItsIntegrityConfigurationThrowsNothingInCloseAll() throws InterruptedException {
		assertThrows(IntegrityTerminalException.class, () -> new Client(exactCountWithoutMode(), null));

		assertFalse(stepRegistered());
		assertEquals(List.of(), stepCloseFailures());
	}

	@Test
	void aLocalStepRefusedForItsIntegrityConfigurationIsNotRegistered() {
		assertThrows(IntegrityTerminalException.class, () -> new Local(exactCountWithoutMode()));

		assertFalse(stepRegistered());
	}

	@Test
	void aStandaloneDeleteWithShuffleIsNotRegistered() {
		final var config = standaloneDelete();
		config.val("load-op-shuffle", true);

		assertThrows(IllegalConfigurationException.class, () -> new Client(config, null));

		assertFalse(stepRegistered());
	}

	/** This one throws in the client's own constructor, after the base constructor has finished. */
	@Test
	void aNegativeFailureBudgetIsNotRegistered() {
		final var config = standaloneDelete();
		config.val("load-op-failureBudget-mode", "fixed");
		config.val("load-op-failureBudget-maxFailedObjects", -1L);
		config.val("load-op-failureBudget-maxFailurePercent", 0.0);
		config.val("load-op-failureBudget-graceSeconds", 30L);

		final var refused = assertThrows(RuntimeException.class, () -> new Client(config, null));

		assertTrue(refused.getMessage().contains("maxFailedObjects"), refused.toString());
		assertFalse(stepRegistered());
	}

	@Test
	void aScenarioConfigCopyThatIsRefusedLeavesOnlyTheGoodStep() throws Exception {
		try (final var good = new Client(TestConfigBuilder.config(), null)) {
			assertThrows(
							IntegrityTerminalException.class,
							() -> good.config(Map.of("storage", Map.of("integrity", Map.of("mode", "bogus")))));

			assertFalse(DaemonBase.isRegistered(daemon -> daemon instanceof Client && daemon != good));
		}
	}

	@Test
	void aFullyBuiltUnclosedStepIsStillClosedByCloseAll() throws InterruptedException {
		final var step = new Client(TestConfigBuilder.config(), null);
		assertTrue(DaemonBase.isRegistered(daemon -> daemon == step));

		assertEquals(List.of(), stepCloseFailures());

		assertTrue(step.isClosed());
	}
}
