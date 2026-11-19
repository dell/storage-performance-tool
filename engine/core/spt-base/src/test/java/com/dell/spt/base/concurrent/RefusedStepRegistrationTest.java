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
import com.dell.spt.base.load.step.linear.LinearLoadStepClient;
import com.dell.spt.base.load.step.linear.LinearLoadStepLocal;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
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

	/** Runs closeAll and returns what it logged about a step, leaving other daemons' records out. */
	private static List<LogRecord> stepCloseFailures() {
		final List<LogRecord> records = new ArrayList<>();
		final Handler handler = new Handler() {
			@Override
			public void publish(final LogRecord record) {
				records.add(record);
			}

			@Override
			public void flush() {}

			@Override
			public void close() {}
		};
		final Logger logger = Logger.getLogger(DaemonBase.class.getSimpleName());
		logger.addHandler(handler);
		try {
			DaemonBase.closeAll();
		} finally {
			logger.removeHandler(handler);
		}
		return records.stream()
						.filter(RefusedStepRegistrationTest::comesFromARefusedStep)
						.toList();
	}

	private static boolean comesFromARefusedStep(final LogRecord record) {
		final var thrown = record.getThrown();
		if (thrown == null) {
			return false;
		}
		return thrown instanceof NullPointerException
						|| Arrays.stream(thrown.getStackTrace())
										.anyMatch(frame -> frame.getClassName().equals(LoadStepClientBase.class.getName()));
	}

	@Test
	void aClientRefusedForItsIntegrityConfigurationThrowsNothingInCloseAll() {
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
	void aFullyBuiltUnclosedStepIsStillClosedByCloseAll() {
		final var step = new Client(TestConfigBuilder.config(), null);
		assertTrue(DaemonBase.isRegistered(daemon -> daemon == step));

		assertEquals(List.of(), stepCloseFailures());

		assertTrue(step.isClosed());
	}
}
