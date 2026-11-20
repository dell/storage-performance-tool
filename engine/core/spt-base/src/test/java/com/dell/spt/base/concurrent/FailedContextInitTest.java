package com.dell.spt.base.concurrent;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.Constants;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.load.step.linear.LinearLoadStepLocal;
import com.dell.spt.base.load.step.local.context.LoadStepContextImpl;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMock;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/** A local step whose init fails after it has its driver leaves no driver open. */
@SuppressWarnings({"rawtypes", "unchecked"
})
class FailedContextInitTest {

	private static final class Step extends LinearLoadStepLocal {
		Step(final Config config, final StorageDriverFactory factory) {
			super(config, List.of(factory), null, mock(MetricsManager.class));
		}

		void runInit() {
			init();
		}
	}

	private DummyStorageDriverMock driver;

	private Config config() throws Exception {
		final var schema = SchemaProvider.resolve(Constants.APP_NAME, getClass().getClassLoader())
						.stream().findFirst().orElseThrow();
		final var config = ConfigUtil.loadConfig(
						Path.of(getClass().getResource("/config/defaults.yaml").toURI()).toFile(), schema);
		config.val("load-step-id", "failed-init-" + UUID.randomUUID());
		config.val("storage-driver-type", "stub");
		return config;
	}

	private StorageDriverFactory factory(final Config config) throws Exception {
		driver = new DummyStorageDriverMock(config.configVal("storage"));
		final StorageDriverFactory factory = mock(StorageDriverFactory.class);
		when(factory.id()).thenReturn("stub");
		when(factory.create(anyString(), any(), any(), anyBoolean(), anyInt())).thenReturn(driver);
		return factory;
	}

	private boolean isContextOfThisDriver(final Daemon daemon) {
		if (!(daemon instanceof LoadStepContextImpl)) {
			return false;
		}
		try {
			final var field = LoadStepContextImpl.class.getDeclaredField("driver");
			field.setAccessible(true);
			return field.get(daemon) == driver;
		} catch (final ReflectiveOperationException e) {
			throw new LinkageError(e.getMessage(), e);
		}
	}

	@Test
	void aContextRefusedForItsRetryLimitClosesTheDriverAndStaysUnregistered() throws Exception {
		final var config = config();
		config.val("load-op-retry", true);
		config.val("load-op-retryLimit", -1);
		final var step = new Step(config, factory(config));
		try {
			final var failure = assertThrows(IllegalStateException.class, step::runInit);

			assertInstanceOf(IllegalConfigurationException.class, failure.getCause());
			assertTrue(driver.isClosed());
			assertFalse(DaemonBase.isRegistered(this::isContextOfThisDriver));
		} finally {
			step.close();
		}
	}

	@Test
	void aContextThatIsBuiltKeepsItsDriverOpenUntilTheStepCloses() throws Exception {
		final var config = config();
		final var step = new Step(config, factory(config));
		step.runInit();

		assertFalse(driver.isClosed());
		step.close();
		assertTrue(driver.isClosed());
	}
}
