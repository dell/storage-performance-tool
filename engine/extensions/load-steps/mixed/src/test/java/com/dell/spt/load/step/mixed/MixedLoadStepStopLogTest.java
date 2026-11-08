package com.dell.spt.load.step.mixed;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMock;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * A mixed step's normal stop is not an error: whatever ends it, the generator logs no ERROR. The
 * driver refuses an operation after admission closes the way the coop drivers do, with an
 * {@link EOFException}.
 */
class MixedLoadStepStopLogTest {

	/**
	 * A step ID unique across test runs. Logging configured before a test sets home_dir keeps the
	 * literal ${ctx:home_dir} in some file paths, so a step's files land under a directory shared by
	 * every run, by step ID, and a node source left there by an earlier run with the same ID fails
	 * the step's artifact aggregation (as in PipelineLoadStepHandoffTest).
	 */
	private static String newStepId(final String stop) {
		return "mixed-stop-" + stop + "-" + UUID.randomUUID();
	}

	@TempDir
	Path home;

	private final EofAtCloseDriverFactory drivers = new EofAtCloseDriverFactory();
	private ErrorLogCapture log;

	@BeforeEach
	void setUp() {
		ThreadContext.put("home_dir", home.toString());
		log = new ErrorLogCapture();
	}

	@AfterEach
	void tearDown() {
		log.close();
		ThreadContext.remove("home_dir");
	}

	@Test
	void aStepStoppedByItsCountLimitLogsNoGeneratorError() throws Exception {
		final String stepId = newStepId("count");
		final Config config = stepConfig(stepId);
		config.val("load-op-limit-count", 20_000L);

		final MetricsManagerImpl metrics = run(config);

		assertTrue(operations(metrics, stepId) > 0, "the step ran operations");
		assertNoGeneratorError();
	}

	@Test
	void aStepStoppedByItsDurationLogsNoGeneratorError() throws Exception {
		final String stepId = newStepId("duration");
		final Config config = stepConfig(stepId);
		config.val("load-step-limit-time", "2s");

		final MetricsManagerImpl metrics = run(config);

		assertTrue(operations(metrics, stepId) > 0, "the step ran operations");
		assertNoGeneratorError();
	}

	@Test
	void aStepStoppedByAnInterruptLogsNoGeneratorError() throws Exception {
		final Config config = stepConfig(newStepId("interrupt"));
		final MetricsManagerImpl metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		final String home = this.home.toString();
		final Thread runner = new Thread(() -> {
			ThreadContext.put("home_dir", home);
			try {
				new MixedLoadStepClient(config, extensions(), null, metrics).run();
			} catch (final Throwable expected) {
				// An interrupted run rethrows the interrupt.
			}
		}, "mixed-stop-log-runner");
		runner.start();
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
		while (drivers.accepted.sum() < 1_000) {
			assertFalse(System.nanoTime() > deadline, "the step started operations");
			Thread.sleep(5);
		}
		runner.interrupt();
		runner.join(TimeUnit.SECONDS.toMillis(30));

		assertFalse(runner.isAlive(), "the interrupted step ended");
		assertNoGeneratorError();
	}

	private MetricsManagerImpl run(final Config config) {
		final MetricsManagerImpl metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		new MixedLoadStepClient(config, extensions(), null, metrics).run();
		return metrics;
	}

	private List<Extension> extensions() {
		return List.of(new MixedLoadStepExtension(), drivers);
	}

	private static long operations(final MetricsManagerImpl metrics, final String stepId) {
		return metrics.getTerminalSteps().stream()
						.filter(entry -> stepId.equals(entry.stepId))
						.mapToLong(entry -> entry.successCount)
						.sum();
	}

	private void assertNoGeneratorError() throws InterruptedException {
		assertTrue(drivers.refused.sum() > 0, "the driver refused an operation with EOF at the stop");
		final List<String> errors = log.errorsContaining("MixedLoadGenerator");
		assertTrue(errors.isEmpty(), errors.toString());
	}

	private Config stepConfig(final String stepId) throws Exception {
		final Config config = defaultConfig();
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-step-limit-time", "30s");
		config.val("load-op-limit-count", 0L);
		config.val("load-op-weights-get", 1);
		config.val("load-op-weights-put", 2);
		config.val("load-op-weights-delete", 1);
		config.val("load-op-weights-stat", 1);
		config.val("storage-driver-type", EofAtCloseDriverFactory.TYPE);
		config.val("storage-driver-limit-concurrency", 4);
		config.val("storage-targetBucket-provision", false);
		config.val("item-data-size", "1KB");
		config.val("item-output-path", "/bucket");
		final Path seeds = home.resolve("seeds.csv");
		final List<String> seedItems = new ArrayList<>();
		for (int i = 0; i < 20; i++) {
			seedItems.add(new DataItemImpl("/bucket/seed-" + i, 0, 1024).toString());
		}
		Files.write(seeds, seedItems);
		config.val("item-input-file", seeds.toString());
		return config;
	}

	/** The shipped defaults of the engine and of this extension, as the engine merges them. */
	private static Config defaultConfig() throws Exception {
		final Config core = ConfigUtil.loadConfig(
						resource("/config/defaults.yaml"), ConfigFormat.YAML, InitialConfigSchemaProvider.provider().schema());
		final Config mixed = ConfigUtil.loadConfig(
						resource("/config/defaults-load-op-mixed.yaml"),
						ConfigFormat.YAML,
						new MixedLoadStepExtension().schemaProvider().schema());
		return ConfigUtil.merge(core.pathSep(), List.of(mixed, core));
	}

	private static String resource(final String name) throws IOException {
		try (InputStream stream = MixedLoadStepStopLogTest.class.getResourceAsStream(name)) {
			return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	/**
	 * The dummy driver, which after its admission closes throws {@link EOFException} from put. Its
	 * admission close waits up to a second for that refusal, so a step test always reaches it.
	 */
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	static final class EofAtCloseDriverFactory implements StorageDriverFactory {

		static final String TYPE = "eof-at-close-mock";

		private static final long REFUSAL_WAIT_NANOS = TimeUnit.SECONDS.toNanos(1);

		final LongAdder accepted = new LongAdder();
		final LongAdder refused = new LongAdder();

		@Override
		public String id() {
			return TYPE;
		}

		@Override
		public SchemaProvider schemaProvider() {
			return null;
		}

		@Override
		public void install(final Path dstPath) {}

		@Override
		public Config defaults(final Path appHomePath) {
			return null;
		}

		@Override
		public StorageDriver create(
						final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
						final int batchSize) {
			final DummyStorageDriverMock driver = spy(new DummyStorageDriverMock(storageConfig));
			final boolean[] closed = new boolean[1];
			doAnswer(call -> {
				synchronized (closed) {
					closed[0] = true;
				}
				final Object result = call.callRealMethod();
				final long deadline = System.nanoTime() + REFUSAL_WAIT_NANOS;
				while (refused.sum() == 0 && System.nanoTime() < deadline) {
					Thread.sleep(1);
				}
				return result;
			}).when(driver).closeAdmission();
			doAnswer(call -> {
				synchronized (closed) {
					if (closed[0]) {
						refused.increment();
						throw new EOFException();
					}
				}
				final boolean result = (boolean) call.callRealMethod();
				if (result) {
					accepted.increment();
				}
				return result;
			}).when(driver).put(any(Operation.class));
			return driver;
		}
	}
}
