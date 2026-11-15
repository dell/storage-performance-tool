package com.dell.spt.load.step.mixed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.StorageDriverFactory;
import com.dell.spt.base.storage.driver.mock.DummyStorageDriverMock;
import com.dell.spt.storage.driver.coop.CoopStorageDriverBase;
import com.dell.spt.storage.driver.coop.nio.mock.NioStorageDriverMock;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A count-limited mixed step runs exactly its count, on a driver that completes each operation as
 * it is handed over, at a concurrency of 16, and ends as soon as those operations have ended, also
 * when one of them produced no counted result. On the NIO mock, a cooperative driver, multipart
 * uploads are counted the same way, also when their parts are enqueued late. Without a count limit
 * the step runs to its time limit as before.
 */
@Timeout(120)
class MixedLoadStepCountLimitTest {

	/** Far beyond what any of these steps needs, so a step that reaches it has waited for it. */
	private static final String TIME_LIMIT = "60s";
	private static final long PROMPT_END_MILLIS = TimeUnit.SECONDS.toMillis(20);

	@TempDir
	Path home;

	private final CountingDriverFactory drivers = new CountingDriverFactory();
	private final NioDriverFactory nioDrivers = new NioDriverFactory();
	private FinalCountsMetricsManager metrics;

	@BeforeEach
	void setUp() {
		ThreadContext.put("home_dir", home.toString());
	}

	@AfterEach
	void tearDown() {
		ThreadContext.remove("home_dir");
	}

	@Test
	void aCountOfOneRunsOneOperation() throws Exception {
		assertTheStepRunsExactly(1);
	}

	@Test
	void aCountOfTwentyRunsTwentyOperations() throws Exception {
		assertTheStepRunsExactly(20);
	}

	@Test
	void aCountOfFourThousandRunsFourThousandOperations() throws Exception {
		assertTheStepRunsExactly(4_000);
	}

	private void assertTheStepRunsExactly(final long countLimit) throws Exception {
		final String stepId = newStepId("exact");
		final Config config = stepConfig(stepId);
		config.val("load-op-limit-count", countLimit);

		final long elapsedMillis = run(config);

		assertEquals(countLimit, drivers.accepted.sum(), "operations the driver accepted");
		assertEquals(countLimit, counted(stepId), "operations the step counted");
		assertTrue(elapsedMillis < PROMPT_END_MILLIS, "the step ended on its count, after " + elapsedMillis + " ms");
	}

	@Test
	void aStepEndsWhenAnOperationEndsWithNoCountedResult() throws Exception {
		final String stepId = newStepId("uncounted");
		final Config config = stepConfig(stepId);
		config.val("load-op-limit-count", 50L);
		drivers.interruptFirstResult.set(true);

		final long elapsedMillis = run(config);

		assertEquals(1, drivers.interrupted.sum(), "results published as INTERRUPTED");
		assertEquals(50, drivers.accepted.sum(), "operations the driver accepted");
		assertEquals(49, counted(stepId), "operations the step counted");
		assertTrue(
						elapsedMillis < PROMPT_END_MILLIS,
						"the step ended without waiting for its time limit, after " + elapsedMillis + " ms");
	}

	@Test
	void aStepWithNoCountLimitRunsToItsTimeLimit() throws Exception {
		final String stepId = newStepId("uncapped");
		final Config config = stepConfig(stepId);
		config.val("load-step-limit-time", "1s");

		final long elapsedMillis = run(config);

		assertTrue(elapsedMillis >= 900, "the step ran to its time limit, " + elapsedMillis + " ms");
		assertTrue(drivers.accepted.sum() > 0, "the step ran operations");
		assertEquals(drivers.accepted.sum(), counted(stepId), "every accepted operation was counted");
	}

	@Test
	void aMultipartUploadWhosePartsAreEnqueuedLateIsCounted() throws Exception {
		final String stepId = newStepId("multipart-late-parts");
		final Config config = multipartStepConfig(stepId, 1);
		nioDrivers.pauseFirstCompletion.set(true);

		final long elapsedMillis = run(config);

		assertEquals(1, nioDrivers.paused.sum(), "completions paused after their result was published");
		assertEquals(1, counted(stepId), "the step counted the upload");
		assertTrue(elapsedMillis < PROMPT_END_MILLIS, "the step ended on its count, after " + elapsedMillis + " ms");
	}

	@Test
	void aCountOfFiftyMultipartUploadsRunsFiftyUploads() throws Exception {
		final String stepId = newStepId("multipart-exact");
		final Config config = multipartStepConfig(stepId, 50);

		final long elapsedMillis = run(config);

		assertEquals(50, counted(stepId), "uploads the step counted");
		assertTrue(elapsedMillis < PROMPT_END_MILLIS, "the step ended on its count, after " + elapsedMillis + " ms");
	}

	/**
	 * A DELETE above the ranges threshold is sent as one composite result, which the step does not
	 * count, so the step counts fewer than its count, but it ends without waiting for its time limit.
	 */
	@Test
	void aStepWithSplitDeletesEndsWithoutCountingThem() throws Exception {
		final String stepId = newStepId("split-delete");
		final Config config = multipartStepConfig(stepId, 200);
		config.val("load-op-weights-put", 5000);
		config.val("load-op-weights-delete", 5000);

		final long elapsedMillis = run(config);

		assertEquals(0, metrics.counted(stepId, OpType.DELETE), "split DELETEs the step counted");
		final long uploads = metrics.counted(stepId, OpType.CREATE);
		assertTrue(uploads > 0 && uploads < 200, "the step ran uploads and split DELETEs, counted " + uploads);
		assertTrue(
						elapsedMillis < PROMPT_END_MILLIS,
						"the step ended without waiting for its time limit, after " + elapsedMillis + " ms");
	}

	/**
	 * A count-limited step of uploads only on the NIO mock, each 1 KB item uploaded in two parts.
	 * The STAT weight meets the mixed step's configuration check but is too small for the schedule
	 * to hold a STAT.
	 */
	private Config multipartStepConfig(final String stepId, final long countLimit) throws Exception {
		final Config config = stepConfig(stepId);
		config.val("storage-driver-type", NioDriverFactory.TYPE);
		config.val("load-op-limit-count", countLimit);
		config.val("load-op-weights-get", 0);
		config.val("load-op-weights-put", 10000);
		config.val("load-op-weights-delete", 0);
		config.val("load-op-weights-stat", 1);
		config.val("item-data-ranges-threshold", "512B");
		return config;
	}

	private long run(final Config config) {
		metrics = new FinalCountsMetricsManager();
		final long start = System.nanoTime();
		new MixedLoadStepClient(config, extensions(), null, metrics).run();
		return TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
	}

	/** Successful and failed operations of every operation type of the step. */
	private long counted(final String stepId) {
		return metrics.counted(stepId);
	}

	private List<Extension> extensions() {
		return List.of(new MixedLoadStepExtension(), drivers, nioDrivers);
	}

	/** A step ID unique across test runs, as in {@link MixedLoadStepStopLogTest}. */
	private static String newStepId(final String kind) {
		return "mixed-count-" + kind + "-" + UUID.randomUUID();
	}

	private Config stepConfig(final String stepId) throws Exception {
		final Config config = defaultConfig();
		config.val("load-step-id", stepId);
		config.val("load-step-idAutoGenerated", false);
		config.val("load-step-limit-time", TIME_LIMIT);
		config.val("load-op-limit-count", 0L);
		config.val("load-op-weights-get", 45);
		config.val("load-op-weights-put", 15);
		config.val("load-op-weights-delete", 10);
		config.val("load-op-weights-stat", 30);
		config.val("storage-driver-type", CountingDriverFactory.TYPE);
		config.val("storage-driver-limit-concurrency", 16);
		config.val("storage-targetBucket-provision", false);
		config.val("item-data-size", "1KB");
		config.val("item-output-path", "/bucket");
		final List<String> seedItems = new ArrayList<>();
		for (int i = 0; i < 200; i++) {
			seedItems.add(new DataItemImpl("/bucket/seed-" + i, 0, 1024).toString());
		}
		config.val("item-input-file", Files.write(home.resolve("seeds-" + stepId + ".csv"), seedItems).toString());
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
		try (InputStream stream = MixedLoadStepCountLimitTest.class.getResourceAsStream(name)) {
			return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
		}
	}

	/**
	 * The dummy driver, counting the operations it accepts. When asked, it publishes its first
	 * result as {@code INTERRUPTED}, which the step does not count.
	 */
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	static final class CountingDriverFactory implements StorageDriverFactory {

		static final String TYPE = "count-limit-mock";

		final LongAdder accepted = new LongAdder();
		final LongAdder interrupted = new LongAdder();
		final AtomicBoolean interruptFirstResult = new AtomicBoolean();

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
			doAnswer(call -> {
				final boolean result = (boolean) call.callRealMethod();
				if (result) {
					accepted.increment();
				}
				return result;
			}).when(driver).put(any(Operation.class));
			// The driver publishes each result to the output this sets, so it is set to one that
			// passes the results on through InterruptingOutput.
			doAnswer(call -> {
				final Field resultOutput = DummyStorageDriverMock.class.getDeclaredField("opResultOut");
				resultOutput.setAccessible(true);
				resultOutput.set(call.getMock(), new InterruptingOutput(call.getArgument(0)));
				return null;
			}).when(driver).operationResultOutput(any());
			return driver;
		}

		/** Publishes the first result it passes on as {@code INTERRUPTED}, when asked to. */
		private final class InterruptingOutput implements Output {

			private final Output output;

			InterruptingOutput(final Output output) {
				this.output = output;
			}

			@Override
			public boolean put(final Object op) {
				if (interruptFirstResult.compareAndSet(true, false)) {
					((Operation) op).status(Operation.Status.INTERRUPTED);
					interrupted.increment();
				}
				return output.put(op);
			}

			@Override
			public int put(final List ops, final int from, final int to) {
				for (int i = from; i < to; i++) {
					if (!put(ops.get(i))) {
						return i - from;
					}
				}
				return to - from;
			}

			@Override
			public int put(final List ops) {
				return put(ops, 0, ops.size());
			}

			@Override
			public Input getInput() {
				return output.getInput();
			}

			@Override
			public void close() throws Exception {
				output.close();
			}
		}
	}

	/**
	 * The shipped NIO mock. When asked, its first completion pauses after the operation's result
	 * is published and its lifecycle is terminal, and before a multipart upload's parts are
	 * enqueued, as a busy completion thread can.
	 */
	@SuppressWarnings({"rawtypes"
	})
	static final class NioDriverFactory implements StorageDriverFactory {

		static final String TYPE = "count-limit-nio-mock";
		private static final long PAUSE_NANOS = TimeUnit.MILLISECONDS.toNanos(200);

		final AtomicBoolean pauseFirstCompletion = new AtomicBoolean();
		final LongAdder paused = new LongAdder();

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
			final NioStorageDriverMock driver = new NioStorageDriverMock(stepId, dataInput, storageConfig, verifyFlag, batchSize);
			if (pauseFirstCompletion.get()) {
				// handleCompleted counts the completion between publishing its result and enqueueing
				// a multipart upload's parts, so the count's increment is where the pause goes.
				try {
					final Field completedOpCount = CoopStorageDriverBase.class.getDeclaredField("completedOpCount");
					completedOpCount.setAccessible(true);
					completedOpCount.set(driver, new PausingCount());
				} catch (final ReflectiveOperationException e) {
					throw new IllegalStateException(e);
				}
			}
			return driver;
		}

		/** Pauses its first increment, when asked to. */
		private final class PausingCount extends LongAdder {

			private static final long serialVersionUID = 1L;

			@Override
			public void increment() {
				if (pauseFirstCompletion.compareAndSet(true, false)) {
					paused.increment();
					LockSupport.parkNanos(PAUSE_NANOS);
				}
				super.increment();
			}
		}
	}
}
