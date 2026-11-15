package com.dell.spt.load.step.mixed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import com.dell.spt.base.config.ConfigFormat;
import com.dell.spt.base.config.ConfigUtil;
import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.CompositeResults;
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
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
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
	/** The start of the INFO line a count-limited step logs on a driver that does not declare. */
	private static final String UNDECLARED_INFO = "does not declare how it publishes composite results";

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
		assertEquals(UncountedFinalResultOutput.class, drivers.resultOutput.get().getClass(), "the result output");
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

	/**
	 * The step above on a driver that does not declare its composite results: the generator is not
	 * capped, so it dispatches past the count, and the step ends when it has counted its count, as
	 * before this change, though one result is not counted. A capped generator would stop at the
	 * count, one short of it, and with the step's own result output nothing would see the
	 * uncounted result, so the step would wait for its time limit. The step logs once that the
	 * driver does not declare.
	 */
	@Test
	void anUndeclaredDriverWithAnUncountedResultRunsPastItsCountAsBefore() throws Exception {
		final String stepId = newStepId("undeclared-uncounted");
		final Config config = stepConfig(stepId);
		config.val("load-op-limit-count", 50L);
		drivers.interruptFirstResult.set(true);
		drivers.undeclared.set(true);

		final long elapsedMillis;
		final List<String> infos;
		try (ErrorLogCapture log = new ErrorLogCapture(Loggers.MSG)) {
			elapsedMillis = run(config);
			infos = log.messagesContaining(UNDECLARED_INFO, Level.INFO);
		}

		assertEquals(1, drivers.interrupted.sum(), "results published as INTERRUPTED");
		assertTrue(
						elapsedMillis < PROMPT_END_MILLIS,
						"the step ended on its count, without waiting for its time limit, after " + elapsedMillis + " ms");
		assertTrue(counted(stepId) >= 50, "the step counted its count, " + counted(stepId));
		assertTrue(drivers.accepted.sum() > 50, "the driver accepted past the count, " + drivers.accepted.sum());
		assertTrue(
						drivers.resultOutput.get() != null
										&& !(drivers.resultOutput.get() instanceof UncountedFinalResultOutput),
						"the step installed its result output without the capped wrapper: " + drivers.resultOutput.get());
		assertEquals(1, infos.size(), "the step's INFO lines about the undeclared driver: " + infos);
		assertTrue(infos.get(0).contains("count limit 50"), infos.get(0));
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
		assertTrue(
						drivers.resultOutput.get() instanceof UncountedFinalResultOutput == false,
						"a step without a count limit keeps its result output");
	}

	/** {@code Long.MAX_VALUE} is no count limit, as 0 is. */
	@Test
	void aStepWithAMaximalCountRunsToItsTimeLimit() throws Exception {
		final String stepId = newStepId("max-count");
		final Config config = stepConfig(stepId);
		config.val("load-step-limit-time", "1s");
		config.val("load-op-limit-count", Long.MAX_VALUE);

		final long elapsedMillis = run(config);

		assertTrue(elapsedMillis >= 900, "the step ran to its time limit, " + elapsedMillis + " ms");
		assertEquals(drivers.accepted.sum(), counted(stepId), "every accepted operation was counted");
		assertTrue(
						drivers.resultOutput.get() instanceof UncountedFinalResultOutput == false,
						"a step without a count limit keeps its result output");
	}

	/**
	 * The dummy driver publishes one result for a composite, which the step does not count, as
	 * before: a count-limited step of flat reads and composite writes counts its reads only, and
	 * ends on its count.
	 */
	@Test
	void aMixOfFlatReadsAndCompositeWritesOnTheDummyDriverEndsOnItsCount() throws Exception {
		final String stepId = newStepId("dummy-composite-mix");
		final Config config = stepConfig(stepId);
		config.val("load-op-limit-count", 50L);
		config.val("item-data-ranges-threshold", "512B");
		config.val("load-op-weights-get", 50);
		config.val("load-op-weights-put", 50);
		config.val("load-op-weights-delete", 0);
		config.val("load-op-weights-stat", 0);
		config.val(
						"item-input-file",
						Files.write(
										home.resolve("tiny-seeds-" + stepId + ".csv"),
										List.of(new DataItemImpl("/bucket/tiny", 0, 64).toString()))
										.toString());

		final long elapsedMillis = run(config);

		assertEquals(50, drivers.accepted.sum(), "operations the driver accepted");
		final long reads = drivers.accepted(OpType.READ);
		assertTrue(reads > 0 && reads < 50, "the step ran reads and writes, " + reads + " reads");
		assertEquals(reads, metrics.counted(stepId, OpType.READ), "reads the step counted");
		assertEquals(0, metrics.counted(stepId, OpType.CREATE), "composite writes the step counted");
		assertTrue(elapsedMillis < PROMPT_END_MILLIS, "the step ended on its count, after " + elapsedMillis + " ms");
	}

	@Test
	void aCompositeWriteOnTheDummyDriverEndsAfterItsOnlyResult() throws Exception {
		final String stepId = newStepId("dummy-composite");
		final Config config = multipartStepConfig(stepId, 1);
		config.val("storage-driver-type", CountingDriverFactory.TYPE);

		final long elapsedMillis = run(config);

		assertEquals(1, drivers.accepted.sum(), "operations the driver accepted");
		assertEquals(0, counted(stepId), "composite writes the step counted");
		assertTrue(elapsedMillis < PROMPT_END_MILLIS, "the step ended on its count, after " + elapsedMillis + " ms");
	}

	/** A STAT above the ranges threshold is one composite result, which the step does not count. */
	@Test
	void aStepOfStatsAboveTheThresholdEndsWithoutCountingThemOnTheDummyDriver() throws Exception {
		final String stepId = newStepId("dummy-split-stat");
		final Config config = statStepConfig(stepId);
		config.val("storage-driver-type", CountingDriverFactory.TYPE);

		final long elapsedMillis = run(config);

		assertEquals(40, drivers.accepted.sum(), "operations the driver accepted");
		assertTrue(drivers.accepted(OpType.STAT) > 0, "the step ran STATs");
		assertEquals(0, counted(stepId), "STATs and composite writes the step counted");
		assertTrue(elapsedMillis < PROMPT_END_MILLIS, "the step ended on its count, after " + elapsedMillis + " ms");
	}

	@Test
	void aStepOfStatsAboveTheThresholdEndsWithoutCountingThemOnTheNioDriver() throws Exception {
		final String stepId = newStepId("nio-split-stat");
		final Config config = statStepConfig(stepId);

		final long elapsedMillis = run(config);

		assertEquals(0, metrics.counted(stepId, OpType.STAT), "STATs the step counted");
		assertTrue(metrics.counted(stepId, OpType.CREATE) < 40, "the step ran STATs");
		assertTrue(elapsedMillis < PROMPT_END_MILLIS, "the step ended on its count, after " + elapsedMillis + " ms");
	}

	/** 40 operations, nearly all STATs of the 1 KB seed items, above a 512 B ranges threshold. */
	private Config statStepConfig(final String stepId) throws Exception {
		final Config config = multipartStepConfig(stepId, 40);
		// A mixed step needs two weights: a few creates of new items run with the STATs.
		config.val("load-op-weights-put", 1);
		config.val("load-op-weights-stat", 100);
		return config;
	}

	@Test
	void aMultipartUploadWhosePartsAreEnqueuedLateIsCounted() throws Exception {
		final String stepId = newStepId("multipart-late-parts");
		final Config config = multipartStepConfig(stepId, 1);
		nioDrivers.pauseFirstCompletion.set(true);

		final long elapsedMillis = run(config);

		assertEquals(1, nioDrivers.paused.sum(), "completions paused after their result was published");
		// The upload was the step's one operation, and its tracker held nothing at the pause: the
		// pause came after its initiation's terminal result and before its parts were queued.
		assertEquals(Boolean.FALSE, nioDrivers.outstandingAtPause.get(), "the driver held an operation at the pause");
		assertEquals(1, counted(stepId), "the step counted the upload");
		assertTrue(elapsedMillis < PROMPT_END_MILLIS, "the step ended on its count, after " + elapsedMillis + " ms");
	}

	/**
	 * A driver that does not declare how it publishes composite results, such as a decorator that
	 * does not forward the declaration, with the late-parts upload above. The step keeps its earlier
	 * route: the step's own result output, with no cap. So an upload's initiation is never taken for
	 * its final result, and the step does not end before it counts an upload, as it did when the
	 * undeclared default meant one result per composite. As before this change, the step dispatches
	 * past its count, counts more than it, and waits for the uploads still running when it stops,
	 * up to its {@code load-op-wait-limit} (on 518a0b4c, too, it ends after that wait, about 30 s at
	 * the default), but not for its time limit.
	 */
	@Test
	void aMultipartUploadOnAnUndeclaredDriverIsCountedAsBefore() throws Exception {
		final String stepId = newStepId("undeclared-late-parts");
		final Config config = multipartStepConfig(stepId, 1);
		// The step stops with uploads still running, and waits for them up to this many seconds,
		// 30 by default, as before this change.
		config.val("load-op-wait-limit", 3);
		nioDrivers.undeclaredDecorator.set(true);
		nioDrivers.pauseFirstCompletion.set(true);

		final long elapsedMillis = run(config);

		assertTrue(counted(stepId) >= 1, "the step counted an upload, " + counted(stepId));
		assertTrue(
						nioDrivers.resultOutput.get() != null
										&& !(nioDrivers.resultOutput.get() instanceof UncountedFinalResultOutput),
						"the step installed its result output without the capped wrapper: " + nioDrivers.resultOutput.get());
		assertEquals(1, nioDrivers.paused.sum(), "completions paused after their result was published");
		assertTrue(
						elapsedMillis < PROMPT_END_MILLIS,
						"the step ended after its wait for running uploads, after " + elapsedMillis + " ms");
	}

	@Test
	void aDeclaredDriverInstallsTheCappedResultOutput() throws Exception {
		final String stepId = newStepId("declared-output");
		final Config config = multipartStepConfig(stepId, 20);
		nioDrivers.recordResultOutput.set(true);

		run(config);

		assertEquals(UncountedFinalResultOutput.class, nioDrivers.resultOutput.get().getClass(), "the result output");
		assertEquals(20, counted(stepId), "uploads the step counted");
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
	 * A cooperative driver that leaves an aborted upload's finalization a success, as the file system
	 * driver does. Once the cooperative base ends such an upload without sending its unsent parts,
	 * the step ends on its count; how the upload is counted is still to be decided, and this test
	 * expects it counted once, as a failure.
	 */
	@Test
	@Disabled("an aborted fs-style upload never ends on the coop base: .scratch/coop-aborted-upload-left-success/issues/01-an-aborted-upload-published-as-success-sends-its-unsent-parts.md")
	void aStepWhoseUploadsAbortEndsOnItsCount() throws Exception {
		final String stepId = newStepId("multipart-abort");
		final Config config = multipartStepConfig(stepId, 2);
		// One part in flight per upload: the first part runs out of retries, and the upload is
		// aborted before its second part is sent, so the abort's result is unfinished.
		config.val("storage-driver-limit-multipart-parts", 1);
		nioDrivers.failParts.set(true);

		final long elapsedMillis = run(config);

		// Each upload's first part: one attempt and three retries (the step's one STAT in 10,001 runs
		// no part).
		final long failedParts = nioDrivers.failedParts.sum();
		assertTrue(failedParts > 0 && failedParts % 4 == 0, "failed part attempts: " + failedParts);
		assertEquals(0, nioDrivers.laterParts.sum(), "second parts sent after their upload was aborted");
		assertEquals(failedParts / 4, metrics.counted(stepId, OpType.CREATE), "aborted uploads the step counted");
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
		final Map<OpType, LongAdder> acceptedByType = new ConcurrentHashMap<>();
		final AtomicReference<Object> resultOutput = new AtomicReference<>();
		final LongAdder interrupted = new LongAdder();
		final AtomicBoolean interruptFirstResult = new AtomicBoolean();
		/** Whether the driver does not declare its composite results, as a decorator might not. */
		final AtomicBoolean undeclared = new AtomicBoolean();

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
			if (undeclared.get()) {
				doReturn(CompositeResults.UNDECLARED).when(driver).compositeResults();
			}
			doAnswer(call -> {
				final boolean result = (boolean) call.callRealMethod();
				if (result) {
					accepted.increment();
					acceptedByType
									.computeIfAbsent(((Operation) call.getArgument(0)).type(), type -> new LongAdder())
									.increment();
				}
				return result;
			}).when(driver).put(any(Operation.class));
			// The driver publishes each result to the output this sets, so it is set to one that
			// passes the results on through InterruptingOutput.
			doAnswer(call -> {
				final Field resultOutput = DummyStorageDriverMock.class.getDeclaredField("opResultOut");
				resultOutput.setAccessible(true);
				resultOutput.set(call.getMock(), new InterruptingOutput(call.getArgument(0)));
				this.resultOutput.set(call.getArgument(0));
				return null;
			}).when(driver).operationResultOutput(any());
			return driver;
		}

		/** The operations of {@code type} the driver accepted. */
		long accepted(final OpType type) {
			final LongAdder count = acceptedByType.get(type);
			return count == null ? 0 : count.sum();
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
		/** Whether the driver's lifecycle tracker held an operation when the pause ran. */
		final AtomicReference<Boolean> outstandingAtPause = new AtomicReference<>();
		final AtomicBoolean failParts = new AtomicBoolean();
		/** Whether the driver is wrapped in a decorator that does not declare its composite results. */
		final AtomicBoolean undeclaredDecorator = new AtomicBoolean();
		/** Whether the driver is wrapped in a decorator that forwards everything, to record its output. */
		final AtomicBoolean recordResultOutput = new AtomicBoolean();
		/** The result output the step installed through a decorator. */
		final AtomicReference<Object> resultOutput = new AtomicReference<>();
		final LongAdder failedParts = new LongAdder();
		/** Parts after the first that the driver sent. */
		final LongAdder laterParts = new LongAdder();

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
			final ProbeDriver driver = new ProbeDriver(stepId, dataInput, storageConfig, verifyFlag, batchSize);
			if (pauseFirstCompletion.get()) {
				// handleCompleted counts the completion between publishing its result and enqueueing
				// a multipart upload's parts, so the count's increment is where the pause goes.
				try {
					final Field completedOpCount = CoopStorageDriverBase.class.getDeclaredField("completedOpCount");
					completedOpCount.setAccessible(true);
					completedOpCount.set(driver, new PausingCount(driver));
				} catch (final ReflectiveOperationException e) {
					throw new IllegalStateException(e);
				}
			}
			if (undeclaredDecorator.get() || recordResultOutput.get()) {
				return decorator(driver, undeclaredDecorator.get());
			}
			return driver;
		}

		/**
		 * An interface decorator, as a driver author might write one before drivers could declare
		 * their composite results: it forwards every method to {@code driver}, and records the result
		 * output the step installs. When {@code undeclared}, it does not forward the declaration, so
		 * it has the interface's default.
		 */
		private StorageDriver decorator(final StorageDriver driver, final boolean undeclared) {
			return (StorageDriver) Proxy.newProxyInstance(
							StorageDriver.class.getClassLoader(), new Class<?>[]{StorageDriver.class
							},
							(proxy, method, args) -> {
								// completesCompositesInParts is the declaration's name on 55abe355, matched so that
								// the same test runs against that revision's sources for the fail check.
								if (undeclared
												&& ("compositeResults".equals(method.getName())
																|| "completesCompositesInParts".equals(method.getName()))) {
									return InvocationHandler.invokeDefault(proxy, method, args);
								}
								if ("operationResultOutput".equals(method.getName())) {
									resultOutput.set(args[0]);
								}
								try {
									return method.invoke(driver, args);
								} catch (final InvocationTargetException e) {
									throw e.getCause();
								}
							});
		}

		/** The NIO mock, whose parts fail with {@code FAIL_IO} when asked to. */
		@SuppressWarnings({"rawtypes", "unchecked"
		})
		private final class ProbeDriver extends NioStorageDriverMock {

			ProbeDriver(
							final String stepId, final DataInput dataInput, final Config storageConfig, final boolean verifyFlag,
							final int batchSize) {
				super(stepId, dataInput, storageConfig, verifyFlag, batchSize);
			}

			@Override
			protected void invokeNio(final Operation op) {
				super.invokeNio(op);
				if (op instanceof PartialOperation<?> part && part.partNumber() > 0) {
					laterParts.increment();
				}
				if (failParts.get() && op instanceof PartialOperation) {
					failedParts.increment();
					op.status(Operation.Status.FAIL_IO);
				}
			}
		}

		/** Pauses its first increment, when asked to. */
		private final class PausingCount extends LongAdder {

			private static final long serialVersionUID = 1L;

			private final transient StorageDriver<?, ?> driver;

			PausingCount(final StorageDriver<?, ?> driver) {
				this.driver = driver;
			}

			@Override
			public void increment() {
				if (pauseFirstCompletion.compareAndSet(true, false)) {
					final var lifecycle = driver.operationLifecycle();
					outstandingAtPause.set(lifecycle.isEnabled() ? lifecycle.hasOutstandingOperations() : null);
					paused.increment();
					LockSupport.parkNanos(PAUSE_NANOS);
				}
				super.increment();
			}
		}
	}
}
