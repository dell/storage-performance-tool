package com.dell.spt.storage.driver.coop.aws.s3;

import static com.dell.spt.base.Constants.KEY_STEP_ID;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.control.run.RunImpl;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.load.step.ScenarioUtil;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.storage.driver.coop.aws.s3.S3AwsMultipartTimingTest.MultipartStub;
import com.github.akurilov.commons.system.SizeInBytes;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The bytes the {@code s3-aws} driver counts, as on the Netty {@code s3} driver: a request that
 * writes an item's data counts the item's size, a multipart upload's bytes are counted by its parts
 * alone, and a request that moves no body counts none.
 */
@Timeout(value = 240, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
final class S3AwsByteCountsTest {

	private static final int PART_SIZE = 1024;
	private static final int PARTS = 3;
	private static final long OBJECT_SIZE = (long) PARTS * PART_SIZE;
	private static final int UPLOADS = 12;
	private static final Duration RESULT_TIMEOUT = Duration.ofSeconds(60);
	private static final String DATA_SEED = "7a42d9c483244167";
	/** A comma outside quotes: the timestamp cell is quoted and holds one. */
	private static final String CSV_SEPARATOR = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

	private final MultipartStub stub = new MultipartStub();

	@TempDir
	Path home;

	@AfterEach
	void stop() {
		stub.close();
	}

	@Test
	void aCompletedUploadsBytesAreCountedByItsPartsAlone() throws Exception {
		final var upload = upload("completed");

		final var parent = upload.get(upload.size() - 1);
		assertEquals(Operation.Status.SUCC, parent.status(), upload::toString);
		assertEquals(0, bytes(parent), "the parent sends no object data of its own");
		assertEquals(OBJECT_SIZE, partBytes(upload), upload::toString);
	}

	@Test
	void anAbortedUploadCountsNoParentBytes() throws Exception {
		stub.failPart = n -> true;
		stub.failureStatus = 403;

		final var upload = upload("aborted");

		final var parent = upload.get(upload.size() - 1);
		assertEquals(Operation.Status.RESP_FAIL_AUTH, parent.status(), upload::toString);
		assertEquals(1, stub.aborts.size(), stub.aborts::toString);
		assertEquals(0, bytes(parent));
	}

	@Test
	void anUploadWhoseCompletionFailsCountsNoParentBytes() throws Exception {
		stub.failCompletion = true;
		stub.failureStatus = 403;

		final var upload = upload("not-completed");

		final var parent = upload.get(upload.size() - 1);
		assertNotEquals(Operation.Status.SUCC, parent.status(), upload::toString);
		assertEquals(1, stub.aborts.size(), stub.aborts::toString);
		assertEquals(0, bytes(parent));
	}

	@ParameterizedTest
	@EnumSource(value = OpType.class, names = {"DELETE", "STAT", "NOOP"
	})
	void aRequestWithoutABodyCountsNoBytes(final OpType type) throws Exception {
		assertEquals(0, bytes(run(plainOp(type, null))));
	}

	@ParameterizedTest
	@EnumSource(value = OpType.class, names = {"CREATE", "UPDATE"
	})
	void aWholeObjectWriteCountsTheItemSize(final OpType type) throws Exception {
		assertEquals(OBJECT_SIZE, bytes(run(plainOp(type, null))));
	}

	@Test
	void aCopyCountsTheItemSize() throws Exception {
		assertEquals(OBJECT_SIZE, bytes(run(plainOp(OpType.CREATE, "/source"))));
	}

	/**
	 * A whole step of multipart uploads: its total is the bytes written, its steady total covers
	 * what its window saw of them, and each upload's trace rows put them on its parts. The window's
	 * edges can fall inside an upload, so the steady total is within one object of its count's bytes.
	 */
	@Test
	void aMultipartStepReportsTheBytesItWroteOnce() throws Exception {
		final var stepId = "s3-aws-byte-counts-" + System.nanoTime();
		final var totals = new Capture(stepId, "byte-counts-total");
		final var steadyTotals = new Capture(stepId, "byte-counts-steady");
		final var traces = new Capture(stepId, "byte-counts-trace");
		totals.attach(Loggers.METRICS_FILE_TOTAL);
		steadyTotals.attach(Loggers.METRICS_STEADY_FILE_TOTAL);
		traces.attach(Loggers.OP_TRACES);
		try {
			write(stepId);

			final var total = row(totals.await(1));
			assertEquals(UPLOADS, Long.parseLong(total.get("CountSucc")), total::toString);
			assertEquals(UPLOADS * OBJECT_SIZE, Long.parseLong(total.get("Size")), total::toString);

			final var steady = row(steadyTotals.await(1));
			final var steadyCount = Long.parseLong(steady.get("CountSucc"));
			assertTrue(steadyCount > 1, steady::toString);
			final var steadySize = Long.parseLong(steady.get("Size"));
			assertTrue(Math.abs(steadySize - steadyCount * OBJECT_SIZE) <= OBJECT_SIZE, steady::toString);

			final var transferSizes = traces.await(1).stream()
							.flatMap(String::lines)
							.map(line -> Long.parseLong(line.substring(line.lastIndexOf(',') + 1)))
							.toList();
			// Each upload has three part rows, and a parent row for its initiation and its completion.
			assertEquals(UPLOADS * (PARTS + 2), transferSizes.size(), transferSizes::toString);
			assertEquals(UPLOADS * PARTS, Collections.frequency(transferSizes, (long) PART_SIZE), transferSizes::toString);
			assertEquals(UPLOADS * 2, Collections.frequency(transferSizes, 0L), transferSizes::toString);
		} finally {
			totals.detach(Loggers.METRICS_FILE_TOTAL);
			steadyTotals.detach(Loggers.METRICS_STEADY_FILE_TOTAL);
			traces.detach(Loggers.OP_TRACES);
		}
	}

	private void write(final String stepId) throws Exception {
		final var config = S3AwsMergedConfig.create(home, stub.port(), Map.of(
						"load-step-id", stepId,
						"load-step-idAutoGenerated", false,
						"item-output-path", "/bucket"));
		final var scenario = """
						Load.config({
							"load": {"op": {"limit": {"count": %d}}},
							"output": {"metrics": {
								"trace": {"persist": true},
								"steady": {"min": {"turnovers": 0, "duration": "0s"}}
							}},
							"storage": {"driver": {"limit": {"concurrency": 1}}},
							"item": {"data": {"size": "%dKB", "ranges": {"threshold": "%dKB"}}}
						}).run();
						""".formatted(UPLOADS, PARTS, PART_SIZE / 1024);
		final var classLoader = Thread.currentThread().getContextClassLoader();
		final List<Extension> extensions = Extension.load(classLoader);
		try (final var metrics = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR)) {
			final var engine = ScenarioUtil.scriptEngineByDefault(classLoader);
			assertNotNull(engine, "the default JavaScript engine is available");
			ScenarioUtil.configure(engine, extensions, config, metrics);
			new RunImpl("s3-aws byte counts", scenario, engine, 1L).run();
		}
	}

	/** The named cells of a metrics file's only data row, under its header. */
	private static Map<String, String> row(final List<String> messages) {
		final var lines = messages.stream().flatMap(String::lines).toList();
		assertEquals(2, lines.size(), lines::toString);
		final var names = lines.get(0).split(CSV_SEPARATOR, -1);
		final var values = lines.get(1).split(CSV_SEPARATOR, -1);
		assertEquals(names.length, values.length, lines::toString);
		final var cells = new LinkedHashMap<String, String>();
		for (var i = 0; i < names.length; i++) {
			cells.put(names[i], values[i]);
		}
		return cells;
	}

	private static long bytes(final Operation<?> result) {
		return ((DataOperation<?>) result).countBytesDone();
	}

	private static long partBytes(final List<Operation<DataItem>> upload) {
		return upload.stream()
						.filter(result -> result instanceof PartialDataOperation<?>)
						.mapToLong(S3AwsByteCountsTest::bytes)
						.sum();
	}

	private static DataItem item(final String name) throws Exception {
		final DataItem item = new DataItemImpl(name, 0, OBJECT_SIZE);
		item.dataInput(DataInput.instance(null, DATA_SEED, new SizeInBytes("4MB"), 1, false));
		return item;
	}

	private static Operation<DataItem> plainOp(final OpType type, final String sourcePath) throws Exception {
		return new DataOperationImpl<>(
						0, type, item("plain"), sourcePath, "/bucket", S3AwsDriverHarness.CREDENTIAL, null, 0);
	}

	private Operation<DataItem> run(final Operation<DataItem> op) throws Exception {
		try (final var started = S3AwsDriverHarness.Started.start(driver(), op)) {
			final var result = started.await(RESULT_TIMEOUT);
			assertEquals(Operation.Status.SUCC, result.status(), result::toString);
			return result;
		}
	}

	/** Every result of one upload through a started driver, in order, up to the parent's last. */
	private List<Operation<DataItem>> upload(final String name) throws Exception {
		final Operation<DataItem> op = new CompositeDataOperationImpl<>(
						0, OpType.CREATE, item(name), null, "/bucket", S3AwsDriverHarness.CREDENTIAL, null, 0, PART_SIZE);
		try (final var started = S3AwsDriverHarness.Started.start(driver(), op)) {
			final List<Operation<DataItem>> upload = new ArrayList<>();
			while (true) {
				final var result = started.await(RESULT_TIMEOUT);
				upload.add(result);
				if (result instanceof CompositeDataOperation<?> composite
								&& (composite.allSubOperationsDone() || result.status() != Operation.Status.SUCC)) {
					return upload;
				}
			}
		}
	}

	@SuppressWarnings("unchecked")
	private S3AwsStorageDriver<DataItem, Operation<DataItem>> driver() throws Exception {
		final var storage = S3AwsMergedConfig.create(home, stub.port(), Map.of()).configVal("storage");
		return (S3AwsStorageDriver<DataItem, Operation<DataItem>>) new S3AwsStorageDriverFactory<DataItem, Operation<DataItem>>()
						.create("s3-aws-byte-counts", DataInput.instance(null, DATA_SEED, new SizeInBytes("64KB"), 16, false),
										storage, false, 16);
	}

	/** The messages one step sends to a logger while attached. */
	private static final class Capture extends AbstractAppender {

		private final String stepId;
		private final List<String> messages = Collections.synchronizedList(new ArrayList<>());

		Capture(final String stepId, final String name) {
			super(name, null, null, true, Property.EMPTY_ARRAY);
			this.stepId = stepId;
		}

		void attach(final Logger logger) {
			start();
			LoggerContext.getContext(false).getLogger(logger.getName()).addAppender(this);
		}

		void detach(final Logger logger) {
			LoggerContext.getContext(false).getLogger(logger.getName()).removeAppender(this);
			stop();
		}

		@Override
		public void append(final LogEvent event) {
			if (stepId.equals(event.getContextData().getValue(KEY_STEP_ID))) {
				messages.add(event.getMessage().getFormattedMessage());
			}
		}

		List<String> await(final int count) throws InterruptedException {
			for (var attempt = 0; attempt < 250; attempt++) {
				LogUtil.flushAll();
				synchronized (messages) {
					if (messages.size() >= count) {
						return List.copyOf(messages);
					}
				}
				Thread.sleep(20);
			}
			throw new AssertionError("expected " + count + " messages from step " + stepId + ", got " + messages);
		}
	}
}
