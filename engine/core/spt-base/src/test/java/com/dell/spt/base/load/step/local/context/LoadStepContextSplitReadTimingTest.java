package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.composite.data.SplitReadMarks;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.metrics.bucket.BucketMeters;
import com.dell.spt.base.metrics.bucket.BucketRecorder;
import com.dell.spt.base.metrics.bucket.BucketTerminalRecord;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A split read's timing columns hold one sample per range request: its finished parent sends no
 * request, so it is counted and adds no duration or latency sample. Multipart uploads and whole-object
 * operations keep theirs. Each case runs through both {@code put} forms, on the real meters.
 */
final class LoadStepContextSplitReadTimingTest {

	private static final int PARTS = 4;
	private static final int OBJECTS = 3;
	private static final long PART_SIZE = 1024;
	private static final String BUCKET = "bucket";
	/** Long enough for two clock reads, in microseconds, to differ. */
	private static final long PAUSE_NANOS = 5_000;

	private final Config config = TestConfigBuilder.config();
	private final MetricsContext<AllMetricsSnapshot> metrics = metricsContext();
	private final BucketMeters bucketMeters = new BucketMeters(OpType.READ, List.of(BUCKET));
	private final List<Operation<DataItem>> traced = new ArrayList<>();
	private final Samples ranges = new Samples();

	@AfterEach
	void close() throws Exception {
		metrics.close();
	}

	@ParameterizedTest(name = "batch={0}")
	@ValueSource(booleans = {false, true
	})
	void aSplitReadsTimingsAreOnePerRange(final boolean batch) {
		final var context = context();
		final List<Long> parentDurations = new ArrayList<>();
		for (var object = 0; object < OBJECTS; object++) {
			final var read = new Published(OpType.READ, "split-" + object).starts();
			for (var part = 0; part < PARTS; part++) {
				read.partSucceeds(part);
			}
			read.ends(Status.SUCC);
			parentDurations.add(read.last().duration());
			assertTrue(read.last().latency() > 0, "the parent has a latency it must not add");
			put(context, batch, read);
		}

		final var snapshot = metrics.terminalSnapshot();
		assertEquals(OBJECTS, snapshot.successSnapshot().count());
		assertEquals(OBJECTS * PARTS * PART_SIZE, snapshot.byteSnapshot().count());
		assertEquals(OBJECTS * PARTS, ranges.count);
		ranges.assertHeldBy(snapshot.durationSnapshot(), snapshot.latencySnapshot());
		assertEquals(OBJECTS * PARTS, snapshot.ttfbSnapshot().count(), "every range has a first byte");

		final var bucket = bucketRecord();
		assertEquals(OBJECTS, bucket.succeeded());
		ranges.assertHeldBy(bucket.duration(), bucket.latency());

		// The whole read's time stays on its result, for the operation trace.
		final var tracedDurations = traced.stream().map(Operation::duration).toList();
		assertEquals(parentDurations, tracedDurations);
	}

	@ParameterizedTest(name = "batch={0}")
	@ValueSource(booleans = {false, true
	})
	void aFailedRangeAttemptAddsNoSampleAndItsRetryAddsOne(final boolean batch) {
		final var read = new Published(OpType.READ, "retried").starts().partFails(1);
		for (var part = 0; part < PARTS; part++) {
			read.partSucceeds(part);
		}
		read.ends(Status.SUCC);

		put(context(), batch, read);

		final var snapshot = metrics.terminalSnapshot();
		assertEquals(1, snapshot.successSnapshot().count());
		assertEquals(PARTS, ranges.count);
		ranges.assertHeldBy(snapshot.durationSnapshot(), snapshot.latencySnapshot());
	}

	@ParameterizedTest(name = "batch={0}")
	@ValueSource(booleans = {false, true
	})
	void aFailedSplitReadAddsNoParentSample(final boolean batch) {
		final var read = new Published(OpType.READ, "failed").starts().partRunsOut(0);
		for (var part = 1; part < PARTS; part++) {
			read.partSucceeds(part);
		}
		read.ends(Status.RESP_FAIL_SVC);
		assertTrue(read.last().duration() > 0, "the failed parent has a time it must not add");

		put(context(), batch, read);

		final var snapshot = metrics.terminalSnapshot();
		assertEquals(0, snapshot.successSnapshot().count());
		assertEquals(1, snapshot.failsSnapshot().count());
		assertEquals(PARTS - 1, ranges.count);
		ranges.assertHeldBy(snapshot.durationSnapshot(), snapshot.latencySnapshot());
		assertEquals(1, bucketRecord().failed());
		ranges.assertHeldBy(bucketRecord().duration(), bucketRecord().latency());
	}

	/**
	 * A parent that read the object itself, as an {@code fs} read split by the threshold does, made a
	 * read of its own: it keeps its sample, next to its ranges'.
	 */
	@ParameterizedTest(name = "batch={0}")
	@ValueSource(booleans = {false, true
	})
	void aSplitReadThatReadTheObjectItselfKeepsItsSample(final boolean batch) {
		final var read = new Published(OpType.READ, "read-itself").readsTheObject();
		for (var part = 0; part < PARTS; part++) {
			read.partSucceeds(part);
		}
		read.ends(Status.SUCC);
		ranges.add(read.last());

		put(context(), batch, read);

		final var snapshot = metrics.terminalSnapshot();
		assertEquals(1, snapshot.successSnapshot().count());
		assertEquals(PARTS + 1, ranges.count);
		ranges.assertHeldBy(snapshot.durationSnapshot(), snapshot.latencySnapshot());
		assertEquals(PARTS + 1, snapshot.ttfbSnapshot().count(), "the parent's first byte is a real one");
		ranges.assertHeldBy(bucketRecord().duration(), bucketRecord().latency());
	}

	/**
	 * The driver's mark decides, not the timestamps: a parent marked requestless adds no sample even
	 * with a first-byte stamp of its own.
	 */
	@ParameterizedTest(name = "batch={0}")
	@ValueSource(booleans = {false, true
	})
	void aParentMarkedRequestlessAddsNoSampleEvenWithAFirstByte(final boolean batch) {
		final var read = new Published(OpType.READ, "marked").readsTheObject().marksRequestless();
		for (var part = 0; part < PARTS; part++) {
			read.partSucceeds(part);
		}
		read.ends(Status.SUCC);
		assertTrue(((DataOperationImpl<?>) read.last()).respDataTimeStart() > 0, "the parent has a first byte");

		put(context(), batch, read);

		final var snapshot = metrics.terminalSnapshot();
		assertEquals(1, snapshot.successSnapshot().count());
		assertEquals(PARTS, ranges.count);
		ranges.assertHeldBy(snapshot.durationSnapshot(), snapshot.latencySnapshot());
		assertEquals(PARTS, snapshot.ttfbSnapshot().count(), "only the ranges' first bytes");
		ranges.assertHeldBy(bucketRecord().duration(), bucketRecord().latency());
	}

	/**
	 * A parent the driver did not mark keeps its sample even with no first byte: {@code fs} reading a
	 * file listed larger than the threshold that turns out empty did I/O of its own.
	 */
	@ParameterizedTest(name = "batch={0}")
	@ValueSource(booleans = {false, true
	})
	void anUnmarkedParentWithNoFirstByteKeepsItsSample(final boolean batch) {
		final var read = new Published(OpType.READ, "end-of-file").startsAtEndOfFile();
		for (var part = 0; part < PARTS; part++) {
			read.partSucceeds(part);
		}
		read.ends(Status.SUCC);
		ranges.add(read.last());

		put(context(), batch, read);

		final var snapshot = metrics.terminalSnapshot();
		assertEquals(1, snapshot.successSnapshot().count());
		assertEquals(PARTS + 1, ranges.count);
		ranges.assertHeldBy(snapshot.durationSnapshot(), snapshot.latencySnapshot());
		assertEquals(PARTS, snapshot.ttfbSnapshot().count(), "the parent has no first byte");
		ranges.assertHeldBy(bucketRecord().duration(), bucketRecord().latency());
	}

	/**
	 * The mark is read for a READ only: a composite of another type keeps its sample even if marked, as
	 * an extension's own composite could be, so its 5.17.0 timing does not change.
	 */
	@ParameterizedTest(name = "batch={0}")
	@ValueSource(booleans = {false, true
	})
	void aMarkedUploadKeepsItsCompletionSample(final boolean batch) {
		final var upload = new Published(OpType.CREATE, "marked-upload").starts().marksRequestless();
		for (var part = 0; part < PARTS; part++) {
			upload.partSucceeds(part);
		}
		upload.ends(Status.SUCC);
		ranges.add(upload.last());

		put(context(), batch, upload);

		final var snapshot = metrics.terminalSnapshot();
		assertEquals(1, snapshot.successSnapshot().count());
		assertEquals(PARTS + 1, ranges.count);
		ranges.assertHeldBy(snapshot.durationSnapshot(), snapshot.latencySnapshot());
		ranges.assertHeldBy(bucketRecord().duration(), bucketRecord().latency());
	}

	/** The rule is for reads: an upload's finished parent is its completion request, which keeps its sample. */
	@ParameterizedTest(name = "batch={0}")
	@ValueSource(booleans = {false, true
	})
	void aMultipartUploadKeepsItsCompletionSample(final boolean batch) {
		final var upload = new Published(OpType.CREATE, "upload").starts();
		for (var part = 0; part < PARTS; part++) {
			upload.partSucceeds(part);
		}
		upload.ends(Status.SUCC);
		ranges.add(upload.last());

		put(context(), batch, upload);

		final var snapshot = metrics.terminalSnapshot();
		assertEquals(1, snapshot.successSnapshot().count());
		assertEquals(PARTS + 1, ranges.count);
		ranges.assertHeldBy(snapshot.durationSnapshot(), snapshot.latencySnapshot());
	}

	@ParameterizedTest(name = "batch={0}")
	@ValueSource(booleans = {false, true
	})
	void aWholeObjectOperationKeepsItsSample(final boolean batch) {
		final List<Operation<DataItem>> results = new ArrayList<>();
		for (final var type : List.of(OpType.READ, OpType.CREATE)) {
			final var op = new DataOperationImpl<DataItem>(
							0, type, item("whole-" + type), null, "/" + BUCKET, null, null, 0);
			respond(op, Status.SUCC);
			op.countBytesDone(PART_SIZE);
			final var result = (Operation<DataItem>) op.result();
			ranges.add(result);
			results.add(result);
		}

		final var context = context();
		if (batch) {
			assertEquals(results.size(), context.put(results, 0, results.size()));
		} else {
			results.forEach(result -> assertTrue(context.put(result)));
		}

		final var snapshot = metrics.terminalSnapshot();
		assertEquals(2, snapshot.successSnapshot().count());
		assertEquals(2 * PART_SIZE, snapshot.byteSnapshot().count());
		ranges.assertHeldBy(snapshot.durationSnapshot(), snapshot.latencySnapshot());
	}

	private BucketTerminalRecord bucketRecord() {
		return bucketMeters.terminalRecords().get(0);
	}

	private static void put(final LoadStepContextImpl<DataItem, Operation<DataItem>> context, final boolean batch,
					final Published published) {
		final var results = published.results;
		if (batch) {
			assertEquals(results.size(), context.put(results, 0, results.size()));
		} else {
			results.forEach(result -> assertTrue(context.put(result)));
		}
	}

	@SuppressWarnings("unchecked")
	private LoadStepContextImpl<DataItem, Operation<DataItem>> context() {
		config.val("load-op-retry", false);
		config.val("load-op-recycle-mode", false);
		final LoadGenerator<DataItem, Operation<DataItem>> generator = mock(LoadGenerator.class);
		final StorageDriver<DataItem, Operation<DataItem>> driver = mock(StorageDriver.class);
		final var context = new LoadStepContextImpl<>(
						"split-read-timing", generator, driver, metrics, config.configVal("load"), false);
		context.bucketRecorder(new BucketRecorder(List.of(BUCKET), bucketMeters, null));
		context.operationsMetricsOutput(new TraceOutput());
		return context;
	}

	private static MetricsContext<AllMetricsSnapshot> metricsContext() {
		final MetricsContext<AllMetricsSnapshot> context = MetricsContextImpl.builder()
						.loadStepId("split-read-timing")
						.opType(OpType.READ)
						.actualConcurrencyGauge(() -> 0)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(PART_SIZE))
						.outputPeriodSec(1)
						.stdOutColorFlag(false)
						.runId(0)
						.build();
		context.start();
		return context;
	}

	private static DataItem item(final String name) {
		return new DataItemImpl("/" + BUCKET + "/" + name, 0, PARTS * PART_SIZE);
	}

	/** One request's timing, each phase long enough to measure, and its outcome. */
	private static void respond(final DataOperationImpl<?> op, final Status status) {
		op.startRequest();
		pause();
		op.finishRequest();
		pause();
		op.startResponse();
		pause();
		op.startDataResponse();
		pause();
		op.finishResponse();
		op.status(status);
	}

	private static void pause() {
		final var start = System.nanoTime();
		while (System.nanoTime() - start < PAUSE_NANOS) {
			Thread.onSpinWait();
		}
	}

	/** The samples expected in the meters: exact, because each meter keeps its sum and extremes. */
	private static final class Samples {

		private long count;
		private long durationSum;
		private long minDuration = Long.MAX_VALUE;
		private long maxDuration;
		private long latencySum;
		private long minLatency = Long.MAX_VALUE;
		private long maxLatency;

		void add(final Operation<?> result) {
			count++;
			durationSum += result.duration();
			minDuration = Math.min(minDuration, result.duration());
			maxDuration = Math.max(maxDuration, result.duration());
			latencySum += result.latency();
			minLatency = Math.min(minLatency, result.latency());
			maxLatency = Math.max(maxLatency, result.latency());
		}

		void assertHeldBy(final TimingMetricSnapshot duration, final TimingMetricSnapshot latency) {
			assertEquals(count, duration.count(), "duration samples");
			assertEquals(durationSum, duration.sum(), "duration sum");
			assertEquals(minDuration, duration.min(), "shortest duration");
			assertEquals(maxDuration, duration.max(), "longest duration");
			assertEquals(count, latency.count(), "latency samples");
			assertEquals(latencySum, latency.sum(), "latency sum");
			assertEquals(minLatency, latency.min(), "shortest latency");
			assertEquals(maxLatency, latency.max(), "longest latency");
		}
	}

	/** What the step gives its operation metrics output: finished operations only. */
	private final class TraceOutput implements Output<Operation<DataItem>> {

		@Override
		public boolean put(final Operation<DataItem> result) {
			return traced.add(result);
		}

		@Override
		public int put(final List<Operation<DataItem>> results, final int from, final int to) {
			traced.addAll(results.subList(from, to));
			return to - from;
		}

		@Override
		public int put(final List<Operation<DataItem>> results) {
			return put(results, 0, results.size());
		}

		@Override
		public Input<Operation<DataItem>> getInput() {
			throw new AssertionError("not read");
		}

		@Override
		public void close() {}
	}

	/**
	 * The results a driver publishes for one composite operation, in order, each timed as a request.
	 * Every successful part result goes to {@link #ranges}, and a part's finished response settles it.
	 * The operation's own timing runs from its first pass to its end, around its parts.
	 */
	private final class Published {

		private final CompositeDataOperationImpl<DataItem> operation;
		private final List<? extends PartialDataOperation<DataItem>> parts;
		final List<Operation<DataItem>> results = new ArrayList<>();

		Published(final OpType type, final String name) {
			operation = new CompositeDataOperationImpl<>(
							0, type, item(name), null, "/" + BUCKET, null, null, 0, PART_SIZE);
			parts = operation.subOperations();
		}

		/**
		 * The initial pass. A split read's sends no request, as on {@code s3} and {@code s3-aws}: it
		 * has request and response times and no data response, and the driver marks it requestless.
		 * An upload's is its initiation request.
		 */
		Published starts() {
			if (OpType.READ.equals(operation.type())) {
				passesWithoutData();
				SplitReadMarks.markRangesSentAsRequests(operation);
			} else {
				respond(operation, Status.SUCC);
			}
			return publishOperation();
		}

		/**
		 * An initial pass with the same times and no mark: a driver that did I/O of its own and saw no
		 * byte, as {@code fs} does reading a file that is empty although it was listed larger.
		 */
		Published startsAtEndOfFile() {
			passesWithoutData();
			return publishOperation();
		}

		/** The driver marks the parent requestless after a pass that has a first-byte stamp. */
		Published marksRequestless() {
			SplitReadMarks.markRangesSentAsRequests(operation);
			return this;
		}

		private void passesWithoutData() {
			operation.startRequest();
			pause();
			operation.finishRequest();
			pause();
			operation.startResponse();
			operation.status(Status.SUCC);
		}

		/** An initial pass that reads the whole object itself, as {@code fs} does, before the ranges run. */
		Published readsTheObject() {
			respond(operation, Status.SUCC);
			operation.countBytesDone(PARTS * PART_SIZE);
			return publishOperation();
		}

		Published partSucceeds(final int part) {
			final var result = publishPart(part, Status.SUCC);
			ranges.add(result);
			return this;
		}

		/** A part attempt fails and the driver retries the part. */
		Published partFails(final int part) {
			publishPart(part, Status.RESP_FAIL_SVC);
			operation.undoMarkSubTaskCompleted();
			return this;
		}

		/** A part's last attempt fails. */
		Published partRunsOut(final int part) {
			publishPart(part, Status.RESP_FAIL_SVC);
			return this;
		}

		Published ends(final Status status) {
			pause();
			operation.finishResponse();
			operation.status(status);
			return publishOperation();
		}

		Operation<DataItem> last() {
			return results.get(results.size() - 1);
		}

		private Published publishOperation() {
			results.add(operation.result());
			return this;
		}

		@SuppressWarnings("unchecked")
		private Operation<DataItem> publishPart(final int index, final Status status) {
			final var part = (DataOperationImpl<DataItem>) (DataOperationImpl<?>) parts.get(index);
			part.reset();
			respond(part, status);
			part.countBytesDone(status == Status.SUCC ? PART_SIZE : 0);
			final var result = (Operation<DataItem>) (Operation<?>) part.result();
			results.add(result);
			return result;
		}
	}
}
