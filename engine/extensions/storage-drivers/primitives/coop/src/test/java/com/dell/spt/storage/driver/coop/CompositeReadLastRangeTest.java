package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.storage.driver.coop.mock.CoopStorageDriverMock;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A multi-range read ends on the thread that completed its last range, when its driver's
 * finalization of a read sends no request, as on the {@code s3} and {@code s3-aws} drivers. It takes
 * no place in the child queue, so it is counted at once and a stop cannot refuse it. A read with a
 * range never sent has no last range: the stop leaves it unresolved. The driver is not started: the
 * test crosses dispatch and completes requests itself, in a set order.
 */
@SuppressWarnings("unchecked")
@Timeout(60)
final class CompositeReadLastRangeTest {

	private static final int RANGES = 3;
	private static final long RANGE_SIZE = 1024;

	/** Ends a read as the {@code s3} driver does, and counts the reads it was asked to end. */
	private static final class FinishingDriver extends CoopStorageDriverMock<Item, Operation<Item>> {
		final AtomicInteger finished = new AtomicInteger();
		volatile Thread finishedOn;

		FinishingDriver(final DataInput in, final Config cfg) throws Exception {
			super("composite-read-last-range", in, cfg, false, 1);
		}

		@Override
		protected boolean finishCompositeRead(final Operation<Item> read) {
			finished.incrementAndGet();
			finishedOn = Thread.currentThread();
			final String rangeFailure = ((CompositeOperation<?>) read).get("mpuFailure");
			read.status(rangeFailure == null ? Operation.Status.SUCC : Operation.Status.valueOf(rangeFailure));
			read.finishResponse();
			handleCompleted(read);
			return true;
		}
	}

	private final List<Operation<Item>> results = Collections.synchronizedList(new ArrayList<>());
	private DataInput dataInput;
	private CoopStorageDriverMock<Item, Operation<Item>> driver;

	@BeforeEach
	void setUp() throws Exception {
		dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
	}

	@AfterEach
	void closeDriver() throws Exception {
		if (driver != null) {
			driver.close();
		}
	}

	private Config config() {
		final var cfg = CompositeReadFailureDriverTest.cfg();
		when(cfg.intVal("driver-limit-queue-input")).thenReturn(64);
		return cfg;
	}

	private void use(final CoopStorageDriverMock<Item, Operation<Item>> newDriver) {
		driver = newDriver;
		final Output<Operation<Item>> out = mock(Output.class);
		when(out.put(any(Operation.class))).thenAnswer(invocation -> results.add(invocation.getArgument(0)));
		driver.operationResultOutput(out);
	}

	private FinishingDriver finishingDriver() throws Exception {
		final var finishing = new FinishingDriver(dataInput, config());
		use(finishing);
		return finishing;
	}

	private static Operation<Item> op(final Operation<?> op) {
		return (Operation<Item>) op;
	}

	private CompositeDataOperationImpl<DataItem> composite(final OpType type, final String name) {
		final var item = new DataItemImpl(name, 0, RANGES * RANGE_SIZE);
		item.dataInput(dataInput);
		return new CompositeDataOperationImpl<>(0, type, item, null, "/bucket", null, null, 0, RANGE_SIZE);
	}

	/**
	 * The read's initial pass, which sends no request, as the drivers run it: it asks for its parts
	 * first, which is what gives a recycled read parts again. Its ranges are queued.
	 */
	private List<PartialDataOperationImpl<DataItem>> initialPass(final CompositeDataOperationImpl<DataItem> read) {
		assertTrue(driver.operationLifecycle().driverQueued(op(read)));
		final var ranges = (List<PartialDataOperationImpl<DataItem>>) (List<?>) read.subOperations();
		read.reset();
		assertTrue(driver.claimDispatchOwnership(op(read)));
		read.startRequest();
		read.finishRequest();
		read.startResponse();
		read.status(Operation.Status.SUCC);
		assertTrue(driver.handleCompleted(op(read)));
		return ranges;
	}

	/** {@code op} crosses dispatch from the head of the driver's child queue. */
	private void dispatch(final Operation<?> op) throws InterruptedException {
		assertSame(op, driver.childOpQueue.poll(5, TimeUnit.SECONDS));
		assertTrue(driver.claimDispatchOwnership(op(op)), () -> op + " crossed dispatch");
	}

	private void dispatchAll(final List<PartialDataOperationImpl<DataItem>> ranges) throws InterruptedException {
		for (final var range : ranges) {
			dispatch(range);
		}
	}

	/** {@code range}'s attempt ends with {@code status} and reaches the driver's completion. */
	private void complete(
					final CompositeDataOperationImpl<DataItem> read,
					final PartialDataOperationImpl<DataItem> range,
					final Operation.Status status) {
		range.status(status);
		read.markSubTaskCompleted();
		assertTrue(driver.handleCompleted(op(range)));
	}

	private void completeAll(
					final CompositeDataOperationImpl<DataItem> read, final List<PartialDataOperationImpl<DataItem>> ranges) {
		for (final var range : ranges) {
			complete(read, range, Operation.Status.SUCC);
		}
	}

	/**
	 * The results {@code read} output, each a copy of it as it then stood: its initial pass and, once
	 * it has ended, its end.
	 */
	private List<Operation<Item>> resultsOf(final Operation<?> read) {
		synchronized (results) {
			return results.stream()
							.filter(result -> result instanceof CompositeOperation && result.item() == read.item())
							.toList();
		}
	}

	/** The read's result at its end, the copy that recycle mode hands back to the driver. */
	private CompositeDataOperationImpl<DataItem> assertEndedOnce(
					final CompositeDataOperationImpl<DataItem> read, final Operation.Status status) {
		final var outcome = resultsOf(read);
		assertEquals(2, outcome.size(), () -> "the initial pass and the read's end: " + outcome);
		final var end = (CompositeDataOperationImpl<DataItem>) (Operation<?>) outcome.get(1);
		assertEquals(status, end.status());
		assertTrue(end.allSubOperationsDone(), "the end is the result that counts");
		assertEquals(OperationLifecycleState.TERMINAL, driver.operationLifecycle().stateOf(op(read)));
		return end;
	}

	private void assertNothingOutstanding() {
		final var snapshot = driver.operationLifecycle().snapshot();
		assertEquals(0, snapshot.inFlight(), snapshot::toString);
		assertEquals(0, snapshot.unresolved(), snapshot::toString);
		assertFalse(driver.operationLifecycle().hasOutstandingOperations(), snapshot::toString);
	}

	@Test
	void theLastRangeEndsItsReadOnItsThreadWithNoQueueEntry() throws Exception {
		final var finishing = finishingDriver();
		final var read = composite(OpType.READ, "open");
		final var ranges = initialPass(read);
		dispatchAll(ranges);

		complete(read, ranges.get(0), Operation.Status.SUCC);
		complete(read, ranges.get(1), Operation.Status.SUCC);
		assertEquals(0, finishing.finished.get(), "ranges are still to end");
		assertEquals(1, resultsOf(read).size(), "only the initial pass so far");

		complete(read, ranges.get(2), Operation.Status.SUCC);

		assertEquals(1, finishing.finished.get());
		assertSame(Thread.currentThread(), finishing.finishedOn);
		assertTrue(driver.childOpQueue.isEmpty(), () -> "queued: " + driver.childOpQueue);
		assertEndedOnce(read, Operation.Status.SUCC);
		assertNothingOutstanding();
	}

	/** Every range was sent before the stop, so the read ends and counts, though admission is closed. */
	@Test
	void aReadWhoseRangesWereAllSentEndsAfterAdmissionCloses() throws Exception {
		final var finishing = finishingDriver();
		final var read = composite(OpType.READ, "closed");
		final var ranges = initialPass(read);
		dispatchAll(ranges);
		driver.closeAdmission();
		assertTrue(driver.recoverQueuedOperations().isEmpty());

		completeAll(read, ranges);

		assertEquals(1, finishing.finished.get());
		assertTrue(driver.childOpQueue.isEmpty(), () -> "queued: " + driver.childOpQueue);
		assertEndedOnce(read, Operation.Status.SUCC);
		assertNothingOutstanding();
	}

	/** The stop takes back a range never sent: the read has no last range and stays unresolved. */
	@Test
	void aReadWithARangeTheStopRefusedIsNotEndedAndStaysUnresolved() throws Exception {
		final var finishing = finishingDriver();
		final var read = composite(OpType.READ, "refused");
		final var ranges = initialPass(read);
		dispatch(ranges.get(0));
		dispatch(ranges.get(1));
		driver.closeAdmission();
		assertEquals(List.of(ranges.get(2)), driver.recoverQueuedOperations(), "the stop takes back the unsent range");

		complete(read, ranges.get(0), Operation.Status.SUCC);
		complete(read, ranges.get(1), Operation.Status.SUCC);

		assertEquals(0, finishing.finished.get());
		assertEquals(1, resultsOf(read).size(), "the read has its initial pass's result and no other");
		assertEquals(OperationLifecycleState.UNRESOLVED, driver.operationLifecycle().stateOf(op(read)));
		final var snapshot = driver.operationLifecycle().snapshot();
		assertEquals(1, snapshot.unresolved(), snapshot::toString);
		assertEquals(0, snapshot.inFlight(), snapshot::toString);
	}

	/** Ranges that end together, on one thread each, end their read exactly once. */
	@Test
	void rangesEndingTogetherEndTheirReadOnce() throws Exception {
		final var finishing = finishingDriver();
		final var rounds = 200;
		try (var threads = Executors.newFixedThreadPool(RANGES)) {
			for (var round = 0; round < rounds; round++) {
				final var read = composite(OpType.READ, "together-" + round);
				final var ranges = initialPass(read);
				dispatchAll(ranges);
				final var start = new CountDownLatch(RANGES);
				final List<Future<?>> ends = new ArrayList<>();
				for (final var range : ranges) {
					ends.add(threads.submit(() -> {
						start.countDown();
						start.await();
						complete(read, range, Operation.Status.SUCC);
						return null;
					}));
				}
				for (final var end : ends) {
					end.get(10, TimeUnit.SECONDS);
				}
				assertEquals(round + 1, finishing.finished.get(), "round " + round);
				assertEndedOnce(read, Operation.Status.SUCC);
			}
		}
		assertTrue(driver.childOpQueue.isEmpty(), () -> "queued: " + driver.childOpQueue);
		assertNothingOutstanding();
	}

	/** A range out of retries gives the read its status when the last range ends. */
	@Test
	void aRangeOutOfRetriesGivesTheReadItsStatus() throws Exception {
		final var finishing = finishingDriver();
		final var read = composite(OpType.READ, "failed");
		final var ranges = initialPass(read);
		dispatchAll(ranges);
		final var failing = ranges.get(1);
		complete(read, ranges.get(0), Operation.Status.SUCC);
		complete(read, ranges.get(2), Operation.Status.SUCC);

		for (var retry = 0; retry < CoopStorageDriverBase.MAX_PART_RETRIES; retry++) {
			complete(read, failing, Operation.Status.FAIL_IO);
			assertEquals(0, finishing.finished.get(), "the range is retried");
			dispatch(failing);
		}
		complete(read, failing, Operation.Status.FAIL_IO);

		assertEquals(1, finishing.finished.get());
		assertTrue(driver.childOpQueue.isEmpty(), () -> "queued: " + driver.childOpQueue);
		assertEndedOnce(read, Operation.Status.FAIL_IO);
		assertNothingOutstanding();
	}

	/**
	 * A recycled read is the result of the ended one. It has no parts and counts them all done, and
	 * neither ends at its initial pass nor before its own last range, over several rounds.
	 */
	@Test
	void aRecycledReadEndsOncePerRoundAfterItsOwnLastRange() throws Exception {
		final var finishing = finishingDriver();
		var read = composite(OpType.READ, "recycled");
		final var rounds = 3;
		for (var round = 0; round < rounds; round++) {
			final var ranges = initialPass(read);
			assertEquals(round, finishing.finished.get(), "the initial pass ends nothing");
			assertEquals(1, resultsOf(read).size(), "only the initial pass");
			assertEquals(RANGES, driver.childOpQueue.size(), "the round's ranges are queued");
			dispatchAll(ranges);
			complete(read, ranges.get(0), Operation.Status.SUCC);
			complete(read, ranges.get(1), Operation.Status.SUCC);
			assertEquals(round, finishing.finished.get(), "a range is still to end");

			complete(read, ranges.get(2), Operation.Status.SUCC);

			assertEquals(round + 1, finishing.finished.get());
			final var recycled = assertEndedOnce(read, Operation.Status.SUCC);

			assertNotSame(read, recycled);
			assertTrue(recycled.allSubOperationsDone(), "a recycled read counts its parts done before it has any");
			results.clear();
			read = recycled;
		}
		assertNothingOutstanding();
	}

	/** A failed read that {@code load.op.retry} repeats starts over, and ends again at its new last range. */
	@Test
	void aRetriedReadEndsAgainAtTheLastRangeOfItsNewAttempt() throws Exception {
		final var finishing = finishingDriver();
		final var read = composite(OpType.READ, "retried");
		var ranges = initialPass(read);
		dispatchAll(ranges);
		ranges.get(0).incrementRetryCount();
		ranges.get(0).incrementRetryCount();
		ranges.get(0).incrementRetryCount();
		complete(read, ranges.get(0), Operation.Status.FAIL_IO);
		complete(read, ranges.get(1), Operation.Status.SUCC);
		complete(read, ranges.get(2), Operation.Status.SUCC);
		assertEquals(1, finishing.finished.get());
		assertEndedOnce(read, Operation.Status.FAIL_IO);

		results.clear();
		read.resetSubOperations();
		read.startNextLifecycle();
		ranges = initialPass(read);
		assertEquals(1, finishing.finished.get(), "the new attempt's initial pass ends nothing");
		dispatchAll(ranges);
		completeAll(read, ranges);

		assertEquals(2, finishing.finished.get());
		assertEndedOnce(read, Operation.Status.SUCC);
		assertNothingOutstanding();
	}

	/** An upload's completion is a request, so its finalization is queued and never comes to the hook. */
	@Test
	void anUploadsFinalizationIsStillQueued() throws Exception {
		final var finishing = finishingDriver();
		final var upload = composite(OpType.CREATE, "upload");
		upload.put("uploadId", "upload-1");
		final var parts = initialPass(upload);
		dispatchAll(parts);

		completeAll(upload, parts);

		assertEquals(0, finishing.finished.get());
		assertSame(upload, driver.childOpQueue.peek());
	}

	/** A driver that does not end reads itself queues the finalization, as every driver used to. */
	@Test
	void aDriverThatDoesNotEndReadsQueuesTheFinalization() throws Exception {
		use(new CoopStorageDriverMock<>("composite-read-queued", dataInput, config(), false, 1));
		final var read = composite(OpType.READ, "queued");
		final var ranges = initialPass(read);
		dispatchAll(ranges);

		completeAll(read, ranges);

		assertSame(read, driver.childOpQueue.peek());
		assertEquals(1, resultsOf(read).size(), "the read has not ended");
	}
}
