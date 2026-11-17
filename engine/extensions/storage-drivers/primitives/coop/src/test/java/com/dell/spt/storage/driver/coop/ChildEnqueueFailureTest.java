package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import com.dell.spt.base.item.op.OperationImpl;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.IntConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A composite whose child cannot be queued, because the child queue stays full until
 * {@value CoopStorageDriverBase#CHILD_OP_ENQUEUE_TIMEOUT_MILLIS_PROPERTY} runs out, the executor
 * refuses the deferred enqueue, or the dispatch deadline refuses the child's claim: the composite
 * ends with one counted result, an upload is aborted at most once, and nothing stays queued or
 * dispatched in the lifecycle tracker. The driver is not started: the test crosses dispatch and
 * completes requests itself, in a set order.
 */
@SuppressWarnings("unchecked")
@Timeout(30)
final class ChildEnqueueFailureTest {

	private static final String TIMEOUT_PROPERTY = CoopStorageDriverBase.CHILD_OP_ENQUEUE_TIMEOUT_MILLIS_PROPERTY;

	/** A driver whose deferred child enqueues run a test hook first, numbered from 1. */
	static final class Driver extends CoopStorageDriverMock<Item, Operation<Item>> {
		final AtomicInteger scheduled = new AtomicInteger();
		volatile IntConsumer beforeScheduled = n -> {};

		Driver(final DataInput in, final Config cfg) throws Exception {
			super("child-enqueue-failure-step", in, cfg, false, 1);
		}

		@Override
		protected void executeChildEnqueue(final Runnable task) {
			final int n = scheduled.incrementAndGet();
			super.executeChildEnqueue(() -> {
				beforeScheduled.accept(n);
				task.run();
			});
		}
	}

	private String previousTimeout;
	private DataInput dataInput;
	private Driver driver;
	private final List<Operation<Item>> results = Collections.synchronizedList(new ArrayList<>());

	@BeforeEach
	void setUp() throws Exception {
		previousTimeout = System.getProperty(TIMEOUT_PROPERTY);
		System.setProperty(TIMEOUT_PROPERTY, "100");
		dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (driver != null) {
			driver.close();
		}
		if (previousTimeout == null) {
			System.clearProperty(TIMEOUT_PROPERTY);
		} else {
			System.setProperty(TIMEOUT_PROPERTY, previousTimeout);
		}
	}

	/** A driver with a child queue of {@code queueCapacity}, whose output keeps every result. */
	private Driver driver(final int queueCapacity) throws Exception {
		final Config cfg = CompositeReadFailureDriverTest.cfg();
		when(cfg.intVal("driver-limit-queue-input")).thenReturn(queueCapacity);
		driver = new Driver(dataInput, cfg);
		final Output<Operation<Item>> out = mock(Output.class);
		when(out.put(any(Operation.class))).thenAnswer(invocation -> results.add(invocation.getArgument(0)));
		driver.operationResultOutput(out);
		return driver;
	}

	private CompositeDataOperationImpl<DataItem> composite(final OpType type, final int parts) {
		final var item = new DataItemImpl("c", 0, 1024L * parts);
		item.dataInput(dataInput);
		return new CompositeDataOperationImpl<>(0, type, item, null, "/bucket", null, null, 0, 1024);
	}

	private static Operation<Item> op(final Operation<?> op) {
		return (Operation<Item>) op;
	}

	private static Operation<Item> filler(final String name) {
		return new OperationImpl<>(0, OpType.CREATE, new DataItemImpl(name, 0, 1), null, "/bucket", null);
	}

	/** The composite's initial pass, its initiation for an upload, is answered; its first children follow. */
	private void initiate(final CompositeDataOperationImpl<DataItem> composite) {
		assertTrue(driver.operationLifecycle().driverQueued(op(composite)));
		assertTrue(driver.claimDispatchOwnership(op(composite)));
		composite.status(Operation.Status.SUCC);
		assertTrue(driver.handleCompleted(op(composite)));
	}

	/** {@code op} crosses dispatch from the driver's queue, as the dispatch task submits it. */
	private void dispatch(final Operation<?> op) {
		assertTrue(driver.childOpQueue.remove(op(op)), () -> op + " was queued: " + driver.childOpQueue);
		assertTrue(driver.claimDispatchOwnership(op(op)), () -> op + " crossed dispatch");
	}

	/** {@code part}'s attempt ends with {@code status} and reaches the driver's completion. */
	private void complete(
					final CompositeDataOperationImpl<DataItem> composite, final PartialDataOperationImpl<DataItem> part,
					final Operation.Status status) {
		part.status(status);
		composite.markSubTaskCompleted();
		assertTrue(driver.handleCompleted(op(part)));
	}

	/** The composite's finalization or abort, dispatched from the queue, ends with {@code status}. */
	private void finalize(final CompositeDataOperationImpl<DataItem> composite, final Operation.Status status) {
		dispatch(composite);
		composite.status(status);
		assertTrue(driver.handleCompleted(op(composite)));
	}

	/**
	 * The counted results of the test's one composite: a composite's result counts when it is done or
	 * failed, as {@code LoadStepContextImpl.put} counts it.
	 */
	private List<Operation.Status> countedResults(final CompositeDataOperationImpl<DataItem> composite) {
		synchronized (results) {
			return results.stream()
							.filter(result -> result instanceof CompositeOperation)
							.filter(result -> result.status() != Operation.Status.SUCC
											|| ((CompositeOperation<?>) result).allSubOperationsDone())
							.map(Operation::status)
							.toList();
		}
	}

	private static void await(final BooleanSupplier condition, final String what) throws InterruptedException {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			TimeUnit.MILLISECONDS.sleep(5);
		}
		assertTrue(condition.getAsBoolean(), what);
	}

	private void assertNothingOutstanding() throws InterruptedException {
		final var lifecycle = driver.operationLifecycle();
		await(() -> !lifecycle.hasOutstandingOperations(), "nothing is outstanding: " + lifecycle.snapshot());
	}

	/**
	 * A 1-slot child queue held full: the upload's parts time out, so the upload records
	 * {@code FAIL_TIMEOUT}, its abort is queued once the slot frees, and it ends as one counted failure.
	 */
	@Test
	void anUploadWhosePartsTimeOutIsAbortedOnceAndCountedOnce() throws Exception {
		driver(1);
		final var upload = composite(OpType.CREATE, 3);
		final var filler = filler("filler");
		assertTrue(driver.childOpQueue.offer(filler));
		// The second deferred enqueue is the abort's: the slot frees just before it.
		driver.beforeScheduled = n -> {
			if (n == 2) {
				driver.childOpQueue.remove(filler);
			}
		};

		initiate(upload);
		await(() -> driver.childOpQueue.contains(op(upload)), "the abort is queued: " + driver.childOpQueue);

		final var parts = upload.subOperations();
		for (final var part : parts) {
			assertEquals(OperationLifecycleState.UNATTEMPTED, part.lifecycle().state());
			assertEquals(Operation.Status.FAIL_TIMEOUT, part.status());
		}
		assertEquals("true", upload.get("mpuAbort"));
		assertEquals(Operation.Status.FAIL_TIMEOUT, CoopStorageDriverBase.abortedUploadStatus(upload));
		assertEquals(2, driver.scheduled.get(), "one deferred enqueue for the parts, one for the abort");
		assertEquals(1, driver.childOpQueue.size());

		finalize(upload, CoopStorageDriverBase.abortedUploadStatus(upload));

		assertEquals(List.of(Operation.Status.FAIL_TIMEOUT), countedResults(upload));
		assertEquals(OperationLifecycleState.TERMINAL, upload.lifecycle().state());
		assertNothingOutstanding();
		assertEquals(1, driver.operationLifecycle().counters().failed());
	}

	/**
	 * Part 1 is queued, parts 2 and 3 wait for the slot and time out. The upload is settled once for
	 * both, its one abort is queued after part 1 is sent, and part 1's success after the abort starts
	 * nothing.
	 */
	@Test
	void aBatchWithATimedOutPartSettlesTheUploadOnce() throws Exception {
		driver(1);
		final var upload = composite(OpType.CREATE, 3);
		final var parts = (List<PartialDataOperationImpl<DataItem>>) (List<?>) upload.subOperations();
		driver.beforeScheduled = n -> {
			if (n == 2) {
				dispatch(parts.get(0));
			}
		};

		initiate(upload);
		await(() -> driver.childOpQueue.contains(op(upload)), "the abort is queued: " + driver.childOpQueue);

		assertEquals(2, driver.scheduled.get(), "one deferred enqueue for parts 2 and 3, one for the abort");
		assertEquals(OperationLifecycleState.DISPATCHED, parts.get(0).lifecycle().state());
		assertEquals(OperationLifecycleState.UNATTEMPTED, parts.get(1).lifecycle().state());
		assertEquals(OperationLifecycleState.UNATTEMPTED, parts.get(2).lifecycle().state());

		complete(upload, parts.get(0), Operation.Status.SUCC);
		finalize(upload, CoopStorageDriverBase.abortedUploadStatus(upload));

		assertTrue(driver.childOpQueue.isEmpty(), () -> "nothing else was queued: " + driver.childOpQueue);
		assertEquals(List.of(Operation.Status.FAIL_TIMEOUT), countedResults(upload));
		assertNothingOutstanding();
	}

	/** A failed part's retry cannot be queued: the upload is aborted once and counted once. */
	@Test
	void aPartRetryWhoseEnqueueTimesOutAbortsTheUpload() throws Exception {
		driver(2);
		final var upload = composite(OpType.CREATE, 2);
		initiate(upload);
		final var parts = (List<PartialDataOperationImpl<DataItem>>) (List<?>) upload.subOperations();
		parts.forEach(this::dispatch);
		final var fillers = List.of(filler("filler-1"), filler("filler-2"));
		driver.childOpQueue.addAll(fillers);
		driver.beforeScheduled = n -> {
			if (n == 2) {
				driver.childOpQueue.removeAll(fillers);
			}
		};

		complete(upload, parts.get(0), Operation.Status.FAIL_IO);
		await(() -> driver.childOpQueue.contains(op(upload)), "the abort is queued: " + driver.childOpQueue);
		assertEquals(1, parts.get(0).retryCount(), "the retry was attempted");
		assertEquals(OperationLifecycleState.UNATTEMPTED, parts.get(0).lifecycle().state());

		// Part 2 ends after the abort is queued: it is not retried and starts nothing.
		complete(upload, parts.get(1), Operation.Status.RESP_FAIL_NOT_FOUND);
		assertEquals(0, parts.get(1).retryCount());
		finalize(upload, CoopStorageDriverBase.abortedUploadStatus(upload));

		assertEquals(List.of(Operation.Status.FAIL_TIMEOUT), countedResults(upload));
		assertTrue(driver.childOpQueue.isEmpty(), () -> "nothing else was queued: " + driver.childOpQueue);
		assertNothingOutstanding();
	}

	/**
	 * Range 1 of a composite READ is queued, ranges 2 and 3 time out. They are failures out of retries,
	 * so range 1's success finalizes the read, which reports {@code FAIL_TIMEOUT} and is counted once.
	 */
	@Test
	void aRangeReadWhoseRangesTimeOutIsOneFailure() throws Exception {
		driver(1);
		final var read = composite(OpType.READ, 3);
		final var ranges = (List<PartialDataOperationImpl<DataItem>>) (List<?>) read.subOperations();

		initiate(read);
		// The failure is recorded under the read's monitor after the ranges are marked unattempted.
		await(() -> {
			synchronized (read) {
				return ranges.get(2).lifecycle().state() == OperationLifecycleState.UNATTEMPTED
								&& Operation.Status.FAIL_TIMEOUT.name().equals(read.get("mpuFailure"))
								&& ranges.get(1).retryCount() >= CoopStorageDriverBase.MAX_PART_RETRIES
								&& ranges.get(2).retryCount() >= CoopStorageDriverBase.MAX_PART_RETRIES;
			}
		}, "ranges 2 and 3 time out and fail the read");
		assertNull(read.get("mpuAbort"), "a read has nothing to abort");
		assertFalse(driver.childOpQueue.contains(op(read)), "range 1 is still to run");

		dispatch(ranges.get(0));
		complete(read, ranges.get(0), Operation.Status.SUCC);
		assertTrue(read.allSubOperationsDone());
		assertSame(op(read), driver.childOpQueue.peek(), "range 1's completion finalizes the read");
		// The S3 driver's finalization of a composite READ reports its recorded range failure.
		finalize(read, Operation.Status.valueOf(read.get("mpuFailure")));

		assertEquals(List.of(Operation.Status.FAIL_TIMEOUT), countedResults(read));
		assertNothingOutstanding();
	}

	/** Every range of the read times out: the read is finalized without waiting for anything. */
	@Test
	void aRangeReadWithNoRangeQueuedIsFinalizedAtOnce() throws Exception {
		driver(1);
		final var read = composite(OpType.READ, 3);
		final var filler = filler("filler");
		assertTrue(driver.childOpQueue.offer(filler));
		driver.beforeScheduled = n -> {
			if (n == 2) {
				driver.childOpQueue.remove(filler);
			}
		};

		initiate(read);
		await(() -> driver.childOpQueue.contains(op(read)), "the read's finalization is queued");
		finalize(read, Operation.Status.valueOf(read.get("mpuFailure")));

		assertEquals(List.of(Operation.Status.FAIL_TIMEOUT), countedResults(read));
		assertNothingOutstanding();
	}

	/**
	 * The upload's completion cannot be queued: the upload is published as a {@code FAIL_TIMEOUT}
	 * failure and is not left dispatched.
	 */
	@Test
	void aCompletionWhoseEnqueueTimesOutPublishesTheUploadAsAFailure() throws Exception {
		driver(1);
		final var upload = composite(OpType.CREATE, 1);
		initiate(upload);
		final var part = (PartialDataOperationImpl<DataItem>) upload.subOperations().get(0);
		dispatch(part);
		assertTrue(driver.childOpQueue.offer(filler("filler")));

		complete(upload, part, Operation.Status.SUCC);
		// The tracker's in-flight count drops after the upload's state change.
		await(() -> upload.lifecycle().state() == OperationLifecycleState.TERMINAL
						&& driver.operationLifecycle().counters().inFlight() == 0, "the upload is published");

		assertEquals(List.of(Operation.Status.FAIL_TIMEOUT), countedResults(upload));
		assertEquals(1, driver.childOpQueue.size(), "only the filler is queued");
		assertEquals(OperationLifecycleState.TERMINAL, part.lifecycle().state());
	}

	/**
	 * The abort of an upload whose part ran out of retries cannot be queued: the upload is published
	 * with that part's failure, and its parts in flight then end without a second result, a retry, a
	 * next part or a new claim of the upload.
	 */
	@Test
	void anAbortWhoseEnqueueTimesOutPublishesTheUploadAndLateSiblingsStartNothing() throws Exception {
		driver(3);
		final var upload = composite(OpType.CREATE, 4);
		final var maxParts = CoopStorageDriverBase.class.getDeclaredField("mpuMaxParts");
		maxParts.setAccessible(true);
		maxParts.setInt(driver, 3);
		initiate(upload);
		final var parts = (List<PartialDataOperationImpl<DataItem>>) (List<?>) upload.subOperations();
		parts.subList(0, 3).forEach(this::dispatch);
		for (var i = 0; i < 3; i++) {
			driver.childOpQueue.add(filler("filler-" + i));
		}

		for (int i = 0; i < CoopStorageDriverBase.MAX_PART_RETRIES; i++) {
			parts.get(0).incrementRetryCount();
		}
		complete(upload, parts.get(0), Operation.Status.RESP_FAIL_SVC);
		await(() -> upload.lifecycle().state() == OperationLifecycleState.TERMINAL, "the upload is published");
		assertEquals(List.of(Operation.Status.RESP_FAIL_SVC), countedResults(upload), "the part's failure is kept");

		complete(upload, parts.get(1), Operation.Status.SUCC);
		complete(upload, parts.get(2), Operation.Status.FAIL_IO);

		assertEquals(List.of(Operation.Status.RESP_FAIL_SVC), countedResults(upload), "no second result");
		assertEquals(0, parts.get(2).retryCount(), "no retry");
		assertEquals(OperationLifecycleState.NEW, parts.get(3).lifecycle().state(), "no next part");
		assertEquals(3, driver.childOpQueue.size(), "only the fillers are queued");
		assertEquals(OperationLifecycleState.TERMINAL, upload.lifecycle().state(), "the upload is not claimed again");
		assertNothingOutstanding();
	}

	/**
	 * Past the dispatch deadline the next part cannot be claimed: the upload, which would never be
	 * dispatched again, is published as one failure instead of staying dispatched.
	 */
	@Test
	void aPartRefusedItsClaimPublishesTheUpload() throws Exception {
		driver(4);
		final var upload = composite(OpType.CREATE, 2);
		final var maxParts = CoopStorageDriverBase.class.getDeclaredField("mpuMaxParts");
		maxParts.setAccessible(true);
		maxParts.setInt(driver, 1);
		initiate(upload);
		final var parts = (List<PartialDataOperationImpl<DataItem>>) (List<?>) upload.subOperations();
		dispatch(parts.get(0));
		driver.operationLifecycle().enforceDispatchDeadline(System.nanoTime() - 1);

		complete(upload, parts.get(0), Operation.Status.SUCC);

		assertEquals(OperationLifecycleState.TERMINAL, upload.lifecycle().state());
		assertEquals(List.of(Operation.Status.FAIL_TIMEOUT), countedResults(upload));
		assertTrue(driver.childOpQueue.isEmpty());
		assertEquals(0, driver.scheduled.get(), "nothing is deferred");
		assertNothingOutstanding();
	}

	/**
	 * As {@link #aPartRefusedItsClaimPublishesTheUpload}, with a second part still in flight when the
	 * next part is refused its claim: that part's later success neither queues the upload's
	 * finalization again nor publishes a second result.
	 */
	@Test
	void aPartRefusedItsClaimWithASiblingInFlightPublishesTheUploadOnce() throws Exception {
		driver(4);
		final var upload = composite(OpType.CREATE, 3);
		final var maxParts = CoopStorageDriverBase.class.getDeclaredField("mpuMaxParts");
		maxParts.setAccessible(true);
		maxParts.setInt(driver, 2);
		initiate(upload);
		final var parts = (List<PartialDataOperationImpl<DataItem>>) (List<?>) upload.subOperations();
		dispatch(parts.get(0));
		dispatch(parts.get(1));
		driver.operationLifecycle().enforceDispatchDeadline(System.nanoTime() - 1);

		complete(upload, parts.get(0), Operation.Status.SUCC);
		assertEquals(OperationLifecycleState.TERMINAL, upload.lifecycle().state());
		assertEquals(List.of(Operation.Status.FAIL_TIMEOUT), countedResults(upload));

		complete(upload, parts.get(1), Operation.Status.SUCC);

		assertEquals(List.of(Operation.Status.FAIL_TIMEOUT), countedResults(upload), "no second result");
		assertTrue(driver.childOpQueue.isEmpty(), () -> "nothing is queued: " + driver.childOpQueue);
		assertEquals(0, driver.scheduled.get(), "nothing is deferred");
		assertNothingOutstanding();
	}

	/**
	 * A read windowed by {@code mpuMaxParts} to 2 ranges, with a 1-slot queue: range 1 is queued and
	 * range 2 times out, so range 4, not started yet, is settled with it and range 3 is never sent.
	 * Range 1's success then finalizes the read, counted once with {@code FAIL_TIMEOUT}.
	 */
	@Test
	void aWindowedRangeReadWhoseRangeTimesOutSettlesTheRangesNotStarted() throws Exception {
		driver(1);
		final var maxParts = CoopStorageDriverBase.class.getDeclaredField("mpuMaxParts");
		maxParts.setAccessible(true);
		maxParts.setInt(driver, 2);
		final var read = composite(OpType.READ, 4);
		final var ranges = (List<PartialDataOperationImpl<DataItem>>) (List<?>) read.subOperations();
		initiate(read);
		await(() -> {
			synchronized (read) {
				return ranges.get(1).lifecycle().state() == OperationLifecycleState.UNATTEMPTED
								&& ranges.get(3).retryCount() >= CoopStorageDriverBase.MAX_PART_RETRIES;
			}
		}, "range 2 timed out");
		assertEquals(OperationLifecycleState.NEW, ranges.get(2).lifecycle().state());
		dispatch(ranges.get(0));
		complete(read, ranges.get(0), Operation.Status.SUCC);
		assertTrue(read.allSubOperationsDone());
		assertSame(op(read), driver.childOpQueue.peek());
		finalize(read, Operation.Status.valueOf(read.get("mpuFailure")));
		assertEquals(List.of(Operation.Status.FAIL_TIMEOUT), countedResults(read));
		assertNothingOutstanding();
	}
}
