package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
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
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A composite operation, a multipart upload or a multi-range read, whose next part, range or
 * finalization the closed admission of a stop refuses, or whose queued work queue recovery takes back,
 * through the driver's own completion, child-enqueue and recovery paths. Such an operation can no
 * longer finish, so it is unresolved at once, instead of at the drain's deadline, which held the step's
 * end for {@code load-op-wait-limit}. An upload whose initiation was answered is open on the target,
 * so it is unresolved even when none of its parts was sent.
 *
 * <p>Concurrency is 4, the explicit part window ({@code driver-limit-multipart-parts}) is 3, two uploads
 * may be open at once ({@code driver-limit-multipart-objects}), and the dispatch batch is 1. The
 * completions run in a set order: the order that a step against a target can only arrange by timing.
 */
@SuppressWarnings("unchecked")
final class TimeLimitStopCompositeTest {

	private static final int MPU_OBJECT_PERMITS = 2;

	private DataInput dataInput;
	private EndedUploadPartTest.DeferringDriver driver;
	private final AtomicInteger uploadResults = new AtomicInteger();

	private void startDriver(final int inputQueueLimit) throws Exception {
		dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var cfg = CompositeReadFailureDriverTest.cfg();
		when(cfg.intVal("driver-limit-queue-input")).thenReturn(inputQueueLimit);
		when(cfg.intVal("driver-limit-multipart-parts")).thenReturn(3);
		when(cfg.intVal("driver-limit-multipart-objects")).thenReturn(MPU_OBJECT_PERMITS);
		driver = new EndedUploadPartTest.DeferringDriver(dataInput, cfg);
		final Output<Operation<Item>> out = mock(Output.class);
		when(out.put(any(Operation.class))).thenAnswer(invocation -> {
			if (invocation.getArgument(0) instanceof CompositeOperation) {
				uploadResults.incrementAndGet();
			}
			return true;
		});
		driver.operationResultOutput(out);
	}

	@AfterEach
	void closeDriver() throws Exception {
		if (driver != null) {
			driver.close();
		}
	}

	private static Operation<Item> op(final Operation<?> op) {
		return (Operation<Item>) op;
	}

	private CompositeDataOperationImpl<DataItem> upload(final long size) {
		final var item = new DataItemImpl("c", 0, size);
		item.dataInput(dataInput);
		return new CompositeDataOperationImpl<DataItem>(0, OpType.CREATE, item, null, "/bucket", null, null, 0, 1024);
	}

	/**
	 * The upload's initiation is dispatched with its upload permit and is in flight. Its answer carries an
	 * upload ID, which the driver records before the completion runs, as the {@code s3} drivers do.
	 */
	private CompositeDataOperationImpl<DataItem> initiationInFlight(final long size) {
		final var upload = upload(size);
		// Package-private on the base, which the mock driver of another package does not inherit.
		final CoopStorageDriverBase<Item, Operation<Item>> base = driver;
		assertTrue(base.tryAcquireMpuObjectPermit(op(upload)), "the initiation takes an upload permit");
		assertTrue(driver.operationLifecycle().driverQueued(op(upload)));
		assertTrue(driver.claimDispatchOwnership(op(upload)));
		upload.put("uploadId", "upload-1");
		upload.status(Operation.Status.SUCC);
		return upload;
	}

	/** A five-part upload whose initiation is answered: the driver has queued its first three parts. */
	private CompositeDataOperationImpl<DataItem> initiatedUpload() {
		final var upload = initiationInFlight(5120);
		assertTrue(driver.handleCompleted(op(upload)));
		assertEquals(1, uploadResults.get());
		return upload;
	}

	private static List<PartialDataOperationImpl<DataItem>> parts(final CompositeDataOperationImpl<DataItem> upload) {
		return (List<PartialDataOperationImpl<DataItem>>) (List<?>) upload.subOperations();
	}

	/** {@code op} crosses dispatch from the head of the driver's child queue, as the dispatch task submits it. */
	private void dispatch(final Operation<?> op) throws InterruptedException {
		assertSame(op, driver.childOpQueue.poll(5, TimeUnit.SECONDS));
		assertTrue(driver.claimDispatchOwnership(op(op)), () -> op + " crossed dispatch");
	}

	/** {@code part}'s attempt ends with {@code status} and reaches the driver's completion. */
	private void complete(
					final CompositeDataOperationImpl<DataItem> upload,
					final PartialDataOperationImpl<DataItem> part,
					final Operation.Status status) {
		part.status(status);
		upload.markSubTaskCompleted();
		assertTrue(driver.handleCompleted(op(part)));
	}

	/** The step's stop: admission closes, then queue recovery takes back what was not dispatched. */
	private List<Operation<Item>> stop() {
		driver.closeAdmission();
		return driver.recoverQueuedOperations();
	}

	private int freeUploadPermits() {
		return driver.mpuObjectThrottle.availablePermits();
	}

	/**
	 * The upload is unresolved, once, with nothing of it in flight or queued and its upload permit back,
	 * and has no result beyond its initiation's.
	 */
	private void assertUnresolvedOnce(final CompositeDataOperationImpl<DataItem> upload) {
		final var lifecycle = driver.operationLifecycle();
		assertEquals(OperationLifecycleState.UNRESOLVED, lifecycle.stateOf(op(upload)), () -> "the upload: " + lifecycle.snapshot());
		final var snapshot = lifecycle.snapshot();
		assertEquals(1, snapshot.unresolved(), snapshot::toString);
		assertEquals(0, snapshot.inFlight(), () -> "nothing holds the drain: " + snapshot);
		assertFalse(lifecycle.hasOutstandingOperations(), snapshot::toString);
		assertTrue(lifecycle.counters().reconciled(), () -> lifecycle.counters().toString());
		assertEquals(1, uploadResults.get(), "the upload has its initiation's result and no other");
		assertEquals(MPU_OBJECT_PERMITS, freeUploadPermits(), "the upload permit is back, once");
	}

	/** The last part's answer comes after the stop, which refuses the upload's completion. */
	@Test
	void anUploadWhoseCompletionTheStopRefusesIsUnresolvedAtOnce() throws Exception {
		startDriver(16);
		final var upload = initiatedUpload();
		final var parts = parts(upload);
		for (int i = 0; i < 3; i++) {
			dispatch(parts.get(i));
		}
		complete(upload, parts.get(0), Operation.Status.SUCC);
		complete(upload, parts.get(1), Operation.Status.SUCC);
		dispatch(parts.get(3));
		dispatch(parts.get(4));
		complete(upload, parts.get(2), Operation.Status.SUCC);
		complete(upload, parts.get(3), Operation.Status.SUCC);
		assertTrue(stop().isEmpty(), "nothing is queued at the stop");

		complete(upload, parts.get(4), Operation.Status.SUCC);

		assertUnresolvedOnce(upload);
	}

	/**
	 * Part 1's answer comes after the stop, which refuses part 4: the upload is unresolved while parts 2
	 * and 3 are still in flight. Their answers then end them without claiming the upload again: part 2's
	 * success fetches part 5, whose enqueue is dropped, and part 3's failure, with retries left, is not
	 * retried.
	 */
	@Test
	void partsInFlightWhenTheUploadSettlesEndWithoutClaimingItAgain() throws Exception {
		startDriver(16);
		final var lifecycle = driver.operationLifecycle();
		final var upload = initiatedUpload();
		final var parts = parts(upload);
		for (int i = 0; i < 3; i++) {
			dispatch(parts.get(i));
		}
		assertTrue(stop().isEmpty(), "nothing is queued at the stop");

		complete(upload, parts.get(0), Operation.Status.SUCC);
		assertEquals(OperationLifecycleState.UNRESOLVED, lifecycle.stateOf(op(upload)), "the upload settles at the refusal");
		assertEquals(OperationLifecycleState.UNATTEMPTED, lifecycle.stateOf(op(parts.get(3))), "part 4 is settled unsent");
		assertEquals(2, lifecycle.snapshot().inFlight(), "parts 2 and 3 are still in flight");
		assertEquals(MPU_OBJECT_PERMITS, freeUploadPermits(), "the upload permit is back");

		complete(upload, parts.get(1), Operation.Status.SUCC);
		assertEquals(OperationLifecycleState.NEW, lifecycle.stateOf(op(parts.get(4))), "part 5 is never queued");
		complete(upload, parts.get(2), Operation.Status.RESP_FAIL_SVC);
		assertTrue(driver.childOpQueue.isEmpty(), () -> "nothing is queued for the settled upload: " + driver.childOpQueue);
		assertTrue(driver.deferred.isEmpty(), "nothing is deferred for the settled upload");
		assertFalse(driver.enqueueChildOperations(List.of(op(parts.get(4))), op(parts.get(1)), "part operation"),
						"a later sibling's enqueue does not claim the settled upload");
		assertEquals(OperationLifecycleState.NEW, lifecycle.stateOf(op(parts.get(4))));

		parts.subList(0, 3).forEach(part -> assertEquals(OperationLifecycleState.TERMINAL, lifecycle.stateOf(op(part)), part::toString));
		assertUnresolvedOnce(upload);
	}

	/** The initiation's answer comes after the stop, which refuses the upload's first parts. */
	@Test
	void anUploadInitiatedAfterTheStopIsUnresolved() throws Exception {
		startDriver(16);
		final var lifecycle = driver.operationLifecycle();
		final var upload = initiationInFlight(5120);
		assertTrue(stop().isEmpty(), "nothing is queued at the stop");

		assertTrue(driver.handleCompleted(op(upload)));

		parts(upload).subList(0, 3).forEach(part -> assertEquals(OperationLifecycleState.UNATTEMPTED, lifecycle.stateOf(op(part)), part::toString));
		assertTrue(driver.childOpQueue.isEmpty());
		assertUnresolvedOnce(upload);
	}

	/**
	 * The initiation is answered before the stop and the first parts are queued, but none is sent: queue
	 * recovery takes them back. The upload is open on the target, so it is unresolved, not unattempted,
	 * and only its parts are returned as unattempted.
	 */
	@Test
	void anInitiatedUploadWhoseQueuedPartsAreRecoveredIsUnresolved() throws Exception {
		startDriver(16);
		final var lifecycle = driver.operationLifecycle();
		final var upload = initiatedUpload();
		final var parts = parts(upload);
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, lifecycle.stateOf(op(upload)));

		final var recovered = stop();

		assertEquals(3, recovered.size(), () -> "the three queued parts: " + recovered);
		parts.subList(0, 3).forEach(part -> assertTrue(recovered.contains(op(part)), part::toString));
		parts.subList(0, 3).forEach(part -> assertEquals(OperationLifecycleState.UNATTEMPTED, lifecycle.stateOf(op(part)), part::toString));
		assertUnresolvedOnce(upload);
	}

	/**
	 * Part 4 waits in the queue when the stop comes, as a part waiting for its NIC's connection does, and
	 * queue recovery takes it back. The upload can no longer complete, so it is unresolved while parts 2
	 * and 3 are still in flight.
	 */
	@Test
	void aDispatchedUploadWhoseQueuedPartIsRecoveredIsUnresolved() throws Exception {
		startDriver(16);
		final var lifecycle = driver.operationLifecycle();
		final var upload = initiatedUpload();
		final var parts = parts(upload);
		for (int i = 0; i < 3; i++) {
			dispatch(parts.get(i));
		}
		complete(upload, parts.get(0), Operation.Status.SUCC);
		assertSame(parts.get(3), driver.childOpQueue.peek());

		assertEquals(List.of(op(parts.get(3))), stop());

		assertEquals(OperationLifecycleState.UNATTEMPTED, lifecycle.stateOf(op(parts.get(3))));
		assertEquals(OperationLifecycleState.UNRESOLVED, lifecycle.stateOf(op(upload)), "the upload settles at recovery");
		complete(upload, parts.get(1), Operation.Status.SUCC);
		complete(upload, parts.get(2), Operation.Status.SUCC);
		assertTrue(driver.childOpQueue.isEmpty());
		assertUnresolvedOnce(upload);
	}

	/** The upload's completion is queued, not sent, when the stop comes; queue recovery takes it back. */
	@Test
	void anUploadWhoseQueuedCompletionIsRecoveredIsUnresolved() throws Exception {
		startDriver(16);
		final var upload = initiatedUpload();
		final var parts = parts(upload);
		for (int i = 0; i < 3; i++) {
			dispatch(parts.get(i));
		}
		for (int i = 0; i < 5; i++) {
			complete(upload, parts.get(i), Operation.Status.SUCC);
			if (i < 2) {
				dispatch(parts.get(i + 3));
			}
		}
		assertSame(upload, driver.childOpQueue.peek(), "the completion is queued");

		assertTrue(stop().isEmpty(), "the dispatched upload is not unattempted");

		assertUnresolvedOnce(upload);
	}

	/**
	 * A full input queue defers the completion's enqueue to the executor, and the deferred enqueue runs
	 * after the stop, which refuses it.
	 */
	@Test
	void anUploadWhoseDeferredCompletionLandsAfterTheStopIsUnresolved() throws Exception {
		startDriver(2);
		final var upload = initiationInFlight(2048);
		assertTrue(driver.handleCompleted(op(upload)));
		final var parts = parts(upload);
		dispatch(parts.get(0));
		dispatch(parts.get(1));
		for (int i = 0; i < 2; i++) {
			final var filler = op(new OperationImpl<>(0, OpType.NOOP, new DataItemImpl("filler-" + i, 0, 1), null, "/bucket", null));
			assertTrue(driver.childOpQueue.offer(filler));
		}
		complete(upload, parts.get(0), Operation.Status.SUCC);
		complete(upload, parts.get(1), Operation.Status.SUCC);
		final var deferredCompletion = Objects.requireNonNull(driver.deferred.poll(), "the completion's enqueue is deferred");
		driver.closeAdmission();

		deferredCompletion.run();

		assertEquals(OperationLifecycleState.UNRESOLVED, driver.operationLifecycle().stateOf(op(upload)));
		driver.recoverQueuedOperations();
		assertUnresolvedOnce(upload);
	}

	/**
	 * Part 2 succeeds and queues part 4; then part 1 runs out of retries and the abort is sent. The stop
	 * comes while the abort is in flight, and queue recovery takes back part 4. The upload is left to its
	 * abort, which ends it with its own result: settling it unresolved then would drop that result.
	 */
	@Test
	void aRecoveredPartOfAnUploadWhoseAbortIsInFlightLeavesTheUploadToItsAbort() throws Exception {
		startDriver(16);
		final var lifecycle = driver.operationLifecycle();
		final var upload = initiatedUpload();
		final var parts = parts(upload);
		for (int i = 0; i < 3; i++) {
			dispatch(parts.get(i));
		}
		complete(upload, parts.get(1), Operation.Status.SUCC);
		for (int i = 0; i < CoopStorageDriverBase.MAX_PART_RETRIES; i++) {
			parts.get(0).incrementRetryCount();
		}
		complete(upload, parts.get(0), Operation.Status.RESP_FAIL_SVC);
		assertEquals("true", upload.get("mpuAbort"));
		assertSame(parts.get(3), driver.childOpQueue.poll(), "part 4 is queued ahead of the abort");
		dispatch(upload);

		assertEquals(List.of(op(parts.get(3))), stop());

		assertEquals(OperationLifecycleState.UNATTEMPTED, lifecycle.stateOf(op(parts.get(3))));
		assertEquals(OperationLifecycleState.DISPATCHED, lifecycle.stateOf(op(upload)), "the abort still owns the upload");
		upload.status(CoopStorageDriverBase.abortedUploadStatus(upload));
		assertTrue(driver.handleCompleted(op(upload)), "the abort ends the upload");
		complete(upload, parts.get(2), Operation.Status.SUCC);
		assertEquals(OperationLifecycleState.TERMINAL, lifecycle.stateOf(op(upload)));
		assertEquals(0, lifecycle.snapshot().unresolved(), () -> lifecycle.snapshot().toString());
		assertFalse(lifecycle.hasOutstandingOperations(), () -> lifecycle.snapshot().toString());
		assertEquals(2, uploadResults.get(), "the upload has its initiation's result and its abort's");
		assertEquals(MPU_OBJECT_PERMITS, freeUploadPermits(), "the upload permit is back, once");
	}

	/** A multi-range read's next range comes after the stop, which refuses it: the read is unresolved at once. */
	@Test
	void aReadWhoseNextRangeTheStopRefusesIsUnresolvedAtOnce() throws Exception {
		startDriver(16);
		final var lifecycle = driver.operationLifecycle();
		final var item = new DataItemImpl("r", 0, 5120);
		item.dataInput(dataInput);
		final var read = new CompositeDataOperationImpl<DataItem>(0, OpType.READ, item, null, "/bucket", null, null, 0, 1024);
		assertTrue(lifecycle.driverQueued(op(read)));
		assertTrue(driver.claimDispatchOwnership(op(read)));
		read.status(Operation.Status.SUCC);
		assertTrue(driver.handleCompleted(op(read)));
		final var ranges = parts(read);
		for (int i = 0; i < 3; i++) {
			dispatch(ranges.get(i));
		}
		assertTrue(stop().isEmpty());

		complete(read, ranges.get(0), Operation.Status.SUCC);
		complete(read, ranges.get(1), Operation.Status.SUCC);
		complete(read, ranges.get(2), Operation.Status.SUCC);

		assertEquals(OperationLifecycleState.UNRESOLVED, lifecycle.stateOf(op(read)));
		final var snapshot = lifecycle.snapshot();
		assertEquals(1, snapshot.unresolved(), snapshot::toString);
		assertEquals(0, snapshot.inFlight(), snapshot::toString);
		assertFalse(lifecycle.hasOutstandingOperations(), snapshot::toString);
		assertEquals(1, uploadResults.get(), "the read has its initial pass's result and no other");
	}
}
