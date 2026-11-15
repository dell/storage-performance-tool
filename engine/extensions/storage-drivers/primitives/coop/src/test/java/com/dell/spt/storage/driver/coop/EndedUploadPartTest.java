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
import com.dell.spt.storage.driver.coop.mock.CoopStorageDriverMock;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A part of a multipart upload that reaches the driver's queue or its dispatch after the upload has
 * ended, through the driver's own completion, child-enqueue and dispatch paths. Concurrency is 4, the
 * explicit part window ({@code driver-limit-multipart-parts}) is 3 and the dispatch batch is 1. The
 * completions run in a set order: the order that a step against a target can only arrange by timing.
 */
@SuppressWarnings("unchecked")
final class EndedUploadPartTest {

	/** Defers every child enqueue that the full input queue sends to the executor, and counts refused submits. */
	static final class DeferringDriver extends CoopStorageDriverMock<Item, Operation<Item>> {
		final Queue<Runnable> deferred = new ConcurrentLinkedQueue<>();
		final AtomicLong refused = new AtomicLong();
		/** Runs on the dispatch task before each submit, so a test can hold the task inside it. */
		volatile Consumer<Operation<Item>> beforeSubmit = op -> {};

		DeferringDriver(final DataInput in, final Config cfg) throws Exception {
			super("ended-upload-part-step", in, cfg, false, 1);
		}

		@Override
		protected void executeChildEnqueue(final Runnable task) {
			deferred.add(task);
		}

		@Override
		protected boolean submit(final Operation<Item> op) {
			beforeSubmit.accept(op);
			final boolean accepted = super.submit(op);
			if (!accepted) {
				refused.incrementAndGet();
			}
			return accepted;
		}
	}

	private DataInput dataInput;
	private DeferringDriver driver;
	private final CountDownLatch deleteCompleted = new CountDownLatch(1);
	private final AtomicInteger uploadResults = new AtomicInteger();
	private final AtomicReference<Runnable> onUploadResult = new AtomicReference<>();

	private void startDriver(final int inputQueueLimit) throws Exception {
		dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var cfg = CompositeReadFailureDriverTest.cfg();
		when(cfg.intVal("driver-limit-queue-input")).thenReturn(inputQueueLimit);
		when(cfg.intVal("driver-limit-multipart-parts")).thenReturn(3);
		driver = new DeferringDriver(dataInput, cfg);
		final Output<Operation<Item>> out = mock(Output.class);
		when(out.put(any(Operation.class))).thenAnswer(invocation -> {
			final Operation<?> result = invocation.getArgument(0);
			if (OpType.DELETE.equals(result.type())) {
				deleteCompleted.countDown();
			} else if (result instanceof CompositeOperation) {
				uploadResults.incrementAndGet();
				final var action = onUploadResult.getAndSet(null);
				if (action != null) {
					action.run();
				}
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

	/** A five-part upload whose initiation is answered: the driver has queued its first three parts. */
	private CompositeDataOperationImpl<DataItem> initiatedUpload() {
		final var item = new DataItemImpl("c", 0, 5120);
		item.dataInput(dataInput);
		final var upload = new CompositeDataOperationImpl<DataItem>(0, OpType.CREATE, item, null, "/bucket", null, null, 0, 1024);
		assertTrue(driver.operationLifecycle().driverQueued(op(upload)));
		assertTrue(driver.claimDispatchOwnership(op(upload)));
		upload.status(Operation.Status.SUCC);
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

	/** {@code part} runs out of retries, so the upload is aborted; the abort is dispatched and answered. */
	private void abort(
					final CompositeDataOperationImpl<DataItem> upload, final PartialDataOperationImpl<DataItem> part)
					throws InterruptedException {
		for (int i = 0; i < CoopStorageDriverBase.MAX_PART_RETRIES; i++) {
			part.incrementRetryCount();
		}
		complete(upload, part, Operation.Status.RESP_FAIL_SVC);
		assertEquals("true", upload.get("mpuAbort"));
		dispatch(upload);
		upload.status(CoopStorageDriverBase.abortedUploadStatus(upload));
		assertTrue(driver.handleCompleted(op(upload)), "the abort ends the upload");
	}

	private void assertUploadEndedOnce(final CompositeDataOperationImpl<DataItem> upload) throws InterruptedException {
		final var lifecycle = driver.operationLifecycle();
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (lifecycle.hasOutstandingOperations() && System.nanoTime() < deadline) {
			TimeUnit.MILLISECONDS.sleep(10);
		}
		assertEquals(OperationLifecycleState.TERMINAL, lifecycle.stateOf(op(upload)), "the ended upload is not claimed again");
		assertFalse(lifecycle.hasOutstandingOperations(), () -> "nothing is outstanding: " + lifecycle.snapshot());
		assertEquals(2, uploadResults.get(), "the upload has its initiation's result and its abort's, and no other");
	}

	/**
	 * Sol's review probe. The input queue holds one operation. Part 1 fails with retries left and its
	 * retry fills the queue. Part 3 succeeds and fetches part 4, whose enqueue the full queue defers to
	 * the executor. Part 2 runs out of retries and the abort ends the upload while part 1's retry is in
	 * flight. Only then does the deferred enqueue put part 4 in the queue, already claimed while the upload
	 * was live. Part 1's retry then fails on 404, and an ordinary DELETE is queued behind part 4.
	 *
	 * <p>Part 4 is settled as unattempted when its dispatch is refused, and the DELETE runs. Before, part 4
	 * stayed {@code DRIVER_QUEUED} at the head of the dispatch task's buffer and the task offered it again
	 * in a busy loop, so the DELETE never ran.
	 */
	@Test
	void aDeferredEnqueueThatLandsAfterTheAbortIsSettledAtDispatch() throws Exception {
		startDriver(1);
		final var lifecycle = driver.operationLifecycle();
		final var upload = initiatedUpload();
		final var parts = parts(upload);
		// The queue holds one part: the initial expansion defers parts 2 and 3.
		dispatch(parts.get(0));
		final var expansion = new Thread(Objects.requireNonNull(driver.deferred.poll()), "initial-part-enqueue");
		expansion.start();
		try {
			dispatch(parts.get(1));
			dispatch(parts.get(2));
		} finally {
			expansion.join(TimeUnit.SECONDS.toMillis(6));
		}
		assertFalse(expansion.isAlive());
		assertTrue(driver.deferred.isEmpty());

		complete(upload, parts.get(0), Operation.Status.RESP_FAIL_SVC);
		assertEquals(1, parts.get(0).retryCount());
		complete(upload, parts.get(2), Operation.Status.SUCC);
		final var deferredPartEnqueue = Objects.requireNonNull(driver.deferred.poll(), "part 4's enqueue is deferred");
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, lifecycle.stateOf(op(parts.get(3))));
		dispatch(parts.get(0));
		abort(upload, parts.get(1));
		deferredPartEnqueue.run();
		assertSame(parts.get(3), driver.childOpQueue.peek());
		complete(upload, parts.get(0), Operation.Status.RESP_FAIL_NOT_FOUND);

		driver.start();
		assertTrue(driver.put(new OperationImpl<>(0, OpType.DELETE, new DataItemImpl("after", 0, 1), null, "/bucket", null)));

		assertTrue(deleteCompleted.await(5, TimeUnit.SECONDS), () -> "the DELETE behind part 4 runs; part 4 is "
						+ lifecycle.stateOf(op(parts.get(3))) + " after " + driver.refused.get() + " refused submits");
		assertEquals(OperationLifecycleState.UNATTEMPTED, lifecycle.stateOf(op(parts.get(3))), "part 4 is settled unsent");
		assertUploadEndedOnce(upload);
	}

	/**
	 * Part 2 succeeds and fetches part 4 while the upload is live. Part 1 runs out of retries, and the
	 * abort ends the upload before part 2's completion enqueues part 4. Part 4 is dropped: the ended
	 * upload is not claimed again for it.
	 */
	@Test
	void aPartEnqueuedAfterItsUploadEndedDoesNotClaimTheUpload() throws Exception {
		startDriver(16);
		final var lifecycle = driver.operationLifecycle();
		final var upload = initiatedUpload();
		final var parts = parts(upload);
		for (int i = 0; i < 3; i++) {
			dispatch(parts.get(i));
		}
		// Part 2's success fetches part 4 under the upload's lock, as its completion does.
		upload.markSubTaskCompleted();
		final var fetched = (List<Operation<Item>>) (List<?>) upload.nextSubOperations(1);
		assertSame(parts.get(3), fetched.get(0));

		abort(upload, parts.get(0));
		assertFalse(driver.enqueueChildOperations(fetched, op(parts.get(1)), "part operation"));

		assertEquals(OperationLifecycleState.NEW, lifecycle.stateOf(op(parts.get(3))), "part 4 is never queued");
		assertTrue(driver.childOpQueue.isEmpty(), () -> "nothing is queued after the abort: " + driver.childOpQueue);
		parts.get(1).status(Operation.Status.SUCC);
		assertTrue(driver.handleCompleted(op(parts.get(1))));
		complete(upload, parts.get(2), Operation.Status.RESP_FAIL_NOT_FOUND);
		assertUploadEndedOnce(upload);
	}

	/**
	 * As {@link #aPartEnqueuedAfterItsUploadEndedDoesNotClaimTheUpload}, but part 4's enqueue lands while
	 * the abort's completion is publishing the upload's result, before its terminal transition. Part 4
	 * is dropped, and the abort's completion still ends the upload.
	 */
	@Test
	void aPartEnqueuedWhileTheAbortCompletesDoesNotClaimTheUpload() throws Exception {
		startDriver(16);
		final var lifecycle = driver.operationLifecycle();
		final var upload = initiatedUpload();
		final var parts = parts(upload);
		for (int i = 0; i < 3; i++) {
			dispatch(parts.get(i));
		}
		// Part 2's success fetches part 4 under the upload's lock, as its completion does.
		upload.markSubTaskCompleted();
		final var fetched = (List<Operation<Item>>) (List<?>) upload.nextSubOperations(1);
		final var enqueued = new AtomicReference<Boolean>();
		final var uploadStateAtEnqueue = new AtomicReference<OperationLifecycleState>();
		onUploadResult.set(() -> {
			uploadStateAtEnqueue.set(lifecycle.stateOf(op(upload)));
			enqueued.set(driver.enqueueChildOperations(fetched, op(parts.get(1)), "part operation"));
		});

		abort(upload, parts.get(0));

		assertEquals(OperationLifecycleState.COMPLETING, uploadStateAtEnqueue.get());
		assertEquals(Boolean.FALSE, enqueued.get());
		assertEquals(OperationLifecycleState.NEW, lifecycle.stateOf(op(parts.get(3))), "part 4 is never queued");
		assertTrue(driver.childOpQueue.isEmpty(), () -> "nothing is queued after the abort: " + driver.childOpQueue);
		parts.get(1).status(Operation.Status.SUCC);
		assertTrue(driver.handleCompleted(op(parts.get(1))));
		complete(upload, parts.get(2), Operation.Status.RESP_FAIL_NOT_FOUND);
		assertUploadEndedOnce(upload);
	}

	/**
	 * Part 3 is still queued, already claimed, when the abort's completion starts to publish the upload's
	 * result, with the upload {@code COMPLETING}. The dispatch task meets part 3 ahead of an ordinary
	 * DELETE while that result is held. Part 3 is settled unsent at its first refusal and the DELETE runs
	 * before the result is released. Before, part 3 stayed {@code DRIVER_QUEUED} and the task offered it
	 * again in a busy loop for as long as the result's publication took.
	 */
	@Test
	void aQueuedPartIsSettledWhileTheAbortsResultIsPublished() throws Exception {
		startDriver(16);
		final var lifecycle = driver.operationLifecycle();
		final var upload = initiatedUpload();
		final var parts = parts(upload);
		dispatch(parts.get(0));
		dispatch(parts.get(1));
		// Part 3 is taken out of the queue so that the abort is dispatched ahead of it, as a deferred
		// enqueue can arrange; it goes back once the abort is publishing.
		assertSame(parts.get(2), driver.childOpQueue.poll());
		final var publishing = new CountDownLatch(1);
		final var releaseResult = new CountDownLatch(1);
		onUploadResult.set(() -> {
			publishing.countDown();
			try {
				assertTrue(releaseResult.await(10, TimeUnit.SECONDS));
			} catch (final InterruptedException e) {
				throw new AssertionError(e);
			}
		});
		final var abortFailure = new AtomicReference<Throwable>();
		final var abortThread = new Thread(() -> {
			try {
				abort(upload, parts.get(0));
			} catch (final Throwable t) {
				abortFailure.set(t);
			}
		}, "abort-result");
		abortThread.start();
		final boolean deleteRanWhilePublishing;
		try {
			assertTrue(publishing.await(5, TimeUnit.SECONDS));
			assertEquals(OperationLifecycleState.COMPLETING, lifecycle.stateOf(op(upload)));
			assertTrue(driver.childOpQueue.offer(op(parts.get(2))));
			driver.start();
			assertTrue(driver.put(new OperationImpl<>(0, OpType.DELETE, new DataItemImpl("after", 0, 1), null, "/bucket", null)));
			deleteRanWhilePublishing = deleteCompleted.await(2, TimeUnit.SECONDS);
		} finally {
			releaseResult.countDown();
			abortThread.join(TimeUnit.SECONDS.toMillis(11));
		}
		assertFalse(abortThread.isAlive());
		assertEquals(null, abortFailure.get());
		assertTrue(deleteRanWhilePublishing, () -> "the DELETE runs while the abort's result is held; part 3 is "
						+ lifecycle.stateOf(op(parts.get(2))) + " after " + driver.refused.get() + " refused submits");
		assertEquals(1, driver.refused.get(), "part 3 is refused once");
		assertEquals(OperationLifecycleState.UNATTEMPTED, lifecycle.stateOf(op(parts.get(2))), "part 3 is settled unsent");
		complete(upload, parts.get(1), Operation.Status.RESP_FAIL_NOT_FOUND);
		assertUploadEndedOnce(upload);
	}

	/**
	 * The upload is also {@code COMPLETING} while its initiation's result is published, before its first
	 * parts are queued. A part that is queued and offered to dispatch then is refused but not settled: the
	 * upload is live. Once the initiation's completion ends, every part is sent, the upload's completion
	 * follows, and the upload ends with its completion's result.
	 */
	@Test
	void aPartOfALiveUploadOfferedWhileItsInitiationCompletesIsStillSent() throws Exception {
		startDriver(16);
		final var lifecycle = driver.operationLifecycle();
		final var item = new DataItemImpl("c", 0, 5120);
		item.dataInput(dataInput);
		final var upload = new CompositeDataOperationImpl<DataItem>(0, OpType.CREATE, item, null, "/bucket", null, null, 0, 1024);
		final var parts = parts(upload);
		final var stateAtOffer = new AtomicReference<OperationLifecycleState>();
		final var claimedAtOffer = new AtomicReference<Boolean>();
		final var partStateAfterOffer = new AtomicReference<OperationLifecycleState>();
		onUploadResult.set(() -> {
			stateAtOffer.set(lifecycle.stateOf(op(upload)));
			assertTrue(lifecycle.driverQueued(op(parts.get(0))));
			claimedAtOffer.set(driver.claimDispatchOwnership(op(parts.get(0))));
			partStateAfterOffer.set(lifecycle.stateOf(op(parts.get(0))));
		});
		assertTrue(lifecycle.driverQueued(op(upload)));
		assertTrue(driver.claimDispatchOwnership(op(upload)));
		upload.status(Operation.Status.SUCC);
		assertTrue(driver.handleCompleted(op(upload)));

		assertEquals(OperationLifecycleState.COMPLETING, stateAtOffer.get());
		assertEquals(Boolean.FALSE, claimedAtOffer.get(), "the part is refused while the initiation completes");
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, partStateAfterOffer.get(), "the part is not settled");
		for (int i = 0; i < 3; i++) {
			dispatch(parts.get(i));
		}
		for (int i = 0; i < 5; i++) {
			complete(upload, parts.get(i), Operation.Status.SUCC);
			if (i < 2) {
				dispatch(parts.get(i + 3));
			}
		}
		dispatch(upload);
		upload.status(Operation.Status.SUCC);
		assertTrue(driver.handleCompleted(op(upload)), "the upload's completion ends it");
		parts.forEach(part -> assertEquals(OperationLifecycleState.TERMINAL, lifecycle.stateOf(op(part)), part::toString));
		assertEquals(0, lifecycle.snapshot().unattempted(), "nothing was settled unsent");
		assertUploadEndedOnce(upload);
	}
}
