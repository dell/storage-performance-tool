package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A split read on the {@code s3} driver whose two ranges run out of retries with different
 * failures. The last attempt of the second range completes on another thread, whose result output
 * is held: its range has settled, but its completion has not recorded its failure. The first
 * range's last attempt then completes through the driver's completion callback, and finds every
 * range settled. The read must not end there with the first range's failure, which it would then
 * replace with the second's: it ends once the held completion records its failure, with the
 * failure recorded last, as when the two ranges complete one after the other. Under
 * {@code load.op.retry} the status decides whether the whole read is retried: an auth failure is
 * counted, a service failure can be retried. No request is sent: the test drives the completions.
 */
@Timeout(60)
final class S3SplitReadFailureStatusTest {

	private static final int RANGES = 2;
	private static final long RANGE_SIZE = 1024;
	private static final int MAX_PART_RETRIES = 3;
	private static final Credential CREDENTIAL = Credential.getInstance("user1", "secret");

	/** Exposes the completion steps the driver's transport runs, and the child queue. */
	private static final class Driver extends S3StorageDriver<Item, Operation<Item>> {

		Driver(final Config cfg) throws Exception {
			super("split-read-failure-status",
							DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true),
							cfg.configVal("storage"), false, cfg.intVal("load-batch-size"));
		}

		boolean initialPass(final Operation<Item> read) {
			return submit(read);
		}

		Operation<Item> dispatchNext() throws InterruptedException {
			final var next = childOpQueue.poll(5, TimeUnit.SECONDS);
			assertTrue(next != null && claimDispatchOwnership(next), () -> "dispatched " + next);
			next.startRequest();
			next.finishRequest();
			next.startResponse();
			return next;
		}

		boolean queueEmpty() {
			return childOpQueue.isEmpty();
		}
	}

	/** Holds the output of the result named, until released, and collects every result. */
	private static final class HeldOutput implements Output<Operation<Item>> {
		final List<Operation<Item>> results = Collections.synchronizedList(new ArrayList<>());
		final CountDownLatch held = new CountDownLatch(1);
		final CountDownLatch release = new CountDownLatch(1);
		volatile Operation<?> holdFor;

		@Override
		public boolean put(final Operation<Item> result) {
			if (result instanceof PartialOperation<?> range && holdFor instanceof PartialOperation<?> heldRange
							&& range.partNumber() == heldRange.partNumber() && range.status() == holdFor.status()
							&& range.retryCount() == MAX_PART_RETRIES) {
				held.countDown();
				try {
					assertTrue(release.await(30, TimeUnit.SECONDS), "the held output is released");
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new IllegalStateException(e);
				}
			}
			return results.add(result);
		}

		@Override
		public int put(final List<Operation<Item>> buffer, final int from, final int to) {
			for (var i = from; i < to; i++) {
				put(buffer.get(i));
			}
			return to - from;
		}

		@Override
		public int put(final List<Operation<Item>> buffer) {
			return put(buffer, 0, buffer.size());
		}

		@Override
		public Input<Operation<Item>> getInput() {
			return null;
		}

		@Override
		public void close() {}

		List<Operation<Item>> readResults() {
			synchronized (results) {
				return results.stream().filter(result -> result instanceof CompositeDataOperation).toList();
			}
		}
	}

	private final ExecutorService completions = Executors.newSingleThreadExecutor();
	private Driver driver;

	@AfterEach
	void close() throws Exception {
		completions.shutdownNow();
		if (driver != null) {
			driver.close();
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	void aRangeWhoseCompletionIsHeldDecidesTheFailureTheReadEndsWith() throws Exception {
		driver = new Driver(S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1:9"));
		final var output = new HeldOutput();
		driver.operationResultOutput(output);
		final var item = new DataItemImpl("held", 0, RANGES * RANGE_SIZE);
		final var read = new CompositeDataOperationImpl<DataItem>(
						0, OpType.READ, item, null, "/bucket", CREDENTIAL, null, 0, RANGE_SIZE);
		final var readOp = (Operation<Item>) (Operation<?>) read;
		assertTrue(driver.operationLifecycle().driverQueued(readOp));
		assertTrue(driver.initialPass(readOp));

		var refused = driver.dispatchNext();
		var unavailable = driver.dispatchNext();
		assertEquals(0, ((PartialOperation<?>) refused).partNumber());
		for (var retry = 0; retry < MAX_PART_RETRIES; retry++) {
			refused.status(Operation.Status.RESP_FAIL_AUTH);
			driver.complete(null, refused);
			unavailable.status(Operation.Status.RESP_FAIL_SVC);
			driver.complete(null, unavailable);
			refused = driver.dispatchNext();
			unavailable = driver.dispatchNext();
		}
		assertEquals(1, output.readResults().size(), "only the initial pass while the ranges retry");

		// The second range's last attempt completes on another thread, and its output is held.
		unavailable.status(Operation.Status.RESP_FAIL_SVC);
		output.holdFor = unavailable;
		final var heldRange = unavailable;
		final Future<?> held = completions.submit(() -> driver.complete(null, heldRange));
		assertTrue(output.held.await(10, TimeUnit.SECONDS), "the second range's output is held");

		refused.status(Operation.Status.RESP_FAIL_AUTH);
		driver.complete(null, refused);

		final var whileHeld = output.readResults();
		output.release.countDown();
		held.get(10, TimeUnit.SECONDS);
		assertEquals(1, whileHeld.size(),
						() -> "the read must not end before the held range records its failure: " + whileHeld);

		final var readResults = output.readResults();
		assertEquals(2, readResults.size(), () -> "the initial pass and the read's end: " + readResults);
		final var end = readResults.get(1);
		assertEquals(Operation.Status.RESP_FAIL_SVC, end.status(), "the failure recorded last");
		assertSame(Operation.Status.RESP_FAIL_SVC, readOp.status(), "the read's status does not change after its end");
		assertEquals(Operation.Status.RESP_FAIL_SVC.name(), read.get("mpuFailure"));
		assertTrue(driver.queueEmpty(), "the read's end takes no place in the queue");
		assertFalse(driver.operationLifecycle().hasOutstandingOperations(),
						() -> "nothing is outstanding: " + driver.operationLifecycle().snapshot());
	}
}
