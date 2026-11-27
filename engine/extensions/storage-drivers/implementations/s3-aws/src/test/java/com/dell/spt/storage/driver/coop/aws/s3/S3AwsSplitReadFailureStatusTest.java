package com.dell.spt.storage.driver.coop.aws.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.services.s3.S3AsyncClient;

/**
 * A split read on the {@code s3-aws} driver whose two ranges run out of retries with different
 * failures. The last attempt of the second range ends on another worker, whose result output is
 * held: its range has settled, but its completion has not recorded its failure. The first range's
 * last attempt then completes and finds every range settled. The read must not end there with the
 * first range's failure, which it would then replace with the second's: it ends once the held
 * completion records its failure, with the failure recorded last, as when the two ranges complete
 * one after the other. No request is sent: the test runs each attempt's end as an I/O worker does,
 * the response's end and then the completion.
 */
@Timeout(60)
final class S3AwsSplitReadFailureStatusTest {

	private static final int RANGES = 2;
	private static final long RANGE_SIZE = 1024;
	private static final int MAX_PART_RETRIES = 3;

	/** Exposes the steps an I/O worker runs, and the child queue. */
	private static final class Driver extends S3AwsStorageDriver<Item, Operation<Item>> {

		Driver(final Config storage) throws Exception {
			super("split-read-failure-status",
							DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false),
							storage, false, 16, mock(S3AsyncClient.class), 100 * 1024L, 8 * 1024 * 1024L);
		}

		/** The read's initial pass, which sends no request: its ranges are queued. */
		void initialPass(final Operation<Item> read) {
			((CompositeDataOperation<?>) read).subOperations();
			assertTrue(claimDispatchOwnership(read));
			read.startRequest();
			read.finishRequest();
			read.startResponse();
			read.status(Operation.Status.SUCC);
			assertTrue(handleCompleted(read));
		}

		Operation<Item> dispatchNext() throws InterruptedException {
			final var next = childOpQueue.poll(5, TimeUnit.SECONDS);
			assertTrue(next != null && claimDispatchOwnership(next), () -> "dispatched " + next);
			next.startRequest();
			next.finishRequest();
			next.startResponse();
			return next;
		}

		/** The attempt's response ends with {@code status}, and the worker completes it. */
		boolean end(final Operation<Item> range, final Operation.Status status) {
			range.status(status);
			range.finishResponse();
			return handleCompleted(range);
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
	void aRangeWhoseCompletionIsHeldDecidesTheFailureTheReadEndsWith(@TempDir final Path home) throws Exception {
		driver = new Driver(S3AwsMergedConfig.create(home, 9000, Map.of()).configVal("storage"));
		final var output = new HeldOutput();
		driver.operationResultOutput(output);
		final var item = new DataItemImpl("held", 0, RANGES * RANGE_SIZE);
		final var read = new CompositeDataOperationImpl<DataItem>(
						0, OpType.READ, item, null, "/bucket", S3AwsDriverHarness.CREDENTIAL, null, 0, RANGE_SIZE);
		final var readOp = (Operation<Item>) (Operation<?>) read;
		assertTrue(driver.operationLifecycle().driverQueued(readOp));
		driver.initialPass(readOp);

		var refused = driver.dispatchNext();
		var unavailable = driver.dispatchNext();
		assertEquals(0, ((PartialOperation<?>) refused).partNumber());
		for (var retry = 0; retry < MAX_PART_RETRIES; retry++) {
			assertTrue(driver.end(refused, Operation.Status.RESP_FAIL_AUTH));
			assertTrue(driver.end(unavailable, Operation.Status.RESP_FAIL_SVC));
			refused = driver.dispatchNext();
			unavailable = driver.dispatchNext();
		}
		assertEquals(1, output.readResults().size(), "only the initial pass while the ranges retry");

		// The second range's last attempt ends on another worker, and its output is held.
		output.holdFor = unavailable;
		final var heldRange = unavailable;
		final Future<Boolean> held = completions.submit(() -> driver.end(heldRange, Operation.Status.RESP_FAIL_SVC));
		assertTrue(output.held.await(10, TimeUnit.SECONDS), "the second range's output is held");

		assertTrue(driver.end(refused, Operation.Status.RESP_FAIL_AUTH));

		final var whileHeld = output.readResults();
		output.release.countDown();
		assertTrue(held.get(10, TimeUnit.SECONDS));
		assertEquals(1, whileHeld.size(),
						() -> "the read must not end before the held range records its failure: " + whileHeld);

		final var readResults = output.readResults();
		assertEquals(2, readResults.size(), () -> "the initial pass and the read's end: " + readResults);
		assertEquals(Operation.Status.RESP_FAIL_SVC, readResults.get(1).status(), "the failure recorded last");
		assertSame(Operation.Status.RESP_FAIL_SVC, readOp.status(), "the read's status does not change after its end");
		assertTrue(driver.queueEmpty(), "the read's end takes no place in the queue");
		assertFalse(driver.operationLifecycle().hasOutstandingOperations(),
						() -> "nothing is outstanding: " + driver.operationLifecycle().snapshot());
	}
}
