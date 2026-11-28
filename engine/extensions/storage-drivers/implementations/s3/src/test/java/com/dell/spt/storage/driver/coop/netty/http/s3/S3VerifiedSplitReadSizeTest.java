package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import com.dell.spt.storage.driver.coop.netty.http.HttpResponseHandlerBase;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpObject;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * A verified split read ({@code item.data.verify} with ranges) on the {@code s3} driver whose second
 * range's body ends short. Each range is a part with its own size check; the driver retries a
 * failed part. A part cut short on every attempt fails the read as corrupt; one cut short once is
 * retried and the read succeeds. No request is sent: the test answers each part through the base
 * response handler and the driver's completion.
 */
@Timeout(60)
final class S3VerifiedSplitReadSizeTest {

	private static final int RANGES = 2;
	private static final int RANGE_SIZE = 1024;
	private static final int MAX_PART_RETRIES = 3;
	private static final int SHORT_BY = 100;
	private static final Credential CREDENTIAL = Credential.getInstance("user1", "secret");

	/** Exposes the driver's queue of parts. */
	private static final class Driver extends S3StorageDriver<Item, Operation<Item>> {

		Driver(final Config cfg, final DataInput input) throws Exception {
			super("verified-split-read-size", input, cfg.configVal("storage"), true, cfg.intVal("load-batch-size"));
		}

		boolean initialPass(final Operation<Item> read) {
			return submit(read);
		}

		Operation<Item> dispatchNext() throws InterruptedException {
			final var next = childOpQueue.poll(5, TimeUnit.SECONDS);
			assertTrue(next != null && claimDispatchOwnership(next), () -> "dispatched " + next);
			next.startRequest();
			next.finishRequest();
			return next;
		}

		boolean queueEmpty() {
			return childOpQueue.isEmpty();
		}
	}

	/** The base response handler with verification on and no protocol headers. */
	private static final class Responses extends HttpResponseHandlerBase<Item, Operation<Item>> {

		Responses(final Driver driver) {
			super(driver, true);
		}

		@Override
		protected void handleResponseHeaders(final Channel channel, final Operation<Item> op, final HttpHeaders headers) {}

		void answer(final Operation<Item> op, final byte[] body) throws IOException {
			for (final HttpObject msg : List.of(
							new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK),
							new DefaultHttpContent(Unpooled.wrappedBuffer(body)), LastHttpContent.EMPTY_LAST_CONTENT)) {
				handle(null, op, msg);
			}
		}
	}

	/** Collects every result. */
	private static final class Results implements Output<Operation<Item>> {
		final List<Operation<Item>> results = Collections.synchronizedList(new ArrayList<>());

		@Override
		public boolean put(final Operation<Item> result) {
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

	private DataInput input;
	private Driver driver;
	private Results output;
	private Responses responses;

	@BeforeEach
	void setUp() throws Exception {
		input = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true);
		driver = new Driver(S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1:9"), input);
		output = new Results();
		driver.operationResultOutput(output);
		responses = new Responses(driver);
	}

	@AfterEach
	void close() throws Exception {
		if (driver != null) {
			driver.close();
		}
	}

	@Test
	void aRangeCutShortOnEveryAttemptFailsTheReadAsCorrupt() throws Exception {
		final var read = submitRead();
		answerFirstRangeAndCutTheSecond(read);
		for (var retry = 0; retry < MAX_PART_RETRIES; retry++) {
			final var again = driver.dispatchNext();
			assertEquals(1, ((PartialOperation<?>) again).partNumber());
			assertEquals(retry + 1, ((PartialOperation<?>) again).retryCount());
			responses.answer(again, Arrays.copyOf(body(RANGE_SIZE), RANGE_SIZE - SHORT_BY));
		}

		final var end = readEnd();
		assertEquals(Operation.Status.RESP_FAIL_CORRUPT, end.status());
		assertEquals(Operation.Status.RESP_FAIL_CORRUPT.name(), ((CompositeDataOperation<?>) read).get("mpuFailure"));
		assertTrue(driver.queueEmpty(), "no attempt is left");
	}

	@Test
	void aRangeCutShortOnceIsRetriedAndTheReadSucceeds() throws Exception {
		final var read = submitRead();
		answerFirstRangeAndCutTheSecond(read);
		final var again = driver.dispatchNext();
		assertEquals(1, ((PartialOperation<?>) again).partNumber());
		responses.answer(again, body(RANGE_SIZE));

		final var end = readEnd();
		assertEquals(Operation.Status.SUCC, end.status());
		assertTrue(driver.queueEmpty(), "no attempt is left");
	}

	@SuppressWarnings("unchecked")
	private Operation<Item> submitRead() {
		final var item = new DataItemImpl("cut", 0, RANGES * RANGE_SIZE);
		item.dataInput(input);
		final var read = new CompositeDataOperationImpl<DataItem>(
						0, OpType.READ, item, null, "/bucket", CREDENTIAL, null, 0, RANGE_SIZE);
		final var readOp = (Operation<Item>) (Operation<?>) read;
		assertTrue(driver.operationLifecycle().driverQueued(readOp));
		assertTrue(driver.initialPass(readOp));
		return readOp;
	}

	/** Answers range 0 in full and range 1 short by {@link #SHORT_BY}. */
	private void answerFirstRangeAndCutTheSecond(final Operation<Item> read) throws Exception {
		final var first = driver.dispatchNext();
		final var second = driver.dispatchNext();
		assertEquals(0, ((PartialOperation<?>) first).partNumber());
		responses.answer(first, body(0));
		assertEquals(Operation.Status.SUCC, first.status());
		responses.answer(second, Arrays.copyOf(body(RANGE_SIZE), RANGE_SIZE - SHORT_BY));
		assertEquals(1, output.readResults().size(), "only the initial pass while the second range retries");
	}

	private Operation<Item> readEnd() {
		final var readResults = output.readResults();
		assertEquals(2, readResults.size(), () -> "the initial pass and the read's end: " + readResults);
		return readResults.get(1);
	}

	/** One range's bytes: the item's data from {@code from}. */
	private byte[] body(final long from) throws IOException {
		final var item = new DataItemImpl("cut", 0, RANGES * RANGE_SIZE);
		item.dataInput(input);
		item.position(from);
		final ByteBuffer bb = ByteBuffer.allocate(RANGE_SIZE);
		while (bb.hasRemaining()) {
			item.read(bb);
		}
		return bb.array();
	}
}
