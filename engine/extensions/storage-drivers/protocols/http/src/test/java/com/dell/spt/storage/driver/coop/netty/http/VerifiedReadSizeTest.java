package com.dell.spt.storage.driver.coop.netty.http;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperation;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.collection.Range;
import com.github.akurilov.commons.system.SizeInBytes;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.LastHttpContent;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;

/**
 * A verified READ ({@code item.data.verify}) ends as {@code RESP_FAIL_CORRUPT} when its body ends at a
 * size other than the item's, or the requested ranges', and passes at exactly that size.
 */
class VerifiedReadSizeTest {

	private static final int SIZE = 1024;
	private static final int SHORT_BY = 100;

	/** A read's outcome: the status it ends with and the bytes it counted. */
	private record Outcome(Operation.Status status, long countBytesDone) {}

	private DataInput input;
	private HttpStorageDriverBase<DataItem, DataOperation<DataItem>> driver;

	@BeforeEach
	@SuppressWarnings("unchecked")
	void setUp() throws Exception {
		input = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false);
		driver = mock(HttpStorageDriverBase.class);
	}

	@AfterEach
	void tearDown() throws Exception {
		input.close();
	}

	// ---------- whole object ----------

	@ParameterizedTest(name = "{0} chunk(s)")
	@ValueSource(ints = {1, 2
	})
	void aWholeObjectReadEndingShortIsCorrupt(final int chunks) throws Exception {
		final var item = item(SIZE);
		assertEquals(Operation.Status.RESP_FAIL_CORRUPT,
						read(true, readOp(item), bytes(item, 0, SIZE - SHORT_BY), chunks).status());
	}

	@Test
	void aWholeObjectReadWithAnEmptyBodyIsCorrupt() throws Exception {
		final var item = item(SIZE);
		assertEquals(new Outcome(Operation.Status.RESP_FAIL_CORRUPT, 0), read(true, readOp(item), new byte[0], 0));
	}

	@ParameterizedTest(name = "{0} chunk(s)")
	@ValueSource(ints = {1, 2
	})
	void aWholeObjectReadOfExactlyTheItemSucceeds(final int chunks) throws Exception {
		final var item = item(SIZE);
		assertEquals(new Outcome(Operation.Status.SUCC, SIZE), read(true, readOp(item), bytes(item, 0, SIZE), chunks));
	}

	@Test
	void anUnverifiedReadEndingShortStillSucceeds() throws Exception {
		final var item = item(SIZE);
		assertEquals(new Outcome(Operation.Status.SUCC, SIZE - SHORT_BY),
						read(false, readOp(item), bytes(item, 0, SIZE - SHORT_BY), 2));
	}

	@Test
	void aZeroByteItemWithAnEmptyBodySucceeds() throws Exception {
		final var item = item(0);
		assertEquals(new Outcome(Operation.Status.SUCC, 0), read(true, readOp(item), new byte[0], 0));
	}

	// ---------- fixed and random ranges ----------

	@Test
	void aFixedRangeReadEndingShortIsCorrupt() throws Exception {
		final var item = item(SIZE);
		final var op = readOp(item, List.of(new Range(900, 1023, -1)), 0);
		assertEquals(Operation.Status.RESP_FAIL_CORRUPT, read(true, op, bytes(item, 900, 24), 1).status());
	}

	@Test
	void aFixedRangeReadOfExactlyTheRangeSucceeds() throws Exception {
		final var item = item(SIZE);
		final var op = readOp(item, List.of(new Range(900, 1023, -1)), 0);
		assertEquals(new Outcome(Operation.Status.SUCC, 124), read(true, op, bytes(item, 900, 124), 2));
	}

	@Test
	void aSuffixRangeReadEndingShortIsCorrupt() throws Exception {
		final var item = item(SIZE);
		final var op = readOp(item, List.of(new Range(-1, 512, -1)), 0);
		assertEquals(Operation.Status.RESP_FAIL_CORRUPT, read(true, op, bytes(item, 512, 256), 1).status());
	}

	/** Cut at a range boundary the count is the next range's, 0, so this is the shape most easily missed. */
	@Test
	void aMultiRangeReadCutAtARangeBoundaryIsCorrupt() throws Exception {
		final var item = item(SIZE);
		final var op = readOp(item, List.of(new Range(0, 99, -1), new Range(200, 299, -1)), 0);
		assertEquals(Operation.Status.RESP_FAIL_CORRUPT, read(true, op, bytes(item, 0, 100), 1).status());
	}

	@Test
	void aMultiRangeReadOfExactlyItsRangesSucceeds() throws Exception {
		final var item = item(SIZE);
		final var op = readOp(item, List.of(new Range(0, 99, -1), new Range(200, 299, -1)), 0);
		final var body = new ByteArrayOutputStream();
		body.write(bytes(item, 0, 100));
		body.write(bytes(item, 200, 100));
		assertEquals(new Outcome(Operation.Status.SUCC, 200), read(true, op, body.toByteArray(), 2));
	}

	@Test
	void aRandomRangeReadEndingShortIsCorrupt() throws Exception {
		final var item = item(SIZE);
		final var op = readOp(item, null, 3);
		final byte[] body = markedRangesBody(item, op);
		final byte[] cut = java.util.Arrays.copyOf(body, body.length / 2);
		assertEquals(Operation.Status.RESP_FAIL_CORRUPT, read(true, op, cut, 1).status());
	}

	@Test
	void aRandomRangeReadOfExactlyItsRangesSucceeds() throws Exception {
		final var item = item(SIZE);
		final var op = readOp(item, null, 3);
		final byte[] body = markedRangesBody(item, op);
		assertEquals(new Outcome(Operation.Status.SUCC, op.markedRangesSize()), read(true, op, body, 2));
	}

	// ---------- updated item ----------

	@Test
	void anUpdatedItemReadOfExactlyTheItemSucceeds() throws Exception {
		final var item = updatedItem(SIZE);
		assertEquals(new Outcome(Operation.Status.SUCC, SIZE), read(true, readOp(item), bytes(item, 0, SIZE), 2));
	}

	@Test
	void anUpdatedItemReadEndingShortIsCorrupt() throws Exception {
		final var item = updatedItem(SIZE);
		assertEquals(Operation.Status.RESP_FAIL_CORRUPT,
						read(true, readOp(item), bytes(item, 0, SIZE - SHORT_BY), 1).status());
	}

	/**
	 * Bytes past the end of an updated item that continue its last cell's data are not caught per
	 * chunk: the chunk check fires only at the next cell's offset (2047 here). The end-of-body
	 * check does.
	 */
	@Test
	void anUpdatedItemReadOfValidBytesPastItsEndIsCorrupt() throws Exception {
		final var item = updatedItem(SIZE);
		assertEquals(Operation.Status.RESP_FAIL_CORRUPT, read(true, readOp(item), bytes(item, 0, 1500), 2).status());
	}

	// ---------- statuses and modes the check leaves alone ----------

	@Test
	void aRefusedVerifiedReadKeepsItsHttpStatus() throws Exception {
		final var item = item(SIZE);
		assertEquals(new Outcome(Operation.Status.RESP_FAIL_NOT_FOUND, 0),
						read(true, readOp(item), HttpResponseStatus.NOT_FOUND, new byte[0], 0));
		assertEquals(new Outcome(Operation.Status.RESP_FAIL_SVC, 0),
						read(true, readOp(item), HttpResponseStatus.SERVICE_UNAVAILABLE, new byte[0], 0));
	}

	/** {@code storage.net.http.read-metadata-only} sends HEAD: no body is expected. */
	@Test
	void aMetadataOnlyVerifiedReadIsNotFailedByTheCheck() throws Exception {
		final Field field = HttpStorageDriverBase.class.getDeclaredField("readMetadataOnly");
		field.setAccessible(true);
		field.setBoolean(driver, true);
		final var item = item(SIZE);
		assertEquals(new Outcome(Operation.Status.SUCC, 0), read(true, readOp(item), new byte[0], 0));
	}

	/** An RDMA-accepted GET delivers its body outside HTTP; the transport checks its length. */
	@Test
	void aReadWhoseBodyArrivesOutOfBandIsNotFailedByTheCheck() throws Exception {
		final var item = item(SIZE);
		final var op = readOp(item);
		when(driver.observesReadBodyOutOfBand(op)).thenReturn(true);
		assertEquals(new Outcome(Operation.Status.SUCC, 0), read(true, op, new byte[0], 0));
	}

	/** A GET the RDMA server declined, served over HTTP, is checked like any HTTP read. */
	@Test
	void anEmptyInBandBodyOfADriverWithAnOutOfBandRouteIsCorrupt() throws Exception {
		final var item = item(SIZE);
		final var op = readOp(item);
		when(driver.observesReadBodyOutOfBand(op)).thenReturn(false);
		assertEquals(new Outcome(Operation.Status.RESP_FAIL_CORRUPT, 0), read(true, op, new byte[0], 0));
	}

	/** Some handler tests build a handler without a driver. */
	@Test
	void aVerifyingHandlerWithoutADriverCanBeBuiltAndCountsChunks() throws Exception {
		final var item = item(SIZE);
		final var op = readOp(item);
		final HttpResponseHandlerBase<DataItem, DataOperation<DataItem>> handler = assertDoesNotThrow(
						() -> handler(null, true));
		handler.handle(mock(Channel.class), op, new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK));
		handler.handle(mock(Channel.class), op, new DefaultHttpContent(Unpooled.wrappedBuffer(bytes(item, 0, 10))));
		assertEquals(new Outcome(Operation.Status.SUCC, 10), new Outcome(op.status(), op.countBytesDone()));
	}

	// ---------- split read parts ----------

	@Test
	void aSplitReadPartEndingShortIsCorruptAndItsRetryCanRecover() throws Exception {
		final var item = item(SIZE);
		final var parent = new CompositeDataOperationImpl<DataItem>(
						0, OpType.READ, item, null, "/bucket/", Credential.NONE, null, 0, 512);
		final var part = (PartialDataOperationImpl<DataItem>) parent.subOperations().get(1);
		assertEquals(512, part.item().size());
		final byte[] partBody = bytes(item, 512, 512);

		assertEquals(Operation.Status.RESP_FAIL_CORRUPT,
						read(true, part, java.util.Arrays.copyOf(partBody, 256), 1).status());

		// what CoopStorageDriverBase does before a part retry
		part.status(Operation.Status.PENDING);
		part.resetTiming();
		part.item().reset();
		part.resetAttemptProgress();
		assertEquals(new Outcome(Operation.Status.SUCC, 512), read(true, part, partBody, 2));
	}

	@Test
	void aSplitReadPartOfExactlyThePartSucceeds() throws Exception {
		final var item = item(SIZE);
		final var parent = new CompositeDataOperationImpl<DataItem>(
						0, OpType.READ, item, null, "/bucket/", Credential.NONE, null, 0, 512);
		final PartialDataOperation<DataItem> part = parent.subOperations().get(0);
		assertEquals(new Outcome(Operation.Status.SUCC, 512), read(true, part, bytes(item, 0, 512), 1));
	}

	// ---------- helpers ----------

	private DataItemImpl item(final long size) {
		final var item = new DataItemImpl("k", 0, size);
		item.dataInput(input);
		return item;
	}

	/** An item whose cell 3 (bytes 7 to 14) holds the next layer's data. */
	private DataItemImpl updatedItem(final long size) {
		final var item = item(size);
		final var updated = new BitSet();
		updated.set(3);
		item.commitUpdatedRanges(new BitSet[]{updated, new BitSet()
		});
		return item;
	}

	private static DataOperationImpl<DataItem> readOp(final DataItem item) {
		return readOp(item, null, 0);
	}

	private static DataOperationImpl<DataItem> readOp(
					final DataItem item, final List<Range> fixedRanges, final int randomRanges) {
		return new DataOperationImpl<>(0, OpType.READ, item, null, "/bucket/", Credential.NONE, fixedRanges, randomRanges);
	}

	/**
	 * {@code len} bytes of {@code item} from {@code from}, cell by cell as the verifier reads them,
	 * continuing the last cell's data past the item's end.
	 */
	private static byte[] bytes(final DataItem item, final long from, final int len) throws Exception {
		final ByteBuffer out = ByteBuffer.allocate(len);
		long pos = from;
		while (out.hasRemaining()) {
			final int n = 63 - Long.numberOfLeadingZeros(pos + 1);
			final long cellOffset = DataItem.rangeOffset(n);
			final DataItem cell = item.slice(cellOffset, Math.max(1, item.rangeSize(n)));
			if (item.isRangeUpdated(n)) {
				cell.layer(item.layer() + 1);
			}
			cell.position(pos - cellOffset);
			final int count = (int) Math.min(out.remaining(), DataItem.rangeOffset(n + 1) - pos);
			final ByteBuffer dst = out.slice(out.position(), count);
			while (dst.hasRemaining()) {
				cell.read(dst);
			}
			out.position(out.position() + count);
			pos += count;
		}
		return out.array();
	}

	/** The body a random-range read of {@code item} asks for: its marked cells in order. */
	private static byte[] markedRangesBody(final DataItem item, final DataOperation<DataItem> op) throws Exception {
		final BitSet marked = op.markedRangesMaskPair()[0];
		final var body = new ByteArrayOutputStream();
		for (int i = marked.nextSetBit(0); i >= 0; i = marked.nextSetBit(i + 1)) {
			body.write(bytes(item, DataItem.rangeOffset(i), (int) item.rangeSize(i)));
		}
		return body.toByteArray();
	}

	private static HttpResponseHandlerBase<DataItem, DataOperation<DataItem>> handler(
					final HttpStorageDriverBase<DataItem, DataOperation<DataItem>> driver, final boolean verify) {
		return new HttpResponseHandlerBase<>(driver, verify) {
			@Override
			protected void handleResponseHeaders(
							final Channel channel, final DataOperation<DataItem> operation, final HttpHeaders headers) {}
		};
	}

	private Outcome read(final boolean verify, final DataOperation<DataItem> op, final byte[] body, final int chunks)
					throws Exception {
		return read(verify, op, HttpResponseStatus.OK, body, chunks);
	}

	/** Answers {@code op} through {@link HttpResponseHandlerBase#handle} with {@code body} in {@code chunks}. */
	@SuppressWarnings("unchecked")
	private Outcome read(
					final boolean verify, final DataOperation<DataItem> op, final HttpResponseStatus status,
					final byte[] body, final int chunks) throws Exception {
		op.startRequest();
		op.finishRequest();
		final var handler = handler(driver, verify);
		final Channel channel = mock(Channel.class);
		handler.handle(channel, op, new DefaultHttpResponse(HttpVersion.HTTP_1_1, status));
		int offset = 0;
		for (int i = 0; i < chunks; i++) {
			final int end = i == chunks - 1 ? body.length : body.length * (i + 1) / chunks;
			handler.handle(channel, op, new DefaultHttpContent(Unpooled.wrappedBuffer(body, offset, end - offset)));
			offset = end;
		}
		handler.handle(channel, op, LastHttpContent.EMPTY_LAST_CONTENT);
		verify(driver).complete(channel, op);
		return new Outcome(op.status(), op.countBytesDone());
	}
}
