package com.dell.spt.base.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The {@code ItemPath} column of {@code op.trace.csv}, for items with and without the bucket in their
 * name, and the timing columns of an attempt that failed before its request was sent. {@code
 * S3OpTraceRequestPathParityTest} checks the manifest rows against the s3 driver's own request
 * paths.
 */
class OperationTraceRecordTest {

	@Test
	void manifestItemPathNamesItsBucket() {
		final var item = new IntegrityManifestDataItem("bucket-a", "prefix/key", 3, "");

		assertEquals("/bucket-a/prefix/key", path(result(OpType.READ, item, null)));
		assertEquals("/bucket-a/prefix/key", path(result(OpType.DELETE, item, null)));
	}

	@Test
	void manifestPathHasOneSlashBetweenBucketAndKey() {
		// The shapes the manifest reader accepts, written as the s3 driver requests them.
		assertEquals("/bucket-a/key", path(result(OpType.READ, manifestItem("/bucket-a", "key"), null)));
		assertEquals("/bucket-a/key", path(result(OpType.READ, manifestItem("bucket-a", "/key"), null)));
		assertEquals("/bucket-a/key", path(result(OpType.READ, manifestItem("bucket-a/", "key"), null)));
		assertEquals("/bucket-a/key", path(result(OpType.READ, manifestItem("/bucket-a/", "/key"), null)));
	}

	@Test
	void manifestKeyThatHoldsItsBucketIsNotPlacedUnderItAgain() {
		assertEquals("/a/k", path(result(OpType.READ, manifestItem("a", "/a/k"), null)));
		assertEquals("/a/k", path(result(OpType.DELETE, manifestItem("/a", "/a/k"), null)));
	}

	@Test
	void manifestCreateNamesItsDestinationNotTheManifestBucket() {
		final var item = manifestItem("source-bucket", "key");

		assertEquals("/destination-bucket/key", path(result(OpType.CREATE, item, "/destination-bucket")));
		assertEquals("/destination-bucket/key", path(result(OpType.CREATE, item, "destination-bucket")));
		assertEquals("/destination-bucket/out/key", path(result(OpType.CREATE, item, "/destination-bucket/out/")));
		// No output path: the request goes to the key alone, and so does the row.
		assertEquals("/key", path(result(OpType.CREATE, item, null)));
	}

	@Test
	void manifestCreatePathEndsAtItsFirstComma() {
		final var op = new DataOperationImpl<DataItem>(
						0, OpType.CREATE, manifestItem("source-bucket", "key"), null, "/bucket/pre,fix",
						Credential.NONE, null, 0);
		op.nodeAddr("10.0.0.1:9020");
		op.status(Operation.Status.SUCC);

		final String row = new OperationTraceCsvLogMessage<>(op.result()).getFormattedMessage();

		final List<String> columns = List.of(row.strip().split(",", -1));
		assertEquals(9, columns.size(), row);
		assertEquals("/bucket/pre", columns.get(1), row);
	}

	@Test
	void manifestPathIsTheOneTheRecordWasBuiltWith() {
		final var item = new IntegrityManifestDataItem("bucket-a", "key", 3, "");
		final var record = new OperationTraceRecord<>(result(OpType.READ, item, null));

		item.bucket("bucket-b");
		item.name("other-key");

		assertEquals("/bucket-a/key", path(record));
	}

	@Test
	void manifestItemKeepsItsKeyOnlyName() {
		final var item = new IntegrityManifestDataItem("bucket-a", "prefix/key", 3, "");

		path(result(OpType.READ, item, null));

		assertEquals("prefix/key", item.name());
		assertTrue(item.toString().startsWith("prefix/key,"), item.toString());
	}

	@Test
	void plainItemPathIsUnchanged() {
		// A row of an engine items.csv: the name holds the bucket path, which the result restores.
		final var listed = new DataItemImpl("/bucket-a/prefix/key", 0, 3);
		assertEquals("/bucket-a/prefix/key", path(result(OpType.READ, listed, null)));
		// A new object: the name is its id, placed under the output path.
		final var created = new DataItemImpl("k0", 0, 3);
		assertEquals("/bucket-a/k0", path(result(OpType.CREATE, created, "/bucket-a")));
	}

	@Test
	void manifestRowHoldsNamesNotCodes() {
		final var op = new DataOperationImpl<DataItem>(
						0, OpType.READ, new IntegrityManifestDataItem("bucket-a", "key", 3, ""), null, null,
						Credential.NONE, null, 0);
		op.nodeAddr("10.0.0.1:9020");
		op.status(Operation.Status.SUCC);

		final String row = new OperationTraceCsvLogMessage<>(op.result()).getFormattedMessage();

		assertTrue(row.endsWith("\n"), row);
		final List<String> columns = List.of(row.strip().split(",", -1));
		assertEquals(9, columns.size(), row);
		assertEquals(List.of("10.0.0.1:9020", "/bucket-a/key", "READ", "SUCC"), columns.subList(0, 4));
	}

	@Test
	void setupFailedRowHasNoStartAndNoDuration() {
		// NettyStorageDriverBase.complete() on an attempt that failed before startRequest():
		// finishResponse() records the end, then throws (caught by the driver).
		final var op = setupFailed();

		final List<String> columns = columns(op);

		assertEquals(List.of("", "/b/k0", "CREATE", "FAIL_IO", "", "", "", "", "7"), columns);
		assertEquals(columns, batchColumns(op));
	}

	@Test
	void setupFailedRowAfterResetTimingKeepsItsNode() {
		// A composite CREATE's completion request, after resetTiming(), failed before it was sent.
		final var op = timedOp(Operation.Status.SUCC);
		op.resetTiming();
		op.status(Operation.Status.RESP_FAIL_SVC);
		assertThrows(IllegalStateException.class, op::finishResponse);

		final List<String> columns = columns(op);

		assertEquals(List.of("10.0.0.1:9020", "/b/k0", "CREATE", "RESP_FAIL_SVC", "", "", "", "", "7"), columns);
		assertEquals(columns, batchColumns(op));
	}

	@Test
	void sentAttemptsKeepTheirTimes() {
		for (final var status : List.of(Operation.Status.FAIL_IO, Operation.Status.SUCC)) {
			final var op = timedOp(status);
			final List<String> columns = columns(op);
			assertEquals(String.valueOf(op.reqTimeStart()), columns.get(4), status.name());
			assertEquals(String.valueOf(op.duration()), columns.get(5), status.name());
			assertEquals(String.valueOf(op.latency()), columns.get(6), status.name());
			assertEquals(String.valueOf(op.dataLatency()), columns.get(7), status.name());
			assertEquals(columns, batchColumns(op));
		}
	}

	@Test
	void sentAttemptWithNoResponseKeepsItsDuration() {
		// Started and sent, then dropped before any response: finishResponse() throws, but the
		// attempt has a start, so its real duration stays.
		final var op = op();
		op.nodeAddr("10.0.0.1:9020");
		op.startRequest();
		op.finishRequest();
		pause();
		op.status(Operation.Status.FAIL_IO);
		assertThrows(IllegalStateException.class, op::finishResponse);

		final List<String> columns = columns(op);

		assertTrue(op.duration() > 0 && op.duration() < op.reqTimeStart(), op.toString());
		assertEquals(String.valueOf(op.reqTimeStart()), columns.get(4));
		assertEquals(String.valueOf(op.duration()), columns.get(5));
		assertEquals(List.of("", ""), columns.subList(6, 8));
		assertEquals(columns, batchColumns(op));
	}

	private static DataOperationImpl<DataItem> op() {
		final var op = new DataOperationImpl<DataItem>(
						0, OpType.CREATE, new DataItemImpl("k0", 0, 1024), null, "/b", Credential.NONE, null, 0);
		op.countBytesDone(7);
		return op;
	}

	private static DataOperationImpl<DataItem> setupFailed() {
		final var op = op();
		op.status(Operation.Status.FAIL_IO);
		assertThrows(IllegalStateException.class, op::finishResponse);
		return op;
	}

	/** An attempt sent and answered, with every time positive. */
	private static DataOperationImpl<DataItem> timedOp(final Operation.Status status) {
		final var op = op();
		op.nodeAddr("10.0.0.1:9020");
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
		return op;
	}

	/** Lets the microsecond clock advance, so the next time differs from the last. */
	private static void pause() {
		final long until = System.nanoTime() + 20_000;
		while (System.nanoTime() < until) {
			Thread.onSpinWait();
		}
	}

	private static List<String> columns(final DataOperation<DataItem> op) {
		final String row = new OperationTraceCsvLogMessage<>(op.result()).getFormattedMessage();
		return List.of(row.strip().split(",", -1));
	}

	private static List<String> batchColumns(final DataOperation<DataItem> op) {
		final String rows = new OperationTraceCsvBatchLogMessage<>(List.of(op.result()), 0, 1).getFormattedMessage();
		return List.of(rows.strip().split(",", -1));
	}

	private static IntegrityManifestDataItem manifestItem(final String bucket, final String key) {
		return new IntegrityManifestDataItem(bucket, key, 3, "");
	}

	static DataOperation<DataItem> result(final OpType type, final DataItem item, final String dstPath) {
		return new DataOperationImpl<>(0, type, item, null, dstPath, Credential.NONE, null, 0).result();
	}

	private static String path(final DataOperation<DataItem> result) {
		return path(new OperationTraceRecord<>(result));
	}

	private static String path(final OperationTraceRecord<?, ?> record) {
		final var strb = new StringBuilder();
		record.format(strb);
		return strb.toString().split(",", -1)[1];
	}
}
