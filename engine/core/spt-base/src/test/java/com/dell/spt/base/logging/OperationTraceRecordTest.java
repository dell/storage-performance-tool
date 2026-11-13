package com.dell.spt.base.logging;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

/** The {@code path} column of {@code op.trace.csv}, for items with and without the bucket in their name. */
class OperationTraceRecordTest {

	@Test
	void manifestItemPathNamesItsBucket() {
		final var item = new IntegrityManifestDataItem("bucket-a", "prefix/key", 3, "");

		assertEquals("/bucket-a/prefix/key", path(result(OpType.READ, item, null)));
		assertEquals("/bucket-a/prefix/key", path(result(OpType.DELETE, item, null)));
	}

	@Test
	void manifestBucketThatStartsWithASlashGetsOneSlash() {
		final var item = new IntegrityManifestDataItem("/bucket-a", "prefix/key", 3, "");

		assertEquals("/bucket-a/prefix/key", path(result(OpType.READ, item, null)));
	}

	@Test
	void manifestBucketIsTheOneTheRecordWasBuiltWith() {
		final var item = new IntegrityManifestDataItem("bucket-a", "key", 3, "");
		final var record = new OperationTraceRecord<>(result(OpType.READ, item, null));

		item.bucket("bucket-b");

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
