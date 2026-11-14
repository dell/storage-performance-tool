package com.dell.spt.base.logging;

import static com.dell.spt.base.logging.OperationTraceRecordTest.result;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.data.DataOperation;
import java.util.List;
import org.junit.jupiter.api.Test;

class OperationTraceCsvBatchLogMessageTest {

	@Test
	void eachRowOfABatchNamesTheBucketOfItsOwnItem() {
		final List<DataOperation<DataItem>> results = List.of(
						result(OpType.READ, new DataItemImpl("/bucket-c/before", 0, 1), null),
						result(OpType.READ, new IntegrityManifestDataItem("bucket-a", "k0", 1, ""), null),
						result(OpType.READ, new IntegrityManifestDataItem("bucket-b", "k1", 1, ""), null),
						result(OpType.READ, new DataItemImpl("/bucket-c/k2", 0, 1), null),
						result(OpType.READ, new IntegrityManifestDataItem("bucket-a", "after", 1, ""), null));

		final String rows = new OperationTraceCsvBatchLogMessage<>(results, 1, 4).getFormattedMessage();

		assertEquals(
						List.of("/bucket-a/k0", "/bucket-b/k1", "/bucket-c/k2"),
						rows.lines().map(row -> row.split(",", -1)[1]).toList());
	}
}
