package com.dell.spt.base.item.op.list;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.item.PathItem;
import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.storage.driver.ListOptions;
import org.junit.jupiter.api.Test;

class ListOperationImplTest {

	@Test
	void resultCopiesStartAfterAndOptions() {
		final PathItem item = new PathItemImpl("prefix/object");
		final var original = new ListOperationImpl<PathItem>(0, OpType.LIST, item, null);
		final var options = ListOptions.builder().startAfter("key-42").build();
		original.options(options);
		original.startAfter("key-42");
		original.continuationToken("token-xyz");
		final var shard = new com.dell.spt.base.item.op.list.shard.ListShard("prefix-a", null, null, null);
		original.shard(shard);
		final var copy = original.result();
		assertEquals("key-42", copy.startAfter());
		assertEquals("token-xyz", copy.continuationToken());
		assertEquals(options, copy.options());
		assertNotSame(shard, copy.shard());
		assertEquals("prefix-a", copy.shard().prefix());
		assertEquals("token-xyz", copy.shard().continuationToken());
		assertEquals("key-42", copy.shard().startAfter());
	}

	@Test
	void resetClearsCountersButKeepsShardCursor() {
		final var op = new ListOperationImpl<PathItem>(0, OpType.LIST, new PathItemImpl("foo"), null);
		op.objectsListed(10);
		op.bytesListed(512);
		op.truncated(true);
		op.startAfter("foo");
		op.continuationToken("token-1");
		final var shard = new com.dell.spt.base.item.op.list.shard.ListShard("prefix", null, "token-1", "foo");
		op.shard(shard);
		op.reset();
		assertEquals(0, op.objectsListed());
		assertEquals(0, op.bytesListed());
		assertFalse(op.truncated());
		assertEquals("foo", op.startAfter());
		assertEquals("token-1", op.continuationToken());
		assertEquals("foo", op.shard().startAfter());
		assertEquals("token-1", op.shard().continuationToken());
	}

	@Test
	void resultCopyPreservesOpRetryCount() {
		// load-op-retry's bounded check reads whatever concrete Operation subclass a
		// driver actually produces, not just base OperationImpl - ListOperationImpl's own
		// copy constructor/result() must still correctly delegate to the base class's
		// opRetryCount handling rather than, say, forgetting to call super(other). LIST
		// workloads use load-op-retry too (e.g. a transient failure mid-listing).
		final var op = new ListOperationImpl<PathItem>(0, OpType.LIST, new PathItemImpl("retry-list-obj"), null);
		op.incrementOpRetryCount();
		op.incrementOpRetryCount();
		op.startRequest();
		op.finishRequest();
		op.startResponse();
		op.finishResponse();

		final var copy = op.result();

		assertEquals(2, copy.opRetryCount(), "ListOperationImpl's result() copy must preserve opRetryCount");
		op.incrementOpRetryCount();
		assertEquals(2, copy.opRetryCount(), "copy opRetryCount must not change when original is mutated afterward");

		op.reset();
		assertEquals(3, op.opRetryCount(), "opRetryCount must survive reset() on the original");
	}
}
