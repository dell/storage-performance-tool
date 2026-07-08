package com.dell.spt.base.item.op.path;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.op.OpType;
import org.junit.jupiter.api.Test;

/**
 * Test gap (load-op-retry code review): opRetryCount()/result() copy preservation was
 * only exercised against a handful of concrete Operation subclasses, not
 * PathOperationImpl. Its copy constructor does call super(other), so this is mainly
 * regression-proofing for future changes rather than evidence of an existing bug.
 */
class PathOperationImplTest {

	@Test
	void resultCopyPreservesOpRetryCount() {
		final var item = new PathItemImpl("retry-path-obj");
		final var op = new PathOperationImpl<>(0, OpType.LIST, item, null);
		op.incrementOpRetryCount();
		op.incrementOpRetryCount();
		op.startRequest();
		op.finishRequest();
		op.startResponse();

		final var copy = op.result();

		assertEquals(2, copy.opRetryCount(), "PathOperationImpl's result() copy must preserve opRetryCount");
		op.incrementOpRetryCount();
		assertEquals(2, copy.opRetryCount(), "copy opRetryCount must not change when original is mutated afterward");

		op.reset();
		assertEquals(3, op.opRetryCount(), "opRetryCount must survive reset() on the original");
	}
}
