package com.dell.spt.base.item.op.token;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.item.TokenItemImpl;
import com.dell.spt.base.item.op.OpType;
import org.junit.jupiter.api.Test;

/**
 * Test gap (load-op-retry code review): opRetryCount()/result() copy preservation was
 * only exercised against a handful of concrete Operation subclasses, not
 * TokenOperationImpl. Its copy constructor does call super(other), so this is mainly
 * regression-proofing for future changes rather than evidence of an existing bug.
 */
class TokenOperationImplTest {

	@Test
	void resultCopyPreservesOpRetryCount() {
		final var item = new TokenItemImpl("retry-token-obj");
		final var op = new TokenOperationImpl<>(0, OpType.CREATE, item, null);
		op.incrementOpRetryCount();
		op.incrementOpRetryCount();
		op.startRequest();
		op.finishRequest();
		op.startResponse();

		final var copy = op.result();

		assertEquals(2, copy.opRetryCount(), "TokenOperationImpl's result() copy must preserve opRetryCount");
		op.incrementOpRetryCount();
		assertEquals(2, copy.opRetryCount(), "copy opRetryCount must not change when original is mutated afterward");

		op.reset();
		assertEquals(3, op.opRetryCount(), "opRetryCount must survive reset() on the original");
	}
}
