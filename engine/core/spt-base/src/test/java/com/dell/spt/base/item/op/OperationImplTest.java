package com.dell.spt.base.item.op;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.item.ItemImpl;
import org.junit.jupiter.api.Test;

class OperationImplTest {

	@Test
	void create_doesNotDeriveSrcPathFromKey() {
		final var item = new ItemImpl("logs/AA/file001");
		final var op = new OperationImpl<>(0, OpType.CREATE, item, null, null, null);
		assertNull(op.srcPath(), "CREATE must not auto-derive srcPath from item name");
		assertEquals("logs/AA/file001", item.name(), "Item name must remain intact for CREATE");
	}

	@Test
	void nonCreate_derivesSrcPathFromKey() {
		final var item = new ItemImpl("bucket/prefix/file002");
		final var op = new OperationImpl<>(0, OpType.READ, item, null, null, null);
		assertEquals("bucket/prefix", op.srcPath(), "READ should derive srcPath from the item name prefix");
		assertEquals("file002", item.name(), "Item name tail should remain as object key");
	}

	@Test
	void hashCodeReflectsOpType() {
		final var baseName = "object.bin";
		final var createOp = new OperationImpl<>(7, OpType.CREATE, new ItemImpl(baseName), null, null, null);
		final var readOp = new OperationImpl<>(7, OpType.READ, new ItemImpl(baseName), null, null, null);
		assertNotEquals(
						createOp.hashCode(),
						readOp.hashCode(),
						"hashCode should change when the operation type differs");
	}

	@Test
	void hashCodeReflectsItemIdentity() {
		final var opA = new OperationImpl<>(3, OpType.READ, new ItemImpl("a.txt"), null, null, null);
		final var opB = new OperationImpl<>(3, OpType.READ, new ItemImpl("b.txt"), null, null, null);
		assertNotEquals(
						opA.hashCode(),
						opB.hashCode(),
						"hashCode should incorporate the item identity");
	}

	@Test
	void hashCodeStableAcrossCopy() {
		final var original = new OperationImpl<>(11, OpType.UPDATE, new ItemImpl("payload"), null, null, null);
		final var clone = original.result();
		assertEquals(
						original.hashCode(),
						clone.hashCode(),
						"Copying the operation should preserve the hashCode");
	}

	@Test
	void latencyReturnsZeroWhenReqTimeDoneNotSet() {
		final var op = new OperationImpl<>(0, OpType.CREATE, new ItemImpl("item"), null, null, null);
		// Only startRequest + startResponse, no finishRequest
		op.startRequest();
		op.startResponse();
		op.finishResponse();
		assertEquals(0, op.latency(), "latency must be 0 when finishRequest was never called");
	}

	@Test
	void latencyReturnsZeroWhenRespTimeStartNotSet() {
		final var op = new OperationImpl<>(0, OpType.CREATE, new ItemImpl("item"), null, null, null);
		// Only startRequest + finishRequest, no startResponse
		op.startRequest();
		op.finishRequest();
		assertEquals(0, op.latency(), "latency must be 0 when startResponse was never called");
	}

	@Test
	void latencyReturnsPositiveWhenBothTimestampsSet() {
		final var op = new OperationImpl<>(0, OpType.READ, new ItemImpl("item"), null, null, null);
		op.startRequest();
		op.finishRequest();
		op.startResponse();
		op.finishResponse();
		assertTrue(op.latency() >= 0, "latency must be non-negative when both timestamps are set");
		assertTrue(op.latency() < 1_000_000, "latency must be reasonable (< 1 second) for in-JVM timing");
	}

	@Test
	void durationReturnsPositiveAfterFullLifecycle() {
		final var op = new OperationImpl<>(0, OpType.READ, new ItemImpl("item"), null, null, null);
		op.startRequest();
		op.finishRequest();
		op.startResponse();
		op.finishResponse();
		assertTrue(op.duration() >= 0, "duration must be non-negative");
		assertTrue(op.duration() < 1_000_000, "duration must be reasonable (< 1 second) for in-JVM timing");
	}

	/**
	 * Invariant: latency() must never return an epoch-scale timestamp.
	 * <p>
	 * Before the fix, when finishRequest() was not called (reqTimeDone == 0), latency()
	 * returned {@code respTimeStart - 0} which is an absolute wall-clock timestamp in
	 * microseconds (~1.77e15). This test covers every possible call-ordering permutation
	 * and asserts that latency never exceeds a sane bound.
	 */
	@Test
	void latencyNeverReturnsEpochTimestamp() {
		final long MAX_SANE_LATENCY_MICROS = 1_000_000_000L; // 1000 seconds

		// Case 1: no lifecycle methods called at all
		var op = new OperationImpl<>(0, OpType.CREATE, new ItemImpl("a"), null, null, null);
		assertTrue(op.latency() < MAX_SANE_LATENCY_MICROS,
						"latency must not be epoch-scale when no lifecycle methods called");

		// Case 2: only startRequest
		op = new OperationImpl<>(0, OpType.CREATE, new ItemImpl("b"), null, null, null);
		op.startRequest();
		assertTrue(op.latency() < MAX_SANE_LATENCY_MICROS,
						"latency must not be epoch-scale after only startRequest");

		// Case 3: startRequest + startResponse (no finishRequest — the actual bug scenario)
		op = new OperationImpl<>(0, OpType.CREATE, new ItemImpl("c"), null, null, null);
		op.startRequest();
		op.startResponse();
		assertTrue(op.latency() < MAX_SANE_LATENCY_MICROS,
						"latency must not be epoch-scale when finishRequest was skipped");

		// Case 4: startRequest + finishRequest (no startResponse)
		op = new OperationImpl<>(0, OpType.CREATE, new ItemImpl("d"), null, null, null);
		op.startRequest();
		op.finishRequest();
		assertTrue(op.latency() < MAX_SANE_LATENCY_MICROS,
						"latency must not be epoch-scale when startResponse was not called");

		// Case 5: normal full lifecycle
		op = new OperationImpl<>(0, OpType.CREATE, new ItemImpl("e"), null, null, null);
		op.startRequest();
		op.finishRequest();
		op.startResponse();
		op.finishResponse();
		assertTrue(op.latency() < MAX_SANE_LATENCY_MICROS,
						"latency must not be epoch-scale in normal lifecycle");
	}

}
