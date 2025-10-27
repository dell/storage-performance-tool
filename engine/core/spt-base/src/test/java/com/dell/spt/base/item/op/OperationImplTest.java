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
}
