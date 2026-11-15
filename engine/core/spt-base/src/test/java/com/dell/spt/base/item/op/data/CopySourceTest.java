package com.dell.spt.base.item.op.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import org.junit.jupiter.api.Test;

/**
 * The copy source a driver keeps on an operation survives every {@code result()} and reset that a
 * retry, a recycle or a PENDING redispatch puts the copy through, while the item's name moves under
 * the destination path; {@code result()} keeps nothing itself. Operations are built as the load
 * generator builds them.
 */
class CopySourceTest {

	private static Operation<DataItemImpl> build(
					final OpType opType, final String name, final String inputPath, final String outputPath)
					throws Exception {
		final var builder = new DataOperationsBuilderImpl<DataItemImpl, DataOperation<DataItemImpl>>(0)
						.opType(opType)
						.inputPath(inputPath)
						.outputPathInput(outputPath == null ? null : new ConstantValueInputImpl<>(outputPath))
						.sizeThreshold(Long.MAX_VALUE);
		builder.credentialInput(null);
		return assertInstanceOf(DataOperationImpl.class, builder.buildOp(new DataItemImpl(name, 0, 1024)));
	}

	/** Completes an attempt and readies the result for the next dispatch, as a retry or recycle does. */
	private static Operation<DataItemImpl> again(final Operation<DataItemImpl> op) {
		final var next = op.result();
		next.reset();
		return next;
	}

	@Test
	void aNewCopyHasNoSourceKept() throws Exception {
		assertNull(build(OpType.CREATE, "/src/k0", "/src", "/dst").copySource());
	}

	@Test
	void resultKeepsNoSourceItself() throws Exception {
		final var second = again(build(OpType.CREATE, "/src/k0", "/src", "/dst"));
		assertEquals("/dst/src/k0", second.item().name());
		assertNull(second.copySource());
	}

	@Test
	void aKeptSourceIsCarriedThroughEveryResultAndReset() throws Exception {
		final var first = build(OpType.CREATE, "/src/k0", "/src", "/dst");
		first.copySource("/gateway/src/k0");
		final var second = again(first);
		assertInstanceOf(DataOperationImpl.class, second);
		final var third = again(second);
		assertEquals("/dst/src/k0", third.item().name());
		assertEquals("/gateway/src/k0", second.copySource());
		assertEquals("/gateway/src/k0", third.copySource());
		assertEquals("/gateway/src/k0", third.result().copySource());
	}

	@Test
	void theCopyConstructorCarriesTheKeptSource() throws Exception {
		final var op = (DataOperationImpl<DataItemImpl>) build(OpType.CREATE, "/src/k0", "/src", "/dst");
		op.copySource("/src/k0");
		assertEquals("/src/k0", new DataOperationImpl<>(op).copySource());
	}
}
