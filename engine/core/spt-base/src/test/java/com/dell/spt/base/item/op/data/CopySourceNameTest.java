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
 * A copy keeps the name its first attempt read the source by, through every {@code result()} and
 * reset that a retry, a recycle or a PENDING redispatch puts it through, while its item's name moves
 * under the destination path. Operations are built as the load generator builds them.
 */
class CopySourceNameTest {

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
	void aFirstAttemptHasNoKeptName() throws Exception {
		assertNull(build(OpType.CREATE, "/src/k0", "/src", "/dst").copySourceName());
	}

	@Test
	void aCopyKeepsItsFirstNameWhileItsItemMovesUnderTheDestination() throws Exception {
		final var first = build(OpType.CREATE, "/src/k0", "/src", "/dst");
		final var second = again(first);
		assertInstanceOf(DataOperationImpl.class, second);
		assertEquals("/dst/src/k0", second.item().name());
		assertEquals("/src/k0", first.copySourceName());
		assertEquals("/src/k0", second.copySourceName());
		final var third = again(second);
		assertEquals("/dst/src/k0", third.item().name());
		assertEquals("/src/k0", third.copySourceName());
		assertEquals("/src/k0", third.result().copySourceName());
	}

	@Test
	void aNestedKeyIsKeptWhole() throws Exception {
		final var second = again(build(OpType.CREATE, "/src/dir/sub/k0002", "/src", "/dst"));
		assertEquals("/dst/src/dir/sub/k0002", second.item().name());
		assertEquals("/src/dir/sub/k0002", again(second).copySourceName());
	}

	@Test
	void anItemsFileNameWithoutThePathIsKeptAsWritten() throws Exception {
		assertEquals("k0001", again(build(OpType.CREATE, "k0001", "/src", "/dst")).copySourceName());
	}

	@Test
	void noOtherOperationKeepsAName() throws Exception {
		assertNull(again(build(OpType.CREATE, "k0", null, "/dst")).copySourceName());
		for (final OpType opType : new OpType[]{OpType.READ, OpType.UPDATE, OpType.DELETE, OpType.STAT, OpType.NOOP
		}) {
			final var op = build(opType, "/src/k0", "/src", null);
			assertNull(again(again(op)).copySourceName(), opType.name());
		}
	}
}
