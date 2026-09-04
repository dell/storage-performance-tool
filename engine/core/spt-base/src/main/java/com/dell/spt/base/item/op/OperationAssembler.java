package com.dell.spt.base.item.op;

import com.dell.spt.base.item.Item;
import java.io.IOException;
import java.util.List;

/**
 * Converts input identities into logical operations without assuming equal cardinality.
 *
 * <p>An assembler accepts every identity in the supplied batch. It may retain identities
 * internally and emit fewer operations than it consumed. Operation cardinality remains the
 * unit used by generator limits, throttles, dispatch buffers, and completion checks.
 */
public interface OperationAssembler<I extends Item, O extends Operation<I>> extends AutoCloseable {

	/** Returns the origin index assigned to emitted operations and used by indexed throttles. */
	int originIndex();

	/** Returns the type of logical operation emitted by this assembler. */
	OpType opType();

	/**
	 * Accepts every supplied input identity and appends zero or more logical operations to an
	 * empty, caller-owned buffer. Implementations may retain identities across calls but must
	 * not retain or replace the output buffer. The returned emitted count must equal the number
	 * of operations appended, and must not exceed the buffer capacity documented by the caller.
	 *
	 * <p>The generator invokes this method serially and clears the output buffer after consuming
	 * its contents, so implementations may reuse immutable result instances for common count
	 * pairs to avoid hot-path allocation.
	 *
	 * @param items input identities to accept in this call
	 * @param operations empty buffer receiving logical operations for dispatch
	 * @return separate consumed-identity and emitted-operation cardinalities
	 * @throws IOException if operation construction fails
	 * @throws IllegalArgumentException if an identity cannot be converted into an operation
	 */
	OperationAssemblyResult assemble(final List<I> items, final List<O> operations)
					throws IOException, IllegalArgumentException;

	/**
	 * Finalizes the assembler exactly once. Stateful assemblers may append one retained logical
	 * operation. A normal completion result is dispatchable; operations returned for any other
	 * reason are recovery identities which callers must classify as unattempted.
	 *
	 * <p>The compatibility default has no retained state.
	 */
	default OperationAssemblyResult finish(
					final OperationAssemblyStopReason reason, final List<O> operations) throws IOException {
		return new OperationAssemblyResult(0, 0);
	}

	/** Releases resources owned or retained by this assembler. */
	@Override
	void close();
}
