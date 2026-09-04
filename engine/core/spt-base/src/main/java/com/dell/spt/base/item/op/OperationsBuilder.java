package com.dell.spt.base.item.op;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Input;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Created by kurila on 14.07.16. */
public interface OperationsBuilder<I extends Item, O extends Operation<I>> extends AutoCloseable {

	int originIndex();

	OpType opType();

	OperationsBuilder<I, O> opType(final OpType opType);

	String inputPath();

	OperationsBuilder<I, O> inputPath(final String inputPath);

	OperationsBuilder<I, O> outputPathInput(final Input<String> outputPathSupplier);

	OperationsBuilder<I, O> credentialInput(final Input<Credential> credentialInput);

	OperationsBuilder<I, O> credentialsByPath(final Map<String, Credential> credentials);

	/**
	 * Selects the next effective credential while consuming the same routing inputs as {@link
	 * #buildOp(Item)}. The compatibility implementation delegates to {@code buildOp}; builders on
	 * allocation-sensitive paths may override this method with equivalent routing logic.
	 *
	 * @param item the item being routed
	 * @return the selected credential, possibly {@code null} when a custom builder uses that legacy
	 *     representation
	 * @throws IOException if routing input cannot be consumed
	 * @throws IllegalArgumentException if the item cannot be routed
	 */
	default Credential nextCredential(final I item) throws IOException, IllegalArgumentException {
		return buildOp(item).credential();
	}

	O buildOp(final I item) throws IOException, IllegalArgumentException;

	void buildOps(final List<I> items, final List<O> buff)
					throws IOException, IllegalArgumentException;

	@Override
	void close();
}
