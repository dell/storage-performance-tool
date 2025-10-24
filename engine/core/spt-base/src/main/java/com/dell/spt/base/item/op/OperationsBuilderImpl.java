package com.dell.spt.base.item.op;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.config.ConstantValueInput;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.io.Input;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Created by kurila on 14.07.16. */
public class OperationsBuilderImpl<I extends Item, O extends Operation<I>>
				implements OperationsBuilder<I, O> {

	protected final int originIndex;

	protected OpType opType = OpType.CREATE; // by default
	protected String inputPath = null;

	protected Input<String> outputPathInput;
	protected boolean constOutputPathFlag = true;
	protected String constOutputPath = null;

	protected Input<Credential> credentialInput;
	protected boolean constCredFlag = true;
	protected Credential constCred = Credential.NONE;

	protected Map<String, Credential> credentialsByPath = null;

	public OperationsBuilderImpl(final int originIndex) {
		this.originIndex = originIndex;
	}

	@Override
	public final int originIndex() {
		return originIndex;
	}

	@Override
	public final OpType opType() {
		return opType;
	}

	@Override
	public OperationsBuilderImpl<I, O> opType(final OpType opType) {
		this.opType = opType;
		return this;
	}

	@Override
	public final String inputPath() {
		return inputPath;
	}

	@Override
	public OperationsBuilderImpl<I, O> inputPath(final String inputPath) {
		this.inputPath = inputPath;
		return this;
	}

	@Override
	public OperationsBuilderImpl<I, O> outputPathInput(final Input<String> ops) {
		this.outputPathInput = ops;
		if (outputPathInput == null) {
			constOutputPathFlag = true;
			constOutputPath = null;
		} else if (outputPathInput instanceof ConstantValueInput) {
			constOutputPathFlag = true;
			constOutputPath = outputPathInput.get();
		} else {
			constOutputPathFlag = false;
		}
		return this;
	}

	@Override
	public OperationsBuilderImpl<I, O> credentialInput(
					final Input<Credential> credentialInput) {
		this.credentialInput = credentialInput;
		if (credentialInput == null) {
			constCredFlag = true;
			constCred = Credential.NONE;
		} else if (credentialInput instanceof ConstantValueInput) {
			constCredFlag = true;
			constCred = credentialInput.get();
		} else {
			constCredFlag = false;
		}
		return this;
	}

	@Override
	public OperationsBuilderImpl<I, O> credentialsByPath(final Map<String, Credential> credByPath) {
		this.credentialsByPath = credByPath;
		return this;
	}

	@Override
	@SuppressWarnings("unchecked")
	public O buildOp(final I item) throws IOException {
		final String outputPath = getNextOutputPath();
		return (O) new OperationImpl<I>(
						originIndex, opType, item, inputPath, outputPath, getNextCredential(outputPath));
	}

	@Override
	@SuppressWarnings("unchecked")
	public void buildOps(final List<I> items, final List<O> buff) throws IOException {
		String outputPath;
		for (final I item : items) {
			outputPath = getNextOutputPath();
			buff.add(
							(O) new OperationImpl<>(
											originIndex, opType, item, inputPath, outputPath, getNextCredential(outputPath)));
		}
	}

	protected final String getNextOutputPath() {
		return constOutputPathFlag || outputPathInput == null ? constOutputPath : outputPathInput.get();
	}

	protected final Credential getNextCredential(final String path) {
		// Prefer path-specific credentials when provided
		if (credentialsByPath != null) {
			final var cred = credentialsByPath.get(path);
			return cred != null ? cred : Credential.NONE;
		}
		// Fall back to configured credential input or constant
		if (!constCredFlag && credentialInput != null) {
			return credentialInput.get();
		}
		return constCred;
	}

	@Override
	public void close() {
		inputPath = null;
		try {
			if (outputPathInput != null) {
				outputPathInput.close();
				outputPathInput = null;
			}
			if (credentialInput != null) {
				credentialInput.close();
				credentialInput = null;
			}
			if (credentialsByPath != null) {
				credentialsByPath.clear();
				credentialsByPath = null;
			}
		} catch (final Exception e) {
			throwUnchecked(e);
		}
	}
}
