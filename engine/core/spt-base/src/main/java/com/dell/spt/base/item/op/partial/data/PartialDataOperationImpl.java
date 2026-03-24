package com.dell.spt.base.item.op.partial.data;

import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.storage.Credential;

/** Created by andrey on 23.11.16. */
public class PartialDataOperationImpl<I extends DataItem> extends DataOperationImpl<I>
				implements PartialDataOperation<I> {

	private int partNumber;
	private CompositeDataOperation<I> parent;
	private int retryCount;

	public PartialDataOperationImpl() {
		super();
	}

	public PartialDataOperationImpl(
					final int originIndex,
					final OpType opType,
					final I part,
					final String srcPath,
					final String dstPath,
					final Credential credential,
					final int partNumber,
					final CompositeDataOperation<I> parent) {
		super(originIndex, opType, part, srcPath, dstPath, credential, null, 0);
		this.partNumber = partNumber;
		this.parent = parent;
	}

	protected PartialDataOperationImpl(final PartialDataOperationImpl<I> other) {
		super(other);
		this.partNumber = other.partNumber;
		this.parent = other.parent;
		this.retryCount = other.retryCount;
	}

	@Override
	public PartialDataOperationImpl<I> result() {
		buildItemPath(item, dstPath == null ? srcPath : dstPath);
		return new PartialDataOperationImpl<>(this);
	}

	@Override
	public final int partNumber() {
		return partNumber;
	}

	@Override
	public final CompositeDataOperation<I> parent() {
		return parent;
	}

	@Override
	public final int retryCount() {
		return retryCount;
	}

	@Override
	public final void incrementRetryCount() {
		retryCount++;
	}

	@Override
	public final void finishResponse() {
		super.finishResponse();
		parent.markSubTaskCompleted();
	}
}
