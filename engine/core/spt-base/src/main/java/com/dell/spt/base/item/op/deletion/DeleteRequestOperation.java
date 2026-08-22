package com.dell.spt.base.item.op.deletion;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.Operation;

/** One first-class standalone DELETE request operation. */
public interface DeleteRequestOperation extends Operation<IntegrityManifestDataItem> {

	/** Returns the complete immutable logical request; {@link #item()} is only a projection. */
	DeleteRequest deleteRequest();

	/** Reconciles and completes this logical request exactly once. */
	void completeDelete(DeleteTransportResult transportResult);

	/** Returns the ordered per-target result, or {@code null} before completion. */
	DeleteRequestResult deleteResult();

	/** Returns a completed-operation snapshot retaining the full request and result. */
	@Override
	DeleteRequestOperation result();
}
