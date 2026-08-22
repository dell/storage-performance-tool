package com.dell.spt.base.item.op.deletion;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.OperationImpl;
import java.util.Objects;

/** Default lifecycle-aware standalone DELETE request operation. */
public final class DeleteRequestOperationImpl extends OperationImpl<IntegrityManifestDataItem>
				implements DeleteRequestOperation {

	private final DeleteRequest deleteRequest;
	private volatile DeleteRequestResult deleteResult;

	/** Creates one pending logical DELETE request operation. */
	public DeleteRequestOperationImpl(final int originIndex, final DeleteRequest deleteRequest) {
		super(
						originIndex,
						OpType.DELETE,
						requireRequest(deleteRequest).targets().get(0).item(),
						null,
						null,
						deleteRequest.credential());
		this.deleteRequest = deleteRequest;
		status(Status.PENDING);
	}

	private DeleteRequestOperationImpl(final DeleteRequestOperationImpl other) {
		super(other);
		this.deleteRequest = other.deleteRequest;
		this.deleteResult = other.deleteResult;
	}

	@Override
	public DeleteRequest deleteRequest() {
		return deleteRequest;
	}

	@Override
	public synchronized void completeDelete(final DeleteTransportResult transportResult) {
		if (deleteResult != null) {
			throw new IllegalStateException("Standalone DELETE request is already complete");
		}
		final var reconciled = DeleteRequestReconciler.reconcile(deleteRequest, transportResult);
		deleteResult = reconciled;
		status(reconciled.operationStatus());
	}

	@Override
	public DeleteRequestResult deleteResult() {
		return deleteResult;
	}

	@Override
	public DeleteRequestOperationImpl result() {
		return new DeleteRequestOperationImpl(this);
	}

	@Override
	public void reset() {
		super.reset();
		deleteResult = null;
	}

	private static DeleteRequest requireRequest(final DeleteRequest request) {
		return Objects.requireNonNull(request, "DELETE request");
	}
}
