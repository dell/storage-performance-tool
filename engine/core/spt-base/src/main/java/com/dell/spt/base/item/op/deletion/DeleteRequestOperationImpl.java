package com.dell.spt.base.item.op.deletion;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.OperationImpl;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Default lifecycle-aware standalone DELETE request operation. */
public final class DeleteRequestOperationImpl extends OperationImpl<IntegrityManifestDataItem>
				implements DeleteRequestOperation {

	private static final long MIN_RECORDED_LATENCY_MICROS = 1;

	private final DeleteRequest deleteRequest;
	private volatile DeleteRequestResult deleteResult;
	private volatile long requestFirstByteTime;
	private volatile long responseFirstByteTime;
	private volatile long transportRequestFirstByteNanos;
	private volatile long transportRequestLatency;

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
		this.requestFirstByteTime = other.requestFirstByteTime;
		this.responseFirstByteTime = other.responseFirstByteTime;
		this.transportRequestFirstByteNanos = other.transportRequestFirstByteNanos;
		this.transportRequestLatency = other.transportRequestLatency;
	}

	@Override
	public synchronized void beginTransportAttempt() {
		clearTransportResponseTiming();
	}

	@Override
	public synchronized void failTransportAttempt() {
		clearTransportResponseTiming();
	}

	private void clearTransportResponseTiming() {
		responseFirstByteTime = 0;
		transportRequestLatency = 0;
		respTimeStart = 0;
		respTimeDone = 0;
	}

	@Override
	public synchronized void markRequestFirstByteSent() {
		if (requestFirstByteTime == 0) {
			requestFirstByteTime = START_OFFSET_MICROS + System.nanoTime() / 1000;
		}
	}

	@Override
	public long requestFirstByteTime() {
		return requestFirstByteTime;
	}

	@Override
	public synchronized void recordTransportRequestTiming(
					final long requestFirstByteNanos, final long responseFirstByteNanos) {
		if (transportRequestFirstByteNanos == 0 && requestFirstByteNanos > 0) {
			transportRequestFirstByteNanos = requestFirstByteNanos;
		}
		if (transportRequestFirstByteNanos > 0
						&& responseFirstByteNanos > transportRequestFirstByteNanos) {
			final long latencyNanos = responseFirstByteNanos - transportRequestFirstByteNanos;
			transportRequestLatency = Math.max(
							MIN_RECORDED_LATENCY_MICROS,
							TimeUnit.NANOSECONDS.toMicros(latencyNanos));
		}
	}

	@Override
	public long transportRequestLatency() {
		final long directLatency = transportRequestLatency;
		return directLatency > 0
						? directLatency
						: DeleteRequestOperation.super.transportRequestLatency();
	}

	@Override
	public synchronized void markResponseFirstByteReceived() {
		if (responseFirstByteTime == 0) {
			startResponse();
			responseFirstByteTime = respTimeStart();
		}
	}

	@Override
	public long responseFirstByteTime() {
		return responseFirstByteTime;
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
		requestFirstByteTime = 0;
		responseFirstByteTime = 0;
		transportRequestFirstByteNanos = 0;
		transportRequestLatency = 0;
	}

	private static DeleteRequest requireRequest(final DeleteRequest request) {
		return Objects.requireNonNull(request, "DELETE request");
	}
}
