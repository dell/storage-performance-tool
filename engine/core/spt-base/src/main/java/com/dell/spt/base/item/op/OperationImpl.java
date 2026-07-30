package com.dell.spt.base.item.op;

import static java.lang.System.nanoTime;

import com.dell.spt.base.integrity.IntegrityMetadata;
import com.dell.spt.base.integrity.IntegrityVerificationResult;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.VersionedItem;
import com.dell.spt.base.storage.Credential;

/** Created by kurila on 20.10.15. */
public class OperationImpl<I extends Item> implements Operation<I> {

	protected int originIndex;
	protected OpType opType;
	protected I item;
	protected String srcPath;
	protected String dstPath;
	protected Credential credential;

	protected volatile String nodeAddr;
	protected volatile Status status;
	protected volatile long reqTimeStart;
	protected volatile long reqTimeDone;
	protected volatile long respTimeStart;
	protected volatile long respTimeDone;
	protected volatile boolean driverRecycled;
	protected volatile int opRetryCount;
	protected volatile String requestedVersionId;
	protected volatile String returnedVersionId;
	protected volatile String responseRequestId;
	protected volatile IntegrityMetadata integrityMetadata;
	protected volatile IntegrityVerificationResult integrityVerificationResult;

	public OperationImpl() {}

	public OperationImpl(
					final int originIndex,
					final OpType opType,
					final I item,
					final String srcPath,
					final String dstPath,
					final Credential credential) {
		this.originIndex = originIndex;
		this.opType = opType;
		this.item = item;

		// For CREATE we must not interpret key prefixes as a copy source.
		// Preserve the item name as-is and keep srcPath as provided (usually null).
		if (OpType.CREATE.equals(opType)) {
			this.srcPath = srcPath;
		} else if (item instanceof IntegrityManifestDataItem manifestItem) {
			this.srcPath = manifestItem.bucket().startsWith(SLASH)
							? manifestItem.bucket()
							: SLASH + manifestItem.bucket();
		} else {
			final String itemName = item.name();
			final int lastSlashIndex = itemName.lastIndexOf(SLASH);
			if (lastSlashIndex > 0 && lastSlashIndex < itemName.length()) {
				this.srcPath = itemName.substring(0, lastSlashIndex);
				item.name(itemName.substring(lastSlashIndex + 1));
			} else {
				this.srcPath = srcPath;
			}
		}

		if (dstPath == null) {
			if (OpType.READ.equals(opType)
							|| OpType.UPDATE.equals(opType)
							|| OpType.DELETE.equals(opType)
							|| OpType.STAT.equals(opType)) {
				this.dstPath = this.srcPath;
			}
		} else {
			this.dstPath = dstPath;
		}

		this.credential = credential;
		if (item instanceof VersionedItem versionedItem) {
			this.requestedVersionId = emptyToNull(versionedItem.versionId());
		}
	}

	protected OperationImpl(final OperationImpl<I> other) {
		this.originIndex = other.originIndex;
		this.opType = other.opType;
		this.item = other.item;
		this.srcPath = other.srcPath;
		this.dstPath = other.dstPath;
		this.credential = other.credential;
		this.nodeAddr = other.nodeAddr;
		this.status = other.status;
		this.reqTimeStart = other.reqTimeStart;
		this.reqTimeDone = other.reqTimeDone;
		this.respTimeStart = other.respTimeStart;
		this.respTimeDone = other.respTimeDone;
		this.driverRecycled = other.driverRecycled;
		// Deliberately propagated (not reset in reset() below): this must survive across
		// the reset()+redispatch cycle a retried operation goes through, or the retry
		// counter added for load-op-retry could never reach its limit.
		this.opRetryCount = other.opRetryCount;
		this.requestedVersionId = other.requestedVersionId;
		this.returnedVersionId = other.returnedVersionId;
		this.responseRequestId = other.responseRequestId;
		this.integrityMetadata = other.integrityMetadata;
		this.integrityVerificationResult = other.integrityVerificationResult;
	}

	@Override
	public OperationImpl<I> result() {
		buildItemPath(item, dstPath == null ? srcPath : dstPath);
		return new OperationImpl<>(this);
	}

	@Override
	public void resetTiming() {
		reqTimeStart = reqTimeDone = respTimeStart = respTimeDone = 0;
	}

	@Override
	public void reset() {
		item.reset();
		nodeAddr = null;
		status = Status.PENDING;
		reqTimeStart = reqTimeDone = respTimeStart = respTimeDone = 0;
		returnedVersionId = null;
		responseRequestId = null;
		integrityVerificationResult = null;
		driverRecycled = false;
	}

	@Override
	public final boolean driverRecycled() {
		return driverRecycled;
	}

	@Override
	public final void driverRecycled(final boolean flag) {
		this.driverRecycled = flag;
	}

	@Override
	public final int opRetryCount() {
		return opRetryCount;
	}

	@Override
	public final void incrementOpRetryCount() {
		opRetryCount++;
	}

	@Override
	public final void resetOpRetryCount() {
		opRetryCount = 0;
	}

	@Override
	public final boolean supportsOpRetryTracking() {
		return true;
	}

	@Override
	public final String requestedVersionId() {
		return requestedVersionId;
	}

	@Override
	public final void requestedVersionId(final String versionId) {
		requestedVersionId = emptyToNull(versionId);
	}

	@Override
	public final String returnedVersionId() {
		return returnedVersionId;
	}

	@Override
	public final void returnedVersionId(final String versionId) {
		returnedVersionId = emptyToNull(versionId);
	}

	@Override
	public final String responseRequestId() {
		return responseRequestId;
	}

	@Override
	public final void responseRequestId(final String requestId) {
		responseRequestId = emptyToNull(requestId);
	}

	@Override
	public final IntegrityMetadata integrityMetadata() {
		return integrityMetadata;
	}

	@Override
	public final void integrityMetadata(final IntegrityMetadata metadata) {
		integrityMetadata = metadata;
	}

	@Override
	public final IntegrityVerificationResult integrityVerificationResult() {
		return integrityVerificationResult;
	}

	@Override
	public final void integrityVerificationResult(final IntegrityVerificationResult result) {
		integrityVerificationResult = result;
	}

	private static String emptyToNull(final String value) {
		return value == null || value.isEmpty() ? null : value;
	}

	@Override
	public final int originIndex() {
		return originIndex;
	}

	@Override
	public final I item() {
		return item;
	}

	@Override
	public final OpType type() {
		return opType;
	}

	@Override
	public final String nodeAddr() {
		return nodeAddr;
	}

	@Override
	public final void nodeAddr(final String nodeAddr) {
		this.nodeAddr = nodeAddr;
	}

	@Override
	public final Status status() {
		return status;
	}

	@Override
	public final void status(final Status status) {
		this.status = status;
	}

	@Override
	public final String srcPath() {
		return srcPath;
	}

	@Override
	public final void srcPath(final String srcPath) {
		this.srcPath = srcPath;
	}

	@Override
	public final String dstPath() {
		return dstPath;
	}

	@Override
	public final void dstPath(final String dstPath) {
		this.dstPath = dstPath;
	}

	@Override
	public final Credential credential() {
		return credential;
	}

	@Override
	public final void credential(final Credential credential) {
		this.credential = credential;
	}

	@Override
	public final void startRequest() {
		reqTimeStart = START_OFFSET_MICROS + nanoTime() / 1000;
		status = Status.ACTIVE;
	}

	@Override
	public final void finishRequest() {
		reqTimeDone = START_OFFSET_MICROS + nanoTime() / 1000;
		if (respTimeStart > 0) {
			throw new IllegalStateException(
							"Request is finished ("
											+ reqTimeDone
											+ ") after the response is started ("
											+ respTimeStart
											+ ")");
		}
	}

	@Override
	public final void startResponse() {
		respTimeStart = START_OFFSET_MICROS + nanoTime() / 1000;
		if (reqTimeDone > respTimeStart) {
			throw new IllegalStateException(
							"Response is started ("
											+ respTimeStart
											+ ") before the request is finished ("
											+ reqTimeDone
											+ ")");
		}
	}

	@Override
	public void finishResponse() {
		respTimeDone = START_OFFSET_MICROS + nanoTime() / 1000;
		if (respTimeStart == 0) {
			throw new IllegalStateException("Response is finished while not started");
		}
	}

	@Override
	public final long reqTimeStart() {
		return reqTimeStart;
	}

	@Override
	public final long reqTimeDone() {
		return reqTimeDone;
	}

	@Override
	public final long respTimeStart() {
		return respTimeStart;
	}

	@Override
	public final long respTimeDone() {
		return respTimeDone;
	}

	@Override
	public final long duration() {
		return respTimeDone - reqTimeStart;
	}

	@Override
	public final long latency() {
		if (reqTimeDone == 0 || respTimeStart == 0) {
			return 0;
		}
		return respTimeStart - reqTimeDone;
	}

	protected static final ThreadLocal<StringBuilder> STRB = ThreadLocal.withInitial(StringBuilder::new);

	@Override
	public String toString() {
		final StringBuilder strb = STRB.get();
		strb.setLength(0);
		return strb.append(opType.name())
						.append(',')
						.append(item.toString())
						.append(',')
						.append(dstPath == null ? "" : dstPath)
						.append(',')
						.toString();
	}

	@Override
	public final int hashCode() {
		int result = Integer.hashCode(originIndex);
		result = 31 * result + (opType != null ? opType.hashCode() : 0);
		result = 31 * result + (item != null ? item.hashCode() : 0);
		return result;
	}
}
