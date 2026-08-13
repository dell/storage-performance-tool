package com.dell.spt.base.item.op;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;

import com.dell.spt.base.integrity.IntegrityMetadata;
import com.dell.spt.base.integrity.IntegrityVerificationResult;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.storage.Credential;

/** Created by kurila on 11.07.16. */
public interface Operation<I extends Item> {

	long START_OFFSET_MICROS = currentTimeMillis() * 1000 - nanoTime() / 1000;

	String SLASH = "/";

	int originIndex();

	// PENDING is used to recycle an item which hasn't been successfully finished due to specifics of any API
	// but is not counted as an error. So the item is recycled, but isn't counted either as succ or as error

	// OMIT is used to complete an item for whatever purpose that shouldn't be registered anywhere in the metrics
	// and affect them

	enum Status {
		PENDING, // 0
		ACTIVE, // 1
		INTERRUPTED, // 2
		FAIL_UNKNOWN, // 3
		SUCC, // 4
		FAIL_IO, // 5
		FAIL_TIMEOUT, // 6
		OMIT, // 7
		RESP_FAIL_UNKNOWN, // 8
		RESP_FAIL_CLIENT, // 9
		RESP_FAIL_SVC, // 10
		RESP_FAIL_NOT_FOUND, // 11
		RESP_FAIL_AUTH, // 12
		RESP_FAIL_CORRUPT, // 13
		RESP_FAIL_SPACE, // 14
	}

	OpType type();

	I item();

	String nodeAddr();

	void nodeAddr(final String nodeAddr);

	Status status();

	void status(final Status status);

	String srcPath();

	void srcPath(final String srcPath);

	String dstPath();

	void dstPath(final String dstPath);

	Credential credential();

	void credential(final Credential credential);

	void startRequest() throws IllegalStateException;

	void finishRequest() throws IllegalStateException;

	void startResponse() throws IllegalStateException;

	void finishResponse() throws IllegalStateException;

	long reqTimeStart();

	long reqTimeDone();

	long respTimeStart();

	long respTimeDone();

	long duration();

	long latency();

	default void buildItemPath(final I item, final String itemPath) {
		if (item instanceof IntegrityManifestDataItem) {
			return;
		}
		String itemName = item.name();
		if (itemPath == null || itemPath.isEmpty()) {
			if (!itemName.startsWith("/")) {
				item.name("/" + itemName);
			}
		} else if (!itemName.startsWith(itemPath)) {
			if (itemPath.endsWith("/")) {
				item.name(itemPath + itemName);
			} else {
				item.name(itemPath + "/" + itemName);
			}
		}
	}

	Operation<I> result();

	void reset();

	void resetTiming();

	/**
	 * Legacy compatibility accessor for the removed direct fast-recycle path.
	 *
	 * @deprecated always returns {@code false}
	 */
	@Deprecated
	default boolean driverRecycled() {
		return false;
	}

	/**
	 * Legacy compatibility mutator for the removed direct fast-recycle path.
	 *
	 * @param flag ignored
	 * @deprecated always a no-op
	 */
	@Deprecated
	default void driverRecycled(final boolean flag) {}

	/**
	 * Number of times this operation has been retried after a failure via {@code
	 * load-op-retry} (unrelated to {@link com.dell.spt.base.item.op.partial.PartialOperation
	 * #retryCount()}, which counts per-part MPU retries — deliberately a different method
	 * name so implementations of both don't collide). Defaults to 0 for implementations that
	 * don't track it.
	 */
	default int opRetryCount() {
		return 0;
	}

	/** Increment the whole-operation retry counter. */
	default void incrementOpRetryCount() {}

	/**
	 * Reset the whole-operation retry counter, e.g. after a terminal success, so a
	 * recycled operation starts its next logical attempt with a clean budget
	 * instead of accumulating retry counts across unrelated cycles.
	 */
	default void resetOpRetryCount() {}

	/** Exact version requested from storage; null/empty means current-version semantics. */
	default String requestedVersionId() {
		return null;
	}

	/** Compatibility-safe setter for the exact requested version. */
	default void requestedVersionId(final String versionId) {}

	/** Version returned by storage, kept separate from requested manifest identity. */
	default String returnedVersionId() {
		return null;
	}

	/** Compatibility-safe setter for the returned storage version. */
	default void returnedVersionId(final String versionId) {}

	/** Request identifier returned by storage for terminal diagnostics. */
	default String responseRequestId() {
		return null;
	}

	/** Compatibility-safe setter for the returned request identifier. */
	default void responseRequestId(final String requestId) {}

	/** Precomputed whole-object metadata attached to CREATE requests in metadata mode. */
	default IntegrityMetadata integrityMetadata() {
		return null;
	}

	/** Compatibility-safe setter for precomputed whole-object integrity metadata. */
	default void integrityMetadata(final IntegrityMetadata metadata) {}

	/** Protocol-complete metadata verification result for a READ response. */
	default IntegrityVerificationResult integrityVerificationResult() {
		return null;
	}

	/** Compatibility-safe setter for a protocol-complete READ verification result. */
	default void integrityVerificationResult(final IntegrityVerificationResult result) {}

	/**
	 * Returns {@code true} if this implementation actually tracks {@link #opRetryCount()} /
	 * {@link #incrementOpRetryCount()} (i.e. the default no-op methods above have been
	 * overridden with real state). {@code load-op-retry}'s bound depends on this being
	 * accurate: an implementation that reports {@code true} here but doesn't really track the
	 * count would retry forever, silently defeating the configured limit.
	 */
	default boolean supportsOpRetryTracking() {
		return false;
	}
}
