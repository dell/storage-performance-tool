package com.dell.spt.base.item.op;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;

import com.dell.spt.base.integrity.IntegrityMetadata;
import com.dell.spt.base.integrity.IntegrityVerificationResult;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemNamePaths;
import com.dell.spt.base.load.lifecycle.OperationLifecycle;
import com.dell.spt.base.storage.Credential;

/** Created by kurila on 11.07.16. */
public interface Operation<I extends Item> {

	/** Offset used to express monotonic timestamps in epoch-relative microseconds. */
	long START_OFFSET_MICROS = currentTimeMillis() * 1000 - nanoTime() / 1000;

	/** Path separator used by operation path builders. */
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

	/**
	 * For a copy (a CREATE with a non-empty source path), the source its first attempt read, in the form the
	 * driver that sent it keeps: the HTTP drivers keep the URI path their own {@code dataUriPath}
	 * resolved for the source, which they pass to the copy headers, and other drivers keep the item's
	 * name as it was before {@link #result()} first rebuilt it under the destination path. The driver
	 * sets it once, when it builds the first request or, if none was built, when the attempt
	 * completes, before {@code result()}; a copy sent again (recycle mode, {@code load.op.retry} or a
	 * {@code PENDING} redispatch) reads it instead of resolving its renamed item again. It is never
	 * cleared, so it survives {@link #reset()}. Null until then, and for any other operation,
	 * including a CREATE whose source path is empty, which no driver treats as a copy.
	 *
	 * <p>Only an operation that keeps it, as {@code OperationImpl} and its subclasses do, gets this; an
	 * implementation with these defaults behaves as before. No driver reads it for a composite,
	 * partial or path operation: those can't copy today (a Netty multipart copy is refused, an
	 * {@code s3-aws} composite CREATE uploads, and path copies are unsupported).
	 */
	default String copySource() {
		return null;
	}

	/**
	 * Keeps {@link #copySource()}; only a driver calls it. The default keeps nothing. It is a member
	 * of this interface, not only of {@code OperationImpl}, because drivers see their operations as
	 * {@code Operation}.
	 */
	default void copySource(final String copySource) {}

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
		} else if (!ItemNamePaths.startsWithPath(itemName, itemPath)) {
			// Compared as written, because not every driver treats a relative path as rooted: to a
			// filesystem driver, /data/key is not under the relative directory data.
			item.name(ItemNamePaths.join(itemPath, itemName));
		}
	}

	Operation<I> result();

	/**
	 * Returns the lossless dispatch lifecycle shared with result snapshots.
	 *
	 * <p>The compatibility default leaves third-party operation implementations untracked.
	 */
	default OperationLifecycle lifecycle() {
		return OperationLifecycle.untracked();
	}

	/**
	 * Starts a new logical dispatch circulation after a retained terminal or recovered
	 * unattempted result is recycled. Implementations with tracked lifecycle state should return
	 * a fresh lifecycle while leaving snapshots of the preceding circulation attached to the old
	 * instance.
	 */
	default OperationLifecycle startNextLifecycle() {
		return lifecycle();
	}

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
