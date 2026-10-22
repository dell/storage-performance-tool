package com.dell.spt.base.storage.driver.bucket;

import java.util.Objects;

/**
 * Classified outcome of one bucket control request.
 *
 * <p>The detail is public: it may reach {@code /status} and the CLI, so it names only the kind of
 * failure (for example {@code HTTP 403} or {@code timed out}) and never carries credentials,
 * headers, query strings, response bodies or endpoints.
 */
public final class TargetBucketResponse {

	public enum Kind {
		/** The bucket exists and is accessible, or was created. */
		OK,
		/** The target reported the bucket missing. */
		NOT_FOUND,
		/** Any other outcome; never interpreted as absence. */
		FAILED
	}

	private static final TargetBucketResponse OK_RESPONSE = new TargetBucketResponse(Kind.OK, "");
	private static final TargetBucketResponse NOT_FOUND_RESPONSE = new TargetBucketResponse(Kind.NOT_FOUND, "not found");

	private final Kind kind;
	private final String detail;

	private TargetBucketResponse(final Kind kind, final String detail) {
		this.kind = Objects.requireNonNull(kind, "kind");
		this.detail = detail;
	}

	public static TargetBucketResponse ok() {
		return OK_RESPONSE;
	}

	public static TargetBucketResponse notFound() {
		return NOT_FOUND_RESPONSE;
	}

	public static TargetBucketResponse failed(final String detail) {
		return new TargetBucketResponse(Kind.FAILED, detail == null || detail.isBlank() ? "request failed" : detail);
	}

	/** Classifies an HTTP status code: 2xx is OK, 404 is NOT_FOUND, anything else FAILED. */
	public static TargetBucketResponse ofHttpStatus(final int statusCode) {
		if (statusCode >= 200 && statusCode < 300) {
			return ok();
		}
		if (statusCode == 404) {
			return notFound();
		}
		return failed("HTTP " + statusCode);
	}

	public Kind kind() {
		return kind;
	}

	public String detail() {
		return detail;
	}

	@Override
	public String toString() {
		return detail.isEmpty() ? kind.name() : kind + " (" + detail + ")";
	}
}
