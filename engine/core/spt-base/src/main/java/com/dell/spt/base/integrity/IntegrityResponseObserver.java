package com.dell.spt.base.integrity;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Map;

/**
 * Per-operation full-response observer. Drivers feed every successful GET body chunk and invoke
 * {@link #finish()} only after normal protocol completion; transport failures never become
 * corruption results.
 */
public final class IntegrityResponseObserver {

	private final MessageDigest digest;
	private final IntegrityMetadata expected;
	private final IntegrityFailureReason metadataFailureReason;
	private final String metadataFailureDetail;
	private final Long responseContentLength;
	private long workerNanos;
	private long actualSize;
	private boolean finished;

	public IntegrityResponseObserver(
					final Iterable<? extends Map.Entry<String, ?>> metadataEntries,
					final Long responseContentLength) {
		final long digestInitStarted = System.nanoTime();
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (final NoSuchAlgorithmException e) {
			throw new IllegalStateException("SHA-256 is unavailable", e);
		}
		workerNanos = System.nanoTime() - digestInitStarted;
		IntegrityMetadata parsed = null;
		IntegrityFailureReason reason = null;
		String detail = null;
		try {
			parsed = IntegrityMetadataCodec.decode(metadataEntries);
		} catch (final IntegrityMetadataException e) {
			reason = e.reason();
			detail = e.getMessage();
		}
		expected = parsed;
		metadataFailureReason = reason;
		metadataFailureDetail = detail;
		this.responseContentLength = responseContentLength;
	}

	public void onBody(final ByteBuffer body) {
		if (finished) {
			throw new IllegalStateException("integrity response observer is already finished");
		}
		if (body == null || !body.hasRemaining()) {
			return;
		}
		final ByteBuffer observed = body.asReadOnlyBuffer();
		actualSize = Math.addExact(actualSize, observed.remaining());
		final long updateStarted = System.nanoTime();
		digest.update(observed);
		workerNanos += System.nanoTime() - updateStarted;
	}

	public IntegrityVerificationResult finish() {
		if (finished) {
			throw new IllegalStateException("integrity response observer is already finished");
		}
		finished = true;
		final long finishStarted = System.nanoTime();
		final String actualDigest = HexFormat.of().formatHex(digest.digest());
		workerNanos += System.nanoTime() - finishStarted;
		if (metadataFailureReason != null) {
			return failed(actualDigest, workerNanos, metadataFailureReason, metadataFailureDetail);
		}
		if (responseContentLength != null
						&& (responseContentLength < 0 || responseContentLength != actualSize)) {
			return failed(
							actualDigest,
							workerNanos,
							IntegrityFailureReason.SIZE_MISMATCH,
							"response content length does not match the complete body");
		}
		if (expected.size() != actualSize) {
			return failed(
							actualDigest,
							workerNanos,
							IntegrityFailureReason.SIZE_MISMATCH,
							"integrity metadata size does not match the complete body");
		}
		if (!MessageDigest.isEqual(
						HexFormat.of().parseHex(expected.digest()), HexFormat.of().parseHex(actualDigest))) {
			return failed(
							actualDigest,
							workerNanos,
							IntegrityFailureReason.DIGEST_MISMATCH,
							"integrity digest does not match the complete body");
		}
		return new IntegrityVerificationResult(
						expected, actualDigest, actualSize, workerNanos, null, null);
	}

	private IntegrityVerificationResult failed(
					final String actualDigest,
					final long workerNanos,
					final IntegrityFailureReason reason,
					final String detail) {
		return new IntegrityVerificationResult(
						expected, actualDigest, actualSize, workerNanos, reason, detail);
	}
}
