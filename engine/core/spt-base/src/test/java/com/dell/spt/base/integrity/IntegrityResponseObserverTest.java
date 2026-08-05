package com.dell.spt.base.integrity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IntegrityResponseObserverTest {

	private static final String ABC_SHA256 = "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad";

	@Test
	void verifiesOnlyAfterTheCompleteBody() {
		final var observer = new IntegrityResponseObserver(metadata(ABC_SHA256, 3).entrySet(), 3L);
		observer.onBody(ByteBuffer.wrap("a".getBytes(StandardCharsets.UTF_8)));
		observer.onBody(ByteBuffer.wrap("bc".getBytes(StandardCharsets.UTF_8)));

		final var result = observer.finish();
		assertTrue(result.verified());
		assertEquals(ABC_SHA256, result.actualDigest());
		assertEquals(3, result.actualSize());
	}

	@Test
	void distinguishesMetadataSizeAndDigestFailures() {
		var observer = new IntegrityResponseObserver(Map.<String, Object> of().entrySet(), 3L);
		observer.onBody(ByteBuffer.wrap("abc".getBytes(StandardCharsets.UTF_8)));
		var result = observer.finish();
		assertFalse(result.verified());
		assertEquals(IntegrityFailureReason.METADATA_MISSING, result.failureReason());

		observer = new IntegrityResponseObserver(metadata(ABC_SHA256, 4).entrySet(), 3L);
		observer.onBody(ByteBuffer.wrap("abc".getBytes(StandardCharsets.UTF_8)));
		result = observer.finish();
		assertEquals(IntegrityFailureReason.SIZE_MISMATCH, result.failureReason());

		observer = new IntegrityResponseObserver(metadata("0".repeat(64), 3).entrySet(), 3L);
		observer.onBody(ByteBuffer.wrap("abc".getBytes(StandardCharsets.UTF_8)));
		result = observer.finish();
		assertEquals(IntegrityFailureReason.DIGEST_MISMATCH, result.failureReason());
	}

	private static Map<String, Object> metadata(final String digest, final long size) {
		return Map.of(
						IntegrityMetadataCodec.KEY_VERSION, "1",
						IntegrityMetadataCodec.KEY_ALGORITHM, "sha256",
						IntegrityMetadataCodec.KEY_DIGEST, digest,
						IntegrityMetadataCodec.KEY_SIZE, Long.toString(size));
	}
}
