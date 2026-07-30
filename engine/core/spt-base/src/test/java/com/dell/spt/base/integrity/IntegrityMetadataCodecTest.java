package com.dell.spt.base.integrity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IntegrityMetadataCodecTest {

	private static final String EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

	@Test
	void decodesLogicalAndHttpKeysCaseInsensitively() throws Exception {
		final var entries = List.<Map.Entry<String, ?>> of(
						new AbstractMap.SimpleImmutableEntry<>("X-Amz-Meta-SPT-Integrity-Version", " 1 "),
						new AbstractMap.SimpleImmutableEntry<>("spt-integrity-algorithm", " SHA256 "),
						new AbstractMap.SimpleImmutableEntry<>("SPT-INTEGRITY-DIGEST", EMPTY_SHA256.toUpperCase()),
						new AbstractMap.SimpleImmutableEntry<>("x-amz-meta-spt-integrity-size", " 0 "));

		assertEquals(new IntegrityMetadata("1", "sha256", EMPTY_SHA256, 0),
						IntegrityMetadataCodec.decode(entries));
	}

	@Test
	void acceptsIdenticalDuplicatesButRejectsConflicts() throws Exception {
		final var entries = validEntries();
		entries.add(new AbstractMap.SimpleImmutableEntry<>(
						"x-amz-meta-spt-integrity-size", List.of("0", "0")));
		entries.add(new AbstractMap.SimpleImmutableEntry<>(
						"x-amz-meta-spt-integrity-digest", EMPTY_SHA256.toUpperCase()));
		assertEquals(0, IntegrityMetadataCodec.decode(entries).size());

		entries.add(new AbstractMap.SimpleImmutableEntry<>("SPT-INTEGRITY-SIZE", "1"));
		final var failure = assertThrows(IntegrityMetadataException.class,
						() -> IntegrityMetadataCodec.decode(entries));
		assertEquals(IntegrityFailureReason.METADATA_INVALID, failure.reason());
	}

	@Test
	void classifiesMissingUnsupportedAndMalformedMetadata() {
		var failure = assertThrows(IntegrityMetadataException.class,
						() -> IntegrityMetadataCodec.decode(Map.of()));
		assertEquals(IntegrityFailureReason.METADATA_MISSING, failure.reason());

		final var unsupported = validEntries();
		unsupported.set(1, new AbstractMap.SimpleImmutableEntry<>(
						IntegrityMetadataCodec.KEY_ALGORITHM, "crc32c"));
		failure = assertThrows(IntegrityMetadataException.class,
						() -> IntegrityMetadataCodec.decode(unsupported));
		assertEquals(IntegrityFailureReason.ALGORITHM_UNSUPPORTED, failure.reason());

		final var malformed = validEntries();
		malformed.set(2, new AbstractMap.SimpleImmutableEntry<>(
						IntegrityMetadataCodec.KEY_DIGEST, "xyz"));
		failure = assertThrows(IntegrityMetadataException.class,
						() -> IntegrityMetadataCodec.decode(malformed));
		assertEquals(IntegrityFailureReason.METADATA_INVALID, failure.reason());
	}

	private static List<Map.Entry<String, ?>> validEntries() {
		return new ArrayList<>(List.of(
						new AbstractMap.SimpleImmutableEntry<>(IntegrityMetadataCodec.KEY_VERSION, "1"),
						new AbstractMap.SimpleImmutableEntry<>(IntegrityMetadataCodec.KEY_ALGORITHM, "sha256"),
						new AbstractMap.SimpleImmutableEntry<>(IntegrityMetadataCodec.KEY_DIGEST, EMPTY_SHA256),
						new AbstractMap.SimpleImmutableEntry<>(IntegrityMetadataCodec.KEY_SIZE, "0")));
	}
}
