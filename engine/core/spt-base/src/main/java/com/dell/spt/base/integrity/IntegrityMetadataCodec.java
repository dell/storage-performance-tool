package com.dell.spt.base.integrity;

import static com.dell.spt.base.integrity.IntegrityFailureReason.ALGORITHM_UNSUPPORTED;
import static com.dell.spt.base.integrity.IntegrityFailureReason.METADATA_INVALID;
import static com.dell.spt.base.integrity.IntegrityFailureReason.METADATA_MISSING;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/** Encodes and validates the public SPT version 1 S3 user-metadata contract. */
public final class IntegrityMetadataCodec {

	public static final String HTTP_PREFIX = "x-amz-meta-";
	public static final String KEY_VERSION = "spt-integrity-version";
	public static final String KEY_ALGORITHM = "spt-integrity-algorithm";
	public static final String KEY_DIGEST = "spt-integrity-digest";
	public static final String KEY_SIZE = "spt-integrity-size";
	public static final String VERSION_1 = "1";
	public static final String ALGORITHM_SHA256 = "sha256";
	public static final int SHA256_HEX_LENGTH = 64;

	private static final Set<String> CONTRACT_KEYS = Set.of(KEY_VERSION, KEY_ALGORITHM, KEY_DIGEST, KEY_SIZE);
	private static final Pattern SHA256_HEX = Pattern.compile("[0-9a-fA-F]{64}");

	private IntegrityMetadataCodec() {}

	/** Logical keys for AWS SDK request builders. */
	public static Map<String, String> logicalMetadata(final IntegrityMetadata metadata) {
		final Map<String, String> values = new LinkedHashMap<>();
		values.put(KEY_VERSION, metadata.version());
		values.put(KEY_ALGORITHM, metadata.algorithm());
		values.put(KEY_DIGEST, metadata.digest());
		values.put(KEY_SIZE, Long.toString(metadata.size()));
		return Map.copyOf(values);
	}

	/** Full HTTP headers for Netty request builders. */
	public static Map<String, String> httpHeaders(final IntegrityMetadata metadata) {
		final Map<String, String> headers = new LinkedHashMap<>();
		logicalMetadata(metadata).forEach((key, value) -> headers.put(HTTP_PREFIX + key, value));
		return Map.copyOf(headers);
	}

	public static IntegrityMetadata decode(final Map<String, String> metadata)
					throws IntegrityMetadataException {
		return decode(metadata.entrySet());
	}

	/**
	 * Decode logical AWS metadata keys or full HTTP metadata headers. Keys are case-insensitive;
	 * values are trimmed; identical duplicates are accepted and conflicting duplicates are rejected.
	 */
	public static IntegrityMetadata decode(
					final Iterable<? extends Map.Entry<String, ?>> metadataEntries)
					throws IntegrityMetadataException {
		final Map<String, String> values = new LinkedHashMap<>();
		for (final Map.Entry<String, ?> entry : metadataEntries) {
			if (entry.getKey() == null) {
				continue;
			}
			String key = entry.getKey().toLowerCase(Locale.ROOT);
			if (key.startsWith(HTTP_PREFIX)) {
				key = key.substring(HTTP_PREFIX.length());
			}
			if (!CONTRACT_KEYS.contains(key)) {
				continue;
			}
			for (final String value : values(entry.getValue())) {
				final String normalizedValue = normalizeValue(key, value);
				final String previous = values.putIfAbsent(key, normalizedValue);
				if (previous != null && !previous.equals(normalizedValue)) {
					throw new IntegrityMetadataException(
									METADATA_INVALID, "conflicting duplicate integrity metadata: " + key);
				}
			}
		}

		for (final String key : CONTRACT_KEYS) {
			if (!values.containsKey(key) || values.get(key).isEmpty()) {
				throw new IntegrityMetadataException(
								METADATA_MISSING, "missing required integrity metadata: " + key);
			}
		}

		final String version = values.get(KEY_VERSION);
		if (!VERSION_1.equals(version)) {
			throw new IntegrityMetadataException(
							METADATA_INVALID, "unsupported integrity metadata version: " + version);
		}
		final String algorithm = values.get(KEY_ALGORITHM).toLowerCase(Locale.ROOT);
		if (!ALGORITHM_SHA256.equals(algorithm)) {
			throw new IntegrityMetadataException(
							ALGORITHM_UNSUPPORTED, "unsupported integrity algorithm: " + algorithm);
		}
		final String digest = values.get(KEY_DIGEST);
		if (!SHA256_HEX.matcher(digest).matches()) {
			throw new IntegrityMetadataException(
							METADATA_INVALID, "integrity digest must contain exactly 64 hexadecimal characters");
		}

		final long size;
		try {
			size = Long.parseLong(values.get(KEY_SIZE));
		} catch (final NumberFormatException e) {
			throw new IntegrityMetadataException(METADATA_INVALID, "integrity size is not a decimal long");
		}
		if (size < 0) {
			throw new IntegrityMetadataException(METADATA_INVALID, "integrity size must be nonnegative");
		}
		return new IntegrityMetadata(VERSION_1, ALGORITHM_SHA256, digest.toLowerCase(Locale.ROOT), size);
	}

	private static String normalizeValue(final String key, final String value) {
		final String trimmed = value == null ? "" : value.trim();
		return KEY_ALGORITHM.equals(key) || KEY_DIGEST.equals(key)
						? trimmed.toLowerCase(Locale.ROOT)
						: trimmed;
	}

	private static Iterable<String> values(final Object value) {
		if (value instanceof Iterable<?> iterable) {
			final List<String> result = new ArrayList<>();
			for (final Object item : iterable) {
				result.add(item == null ? null : item.toString());
			}
			return result;
		}
		return List.of(value == null ? "" : value.toString());
	}
}
