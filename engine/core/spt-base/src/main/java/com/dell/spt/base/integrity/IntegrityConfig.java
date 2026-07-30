package com.dell.spt.base.integrity;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.github.akurilov.confuse.Config;
import java.util.Locale;
import java.util.NoSuchElementException;

/** Parsed and validated {@code storage.integrity} configuration. */
public record IntegrityConfig(
			IntegrityMode mode,
			String algorithm,
			IntegrityInputProvenance inputProvenance,
			String expectedProducerId) {

	private static final IntegrityConfig DISABLED = new IntegrityConfig(
			IntegrityMode.NONE,
			IntegrityMetadataCodec.ALGORITHM_SHA256,
			IntegrityInputProvenance.NONE,
			null);

	public static IntegrityConfig disabled() {
		return DISABLED;
	}

	public static IntegrityConfig fromStorage(final Config storageConfig) {
		if (storageConfig == null) {
			return DISABLED;
		}
		final Config config;
		try {
			config = storageConfig.configVal("integrity");
		} catch (final NoSuchElementException e) {
			return DISABLED;
		}
		if (config == null) {
			return DISABLED;
		}

		final IntegrityMode mode = parseEnum(
				IntegrityMode.values(), config.stringVal("mode"), "storage.integrity.mode");
		final String algorithm = normalize(config.stringVal("algorithm"));
		if (mode == IntegrityMode.METADATA
				&& !IntegrityMetadataCodec.ALGORITHM_SHA256.equals(algorithm)) {
			throw new IllegalConfigurationException(
					"storage.integrity.algorithm must be sha256 in metadata mode");
		}

		final Config inputConfig = config.configVal("input");
		final IntegrityInputProvenance provenance = parseEnum(
				IntegrityInputProvenance.values(),
				inputConfig.stringVal("provenance"),
				"storage.integrity.input.provenance");
		final String expectedProducerId = emptyToNull(inputConfig.stringVal("expectedProducerId"));
		return new IntegrityConfig(mode, algorithm, provenance, expectedProducerId);
	}

	public boolean enabled() {
		return mode == IntegrityMode.METADATA;
	}

	public void requireReadProvenance() {
		if (enabled() && inputProvenance == IntegrityInputProvenance.NONE) {
			throw new IllegalConfigurationException(
					"metadata-mode READ requires explicit storage.integrity.input.provenance");
		}
	}

	private static <T extends Enum<T>> T parseEnum(
				final T[] values, final String configuredValue, final String path) {
		final String normalized = normalize(configuredValue);
		for (final T value : values) {
			if (value.name().toLowerCase(Locale.ROOT).equals(normalized)) {
				return value;
			}
		}
		throw new IllegalConfigurationException(path + " has unsupported value: " + configuredValue);
	}

	private static String normalize(final String value) {
		return value == null ? "" : value.trim().toLowerCase(Locale.ROOT);
	}

	private static String emptyToNull(final String value) {
		return value == null || value.isBlank() ? null : value;
	}
}
