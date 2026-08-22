package com.dell.spt.base.integrity;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TimeUtil;
import com.dell.spt.base.util.BinarySizeFormat;
import com.github.akurilov.commons.reflection.TypeUtil;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Set;

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

	private static final Set<String> SUPPORTED_DRIVER_TYPES = Set.of("s3", "s3-aws", "s3-rdma");

	public static IntegrityConfig disabled() {
		return DISABLED;
	}

	public static IntegrityConfig fromStorage(final Config storageConfig) {
		if (storageConfig == null) {
			throw new IllegalConfigurationException("storage configuration is missing");
		}
		final Config config;
		try {
			config = storageConfig.configVal("integrity");
		} catch (final NoSuchElementException e) {
			throw new IllegalConfigurationException("storage.integrity configuration is missing", e);
		}
		if (config == null) {
			throw new IllegalConfigurationException("storage.integrity configuration is missing");
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

	/** Returns whether this step requires its output manifest to match the configured operation count. */
	public static boolean requiresExactOutputCount(final Config storageConfig) {
		final Config integrityOutput = storageConfig
					.configVal("integrity")
					.configVal("output");
		return integrityOutput != null && integrityOutput.boolVal("requireExactCount");
	}

	/** Returns whether LIST discovery must freeze at least one selected identity. */
	public static boolean requiresNonEmptySelection(final Config storageConfig) {
		final Config integritySelection = storageConfig
					.configVal("integrity")
					.configVal("selection");
		return integritySelection != null && integritySelection.boolVal("requireNonEmpty");
	}

	public static boolean isSupportedDriver(final String driverType) {
		return driverType != null
				&& SUPPORTED_DRIVER_TYPES.contains(driverType.trim().toLowerCase(Locale.ROOT));
	}

	public static void requireSupportedDriver(final String driverType) {
		if (!isSupportedDriver(driverType)) {
			throw new IllegalConfigurationException(
					"storage.integrity.mode=metadata is unsupported for driver " + driverType);
		}
	}

	/** Validates all effective v1 exclusions after scenario and override resolution. */
	public static IntegrityConfig validateLoadStep(final Config stepConfig) {
		if (stepConfig == null) {
			throw new IllegalConfigurationException("load step configuration is missing");
		}
		final IntegrityConfig integrity = fromStorage(stepConfig.configVal("storage"));
		final boolean requireExactOutputCount =
					requiresExactOutputCount(stepConfig.configVal("storage"));
		final boolean requireNonEmptySelection =
					requiresNonEmptySelection(stepConfig.configVal("storage"));
		if (!integrity.enabled()) {
			if (requireExactOutputCount) {
				throw excluded(
							"storage.integrity.output.requireExactCount requires metadata integrity mode");
			}
			if (requireNonEmptySelection) {
				throw excluded(
							"storage.integrity.selection.requireNonEmpty requires metadata integrity mode");
			}
			return integrity;
		}
		final long selectionMaxCount = stepConfig
					.configVal("storage")
					.configVal("integrity")
					.configVal("selection")
					.longVal("maxCount");
		if (selectionMaxCount < 0) {
			throw excluded("storage.integrity.selection.maxCount must be non-negative");
		}
		requireSupportedDriver(stepConfig.stringVal("storage-driver-type"));
		final String opType = normalize(stepConfig.stringVal("load-op-type"));
		if ("update".equals(opType)) {
			throw excluded("UPDATE operations are outside integrity metadata v1");
		}
		if (!Set.of("create", "read", "list", "delete").contains(opType)) {
			throw excluded("operation type " + opType + " is outside integrity metadata v1");
		}
		if (requireNonEmptySelection && !"list".equals(opType)) {
			throw excluded("storage.integrity.selection.requireNonEmpty is valid only for LIST");
		}
		if (requireNonEmptySelection) {
			final String outputFile = stepConfig.stringVal("item-output-file");
			if (outputFile == null || outputFile.isBlank()) {
				throw excluded("storage.integrity.selection.requireNonEmpty requires item.output.file");
			}
			requireEmpty(stepConfig.stringVal("item-input-file"),
						"guarded LIST discovery requires exact-prefix input rather than item.input.file");
			if (stepConfig.longVal("load-op-limit-count") != 0) {
				throw excluded("guarded LIST discovery requires load.op.limit.count=0");
			}
			if (timeLimitSeconds(stepConfig.val("load-step-limit-time")) != 0) {
				throw excluded("guarded LIST discovery requires load.step.limit.time=0");
			}
			if (!isZeroSizeLimit(stepConfig.val("load-step-limit-size"))) {
				throw excluded("guarded LIST discovery requires load.step.limit.size=0");
			}
		}
		if (requireExactOutputCount) {
			if (!"create".equals(opType)) {
				throw excluded("storage.integrity.output.requireExactCount is valid only for CREATE");
			}
			if (stepConfig.longVal("load-op-limit-count") <= 0) {
				throw excluded("storage.integrity.output.requireExactCount requires a positive load.op.limit.count");
			}
			final String outputFile = stepConfig.stringVal("item-output-file");
			if (outputFile == null || outputFile.isBlank()) {
				throw excluded("storage.integrity.output.requireExactCount requires item.output.file");
			}
		}
		if ("create".equals(opType)) {
			requireEmpty(stepConfig.stringVal("item-data-input-file"),
					"file-backed CREATE payloads are outside integrity metadata v1");
			requireEmpty(stepConfig.stringVal("item-input-file"),
					"copy CREATE operations are outside integrity metadata v1");
			requireEmpty(stepConfig.stringVal("item-data-ranges-concat"),
					"concatenated CREATE operations are outside integrity metadata v1");
		}
		if ("create".equals(opType) || "read".equals(opType)) {
			if (stepConfig.boolVal("load-op-recycle-mode")) {
				throw excluded("operation recycling is outside integrity metadata v1");
			}
			if (stepConfig.boolVal("load-op-recycle-content-update")) {
				throw excluded("recycle content update is outside integrity metadata v1");
			}
		}
		if ("read".equals(opType)) {
			integrity.requireReadProvenance();
			final List<String> fixed = stepConfig.listVal("item-data-ranges-fixed");
			if (stepConfig.intVal("item-data-ranges-random") > 0 || (fixed != null && !fixed.isEmpty())) {
				throw excluded("range READ cannot use whole-object integrity metadata v1");
			}
		}
		return integrity;
	}

	private static void requireEmpty(final String value, final String message) {
		if (value != null && !value.isBlank()) {
			throw excluded(message);
		}
	}

	private static long timeLimitSeconds(final Object rawValue) {
		return rawValue instanceof String
					? TimeUtil.getTimeInSeconds((String) rawValue)
					: TypeUtil.typeConvert(rawValue, long.class);
	}

	private static boolean isZeroSizeLimit(final Object rawValue) {
		final SizeInBytes size = rawValue instanceof String
					? BinarySizeFormat.parseSize((String) rawValue)
					: new SizeInBytes(TypeUtil.typeConvert(rawValue, long.class));
		return size.getMin() == 0 && size.getMax() == 0;
	}

	private static IllegalConfigurationException excluded(final String message) {
		return new IllegalConfigurationException(message);
	}

	public void requireReadProvenance() {
		requireInputProvenance("READ");
	}

	public void requireInputProvenance(final String operation) {
		if (enabled() && inputProvenance == IntegrityInputProvenance.NONE) {
			throw new IllegalConfigurationException(
					"metadata-mode " + operation
							+ " requires explicit storage.integrity.input.provenance");
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
