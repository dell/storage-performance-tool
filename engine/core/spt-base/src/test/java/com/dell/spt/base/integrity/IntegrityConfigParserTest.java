package com.dell.spt.base.integrity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TestConfigBuilder;
import org.junit.jupiter.api.Test;

class IntegrityConfigParserTest {

	@Test
	void parsesDisabledDefaultsWithoutAllocatingEnabledState() {
		final var parsed = IntegrityConfig.fromStorage(
						TestConfigBuilder.config().configVal("storage"));
		assertFalse(parsed.enabled());
		assertEquals(IntegrityInputProvenance.NONE, parsed.inputProvenance());
	}

	@Test
	void parsesMetadataModeAndRequiresExplicitReadProvenance() {
		final var config = TestConfigBuilder.config();
		config.val("storage-integrity-mode", "metadata");
		var parsed = IntegrityConfig.fromStorage(config.configVal("storage"));
		assertTrue(parsed.enabled());
		assertThrows(IllegalConfigurationException.class, parsed::requireReadProvenance);

		config.val("storage-integrity-input-provenance", "external");
		parsed = IntegrityConfig.fromStorage(config.configVal("storage"));
		parsed.requireReadProvenance();
		assertEquals(IntegrityInputProvenance.EXTERNAL, parsed.inputProvenance());
	}

	@Test
	void rejectsUnknownModeAlgorithmAndProvenance() {
		final var unknownMode = TestConfigBuilder.config();
		unknownMode.val("storage-integrity-mode", "other");
		assertThrows(IllegalConfigurationException.class,
						() -> IntegrityConfig.fromStorage(unknownMode.configVal("storage")));

		var config = TestConfigBuilder.config();
		config.val("storage-integrity-mode", "metadata");
		config.val("storage-integrity-algorithm", "crc32c");
		final var unsupportedAlgorithm = config;
		assertThrows(IllegalConfigurationException.class,
						() -> IntegrityConfig.fromStorage(unsupportedAlgorithm.configVal("storage")));

		config = TestConfigBuilder.config();
		config.val("storage-integrity-input-provenance", "untrusted");
		final var unsupportedProvenance = config;
		assertThrows(IllegalConfigurationException.class,
						() -> IntegrityConfig.fromStorage(unsupportedProvenance.configVal("storage")));
	}

	@Test
	void validatesResolvedIntegrityExclusionsBeforeExecution() {
		for (final String opType : new String[]{"create", "read", "list", "delete"
		}) {
			IntegrityConfig.validateLoadStep(metadataStep(opType));
		}

		final var unsupported = metadataStep("create");
		unsupported.val("storage-driver-type", "dummy-mock");
		assertThrows(IllegalConfigurationException.class, () -> IntegrityConfig.validateLoadStep(unsupported));

		for (final String path : new String[]{
				"item-data-input-file", "item-input-file", "item-data-ranges-concat"
		}) {
			final var config = metadataStep("create");
			config.val(path, "configured");
			assertThrows(IllegalConfigurationException.class, () -> IntegrityConfig.validateLoadStep(config), path);
		}
		for (final String path : new String[]{"load-op-recycle-mode", "load-op-recycle-content-update"
		}) {
			final var config = metadataStep("create");
			config.val(path, true);
			assertThrows(IllegalConfigurationException.class, () -> IntegrityConfig.validateLoadStep(config), path);
		}
		final var update = metadataStep("update");
		assertThrows(IllegalConfigurationException.class, () -> IntegrityConfig.validateLoadStep(update));
		final var rangeRandom = metadataStep("read");
		rangeRandom.val("item-data-ranges-random", 1);
		assertThrows(IllegalConfigurationException.class, () -> IntegrityConfig.validateLoadStep(rangeRandom));
		final var rangeFixed = metadataStep("read");
		rangeFixed.val("item-data-ranges-fixed", java.util.List.of("0-1"));
		assertThrows(IllegalConfigurationException.class, () -> IntegrityConfig.validateLoadStep(rangeFixed));
		final var missingProvenance = metadataStep("read");
		missingProvenance.val("storage-integrity-input-provenance", "none");
		assertThrows(IllegalConfigurationException.class, () -> IntegrityConfig.validateLoadStep(missingProvenance));
		final var negativeSelection = metadataStep("list");
		negativeSelection.val("storage-integrity-selection-maxCount", -1L);
		assertThrows(IllegalConfigurationException.class, () -> IntegrityConfig.validateLoadStep(negativeSelection));
	}

	@Test
	void missingStorageConfigurationIsFatalRatherThanDisabled() {
		assertThrows(IllegalConfigurationException.class, () -> IntegrityConfig.fromStorage(null));
	}

	@Test
	void exactOutputCountPolicyRequiresFiniteCreate() {
		final var create = metadataStep("create");
		create.val("storage-integrity-output-requireExactCount", true);
		create.val("load-op-limit-count", 2L);
		create.val("item-output-file", "written.csv");
		IntegrityConfig.validateLoadStep(create);

		final var unlimitedCreate = metadataStep("create");
		unlimitedCreate.val("storage-integrity-output-requireExactCount", true);
		unlimitedCreate.val("item-output-file", "written.csv");
		assertThrows(
						IllegalConfigurationException.class,
						() -> IntegrityConfig.validateLoadStep(unlimitedCreate));

		final var read = metadataStep("read");
		read.val("storage-integrity-output-requireExactCount", true);
		read.val("load-op-limit-count", 2L);
		assertThrows(
						IllegalConfigurationException.class,
						() -> IntegrityConfig.validateLoadStep(read));

		final var disabled = TestConfigBuilder.config();
		disabled.val("storage-integrity-output-requireExactCount", true);
		assertThrows(
						IllegalConfigurationException.class,
						() -> IntegrityConfig.validateLoadStep(disabled));
	}

	private static com.github.akurilov.confuse.Config metadataStep(final String opType) {
		final var config = TestConfigBuilder.config();
		config.val("storage-driver-type", "s3");
		config.val("storage-integrity-mode", "metadata");
		config.val("storage-integrity-input-provenance", "external");
		config.val("load-op-type", opType);
		return config;
	}
}
