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
}
