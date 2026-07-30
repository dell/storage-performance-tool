package com.dell.spt.base.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dell.spt.base.Constants;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Proves the storage.integrity configuration contract resolves end to end against the shipped schema
 * and defaults: the disabled default ships, every required path is reachable, nested overrides apply,
 * and an undeclared path is rejected.
 */
class IntegrityConfigTest {

	private static Map<String, Object> schema() throws Exception {
		return SchemaProvider.resolve(Constants.APP_NAME, Thread.currentThread().getContextClassLoader())
						.stream()
						.findFirst()
						.orElseThrow();
	}

	private static Config shippedDefaults() throws Exception {
		final URL defaultsUrl = IntegrityConfigTest.class.getClassLoader().getResource("config/defaults.yaml");
		assertNotNull(defaultsUrl, "defaults.yaml should be on the classpath");
		return ConfigUtil.loadConfig(Path.of(defaultsUrl.toURI()).toFile(), schema());
	}

	@Test
	void shippedDefaultsDisableIntegrity() throws Exception {
		final var config = shippedDefaults();
		assertEquals("none", config.stringVal("storage-integrity-mode"));
		assertEquals("sha256", config.stringVal("storage-integrity-algorithm"));
		assertEquals("none", config.stringVal("storage-integrity-input-provenance"));
		assertEquals("", config.stringVal("storage-integrity-input-expectedProducerId"));
	}

	@Test
	void nestedAndDashedPathsResolveToTheSameLeaves() throws Exception {
		final var integrityConfig = shippedDefaults().configVal("storage").configVal("integrity");
		assertEquals("none", integrityConfig.stringVal("mode"));
		assertEquals("sha256", integrityConfig.stringVal("algorithm"));
		assertEquals("none", integrityConfig.configVal("input").stringVal("provenance"));
		assertEquals("", integrityConfig.configVal("input").stringVal("expectedProducerId"));
	}

	@Test
	void metadataModeOverrideApplies() throws Exception {
		final var config = shippedDefaults();
		config.val("storage-integrity-mode", "metadata");
		config.val("storage-integrity-input-provenance", "engine_step");
		config.val("storage-integrity-input-expectedProducerId", "mt-001-20260730.120000.000-create");
		assertEquals("metadata", config.stringVal("storage-integrity-mode"));
		assertEquals("engine_step", config.stringVal("storage-integrity-input-provenance"));
		assertEquals(
						"mt-001-20260730.120000.000-create",
						config.stringVal("storage-integrity-input-expectedProducerId"));
		assertEquals("sha256", config.stringVal("storage-integrity-algorithm"), "unset leaves keep their defaults");
	}

	/**
	 * The CLI capability probe relies on an integrity-unaware engine rejecting the configuration rather
	 * than silently ignoring it. This proves the schema is strict about undeclared integrity paths.
	 */
	@Test
	void undeclaredIntegrityPathIsRejected() throws Exception {
		final var config = shippedDefaults();
		assertThrows(InvalidValuePathException.class, () -> config.val("storage-integrity-notAKey", "x"));
	}

	@Test
	void runIdDefaultIsZeroSoTheRuntimeAssignsIt() throws Exception {
		assertEquals(0L, shippedDefaults().longVal("run-id"));
	}
}
