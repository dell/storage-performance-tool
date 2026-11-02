package com.dell.spt.storage.driver.coop.netty;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.CliArgUtil;
import com.dell.spt.base.config.NicSetConfig;
import com.dell.spt.base.env.Extension;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Guards the shipped {@code storage.net.timeoutMilliSec} default against silent regression.
 *
 * <p>This is a real-resource integration check (loads the actual packaged
 * {@code defaults-storage-net.yaml}, not a copy/mock of it) rather than a unit test against
 * a hand-written config tree, so it fails if the shipped default ever drifts back toward the
 * old 300000ms (5 minute) value -- which was long enough that a stuck S3 operation could
 * never be caught by Netty's own {@code IdleStateHandler} within a typical short test run
 * (see CHANGELOG.md [Unreleased]).
 */
@SuppressWarnings("unchecked")
class NettyDefaultsConfigTest {

	private static final String DEFAULTS_RESOURCE = "/config/defaults-storage-net.yaml";

	@Test
	void shippedDefaultTimeoutIsThirtySeconds() throws Exception {
		final Map<String, Object> root;
		try (final InputStream in = NettyDefaultsConfigTest.class.getResourceAsStream(DEFAULTS_RESOURCE)) {
			assertNotNull(in, "missing packaged resource: " + DEFAULTS_RESOURCE);
			root = new YAMLMapper().readValue(in, Map.class);
		}

		final var storage = (Map<String, Object>) root.get("storage");
		assertNotNull(storage, "defaults YAML has no 'storage' branch");
		final var net = (Map<String, Object>) storage.get("net");
		assertNotNull(net, "defaults YAML has no 'storage.net' branch");

		final var timeoutMilliSec = (Number) net.get("timeoutMilliSec");
		assertNotNull(timeoutMilliSec, "storage.net.timeoutMilliSec is not set in the shipped defaults");
		assertEquals(30_000, timeoutMilliSec.intValue(),
						"storage.net.timeoutMilliSec default regressed from the intended 30s idle timeout "
										+ "(matches the sibling S3-AWS driver's socketTimeoutMs default) -- "
										+ "5 minutes was long enough that a stuck operation could never be "
										+ "caught within a typical test run; see CHANGELOG.md [Unreleased]");

		// The same value also bounds the initial connection-establish wait
		// (NettyStorageDriverBase's ChannelOption.CONNECT_TIMEOUT_MILLIS) -- confirm it stayed
		// comfortably generous for that purpose too rather than accidentally shrinking further.
		assertTrue(timeoutMilliSec.intValue() >= 10_000,
						"storage.net.timeoutMilliSec also bounds initial connection attempts; "
										+ "keep it well above normal same-datacenter connect latency");
	}

	@Test
	void shippedNicSetIsEmptyAndDeclaredAsAList() throws Exception {
		final Map<String, Object> defaults;
		try (final InputStream in = NettyDefaultsConfigTest.class.getResourceAsStream(DEFAULTS_RESOURCE)) {
			defaults = new YAMLMapper().readValue(in, Map.class);
		}
		final var net = (Map<String, Object>) ((Map<String, Object>) defaults.get("storage")).get("net");
		assertEquals(List.of(), net.get("interfaces"), "storage.net.interfaces must ship empty: the kernel's route");

		final Map<String, Object> schema;
		try (final InputStream in = NettyDefaultsConfigTest.class.getResourceAsStream("/config-schema-storage-net.yaml")) {
			assertNotNull(in, "missing packaged resource: /config-schema-storage-net.yaml");
			schema = new YAMLMapper().readValue(in, Map.class);
		}
		final var netSchema = (Map<String, Object>) ((Map<String, Object>) schema.get("storage")).get("net");
		assertEquals("list", netSchema.get("interfaces"));
	}

	@Test
	void engineCliSetsTheNicSetThroughTheMergedSchema() throws Exception {
		final var schemas = new ArrayList<Map<String, Object>>();
		for (final var extension : Extension.load(Thread.currentThread().getContextClassLoader())) {
			final var provider = extension.schemaProvider();
			if (provider != null && provider.schema() != null) {
				schemas.add(provider.schema());
			}
		}
		SchemaProvider.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
						.stream().findFirst().ifPresent(schemas::add);
		final var schema = TreeUtil.reduceForest(schemas);
		final var config = new BasicConfig("-", schema,
						Map.of("storage", Map.of("net", Map.of("interfaces", List.of()))));
		assertEquals(List.of(), NicSetConfig.interfaces(config.configVal("storage")));

		CliArgUtil.parseArgs("--storage-net-interfaces=ens2np0,192.0.2.10").forEach(config::val);
		assertEquals(List.of("ens2np0", "192.0.2.10"), NicSetConfig.interfaces(config.configVal("storage")));
		CliArgUtil.parseArgs("--storage-net-interfaces=").forEach(config::val);
		assertEquals(List.of(), NicSetConfig.interfaces(config.configVal("storage")));
	}
}
