package com.dell.spt.storage.driver.coop.netty;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.io.InputStream;
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
}
