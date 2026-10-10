package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.storage.driver.endpoint.EndpointSelectionDriverFactory;
import com.github.akurilov.confuse.Config;
import java.util.List;
import org.junit.jupiter.api.Test;

class S3EndpointSelectionFactoryTest {

	private static final RangeReadPolicy RANGE_POLICY = new RangeReadPolicy(65_536, 0L, 4_096);
	private static final SeedDataInput INPUT = new SeedDataInput(1, 1024, 1, true);

	@Test
	void ordinaryS3FactoryOptsIn() {
		assertInstanceOf(EndpointSelectionDriverFactory.class, new S3StorageDriverExtension<>());
	}

	@Test
	void invalidSettingsFailBeforeConstruction() {
		final var root = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		root.val("storage-net-endpoint-hostname", "s3.example.test");

		final var failure = assertThrows(IllegalConfigurationException.class,
						() -> new S3StorageDriverExtension<>().create("step", null, root.configVal("storage"), false, 1));

		assertTrue(failure.getMessage().contains("requires round-robin"), failure.getMessage());
	}

	@Test
	void partialObjectReadsUseTheSelectionDriverForNonDefaultModes() throws Exception {
		for (final var storage : List.of(roundRobin(), perRequestDns())) {
			try (final var driver = new S3StorageDriverExtension<>().createRangeRead("step", INPUT, storage, 1, RANGE_POLICY)) {
				assertInstanceOf(S3RangeEndpointSelectionDriver.class, driver);
			}
		}
	}

	@Test
	void partialObjectReadsKeepTheDefaultRangeDriverInDefaultMode() throws Exception {
		final var storage = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1").configVal("storage");
		storage.val("net-timeoutMilliSec", 1_000);
		try (final var driver = new S3StorageDriverExtension<>().createRangeRead("step", INPUT, storage, 1, RANGE_POLICY)) {
			assertEquals(S3RangeStorageDriver.class, driver.getClass());
		}
	}

	private static Config roundRobin() {
		final var root = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		root.val("storage-net-endpoint-selection", "round-robin");
		root.val("storage-net-endpoint-connect-timeoutMilliSec", 30_000);
		root.val("storage-net-timeoutMilliSec", 1_000);
		return root.configVal("storage");
	}

	private static Config perRequestDns() {
		final var root = S3StorageDriverTest.baseConfig(false, 4, false, null, "s3.example.test");
		root.val("storage-net-endpoint-selection", "per-request-dns");
		root.val("storage-net-endpoint-connect-timeoutMilliSec", 30_000);
		root.val("storage-net-endpoint-dns-timeoutMilliSec", 5_000);
		// An explicit server keeps construction independent of this host's resolver configuration.
		root.val("storage-net-endpoint-dns-server", "127.0.0.1:53");
		root.val("storage-net-timeoutMilliSec", 1_000);
		return root.configVal("storage");
	}
}
