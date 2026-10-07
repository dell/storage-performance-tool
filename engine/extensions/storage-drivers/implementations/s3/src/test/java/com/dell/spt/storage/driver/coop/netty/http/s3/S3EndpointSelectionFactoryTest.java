package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.storage.driver.endpoint.EndpointSelectionDriverFactory;
import com.github.akurilov.confuse.Config;
import java.util.List;
import org.junit.jupiter.api.Test;

class S3EndpointSelectionFactoryTest {

	private static final RangeReadPolicy RANGE_POLICY = new RangeReadPolicy(65_536, 0L, 4_096);

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
	void partialObjectReadsRejectNonDefaultSelection() {
		for (final var storage : List.of(roundRobin(), perRequestDns())) {
			final var failure = assertThrows(IllegalConfigurationException.class,
							() -> new S3StorageDriverExtension<>().createRangeRead("step", null, storage, 1, RANGE_POLICY));
			assertTrue(failure.getMessage().contains("Partial-object Reads"), failure.getMessage());
		}
	}

	private static Config roundRobin() {
		final var root = S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1");
		root.val("storage-net-endpoint-selection", "round-robin");
		root.val("storage-net-endpoint-connect-timeoutMilliSec", 30_000);
		return root.configVal("storage");
	}

	private static Config perRequestDns() {
		final var root = S3StorageDriverTest.baseConfig(false, 4, false, null, "s3.example.test");
		root.val("storage-net-endpoint-selection", "per-request-dns");
		root.val("storage-net-endpoint-connect-timeoutMilliSec", 30_000);
		root.val("storage-net-endpoint-dns-timeoutMilliSec", 5_000);
		return root.configVal("storage");
	}
}
