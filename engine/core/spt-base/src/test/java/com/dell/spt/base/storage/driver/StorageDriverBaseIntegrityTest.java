package com.dell.spt.base.storage.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doReturn;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.github.akurilov.confuse.Config;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class StorageDriverBaseIntegrityTest {

	@Test
	void disabledModeDoesNotAllocateOrTraverseIntegrityState() throws Exception {
		final var driver = driver(TestConfigBuilder.config());
		final var op = operation(OpType.CREATE, null, null, 100);

		driver.prepare(op);

		assertEquals(0, driver.integrityDigestWorkerCount());
		assertNull(driver.integrityPerformanceSnapshot());
		assertNull(op.integrityMetadata());
	}

	@Test
	void metadataModePrehashesCreateExactlyOnceAcrossRetryReset() throws Exception {
		final var config = metadataConfig();
		final var driver = driver(config);
		final var op = operation(OpType.CREATE, null, null, 100);

		driver.prepare(op);
		final var firstMetadata = op.integrityMetadata();
		driver.prepare(op);

		assertNotNull(firstMetadata);
		assertEquals(firstMetadata, op.integrityMetadata());
		assertEquals(100, firstMetadata.size());
		assertEquals(1, driver.integrityPerformanceSnapshot().objects());
	}

	@Test
	void metadataModeRejectsUnsafeOperationsBeforeRequestDispatch() throws Exception {
		final var missingProvenanceDriver = driver(metadataConfig());
		final var missingProvenanceOp = operation(OpType.READ, null, null, 100);
		final var provenanceFailure = assertThrows(
						IntegrityTerminalException.class,
						() -> missingProvenanceDriver.prepare(missingProvenanceOp));
		assertEquals(
						IntegrityTerminalException.Category.CONFIGURATION, provenanceFailure.category());

		final var externalConfig = metadataConfig();
		externalConfig.val("storage-integrity-input-provenance", "external");
		final var rangeDriver = driver(externalConfig);
		final var rangeOp = operation(
						OpType.READ,
						List.of(new com.github.akurilov.commons.collection.Range(0, 9, -1)),
						null,
						100);
		final var rangeFailure = assertThrows(
						IntegrityTerminalException.class, () -> rangeDriver.prepare(rangeOp));
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, rangeFailure.category());

		final var updateDriver = driver(metadataConfig());
		final var updateOp = operation(OpType.UPDATE, null, null, 100);
		assertThrows(IntegrityTerminalException.class, () -> updateDriver.prepare(updateOp));
	}

	private static Config metadataConfig() {
		final var config = TestConfigBuilder.config();
		config.val("storage-integrity-mode", "metadata");
		return config;
	}

	@SuppressWarnings("unchecked")
	private static StorageDriverBase<DataItem, DataOperation<DataItem>> driver(final Config config)
					throws Exception {
		final var dataInput = new SeedDataInput(1, 1024, 1, true);
		final StorageDriverBase<DataItem, DataOperation<DataItem>> driver = Mockito.mock(
						StorageDriverBase.class,
						Mockito.withSettings()
										.useConstructor("test", dataInput, config.configVal("storage"), false)
										.defaultAnswer(CALLS_REAL_METHODS));
		doReturn("/bucket").when(driver).requestNewPath(any());
		doReturn(null).when(driver).requestNewAuthToken(any());
		return driver;
	}

	private static DataOperation<DataItem> operation(
					final OpType type,
					final List<com.github.akurilov.commons.collection.Range> ranges,
					final String srcPath,
					final long size) {
		final var item = new DataItemImpl("object", 0, size);
		return new DataOperationImpl<>(0, type, item, srcPath, "/bucket", null, ranges, 0);
	}
}
