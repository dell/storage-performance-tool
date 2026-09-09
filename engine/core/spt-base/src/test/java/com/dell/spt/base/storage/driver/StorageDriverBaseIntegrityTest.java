package com.dell.spt.base.storage.driver;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
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
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.range.RangeReadOperation;
import com.dell.spt.base.item.op.data.range.RangeReadOperationsBuilder;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.metrics.range.RangeReadMetrics;
import com.github.akurilov.confuse.Config;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class StorageDriverBaseIntegrityTest {

	@Test
	void constructionSelectedTrackerSettlesQueuedRangeRetryAtDriverDeadline() throws Exception {
		try (var driver = rangeDriver()) {
			var policy = new RangeReadPolicy(2, 0L, 1);
			var metrics = new RangeReadMetrics(policy);
			var tracker = OperationLifecycleTracker.<RangeReadOperation<DataItem>> withDeadlineSettlement(
							(op, owner) -> op.circulation().settleAtDeadline(op, owner));
			driver.enableOperationLifecycle(); // Cooperative bases enable ordinary tracking first.
			driver.enableOperationLifecycle(tracker);
			driver.enableOperationLifecycle(); // A later base call must preserve the installed policy.
			assertSame(tracker, driver.operationLifecycle());
			tracker.terminalObserver(op -> metrics.recordFinal(op.circulation()));
			var op = new RangeReadOperationsBuilder<DataItem>(0, policy)
							.buildOp(new DataItemImpl("object", 0, 10));
			assertTrue(tracker.generatorBuffered(op));
			assertTrue(tracker.driverQueued(op));
			var first = op.circulation().beginAttempt(null);
			assertTrue(driver.markOperationDispatched(op));
			assertTrue(metrics.requestHandoff(first));
			assertTrue(first.transportFailure(Operation.Status.FAIL_IO));
			metrics.recordAttempt(op.circulation(), first);
			var retry = op.circulation().beginAttempt(first);
			assertNotNull(retry);
			driver.operationLifecycle().expireTerminalDeadline();
			assertEquals(1, tracker.counters().failed());
			assertEquals(0, tracker.counters().unattempted());
			assertEquals(0, tracker.counters().unresolved());
			assertFalse(retry.requestHandoff());
			assertEquals(1, metrics.snapshot(tracker.counters()).requestsSent());
			assertTrue(metrics.snapshot(tracker.counters()).reconciled());
		}
	}

	@Test
	void trackerInstallationRejectsStartedDriversAndExistingCustody() throws Exception {
		try (var driver = rangeDriver()) {
			var tracker = new OperationLifecycleTracker<RangeReadOperation<DataItem>>();
			assertThrows(IllegalArgumentException.class,
							() -> driver.enableOperationLifecycle(OperationLifecycleTracker.disabled()));
			var op = new RangeReadOperationsBuilder<DataItem>(0, new RangeReadPolicy(2, 0L, 1))
							.buildOp(new DataItemImpl("object", 0, 10));
			assertTrue(tracker.generatorBuffered(op));
			assertThrows(IllegalStateException.class, () -> driver.enableOperationLifecycle(tracker));
			assertFalse(driver.operationLifecycle().isEnabled());
			var installed = new OperationLifecycleTracker<RangeReadOperation<DataItem>>();
			driver.enableOperationLifecycle(installed);
			var other = new RangeReadOperationsBuilder<DataItem>(0, new RangeReadPolicy(2, 0L, 1))
							.buildOp(new DataItemImpl("other", 0, 10));
			assertTrue(installed.generatorBuffered(other));
			assertThrows(IllegalStateException.class,
							() -> driver.enableOperationLifecycle(new OperationLifecycleTracker<>()));
			assertSame(installed, driver.operationLifecycle());
			tracker.unattempted(op);
			installed.unattempted(other);
		}
		try (var driver = rangeDriver()) {
			driver.start();
			assertThrows(IllegalStateException.class,
							() -> driver.enableOperationLifecycle(new OperationLifecycleTracker<>()));
			driver.stop();
			assertThrows(IllegalStateException.class,
							() -> driver.enableOperationLifecycle(new OperationLifecycleTracker<>()));
		}
	}

	@SuppressWarnings("unchecked")
	private static StorageDriverBase<DataItem, RangeReadOperation<DataItem>> rangeDriver() throws Exception {
		return Mockito.mock(StorageDriverBase.class, Mockito.withSettings()
						.useConstructor("range-test", new SeedDataInput(1, 1024, 1, true),
										TestConfigBuilder.config().configVal("storage"), false)
						.defaultAnswer(CALLS_REAL_METHODS));
	}

	@Test
	void directLegacySubclassRetainsDisabledLifecycleCompatibility() throws Exception {
		final var driver = driver(TestConfigBuilder.config());

		assertFalse(driver.operationLifecycle().isEnabled(),
						"only lifecycle-instrumented driver bases may replace the active-count drain");
	}

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
	void metadataModeCountsAdditionalPassOnceOnParentAndSkipsMultipartParts() throws Exception {
		final var driver = driver(metadataConfig());
		doReturn(true).when(driver).integrityAdditionalPayloadPassRequired(any());
		final var parent = operation(OpType.CREATE, null, null, 100);

		driver.prepare(parent);
		driver.prepare(parent);
		assertEquals(1, driver.integrityPerformanceSnapshot().objects());
		assertEquals(1, driver.integrityPerformanceSnapshot().additionalPayloadPasses());

		@SuppressWarnings("unchecked")
		final CompositeDataOperation<DataItem> compositeParent = Mockito.mock(CompositeDataOperation.class);
		final var part = new PartialDataOperationImpl<DataItem>(
						0, OpType.CREATE, new DataItemImpl("object", 0, 50), null, "/bucket", null, 0,
						compositeParent);
		driver.prepare(part);

		assertNull(part.integrityMetadata());
		assertEquals(1, driver.integrityPerformanceSnapshot().objects());
		assertEquals(1, driver.integrityPerformanceSnapshot().additionalPayloadPasses());
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
		config.val("storage-driver-type", "s3");
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
