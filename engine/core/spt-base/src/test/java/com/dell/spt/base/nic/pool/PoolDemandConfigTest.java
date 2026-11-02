package com.dell.spt.base.nic.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.nic.pool.PoolDemand.OpKind;
import com.dell.spt.base.nic.pool.PoolDemand.Operation;
import com.github.akurilov.confuse.Config;
import java.util.List;
import org.junit.jupiter.api.Test;

/** What Pool Demand reads from a step's effective configuration. */
final class PoolDemandConfigTest {

	private static final ItemSizeSummary EMPTY = emptySummary();

	private static ItemSizeSummary emptySummary() {
		final var recorder = new ItemSizeSummary.Recorder("scanned");
		recorder.finish();
		return recorder.summary();
	}

	private static Config root(final String op) {
		final Config config = TestConfigBuilder.config();
		config.val("load-op-type", op);
		config.val("item-type", "data");
		config.val("item-data-size", "1GiB");
		config.val("item-data-ranges-threshold", 0);
		config.val("item-data-ranges-random", 0);
		config.val("storage-driver-type", "s3-rdma");
		return config;
	}

	private static Operation only(final Config config) {
		final var operations = PoolDemandConfig.operations(config, file -> EMPTY);
		assertEquals(1, operations.size());
		return operations.get(0);
	}

	@Test
	void aCreateTakesTheConfiguredSizeAndItsPartSize() {
		final var config = root("create");
		config.val("item-data-size", "3MiB-4MiB");
		config.val("item-data-ranges-threshold", "64MiB");
		final var operation = only(config);
		assertEquals(OpKind.CREATE, operation.kind());
		assertEquals(64L << 20, operation.partBytes());
		final var sizes = assertInstanceOf(PoolDemand.Range.class, operation.sizes());
		assertEquals(3L << 20, sizes.min());
		assertEquals(4L << 20, sizes.max());
		assertEquals("item.data.size", sizes.source());
	}

	@Test
	void aReadWithoutAnInputTakesTheConfiguredSize() {
		assertInstanceOf(PoolDemand.Range.class, only(root("read")).sizes());
	}

	@Test
	void anOperationThatUsesNoRdmaIsOther() {
		for (final var op : List.of("delete", "noop", "list")) {
			assertEquals(OpKind.OTHER, only(root(op)).kind(), op);
		}
	}

	@Test
	void aPathItemIsOther() {
		final var config = root("read");
		config.val("item-type", "path");
		assertEquals(OpKind.OTHER, only(config).kind());
	}

	@Test
	void fixedRandomAndSingleRangesAreRanged() {
		final var fixed = root("read");
		fixed.val("item-data-ranges-fixed", List.of("0-1023"));
		assertTrue(only(fixed).ranged());
		final var random = root("read");
		random.val("item-data-ranges-random", 2);
		assertTrue(only(random).ranged());
		final var single = root("read");
		single.val("load-op-read-range-size", "1MiB");
		assertTrue(only(single).ranged());
		assertFalse(only(root("read")).ranged());
	}

	@Test
	void anItemFileIsScannedWhenTheConfigurationHoldsNoSummary() {
		final var config = root("read");
		config.val("item-input-file", "/items.csv");
		assertEquals("/items.csv", PoolDemandConfig.fileToScan(config));
		final var sizes = assertInstanceOf(PoolDemand.Listed.class, only(config).sizes());
		assertSame(EMPTY, sizes.summary());
	}

	@Test
	void aSummaryInTheConfigurationIsUsedAndTheFileIsNotScanned() {
		final var recorder = new ItemSizeSummary.Recorder("entry");
		recorder.record(4 << 20);
		recorder.finish();
		final var config = root("read");
		config.val("item-input-file", "/slice");
		config.val(ItemSizeSummary.CONFIG_PATH, recorder.summary().encode());
		final var operations = PoolDemandConfig.operations(config, file -> {
			throw new AssertionError("a worker never reads its slice twice");
		});
		final var sizes = assertInstanceOf(PoolDemand.Listed.class, operations.get(0).sizes());
		assertEquals("entry", sizes.summary().source());
		assertEquals(null, PoolDemandConfig.fileToScan(config));
	}

	@Test
	void aSummaryThatCannotBeReadIsUnknownAndNeverFailsTheStep() {
		final var config = root("read");
		config.val(ItemSizeSummary.CONFIG_PATH, "{not json");
		assertInstanceOf(PoolDemand.NotKnown.class, only(config).sizes());
	}

	@Test
	void aBucketListingIsUnknown() {
		final var config = root("read");
		config.val("item-input-path", "/bucket");
		final var sizes = assertInstanceOf(PoolDemand.NotKnown.class, only(config).sizes());
		assertTrue(sizes.reason().contains("/bucket"), sizes.reason());
	}

	@Test
	void onlyAnRdmaDriverWithANicSetAndAReadOrCreateRecordsItemSizes() {
		final var withNics = root("read");
		withNics.val("storage-net-interfaces", List.of("ens1"));
		assertTrue(PoolDemandConfig.applies(withNics));
		assertTrue(PoolDemandConfig.recordsItemSizes(withNics));

		assertFalse(PoolDemandConfig.applies(root("read")), "no NIC set");
		assertFalse(PoolDemandConfig.recordsItemSizes(root("read")));

		final var s3 = root("read");
		s3.val("storage-net-interfaces", List.of("ens1"));
		s3.val("storage-driver-type", "s3");
		assertFalse(PoolDemandConfig.applies(s3), "s3 has no NIC Pools");

		final var delete = root("delete");
		delete.val("storage-net-interfaces", List.of("ens1"));
		assertFalse(PoolDemandConfig.recordsItemSizes(delete), "a DELETE has zero demand");
	}

	@Test
	void anyValueButWarnFailsOnShortfall() {
		assertTrue(PoolDemandConfig.failsOnShortfall("fail"));
		assertFalse(PoolDemandConfig.failsOnShortfall("warn"));
	}
}
