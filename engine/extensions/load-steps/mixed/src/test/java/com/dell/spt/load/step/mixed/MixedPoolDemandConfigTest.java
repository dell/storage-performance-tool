package com.dell.spt.load.step.mixed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.nic.pool.ItemSizeSummary;
import com.dell.spt.base.nic.pool.PoolDemand;
import com.dell.spt.base.nic.pool.PoolDemand.OpKind;
import com.dell.spt.base.nic.pool.PoolDemandConfig;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** A mixed step's one driver: its GETs read the item input, and its PUTs write objects of its configured size. */
final class MixedPoolDemandConfigTest {

	private static final long GIB = 1L << 30;

	private Config config() throws Exception {
		final List<Map<String, Object>> schemas = new java.util.ArrayList<>();
		schemas.add(InitialConfigSchemaProvider.provider().schema());
		SchemaProvider.resolve(com.dell.spt.base.Constants.APP_NAME, getClass().getClassLoader()).forEach(schemas::add);
		schemas.add(new MixedLoadStepExtension().schemaProvider().schema());
		final Config config = new BasicConfig("-", TreeUtil.reduceForest(schemas));
		config.val("load-op-type", "create");
		config.val("item-type", "data");
		config.val("item-data-size", "1GiB");
		config.val("item-data-ranges-threshold", 0);
		config.val("item-data-ranges-random", 0);
		config.val("load-op-weights-get", 50);
		config.val("load-op-weights-put", 30);
		config.val("load-op-weights-delete", 20);
		config.val("load-op-weights-stat", 0);
		config.val("item-input-file", "/spt-input/items/mixed-read-items.csv");
		config.val("storage-driver-type", "s3-rdma");
		config.val("storage-net-interfaces", List.of("ens1"));
		return config;
	}

	private static ItemSizeSummary summary(final long size) {
		final var recorder = new ItemSizeSummary.Recorder("items.csv");
		recorder.record(size);
		recorder.finish();
		return recorder.summary();
	}

	/** The mixed extension's shipped defaults set its weights on every step of a run, so they cannot mark a mixed step. */
	@Test
	void plainStepsIgnoreTheWeightsTheMixedDefaultsGiveEveryStep() throws Exception {
		try (final var shipped = getClass().getResourceAsStream("/config/defaults-load-op-mixed.yaml")) {
			final var text = new String(shipped.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8);
			assertTrue(text.contains("get: 45"), "the premise of this test: " + text);
		}
		for (final var plain : Map.of("create", OpKind.CREATE, "read", OpKind.READ, "delete", OpKind.OTHER).entrySet()) {
			final var config = config();
			config.val("load-op-type", plain.getKey());
			final var operations = PoolDemandConfig.operations(config, file -> summary(4L << 20));
			assertEquals(List.of(plain.getValue()), operations.stream().map(PoolDemand.Operation::kind).toList(), plain.getKey());
			assertEquals(plain.getValue() != OpKind.OTHER, PoolDemandConfig.recordsItemSizes(config), plain.getKey());
		}
	}

	/** The participant's flag decides: a read step's PUTs count only for a mixed step. */
	@Test
	void aMixedParticipantsDemandComesFromItsWeights() throws Exception {
		final var config = config();
		config.val("load-op-type", "read");
		assertTrue(requiredLimit(config, true) > 0, "its PUTs have a known size");
		assertEquals(0, requiredLimit(config, false), "a plain read of a file that cannot be read knows nothing");
	}

	private static long requiredLimit(final Config config, final boolean mixed) throws Exception {
		final var driver = org.mockito.Mockito.mock(com.dell.spt.base.storage.driver.PoolDemandDriver.class);
		org.mockito.Mockito.when(driver.poolLayout()).thenReturn(new com.dell.spt.base.storage.driver.PoolDemandDriver.Layout(
						"host-a", 512 * GIB, 8, 1L << 20, 0, 0, false, List.of("a", "b")));
		org.mockito.Mockito.when(driver.startPrewarm(org.mockito.ArgumentMatchers.any())).thenAnswer(call -> {
			final List<com.dell.spt.base.storage.driver.PoolDemandDriver.PrewarmOrder> orders = call.getArgument(0);
			return orders.stream().map(order -> java.util.concurrent.CompletableFuture.completedFuture(
							new com.dell.spt.base.storage.driver.PoolDemandDriver.PrewarmResult(order.nic(), order.buffers(), 1, null, false))).toList();
		});
		final var preparation = org.mockito.Mockito.mock(com.dell.spt.base.storage.driver.AdmissionPreparable.Preparation.class);
		org.mockito.Mockito.when(preparation.deadlineNanos()).thenReturn(System.nanoTime() + 60_000_000_000L);
		return com.dell.spt.base.nic.pool.PoolPreparation.run("step-1",
						List.of(new com.dell.spt.base.nic.pool.PoolPreparation.Participant(0, driver, config, mixed)), preparation,
						new com.dell.spt.base.nic.pool.PoolPreparation.Environment(System::nanoTime, name -> null))
						.requiredLimitBytes();
	}

	@Test
	void aMixedDriverHasItsListedGetsAndItsPuts() throws Exception {
		final var operations = PoolDemandConfig.mixedOperations(config(), file -> summary(4L << 20));
		assertEquals(List.of(OpKind.READ, OpKind.CREATE), operations.stream().map(PoolDemand.Operation::kind).toList());
		assertInstanceOf(PoolDemand.Listed.class, operations.get(0).sizes());
		assertInstanceOf(PoolDemand.Range.class, operations.get(1).sizes());
		final var demand = PoolDemand.estimate(new PoolDemand.Workload(4, 1L << 20, 0, 0, operations), List.of("a", "b"));
		assertEquals(GIB, demand.nics().get(0).sizeClass(), "the larger of its PUTs and its listed GETs");
		assertTrue(demand.spansClasses());
	}

	@Test
	void aMixedDriverWhoseItemFileIsUnreadKeepsItsPuts() throws Exception {
		final var recorder = new ItemSizeSummary.Recorder("items.csv");
		recorder.fail(ItemSizeSummary.Reason.MALFORMED_ITEM, "bad");
		final var operations = PoolDemandConfig.mixedOperations(config(), file -> recorder.summary());
		final var demand = PoolDemand.estimate(new PoolDemand.Workload(4, 1L << 20, 0, 0, operations), List.of("a", "b"));
		assertEquals(PoolDemand.State.KNOWN, demand.state());
		assertTrue(demand.hasUnknown());
		assertEquals(2 * GIB, demand.maxNeedBytes());
	}

	@Test
	void aMixedStepRecordsItemSizesOnlyForItsGets() throws Exception {
		final var config = config();
		assertTrue(PoolDemandConfig.mixedRecordsItemSizes(config));
		config.val("load-op-weights-get", 0);
		assertFalse(PoolDemandConfig.mixedRecordsItemSizes(config), "its PUTs take the configured size, and a DELETE needs none");
	}
}
