package com.dell.spt.base.load.generator;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.data.range.*;
import com.dell.spt.base.load.step.local.context.range.RangeReadRuntime;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.dell.spt.base.storage.driver.range.RangeReadDriverSupport;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class RangeReadGeneratorBuilderTest {
	@Test
	void configuredPolicyMustMatchTheInstalledDriverBeforeGeneration() throws Exception {
		try (var f = new Fixture(new RangeReadPolicy(16, null, 1))) {
			f.config.val("load-op-read-range-size", "8");
			assertThrows(com.dell.spt.base.config.IllegalConfigurationException.class,
							() -> f.builder(true).build());
			verify(f.driver, never()).put(any(RangeReadOperation.class));
		}
		try (var f = new Fixture(new RangeReadPolicy(16, null, 1))) {
			f.config.val("load-op-read-range-size", "16");
			f.generator = f.builder(true).build();
			assertNotNull(f.generator);
			verify(f.driver, never()).put(any(RangeReadOperation.class));
		}
	}

	private static final class Fixture implements AutoCloseable {
		final Config config = TestConfigBuilder.config();
		final StorageDriver<DataItem, RangeReadOperation<DataItem>> driver = mock(StorageDriver.class,
						withSettings().extraInterfaces(RangeReadDriverSupport.class));
		final Input<DataItem> input = mock(Input.class);
		final RangeReadRuntime<DataItem> runtime;
		final AtomicReference<RangeReadOperation<DataItem>> received = new AtomicReference<>();
		final AtomicInteger accounted = new AtomicInteger();
		LoadGeneratorImpl<DataItem, RangeReadOperation<DataItem>> generator;

		Fixture(RangeReadPolicy policy) {
			config.val("load-op-type", "read");
			config.val("load-batch-size", 1);
			config.val("load-op-limit-count", 1);
			config.val("load-op-recycle-mode", false);
			config.val("load-op-retry", false);
			config.val("item-data-ranges-threshold", 0);
			runtime = new RangeReadRuntime<>(policy, 1, driver, (op, circulation) -> {});
			doReturn(runtime).when((RangeReadDriverSupport) driver).rangeReadRuntime();
			when(driver.put(any(RangeReadOperation.class))).thenAnswer(call -> {
				RangeReadOperation<DataItem> op = call.getArgument(0);
				assertTrue(runtime.admission().isAdmitted(op.circulation()));
				received.set(op);
				return runtime.tracker().driverQueued(op);
			});
		}

		LoadGeneratorBuilderImpl<DataItem, RangeReadOperation<DataItem>, LoadGeneratorImpl<DataItem, RangeReadOperation<DataItem>>> builder(boolean inputFirst) {
			var builder = new LoadGeneratorBuilderImpl<DataItem, RangeReadOperation<DataItem>, LoadGeneratorImpl<DataItem, RangeReadOperation<DataItem>>>()
							.itemConfig(config.configVal("item")).loadConfig(config.configVal("load"))
							.itemType(ItemType.DATA).itemFactory(new DataItemFactoryImpl())
							.authConfig(config.configVal("storage-auth")).originIndex(0);
			if (inputFirst)
				builder.itemInput(input).loadOperationsOutput(driver);
			else
				builder.loadOperationsOutput(driver).itemInput(input);
			return builder;
		}

		void build(boolean inputFirst) throws Exception {
			generator = builder(inputFirst).build();
			generator.operationLifecycle(runtime.tracker());
			runtime.bind(generator, runtime.tracker(), false, 0,
							(delay, task) -> CompletableFuture.completedFuture(null),
							op -> accounted.incrementAndGet(), op -> {});
		}

		void emit(DataItem item) throws Exception {
			var reads = new AtomicInteger();
			doAnswer(call -> {
				if (reads.getAndIncrement() != 0)
					throw new EOFException();
				List<DataItem> items = call.getArgument(0);
				items.add(item);
				return 1;
			}).when(input).get(anyList(), anyInt());
			generator.doWork();
		}

		@Override
		public void close() throws Exception {
			if (generator != null)
				generator.close();
			runtime.tracker().expireTerminalDeadline();
			runtime.close();
		}
	}

	@Test
	void fixedRangeSetupNeverSamplesInputOrObjectSizeInEitherSetterOrder() throws Exception {
		for (boolean inputFirst : List.of(false, true)) {
			try (var fixture = new Fixture(new RangeReadPolicy(Long.MAX_VALUE, 0L, 1))) {
				fixture.build(inputFirst);
				verify(fixture.input, never()).get(anyList(), anyInt());
				verify(fixture.input, never()).reset();
				verify(fixture.driver).adjustIoBuffers(StorageDriver.BUFF_SIZE_MIN, OpType.READ);
				var item = spy(new DataItemImpl("object", 7, 0));
				doThrow(new IOException("size unavailable")).when(item).size();
				fixture.emit(item);
				assertNotNull(fixture.received.get());
				assertSame(fixture.runtime.policy(), fixture.received.get().policy());
				assertSame(item, fixture.received.get().item());
				verify(item, never()).size();
				assertEquals(1, fixture.runtime.tracker().counters().selected());
				assertEquals(0, fixture.runtime.snapshot().requestsSent());
				assertEquals(1, fixture.runtime.admission().admittedCirculations());
			}
		}
	}

	@Test
	void randomUnknownSizeIsAccountedLocallyWithoutDriverAdmission() throws Exception {
		try (var fixture = new Fixture(new RangeReadPolicy(2, null, 1))) {
			fixture.build(true);
			fixture.emit(new DataItemImpl("unknown", 0, 0));
			assertNull(fixture.received.get());
			assertEquals(1, fixture.accounted.get());
			assertEquals(1, fixture.runtime.tracker().counters().failed());
			assertEquals(0, fixture.runtime.snapshot().requestsSent());
			assertEquals(0, fixture.runtime.admission().admittedCirculations());
			assertTrue(fixture.runtime.snapshot().reconciled());
		}
	}

	@Test
	void activeLegacyRangesAreRejectedBeforeInputConsumption() throws Exception {
		for (String key : List.of("fixed", "random", "threshold")) {
			try (var fixture = new Fixture(new RangeReadPolicy(2, 0L, 1))) {
				fixture.config.val("item-data-ranges-" + key,
								key.equals("fixed") ? List.of("0-1") : 1);
				assertThrows(IllegalConfigurationException.class, () -> fixture.builder(true).build());
				verify(fixture.input, never()).get(anyList(), anyInt());
			}
		}
	}

	@Test
	void inertReadConcatIsAllowedAndNonReadIsRejected() throws Exception {
		try (var fixture = new Fixture(new RangeReadPolicy(2, 0L, 1))) {
			fixture.config.val("item-data-ranges-concat", "1-2");
			fixture.build(false);
			fixture.emit(new DataItemImpl("object", 0, 10));
			assertNotNull(fixture.received.get());
		}
		try (var fixture = new Fixture(new RangeReadPolicy(2, 0L, 1))) {
			fixture.config.val("load-op-type", "create");
			assertThrows(IllegalConfigurationException.class, () -> fixture.builder(false).build());
			verify(fixture.input, never()).get(anyList(), anyInt());
		}
	}

	@Test
	void discoveryRetainsOriginalDriverAndDoesNotListDuringConstruction() throws Exception {
		try (var fixture = new Fixture(new RangeReadPolicy(2, 0L, 1))) {
			fixture.config.val("item-input-path", "/bucket");
			fixture.config.val("item-input-file", null);
			var item = new DataItemImpl("object", 0, 10);
			when(fixture.driver.list(any(), eq("/bucket"), any(), anyInt(), isNull(), eq(1), any()))
							.thenReturn(List.of(item));
			fixture.generator = fixture.builder(false).itemInput(null).build();
			fixture.generator.operationLifecycle(fixture.runtime.tracker());
			fixture.runtime.bind(fixture.generator, fixture.runtime.tracker(), false, 0,
							(delay, task) -> CompletableFuture.completedFuture(null), op -> {}, op -> {});
			verify(fixture.driver, never()).list(any(), any(), any(), anyInt(), any(), anyInt(), any());
			fixture.generator.doWork();
			assertNotNull(fixture.received.get());
			assertSame(item, fixture.received.get().item());
			verify(fixture.driver).list(any(), eq("/bucket"), any(), anyInt(), isNull(), eq(1), any());
		}
	}

	@Test
	void ordinaryReadStillEstimatesAndResetsSuppliedInputAtBuild() throws Exception {
		try (var fixture = new Fixture(new RangeReadPolicy(2, 0L, 1))) {
			StorageDriver<DataItem, RangeReadOperation<DataItem>> ordinary = mock(StorageDriver.class);
			doAnswer(call -> {
				List<DataItem> items = call.getArgument(0);
				int count = call.getArgument(1);
				for (int i = 0; i < count; i++)
					items.add(new DataItemImpl("object", 0, 8192));
				return count;
			}).when(fixture.input).get(anyList(), anyInt());
			try (var generator = fixture.builder(true).loadOperationsOutput(ordinary).build()) {
				verify(fixture.input).reset();
				verify(ordinary).adjustIoBuffers(8192, OpType.READ);
				assertTrue(generator.supportsRangeRetry());
			}
		}
	}

}
