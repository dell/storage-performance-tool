package com.dell.spt.base.load.generator;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.lang.Exceptions;
import com.github.akurilov.confuse.Config;
import java.io.EOFException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A generator leaves its step's weight cycle when its scheduling ends. Origin 0 is the generator;
 * origin 1 stands for the other sub-load of the step, and gets a new share only once the generator,
 * whose own share is far from used, has left the cycle.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class LoadGeneratorWeightCycleTest {

	private static final long WAIT_MILLIS = 5_000;
	private static final long SETTLE_MILLIS = 100;
	private static final int OTHER_ORIGIN = 1;
	private static final int OTHER_WEIGHT = 5;

	private final RetiringWeightsThrottle cycle = new RetiringWeightsThrottle(new int[]{1_000_000, OTHER_WEIGHT
	});
	private final AtomicInteger signals = new AtomicInteger();

	@ParameterizedTest
	@ValueSource(booleans = {false, true
	})
	void reachingTheCountLimitLeavesTheCycle(final boolean retry) throws Exception {
		final var output = new CountingOutput(true);
		try (var generator = generator(config(retry, 2, 3), 100, output, true)) {
			generator.start();
			awaitCycleLeft();
			assertEquals(3, output.accepted.get());
			settle();
			assertEquals(1, signals.get());
			assertTrue(generator.schedulingExhaustionNanos().isEmpty(), "the input was not read to its end");
			generator.stop();
		}
	}

	@Test
	void theCycleIsLeftWithNoListenerSet() throws Exception {
		final var output = new CountingOutput(true);
		try (var generator = generator(config(false, 2, 3), 100, output, false)) {
			generator.start();
			awaitCycleLeft();
			assertEquals(3, output.accepted.get());
			generator.stop();
		}
	}

	@Test
	void theEndOfTheInputLeavesTheCycle() throws Exception {
		final var output = new CountingOutput(true);
		try (var generator = generator(config(false, 10, 0), 5, output, true)) {
			generator.start();
			awaitCycleLeft();
			assertEquals(5, output.accepted.get());
			await(generator::isStopped, "the generator to stop");
			assertEquals(1, signals.get());
			assertTrue(generator.schedulingExhaustionNanos().isPresent());
		}
	}

	/** With retry on, the input's end is read after the last handoff, so the idle branch sees it. */
	@Test
	void theRetryIdleBranchLeavesTheCycleThroughTheSchedulingEnd() throws Exception {
		final var output = new CountingOutput(true);
		try (var generator = generator(config(true, 10, 0), 5, output, true)) {
			generator.start();
			awaitCycleLeft();
			settle();
			assertEquals(5, output.accepted.get());
			assertEquals(1, signals.get(), "the listener runs exactly once");
			assertTrue(generator.schedulingExhaustionNanos().isPresent());
			assertFalse(generator.isStopped(), "the generator stays up for retries");
			generator.stop();
		}
	}

	@Test
	void operationsStillToHandOverKeepTheGeneratorInTheCycle() throws Exception {
		final var output = new CountingOutput(false);
		try (var generator = generator(config(true, 10, 0), 5, output, true)) {
			generator.start();
			await(generator::isItemInputFinished, "the end of the input");
			settle();
			assertFalse(cycleWasLeft());
			assertEquals(0, signals.get());
			output.open = true;
			awaitCycleLeft();
			assertEquals(5, output.accepted.get());
			generator.stop();
		}
	}

	@Test
	void aRetryAfterLeavingTheCycleIsDispatched() throws Exception {
		final var output = new CountingOutput(true);
		try (var generator = generator(config(true, 10, 0), 3, output, true)) {
			generator.start();
			awaitCycleLeft();
			generator.retry(new DataOperationImpl<>(
							0, OpType.CREATE, new DataItemImpl("retried", 0, 64), null, "bucket", null, List.of(), 0));
			await(() -> output.accepted.get() == 4, "the retry to be dispatched");
			settle();
			assertEquals(1, signals.get());
			generator.stop();
		}
	}

	private void awaitCycleLeft() throws InterruptedException {
		await(this::cycleWasLeft, "the generator to leave the weight cycle");
	}

	/** Uses up the other origin's share, then asks for more: only a new cycle can answer. */
	private boolean cycleWasLeft() {
		cycle.tryAcquire(OTHER_ORIGIN, OTHER_WEIGHT);
		return cycle.tryAcquire(OTHER_ORIGIN, OTHER_WEIGHT) == OTHER_WEIGHT;
	}

	private static Config config(final boolean retry, final int batch, final long count) {
		final Config config = TestConfigBuilder.config();
		config.val("item-type", "data");
		config.val("item-data-ranges-concat", null);
		config.val("load-batch-size", batch);
		config.val("load-op-limit-count", count);
		config.val("load-op-recycle-mode", false);
		config.val("load-op-retry", retry);
		return config;
	}

	@SuppressWarnings({"unchecked", "rawtypes"
	})
	private LoadGenerator<DataItem, Operation<DataItem>> generator(
					final Config config, final int items, final CountingOutput output, final boolean listener)
					throws Exception {
		final ItemType itemType = ItemType.DATA;
		final LoadGeneratorBuilder builder = new LoadGeneratorBuilderImpl<>()
						.itemConfig(config.configVal("item"))
						.loadConfig(config.configVal("load"))
						.itemType(itemType)
						.itemFactory((ItemFactory) ItemType.getItemFactory(itemType))
						.itemInput(new EndReportedLateInput(items))
						.loadOperationsOutput(output)
						.authConfig(config.configVal("storage").configVal("auth"))
						.originIndex(0)
						.addThrottle(cycle);
		final LoadGenerator<DataItem, Operation<DataItem>> generator = builder.build();
		if (listener) {
			generator.onSchedulingExhaustion(signals::incrementAndGet);
		}
		return generator;
	}

	private static void await(final BooleanSupplier condition, final String description) throws InterruptedException {
		final long deadline = System.currentTimeMillis() + WAIT_MILLIS;
		while (!condition.getAsBoolean()) {
			assertTrue(System.currentTimeMillis() < deadline, "timed out waiting for " + description);
			Thread.sleep(2);
		}
	}

	private static void settle() throws InterruptedException {
		Thread.sleep(SETTLE_MILLIS);
	}

	/** Like {@code CsvItemInput}: a short batch first, and the end only on the next read. */
	private static final class EndReportedLateInput implements Input<DataItem> {
		private final int total;
		private int produced;

		EndReportedLateInput(final int total) {
			this.total = total;
		}

		@Override
		public DataItem get() {
			return produced < total ? new DataItemImpl("item-" + produced++, 0, 64) : null;
		}

		@Override
		public int get(final List<DataItem> buffer, final int limit) {
			if (produced >= total) {
				Exceptions.throwUnchecked(new EOFException());
			}
			final int n = Math.min(limit, total - produced);
			for (int i = 0; i < n; i++) {
				buffer.add(get());
			}
			return n;
		}

		@Override
		public long skip(final long count) {
			return 0;
		}

		/** The builder samples a supplied input for its transfer size, then rewinds it. */
		@Override
		public void reset() {
			produced = 0;
		}

		@Override
		public void close() {}
	}

	/** Accepts operations while it is open. */
	private static final class CountingOutput implements Output<Operation<DataItem>> {
		final AtomicLong accepted = new AtomicLong();
		volatile boolean open;

		CountingOutput(final boolean open) {
			this.open = open;
		}

		@Override
		public boolean put(final Operation<DataItem> op) {
			if (!open) {
				return false;
			}
			accepted.incrementAndGet();
			return true;
		}

		@Override
		public int put(final List<Operation<DataItem>> ops, final int from, final int to) {
			int i = from;
			while (i < to && put(ops.get(i))) {
				i++;
			}
			return i - from;
		}

		@Override
		public int put(final List<Operation<DataItem>> ops) {
			return put(ops, 0, ops.size());
		}

		@Override
		public Input<Operation<DataItem>> getInput() {
			throw new AssertionError();
		}

		@Override
		public void close() {}
	}
}
