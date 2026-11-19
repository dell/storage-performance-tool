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
import com.github.akurilov.commons.concurrent.throttle.Throttle;
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
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * The end of scheduling of a finite input, with {@code load-op-retry} on. The input reports its end
 * only on the read after its last item, so that read can follow the last handoff, and the generator,
 * which keeps running for retries, then sits in its idle branch.
 */
@Timeout(value = 30, unit = TimeUnit.SECONDS)
class LoadGeneratorRetryFiniteInputTest {

	private static final long WAIT_MILLIS = 5_000;
	private static final long SETTLE_MILLIS = 100;
	private static final long OPEN = Long.MAX_VALUE;

	private final AtomicInteger signals = new AtomicInteger();
	private final AtomicLong acceptedAtSignal = new AtomicLong(-1);

	@ParameterizedTest
	@CsvSource({"0,1", "1,1", "1,4096", "7,3", "8,4", "1000,1", "4097,4096"
	})
	void aFiniteInputSignalsOnceAndTheGeneratorKeepsRunning(final int items, final int batch) throws Exception {
		final var output = new GatedOutput(OPEN);
		try (var generator = generator(config(true, batch, 0), items, output, null)) {
			generator.start();
			await(() -> signals.get() > 0, "the scheduling end signal");
			settle();
			assertEquals(1, signals.get());
			assertEquals(items, output.accepted.get());
			assertTrue(generator.schedulingExhaustionNanos().isPresent());
			assertFalse(generator.isStopped(), "the generator stays up for retries");
			generator.stop();
		}
	}

	@Test
	void operationsAFullOutputRefusesHoldTheSignalUntilTheFinalHandoff() throws Exception {
		final int items = 5;
		final var output = new GatedOutput(0);
		try (var generator = generator(config(true, 10, 0), items, output, null)) {
			generator.start();
			await(generator::isItemInputFinished, "the end of the input");
			settle();
			assertEquals(0, signals.get(), "operations are still pending");
			output.room.set(OPEN);
			await(() -> signals.get() > 0, "the scheduling end signal");
			settle();
			assertEquals(1, signals.get());
			assertEquals(items, acceptedAtSignal.get(), "the signal follows the last handoff");
			generator.stop();
		}
	}

	@Test
	void operationsARateLimitHoldsBackKeepTheSignalUntilTheFinalHandoff() throws Exception {
		final int items = 5;
		final var output = new GatedOutput(OPEN);
		final var permits = new AtomicInteger();
		try (var generator = generator(config(true, 10, 0), items, output, new CountedThrottle(permits))) {
			generator.start();
			await(generator::isItemInputFinished, "the end of the input");
			permits.set(items - 1);
			await(() -> output.accepted.get() == items - 1, "all but the last handoff");
			settle();
			assertEquals(0, signals.get(), "one operation is still pending");
			permits.set(1);
			await(() -> signals.get() > 0, "the scheduling end signal");
			settle();
			assertEquals(1, signals.get());
			assertEquals(items, acceptedAtSignal.get(), "the signal follows the last handoff");
			generator.stop();
		}
	}

	@ParameterizedTest
	@ValueSource(ints = {0, 5, 4096
	})
	void withoutRetryTheGeneratorSignalsOnceAndStops(final int items) throws Exception {
		final var output = new GatedOutput(OPEN);
		try (var generator = generator(config(false, 4096, 0), items, output, null)) {
			generator.start();
			await(generator::isStopped, "the generator to stop");
			assertEquals(1, signals.get());
			assertEquals(items, output.accepted.get());
			assertTrue(generator.schedulingExhaustionNanos().isPresent());
		}
	}

	@ParameterizedTest
	@CsvSource({"3,5", "5,5"
	})
	void aCountLimitWithinTheInputKeepsItsOwnSingleSignal(final int count, final int items) throws Exception {
		final var output = new GatedOutput(OPEN);
		try (var generator = generator(config(true, 2, count), items, output, null)) {
			generator.start();
			await(() -> signals.get() > 0, "the scheduling end signal");
			settle();
			assertEquals(1, signals.get());
			assertEquals(count, output.accepted.get());
			assertTrue(generator.schedulingExhaustionNanos().isEmpty(), "the input was not read to its end");
			assertFalse(generator.isStopped());
			generator.stop();
		}
	}

	@Test
	void aCountLimitAboveTheInputSignalsOnceAtExhaustion() throws Exception {
		final int items = 5;
		final var output = new GatedOutput(OPEN);
		try (var generator = generator(config(true, 2, items + 3), items, output, null)) {
			generator.start();
			await(() -> signals.get() > 0, "the scheduling end signal");
			settle();
			assertEquals(1, signals.get());
			assertEquals(items, output.accepted.get());
			assertTrue(generator.schedulingExhaustionNanos().isPresent());
			generator.stop();
		}
	}

	@Test
	void aRetryAfterTheSignalIsDispatchedAndDoesNotSignalAgain() throws Exception {
		final int items = 3;
		final var output = new GatedOutput(OPEN);
		try (var generator = generator(config(true, 10, 0), items, output, null)) {
			generator.start();
			await(() -> signals.get() > 0, "the scheduling end signal");
			final long signalledAt = generator.schedulingExhaustionNanos().orElseThrow();
			generator.retry(new DataOperationImpl<>(
							0, OpType.CREATE, new DataItemImpl("retried", 0, 64), null, "bucket", null, List.of(), 0));
			await(() -> output.accepted.get() == items + 1, "the retry to be dispatched");
			settle();
			assertEquals(1, signals.get());
			assertEquals(signalledAt, generator.schedulingExhaustionNanos().orElseThrow());
			assertFalse(generator.isStopped());
			generator.stop();
		}
	}

	@Test
	void withRecycleTheIdleBranchNeverSignals() throws Exception {
		final int items = 5;
		final var output = new GatedOutput(OPEN);
		final Config config = config(true, 10, 0);
		config.val("load-op-recycle-mode", true);
		try (var generator = generator(config, items, output, null)) {
			generator.start();
			await(() -> generator.isItemInputFinished() && output.accepted.get() == items, "the input to be handed over");
			settle();
			assertEquals(0, signals.get());
			assertTrue(generator.schedulingExhaustionNanos().isEmpty());
			assertFalse(generator.isStopped());
			generator.stop();
		}
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
					final Config config, final int items, final GatedOutput output, final Throttle throttle)
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
						.originIndex(0);
		if (throttle != null) {
			builder.addThrottle(throttle);
		}
		final LoadGenerator<DataItem, Operation<DataItem>> generator = builder.build();
		generator.onSchedulingExhaustion(() -> {
			acceptedAtSignal.compareAndSet(-1, output.accepted.get());
			signals.incrementAndGet();
		});
		return generator;
	}

	private static void await(final BooleanSupplier condition, final String description) throws InterruptedException {
		final long deadline = System.currentTimeMillis() + WAIT_MILLIS;
		while (!condition.getAsBoolean()) {
			assertTrue(System.currentTimeMillis() < deadline, "timed out waiting for " + description);
			Thread.sleep(2);
		}
	}

	/** Gives the work loop many more passes, in which a wrong or repeated signal would show. */
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

	/** Accepts operations while it has room. */
	private static final class GatedOutput implements Output<Operation<DataItem>> {
		final AtomicLong room;
		final AtomicLong accepted = new AtomicLong();

		GatedOutput(final long room) {
			this.room = new AtomicLong(room);
		}

		@Override
		public boolean put(final Operation<DataItem> op) {
			if (room.get() <= 0) {
				return false;
			}
			if (room.get() != OPEN) {
				room.decrementAndGet();
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

	/** Grants only the permits the test has released. */
	private record CountedThrottle(AtomicInteger permits) implements Throttle {

	@Override
	public boolean tryAcquire() {
		return tryAcquire(1) == 1;
	}

	@Override
	public int tryAcquire(final int requested) {
		final int granted = Math.min(requested, Math.max(0, permits.get()));
		permits.addAndGet(-granted);
		return granted;
	}
}}
