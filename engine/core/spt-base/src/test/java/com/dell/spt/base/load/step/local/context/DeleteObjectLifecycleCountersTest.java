package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import com.dell.spt.base.load.failure.ObjectFailureBudgetConfig;
import com.dell.spt.base.load.failure.ObjectFailureBudgetController;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class DeleteObjectLifecycleCountersTest {
	@Test
	void atomicSnapshotsKeepExactPercentageBoundaryStableDuringConcurrentPublication()
					throws Exception {
		final var counters = new DeleteObjectLifecycleCounters();
		final var controller = new ObjectFailureBudgetController(
						ObjectFailureBudgetConfig.percentage(50, Duration.ZERO));
		final var start = new CountDownLatch(1);
		final var writersRunning = new AtomicBoolean(true);
		final var failure = new AtomicReference<Throwable>();
		final Thread reader = Thread.ofPlatform().start(() -> {
			try {
				start.await();
				while (writersRunning.get()) {
					final var snapshot = counters.snapshot();
					if (snapshot.accepted() != snapshot.failed()) {
						throw new AssertionError("counter snapshot crossed the exact percentage boundary: "
										+ snapshot);
					}
					final long attempted = snapshot.accepted() + snapshot.failed();
					final var lifecycle = new DeleteObjectLifecycleSnapshot(
									attempted,
									attempted,
									snapshot.accepted(),
									snapshot.failed(),
									0,
									0,
									snapshot.protocolFailed(),
									snapshot.fullSuccessfulRequests(),
									true);
					if (controller.evaluate(List.of(lifecycle), Duration.ZERO, false).stopScheduling()) {
						throw new AssertionError("exact-boundary snapshot breached the percentage budget: "
										+ snapshot);
					}
				}
			} catch (final Throwable throwable) {
				failure.compareAndSet(null, throwable);
			}
		});
		final List<Thread> writers = new ArrayList<>();
		for (int worker = 0; worker < 8; worker++) {
			writers.add(Thread.ofPlatform().start(() -> {
				try {
					start.await();
					for (int update = 0; update < 10_000; update++) {
						counters.recordTerminal(1, 1, 0, 0);
					}
				} catch (final Throwable throwable) {
					failure.compareAndSet(null, throwable);
				}
			}));
		}

		start.countDown();
		for (final Thread writer : writers) {
			writer.join();
		}
		writersRunning.set(false);
		reader.join();

		assertNull(failure.get());
		assertFalse(controller.evaluate(
						List.of(new DeleteObjectLifecycleSnapshot(
										160_000, 160_000, 80_000, 80_000, 0, 0, 0, 0, true)),
						Duration.ZERO,
						false)
						.stopScheduling());
	}
}
