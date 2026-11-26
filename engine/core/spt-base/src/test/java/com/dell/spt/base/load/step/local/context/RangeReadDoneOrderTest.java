package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.dell.spt.base.item.op.data.range.RangeReadCirculation;
import com.dell.spt.base.item.op.data.range.RangeReadOperation;
import com.dell.spt.base.item.op.data.range.RangeReadOperationsBuilder;
import com.dell.spt.base.item.op.data.range.RangeReadPolicy;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.generator.LoadGeneratorImpl;
import com.dell.spt.base.load.step.local.context.range.RangeReadRuntime;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.dell.spt.base.storage.driver.range.RangeReadDriverSupport;
import com.github.akurilov.commons.io.Input;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The range branch of {@link LoadStepContextImpl#isDone()} with one recycled read circulating. The
 * check runs on a monitor thread while the other parties (terminal accounting, result output with
 * recycle, one generator pass) run on the test thread at chosen reads of the check. The read must
 * keep circulating, so every "done" here is a false one, except in the exhaustion case.
 *
 * <p>Seam points, each a stub on a delegating view of a real object:
 * <ul>
 * <li>{@code hasPendingResults} on the range runtime view (the driver's {@code rangeReadRuntime()});
 * <li>{@code isItemInputFinished}, {@code generatedOpCount} and {@code isNothingToRecycle} on the
 * generator view passed to the context.
 * </ul>
 * At each point the monitor thread stops before and after the real read, and the test thread runs
 * the step scheduled for that point, if any, then lets the monitor go on. The custody read goes to
 * the driver's real tracker and has no seam: the steps around it place it.
 */
@SuppressWarnings({"unchecked", "rawtypes"
})
class RangeReadDoneOrderTest {

	private static final String PENDING = "hasPendingResults";
	private static final String QUEUE = "isNothingToRecycle";

	abstract static class Driver extends StorageDriverBase<DataItemImpl, RangeReadOperation<DataItemImpl>>
					implements RangeReadDriverSupport {
		final RangeReadRuntime<DataItemImpl> runtime;
		final BlockingQueue<RangeReadOperation<DataItemImpl>> wire = new LinkedBlockingQueue<>();
		volatile RangeReadRuntime<DataItemImpl> runtimeView;

		Driver(RangeReadPolicy policy) {
			super("range-done-order", new SeedDataInput(1, 1024, 1, true),
							TestConfigBuilder.config().configVal("storage"), false);
			runtime = new RangeReadRuntime<>(policy, 16, this, this::publishRetainedRangeResult);
			enableOperationLifecycle(runtime.tracker());
		}

		boolean publish(RangeReadOperation<DataItemImpl> op, RangeReadCirculation circulation) {
			return publishRetainedRangeResult(op, circulation);
		}

		@Override
		public RangeReadRuntime<DataItemImpl> rangeReadRuntime() {
			return runtimeView != null ? runtimeView : runtime;
		}

		@Override
		public boolean put(RangeReadOperation<DataItemImpl> op) {
			if (!operationLifecycle().driverQueued(op))
				return false;
			wire.add(op);
			return true;
		}

		@Override
		public int put(List<RangeReadOperation<DataItemImpl>> ops, int from, int to) {
			int n = 0;
			for (int i = from; i < to && put(ops.get(i)); i++)
				n++;
			return n;
		}
	}

	/** Exposes one generator pass ({@code doWork}) to the test thread. */
	static final class Generator extends LoadGeneratorImpl<DataItemImpl, RangeReadOperation<DataItemImpl>> {
		Generator(Input<DataItemImpl> input, OperationsBuilder<DataItemImpl, RangeReadOperation<DataItemImpl>> builder,
						RangeReadRuntime<DataItemImpl> runtime) {
			super(input, builder, List.of(), runtime.admission(), 4, 0, 1000, true, false);
		}

		void pass() {
			try {
				doWork();
			} catch (Exception e) {
				throw new IllegalStateException(e);
			}
		}
	}

	private record Arrival(String point, CountDownLatch release) {}

	private static final class Rig implements AutoCloseable {
		final RangeReadPolicy policy = new RangeReadPolicy(2, 0L, 1);
		final Driver driver;
		final RangeReadRuntime<DataItemImpl> runtime;
		final Generator generator;
		final LoadStepContextImpl<DataItemImpl, RangeReadOperation<DataItemImpl>> context;
		final BlockingQueue<Arrival> arrivals = new LinkedBlockingQueue<>();
		final Map<String, Integer> visits = new HashMap<>();
		final ExecutorService monitorPool = Executors.newSingleThreadExecutor();
		volatile Thread monitor;

		Rig() throws Exception {
			driver = mock(Driver.class, withSettings().useConstructor(policy).defaultAnswer(CALLS_REAL_METHODS));
			runtime = driver.runtime;
			final RangeReadRuntime<DataItemImpl> runtimeView = mock(
							RangeReadRuntime.class, withSettings().defaultAnswer(delegatesTo(runtime)));
			doAnswer(call -> seam(PENDING, runtime::hasPendingResults)).when(runtimeView).hasPendingResults();
			driver.runtimeView = runtimeView;
			final Input<DataItemImpl> itemInput = mock(Input.class);
			final OperationsBuilder<DataItemImpl, RangeReadOperation<DataItemImpl>> builder = mock(OperationsBuilder.class);
			when(builder.opType()).thenReturn(OpType.READ);
			when(builder.originIndex()).thenReturn(0);
			generator = new Generator(itemInput, builder, runtime);
			final LoadGenerator<DataItemImpl, RangeReadOperation<DataItemImpl>> generatorView = mock(
							LoadGenerator.class, withSettings().defaultAnswer(delegatesTo(generator)));
			// The test runs generator passes itself; the context must not start the generator's thread.
			doNothing().when(generatorView).start();
			doAnswer(call -> seam("isItemInputFinished", generator::isItemInputFinished))
							.when(generatorView).isItemInputFinished();
			doAnswer(call -> seam("generatedOpCount", generator::generatedOpCount)).when(generatorView).generatedOpCount();
			doAnswer(call -> seam(QUEUE, generator::isNothingToRecycle)).when(generatorView).isNothingToRecycle();
			final MetricsContext metrics = mock(MetricsContext.class, RETURNS_DEEP_STUBS);
			final var config = TestConfigBuilder.config();
			config.val("load-op-type", "read");
			config.val("load-op-retry", false);
			config.val("load-op-recycle-mode", true);
			config.val("load-op-limit-count", 0);
			config.val("load-op-wait-finish", true);
			config.val("load-op-wait-limit", 0);
			context = new LoadStepContextImpl<>("range-done-order", generatorView, driver, metrics, null,
							config.configVal("load"), false, null, null, config.configVal("item"));
			context.start();
			generator.pass(); // exhausts the empty item input
			assertTrue(generator.isItemInputFinished());
		}

		private <T> T seam(String read, java.util.function.Supplier<T> real) {
			if (Thread.currentThread() != monitor)
				return real.get();
			final int n;
			synchronized (visits) {
				n = visits.merge(read, 1, Integer::sum);
			}
			stop("before:" + read + "#" + n);
			final T value = real.get();
			stop("after:" + read + "#" + n);
			return value;
		}

		private void stop(String point) {
			final var release = new CountDownLatch(1);
			arrivals.add(new Arrival(point, release));
			try {
				if (!release.await(10, TimeUnit.SECONDS))
					throw new IllegalStateException("test thread did not release " + point);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException(e);
			}
		}

		/** Seeds the single object's first read, as the item input's last read would recycle it. */
		RangeReadOperation<DataItemImpl> seed() {
			final var op = new RangeReadOperationsBuilder<DataItemImpl>(0, policy)
							.buildOp(new DataItemImpl("object", 0, 10));
			generator.recycle(op);
			return op;
		}

		/** Transport result up to terminal accounting (counted, custody released, result pending). */
		void terminal(RangeReadOperation<DataItemImpl> op, boolean success) {
			final var circulation = op.circulation();
			final var attempt = circulation.beginAttempt(null);
			assertTrue(runtime.tracker().explicitlyDispatched(op));
			assertTrue(runtime.metrics().requestHandoff(attempt));
			if (success) {
				assertTrue(attempt.headers(206, Status.SUCC, List.of("bytes 0-1/10"), List.of("2"), List.of(), false));
				assertTrue(attempt.bodyBytes(2));
				assertTrue(attempt.finish(true));
			} else {
				assertTrue(attempt.transportFailure(Status.FAIL_IO));
			}
			assertTrue(circulation.complete(op, runtime.tracker(), attempt));
		}

		/**
		 * Runs one isDone() on the monitor thread. At each seam point the step mapped to it runs on this
		 * thread before the monitor goes on. Returns the check's answer; every required step must run.
		 */
		boolean isDone(Map<String, Runnable> required, Map<String, Runnable> optional) throws Exception {
			final var steps = new HashMap<>(required);
			steps.putAll(optional);
			synchronized (visits) {
				visits.clear();
			}
			final Future<Boolean> done = monitorPool.submit(() -> {
				monitor = Thread.currentThread();
				try {
					return context.isDone();
				} finally {
					monitor = null;
				}
			});
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
			while (!done.isDone()) {
				assertTrue(System.nanoTime() < deadline, "isDone() did not return");
				final var arrival = arrivals.poll(10, TimeUnit.MILLISECONDS);
				if (arrival == null)
					continue;
				try {
					final var step = steps.remove(arrival.point());
					if (step != null)
						step.run();
				} finally {
					arrival.release().countDown();
				}
			}
			final boolean result = done.get();
			for (var point : required.keySet())
				assertFalse(steps.containsKey(point), "the check never reached " + point);
			return result;
		}

		@Override
		public void close() throws Exception {
			monitorPool.shutdownNow();
			try {
				context.close();
			} finally {
				try {
					generator.close();
				} finally {
					driver.close();
				}
			}
		}
	}

	/**
	 * First cycle, so the result count is 1 and the generated count is 1 before the successor:
	 * the first read is terminal and its result is waiting for output. Returns that read.
	 */
	private static RangeReadOperation<DataItemImpl> firstReadTerminal(Rig r) throws Exception {
		r.seed();
		r.generator.pass();
		assertEquals(1, r.generator.generatedOpCount());
		final var first = r.driver.wire.poll();
		assertNotNull(first);
		r.terminal(first, true);
		assertFalse(r.runtime.tracker().hasOutstandingOperations());
		assertTrue(r.runtime.hasPendingResults());
		assertFalse(r.context.isDone(), "the pending result vetoes done");
		return first;
	}

	/** Publication: item/timing output, recycle of the successor, pending result removed. */
	private static void publish(Rig r, RangeReadOperation<DataItemImpl> op, RangeReadCirculation circulation) {
		assertTrue(r.driver.publish(op, circulation));
		assertFalse(r.runtime.hasPendingResults());
		assertFalse(r.generator.isNothingToRecycle(), "the successor is in the recycle queue");
	}

	/** One generator pass polls the successor, counts it as generated and hands it to the driver. */
	private static void successorToDriver(Rig r) {
		r.generator.pass();
		assertEquals(2, r.generator.generatedOpCount());
		assertEquals(1, r.driver.wire.size());
		assertTrue(r.generator.isNothingToRecycle());
		assertTrue(r.runtime.tracker().hasOutstandingOperations());
	}

	/**
	 * The ticket's interleaving: the custody read falls in the gap between the first read's terminal
	 * accounting and its output; publication (output and recycle) completes before the pending read;
	 * a full generator pass runs before the queue test. Only the last read can veto, so the generated
	 * count has to be the last read and the result count the first.
	 */
	@Test
	void notDoneWhenTheSuccessorIsGeneratedBeforeTheQueueTest() throws Exception {
		try (var r = new Rig()) {
			final var op = firstReadTerminal(r);
			final var circulation = op.circulation();
			final boolean done = r.isDone(Map.of(
							"before:" + PENDING + "#1", () -> publish(r, op, circulation),
							"before:" + QUEUE + "#1", () -> successorToDriver(r)), Map.of());
			assertFalse(done, "done reported with the successor read in the driver");
			assertFalse(r.context.isDone(), "a quiet evaluation is false: the successor is in the driver");
		}
	}

	/**
	 * A second cycle: as the ticket's interleaving, then the successor reaches terminal accounting
	 * right after the queue test, and its output completes at a second pending read if the check
	 * takes one. A check that tests the queue before reading the result count, or that rechecks
	 * custody and pending results after the queue test, reports done here.
	 */
	@Test
	void notDoneWhenTheSuccessorCompletesAfterTheQueueTest() throws Exception {
		try (var r = new Rig()) {
			final var op = firstReadTerminal(r);
			final var circulation = op.circulation();
			final RangeReadOperation<DataItemImpl>[] successor = new RangeReadOperation[1];
			final RangeReadCirculation[] successorCirculation = new RangeReadCirculation[1];
			final boolean done = r.isDone(Map.of(
							"before:" + PENDING + "#1", () -> publish(r, op, circulation),
							"before:" + QUEUE + "#1", () -> successorToDriver(r),
							"after:" + QUEUE + "#1", () -> {
								successor[0] = r.driver.wire.poll();
								assertNotNull(successor[0]);
								successorCirculation[0] = successor[0].circulation();
								r.terminal(successor[0], true);
							}), Map.of(
											"before:" + PENDING + "#2",
											() -> assertTrue(r.driver.publish(successor[0], successorCirculation[0]))));
			assertFalse(done, "done reported while the read still circulates");
			if (r.runtime.hasPendingResults())
				assertTrue(r.driver.publish(successor[0], successorCirculation[0]));
		}
	}

	/** Real exhaustion still ends the step: the only read failed, so nothing recycles. */
	@Test
	void doneWhenTheOnlyReadFailed() throws Exception {
		try (var r = new Rig()) {
			r.seed();
			r.generator.pass();
			final var first = r.driver.wire.poll();
			assertNotNull(first);
			final var circulation = first.circulation();
			r.terminal(first, false);
			assertFalse(r.context.isDone(), "the pending failure vetoes done");
			assertTrue(r.driver.publish(first, circulation));
			assertTrue(r.generator.isNothingToRecycle(), "a failed read is not recycled");
			assertEquals(1, r.generator.generatedOpCount());
			assertTrue(r.isDone(Map.of(), Map.of()));
		}
	}
}
