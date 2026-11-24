package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.Mockito.*;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.io.DelayedTransferConvertBuffer;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.data.range.*;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.generator.LoadGeneratorImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.step.local.context.range.RangeReadRuntime;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.dell.spt.base.storage.driver.range.RangeReadDriverSupport;
import com.github.akurilov.commons.concurrent.throttle.Throttle;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.junit.jupiter.api.Test;

/**
 * A stopped range READ publishes every result settled at the stop: retries the generator held,
 * and outcomes the drain deadline settles. Real generator, runtime and step context together.
 */
@SuppressWarnings("unchecked")
class RangeReadStopPublicationTest {
	private static final String STEP_ID = "range-stop-step";
	private static final RangeReadPolicy POLICY = new RangeReadPolicy(2, 0L, 1);
	private static final long LONG_BOUND_NANOS = TimeUnit.SECONDS.toNanos(5);
	/** A stop which waits for the drain bound takes at least LONG_BOUND_NANOS. */
	private static final long PROMPT_NANOS = TimeUnit.SECONDS.toNanos(3);

	abstract static class Driver extends StorageDriverBase<DataItemImpl, RangeReadOperation<DataItemImpl>>
					implements RangeReadDriverSupport {
		final RangeReadRuntime<DataItemImpl> runtime;
		final List<RangeReadOperation<DataItemImpl>> accepted = new ArrayList<>();
		final AtomicReference<Runnable> refuse = new AtomicReference<>();
		final AtomicBoolean failPublish = new AtomicBoolean();
		final AtomicReference<CountDownLatch> blockPublish = new AtomicReference<>();
		final AtomicInteger publishCalls = new AtomicInteger();
		final AtomicInteger delivered = new AtomicInteger();
		final AtomicReference<Thread> publishThread = new AtomicReference<>();

		Driver() {
			super(STEP_ID, new SeedDataInput(1, 1024, 1, true), TestConfigBuilder.config().configVal("storage"), false);
			runtime = new RangeReadRuntime<>(POLICY, 4, this, (op, circulation) -> {
				publishCalls.incrementAndGet();
				publishThread.set(Thread.currentThread());
				if (failPublish.get())
					throw new IllegalStateException("injected publication failure");
				final var gate = blockPublish.get();
				if (gate != null) {
					try {
						gate.await(10, TimeUnit.SECONDS);
					} catch (InterruptedException interrupted) {
						Thread.currentThread().interrupt();
					}
				}
				if (publishRetainedRangeResult(op, circulation))
					delivered.incrementAndGet();
			});
			enableOperationLifecycle(runtime.tracker());
		}

		boolean publishNow(RangeReadOperation<DataItemImpl> op) {
			return publishRetainedRangeResult(op, op.circulation());
		}

		@Override
		public RangeReadRuntime<DataItemImpl> rangeReadRuntime() {
			return runtime;
		}

		@Override
		public boolean put(RangeReadOperation<DataItemImpl> op) {
			final var refusal = refuse.get();
			if (refusal != null) {
				refusal.run();
				return false;
			}
			accepted.add(op);
			return op.lifecycle().state() != OperationLifecycleState.GENERATOR_BUFFERED
							|| operationLifecycle().driverQueued(op);
		}
	}

	static final class Generator extends LoadGeneratorImpl<DataItemImpl, RangeReadOperation<DataItemImpl>> {
		Generator(Input<DataItemImpl> input, List<Object> throttles,
						Output<RangeReadOperation<DataItemImpl>> output, boolean retry) {
			super(input, new RangeReadOperationsBuilder<>(0, POLICY), throttles, output, 4, 0, 10, false, false,
							retry);
		}

		void step() throws Exception {
			doWork();
		}
	}

	private static final class Rig implements AutoCloseable {
		final Driver driver;
		final RangeReadRuntime<DataItemImpl> runtime;
		final Generator generator;
		final LoadStepContextImpl<DataItemImpl, RangeReadOperation<DataItemImpl>> context;
		final List<Runnable> scheduled = new ArrayList<>();
		final MetricsContext metrics = mock(MetricsContext.class, RETURNS_DEEP_STUBS);

		Rig(int items, boolean retry, boolean trace, boolean waitFinish) throws Exception {
			driver = mock(Driver.class, withSettings().useConstructor().defaultAnswer(CALLS_REAL_METHODS));
			runtime = driver.runtime;
			final Input<DataItemImpl> input = mock(Input.class);
			final var remaining = new AtomicInteger(items);
			doAnswer(call -> {
				final int left = remaining.get();
				if (left <= 0)
					throw new EOFException();
				final List<DataItemImpl> buffer = call.getArgument(0);
				final int take = Math.min(left, (Integer) call.getArgument(1));
				remaining.addAndGet(-take);
				for (int i = 0; i < take; i++)
					buffer.add(new DataItemImpl("object-" + (items - left + i), 0, 10));
				return take;
			}).when(input).get(anyList(), anyInt());
			final Throttle throttle = mock(Throttle.class);
			when(throttle.tryAcquire(anyInt())).thenAnswer(call -> call.getArgument(0));
			generator = new Generator(input, List.of(throttle), runtime.admission(), retry);
			final LoadGenerator<DataItemImpl, RangeReadOperation<DataItemImpl>> generatorView = mock(
							LoadGenerator.class, withSettings().defaultAnswer(delegatesTo(generator)));
			// The test runs generator passes itself; the context must not start the generator's thread.
			doNothing().when(generatorView).start();
			final var config = TestConfigBuilder.config();
			config.val("load-op-type", "read");
			config.val("load-op-retry", retry);
			config.val("load-op-retryLimit", 5);
			config.val("load-op-recycle-mode", false);
			config.val("load-op-limit-count", 0);
			config.val("load-op-wait-finish", waitFinish);
			config.val("load-op-wait-limit", 0);
			context = new LoadStepContextImpl<>(STEP_ID, generatorView, driver, metrics,
							null, config.configVal("load"), trace, null, null, config.configVal("item"));
			context.setRetryScheduler((delay, task) -> {
				scheduled.add(task);
				return new CompletableFuture<>();
			});
			context.start();
		}

		/** Generates every item and answers each first attempt with a 503. */
		List<RangeReadOperation<DataItemImpl>> dispatchAndFail() throws Exception {
			generator.step();
			final var ops = List.copyOf(driver.accepted);
			assertFalse(ops.isEmpty());
			for (final var op : ops)
				assertTrue(runtime.completed(op, op.circulation(), respond503(op)));
			return ops;
		}

		/** Fails every first attempt and hands each retry to the generator, which holds it. */
		List<RangeReadOperation<DataItemImpl>> holdRetries() throws Exception {
			final var ops = dispatchAndFail();
			assertEquals(ops.size(), scheduled.size());
			scheduled.forEach(Runnable::run);
			assertFalse(generator.isNothingPendingRetry(), "the generator holds the retries");
			driver.accepted.clear();
			return ops;
		}

		RangeReadAttempt respond503(RangeReadOperation<DataItemImpl> op) {
			final var attempt = dispatch(op);
			assertFalse(attempt.headers(503, Status.RESP_FAIL_SVC, List.of(), List.of(), List.of(), false));
			return attempt;
		}

		RangeReadAttempt dispatch(RangeReadOperation<DataItemImpl> op) {
			final var attempt = op.circulation().beginAttempt(null);
			assertTrue(runtime.tracker().explicitlyDispatched(op));
			assertTrue(runtime.metrics().requestHandoff(attempt));
			return attempt;
		}

		/** Generates one item whose 206 is decoded, but whose completion callback has not run yet. */
		RangeReadAttempt decodedSuccessAwaitingCompletion() throws Exception {
			generator.step();
			final var attempt = dispatch(driver.accepted.getFirst());
			assertTrue(attempt.headers(206, Status.SUCC, List.of("bytes 0-1/10"), List.of("2"), List.of(), false));
			assertTrue(attempt.bodyBytes(2));
			assertTrue(attempt.finish(true));
			return attempt;
		}

		/**
		 * Runs the decoded success's transport callback as soon as the drain deadline commits it, as
		 * Netty runs the completion it queued behind the rest of its read batch.
		 */
		Thread completeOnceCommitted(RangeReadAttempt attempt) {
			final var op = driver.accepted.getFirst();
			return Thread.ofPlatform().start(() -> {
				final long deadline = System.nanoTime() + 2 * LONG_BOUND_NANOS;
				while (op.lifecycle().state() != OperationLifecycleState.TERMINAL && System.nanoTime() < deadline)
					Thread.onSpinWait();
				// False: the deadline already did the accounting; the callback only publishes.
				runtime.completed(op, op.circulation(), attempt);
			});
		}

		/**
		 * Drains with a 100 ms bound on another thread while the success's late callback runs its
		 * blocked output. The deadline commits the success but never runs its output on the drain
		 * thread, and the drain fails at its bound instead of waiting on the blocked output.
		 */
		void assertSuccessFailsAtTheBound(String label, Thread callback) throws Exception {
			final long bound = TimeUnit.MILLISECONDS.toNanos(100);
			final var outcome = new CompletableFuture<Throwable>();
			final var elapsed = new AtomicLong();
			final var drainer = Thread.ofPlatform().start(() -> {
				final long started = System.nanoTime();
				try {
					context.drainDispatchedOperationsForStepStop(started + bound);
					outcome.complete(null);
				} catch (Throwable failure) {
					outcome.complete(failure);
				} finally {
					elapsed.set(System.nanoTime() - started);
				}
			});
			final Throwable failure;
			try {
				failure = outcome.get(2, TimeUnit.SECONDS);
			} catch (java.util.concurrent.TimeoutException blocked) {
				throw new AssertionError(label + ": the drain ran blocking output past its bound", blocked);
			}
			drainer.join(5_000);
			assertTrue(elapsed.get() < bound + TimeUnit.MILLISECONDS.toNanos(400), label + ": the drain ends at its bound");
			final var terminal = assertInstanceOf(IntegrityTerminalException.class, failure, label);
			assertEquals("Range result delivery exceeded the step drain bound", terminal.getCause().getMessage(), label);
			final var publisher = driver.publishThread.get();
			assertNotSame(drainer, publisher, label + ": no success output on the drain thread");
			if (publisher != null)
				assertSame(callback, publisher, label + ": only the late callback publishes it");
			assertEquals(1, runtime.snapshot().logical().accepted(), label + ": the committed outcome is kept");
		}

		/** Drains with a 5 s bound while the late callback publishes; it must end well within the bound. */
		void assertDeliveredWellWithinTheBound(String label, Runnable whileDraining, Thread callback) throws Exception {
			final var outcome = new CompletableFuture<Long>();
			final var drainer = Thread.ofPlatform().start(() -> {
				try {
					outcome.complete(drain(LONG_BOUND_NANOS));
				} catch (Throwable failure) {
					outcome.completeExceptionally(failure);
				}
			});
			whileDraining.run();
			final long elapsed = outcome.get(2 * LONG_BOUND_NANOS, TimeUnit.NANOSECONDS);
			drainer.join(5_000);
			callback.join(5_000);
			// A drain which waits for a result nothing will deliver takes the whole 5 s bound.
			assertTrue(elapsed < LONG_BOUND_NANOS / 10, label + ": the drain ends once the result is out, took "
							+ TimeUnit.NANOSECONDS.toMillis(elapsed) + " ms");
			assertSame(callback, driver.publishThread.get(), label + ": the late callback publishes it");
			assertSettledAndDelivered(1);
			assertEquals(1, runtime.snapshot().logical().accepted(), label);
		}

		/** Drains with the given bound and returns its elapsed time. */
		long drain(long boundNanos) {
			final long started = System.nanoTime();
			context.drainDispatchedOperationsForStepStop(started + boundNanos);
			return System.nanoTime() - started;
		}

		void assertSettledAndDelivered(int count) {
			assertNull(runtime.failure());
			assertEquals(count, driver.delivered.get(), "each settled result is published once");
			assertFalse(runtime.hasPendingResults());
			assertEquals(0, runtime.admission().admittedCirculations());
			assertEquals(0, runtime.snapshot().logical().unresolved());
			assertTrue(runtime.snapshot().reconciled());
		}

		@Override
		public void close() throws Exception {
			try {
				context.close();
			} catch (IntegrityTerminalException expected) {
				if (runtime.failure() != expected)
					throw expected;
			} finally {
				driver.close();
			}
		}
	}

	@Test
	void retriesHeldByTheGeneratorArePublishedWhenTheStopRecoversThem() throws Exception {
		for (boolean waitFinish : List.of(true, false)) {
			try (var r = new Rig(2, true, false, waitFinish)) {
				r.holdRetries();
				final long elapsed = r.drain(LONG_BOUND_NANOS);
				assertTrue(elapsed < PROMPT_NANOS, "wait.finish=" + waitFinish + ": the stop waited "
								+ TimeUnit.NANOSECONDS.toMillis(elapsed) + " ms");
				r.assertSettledAndDelivered(2);
				assertEquals(2, r.runtime.snapshot().logical().failed());
			}
		}
	}

	@Test
	void retriesHeldByTheGeneratorArePublishedWithAZeroDrainBound() throws Exception {
		for (boolean waitFinish : List.of(true, false)) {
			try (var r = new Rig(2, true, false, waitFinish)) {
				r.holdRetries();
				assertDoesNotThrow(() -> r.drain(0));
				r.assertSettledAndDelivered(2);
			}
		}
	}

	@Test
	void retrySettledAtTheGeneratorsOutputEofIsPublished() throws Exception {
		try (var r = new Rig(1, true, false, true)) {
			r.holdRetries();
			// The stop closes range admission before the generator, so its next retry handoff sees EOF.
			r.runtime.closeAdmission();
			r.generator.step();
			assertEquals(1, r.driver.delivered.get());
			assertFalse(r.runtime.hasPendingResults());
			assertTrue(r.drain(LONG_BOUND_NANOS) < PROMPT_NANOS);
			r.assertSettledAndDelivered(1);
		}
	}

	@Test
	void retryRequeuedAfterAdmissionClosedIsPublished() throws Exception {
		try (var r = new Rig(1, true, false, true)) {
			r.holdRetries();
			// The stop closes generator admission while the driver refuses the retry.
			r.driver.refuse.set(r.generator::closeAdmission);
			r.generator.step();
			assertEquals(1, r.driver.delivered.get());
			assertTrue(r.drain(LONG_BOUND_NANOS) < PROMPT_NANOS);
			r.assertSettledAndDelivered(1);
		}
	}

	@Test
	void retriesDrainedFromTheGeneratorArePublished() throws Exception {
		try (var r = new Rig(2, true, false, true)) {
			r.holdRetries();
			assertTrue(r.generator.drainPendingRetries().isEmpty());
			assertEquals(2, r.driver.delivered.get());
			assertTrue(r.drain(LONG_BOUND_NANOS) < PROMPT_NANOS);
			r.assertSettledAndDelivered(2);
		}
	}

	@Test
	void retryRefusedByAClosedGeneratorIsPublishedOnce() throws Exception {
		try (var r = new Rig(1, true, false, true)) {
			final var op = r.dispatchAndFail().getFirst();
			r.generator.closeAdmission();
			final var attempt = op.circulation().pendingAttempt();
			assertFalse(r.generator.retryRange(op, attempt));
			// The refusal or the coordinator's own handoff publishes the settled retry, never both.
			r.scheduled.forEach(Runnable::run);
			assertTrue(r.drain(LONG_BOUND_NANOS) < PROMPT_NANOS);
			r.assertSettledAndDelivered(1);
		}
	}

	@Test
	void retryWhoseSettlementThrowsAfterItCommittedIsStillPublished() throws Exception {
		for (boolean viaDrainPending : List.of(false, true)) {
			final var label = viaDrainPending ? "drainPendingRetries" : "recoverBufferedOperations";
			try (var r = new Rig(2, true, false, true)) {
				r.holdRetries();
				// Terminal accounting of the second settled retry throws after its circulation committed.
				final var calls = new AtomicInteger();
				doAnswer(call -> {
					if (calls.incrementAndGet() == 2)
						throw new IllegalStateException("injected accounting failure");
					return null;
				}).when(r.metrics).markFail();
				final var thrown = assertThrows(IllegalStateException.class, () -> {
					if (viaDrainPending)
						r.generator.drainPendingRetries();
					else
						r.generator.recoverBufferedOperations();
				}, label);
				assertEquals("injected accounting failure", thrown.getMessage(), label);
				assertEquals(2, r.driver.delivered.get(), label + ": both settled retries are published");
				assertFalse(r.runtime.hasPendingResults(), label);
				assertEquals(2, r.runtime.snapshot().logical().failed(), label);
			}
		}
	}

	@Test
	void throwingPublicationIsRecordedOnceAndReleasesEverySlot() throws Exception {
		try (var r = new Rig(2, true, false, true)) {
			r.holdRetries();
			r.driver.failPublish.set(true);
			final long started = System.nanoTime();
			final var failure = assertThrows(IntegrityTerminalException.class,
							() -> r.context.drainDispatchedOperationsForStepStop(started + LONG_BOUND_NANOS));
			assertTrue(System.nanoTime() - started < PROMPT_NANOS, "a failed output is not waited for");
			assertSame(r.runtime.failure(), failure);
			assertEquals(IntegrityTerminalException.Category.PUBLICATION, failure.category());
			assertEquals(2, r.driver.publishCalls.get(), "each settled retry is offered once, never retried");
			assertFalse(r.runtime.hasPendingResults());
			assertEquals(0, r.runtime.admission().admittedCirculations());
			assertEquals(2, r.runtime.snapshot().logical().failed());
		}
	}

	@Test
	void throwingPublicationOfAWaitingRetryIsRetiredAtTheStop() throws Exception {
		try (var r = new Rig(1, true, false, true)) {
			r.dispatchAndFail(); // its retry still waits for the timer when the stop cancels it
			r.driver.failPublish.set(true);
			final long started = System.nanoTime();
			final var failure = assertThrows(IntegrityTerminalException.class,
							() -> r.context.drainDispatchedOperationsForStepStop(started + LONG_BOUND_NANOS));
			assertTrue(System.nanoTime() - started < PROMPT_NANOS, "a failed output is not waited for");
			assertEquals(IntegrityTerminalException.Category.PUBLICATION, failure.category());
			assertEquals(1, r.driver.publishCalls.get());
			assertFalse(r.runtime.hasPendingResults());
			assertEquals(0, r.runtime.admission().admittedCirculations());
			assertEquals(1, r.runtime.snapshot().logical().failed());
		}
	}

	@Test
	void failureSettledAtTheDrainDeadlineIsPublishedWithRetryOff() throws Exception {
		for (boolean waitFinish : List.of(true, false)) {
			try (var r = new Rig(1, false, false, waitFinish)) {
				r.generator.step();
				final var op = r.driver.accepted.getFirst();
				final var attempt = r.dispatch(op);
				// The response arrived, but its completion callback has not run yet.
				assertFalse(attempt.headers(503, Status.RESP_FAIL_SVC, List.of(), List.of(), List.of(), false));
				final var label = "wait.finish=" + waitFinish;
				assertDoesNotThrow(() -> r.drain(0), label);
				r.assertSettledAndDelivered(1);
				assertEquals(1, r.runtime.snapshot().logical().failed(), label);
			}
		}
	}

	@Test
	void deadlineSettlementOfSuccessWithBlockingOutputHonoursDrainBound() throws Exception {
		for (boolean waitFinish : List.of(true, false)) {
			final var label = "wait.finish=" + waitFinish;
			final var gate = new CountDownLatch(1);
			final Output<RangeReadOperation<DataItemImpl>> output = mock(Output.class);
			when(output.put(any(RangeReadOperation.class))).thenAnswer(call -> gate.await(10, TimeUnit.SECONDS));
			try (var r = new Rig(1, false, false, waitFinish)) {
				r.context.operationsResultsOutput(output);
				final var callback = r.completeOnceCommitted(r.decodedSuccessAwaitingCompletion());
				try {
					r.assertSuccessFailsAtTheBound(label, callback);
				} finally {
					gate.countDown();
					callback.join(5_000);
				}
			}
		}
	}

	@Test
	void deadlineSettlementOfSuccessWithOutputReleasedDuringTheDrainIsDelivered() throws Exception {
		// wait.finish=false: the stop settles at once, and waits only while the result is on its way out.
		final var gate = new CountDownLatch(1);
		final var entered = new CountDownLatch(1);
		final Output<RangeReadOperation<DataItemImpl>> output = mock(Output.class);
		when(output.put(any(RangeReadOperation.class))).thenAnswer(call -> {
			entered.countDown();
			return gate.await(10, TimeUnit.SECONDS);
		});
		try (var r = new Rig(1, false, false, false)) {
			r.context.operationsResultsOutput(output);
			final var callback = r.completeOnceCommitted(r.decodedSuccessAwaitingCompletion());
			try {
				r.assertDeliveredWellWithinTheBound("released output", () -> {
					try {
						assertTrue(entered.await(5, TimeUnit.SECONDS), "the late callback runs the output");
					} catch (InterruptedException interrupted) {
						throw new AssertionError(interrupted);
					} finally {
						gate.countDown();
					}
				}, callback);
			} finally {
				gate.countDown();
			}
			verify(output).put(any(RangeReadOperation.class));
		}
	}

	@Test
	void deadlineSettlementOfSuccessIntoAFullPipelineHandoffHonoursDrainBound() throws Exception {
		for (boolean waitFinish : List.of(true, false)) {
			// A pipeline stage's handoff whose next stage has stopped consuming: its put spins.
			final var handoff = new DelayedTransferConvertBuffer<DataItemImpl, RangeReadOperation<DataItemImpl>>(
							1, 0, TimeUnit.SECONDS);
			assertTrue(handoff.put(mock(RangeReadOperation.class)));
			try (var r = new Rig(1, false, false, waitFinish)) {
				r.context.operationsResultsOutput(handoff);
				final var callback = r.completeOnceCommitted(r.decodedSuccessAwaitingCompletion());
				try {
					r.assertSuccessFailsAtTheBound("wait.finish=" + waitFinish, callback);
				} finally {
					handoff.close(); // releases a spinning put
					callback.join(5_000);
				}
			}
		}
	}

	@Test
	void successSettledAtTheDeadlineIsPublishedByItsLateCallbackWithWaitFinishOff() throws Exception {
		// The stop settles at once while a decoded 206's callback is still queued on its event loop.
		final Output<RangeReadOperation<DataItemImpl>> output = mock(Output.class);
		when(output.put(any(RangeReadOperation.class))).thenReturn(true);
		try (var r = new Rig(1, false, false, false)) {
			r.context.operationsResultsOutput(output);
			final var callback = r.completeOnceCommitted(r.decodedSuccessAwaitingCompletion());
			r.assertDeliveredWellWithinTheBound("unblocked output", () -> {}, callback);
			verify(output).put(any(RangeReadOperation.class));
			assertEquals(1, r.driver.publishCalls.get(), "published once");
		}
	}

	@Test
	void resultPublishedLateIsAwaitedWithWaitFinishOff() throws Exception {
		try (var r = new Rig(1, false, false, false)) {
			r.generator.step();
			final var op = r.driver.accepted.getFirst();
			final var attempt = r.respond503(op);
			// Settled on a transport thread, which has not published it yet.
			assertTrue(op.circulation().complete(op, r.runtime.tracker(), attempt));
			final var outcome = new CompletableFuture<Throwable>();
			final var drainer = Thread.ofPlatform().start(() -> {
				try {
					r.drain(LONG_BOUND_NANOS);
					outcome.complete(null);
				} catch (Throwable failure) {
					outcome.complete(failure);
				}
			});
			// Publish once the drain waits for the result, or once it has ended without waiting.
			final long deadline = System.nanoTime() + LONG_BOUND_NANOS;
			while (drainer.isAlive() && !waitsForOutput(drainer) && System.nanoTime() < deadline)
				Thread.sleep(1);
			final boolean published = r.driver.publishNow(op);
			drainer.join(10_000);
			assertNull(outcome.get(1, TimeUnit.SECONDS), "the drain waits for a settled result within its bound");
			assertTrue(published);
			assertFalse(r.runtime.hasPendingResults());
		}
	}

	private static boolean waitsForOutput(Thread drainer) {
		for (final var frame : drainer.getStackTrace()) {
			if ("awaitOutstanding".equals(frame.getMethodName()))
				return true;
		}
		return false;
	}

	@Test
	void blockedPublicationStillFailsAtTheDrainBound() throws Exception {
		for (boolean waitFinish : List.of(true, false)) {
			try (var r = new Rig(1, false, false, waitFinish)) {
				r.generator.step();
				final var op = r.driver.accepted.getFirst();
				final var attempt = r.respond503(op);
				final var gate = new CountDownLatch(1);
				r.driver.blockPublish.set(gate);
				final var publisher = Thread.ofPlatform().start(() -> r.runtime.completed(op, op.circulation(), attempt));
				try {
					final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
					while (r.driver.publishCalls.get() == 0 && System.nanoTime() < deadline)
						Thread.sleep(5);
					assertEquals(1, r.driver.publishCalls.get());
					final long started = System.nanoTime();
					final var failure = assertThrows(IntegrityTerminalException.class,
									() -> r.context.drainDispatchedOperationsForStepStop(
													started + TimeUnit.MILLISECONDS.toNanos(100)));
					assertTrue(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(2),
									"wait.finish=" + waitFinish + ": the drain honours its bound");
					assertEquals("Range result delivery exceeded the step drain bound",
									failure.getCause().getMessage());
				} finally {
					gate.countDown();
					publisher.join(5_000);
				}
			}
		}
	}

	@Test
	void tracedStopWritesOneRowPerOperationToItsOwnStep() throws Exception {
		final var logger = (Logger) Loggers.OP_TRACES;
		final var appender = mock(Appender.class);
		when(appender.getName()).thenReturn("range-stop-trace");
		when(appender.isStarted()).thenReturn(true);
		final var events = new LinkedBlockingQueue<List<String>>();
		doAnswer(call -> {
			final LogEvent event = call.getArgument(0);
			events.add(List.of(event.getMessage().getFormattedMessage(),
							String.valueOf(event.getContextData().<Object> getValue("step_id"))));
			return null;
		}).when(appender).append(any());
		final var previousLevel = logger.getLevel();
		final var previousStepId = ThreadContext.get("step_id");
		try (var r = new Rig(2, true, true, true)) {
			logger.addAppender(appender);
			logger.setLevel(Level.INFO);
			r.holdRetries();
			// The generator thread of a shared executor carries another step's routing key.
			ThreadContext.put("step_id", "another-step");
			r.runtime.closeAdmission();
			r.generator.step();
			assertEquals("another-step", ThreadContext.get("step_id"), "the caller's routing key is kept");
			ThreadContext.remove("step_id");
			r.drain(LONG_BOUND_NANOS);
			r.assertSettledAndDelivered(2);
			// A same-thread fence drains the earlier asynchronous trace events.
			final var fence = "range-stop-trace-fence";
			logger.info(fence);
			final var rows = new ArrayList<String>();
			for (var event = events.poll(5, TimeUnit.SECONDS); event != null && !fence.equals(event.get(0)); event = events.poll(5, TimeUnit.SECONDS)) {
				rows.add(event.get(1));
			}
			assertEquals(List.of(STEP_ID, STEP_ID), rows, "one trace row per operation, in its step");
		} finally {
			if (previousStepId == null)
				ThreadContext.remove("step_id");
			else
				ThreadContext.put("step_id", previousStepId);
			logger.removeAppender(appender);
			logger.setLevel(previousLevel);
		}
	}
}
