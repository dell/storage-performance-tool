package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.SeedDataInput;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.data.range.*;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.step.local.context.range.RangeReadRuntime;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.dell.spt.base.storage.driver.range.RangeReadDriverSupport;
import com.github.akurilov.commons.io.Output;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class RangeReadStepContextTest {
	abstract static class Driver extends StorageDriverBase<DataItemImpl, RangeReadOperation<DataItemImpl>>
					implements RangeReadDriverSupport {
		final RangeReadRuntime<DataItemImpl> runtime;

		Driver(RangeReadPolicy policy) {
			super("range-step", new SeedDataInput(1, 1024, 1, true), TestConfigBuilder.config().configVal("storage"), false);
			runtime = new RangeReadRuntime<>(policy, 2, this, this::publishRetainedRangeResult);
			enableOperationLifecycle(runtime.tracker());
		}

		@Override
		public RangeReadRuntime<DataItemImpl> rangeReadRuntime() {
			return runtime;
		}

		@Override
		public boolean put(RangeReadOperation<DataItemImpl> op) {
			return operationLifecycle().driverQueued(op);
		}
	}

	private static final class Fixture implements AutoCloseable {
		final Driver driver;
		final RangeReadRuntime<DataItemImpl> runtime;
		final LoadGenerator<DataItemImpl, RangeReadOperation<DataItemImpl>> generator;
		final MetricsContext metrics;
		final LoadStepContextImpl<DataItemImpl, RangeReadOperation<DataItemImpl>> context;
		final AtomicInteger generated = new AtomicInteger();
		final List<Runnable> scheduled = new ArrayList<>();
		final List<RangeReadOperation<DataItemImpl>> recycled = new ArrayList<>();
		final RangeReadPolicy policy;

		@SuppressWarnings("unchecked")
		Fixture(boolean retry, boolean recycle, RangeReadPolicy policy) throws Exception {
			this(retry, recycle, policy, false);
		}

		Fixture(boolean retry, boolean recycle, RangeReadPolicy policy, boolean trace) throws Exception {
			this.policy = policy;
			driver = mock(Driver.class, withSettings().useConstructor(policy).defaultAnswer(CALLS_REAL_METHODS));
			runtime = driver.runtime;
			generator = mock(LoadGenerator.class);
			when(generator.supportsRetry()).thenReturn(true);
			when(generator.supportsRangeRetry()).thenReturn(true);
			when(generator.generatedOpCount()).thenAnswer(call -> (long) generated.get());
			when(generator.isItemInputFinished()).thenReturn(true);
			when(generator.isNothingPendingRetry()).thenReturn(true);
			when(generator.isNothingToRecycle()).thenAnswer(call -> recycled.isEmpty());
			when(generator.recoverBufferedOperations()).thenAnswer(call -> List.copyOf(recycled));
			doAnswer(call -> {
				RangeReadOperation<DataItemImpl> op = call.getArgument(0);
				assertTrue(runtime.tracker().generatorBuffered(op));
				recycled.add(op);
				generated.incrementAndGet();
				return null;
			}).when(generator).recycle(any());
			metrics = mock(MetricsContext.class, RETURNS_DEEP_STUBS);
			var config = TestConfigBuilder.config();
			config.val("load-op-type", "read");
			config.val("load-op-retry", retry);
			config.val("load-op-retryLimit", 1);
			config.val("load-op-recycle-mode", recycle);
			config.val("load-op-limit-count", recycle ? 0 : 1);
			config.val("load-op-wait-finish", true);
			config.val("load-op-wait-limit", 0);
			context = new LoadStepContextImpl<>("range-step", generator, driver, metrics, null,
							config.configVal("load"), trace, null, null, config.configVal("item"));
			context.setRetryScheduler((delay, task) -> {
				scheduled.add(task);
				return new CompletableFuture<>();
			});
			context.start();
			verify(driver).operationResultOutput(runtime);
		}

		RangeReadOperation<DataItemImpl> admit(long size) {
			var op = new RangeReadOperationsBuilder<DataItemImpl>(0, policy).buildOp(new DataItemImpl("object", 0, size));
			generated.incrementAndGet();
			assertTrue(runtime.tracker().generatorBuffered(op));
			assertTrue(runtime.admission().put(op));
			return op;
		}

		RangeReadAttempt attempt(RangeReadOperation<DataItemImpl> op) {
			var attempt = op.circulation().beginAttempt(null);
			assertTrue(runtime.tracker().explicitlyDispatched(op));
			assertTrue(runtime.metrics().requestHandoff(attempt));
			return attempt;
		}

		void success(RangeReadOperation<DataItemImpl> op, boolean publish) {
			var attempt = attempt(op);
			assertTrue(attempt.headers(206, Status.SUCC, List.of("bytes 0-1/10"), List.of("2"), List.of(), false));
			assertTrue(attempt.bodyBytes(2));
			assertTrue(attempt.finish(true));
			if (publish)
				assertTrue(runtime.completed(op, op.circulation(), attempt));
			else
				assertTrue(op.circulation().complete(op, runtime.tracker(), attempt));
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
	@SuppressWarnings("unchecked")
	void explicitTraceIncludesFailedResultsOnceWithoutRecyclingOrSuccessfulItemOutput() throws Exception {
		var logger = (org.apache.logging.log4j.core.Logger) com.dell.spt.base.logging.Loggers.OP_TRACES;
		var appender = mock(org.apache.logging.log4j.core.Appender.class);
		when(appender.getName()).thenReturn("range-failure-test");
		when(appender.isStarted()).thenReturn(true);
		var events = new java.util.concurrent.LinkedBlockingQueue<String>();
		doAnswer(call -> {
			org.apache.logging.log4j.core.LogEvent event = call.getArgument(0);
			events.add(event.getMessage().getFormattedMessage());
			return null;
		}).when(appender).append(any());
		var previousLevel = logger.getLevel();
		logger.addAppender(appender);
		logger.setLevel(org.apache.logging.log4j.Level.INFO);
		try {
			for (boolean trace : List.of(false, true)) {
				for (boolean local : List.of(false, true)) {
					try (var f = new Fixture(false, true, new RangeReadPolicy(2, local ? null : Long.valueOf(0), 1), trace)) {
						logger.addAppender(appender);
						logger.setLevel(org.apache.logging.log4j.Level.INFO);
						events.clear();
						Output<RangeReadOperation<DataItemImpl>> itemOutput = mock(Output.class);
						f.context.operationsResultsOutput(itemOutput);
						var op = f.admit(local ? 1 : 10);
						if (local) {
							assertTrue(f.runtime.put(op.result()));
						} else {
							var attempt = f.attempt(op);
							assertTrue(attempt.transportFailure(Status.FAIL_IO));
							assertTrue(f.runtime.completed(op, op.circulation(), attempt));
						}
						assertFalse(f.runtime.put(op.result()), "Duplicate failure must not produce another trace");
						// A same-thread logging fence drains earlier async events, so disabled
						// traces and duplicate suppression are asserted without timing guesses.
						String fence = "range-trace-fence-" + trace + "-" + local;
						logger.info(fence);
						String event = events.poll(2, java.util.concurrent.TimeUnit.SECONDS);
						if (trace) {
							assertNotNull(event);
							String[] columns = event.strip().split(",", -1);
							assertEquals(9, columns.length);
							assertEquals(op.item().name(), columns[1]);
							assertEquals("READ", columns[2]);
							assertEquals(op.status().name(), columns[3]);
							event = events.poll(2, java.util.concurrent.TimeUnit.SECONDS);
						}
						assertEquals(fence, event, "Exactly one enabled trace, none when disabled");
						verifyNoInteractions(itemOutput);
						assertTrue(f.recycled.isEmpty());
						assertEquals(0, f.runtime.snapshot().successfulBytes());
						assertTrue(f.runtime.snapshot().reconciled());
						verify(f.metrics).markFail();
					}
				}
			}
		} finally {
			logger.removeAppender(appender);
			logger.setLevel(previousLevel);
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void rangeBindingRejectsUnsupportedRetryGeneratorAndReadVerify() throws Exception {
		for (boolean verify : List.of(false, true)) {
			var driver = mock(Driver.class, withSettings().useConstructor(new RangeReadPolicy(2, 0L, 1))
							.defaultAnswer(CALLS_REAL_METHODS));
			try {
				LoadGenerator<DataItemImpl, RangeReadOperation<DataItemImpl>> generator = mock(LoadGenerator.class);
				when(generator.supportsRetry()).thenReturn(true);
				when(generator.supportsRangeRetry()).thenReturn(verify);
				var config = TestConfigBuilder.config();
				config.val("load-op-type", "read");
				config.val("load-op-retry", true);
				config.val("item-data-verify", verify);
				var failure = assertThrows(RuntimeException.class,
								() -> new LoadStepContextImpl<>("unsupported-range", generator,
												driver, mock(MetricsContext.class, RETURNS_DEEP_STUBS), null,
												config.configVal("load"), false, null, null, config.configVal("item")));
				if (verify)
					assertInstanceOf(IllegalConfigurationException.class, failure);
				else
					assertInstanceOf(IllegalStateException.class, failure);
				assertTrue(failure.getMessage().contains(verify ? "Range runtime requires" : "retained range retries"));
				assertEquals(0, driver.runtime.tracker().counters().selected());
			} finally {
				driver.close();
			}
		}
	}

	@Test
	void localFailureUpdatesMainMetricsAndCountWithoutDriverCompletion() throws Exception {
		try (var f = new Fixture(false, false, new RangeReadPolicy(2, null, 1))) {
			var op = f.admit(0);
			verify(f.metrics).markFail();
			verify(f.metrics, never()).markSucc(anyLong(), anyLong(), anyLong(), anyLong());
			verify(f.driver, never()).put(op);
			assertTrue(f.context.isDone());
			assertEquals(0, f.runtime.snapshot().requestsSent());
			assertEquals(1, f.runtime.snapshot().localSelectionErrors());
			assertTrue(f.runtime.snapshot().reconciled());
			assertEquals(1, f.runtime.put(List.of(op.result(), op.result())));
			verify(f.metrics, times(1)).markFail();
		}
	}

	@Test
	void configuredRetryWaitsWithoutFinalFailureThenShutdownSettlesOnce() throws Exception {
		for (boolean retry : List.of(false, true)) {
			try (var f = new Fixture(retry, false, new RangeReadPolicy(2, 0L, 1))) {
				var op = f.admit(10);
				var attempt = f.attempt(op);
				assertTrue(attempt.transportFailure(Status.FAIL_IO));
				assertTrue(f.runtime.completed(op, op.circulation(), attempt));
				assertEquals(retry ? 1 : 0, f.scheduled.size());
				if (retry) {
					assertFalse(f.context.isDone());
					verify(f.metrics, never()).markFail();
					f.context.recoverQueuedOperationsForStepStop();
					f.scheduled.getFirst().run();
					verify(f.generator, never()).retryRange(any(), any());
				}
				verify(f.metrics).markFail();
				assertEquals(1, f.runtime.snapshot().logical().failed());
				assertEquals(0, f.runtime.snapshot().logical().unattempted());
				assertEquals(0, f.runtime.admission().admittedCirculations());
			}
		}
	}

	@Test
	void retrySuccessUpdatesMainMetricsOnlyForTheFinalLogicalRead() throws Exception {
		try (var f = new Fixture(true, false, new RangeReadPolicy(2, 0L, 1))) {
			var op = f.admit(10);
			var first = f.attempt(op);
			assertTrue(first.transportFailure(Status.FAIL_IO));
			assertTrue(f.runtime.completed(op, op.circulation(), first));
			var queued = new AtomicReference<RangeReadAttempt>();
			when(f.generator.retryRange(any(), any())).thenAnswer(call -> {
				RangeReadAttempt retry = call.getArgument(1);
				assertTrue(op.circulation().claimRetryQueue(retry));
				queued.set(retry);
				return true;
			});
			f.scheduled.getFirst().run();
			assertFalse(f.context.isDone());
			verify(f.metrics, never()).markFail();
			var retry = queued.get();
			assertNotNull(retry);
			assertTrue(f.runtime.metrics().requestHandoff(retry));
			assertTrue(retry.headers(206, Status.SUCC, List.of("bytes 0-1/10"), List.of("2"), List.of(), false));
			assertTrue(retry.bodyBytes(2));
			assertTrue(retry.finish(true));
			assertTrue(f.runtime.completed(op, op.circulation(), retry));
			verify(f.metrics, times(1)).markSucc(eq(2L), anyLong(), eq(0L), eq(0L));
			verify(f.metrics, never()).markFail();
			assertTrue(f.context.isDone());
			assertEquals(1, f.runtime.snapshot().logical().selected());
			assertEquals(2, f.runtime.snapshot().requestsSent());
			assertEquals(0, f.runtime.admission().admittedCirculations());
			assertTrue(f.runtime.snapshot().reconciled());
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void rejectedOutputIsRunFailureAfterSuccessfulMetricsCommit() throws Exception {
		try (var f = new Fixture(false, false, new RangeReadPolicy(2, 0L, 1))) {
			Output<RangeReadOperation<DataItemImpl>> output = mock(Output.class);
			when(output.put(any(RangeReadOperation.class))).thenAnswer(call -> {
				verify(f.metrics).markSucc(eq(2L), anyLong(), eq(0L), eq(0L));
				assertEquals(0, f.runtime.tracker().inFlightCount());
				assertEquals(1, f.runtime.admission().admittedCirculations());
				return false;
			});
			f.context.operationsResultsOutput(output);
			var op = f.admit(10);
			f.success(op, true);
			assertThrows(IntegrityTerminalException.class, f.context::isDone);
			assertEquals(1, f.runtime.snapshot().logical().accepted());
			assertEquals(0, f.runtime.snapshot().logical().unresolved());
			assertEquals(0, f.runtime.admission().admittedCirculations());
			assertTrue(f.runtime.snapshot().reconciled());
		}
	}

	@Test
	void batchResultRoutingAccountsAndRecyclesOnceAcrossDuplicateCopies() throws Exception {
		try (var f = new Fixture(false, true, new RangeReadPolicy(2, 0L, 1))) {
			var first = f.admit(10);
			var second = f.admit(10);
			var early = first.result();
			f.success(first, false);
			f.success(second, false);
			assertEquals(2, f.runtime.put(List.of(early, second.result())));
			assertEquals(0, f.runtime.put(List.of(first.result(), second.result())));
			verify(f.metrics, times(2)).markSucc(eq(2L), anyLong(), eq(0L), eq(0L));
			assertEquals(2, f.recycled.size());
			assertFalse(f.context.isDone());
			assertEquals(0, f.runtime.admission().admittedCirculations());
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void blockedOutputUsesExistingDrainDeadlineWithoutErasingSuccess() throws Exception {
		try (var f = new Fixture(false, false, new RangeReadPolicy(2, 0L, 1));
						var pool = Executors.newSingleThreadExecutor()) {
			var entered = new CountDownLatch(1);
			var release = new CountDownLatch(1);
			Output<RangeReadOperation<DataItemImpl>> output = mock(Output.class);
			when(output.put(any(RangeReadOperation.class))).thenAnswer(call -> {
				entered.countDown();
				assertTrue(release.await(5, TimeUnit.SECONDS));
				return true;
			});
			f.context.operationsResultsOutput(output);
			var op = f.admit(10);
			var completion = pool.submit(() -> f.success(op, true));
			try {
				assertTrue(entered.await(5, TimeUnit.SECONDS));
				assertTrue(f.runtime.hasPendingResults());
				assertThrows(IntegrityTerminalException.class,
								() -> f.context.drainDispatchedOperationsForStepStop(System.nanoTime()));
				assertEquals(1, f.runtime.snapshot().logical().accepted());
				assertEquals(0, f.runtime.snapshot().logical().unresolved());
				assertEquals(0, f.runtime.admission().admittedCirculations());
			} finally {
				release.countDown();
			}
			completion.get(5, TimeUnit.SECONDS);
			assertThrows(IntegrityTerminalException.class, f.context::isDone);
			assertFalse(f.runtime.put(op.result()));
		}
	}
}
