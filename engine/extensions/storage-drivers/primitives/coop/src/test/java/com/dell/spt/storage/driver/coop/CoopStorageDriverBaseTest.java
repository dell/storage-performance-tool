package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycle;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.dell.spt.storage.driver.coop.mock.CoopStorageDriverMock;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.exceptions.InvalidValuePathException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.Test;

/**
 * Tests for CoopStorageDriverBase concurrency management and part-level retry.
 */
@SuppressWarnings("unchecked")
class CoopStorageDriverBaseTest {

	private static class CapturingAppender extends AbstractAppender {
		private final List<LogEvent> events = Collections.synchronizedList(new ArrayList<>());

		CapturingAppender() {
			super("coop-driver-base-test-capture", null, null, true, Property.EMPTY_ARRAY);
		}

		@Override
		public void append(final LogEvent event) {
			events.add(event.toImmutable());
		}

		List<LogEvent> events() {
			return List.copyOf(events);
		}
	}

	private static Config storageConfigForMultipartLimits(final int mpuObjectLimit, final int mpuPartLimit) {
		return storageConfigForMultipartLimits(mpuObjectLimit, mpuPartLimit, 4);
	}

	private static Config storageConfigForMultipartLimits(
					final int mpuObjectLimit, final int mpuPartLimit, final int concurrencyLimit) {
		final var storageConfig = mock(Config.class);
		final var driverConfig = mock(Config.class);
		final var limitConfig = mock(Config.class);
		final var authConfig = mock(Config.class);
		final var integrityConfig = mock(Config.class);
		final var integrityInputConfig = mock(Config.class);

		when(storageConfig.configVal("driver")).thenReturn(driverConfig);
		when(driverConfig.configVal("limit")).thenReturn(limitConfig);
		when(storageConfig.stringVal("namespace")).thenReturn("test-ns");
		when(storageConfig.configVal("auth")).thenReturn(authConfig);
		when(storageConfig.configVal("integrity")).thenReturn(integrityConfig);
		when(integrityConfig.stringVal("mode")).thenReturn("none");
		when(integrityConfig.stringVal("algorithm")).thenReturn("sha256");
		when(integrityConfig.configVal("input")).thenReturn(integrityInputConfig);
		when(integrityInputConfig.stringVal("provenance")).thenReturn("none");
		when(integrityInputConfig.stringVal("expectedProducerId")).thenReturn("");
		when(authConfig.stringVal("uid")).thenReturn("user");
		when(authConfig.stringVal("secret")).thenReturn("secret");
		when(authConfig.stringVal("token")).thenReturn(null);
		when(limitConfig.intVal("concurrency")).thenReturn(concurrencyLimit);
		when(driverConfig.intVal("threads")).thenReturn(0);
		when(storageConfig.intVal("driver-limit-queue-input")).thenReturn(16);
		when(storageConfig.intVal("driver-limit-multipart-objects")).thenReturn(mpuObjectLimit);
		when(storageConfig.intVal("driver-limit-multipart-parts")).thenReturn(mpuPartLimit);

		return storageConfig;
	}

	private static Config storageConfigWithMalformedMultipartLimit(final RuntimeException parseException) {
		final var storageConfig = storageConfigForMultipartLimits(0, 0);
		when(storageConfig.intVal("driver-limit-multipart-objects")).thenThrow(parseException);
		return storageConfig;
	}

	private static Config storageConfigWithMissingMultipartLimits() {
		final var storageConfig = storageConfigForMultipartLimits(0, 0);
		when(storageConfig.intVal("driver-limit-multipart-objects"))
						.thenThrow(new NoSuchElementException("driver-limit-multipart-objects"));
		when(storageConfig.intVal("driver-limit-multipart-parts"))
						.thenThrow(new InvalidValuePathException("driver-limit-multipart-parts"));
		return storageConfig;
	}

	private static Semaphore mpuObjectThrottleOf(final CoopStorageDriverBase<Item, Operation<Item>> driver) throws Exception {
		final var field = CoopStorageDriverBase.class.getDeclaredField("mpuObjectThrottle");
		field.setAccessible(true);
		return (Semaphore) field.get(driver);
	}

	private static int mpuMaxPartsOf(final CoopStorageDriverBase<Item, Operation<Item>> driver) throws Exception {
		final var field = CoopStorageDriverBase.class.getDeclaredField("mpuMaxParts");
		field.setAccessible(true);
		return field.getInt(driver);
	}

	private static CompositeDataOperationImpl<DataItem> newCompositeParent(
					final String name, final long size, final long partSize) throws Exception {
		final var baseItem = new DataItemImpl(name, 0, size);
		baseItem.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		return new CompositeDataOperationImpl<>(
						0, OpType.CREATE, baseItem, null, "/bucket", null, null, 0, partSize);
	}

	private static void awaitCapturedEvents(
					final CapturingAppender appender, final int minCount, final long timeoutMillis)
					throws InterruptedException {
		final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
		while (appender.events().size() < minCount && System.nanoTime() < deadlineNanos) {
			Thread.sleep(10);
		}
	}

	private static void awaitCondition(
					final BooleanSupplier condition, final String failureMessage, final long timeoutMillis)
					throws InterruptedException {
		final long deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
		while (!condition.getAsBoolean() && System.nanoTime() < deadlineNanos) {
			Thread.sleep(10);
		}
		assertTrue(condition.getAsBoolean(), failureMessage);
	}

	@Test
	void dispatcherCannotObserveInputBeforeDriverQueueOwnershipIsPublished() throws Exception {
		final var lifecycleClaimEntered = new CountDownLatch(1);
		final var releaseLifecycleClaim = new CountDownLatch(1);
		final var submitEntered = new CountDownLatch(1);
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 1);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new CoopStorageDriverMock<DataItem, Operation<DataItem>>(
						"atomic-queue-publication-step", dataInput, storageConfig, false, 1) {
			@Override
			protected boolean submit(final Operation<DataItem> op) {
				submitEntered.countDown();
				return super.submit(op);
			}
		};
		final Output<Operation<DataItem>> resultOutput = mock(Output.class);
		when(resultOutput.put(any(Operation.class))).thenReturn(true);
		driver.operationResultOutput(resultOutput);
		final Operation<DataItem> op = new OperationImpl<>(
						0, OpType.DELETE, new DataItemImpl("atomic-queue-publication", 0, 1),
						null, "/bucket", null) {
			@Override
			public synchronized OperationLifecycle startNextLifecycle() {
				lifecycleClaimEntered.countDown();
				try {
					releaseLifecycleClaim.await();
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new AssertionError(e);
				}
				return super.startNextLifecycle();
			}
		};
		final var putResult = new AtomicReference<Boolean>();
		final var asyncFailure = new AtomicReference<Throwable>();
		final var dispatchTaskField = CoopStorageDriverBase.class.getDeclaredField("opDispatchTask");
		dispatchTaskField.setAccessible(true);
		final var dispatchTask = (OperationDispatchTask<DataItem, Operation<DataItem>>) dispatchTaskField.get(driver);

		try {
			driver.start();
			dispatchTask.stop();
			assertTrue(dispatchTask.await(2, TimeUnit.SECONDS));
			final var producer = Thread.ofVirtual().start(() -> {
				try {
					putResult.set(driver.put(op));
				} catch (final Throwable t) {
					asyncFailure.compareAndSet(null, t);
				}
			});
			assertTrue(lifecycleClaimEntered.await(2, TimeUnit.SECONDS));
			final var dispatcher = Thread.ofVirtual().start(() -> {
				try {
					dispatchTask.doWork();
				} catch (final Throwable t) {
					asyncFailure.compareAndSet(null, t);
				}
			});

			assertFalse(submitEntered.await(200, TimeUnit.MILLISECONDS),
							"dispatch must not observe an input before its lifecycle ownership");
			releaseLifecycleClaim.countDown();
			producer.join();
			dispatcher.join();
			assertNull(asyncFailure.get());
			assertEquals(Boolean.TRUE, putResult.get());
			assertTrue(submitEntered.await(2, TimeUnit.SECONDS));
			awaitCondition(
							() -> op.lifecycle().state() == OperationLifecycleState.TERMINAL,
							"the published operation should complete exactly once", 2000);
			assertEquals(0, driver.operationLifecycle().snapshot().driverQueued());
			assertEquals(0, driver.operationLifecycle().snapshot().inFlight());
			assertEquals(1, driver.operationLifecycle().snapshot().terminal());
		} finally {
			releaseLifecycleClaim.countDown();
			driver.close();
		}
	}

	@Test
	void queueClosureRecoversUndispatchedWorkAndRejectsNewAdmission() throws Exception {
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 1);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new CoopStorageDriverMock<DataItem, Operation<DataItem>>(
						"queue-close-step", dataInput, storageConfig, false, 4) {
			@Override
			protected boolean submit(final Operation<DataItem> op) {
				return false;
			}

			@Override
			protected int submit(
							final List<Operation<DataItem>> ops, final int from, final int to) {
				return 0;
			}

			@Override
			protected int submit(final List<Operation<DataItem>> ops) {
				return 0;
			}
		};
		final Output<Operation<DataItem>> resultOutput = mock(Output.class);
		when(resultOutput.put(any(Operation.class))).thenReturn(true);
		driver.operationResultOutput(resultOutput);
		final Operation<DataItem> queued = new DataOperationImpl<>(
						0,
						OpType.DELETE,
						new DataItemImpl("queued", 0, 1),
						null,
						"/bucket",
						null,
						List.of(),
						0);

		try {
			driver.start();
			assertTrue(driver.put(queued));
			awaitCondition(
							() -> driver.operationLifecycle().snapshot().driverQueued() == 1,
							"operation should remain queued while dispatch capacity is exhausted",
							2000);

			driver.closeAdmission();
			final var recovered = driver.recoverQueuedOperations();

			assertEquals(List.of(queued), recovered);
			assertEquals(OperationLifecycleState.UNATTEMPTED, queued.lifecycle().state());
			assertEquals(0, driver.operationLifecycle().snapshot().dispatched());
			assertEquals(1, driver.operationLifecycle().snapshot().unattempted());
			assertThrows(java.io.EOFException.class, () -> driver.put(new DataOperationImpl<>(
							0,
							OpType.DELETE,
							new DataItemImpl("late", 0, 1),
							null,
							"/bucket",
							null,
							List.of(),
							0)));
			assertTrue(driver.recoverQueuedOperations().isEmpty(), "queue recovery must be idempotent");
		} finally {
			driver.close();
		}
	}

	@Test
	void queueRecoveryPreservesDistinctValueEqualCompatibilityOperations() throws Exception {
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 1);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new CoopStorageDriverMock<DataItem, Operation<DataItem>>(
						"identity-recovery-step", dataInput, storageConfig, false, 4) {
			@Override
			protected boolean submit(final Operation<DataItem> op) {
				return false;
			}

			@Override
			protected int submit(
							final List<Operation<DataItem>> ops, final int from, final int to) {
				return 0;
			}
		};
		final var first = new ValueEqualLegacyOperation("value-equal-first", 31, 9).operation();
		final var second = new ValueEqualLegacyOperation("value-equal-second", 31, 9).operation();
		assertTrue(first.equals(second));

		try {
			driver.start();
			assertEquals(2, driver.put(List.of(first, second)));
			awaitCondition(
							() -> driver.operationLifecycle().snapshot().driverQueued() == 2,
							"both compatibility identities should remain driver-queued", 2000);

			driver.closeAdmission();
			final var recovered = driver.recoverQueuedOperations();

			assertEquals(2, recovered.size());
			assertTrue(recovered.stream().anyMatch(op -> op == first));
			assertTrue(recovered.stream().anyMatch(op -> op == second));
			assertEquals(2, driver.operationLifecycle().snapshot().unattempted());
			assertEquals(0, driver.operationLifecycle().snapshot().driverQueued());
		} finally {
			driver.close();
		}
	}

	@Test
	void submitOutlivingDispatcherStopWaitIsUnresolvedRatherThanFalselyUnattempted() throws Exception {
		final var submitEntered = new CountDownLatch(1);
		final var releaseSubmit = new CountDownLatch(1);
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 1);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new CoopStorageDriverMock<DataItem, Operation<DataItem>>(
						"blocked-submit-step", dataInput, storageConfig, false, 1) {
			@Override
			protected boolean submit(final Operation<DataItem> op) {
				submitEntered.countDown();
				while (releaseSubmit.getCount() > 0) {
					try {
						releaseSubmit.await();
					} catch (final InterruptedException ignored) {
						// Deliberately model an extension which does not honor task interruption.
					}
				}
				return true;
			}

			@Override
			protected int submit(
							final List<Operation<DataItem>> ops, final int from, final int to) {
				return submit(ops.get(from)) ? 1 : 0;
			}
		};
		final Operation<DataItem> queued = new DataOperationImpl<>(
						0, OpType.DELETE, new DataItemImpl("blocked", 0, 1), null, "/bucket",
						null, List.of(), 0);
		try {
			driver.start();
			assertTrue(driver.put(queued));
			assertTrue(submitEntered.await(2, TimeUnit.SECONDS));

			final long startedAt = System.nanoTime();
			driver.closeAdmission();
			final var recovered = driver.recoverQueuedOperations();
			final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedAt);

			assertTrue(elapsedMillis < 2500, "recovery must retain its one-second task-stop bound");
			assertTrue(recovered.isEmpty(), "an indeterminate submit must not be reported as unattempted");
			assertEquals(OperationLifecycleState.UNRESOLVED, queued.lifecycle().state());
			assertEquals(1, driver.operationLifecycle().snapshot().unresolved());
			releaseSubmit.countDown();
			awaitCondition(
							() -> driver.operationLifecycle().snapshot().unresolved() == 1,
							"a late successful submit return must not change the bounded outcome", 2000);
			assertEquals(0, driver.operationLifecycle().snapshot().dispatched());
			assertEquals(0, driver.operationLifecycle().snapshot().terminal());
		} finally {
			releaseSubmit.countDown();
			driver.close();
		}
	}

	@Test
	void legacyBatchCompletionDoesNotMakeHungSubmittingMembersUnattempted() throws Exception {
		final var submitEntered = new CountDownLatch(1);
		final var releaseSubmit = new CountDownLatch(1);
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 3);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new CoopStorageDriverMock<DataItem, Operation<DataItem>>(
						"blocked-legacy-batch-step", dataInput, storageConfig, false, 3) {
			@Override
			protected boolean submit(final Operation<DataItem> op) {
				return false;
			}

			@Override
			protected int submit(
							final List<Operation<DataItem>> ops, final int from, final int to) {
				assertTrue(handleCompleted(ops.get(from)));
				submitEntered.countDown();
				while (releaseSubmit.getCount() > 0) {
					try {
						releaseSubmit.await();
					} catch (final InterruptedException ignored) {
						// Model a legacy batch blocked after completing one request and starting another.
					}
				}
				return to - from;
			}
		};
		final Output<Operation<DataItem>> resultOutput = mock(Output.class);
		when(resultOutput.put(any(Operation.class))).thenReturn(true);
		driver.operationResultOutput(resultOutput);
		final List<Operation<DataItem>> ops = List.of(
						new DataOperationImpl<>(
										0, OpType.DELETE, new DataItemImpl("legacy-batch-first", 0, 1), null,
										"/bucket", null, List.of(), 0),
						new DataOperationImpl<>(
										0, OpType.DELETE, new DataItemImpl("legacy-batch-second", 0, 1), null,
										"/bucket", null, List.of(), 0),
						new DataOperationImpl<>(
										0, OpType.DELETE, new DataItemImpl("legacy-batch-third", 0, 1), null,
										"/bucket", null, List.of(), 0));
		try {
			driver.start();
			assertEquals(ops.size(), driver.put(ops));
			assertTrue(submitEntered.await(2, TimeUnit.SECONDS));

			driver.closeAdmission();
			final var recovered = driver.recoverQueuedOperations();

			assertTrue(recovered.isEmpty(),
							"a pre-hook batch cannot prove which remaining submissions started transport");
			assertEquals(OperationLifecycleState.TERMINAL, ops.get(0).lifecycle().state());
			assertEquals(OperationLifecycleState.UNRESOLVED, ops.get(1).lifecycle().state());
			assertEquals(OperationLifecycleState.UNRESOLVED, ops.get(2).lifecycle().state());
			assertEquals(1, driver.operationLifecycle().snapshot().terminal());
			assertEquals(2, driver.operationLifecycle().snapshot().unresolved());
			assertEquals(0, driver.operationLifecycle().snapshot().unattempted());
		} finally {
			releaseSubmit.countDown();
			driver.close();
		}
	}

	@Test
	void legacyBatchRecoveryDoesNotResolveARecycledNextCirculation() throws Exception {
		final var submitEntered = new CountDownLatch(1);
		final var releaseSubmit = new CountDownLatch(1);
		final var recycled = new CountDownLatch(1);
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 2);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new CoopStorageDriverMock<DataItem, Operation<DataItem>>(
						"recycled-legacy-batch-step", dataInput, storageConfig, false, 2) {
			@Override
			protected boolean submit(final Operation<DataItem> op) {
				return false;
			}

			@Override
			protected int submit(
							final List<Operation<DataItem>> ops, final int from, final int to) {
				assertTrue(handleCompleted(ops.get(from)));
				submitEntered.countDown();
				while (releaseSubmit.getCount() > 0) {
					try {
						releaseSubmit.await();
					} catch (final InterruptedException ignored) {
						// Keep the original batch active while its first identity is recycled.
					}
				}
				return to - from;
			}
		};
		final Operation<DataItem> first = mock(Operation.class, CALLS_REAL_METHODS);
		final Operation<DataItem> second = mock(Operation.class, CALLS_REAL_METHODS);
		when(first.result()).thenReturn(first);
		final Output<Operation<DataItem>> recyclingOutput = mock(Output.class);
		when(recyclingOutput.put(first)).thenAnswer(invocation -> {
			assertTrue(driver.operationLifecycle().generatorBuffered(first));
			assertTrue(driver.put(first));
			recycled.countDown();
			return true;
		});
		driver.operationResultOutput(recyclingOutput);

		try {
			driver.start();
			assertEquals(2, driver.put(List.of(first, second)));
			assertTrue(recycled.await(2, TimeUnit.SECONDS));
			assertTrue(submitEntered.await(2, TimeUnit.SECONDS));

			driver.closeAdmission();
			final var recovered = driver.recoverQueuedOperations();

			assertEquals(1, recovered.size());
			assertSame(first, recovered.get(0),
							"the recycled circulation remains recoverable independently of the hung batch");
			assertEquals(OperationLifecycleState.UNATTEMPTED,
							driver.operationLifecycle().stateOf(first));
			assertEquals(OperationLifecycleState.UNRESOLVED,
							driver.operationLifecycle().stateOf(second));
			assertEquals(1, driver.operationLifecycle().snapshot().terminal());
			assertEquals(1, driver.operationLifecycle().snapshot().unattempted());
			assertEquals(1, driver.operationLifecycle().snapshot().unresolved());
		} finally {
			releaseSubmit.countDown();
			driver.close();
		}
	}

	@Test
	void explicitBatchDispatchRecoversMembersStillQueuedBehindAHungFirstAttempt() throws Exception {
		final var submitEntered = new CountDownLatch(1);
		final var releaseSubmit = new CountDownLatch(1);
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 3);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new CoopStorageDriverMock<DataItem, Operation<DataItem>>(
						"blocked-explicit-batch-step", dataInput, storageConfig, false, 3) {
			@Override
			protected boolean submit(final Operation<DataItem> op) {
				// Retain an early/spuriously drained first member until the dispatcher can
				// fill its batch buffer from the other already-admitted operations.
				return false;
			}

			@Override
			protected int submit(
							final List<Operation<DataItem>> ops, final int from, final int to) {
				if (!beginDispatch(ops.get(from))) {
					return 0;
				}
				submitEntered.countDown();
				while (releaseSubmit.getCount() > 0) {
					try {
						releaseSubmit.await();
					} catch (final InterruptedException ignored) {
						// Model a connection lease which has not returned to submit yet.
					}
				}
				return 1;
			}
		};
		final List<Operation<DataItem>> ops = List.of(
						new DataOperationImpl<>(
										0, OpType.DELETE, new DataItemImpl("batch-first", 0, 1), null,
										"/bucket", null, List.of(), 0),
						new DataOperationImpl<>(
										0, OpType.DELETE, new DataItemImpl("batch-second", 0, 1), null,
										"/bucket", null, List.of(), 0),
						new DataOperationImpl<>(
										0, OpType.DELETE, new DataItemImpl("batch-third", 0, 1), null,
										"/bucket", null, List.of(), 0));
		try {
			driver.start();
			assertEquals(ops.size(), driver.put(ops));
			assertTrue(submitEntered.await(2, TimeUnit.SECONDS));

			driver.closeAdmission();
			final var recovered = driver.recoverQueuedOperations();

			assertEquals(2, recovered.size());
			assertTrue(recovered.containsAll(List.of(ops.get(1), ops.get(2))),
							"only the first batch member crossed the explicit dispatch boundary");
			assertEquals(OperationLifecycleState.DISPATCHED, ops.get(0).lifecycle().state());
			assertEquals(OperationLifecycleState.UNATTEMPTED, ops.get(1).lifecycle().state());
			assertEquals(OperationLifecycleState.UNATTEMPTED, ops.get(2).lifecycle().state());
			assertEquals(1, driver.operationLifecycle().resolveOutstandingAsUnresolved());
			assertEquals(OperationLifecycleState.UNRESOLVED, ops.get(0).lifecycle().state());
		} finally {
			releaseSubmit.countDown();
			driver.close();
		}
	}

	@Test
	void blockedResultOutputRemainsInFlightUntilDeadlineWins() throws Exception {
		final var outputEntered = new CountDownLatch(1);
		final var releaseOutput = new CountDownLatch(1);
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 1);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new CoopStorageDriverMock<DataItem, Operation<DataItem>>(
						"blocked-result-step", dataInput, storageConfig, false, 1);
		driver.operationResultOutput(new Output<>() {
			@Override
			public boolean put(final Operation<DataItem> item) {
				outputEntered.countDown();
				try {
					releaseOutput.await();
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
					return false;
				}
				return true;
			}

			@Override
			public int put(final List<Operation<DataItem>> buffer, final int from, final int to) {
				return to - from;
			}

			@Override
			public int put(final List<Operation<DataItem>> buffer) {
				return buffer.size();
			}

			@Override
			public com.github.akurilov.commons.io.Input<Operation<DataItem>> getInput() {
				return null;
			}

			@Override
			public void close() {}
		});
		final Operation<DataItem> op = new DataOperationImpl<>(
						0, OpType.DELETE, new DataItemImpl("result", 0, 1), null, "/bucket",
						null, List.of(), 0);
		final var accepted = new AtomicReference<Boolean>();
		try {
			driver.start();
			assertTrue(driver.operationLifecycle().driverQueued(op));
			assertTrue(driver.operationLifecycle().dispatched(op));
			final var completion = Thread.ofVirtual().start(() -> accepted.set(driver.handleCompleted(op)));
			assertTrue(outputEntered.await(2, TimeUnit.SECONDS));

			assertEquals(OperationLifecycleState.COMPLETING, op.lifecycle().state());
			assertEquals(1, driver.operationLifecycle().inFlightCount());
			assertEquals(0, driver.operationLifecycle().snapshot().terminal());
			assertEquals(1, driver.operationLifecycle().resolveOutstandingAsUnresolved(),
							"the deadline must resolve a completion whose output has not accepted the result");

			releaseOutput.countDown();
			completion.join();
			assertEquals(Boolean.FALSE, accepted.get());
			assertEquals(OperationLifecycleState.UNRESOLVED, op.lifecycle().state());
			assertEquals(0, driver.operationLifecycle().snapshot().inFlight());
			assertEquals(0, driver.operationLifecycle().snapshot().terminal());
			assertEquals(1, driver.operationLifecycle().snapshot().unresolved());
		} finally {
			releaseOutput.countDown();
			driver.close();
		}
	}

	@Test
	void rejectedResultOutputRemainsUnresolvedWithoutRetainedBacklog() throws Exception {
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 1);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new CoopStorageDriverMock<DataItem, Operation<DataItem>>(
						"rejected-result-step", dataInput, storageConfig, false, 1);
		driver.operationResultOutput(mock(Output.class));
		final Operation<DataItem> op = new DataOperationImpl<>(
						0, OpType.DELETE, new DataItemImpl("rejected", 0, 1), null, "/bucket",
						null, List.of(), 0);
		try {
			driver.start();
			assertTrue(driver.operationLifecycle().driverQueued(op));
			assertTrue(driver.operationLifecycle().dispatched(op));

			assertFalse(driver.handleCompleted(op));

			assertEquals(OperationLifecycleState.UNRESOLVED, op.lifecycle().state());
			assertEquals(0, driver.operationLifecycle().snapshot().terminal());
			assertEquals(1, driver.operationLifecycle().snapshot().unresolved());
			assertEquals(0, driver.operationLifecycle().inFlightCount());
		} finally {
			driver.close();
		}
	}

	@Test
	void compatibilityResultMayRecycleSameInstanceDuringSynchronousOutput() throws Exception {
		final var driver = newRetryTestDriver();
		final var lifecycle = new OperationLifecycleTracker<Operation<Item>>();
		final var lifecycleField = StorageDriverBase.class.getDeclaredField("operationLifecycle");
		lifecycleField.setAccessible(true);
		lifecycleField.set(driver, lifecycle);
		final Operation<Item> legacy = mock(Operation.class, CALLS_REAL_METHODS);
		when(legacy.status()).thenReturn(Operation.Status.SUCC);
		when(legacy.result()).thenReturn(legacy);
		assertTrue(lifecycle.driverQueued(legacy));
		assertTrue(lifecycle.dispatched(legacy));
		final Output<Operation<Item>> recyclingOutput = mock(Output.class);
		when(recyclingOutput.put(legacy)).thenAnswer(invocation -> {
			assertTrue(lifecycle.generatorBuffered(legacy));
			assertTrue(lifecycle.driverQueued(legacy));
			return true;
		});
		final var outField = StorageDriverBase.class.getDeclaredField("opResultOut");
		outField.setAccessible(true);
		outField.set(driver, recyclingOutput);

		assertTrue(driver.handleCompleted(legacy));

		assertEquals(OperationLifecycleState.DRIVER_QUEUED, lifecycle.stateOf(legacy));
		assertEquals(1, lifecycle.snapshot().terminal());
		assertEquals(0, lifecycle.snapshot().inFlight());
	}

	@Test
	void lateOutputReturnAfterDeadlineAndResetDoesNotMutateNewRunCounters() throws Exception {
		final var outputEntered = new CountDownLatch(1);
		final var releaseOutput = new CountDownLatch(1);
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 1);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new CoopStorageDriverMock<DataItem, Operation<DataItem>>(
						"late-rejected-result-step", dataInput, storageConfig, false, 1);
		driver.operationResultOutput(new Output<>() {
			@Override
			public boolean put(final Operation<DataItem> item) {
				outputEntered.countDown();
				try {
					releaseOutput.await();
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
				}
				return false;
			}

			@Override
			public int put(final List<Operation<DataItem>> buffer, final int from, final int to) {
				return 0;
			}

			@Override
			public int put(final List<Operation<DataItem>> buffer) {
				return 0;
			}

			@Override
			public com.github.akurilov.commons.io.Input<Operation<DataItem>> getInput() {
				return null;
			}

			@Override
			public void close() {}
		});
		final Operation<DataItem> op = new DataOperationImpl<>(
						0, OpType.DELETE, new DataItemImpl("late-rejected", 0, 1), null, "/bucket",
						null, List.of(), 0);
		try {
			driver.start();
			assertTrue(driver.operationLifecycle().driverQueued(op));
			assertTrue(driver.operationLifecycle().dispatched(op));
			final var completion = Thread.ofVirtual().start(() -> driver.handleCompleted(op));
			assertTrue(outputEntered.await(2, TimeUnit.SECONDS));

			assertEquals(1, driver.operationLifecycle().resolveOutstandingAsUnresolved());
			driver.operationLifecycle().reset();
			releaseOutput.countDown();
			completion.join();

			assertEquals(OperationLifecycleState.UNRESOLVED, op.lifecycle().state());
			assertEquals(0, driver.operationLifecycle().snapshot().terminal());
			assertEquals(0, driver.operationLifecycle().snapshot().unresolved());
		} finally {
			releaseOutput.countDown();
			driver.close();
		}
	}

	@Test
	void malformedMultipartLimitLogsWarningAndFallsBackToUnlimited() throws Exception {
		final var storageConfig = storageConfigWithMalformedMultipartLimit(
						new IllegalArgumentException("For input string: \"oops\""));
		final var appender = new CapturingAppender();
		appender.start();

		final var loggerCtx = LoggerContext.getContext(false);
		final var logger = loggerCtx.getLogger(Loggers.ERR.getName());
		final var originalLevel = logger.getLevel();
		logger.addAppender(appender);
		logger.setLevel(Level.WARN);

		final var dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		CoopStorageDriverMock<Item, Operation<Item>> driver = null;
		try {
			driver = new CoopStorageDriverMock<>("parse-warn-step", dataInput, storageConfig, false, 16);

			assertNull(mpuObjectThrottleOf(driver), "malformed multipart limits should fall back to unlimited MPU objects");
			assertEquals(0, mpuMaxPartsOf(driver), "malformed multipart limits should fall back to unlimited MPU parts");
			awaitCapturedEvents(appender, 1, 2000);

			final var parseWarnEvents = appender.events().stream()
							.filter(e -> Level.WARN.equals(e.getLevel()))
							.map(e -> e.getMessage().getFormattedMessage())
							.filter(msg -> msg.contains("Failed to parse multipart limits"))
							.toList();
			assertEquals(1, parseWarnEvents.size(), "exactly one multipart parse warning should be logged");
			assertTrue(parseWarnEvents.get(0).contains("For input string: \"oops\""),
							"warning should include parse failure details");
			assertTrue(parseWarnEvents.get(0).contains("Proceeding with unlimited"),
							"warning should indicate fallback behavior");
		} finally {
			try {
				if (driver != null) {
					driver.close();
				} else {
					dataInput.close();
				}
			} finally {
				logger.removeAppender(appender);
				logger.setLevel(originalLevel);
				appender.stop();
			}
		}
	}

	@Test
	void validMultipartLimitsConfigureThrottleWithoutParseWarning() throws Exception {
		final var storageConfig = storageConfigForMultipartLimits(3, 9);
		final var appender = new CapturingAppender();
		appender.start();

		final var loggerCtx = LoggerContext.getContext(false);
		final var logger = loggerCtx.getLogger(Loggers.ERR.getName());
		final var originalLevel = logger.getLevel();
		logger.addAppender(appender);
		logger.setLevel(Level.WARN);

		final var dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		CoopStorageDriverMock<Item, Operation<Item>> driver = null;
		try {
			driver = new CoopStorageDriverMock<>("parse-ok-step", dataInput, storageConfig, false, 16);

			final var mpuThrottle = mpuObjectThrottleOf(driver);
			assertNotNull(mpuThrottle, "valid multipart object limit should create MPU throttle");
			assertEquals(3, mpuThrottle.availablePermits(),
							"MPU throttle permits should match configured multipart object limit");
			assertEquals(9, mpuMaxPartsOf(driver), "multipart part limit should match configured value");

			final var parseWarnCount = appender.events().stream()
							.filter(e -> Level.WARN.equals(e.getLevel()))
							.map(e -> e.getMessage().getFormattedMessage())
							.filter(msg -> msg.contains("Failed to parse multipart limits"))
							.count();
			assertEquals(0, parseWarnCount, "valid multipart limits should not produce parse warnings");
		} finally {
			try {
				if (driver != null) {
					driver.close();
				} else {
					dataInput.close();
				}
			} finally {
				logger.removeAppender(appender);
				logger.setLevel(originalLevel);
				appender.stop();
			}
		}
	}

	@Test
	void mpuSchedulingDerivesDefaultsForLargeMultipartObject() throws Exception {
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 100);
		final var dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		try (final var driver = new CoopStorageDriverMock<Item, Operation<Item>>(
						"derive-large-mpu", dataInput, storageConfig, false, 16)) {
			final var parent = newCompositeParent("large", 5000, 1);

			assertTrue(((CoopStorageDriverBase<Item, Operation<Item>>) driver)
							.tryAcquireMpuObjectPermit((Operation<Item>) (Operation<?>) parent));

			final var throttle = mpuObjectThrottleOf(driver);
			assertNotNull(throttle, "derived MPU object throttle should be created");
			assertEquals(0, throttle.availablePermits(), "5000-part object should allow one active MPU at 100 threads");
			assertEquals(100, mpuMaxPartsOf(driver), "large MPU should use all 100 threads as the part window");
		}
	}

	@Test
	void tryAcquireMpuObjectPermitIgnoresNonMpuOperations() throws Exception {
		final var driver = newRetryTestDriver();
		final var mpuThrottle = new Semaphore(1, true);
		mpuThrottle.acquire();
		final var mpuField = CoopStorageDriverBase.class.getDeclaredField("mpuObjectThrottle");
		mpuField.setAccessible(true);
		mpuField.set(driver, mpuThrottle);

		final Operation<Item> op = mock(Operation.class);
		when(op.type()).thenReturn(OpType.CREATE);

		assertTrue(driver.tryAcquireMpuObjectPermit(op), "non-MPU operations should not consume MPU object permits");
		assertEquals(0, mpuThrottle.availablePermits(), "non-MPU operation should leave MPU permits unchanged");
	}

	@Test
	void mpuSchedulingDerivesMultipleActiveObjectsForSmallMultipartObjects() throws Exception {
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 100);
		final var dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		try (final var driver = new CoopStorageDriverMock<Item, Operation<Item>>(
						"derive-small-mpu", dataInput, storageConfig, false, 16)) {
			final var parent = newCompositeParent("small", 5, 1);

			assertTrue(((CoopStorageDriverBase<Item, Operation<Item>>) driver)
							.tryAcquireMpuObjectPermit((Operation<Item>) (Operation<?>) parent));

			final var throttle = mpuObjectThrottleOf(driver);
			assertNotNull(throttle, "derived MPU object throttle should be created");
			assertEquals(19, throttle.availablePermits(), "5-part objects should admit 20 active MPUs at 100 threads");
			assertEquals(5, mpuMaxPartsOf(driver), "part window should not exceed the object part count");
		}
	}

	@Test
	void mpuSchedulingPreservesExplicitObjectLimitAndDerivesPartWindow() throws Exception {
		final var storageConfig = storageConfigForMultipartLimits(2, 0, 100);
		final var dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		try (final var driver = new CoopStorageDriverMock<Item, Operation<Item>>(
						"explicit-objects-mpu", dataInput, storageConfig, false, 16)) {
			final var parent = newCompositeParent("explicit-objects", 5000, 1);

			assertTrue(((CoopStorageDriverBase<Item, Operation<Item>>) driver)
							.tryAcquireMpuObjectPermit((Operation<Item>) (Operation<?>) parent));

			assertEquals(1, mpuObjectThrottleOf(driver).availablePermits(),
							"explicit object limit should be respected exactly");
			assertEquals(50, mpuMaxPartsOf(driver), "part window should be derived to fill 100 threads across 2 MPUs");
		}
	}

	@Test
	void mpuSchedulingPreservesExplicitPartLimitAndDerivesObjectLimit() throws Exception {
		final var storageConfig = storageConfigForMultipartLimits(0, 10, 100);
		final var dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		try (final var driver = new CoopStorageDriverMock<Item, Operation<Item>>(
						"explicit-parts-mpu", dataInput, storageConfig, false, 16)) {
			final var parent = newCompositeParent("explicit-parts", 5000, 1);

			assertTrue(((CoopStorageDriverBase<Item, Operation<Item>>) driver)
							.tryAcquireMpuObjectPermit((Operation<Item>) (Operation<?>) parent));

			assertEquals(9, mpuObjectThrottleOf(driver).availablePermits(),
							"derived object limit should fill 100 threads with a 10-part window");
			assertEquals(10, mpuMaxPartsOf(driver), "explicit part limit should be respected exactly");
		}
	}

	@Test
	void missingMultipartLimitsDoNotEmitParseWarnings() throws Exception {
		final var storageConfig = storageConfigWithMissingMultipartLimits();
		final var appender = new CapturingAppender();
		appender.start();

		final var loggerCtx = LoggerContext.getContext(false);
		final var logger = loggerCtx.getLogger(Loggers.ERR.getName());
		final var originalLevel = logger.getLevel();
		logger.addAppender(appender);
		logger.setLevel(Level.WARN);

		final var dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		CoopStorageDriverMock<Item, Operation<Item>> driver = null;
		try {
			driver = new CoopStorageDriverMock<>("parse-missing-step", dataInput, storageConfig, false, 16);

			assertNull(mpuObjectThrottleOf(driver), "missing multipart object limit should keep MPU objects unlimited");
			assertEquals(0, mpuMaxPartsOf(driver), "missing multipart part limit should keep MPU parts unlimited");

			final var parseWarnCount = appender.events().stream()
							.filter(e -> Level.WARN.equals(e.getLevel()))
							.map(e -> e.getMessage().getFormattedMessage())
							.filter(msg -> msg.contains("Failed to parse multipart limits"))
							.count();
			assertEquals(0, parseWarnCount, "missing optional multipart limits should not emit parse warnings");
		} finally {
			try {
				if (driver != null) {
					driver.close();
				} else {
					dataInput.close();
				}
			} finally {
				logger.removeAppender(appender);
				logger.setLevel(originalLevel);
				appender.stop();
			}
		}
	}

	@Test
	void scheduledAndCompletedCountersAreIndependent() throws Exception {
		final var scheduledField = CoopStorageDriverBase.class.getDeclaredField("scheduledOpCount");
		scheduledField.setAccessible(true);
		final var completedField = CoopStorageDriverBase.class.getDeclaredField("completedOpCount");
		completedField.setAccessible(true);

		final var driver = mock(CoopStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
		scheduledField.set(driver, new LongAdder());
		completedField.set(driver, new LongAdder());

		assertEquals(0, driver.scheduledOpCount());
		assertEquals(0, driver.completedOpCount());
	}

	@Test
	void activeOpCountReflectsSemaphoreState() throws Exception {
		final var driver = mock(CoopStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		final var sem = new Semaphore(4, true);
		semField.set(driver, sem);

		final var limitField = CoopStorageDriverBase.class.getSuperclass().getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 4);

		assertEquals(0, driver.activeOpCount(), "all permits free = 0 active");

		sem.acquire(2);
		assertEquals(2, driver.activeOpCount(), "2 permits taken = 2 active");

		sem.release(2);
		assertEquals(0, driver.activeOpCount(), "all permits released = 0 active");
	}

	@Test
	void isIdleWhenAllPermitsFree() throws Exception {
		final var driver = mock(CoopStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));
		final var semField = CoopStorageDriverBase.class.getDeclaredField("concurrencyThrottle");
		semField.setAccessible(true);
		final var sem = new Semaphore(4, true);
		semField.set(driver, sem);

		final var limitField = CoopStorageDriverBase.class.getSuperclass().getDeclaredField("concurrencyLimit");
		limitField.setAccessible(true);
		limitField.set(driver, 4);

		assertTrue(driver.isIdle(), "should be idle when all permits free");

		sem.acquire(1);
		assertFalse(driver.isIdle(), "should not be idle with permit held");

		sem.release(1);
		assertTrue(driver.isIdle(), "should be idle after release");
	}

	@Test
	@SuppressWarnings("deprecation")
	void deprecatedFastRecycleHooksWarnOnlyOncePerDriver() throws Exception {
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var appender = new CapturingAppender();
		appender.start();
		final var loggerCtx = LoggerContext.getContext(false);
		final var logger = loggerCtx.getLogger(Loggers.MSG.getName());
		final var originalLevel = logger.getLevel();

		try (final var driver = new CoopStorageDriverMock<Item, Operation<Item>>(
						"deprecated-fast-recycle",
						dataInput,
						storageConfigForMultipartLimits(0, 0),
						false,
						16)) {
			logger.addAppender(appender);
			logger.setLevel(Level.WARN);

			driver.enableFastRecycle(4);
			driver.enableFastRecycle(8);
			driver.enableFastRecycleQuiesce();
			awaitCapturedEvents(appender, 1, 2000);

			final var warningMessages = appender.events().stream()
							.filter(e -> Level.WARN.equals(e.getLevel()))
							.map(e -> e.getMessage().getFormattedMessage())
							.filter(msg -> msg.contains("deprecated fast-recycle request ignored"))
							.toList();
			assertEquals(1, warningMessages.size(), "deprecated hooks should warn once per driver");
			assertTrue(warningMessages.get(0).contains("shared generator circulation"));
		} finally {
			logger.removeAppender(appender);
			logger.setLevel(originalLevel);
			appender.stop();
		}
	}

	// ---------- Simple-op completion path ----------

	@Test
	void handleCompleted_simpleSuccessOpSendsResultToOutput() throws Exception {
		final var driver = newRetryTestDriver();

		// A simple (non-composite, non-partial) operation
		final Operation<Item> op = mock(Operation.class);
		final Operation<Item> resultCopy = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(resultCopy);

		boolean result = driver.handleCompleted(op);

		assertTrue(result, "handleCompleted should return true for simple successful op");
		// Verify the result copy (not the original) was sent to opResultOut
		final var outField = StorageDriverBase.class.getDeclaredField("opResultOut");
		outField.setAccessible(true);
		final Output<Operation<Item>> opResultOut = (Output<Operation<Item>>) outField.get(driver);
		verify(opResultOut).put(resultCopy);
	}

	@Test
	void handleCompleted_simpleOpDoesNotUseChildQueue() throws Exception {
		final var driver = newRetryTestDriver();

		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		driver.handleCompleted(op);

		final var queue = childQueueOf(driver);
		assertTrue(queue.isEmpty(), "childOpQueue should remain empty for simple ops");
	}

	@Test
	void handleCompleted_simpleOpIncrementsCompletedCount() throws Exception {
		final var driver = newRetryTestDriver();

		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		assertEquals(0, driver.completedOpCount(), "precondition: count starts at 0");

		driver.handleCompleted(op);

		assertEquals(1, driver.completedOpCount(), "completedOpCount should be 1 after one completion");
	}

	@Test
	void handleCompleted_returnsFalseWhenOutputFull() throws Exception {
		final var driver = newRetryTestDriver();

		// Override opResultOut to reject (simulating queue full)
		final Output<Operation<Item>> fullOutput = mock(Output.class);
		when(fullOutput.put(any(Operation.class))).thenReturn(false);
		final var outField = StorageDriverBase.class.getDeclaredField("opResultOut");
		outField.setAccessible(true);
		outField.set(driver, fullOutput);

		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		boolean result = driver.handleCompleted(op);

		assertFalse(result, "should return false when opResultOut rejects the result");
	}

	// ---------- Part-level retry tests ----------

	/** Set up a mock CoopStorageDriverBase with the fields needed by handleCompleted(). */
	private CoopStorageDriverBase<Item, Operation<Item>> newRetryTestDriver() throws Exception {
		return newRetryTestDriver(100);
	}

	/** Set up a mock CoopStorageDriverBase with the fields needed by handleCompleted(). */
	private CoopStorageDriverBase<Item, Operation<Item>> newRetryTestDriver(final int childQueueCapacity) throws Exception {
		final var driver = mock(CoopStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));

		// childOpQueue
		final var childQueueField = CoopStorageDriverBase.class.getDeclaredField("childOpQueue");
		childQueueField.setAccessible(true);
		childQueueField.set(driver, new ArrayBlockingQueue<>(childQueueCapacity));

		// completedOpCount
		final var completedField = CoopStorageDriverBase.class.getDeclaredField("completedOpCount");
		completedField.setAccessible(true);
		completedField.set(driver, new LongAdder());

		// dispatch lock and condition (needed by signalDispatch)
		final var lockField = CoopStorageDriverBase.class.getDeclaredField("dispatchLock");
		lockField.setAccessible(true);
		final var lock = new ReentrantLock();
		lockField.set(driver, lock);
		final var condField = CoopStorageDriverBase.class.getDeclaredField("dispatchReady");
		condField.setAccessible(true);
		condField.set(driver, lock.newCondition());

		// opResultOut on StorageDriverBase (parent) — mock that accepts anything
		final Output<Operation<Item>> mockOutput = mock(Output.class);
		when(mockOutput.put(any(Operation.class))).thenReturn(true);
		final var outField = StorageDriverBase.class.getDeclaredField("opResultOut");
		outField.setAccessible(true);
		outField.set(driver, mockOutput);

		return driver;
	}

	private BlockingQueue<Operation<Item>> childQueueOf(CoopStorageDriverBase<Item, Operation<Item>> driver) throws Exception {
		final var field = CoopStorageDriverBase.class.getDeclaredField("childOpQueue");
		field.setAccessible(true);
		return (BlockingQueue<Operation<Item>>) field.get(driver);
	}

	@Test
	void extensionChildEnqueueRegistersRecoverableDriverOwnership() throws Exception {
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 1);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new CoopStorageDriverMock<Item, Operation<Item>>(
						"child-enqueue-step", dataInput, storageConfig, false, 4);
		final Operation<Item> child = new OperationImpl<>(
						0, OpType.CREATE, new DataItemImpl("child", 0, 1), null, "/bucket", null);
		try {
			assertTrue(driver.enqueueChildOperation(child, child, "extension child"));
			assertEquals(OperationLifecycleState.DRIVER_QUEUED, child.lifecycle().state());
			assertTrue(childQueueOf(driver).contains(child));
		} finally {
			driver.close();
		}
	}

	@Test
	void legacyProtectedChildQueueIsClaimedBeforeDispatchAndRemainsRecoverable() throws Exception {
		final var submitEntered = new CountDownLatch(1);
		final var stateAtSubmit = new AtomicReference<OperationLifecycleState>();
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 1);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final class LegacyChildQueueDriver
						extends CoopStorageDriverMock<Item, Operation<Item>> {
			private LegacyChildQueueDriver() throws Exception {
				super("legacy-child-queue-step", dataInput, storageConfig, false, 4);
			}

			private boolean legacyEnqueue(final Operation<Item> op) {
				return childOpQueue.offer(op);
			}

			@Override
			protected boolean submit(final Operation<Item> op) {
				stateAtSubmit.compareAndSet(null, operationLifecycle().stateOf(op));
				submitEntered.countDown();
				return false;
			}
		}
		final var driver = new LegacyChildQueueDriver();
		final Operation<Item> child = new OperationImpl<>(
						0, OpType.CREATE, new DataItemImpl("legacy-child", 0, 1), null, "/bucket", null);
		try {
			assertTrue(driver.legacyEnqueue(child));
			driver.start();
			assertTrue(submitEntered.await(2, TimeUnit.SECONDS));
			assertEquals(OperationLifecycleState.DRIVER_QUEUED, stateAtSubmit.get(),
							"legacy protected-queue work must gain registry ownership before submit");

			driver.closeAdmission();
			assertEquals(List.of(child), driver.recoverQueuedOperations());
			assertEquals(OperationLifecycleState.UNATTEMPTED, child.lifecycle().state());
		} finally {
			driver.close();
		}
	}

	@Test
	void deferredChildEnqueueOwnsLifecycleBeforeAsyncScheduling() throws Exception {
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 1);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new CoopStorageDriverMock<Item, Operation<Item>>(
						"deferred-child-step", dataInput, storageConfig, false, 4);
		final var queue = new ArrayBlockingQueue<Operation<Item>>(1);
		final var childQueueField = CoopStorageDriverBase.class.getDeclaredField("childOpQueue");
		childQueueField.setAccessible(true);
		childQueueField.set(driver, queue);
		queue.add(new OperationImpl<>(
						0, OpType.CREATE, new DataItemImpl("filler", 0, 1), null, "/bucket", null));
		final Operation<Item> child = new OperationImpl<>(
						0, OpType.CREATE, new DataItemImpl("deferred-child", 0, 1), null, "/bucket", null);
		try {
			assertTrue(driver.enqueueChildOperation(child, child, "deferred extension child"));
			assertEquals(OperationLifecycleState.DRIVER_QUEUED, child.lifecycle().state(),
							"async scheduling must never be the only owner of child work");

			driver.closeAdmission();
			awaitCondition(
							() -> child.lifecycle().state() == OperationLifecycleState.UNATTEMPTED,
							"admission closure should recover the deferred child", 2000);
		} finally {
			driver.close();
		}
	}

	@Test
	void deferredChildEnqueueTimeoutProducesUnattemptedOutcome() throws Exception {
		final String property = CoopStorageDriverBase.CHILD_OP_ENQUEUE_TIMEOUT_MILLIS_PROPERTY;
		final String previous = System.getProperty(property);
		System.setProperty(property, "20");
		final var driver = newRetryTestDriver(1);
		final var lifecycleField = StorageDriverBase.class.getDeclaredField("operationLifecycle");
		lifecycleField.setAccessible(true);
		lifecycleField.set(driver, new OperationLifecycleTracker<Operation<Item>>());
		final var queue = childQueueOf(driver);
		queue.add(mock(Operation.class));
		final Operation<Item> child = new OperationImpl<>(
						0, OpType.CREATE, new DataItemImpl("timed-out-child", 0, 1), null, "/bucket", null);
		try {
			assertTrue(driver.enqueueChildOperation(child, child, "timed out child"));
			awaitCondition(
							() -> child.lifecycle().state() == OperationLifecycleState.UNATTEMPTED,
							"enqueue timeout should retain an unattempted child identity", 2000);
		} finally {
			if (previous == null) {
				System.clearProperty(property);
			} else {
				System.setProperty(property, previous);
			}
		}
	}

	@Test
	void bulkChildExpansionAfterAdmissionClosureRecoversEveryIdentity() throws Exception {
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 1);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new CoopStorageDriverMock<Item, Operation<Item>>(
						"bulk-child-close-step", dataInput, storageConfig, false, 4);
		final List<Operation<Item>> children = List.of(
						new OperationImpl<>(
										0, OpType.CREATE, new DataItemImpl("child-1", 0, 1), null, "/bucket", null),
						new OperationImpl<>(
										0, OpType.CREATE, new DataItemImpl("child-2", 0, 1), null, "/bucket", null));
		try {
			driver.closeAdmission();

			assertFalse(driver.enqueueChildOperations(children, children.get(0), "bulk children"));
			assertTrue(children.stream()
							.allMatch(child -> child.lifecycle().state() == OperationLifecycleState.UNATTEMPTED));
			assertEquals(2, driver.operationLifecycle().snapshot().unattempted());
		} finally {
			driver.close();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void closedAdmissionRecoversCompositeParentCreatedAfterItsPriorTerminalPhase() throws Exception {
		final var storageConfig = storageConfigForMultipartLimits(0, 0, 1);
		final var dataInput = DataInput.instance(
						null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var driver = new CoopStorageDriverMock<Item, Operation<Item>>(
						"late-composite-close-step", dataInput, storageConfig, false, 4);
		final var parent = newCompositeParent("late-composite", 2048, 1024);
		final Operation<Item> parentOp = (Operation<Item>) (Operation<?>) parent;
		final List<Operation<Item>> children = (List<Operation<Item>>) (List<?>) parent.subOperations();
		try {
			assertTrue(driver.operationLifecycle().driverQueued(parentOp));
			assertTrue(driver.operationLifecycle().dispatched(parentOp));
			assertTrue(driver.operationLifecycle().completionStarted(parentOp));
			assertTrue(driver.operationLifecycle().terminal(parentOp),
							"the transport phase may finish before the composite callback expands children");
			driver.closeAdmission();

			assertFalse(driver.enqueueChildOperations(children, parentOp, "late composite children"));

			assertEquals(OperationLifecycleState.UNATTEMPTED, parent.lifecycle().state(),
							"closed admission must reconcile the fresh composite phase after the recovery scan");
			assertTrue(children.stream()
							.allMatch(child -> child.lifecycle().state() == OperationLifecycleState.UNATTEMPTED));
			assertEquals(3, driver.operationLifecycle().snapshot().unattempted());
		} finally {
			driver.close();
		}
	}

	@Test
	void compositeExpansionClaimsParentAndEverySiblingBeforeExecutorRejection() throws Exception {
		final var driver = newRetryTestDriver(1);
		final var lifecycleField = StorageDriverBase.class.getDeclaredField("operationLifecycle");
		lifecycleField.setAccessible(true);
		lifecycleField.set(driver, new OperationLifecycleTracker<Operation<Item>>());
		doThrow(new RejectedExecutionException("test rejection"))
						.when(driver).executeChildEnqueue(any(Runnable.class));

		final var parent = newCompositeParent("rejected-expansion", 3072, 1024);
		parent.subOperations();
		parent.status(Operation.Status.SUCC);
		assertTrue(driver.operationLifecycle().driverQueued((Operation<Item>) (Operation<?>) parent));
		assertTrue(driver.operationLifecycle().dispatched((Operation<Item>) (Operation<?>) parent));

		assertTrue(driver.handleCompleted((Operation<Item>) (Operation<?>) parent),
						"terminal result publication remains accepted even when child scheduling fails");

		final var children = parent.subOperations();
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, parent.lifecycle().state(),
						"the parent must own the incomplete composite phase");
		assertEquals(OperationLifecycleState.DRIVER_QUEUED, children.get(0).lifecycle().state());
		assertEquals(OperationLifecycleState.UNATTEMPTED, children.get(1).lifecycle().state());
		assertEquals(OperationLifecycleState.UNATTEMPTED, children.get(2).lifecycle().state(),
						"later siblings must be claimed before the first scheduling failure");
		assertEquals(2, driver.operationLifecycle().snapshot().unattempted());
	}

	@Test
	void handleCompleted_retriesFailedPartWhenRetriesRemain() throws Exception {
		final var driver = newRetryTestDriver();
		// Build parent (4096 bytes, 1024-byte threshold → 4 parts)
		final var baseItem = new DataItemImpl("obj1", 0, 4096);
		baseItem.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		final var parent = new CompositeDataOperationImpl<DataItem>(
						0, OpType.CREATE, baseItem, null, "/bucket", null, null, 0, 1024);
		parent.subOperations(); // initializes pendingSubTasksCount = 4

		// Grab first part and simulate a failed IO
		final var part = (PartialDataOperationImpl<DataItem>) parent.nextSubOperations(1).get(0);
		part.status(Operation.Status.FAIL_IO);
		// Simulate finishResponse() having been called (decrements parent pending count)
		parent.markSubTaskCompleted(); // pending = 3

		// Act
		boolean result = driver.handleCompleted((Operation<Item>) (Operation<?>) part);

		// Assert
		assertTrue(result, "handleCompleted should return true");
		assertEquals(1, part.retryCount(), "retryCount should be incremented to 1");
		assertEquals(Operation.Status.PENDING, part.status(), "part should be reset to PENDING");
		assertNull(parent.get("mpuAbort"), "mpuAbort should NOT be set when retries remain");
		// Part should be re-enqueued in the child op queue
		final var queue = childQueueOf(driver);
		assertTrue(queue.contains(part), "failed part should be re-enqueued for retry");
		// Parent should NOT be in the queue (other parts still pending)
		assertFalse(queue.contains(parent), "parent should not be re-enqueued yet");
	}

	@Test
	void handleCompleted_abortsWhenRetriesExhausted() throws Exception {
		final var driver = newRetryTestDriver();
		final var baseItem = new DataItemImpl("obj2", 0, 2048);
		baseItem.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		final var parent = new CompositeDataOperationImpl<DataItem>(
						0, OpType.CREATE, baseItem, null, "/bucket", null, null, 0, 1024);
		parent.subOperations(); // pendingSubTasksCount = 2

		final var part = (PartialDataOperationImpl<DataItem>) parent.nextSubOperations(1).get(0);
		// Exhaust retries
		for (int i = 0; i < CoopStorageDriverBase.MAX_PART_RETRIES; i++) {
			part.incrementRetryCount();
		}
		part.status(Operation.Status.FAIL_IO);
		parent.markSubTaskCompleted(); // simulate finishResponse

		boolean result = driver.handleCompleted((Operation<Item>) (Operation<?>) part);

		assertTrue(result, "handleCompleted should return true");
		assertEquals("true", parent.get("mpuAbort"), "mpuAbort should be set when retries exhausted");
		assertEquals(CoopStorageDriverBase.MAX_PART_RETRIES, part.retryCount(),
						"retryCount should not be incremented further");
	}

	@Test
	void handleCompleted_successfulPartDoesNotRetryOrAbort() throws Exception {
		final var driver = newRetryTestDriver();
		final var baseItem = new DataItemImpl("obj3", 0, 2048);
		baseItem.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		final var parent = new CompositeDataOperationImpl<DataItem>(
						0, OpType.CREATE, baseItem, null, "/bucket", null, null, 0, 1024);
		parent.subOperations(); // pendingSubTasksCount = 2

		final var part = (PartialDataOperationImpl<DataItem>) parent.nextSubOperations(1).get(0);
		part.status(Operation.Status.SUCC);
		parent.markSubTaskCompleted(); // simulate finishResponse; pending = 1

		boolean result = driver.handleCompleted((Operation<Item>) (Operation<?>) part);

		assertTrue(result);
		assertEquals(0, part.retryCount(), "retryCount should remain 0 for successful part");
		assertNull(parent.get("mpuAbort"), "mpuAbort should not be set for successful part");
		// Part should NOT be re-enqueued
		final var queue = childQueueOf(driver);
		assertFalse(queue.contains(part), "successful part should not be re-enqueued");
	}

	@Test
	void handleCompleted_reEnqueuesParentWhenAllPartsDone() throws Exception {
		final var driver = newRetryTestDriver();
		final var baseItem = new DataItemImpl("obj4", 0, 1024);
		baseItem.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		// Single part (1024-byte item, 1024-byte threshold → 1 part)
		final var parent = new CompositeDataOperationImpl<DataItem>(
						0, OpType.CREATE, baseItem, null, "/bucket", null, null, 0, 1024);
		parent.subOperations(); // pendingSubTasksCount = 1

		final var part = (PartialDataOperationImpl<DataItem>) parent.nextSubOperations(1).get(0);
		part.status(Operation.Status.SUCC);
		parent.markSubTaskCompleted(); // pending = 0, allSubOperationsDone() → true

		driver.handleCompleted((Operation<Item>) (Operation<?>) part);

		final var queue = childQueueOf(driver);
		assertTrue(queue.contains(parent), "parent should be re-enqueued when all parts done");
	}

	// ---------- Output-full side-effect tests ----------

	@Test
	void handleCompleted_outputFullSkipsCompletedCountIncrement() throws Exception {
		// When opResultOut.put() returns false (queue full), the base class
		// handleCompleted returns false, so CoopStorageDriverBase should NOT
		// increment completedOpCount.
		final var driver = newRetryTestDriver();

		// Override opResultOut to reject
		final Output<Operation<Item>> fullOutput = mock(Output.class);
		when(fullOutput.put(any(Operation.class))).thenReturn(false);
		final var outField = StorageDriverBase.class.getDeclaredField("opResultOut");
		outField.setAccessible(true);
		outField.set(driver, fullOutput);

		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		assertEquals(0, driver.completedOpCount(), "precondition: count starts at 0");

		driver.handleCompleted(op);

		assertEquals(0, driver.completedOpCount(),
						"completedOpCount must NOT be incremented when output rejects the result");
	}

	// ---------- Concurrent signalDispatch tests ----------

	@Test
	void signalDispatch_concurrentCompletionsDoNotLoseSignals() throws Exception {
		// Multiple threads calling handleCompleted simultaneously. The dispatch
		// task's Condition should receive at least one signal for every batch of
		// completions. We verify by counting signals received.
		final var driver = mock(CoopStorageDriverBase.class, withSettings().defaultAnswer(CALLS_REAL_METHODS));

		// childOpQueue
		final var childQueueField = CoopStorageDriverBase.class.getDeclaredField("childOpQueue");
		childQueueField.setAccessible(true);
		childQueueField.set(driver, new ArrayBlockingQueue<>(100));

		// completedOpCount
		final var completedField = CoopStorageDriverBase.class.getDeclaredField("completedOpCount");
		completedField.setAccessible(true);
		completedField.set(driver, new LongAdder());

		// Use a real lock/condition so we can observe signals
		final var lock = new ReentrantLock();
		final var condition = lock.newCondition();
		final var lockField = CoopStorageDriverBase.class.getDeclaredField("dispatchLock");
		lockField.setAccessible(true);
		lockField.set(driver, lock);
		final var condField = CoopStorageDriverBase.class.getDeclaredField("dispatchReady");
		condField.setAccessible(true);
		condField.set(driver, condition);

		// opResultOut accepts everything
		final Output<Operation<Item>> output = mock(Output.class);
		when(output.put(any(Operation.class))).thenReturn(true);
		final var outField = StorageDriverBase.class.getDeclaredField("opResultOut");
		outField.setAccessible(true);
		outField.set(driver, output);

		// Signal counter: a separate thread waits on the condition and counts signals
		final AtomicInteger signalCount = new AtomicInteger(0);
		final int completionCount = 32;
		final var allCompleted = new CountDownLatch(completionCount);
		final var listenerReady = new CountDownLatch(1);
		final var listenerDone = new CountDownLatch(1);

		// Listener thread: simulates the dispatch task waiting for signals
		Thread.ofVirtual().start(() -> {
			listenerReady.countDown();
			try {
				// Keep listening until all completions are done
				while (!allCompleted.await(0, TimeUnit.MILLISECONDS)) {
					lock.lock();
					try {
						if (condition.await(50, TimeUnit.MILLISECONDS)) {
							signalCount.incrementAndGet();
						}
					} finally {
						lock.unlock();
					}
				}
				// Drain any remaining signals
				lock.lock();
				try {
					while (condition.await(10, TimeUnit.MILLISECONDS)) {
						signalCount.incrementAndGet();
					}
				} finally {
					lock.unlock();
				}
			} catch (final InterruptedException ignored) {}
			listenerDone.countDown();
		});

		assertTrue(listenerReady.await(5, TimeUnit.SECONDS), "listener should be ready");

		// Fire completions from multiple threads simultaneously
		final var startLatch = new CountDownLatch(1);
		for (int i = 0; i < completionCount; i++) {
			Thread.ofVirtual().start(() -> {
				try {
					startLatch.await(5, TimeUnit.SECONDS);
					final Operation<Item> op = mock(Operation.class);
					when(op.status()).thenReturn(Operation.Status.SUCC);
					when(op.result()).thenReturn(mock(Operation.class));
					driver.handleCompleted(op);
				} catch (final Exception ignored) {} finally {
					allCompleted.countDown();
				}
			});
		}

		startLatch.countDown(); // release all completion threads
		assertTrue(allCompleted.await(10, TimeUnit.SECONDS), "all completions should finish");
		assertTrue(listenerDone.await(5, TimeUnit.SECONDS), "listener should finish");

		// We expect at least 1 signal (signals can coalesce), and all ops completed
		assertTrue(signalCount.get() >= 1,
						"at least one signal must be received by the dispatch listener");
		assertEquals(completionCount, driver.completedOpCount(),
						"all ops should be counted as completed");
	}

	@Test
	void handleCompleted_returnsFalseWhenDriverStopped() throws Exception {
		// When the driver is stopped, handleCompleted should return false
		// (base class checks isStopped()).
		final var driver = newRetryTestDriver();
		when(driver.isStopped()).thenReturn(true);

		final Operation<Item> op = mock(Operation.class);
		when(op.status()).thenReturn(Operation.Status.SUCC);
		when(op.result()).thenReturn(mock(Operation.class));

		boolean result = driver.handleCompleted(op);

		assertFalse(result, "handleCompleted should return false when driver is stopped");
		assertEquals(0, driver.completedOpCount(),
						"completedOpCount should not be incremented when driver is stopped");
	}

	@Test
	void handleCompleted_childOpQueueOverflowReleasesMpuObjectPermit() throws Exception {
		final var driver = newRetryTestDriver();
		System.setProperty(CoopStorageDriverBase.CHILD_OP_ENQUEUE_TIMEOUT_MILLIS_PROPERTY, "1");

		try {
			// Set up an MPU object throttle with 1 permit, and acquire it so 0 are available
			final Semaphore mpuThrottle = new Semaphore(1, true);
			mpuThrottle.acquire();
			final var mpuField = CoopStorageDriverBase.class.getDeclaredField("mpuObjectThrottle");
			mpuField.setAccessible(true);
			mpuField.set(driver, mpuThrottle);

			final var maxPartsField = CoopStorageDriverBase.class.getDeclaredField("mpuMaxParts");
			maxPartsField.setAccessible(true);
			maxPartsField.set(driver, 0);

			// Fill the childOpQueue to force bounded enqueue timeout
			final var queue = childQueueOf(driver);
			while (queue.remainingCapacity() > 0) {
				queue.add(mock(Operation.class));
			}

			// Create a parent MPU operation
			final var baseItem = new com.dell.spt.base.item.DataItemImpl("obj1", 0, 2048);
			baseItem.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false, 0.0, true));
			final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
							0, com.dell.spt.base.item.op.OpType.CREATE, baseItem, null, "/bucket", null, null, 0, 1024);
			parent.subOperations(); // pendingSubTasksCount = 2

			// Act: complete the first part
			final var part = (com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>) parent.nextSubOperations(1).get(0);
			part.status(Operation.Status.SUCC);
			parent.markSubTaskCompleted(); // pending = 1

			boolean result = driver.handleCompleted((Operation<Item>) (Operation<?>) part);

			assertTrue(result, "handleCompleted should accept the completion without blocking on a full child queue");
			awaitCondition(
							() -> mpuThrottle.availablePermits() == 1,
							"MPU object permit should be safely released after asynchronous enqueue timeout",
							2000);
			assertEquals(1, mpuThrottle.availablePermits(), "MPU object permit should be safely released on timeout");
			assertEquals("true", parent.get("permitReleased"));
		} finally {
			System.clearProperty(CoopStorageDriverBase.CHILD_OP_ENQUEUE_TIMEOUT_MILLIS_PROPERTY);
		}
	}

	@Test
	void handleCompleted_parentEnqueueOverflowReleasesMpuObjectPermit() throws Exception {
		final var driver = newRetryTestDriver();
		System.setProperty(CoopStorageDriverBase.CHILD_OP_ENQUEUE_TIMEOUT_MILLIS_PROPERTY, "1");

		try {
			final Semaphore mpuThrottle = new Semaphore(1, true);
			mpuThrottle.acquire();
			final var mpuField = CoopStorageDriverBase.class.getDeclaredField("mpuObjectThrottle");
			mpuField.setAccessible(true);
			mpuField.set(driver, mpuThrottle);

			final var maxPartsField = CoopStorageDriverBase.class.getDeclaredField("mpuMaxParts");
			maxPartsField.setAccessible(true);
			maxPartsField.set(driver, 0);

			// Single part MPU
			final var baseItem = new com.dell.spt.base.item.DataItemImpl("obj2", 0, 1024);
			baseItem.dataInput(com.dell.spt.base.data.DataInput.instance(null, "7a42d9c483244167", new com.github.akurilov.commons.system.SizeInBytes("64KB"), 4, false, 0.0, true));
			final var parent = new com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl<com.dell.spt.base.item.DataItem>(
							0, com.dell.spt.base.item.op.OpType.CREATE, baseItem, null, "/bucket", null, null, 0, 1024);
			parent.subOperations(); // pendingSubTasksCount = 1

			final var part = (com.dell.spt.base.item.op.partial.data.PartialDataOperationImpl<com.dell.spt.base.item.DataItem>) parent.nextSubOperations(1).get(0);
			part.status(Operation.Status.SUCC);
			parent.markSubTaskCompleted(); // pending = 0, all done

			// Fill the childOpQueue so the attempt to re-enqueue the parent times out
			final var queue = childQueueOf(driver);
			while (queue.remainingCapacity() > 0) {
				queue.add(mock(Operation.class));
			}

			boolean result = driver.handleCompleted((Operation<Item>) (Operation<?>) part);

			assertTrue(result, "handleCompleted should accept the completion without blocking on a full child queue");
			awaitCondition(
							() -> mpuThrottle.availablePermits() == 1,
							"MPU object permit should be safely released after asynchronous parent enqueue timeout",
							2000);
			assertEquals(1, mpuThrottle.availablePermits(), "MPU object permit should be safely released on parent enqueue timeout");
		} finally {
			System.clearProperty(CoopStorageDriverBase.CHILD_OP_ENQUEUE_TIMEOUT_MILLIS_PROPERTY);
		}
	}

	@Test
	void handleCompleted_enqueuesCompositeFinalizationOnlyOnceWhenPartsFinishConcurrently() throws Exception {
		final var driver = newRetryTestDriver();
		final var queue = childQueueOf(driver);
		final var parent = newCompositeParent("concurrent-finalize", 2048, 1024);
		parent.subOperations(); // pendingSubTasksCount = 2

		final var parts = parent.nextSubOperations(0);
		assertEquals(2, parts.size(), "test setup should yield both part operations");
		for (final var rawPart : parts) {
			final var part = (PartialDataOperationImpl<DataItem>) rawPart;
			part.status(Operation.Status.SUCC);
			parent.markSubTaskCompleted();
		}
		assertTrue(parent.allSubOperationsDone(), "test setup should simulate both parts finishing before handling");

		assertTrue(driver.handleCompleted((Operation<Item>) (Operation<?>) parts.get(0)));
		assertTrue(driver.handleCompleted((Operation<Item>) (Operation<?>) parts.get(1)));

		assertEquals(1, queue.size(), "only one parent finalization should be enqueued");
		assertSame(parent, queue.peek(), "queued operation should be the composite parent");
	}

	@Test
	void handleCompleted_doesNotFinalizeWhileFailedPartWillBeRetried() throws Exception {
		final var driver = newRetryTestDriver();
		final var queue = childQueueOf(driver);
		final var parent = newCompositeParent("retry-race", 2048, 1024);
		parent.subOperations(); // pendingSubTasksCount = 2

		final var parts = parent.nextSubOperations(0);
		assertEquals(2, parts.size(), "test setup should yield both part operations");
		final var successfulPart = (PartialDataOperationImpl<DataItem>) parts.get(0);
		final var failedPart = (PartialDataOperationImpl<DataItem>) parts.get(1);
		successfulPart.status(Operation.Status.SUCC);
		failedPart.status(Operation.Status.FAIL_IO);
		parent.markSubTaskCompleted();
		parent.markSubTaskCompleted();
		assertTrue(parent.allSubOperationsDone(), "test setup should expose the completion race window");

		assertTrue(driver.handleCompleted((Operation<Item>) (Operation<?>) successfulPart));

		assertFalse(queue.contains(parent),
						"parent finalization must not be enqueued while another failed part can still be retried");

		assertTrue(driver.handleCompleted((Operation<Item>) (Operation<?>) failedPart));

		assertEquals(1, failedPart.retryCount(), "failed part should be scheduled for retry");
		assertEquals(Operation.Status.PENDING, failedPart.status(), "failed part should be reset before retry");
		assertFalse(parent.allSubOperationsDone(), "retry should restore the pending subtask count");
		assertTrue(queue.contains(failedPart), "failed part should be re-enqueued for retry");
		assertFalse(queue.contains(parent), "parent finalization should remain blocked until the retry succeeds");
	}

	@Test
	void handleCompleted_doesNotBlockWhenChildQueueIsFull() throws Exception {
		final var driver = newRetryTestDriver(1);
		System.setProperty(CoopStorageDriverBase.CHILD_OP_ENQUEUE_TIMEOUT_MILLIS_PROPERTY, "1000");
		try {
			final var queue = childQueueOf(driver);
			final Operation<Item> filler = mock(Operation.class);
			queue.add(filler);

			final var parent = newCompositeParent("backpressure", 2048, 1024);
			parent.subOperations(); // pendingSubTasksCount = 2
			final var part = (PartialDataOperationImpl<DataItem>) parent.nextSubOperations(1).get(0);
			part.status(Operation.Status.SUCC);
			parent.markSubTaskCompleted(); // pending = 1, so handleCompleted should enqueue the next part

			final long startNanos = System.nanoTime();
			final boolean result = driver.handleCompleted((Operation<Item>) (Operation<?>) part);
			final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

			assertTrue(result, "completion handling should accept asynchronous child enqueue");
			assertTrue(elapsedMillis < 500, "handleCompleted should not wait while the child queue is full");
			assertSame(filler, queue.take(), "test should free exactly one queue slot");
			awaitCondition(
							() -> queue.size() == 1 && queue.peek() != filler,
							"asynchronous enqueue should publish the next MPU child after queue capacity appears",
							5000);
			assertEquals(1, queue.size(), "next MPU child operation should be enqueued");
			assertNotSame(filler, queue.peek(), "queued operation should be the next MPU child, not the filler");
		} finally {
			System.clearProperty(CoopStorageDriverBase.CHILD_OP_ENQUEUE_TIMEOUT_MILLIS_PROPERTY);
		}
	}

	@Test
	void releaseMpuObjectPermitSignalsDispatchWaiter() throws Exception {
		final var driver = newRetryTestDriver();

		final Semaphore mpuThrottle = new Semaphore(1, true);
		mpuThrottle.acquire(); // force available permits to 0 before release
		final var mpuField = CoopStorageDriverBase.class.getDeclaredField("mpuObjectThrottle");
		mpuField.setAccessible(true);
		mpuField.set(driver, mpuThrottle);

		final var lockField = CoopStorageDriverBase.class.getDeclaredField("dispatchLock");
		lockField.setAccessible(true);
		final ReentrantLock lock = (ReentrantLock) lockField.get(driver);

		final var condField = CoopStorageDriverBase.class.getDeclaredField("dispatchReady");
		condField.setAccessible(true);
		final var condition = (java.util.concurrent.locks.Condition) condField.get(driver);

		final var waiterReady = new CountDownLatch(1);
		final var waiterDone = new CountDownLatch(1);
		final boolean[] awakened = {false
		};

		Thread.ofVirtual().start(() -> {
			lock.lock();
			try {
				waiterReady.countDown();
				awakened[0] = condition.await(2, TimeUnit.SECONDS);
			} catch (final InterruptedException ignored) {} finally {
				lock.unlock();
				waiterDone.countDown();
			}
		});

		assertTrue(waiterReady.await(5, TimeUnit.SECONDS), "waiter should be parked on dispatch condition");

		driver.releaseMpuObjectPermit();

		assertTrue(waiterDone.await(5, TimeUnit.SECONDS), "waiter should complete after permit release signal");
		assertTrue(awakened[0], "permit release should signal the dispatch condition");
		assertEquals(1, mpuThrottle.availablePermits(), "permit release should return one permit");
	}

	private static final class ValueEqualLegacyOperation implements InvocationHandler {
		private final int equalityGroup;
		private final int hashCodeValue;
		private final DataOperationImpl<DataItem> delegate;
		private final Operation<DataItem> operation;

		@SuppressWarnings("unchecked")
		private ValueEqualLegacyOperation(
						final String name, final int hashCodeValue, final int equalityGroup) {
			this.hashCodeValue = hashCodeValue;
			this.equalityGroup = equalityGroup;
			this.delegate = new DataOperationImpl<>(
							0, OpType.DELETE, new DataItemImpl(name, 0, 1), null, "/bucket", null,
							List.of(), 0);
			this.operation = (Operation<DataItem>) Proxy.newProxyInstance(
							Operation.class.getClassLoader(), new Class<?>[]{Operation.class
							}, this);
		}

		private Operation<DataItem> operation() {
			return operation;
		}

		@Override
		public Object invoke(final Object proxy, final Method method, final Object[] args)
						throws Throwable {
			return switch (method.getName()) {
			case "lifecycle", "startNextLifecycle" -> OperationLifecycle.untracked();
			case "hashCode" -> hashCodeValue;
			case "equals" -> args != null
							&& args.length == 1
							&& args[0] != null
							&& Proxy.isProxyClass(args[0].getClass())
							&& Proxy.getInvocationHandler(args[0]) instanceof ValueEqualLegacyOperation other
							&& equalityGroup == other.equalityGroup;
			case "toString" -> "legacy-operation-" + equalityGroup;
			default -> method.invoke(delegate, args);
			};
		}
	}
}
