package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.AsyncRunnableBase;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.io.RemainingItemCountInput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.deletion.DeleteFailureClassification;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOutcome;
import com.dell.spt.base.item.op.deletion.DeleteTransportResult;
import com.dell.spt.base.item.op.deletion.DeleteTransportTargetResult;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.generator.LoadGeneratorBuilderImpl;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.commons.system.SizeInBytes;
import java.io.EOFException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class StandaloneDeleteEngineStepTest {
	@Test
	void batchSizeOneExecutesOneRequestAndOnePermitPerSelectedIdentity() throws Exception {
		final var config = config(1, 10);
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		final var generator = generator(config, driver, new ManifestInput(2));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-size-one", generator, driver, metrics, config.configVal("load"), false);

		try {
			step.start();
			awaitDone(step);
			metrics.refreshLastSnapshot();

			assertEquals(2, driver.completed.size());
			assertTrue(driver.completed.stream()
							.allMatch(operation -> operation.deleteRequest().targets().size() == 1));
			assertEquals(2, step.operationLifecycle().dispatched());
			assertEquals(2, step.operationLifecycle().terminal());
			assertEquals(2, step.deleteObjectLifecycle().selected());
			assertEquals(2, step.deleteObjectLifecycle().accepted());
			assertTrue(step.deleteObjectLifecycle().reconciled());
			assertEquals(2, metrics.lastSnapshot().successSnapshot().count());
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void realStepExecutesBatchesAndPublishesRequestAndObjectOutcomes() throws Exception {
		final var config = config(2, 10);
		final var driver = new DeterministicDeleteDriver(DriverMode.DEFAULT);
		final var generator = generator(config, driver, new ManifestInput(4));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-canary", generator, driver, metrics, config.configVal("load"), false);

		try {
			step.start();
			awaitDone(step);
			metrics.refreshLastSnapshot();

			assertEquals(2, driver.completed.size());
			assertEquals(
							List.of(List.of("key-0", "key-1"), List.of("key-2", "key-3")),
							driver.completed.stream()
											.map(op -> op.deleteRequest().targets().stream().map(target -> target.key()).toList())
											.toList());
			assertEquals(
							List.of(DeleteRequestOutcome.FULL_SUCCESS, DeleteRequestOutcome.PARTIAL),
							driver.completed.stream().map(op -> op.deleteResult().outcome()).toList());
			assertEquals(
							List.of(DeleteFailureClassification.NONE, DeleteFailureClassification.OPERATIONAL),
							driver.completed.stream().map(op -> op.deleteResult().failureClassification()).toList());

			final var objects = step.deleteObjectLifecycle();
			assertEquals(4, objects.selected());
			assertEquals(4, objects.attempted());
			assertEquals(3, objects.accepted());
			assertEquals(1, objects.failed());
			assertEquals(0, objects.unattempted());
			assertEquals(0, objects.unresolved());
			assertEquals(0, objects.protocolFailed());
			assertTrue(objects.reconciled());
			assertEquals(2, step.operationLifecycle().dispatched());
			assertEquals(2, step.operationLifecycle().terminal());
			assertEquals(0, metrics.lastSnapshot().byteSnapshot().count());
			assertEquals(1, metrics.lastSnapshot().successSnapshot().count());
			assertEquals(1, metrics.lastSnapshot().failsSnapshot().count());
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void dispatchedCancellationWithoutCompletionFailsClosedAsUnresolvedAtDrainBound() throws Exception {
		final var config = config(3, 0);
		final var driver = new DeterministicDeleteDriver(DriverMode.HOLD);
		final var generator = generator(config, driver, new ManifestInput(3));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-unresolved", generator, driver, metrics, config.configVal("load"), false);

		try {
			step.start();
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (driver.scheduled.get() == 0 && System.nanoTime() < deadline) {
				Thread.onSpinWait();
			}
			assertEquals(1, driver.scheduled.get());
			assertEquals(1, step.operationLifecycle().dispatched());
			step.stop();

			final var objects = step.deleteObjectLifecycle();
			assertEquals(3, objects.selected());
			assertEquals(3, objects.attempted());
			assertEquals(0, objects.accepted());
			assertEquals(0, objects.failed());
			assertEquals(0, objects.unattempted());
			assertEquals(3, objects.unresolved());
			assertTrue(objects.reconciled());
			assertEquals(1, step.operationLifecycle().unresolved());
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void cancellationAccountsBillionIdentityUnreadSuffixWithoutReadingOrRetainingIt()
					throws Exception {
		final var config = config(2, 0);
		final var driver = new DeterministicDeleteDriver(DriverMode.HOLD);
		final var input = new InterruptGatedManifestInput(1_000_000_000, 4);
		final var generator = generator(config, driver, input);
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-unread", generator, driver, metrics, config.configVal("load"), false);

		try {
			step.start();
			assertTrue(input.awaitSecondRead(), "generator should block before consuming the unread suffix");
			assertEquals(2, driver.scheduled.get());
			step.stop();

			final var objects = step.deleteObjectLifecycle();
			assertEquals(1_000_000_000, objects.selected());
			assertEquals(4, objects.attempted());
			assertEquals(0, objects.accepted());
			assertEquals(0, objects.failed());
			assertEquals(999_999_996, objects.unattempted());
			assertEquals(4, objects.unresolved());
			assertTrue(objects.reconciled());
			assertEquals(0, step.operationLifecycle().unattempted());
			assertEquals(2, step.operationLifecycle().unresolved());
			assertEquals(4, generator.consumedItemCount());
			assertEquals(999_999_996, generator.aggregateUnattemptedItemCount());
			assertEquals(0, input.recoveryReadAttempts.get());
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void drainWinsBlockedTerminalPublicationWithoutRecordingRequestOrObjectOutcome()
					throws Exception {
		final var config = config(2, 0);
		final var driver = new DeterministicDeleteDriver(DriverMode.BLOCK_TERMINAL_PUBLICATION);
		final var generator = generator(config, driver, new ManifestInput(2));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-publication-race",
						generator,
						driver,
						metrics,
						config.configVal("load"),
						false);

		try {
			step.start();
			assertTrue(driver.awaitOutputAcceptance(), "result output should accept before terminal commit");
			step.stop();
			driver.releaseTerminalPublication();
			assertTrue(driver.awaitCompletionThread(), "late terminal attempt should finish");
			metrics.refreshLastSnapshot();

			final var objects = step.deleteObjectLifecycle();
			assertEquals(2, objects.selected());
			assertEquals(2, objects.attempted());
			assertEquals(0, objects.accepted());
			assertEquals(0, objects.failed());
			assertEquals(0, objects.unattempted());
			assertEquals(2, objects.unresolved());
			assertTrue(objects.reconciled());
			assertEquals(0, step.operationLifecycle().terminal());
			assertEquals(1, step.operationLifecycle().unresolved());
			assertEquals(0, metrics.lastSnapshot().successSnapshot().count());
			assertEquals(0, metrics.lastSnapshot().failsSnapshot().count());
		} finally {
			driver.releaseTerminalPublication();
			step.close();
			metrics.close();
		}
	}

	@Test
	void transportAndProtocolFailuresReconcileEverySelectedObject() throws Exception {
		assertFailedObjectReconciliation(DriverMode.TRANSPORT_FAILURE, 0);
		assertFailedObjectReconciliation(DriverMode.PROTOCOL_DEFECT, 3);
	}

	@Test
	@SuppressWarnings("unchecked")
	void queuedCancellationAccountsEveryRetainedTargetAsUnattempted() {
		final var config = config(3, 0);
		final var driver = new DeterministicDeleteDriver(DriverMode.HOLD);
		final LoadGenerator<IntegrityManifestDataItem, DeleteRequestOperation> generator = mock(LoadGenerator.class);
		when(generator.consumedItemCount()).thenReturn(3L);
		final var targets = List.of(
						new com.dell.spt.base.item.op.deletion.DeleteTarget(
										new IntegrityManifestDataItem("bucket", "queued-0", 1, null)),
						new com.dell.spt.base.item.op.deletion.DeleteTarget(
										new IntegrityManifestDataItem("bucket", "queued-1", 1, null)),
						new com.dell.spt.base.item.op.deletion.DeleteTarget(
										new IntegrityManifestDataItem("bucket", "queued-2", 1, null)));
		final var operation = new com.dell.spt.base.item.op.deletion.DeleteRequestOperationImpl(
						0,
						new com.dell.spt.base.item.op.deletion.DeleteRequest(
										"bucket", com.dell.spt.base.storage.Credential.NONE, targets));
		driver.lifecycle.generatorBuffered(operation);
		driver.lifecycle.unattempted(operation);
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-cancelled",
						generator,
						driver,
						mock(MetricsContext.class),
						config.configVal("load"),
						false);

		final var objects = step.deleteObjectLifecycle();
		assertEquals(3, objects.selected());
		assertEquals(0, objects.attempted());
		assertEquals(0, objects.accepted());
		assertEquals(0, objects.failed());
		assertEquals(3, objects.unattempted());
		assertEquals(0, objects.unresolved());
		assertTrue(objects.reconciled());
	}

	private static void assertFailedObjectReconciliation(
					final DriverMode mode, final long expectedProtocolFailures) throws Exception {
		final var config = config(3, 10);
		final var driver = new DeterministicDeleteDriver(mode);
		final var generator = generator(config, driver, new ManifestInput(3));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-" + mode.name().toLowerCase(Locale.ROOT),
						generator,
						driver,
						metrics,
						config.configVal("load"),
						false);
		try {
			step.start();
			awaitDone(step);
			metrics.refreshLastSnapshot();

			final var objects = step.deleteObjectLifecycle();
			assertEquals(3, objects.selected());
			assertEquals(3, objects.attempted());
			assertEquals(0, objects.accepted());
			assertEquals(3, objects.failed());
			assertEquals(expectedProtocolFailures, objects.protocolFailed());
			assertTrue(objects.reconciled());
			assertEquals(1, metrics.lastSnapshot().failsSnapshot().count());
		} finally {
			step.close();
			metrics.close();
		}
	}

	private static Config config(final int deleteBatchSize, final int waitLimit) {
		final var config = TestConfigBuilder.config();
		config.val("item-type", "data");
		config.val("item-output-file", "");
		config.val("load-batch-size", 4);
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-batchSize", deleteBatchSize);
		config.val("load-op-recycle-mode", false);
		config.val("load-op-retry", false);
		config.val("load-op-limit-count", 0L);
		config.val("load-op-wait-finish", true);
		config.val("load-op-wait-limit", waitLimit);
		return config;
	}

	@SuppressWarnings({"rawtypes", "unchecked"
	})
	private static LoadGenerator<IntegrityManifestDataItem, DeleteRequestOperation> generator(
					final Config config,
					final DeterministicDeleteDriver driver,
					final Input<IntegrityManifestDataItem> input) {
		return new LoadGeneratorBuilderImpl()
						.authConfig(config.configVal("storage-auth"))
						.itemConfig(config.configVal("item"))
						.itemFactory((ItemFactory) new DataItemFactoryImpl<>())
						.itemType(ItemType.DATA)
						.loadConfig(config.configVal("load"))
						.itemInput(input)
						.loadOperationsOutput(driver)
						.originIndex(0)
						.build();
	}

	private static MetricsContext<AllMetricsSnapshot> metrics() {
		final MetricsContext<AllMetricsSnapshot> metrics = MetricsContextImpl.builder()
						.loadStepId("standalone-delete-canary")
						.opType(OpType.DELETE)
						.actualConcurrencyGauge(() -> 0)
						.concurrencyLimit(4)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(0))
						.outputPeriodSec(1)
						.stdOutColorFlag(false)
						.runId(0)
						.build();
		metrics.start();
		return metrics;
	}

	private static void awaitDone(final LoadStepContextImpl<?, ?> step) throws Exception {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!step.isDone() && System.nanoTime() < deadline) {
			Thread.sleep(1);
		}
		assertTrue(step.isDone(), "standalone DELETE step should complete");
	}

	private static final class ManifestInput implements RemainingItemCountInput<IntegrityManifestDataItem> {
		private final int count;
		private int next;

		private ManifestInput(final int count) {
			this.count = count;
		}

		@Override
		public IntegrityManifestDataItem get() {
			return next < count
							? new IntegrityManifestDataItem("bucket", "key-" + next, next++, null)
							: null;
		}

		@Override
		public int get(final List<IntegrityManifestDataItem> buffer, final int limit) {
			if (next >= count) {
				com.github.akurilov.commons.lang.Exceptions.throwUnchecked(new EOFException());
			}
			final int start = next;
			while (next < count && next - start < limit) {
				buffer.add(new IntegrityManifestDataItem("bucket", "key-" + next, next, null));
				next++;
			}
			return next - start;
		}

		@Override
		public long skip(final long count) {
			return 0;
		}

		@Override
		public long remainingItemCount() {
			return (long) count - next;
		}

		@Override
		public void reset() {
			next = 0;
		}

		@Override
		public void close() {}
	}

	private static final class InterruptGatedManifestInput
					implements RemainingItemCountInput<IntegrityManifestDataItem> {
		private final int count;
		private final int initialReadCount;
		private final CountDownLatch secondReadStarted = new CountDownLatch(1);
		private final AtomicInteger recoveryReadAttempts = new AtomicInteger();
		private volatile boolean recovering;
		private int next;

		private InterruptGatedManifestInput(final int count, final int initialReadCount) {
			this.count = count;
			this.initialReadCount = initialReadCount;
		}

		private boolean awaitSecondRead() throws InterruptedException {
			return secondReadStarted.await(5, TimeUnit.SECONDS);
		}

		@Override
		public IntegrityManifestDataItem get() {
			throw new AssertionError();
		}

		@Override
		public int get(final List<IntegrityManifestDataItem> buffer, final int limit) {
			if (next >= count) {
				com.github.akurilov.commons.lang.Exceptions.throwUnchecked(new EOFException());
			}
			if (next >= initialReadCount && !recovering) {
				secondReadStarted.countDown();
				try {
					new CountDownLatch(1).await();
				} catch (final InterruptedException e) {
					recovering = true;
					com.github.akurilov.commons.lang.Exceptions.throwUnchecked(e);
				}
			}
			if (recovering) {
				recoveryReadAttempts.incrementAndGet();
				throw new AssertionError("cancellation recovery must not read the unread manifest suffix");
			}
			final var start = next;
			while (next < count && next - start < limit) {
				buffer.add(new IntegrityManifestDataItem("bucket", "key-" + next, next, null));
				next++;
			}
			return next - start;
		}

		@Override
		public long skip(final long count) {
			return 0;
		}

		@Override
		public long remainingItemCount() {
			return (long) count - next;
		}

		@Override
		public void reset() {
			next = 0;
			recovering = false;
		}

		@Override
		public void close() {}
	}

	private enum DriverMode {
		DEFAULT, HOLD, TRANSPORT_FAILURE, PROTOCOL_DEFECT, BLOCK_TERMINAL_PUBLICATION
	}

	private static final class DeterministicDeleteDriver extends AsyncRunnableBase
					implements StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> {
		private final OperationLifecycleTracker<DeleteRequestOperation> lifecycle = new OperationLifecycleTracker<>();
		private final DriverMode mode;
		private final AtomicInteger scheduled = new AtomicInteger();
		private final List<DeleteRequestOperation> completed = new ArrayList<>();
		private final CountDownLatch outputAccepted = new CountDownLatch(1);
		private final CountDownLatch terminalPublicationRelease = new CountDownLatch(1);
		private volatile Output<DeleteRequestOperation> resultOutput;
		private volatile Thread completionThread;

		private DeterministicDeleteDriver(final DriverMode mode) {
			this.mode = mode;
		}

		@Override
		public boolean put(final DeleteRequestOperation operation) {
			lifecycle.driverQueued(operation);
			lifecycle.explicitlyDispatched(operation);
			final int requestIndex = scheduled.getAndIncrement();
			if (mode == DriverMode.HOLD) {
				return true;
			}
			if (mode == DriverMode.BLOCK_TERMINAL_PUBLICATION) {
				completionThread = Thread.ofVirtual().start(() -> complete(operation, requestIndex));
				return true;
			}
			return complete(operation, requestIndex);
		}

		private boolean complete(final DeleteRequestOperation operation, final int requestIndex) {
			operation.reset();
			operation.startRequest();
			operation.finishRequest();
			operation.startResponse();
			operation.finishResponse();
			if (mode == DriverMode.TRANSPORT_FAILURE) {
				operation.completeDelete(DeleteTransportResult.failure(
								com.dell.spt.base.item.op.Operation.Status.FAIL_TIMEOUT,
								"injected timeout"));
			} else if (mode == DriverMode.PROTOCOL_DEFECT) {
				operation.completeDelete(new DeleteTransportResult(
								List.of(DeleteTransportTargetResult.succeeded(
												operation.deleteRequest().targets().get(0))),
								null,
								null));
			} else if (requestIndex == 0 || operation.deleteRequest().targets().size() == 1) {
				operation.completeDelete(DeleteTransportResult.success(operation.deleteRequest().targets()));
			} else {
				final var targets = operation.deleteRequest().targets();
				operation.completeDelete(new DeleteTransportResult(
								List.of(
												DeleteTransportTargetResult.failed(targets.get(1), "injected failure"),
												DeleteTransportTargetResult.succeeded(targets.get(0))),
								null,
								null));
			}
			lifecycle.completionStarted(operation);
			final var result = operation.result();
			completed.add(result);
			final boolean accepted = resultOutput.put(result);
			if (mode == DriverMode.BLOCK_TERMINAL_PUBLICATION) {
				outputAccepted.countDown();
				awaitUninterruptibly(terminalPublicationRelease);
			}
			if (accepted) {
				lifecycle.terminal(operation);
			}
			return accepted;
		}

		private boolean awaitOutputAcceptance() throws InterruptedException {
			return outputAccepted.await(5, TimeUnit.SECONDS);
		}

		private void releaseTerminalPublication() {
			terminalPublicationRelease.countDown();
		}

		private boolean awaitCompletionThread() throws InterruptedException {
			final var thread = completionThread;
			if (thread == null) {
				return true;
			}
			thread.join(TimeUnit.SECONDS.toMillis(5));
			return !thread.isAlive();
		}

		private static void awaitUninterruptibly(final CountDownLatch latch) {
			var interrupted = false;
			while (true) {
				try {
					latch.await();
					break;
				} catch (final InterruptedException e) {
					interrupted = true;
				}
			}
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}

		@Override
		public int put(final List<DeleteRequestOperation> operations, final int from, final int to) {
			var i = from;
			while (i < to && put(operations.get(i))) {
				i++;
			}
			return i - from;
		}

		@Override
		public int put(final List<DeleteRequestOperation> operations) {
			return put(operations, 0, operations.size());
		}

		@Override
		public void operationResultOutput(final Output<DeleteRequestOperation> output) {
			resultOutput = output;
		}

		@Override
		public List<IntegrityManifestDataItem> list(
						final ItemFactory<IntegrityManifestDataItem> itemFactory,
						final String path,
						final String prefix,
						final int idRadix,
						final IntegrityManifestDataItem lastPrevItem,
						final int count) {
			return List.of();
		}

		@Override
		public Input<DeleteRequestOperation> getInput() {
			throw new AssertionError();
		}

		@Override
		public int concurrencyLimit() {
			return 4;
		}

		@Override
		public boolean supportsStandaloneDeleteRequests() {
			return true;
		}

		@Override
		public int activeOpCount() {
			return (int) lifecycle.inFlightCount();
		}

		@Override
		public long scheduledOpCount() {
			return scheduled.get();
		}

		@Override
		public long completedOpCount() {
			return completed.size();
		}

		@Override
		public boolean isIdle() {
			return lifecycle.inFlightCount() == 0;
		}

		@Override
		public OperationLifecycleTracker<DeleteRequestOperation> operationLifecycle() {
			return lifecycle;
		}

		@Override
		public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {}
	}
}
