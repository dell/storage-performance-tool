package com.dell.spt.base.load.step.local.context;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.AsyncRunnableBase;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException;
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
import com.dell.spt.base.load.step.DurationTime;
import com.dell.spt.base.load.step.local.LoadStepLocalBase;
import com.dell.spt.base.metrics.MetricsManager;
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
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

class StandaloneDeleteEngineStepTest {
	private static final int MULTI_INPUT_CONTEXT_COUNT = 10;

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
			assertEquals(2, step.deleteObjectLifecycle().fullSuccessfulRequests());
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
		final var config = config(3, 0, true);
		final var driver = new DeterministicDeleteDriver(DriverMode.HOLD);
		final var generator = generator(config, driver, new ManifestInput(3));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-unresolved", generator, driver, metrics, config.configVal("load"), false);

		try {
			step.start();
			final long durationStartNanos = System.nanoTime();
			step.startDurationInterval(
							durationStartNanos, durationStartNanos + TimeUnit.SECONDS.toNanos(1));
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
			final var failure = assertThrows(
							com.dell.spt.base.integrity.IntegrityTerminalException.class,
							step::validateTerminalState);
			assertTrue(failure.getMessage().contains("unresolved"));
			assertTrue(step.deletePhaseTiming().scheduledNanos() > 0);
			assertTrue(step.deletePhaseTiming().drainNanos() >= 0);
			assertTrue(step.deletePhaseTiming().drainNanos() < TimeUnit.SECONDS.toNanos(1));
		} finally {
			step.close();
			metrics.close();
		}
	}

	@Test
	void durationDeadlineDrainsDispatchedCompletionIntoMeasuredOutcomes() throws Exception {
		final var config = config(2, 2, true);
		final var driver = new DeterministicDeleteDriver(DriverMode.COMPLETE_DURING_DRAIN);
		final var generator = generator(config, driver, new ManifestInput(2));
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-duration-drain",
						generator,
						driver,
						metrics,
						config.configVal("load"),
						false);

		try {
			step.start();
			final long durationStartNanos = System.nanoTime();
			step.startDurationInterval(
							durationStartNanos, durationStartNanos + TimeUnit.SECONDS.toNanos(2));
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (driver.scheduled.get() == 0 && System.nanoTime() < deadline) {
				Thread.onSpinWait();
			}
			assertEquals(1, driver.scheduled.get());

			final long drainDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
			step.closeOperationAdmissionForStepStop();
			step.recoverQueuedOperationsForStepStop();
			step.drainDispatchedOperationsForStepStop(drainDeadline);
			assertTrue(driver.awaitCompletionThread());
			metrics.refreshLastSnapshot();

			assertDoesNotThrow(step::validateTerminalState);
			assertEquals(2, step.deleteObjectLifecycle().attempted());
			assertEquals(2, step.deleteObjectLifecycle().accepted());
			assertEquals(0, step.deleteObjectLifecycle().unresolved());
			assertEquals(1, metrics.lastSnapshot().successSnapshot().count());
			assertTrue(step.deletePhaseTiming().scheduledNanos() > 0);
			assertTrue(step.deletePhaseTiming().drainNanos() > 0);
			assertTrue(System.nanoTime() < drainDeadline);
		} finally {
			driver.releaseDrainCompletion();
			step.close();
			metrics.close();
		}
	}

	@Test
	void durationDeadlineRejectsCompletionArrivingWhileRecoveryIsStillBlocked() throws Exception {
		final var config = config(2, 0, true);
		final var driver = new DeterministicDeleteDriver(
						DriverMode.COMPLETE_AFTER_DEADLINE_DURING_RECOVERY);
		final var metrics = metrics();
		final var step = new LoadStepContextImpl<>(
						"standalone-delete-post-deadline-recovery",
						generator(config, driver, new ManifestInput(2)),
						driver,
						metrics,
						config.configVal("load"),
						false);
		final AtomicReference<Throwable> recoveryFailure = new AtomicReference<>();
		final Thread recoveryThread = Thread.ofPlatform().unstarted(() -> {
			try {
				step.recoverQueuedOperationsForStepStop();
			} catch (final Throwable failure) {
				recoveryFailure.set(failure);
			}
		});

		try {
			step.start();
			final long durationStartNanos = System.nanoTime();
			final long scheduledDeadlineNanos = DurationTime.deadlineAfter(
							durationStartNanos, TimeUnit.MILLISECONDS.toNanos(100));
			step.startDurationInterval(durationStartNanos, scheduledDeadlineNanos);
			final long dispatchDeadlineNanos = DurationTime.deadlineAfter(
							System.nanoTime(), TimeUnit.SECONDS.toNanos(5));
			while (driver.scheduled.get() == 0
							&& !DurationTime.deadlineReached(dispatchDeadlineNanos, System.nanoTime())) {
				Thread.onSpinWait();
			}
			assertEquals(1, driver.scheduled.get());

			step.closeOperationAdmissionForStepStop();
			recoveryThread.start();
			assertTrue(driver.awaitRecoveryEntered());
			while (!DurationTime.deadlineReached(scheduledDeadlineNanos, System.nanoTime())) {
				Thread.onSpinWait();
			}
			driver.releaseLateCompletion();
			assertTrue(driver.awaitCompletionThread());
			metrics.refreshLastSnapshot();

			assertEquals(0, step.operationLifecycle().terminal());
			assertEquals(1, step.operationLifecycle().unresolved());
			assertEquals(2, step.deleteObjectLifecycle().unresolved());
			assertEquals(0, metrics.lastSnapshot().successSnapshot().count());
		} finally {
			driver.releaseLateCompletion();
			driver.releaseRecovery();
			recoveryThread.join(TimeUnit.SECONDS.toMillis(5));
			step.close();
			metrics.close();
		}
		assertFalse(recoveryThread.isAlive());
		assertNull(recoveryFailure.get());
	}

	@Test
	void durationAwaitInvalidatesWhenLastScheduledRequestRemainsInFlight() throws Exception {
		final var config = config(2, 1, true);
		final var driver = new DeterministicDeleteDriver(DriverMode.COMPLETE_DURING_DRAIN);
		final var metrics = metrics();
		final var context = new LoadStepContextImpl<>(
						"standalone-delete-duration-exhausted-in-flight",
						generator(config, driver, new ManifestInput(2)),
						driver,
						metrics,
						config.configVal("load"),
						false);
		final var step = new MultiInputDeleteStep(
						config, mock(MetricsManager.class), context);

		try {
			step.start();
			step.startDurationInterval(TimeUnit.SECONDS.toNanos(1));
			awaitScheduled(driver);

			final var failure = assertThrows(
							IntegrityTerminalException.class,
							() -> step.await(250, TimeUnit.MILLISECONDS));

			assertTrue(failure.getMessage().contains("inventory slice exhausted"));
			assertEquals(1, context.operationLifecycle().dispatched());
			assertEquals(0, context.operationLifecycle().terminal());
		} finally {
			driver.releaseDrainCompletion();
			step.close();
			metrics.close();
		}
	}

	@Test
	void durationPublicStopBoundsRealMultiInputResourcesAndReconcilesEveryQueue() throws Exception {
		final var config = config(2, 1, true);
		final int contextCount = MULTI_INPUT_CONTEXT_COUNT;
		final var phaseConcurrency = new PhaseConcurrencyProbe(contextCount);
		final List<DeterministicDeleteDriver> drivers = new ArrayList<>(contextCount);
		final List<InterruptGatedManifestInput> inputs = new ArrayList<>(contextCount);
		final var generators = new ArrayList<LoadGenerator<IntegrityManifestDataItem, DeleteRequestOperation>>(
						contextCount);
		final List<MetricsContext<AllMetricsSnapshot>> metricsContexts = new ArrayList<>(contextCount);
		final var contexts = new ArrayList<LoadStepContextImpl<IntegrityManifestDataItem, DeleteRequestOperation>>(
						contextCount);
		for (int i = 0; i < contextCount; i++) {
			final var driver = new DeterministicDeleteDriver(
							DriverMode.COMPLETE_DURING_DRAIN, phaseConcurrency);
			final var input = new InterruptGatedManifestInput(1_000_000_000, 4);
			final var generator = generator(config, driver, input);
			final var metrics = metrics();
			drivers.add(driver);
			inputs.add(input);
			generators.add(generator);
			metricsContexts.add(metrics);
			contexts.add(new LoadStepContextImpl<>(
							"standalone-delete-duration-" + i,
							generator,
							driver,
							metrics,
							config.configVal("load"),
							false));
		}
		final var step = new MultiInputDeleteStep(
						config,
						mock(MetricsManager.class),
						contexts.toArray(LoadStepContext[]::new));

		try {
			step.start();
			step.startDurationInterval(TimeUnit.SECONDS.toNanos(1));
			for (int i = 0; i < contextCount; i++) {
				assertTrue(inputs.get(i).awaitSecondRead());
				awaitScheduled(drivers.get(i), 2);
			}

			final long stopStartedNanos = System.nanoTime();
			step.stop();

			assertTrue(step.isStopped());
			assertTrue(
							System.nanoTime() - stopStartedNanos < TimeUnit.SECONDS.toNanos(1),
							"multi-input stop exceeded the one step-wide drain budget");
			assertTrue(phaseConcurrency.maximumActive() > 1);
			assertEquals(contextCount, phaseConcurrency.maximumActive());
			assertEquals(0, phaseConcurrency.active());
			for (int i = 0; i < contextCount; i++) {
				final var driver = drivers.get(i);
				final var generator = generators.get(i);
				final var context = contexts.get(i);
				assertTrue(driver.awaitCompletionThread());
				assertDoesNotThrow(context::validateTerminalState);
				assertEquals(1_000_000_000, context.deleteObjectLifecycle().selected());
				assertEquals(4, context.deleteObjectLifecycle().attempted());
				assertEquals(999_999_996, context.deleteObjectLifecycle().unattempted());
				assertEquals(0, context.deleteObjectLifecycle().unresolved());
				assertEquals(2, context.operationLifecycle().terminal());
				assertEquals(0, context.operationLifecycle().unresolved());
				assertTrue(context.deleteObjectLifecycle().reconciled());
				assertEquals(4, generator.consumedItemCount());
				assertEquals(999_999_996, generator.aggregateUnattemptedItemCount());
				assertEquals(0, inputs.get(i).recoveryReadAttempts.get());
			}
		} finally {
			drivers.forEach(DeterministicDeleteDriver::releaseDrainCompletion);
			step.close();
			metricsContexts.forEach(MetricsContext::close);
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
		return config(deleteBatchSize, waitLimit, false);
	}

	private static Config config(
					final int deleteBatchSize, final int waitLimit, final boolean durationMode) {
		final var config = TestConfigBuilder.config();
		config.val("item-type", "data");
		config.val("item-output-file", "");
		config.val("load-batch-size", 4);
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-batchSize", deleteBatchSize);
		config.val("load-op-delete-duration", durationMode);
		config.val("load-step-limit-time", durationMode ? "60s" : "0s");
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

	private static void awaitScheduled(final DeterministicDeleteDriver driver) {
		awaitScheduled(driver, 1);
	}

	private static void awaitScheduled(
					final DeterministicDeleteDriver driver, final int expectedCount) {
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (driver.scheduled.get() < expectedCount && System.nanoTime() < deadline) {
			Thread.onSpinWait();
		}
		assertEquals(expectedCount, driver.scheduled.get());
	}

	private static final class PhaseConcurrencyProbe {
		private final CountDownLatch firstWaveEntered;
		private final AtomicInteger active = new AtomicInteger();
		private final AtomicInteger maximumActive = new AtomicInteger();

		private PhaseConcurrencyProbe(final int firstWaveSize) {
			firstWaveEntered = new CountDownLatch(firstWaveSize);
		}

		private void observe() {
			final int currentActive = active.incrementAndGet();
			maximumActive.accumulateAndGet(currentActive, Math::max);
			firstWaveEntered.countDown();
			try {
				assertTrue(
								firstWaveEntered.await(5, TimeUnit.SECONDS),
								"real local stop contexts were serialized below their configured bound");
			} catch (final InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new AssertionError(e);
			} finally {
				active.decrementAndGet();
			}
		}

		private int active() {
			return active.get();
		}

		private int maximumActive() {
			return maximumActive.get();
		}
	}

	private static final class MultiInputDeleteStep extends LoadStepLocalBase {
		private MultiInputDeleteStep(
						final Config config,
						final MetricsManager metricsManager,
						final LoadStepContext<?, ?>... contexts) {
			super(config, List.<Extension> of(), List.<Config> of(), metricsManager);
			stepContexts.addAll(List.of(contexts));
		}

		@Override
		public String getTypeName() {
			return "standalone-delete-multi-input-test";
		}

		@Override
		protected void init() {
			// The real contexts are provided directly by this lifecycle integration canary.
		}
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
		DEFAULT, HOLD, TRANSPORT_FAILURE, PROTOCOL_DEFECT, BLOCK_TERMINAL_PUBLICATION, COMPLETE_DURING_DRAIN, COMPLETE_AFTER_DEADLINE_DURING_RECOVERY
	}

	private static final class DeterministicDeleteDriver extends AsyncRunnableBase
					implements StorageDriver<IntegrityManifestDataItem, DeleteRequestOperation> {
		private final OperationLifecycleTracker<DeleteRequestOperation> lifecycle = new OperationLifecycleTracker<>();
		private final DriverMode mode;
		private final AtomicInteger scheduled = new AtomicInteger();
		private final List<DeleteRequestOperation> completed = java.util.Collections.synchronizedList(new ArrayList<>());
		private final List<Thread> completionThreads = java.util.Collections.synchronizedList(new ArrayList<>());
		private final CountDownLatch outputAccepted = new CountDownLatch(1);
		private final CountDownLatch terminalPublicationRelease = new CountDownLatch(1);
		private final CountDownLatch drainCompletionRelease = new CountDownLatch(1);
		private final CountDownLatch lateCompletionRelease = new CountDownLatch(1);
		private final CountDownLatch recoveryEntered = new CountDownLatch(1);
		private final CountDownLatch recoveryRelease = new CountDownLatch(1);
		private final PhaseConcurrencyProbe phaseConcurrency;
		private volatile Output<DeleteRequestOperation> resultOutput;

		private DeterministicDeleteDriver(final DriverMode mode) {
			this(mode, null);
		}

		private DeterministicDeleteDriver(
						final DriverMode mode, final PhaseConcurrencyProbe phaseConcurrency) {
			this.mode = mode;
			this.phaseConcurrency = phaseConcurrency;
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
				completionThreads.add(Thread.ofVirtual().start(() -> complete(operation, requestIndex)));
				return true;
			}
			if (mode == DriverMode.COMPLETE_DURING_DRAIN) {
				completionThreads.add(Thread.ofVirtual().start(() -> {
					awaitUninterruptibly(drainCompletionRelease);
					complete(operation, requestIndex);
				}));
				return true;
			}
			if (mode == DriverMode.COMPLETE_AFTER_DEADLINE_DURING_RECOVERY) {
				completionThreads.add(Thread.ofVirtual().start(() -> {
					awaitUninterruptibly(lateCompletionRelease);
					complete(operation, requestIndex);
				}));
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
			if (!lifecycle.completionStarted(operation)) {
				return false;
			}
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

		private void releaseDrainCompletion() {
			drainCompletionRelease.countDown();
		}

		private void releaseLateCompletion() {
			lateCompletionRelease.countDown();
		}

		private boolean awaitRecoveryEntered() throws InterruptedException {
			return recoveryEntered.await(5, TimeUnit.SECONDS);
		}

		private void releaseRecovery() {
			recoveryRelease.countDown();
		}

		private boolean awaitCompletionThread() throws InterruptedException {
			for (final Thread thread : List.copyOf(completionThreads)) {
				thread.join(TimeUnit.SECONDS.toMillis(5));
				if (thread.isAlive()) {
					return false;
				}
			}
			return true;
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
		public List<DeleteRequestOperation> recoverQueuedOperations() {
			if (mode == DriverMode.COMPLETE_AFTER_DEADLINE_DURING_RECOVERY) {
				recoveryEntered.countDown();
				awaitUninterruptibly(recoveryRelease);
			}
			return List.of();
		}

		@Override
		public void closeAdmission() {
			if (phaseConcurrency != null) {
				phaseConcurrency.observe();
			}
			releaseDrainCompletion();
		}

		@Override
		public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {}
	}
}
