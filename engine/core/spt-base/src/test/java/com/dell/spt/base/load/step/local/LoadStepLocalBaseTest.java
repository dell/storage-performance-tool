package com.dell.spt.base.load.step.local;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import com.dell.spt.base.load.step.DurationAwaitStatus;
import com.dell.spt.base.load.step.DurationTime;
import com.dell.spt.base.load.step.local.context.LoadStepContext;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.rmi.RemoteException;
import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

/**
 * Focused tests for LoadStepLocalBase.initMetrics to ensure we surface configuration issues without
 * flagging intentionally unlimited steps.
 */
class LoadStepLocalBaseTest {
	private static final int BLOCKING_PHASE_WIDTH = 8;

	@Test
	void initMetricsStoresValidCountLimitWithoutWarnings() {
		final Config config = baseConfig();
		config.val("load-step-id", "step-valid");
		config.val("run-id", 42L);
		config.val("run-comment", "healthy");
		config.val("load-op-limit-count", 500L);
		config.val("load-step-limit-time", "60s");

		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(config, mockMetricsManager(), mockContext());

		loadStep.initMetricsForTest(defaultMetricsConfig(config));

		final Map<String, Object> metadata = loadStep.latestMetadata();
		assertEquals(500L, metadata.get(MetricsConstants.METADATA_LIMIT_OP_COUNT));
		assertEquals(60L, metadata.get(MetricsConstants.METADATA_LIMIT_TIME_SEC));
	}

	@Test
	void initMetricsWarnsWhenLimitsConfiguredButInvalid() {
		final Config config = baseConfig();
		config.val("load-step-id", "step-invalid");
		config.val("run-id", 99L);
		config.val("run-comment", "bad-limits");

		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(config, mockMetricsManager(), mockContext());
		loadStep.setRawValue("load-op-limit-count", "not-a-number");
		loadStep.setRawValue("load-step-limit-time", "bogus");

		loadStep.initMetricsForTest(defaultMetricsConfig(config));

		final Map<String, Object> metadata = loadStep.latestMetadata();
		assertFalse(metadata.containsKey(MetricsConstants.METADATA_LIMIT_OP_COUNT));
		assertFalse(metadata.containsKey(MetricsConstants.METADATA_LIMIT_TIME_SEC));
	}

	@Test
	void effectiveVerifyFlagDisablesVerifyForNonDedupableData() {
		final Config config = baseConfig();
		config.val("load-step-id", "step-verify");
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(config, mockMetricsManager(), mockContext());
		assertFalse(loadStep.effectiveVerifyFlagForTest(true, false, "step-verify"));
	}

	@Test
	void effectiveVerifyFlagPreservesVerifyForDedupableData() {
		final Config config = baseConfig();
		config.val("load-step-id", "step-verify-ok");
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(config, mockMetricsManager(), mockContext());
		assertTrue(loadStep.effectiveVerifyFlagForTest(true, true, "step-verify-ok"));
		assertFalse(loadStep.effectiveVerifyFlagForTest(false, false, "step-verify-ok"));
	}

	@Test
	void standaloneDeletePublishesAggregatedWorkerObjectCounters() {
		final Config config = baseConfig();
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", false);
		config.val("load-op-limit-count", 10L);
		config.val("load-step-limit-time", "0s");
		final LoadStepContext first = mockContext();
		final LoadStepContext second = mockContext();
		when(first.deleteObjectLifecycle()).thenReturn(
						new DeleteObjectLifecycleSnapshot(3, 2, 1, 1, 1, 0, 0, 1, true));
		when(second.deleteObjectLifecycle()).thenReturn(
						new DeleteObjectLifecycleSnapshot(4, 4, 2, 2, 0, 0, 1, 1, true));
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), first, second);

		assertEquals(
						new DeleteObjectLifecycleSnapshot(7, 6, 3, 3, 1, 0, 1, 2, true),
						loadStep.deleteObjectLifecycle());
	}

	@Test
	void countDeleteHoldsEveryContextBeforeStartAndReleasesEveryContextTogether() throws Exception {
		final Config config = baseConfig();
		config.val("load-step-id", "count-admission-barrier");
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", false);
		config.val("load-op-limit-count", 10L);
		config.val("load-step-limit-time", "0s");
		final LoadStepContext first = mockContext();
		final LoadStepContext second = mockContext();
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), first, second);

		loadStep.startContextsForTest();
		final InOrder preparationOrder = inOrder(first, second);
		preparationOrder.verify(first).holdObjectFailureBudgetAdmission();
		preparationOrder.verify(second).holdObjectFailureBudgetAdmission();
		preparationOrder.verify(first).start();
		preparationOrder.verify(second).start();

		loadStep.releaseObjectFailureBudgetAdmission();
		verify(first).releaseObjectFailureBudgetAdmission();
		verify(second).releaseObjectFailureBudgetAdmission();
	}

	@Test
	void countDeleteFailsClosedWhenOneExpectedContextFailsToStart() throws Exception {
		final Config config = countDeleteConfig("count-start-failure");
		final LoadStepContext failing = mockContext();
		final LoadStepContext healthy = mockContext();
		when(failing.start()).thenThrow(new RemoteException("boom"));
		when(failing.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
		when(healthy.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), failing, healthy);

		assertDoesNotThrow(loadStep::startContextsForTest);

		assertNull(loadStep.deleteObjectLifecycle());
	}

	@Test
	void countDeleteFailsClosedWhenOneExpectedContextFailsToShutdown() throws Exception {
		final Config config = countDeleteConfig("count-shutdown-failure");
		final LoadStepContext failing = mockContext();
		final LoadStepContext healthy = mockContext();
		doThrow(new RemoteException("boom")).when(failing).shutdown();
		when(failing.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
		when(healthy.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), failing, healthy);
		loadStep.startContextsForTest();

		loadStep.shutdownContextsForTest();

		assertNull(loadStep.deleteObjectLifecycle());
	}

	@Test
	void countDeleteFailsClosedOnMissingCompatibilityContextObjectEvidence() {
		final Config config = countDeleteConfig("count-compatibility-evidence");
		final LoadStepContext modern = mockContext();
		final LoadStepContext compatibility = compatibilityContext();
		when(modern.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), modern, compatibility);

		assertNull(loadStep.deleteObjectLifecycle());
	}

	@Test
	void countDeleteRejectsCompatibilityContextBeforeAnyContextStarts() throws Exception {
		final Config config = countDeleteConfig("count-compatibility-admission");
		final LoadStepContext modern = mockContext();
		final LoadStepContext compatibility = compatibilityContext();
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), modern, compatibility);

		assertThrows(UnsupportedOperationException.class, loadStep::startContextsForTest);

		verify(modern).holdObjectFailureBudgetAdmission();
		verify(modern, never()).start();
	}

	@Test
	void countDeleteCompatibilityContextRejectsAdmissionRelease() {
		final LoadStepContext compatibility = compatibilityContext();

		assertThrows(
						UnsupportedOperationException.class,
						compatibility::releaseObjectFailureBudgetAdmission);
	}

	private static Config countDeleteConfig(final String stepId) {
		final Config config = baseConfig();
		config.val("load-step-id", stepId);
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", false);
		config.val("load-op-limit-count", 10L);
		config.val("load-step-limit-time", "0s");
		return config;
	}

	private static DeleteObjectLifecycleSnapshot cleanDeleteLifecycle() {
		return new DeleteObjectLifecycleSnapshot(1, 1, 1, 0, 0, 0, 0, 1, true);
	}

	private static LoadStepContext compatibilityContext() {
		return mock(LoadStepContext.class, CALLS_REAL_METHODS);
	}

	private static Config baseConfig() {
		return TestConfigBuilder.config();
	}

	private static Config defaultMetricsConfig(final Config config) {
		return config.configVal("output").configVal("metrics");
	}

	private static MetricsManager mockMetricsManager() {
		return mock(MetricsManager.class);
	}

	@SuppressWarnings({"unchecked", "rawtypes"
	})
	private static LoadStepContext mockContext() {
		final LoadStepContext ctx = mock(LoadStepContext.class);
		when(ctx.activeOpCount()).thenReturn(0);
		return ctx;
	}

	@Test
	void doStartWrappedRemovesFailingContext() throws RemoteException {
		final Config config = baseConfig();
		config.val("load-step-id", "step-start");

		final LoadStepContext failing = mock(LoadStepContext.class);
		final LoadStepContext healthy = mock(LoadStepContext.class);
		when(failing.start()).thenThrow(new RemoteException("boom"));

		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(config, mockMetricsManager(), failing, healthy);

		assertDoesNotThrow(loadStep::startContextsForTest);
		assertEquals(1, loadStep.contextCount());
		assertSame(healthy, loadStep.contextAt(0));
		verify(failing).start();
		verify(healthy).start();
	}

	@Test
	void doStartWrappedThrowsWhenAllContextsFail() throws RemoteException {
		final Config config = baseConfig();
		config.val("load-step-id", "step-none");

		final LoadStepContext failing = mock(LoadStepContext.class);
		when(failing.start()).thenThrow(new RemoteException("boom"));

		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(config, mockMetricsManager(), failing);

		assertThrows(IllegalStateException.class, loadStep::startContextsForTest);
		assertEquals(0, loadStep.contextCount());
		verify(failing).start();
	}

	@Test
	void doShutdownRemovesFailingContext() throws RemoteException {
		final Config config = baseConfig();
		config.val("load-step-id", "step-shutdown");

		final LoadStepContext failing = mock(LoadStepContext.class);
		final LoadStepContext healthy = mock(LoadStepContext.class);
		doThrow(new RemoteException("boom")).when(failing).shutdown();

		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(config, mockMetricsManager(), failing, healthy);

		loadStep.shutdownContextsForTest();
		assertEquals(1, loadStep.contextCount());
		assertSame(healthy, loadStep.contextAt(0));
		verify(failing).shutdown();
		verify(healthy).shutdown();
	}

	@Test
	void awaitRemovesContextOnRemoteFailure() throws Exception {
		final Config config = baseConfig();
		config.val("load-step-id", "step-await");

		final LoadStepContext failing = mock(LoadStepContext.class);
		when(failing.isDone()).thenReturn(false);
		when(failing.await(anyLong(), any(TimeUnit.class))).thenThrow(new RemoteException("boom"));

		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(config, mockMetricsManager(), failing);

		assertTrue(loadStep.awaitContextsForTest(5, TimeUnit.SECONDS));
		assertEquals(0, loadStep.contextCount());
		verify(failing).await(anyLong(), eq(TimeUnit.NANOSECONDS));
	}

	@Test
	void durationAwaitFailsWhenAnyOneOfMultipleContextsExhaustsEarly() {
		final Config config = durationConfig();
		final LoadStepContext exhausted = mock(LoadStepContext.class);
		final LoadStepContext stillActive = mock(LoadStepContext.class);
		when(exhausted.isDone()).thenReturn(true);
		when(stillActive.isDone()).thenReturn(false);
		when(exhausted.schedulingExhaustedAtNanos()).thenReturn(Long.MAX_VALUE);
		when(stillActive.schedulingExhaustedAtNanos()).thenReturn(Long.MAX_VALUE);

		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), exhausted, stillActive);

		final var failure = assertThrows(
						IntegrityTerminalException.class,
						() -> loadStep.awaitContextsForTest(5, TimeUnit.SECONDS));
		assertTrue(failure.getMessage().contains("inventory slice exhausted before the requested duration"));
		assertTrue(failure.getMessage().contains("increase --seed-objects"));
		assertEquals(2, loadStep.contextCount());
	}

	@Test
	void durationShortProbeInspectsLaterContextWithoutWaitingOnEarlierContext() throws Exception {
		final Config config = durationConfig();
		final LoadStepContext stillActive = mock(LoadStepContext.class);
		final LoadStepContext exhausted = mock(LoadStepContext.class);
		when(stillActive.isDone()).thenReturn(false);
		when(exhausted.isDone()).thenReturn(true);
		when(stillActive.schedulingExhaustedAtNanos()).thenReturn(Long.MAX_VALUE);
		when(exhausted.schedulingExhaustedAtNanos()).thenReturn(Long.MAX_VALUE);
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), stillActive, exhausted);

		final var failure = assertThrows(
						IntegrityTerminalException.class,
						() -> loadStep.awaitContextsForTest(100, TimeUnit.MILLISECONDS));

		assertTrue(failure.getMessage().contains("inventory slice exhausted"));
		verify(exhausted).isDone();
		verify(stillActive, never()).await(anyLong(), any(TimeUnit.class));
	}

	@Test
	void durationPublicStopUsesStepWideAdmissionRecoveryAndSharedConcurrentDrainPhases() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 1);
		final CountDownLatch admissionsClosed = new CountDownLatch(2);
		final CountDownLatch drainsEntered = new CountDownLatch(2);
		final AtomicLong firstDeadline = new AtomicLong();
		final AtomicLong secondDeadline = new AtomicLong();
		final LoadStepContext first = phasedContext(
						admissionsClosed, drainsEntered, firstDeadline);
		final LoadStepContext second = phasedContext(
						admissionsClosed, drainsEntered, secondDeadline);
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), first, second);

		assertDoesNotThrow(loadStep::stop);

		assertTrue(firstDeadline.get() > 0);
		assertEquals(firstDeadline.get(), secondDeadline.get());
		verify(first).recoverQueuedOperationsForStepStop();
		verify(second).recoverQueuedOperationsForStepStop();
		verify(first).shutdown();
		verify(second).shutdown();
		verify(first).stop();
		verify(second).stop();
	}

	@Test
	void durationRestartCreatesAFreshAdmissionBarrierAndDrainDeadline() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 1);
		final LoadStepContext context = mock(LoadStepContext.class);
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), context);
		final String deadlineThreadName = "spt-delete-deadline-duration-multi-input";
		final String drainDeadlineThreadName = "spt-delete-drain-deadline-duration-multi-input";

		loadStep.start();
		loadStep.startDurationInterval(TimeUnit.SECONDS.toNanos(10));
		assertTrue(threadIsAlive(deadlineThreadName));
		assertTrue(threadIsAlive(drainDeadlineThreadName));
		loadStep.stop();
		assertNoLiveThread(deadlineThreadName);
		assertNoLiveThread(drainDeadlineThreadName);
		loadStep.setRawValue("load-op-wait-limit", 2);
		loadStep.start();
		loadStep.startDurationInterval(TimeUnit.SECONDS.toNanos(10));
		assertTrue(threadIsAlive(deadlineThreadName));
		assertTrue(threadIsAlive(drainDeadlineThreadName));
		loadStep.stop();
		assertNoLiveThread(deadlineThreadName);
		assertNoLiveThread(drainDeadlineThreadName);

		final ArgumentCaptor<Long> deadlines = ArgumentCaptor.forClass(Long.class);
		verify(context, times(2)).closeOperationAdmissionForStepStop();
		verify(context, times(2)).recoverQueuedOperationsForStepStop();
		verify(context, times(2)).drainDispatchedOperationsForStepStop(deadlines.capture());
		assertTrue(
						deadlines.getAllValues().get(1) - deadlines.getAllValues().get(0) > TimeUnit.MILLISECONDS.toNanos(500),
						"a restarted run must not inherit the first run's drain deadline");
		assertDoesNotThrow(loadStep::close);
	}

	private static boolean threadIsAlive(final String exactName) {
		return Thread.getAllStackTraces().keySet().stream()
						.anyMatch(thread -> thread.isAlive() && exactName.equals(thread.getName()));
	}

	private static void assertNoLiveThread(final String exactName) throws InterruptedException {
		final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
		do {
			if (!threadIsAlive(exactName)) {
				return;
			}
			Thread.sleep(10);
		} while (System.nanoTime() < deadlineNanos);
		assertFalse(threadIsAlive(exactName), "lifecycle thread remains live: " + exactName);
	}

	@Test
	void workerDeadlineClosesAdmissionWithoutWaitingForTheController() throws Exception {
		final Config config = durationConfig();
		final LoadStepContext context = mock(LoadStepContext.class);
		final CountDownLatch admissionClosed = new CountDownLatch(1);
		doAnswer(invocation -> {
			admissionClosed.countDown();
			return null;
		}).when(context).closeOperationAdmissionForStepStop();
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), context);
		final long durationNanos = TimeUnit.MILLISECONDS.toNanos(50);

		loadStep.start();
		loadStep.prepareDurationInterval(durationNanos);
		final long startedNanos = System.nanoTime();
		loadStep.startDurationInterval(durationNanos);

		assertTrue(admissionClosed.await(1, TimeUnit.SECONDS));
		assertTrue(System.nanoTime() - startedNanos >= TimeUnit.MILLISECONDS.toNanos(25));
		loadStep.stop();
		verify(context, times(1)).closeOperationAdmissionForStepStop();
		assertDoesNotThrow(loadStep::close);
	}

	@Test
	void armedDurationAwaitHonorsEachShortControlPlanePollTimeout() throws Exception {
		final Config config = durationConfig();
		final LoadStepContext context = mock(LoadStepContext.class);
		when(context.schedulingExhaustedAtNanos()).thenReturn(Long.MAX_VALUE);
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), context);
		loadStep.startDurationInterval(TimeUnit.MILLISECONDS.toNanos(400));

		final long pollStartedNanos = System.nanoTime();
		assertFalse(loadStep.awaitContextsForTest(20, TimeUnit.MILLISECONDS));
		final long pollElapsedNanos = System.nanoTime() - pollStartedNanos;

		assertTrue(
						pollElapsedNanos < TimeUnit.MILLISECONDS.toNanos(200),
						"a short control-plane poll blocked until the worker duration deadline");
		assertEquals(DurationAwaitStatus.RUNNING, loadStep.durationAwaitStatus());
		assertDoesNotThrow(loadStep::stop);
		assertDoesNotThrow(loadStep::close);
	}

	@Test
	void workerDeadlineClosesAdmissionWhileAContextReleaseIsStillDelayed() throws Exception {
		final Config config = durationConfig();
		final CountDownLatch startEntered = new CountDownLatch(1);
		final CountDownLatch releaseStart = new CountDownLatch(1);
		final CountDownLatch admissionClosed = new CountDownLatch(1);
		final LoadStepContext delayed = mock(LoadStepContext.class);
		doAnswer(invocation -> {
			startEntered.countDown();
			assertTrue(releaseStart.await(2, TimeUnit.SECONDS));
			return null;
		}).when(delayed).startDurationInterval(anyLong(), anyLong());
		final LoadStepContext healthy = mock(LoadStepContext.class);
		doAnswer(invocation -> {
			admissionClosed.countDown();
			return null;
		}).when(healthy).closeOperationAdmissionForStepStop();
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), delayed, healthy);
		final AtomicReference<Throwable> startFailure = new AtomicReference<>();
		final Thread startThread = Thread.ofPlatform().start(() -> {
			try {
				loadStep.startDurationInterval(TimeUnit.MILLISECONDS.toNanos(50));
			} catch (final Throwable failure) {
				startFailure.set(failure);
			}
		});

		try {
			assertTrue(startEntered.await(1, TimeUnit.SECONDS));
			assertTrue(
							admissionClosed.await(500, TimeUnit.MILLISECONDS),
							"the absolute worker deadline waited for a delayed context release");
		} finally {
			releaseStart.countDown();
			startThread.join(TimeUnit.SECONDS.toMillis(2));
		}

		assertFalse(startThread.isAlive());
		assertNull(startFailure.get());
		assertDoesNotThrow(loadStep::close);
	}

	@Test
	void localDrainBudgetStartsAtTheWorkerDeadlineBeforeAdmissionClosure() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 1);
		final AtomicLong drainDeadlineNanos = new AtomicLong(Long.MAX_VALUE);
		final LoadStepContext context = mock(LoadStepContext.class);
		doAnswer(invocation -> {
			Thread.sleep(700);
			return null;
		}).when(context).closeOperationAdmissionForStepStop();
		doAnswer(invocation -> {
			drainDeadlineNanos.set(invocation.getArgument(0));
			return null;
		}).when(context).drainDispatchedOperationsForStepStop(anyLong());
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), context);

		loadStep.startDurationInterval(TimeUnit.MILLISECONDS.toNanos(1));
		assertDoesNotThrow(loadStep::stop);

		final long remainingNanos = drainDeadlineNanos.get() - System.nanoTime();
		assertTrue(remainingNanos > 0);
		assertTrue(
						remainingNanos < TimeUnit.MILLISECONDS.toNanos(600),
						"admission closure received time outside the worker deadline drain budget");
		assertDoesNotThrow(loadStep::close);
	}

	@Test
	void controllerSuppliedDrainBudgetTightensTheArmedWorkerDeadline() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 5);
		final AtomicLong drainDeadlineNanos = new AtomicLong(Long.MAX_VALUE);
		final AtomicLong drainReceivedNanos = new AtomicLong(Long.MIN_VALUE);
		final LoadStepContext context = mock(LoadStepContext.class);
		doAnswer(invocation -> {
			drainReceivedNanos.set(System.nanoTime());
			drainDeadlineNanos.set(invocation.getArgument(0));
			return null;
		}).when(context).drainDispatchedOperationsForStepStop(anyLong());
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), context);
		final long controllerDrainBudgetNanos = TimeUnit.MILLISECONDS.toNanos(100);

		loadStep.startDurationInterval(TimeUnit.SECONDS.toNanos(10));
		loadStep.enforceDispatchedOperationsDeadlineForStepStop(controllerDrainBudgetNanos);
		loadStep.closeOperationAdmissionForStepStop();
		loadStep.recoverQueuedOperationsForStepStop();
		loadStep.drainDispatchedOperationsForStepStop(controllerDrainBudgetNanos);

		final long propagatedBudgetNanos = DurationTime.remainingNanos(
						drainDeadlineNanos.get(), drainReceivedNanos.get());
		assertTrue(propagatedBudgetNanos > 0);
		assertTrue(
						propagatedBudgetNanos <= controllerDrainBudgetNanos,
						"the worker retained its original duration deadline instead of the controller's earlier cutoff");
		assertDoesNotThrow(loadStep::close);
	}

	@Test
	void controllerDeadlineExpiresDispatchWhileRecoveryIsStillBlocked() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 5);
		final CountDownLatch recoveryEntered = new CountDownLatch(1);
		final CountDownLatch releaseRecovery = new CountDownLatch(1);
		final CountDownLatch dispatchExpired = new CountDownLatch(1);
		final LoadStepContext context = mock(LoadStepContext.class);
		doAnswer(invocation -> {
			recoveryEntered.countDown();
			assertTrue(releaseRecovery.await(2, TimeUnit.SECONDS));
			return null;
		}).when(context).recoverQueuedOperationsForStepStop();
		doAnswer(invocation -> {
			dispatchExpired.countDown();
			return null;
		}).when(context).expireDispatchedOperationsDeadlineForStepStop();
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), context);
		final AtomicReference<Throwable> recoveryFailure = new AtomicReference<>();

		loadStep.startDurationInterval(TimeUnit.SECONDS.toNanos(10));
		loadStep.enforceDispatchedOperationsDeadlineForStepStop(
						TimeUnit.MILLISECONDS.toNanos(100));
		loadStep.closeOperationAdmissionForStepStop();
		final Thread recoveryThread = Thread.ofPlatform().start(() -> {
			try {
				loadStep.recoverQueuedOperationsForStepStop();
			} catch (final Throwable failure) {
				recoveryFailure.set(failure);
			}
		});
		try {
			assertTrue(recoveryEntered.await(1, TimeUnit.SECONDS));
			assertTrue(
							dispatchExpired.await(1, TimeUnit.SECONDS),
							"the worker terminal cutoff waited for queue recovery to complete");
		} finally {
			releaseRecovery.countDown();
			recoveryThread.join(TimeUnit.SECONDS.toMillis(2));
		}

		assertFalse(recoveryThread.isAlive());
		assertNull(recoveryFailure.get());
		verify(context, atLeastOnce())
						.enforceDispatchedOperationsDeadlineForStepStop(anyLong());
		verify(context).expireDispatchedOperationsDeadlineForStepStop();
		loadStep.drainDispatchedOperationsForStepStop(0);
		assertDoesNotThrow(loadStep::close);
	}

	@Test
	void deterministicTerminalValidationFailsTheVerdictWithoutBlockingContextClose() throws Exception {
		final Config config = durationConfig();
		final LoadStepContext context = mock(LoadStepContext.class);
		doThrow(new IntegrityTerminalException(
						IntegrityTerminalException.Category.EXECUTION,
						"unresolved dispatched request"))
						.when(context).validateTerminalState();
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), context);

		loadStep.closeOperationAdmissionForStepStop();
		loadStep.recoverQueuedOperationsForStepStop();
		loadStep.drainDispatchedOperationsForStepStop(TimeUnit.SECONDS.toNanos(1));
		final var failure = assertThrows(
						IntegrityTerminalException.class,
						loadStep::validateTerminalStateForStepStop);
		assertTrue(failure.getMessage().contains("unresolved dispatched request"));
		assertDoesNotThrow(loadStep::stop);
		assertDoesNotThrow(loadStep::close);

		assertTrue(loadStep.isClosed());
		verify(context).close();
		verify(context).validateTerminalState();
	}

	@Test
	void durationAdmissionFailureIsReportedWithoutCrossingRecoveryBarrier() throws Exception {
		final Config config = durationConfig();
		final LoadStepContext failing = mock(LoadStepContext.class);
		final LoadStepContext healthy = mock(LoadStepContext.class);
		doThrow(new IllegalStateException("admission failure"))
						.when(failing).closeOperationAdmissionForStepStop();
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), failing, healthy);

		assertDoesNotThrow(loadStep::stop);
		final var failure = assertThrows(IntegrityTerminalException.class, loadStep::close);

		assertTrue(failure.getMessage().contains("close operation admission"));
		for (final LoadStepContext context : List.of(failing, healthy)) {
			verify(context).closeOperationAdmissionForStepStop();
			verify(context, never()).recoverQueuedOperationsForStepStop();
			verify(context, never()).drainDispatchedOperationsForStepStop(anyLong());
			verify(context, never()).shutdown();
			verify(context, never()).stop();
			verify(context, never()).close();
			verify(context, never()).validateTerminalState();
		}
	}

	@Test
	void durationFailedCloseRetainsContextsForSafeCleanupRetry() throws Exception {
		final Config config = durationConfig();
		final AtomicLong admissionAttempts = new AtomicLong();
		final LoadStepContext context = mock(LoadStepContext.class);
		doAnswer(invocation -> {
			if (admissionAttempts.incrementAndGet() == 1) {
				throw new IllegalStateException("transient admission failure");
			}
			return null;
		}).when(context).closeOperationAdmissionForStepStop();
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), context);

		assertDoesNotThrow(loadStep::stop);
		assertThrows(IntegrityTerminalException.class, loadStep::close);
		verify(context, never()).close();

		assertDoesNotThrow(loadStep::close);
		verify(context, times(2)).closeOperationAdmissionForStepStop();
		verify(context).recoverQueuedOperationsForStepStop();
		verify(context).drainDispatchedOperationsForStepStop(anyLong());
		verify(context).shutdown();
		verify(context).stop();
		verify(context).close();
		verify(context).validateTerminalState();
		assertTrue(loadStep.isClosed());
	}

	@Test
	void durationRunRetriesTransientCleanupAndClosesRetainedContextThreads() throws Exception {
		final Config config = durationConfig();
		config.val("load-step-id", "duration-run-cleanup-retry");
		final AtomicLong admissionAttempts = new AtomicLong();
		final LoadStepContext context = mock(LoadStepContext.class);
		when(context.isDone()).thenReturn(true);
		when(context.schedulingExhaustedAtNanos()).thenReturn(System.nanoTime());
		doAnswer(invocation -> {
			if (admissionAttempts.incrementAndGet() == 1) {
				throw new IllegalStateException("transient admission failure");
			}
			return null;
		}).when(context).closeOperationAdmissionForStepStop();
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), context);

		final var failure = assertThrows(IntegrityTerminalException.class, loadStep::run);

		assertTrue(failure.getMessage().contains("inventory slice exhausted before the requested duration"));
		verify(context, times(2)).closeOperationAdmissionForStepStop();
		verify(context).recoverQueuedOperationsForStepStop();
		verify(context).drainDispatchedOperationsForStepStop(anyLong());
		verify(context).shutdown();
		verify(context).stop();
		verify(context).close();
		assertTrue(loadStep.isClosed());
		assertNoLiveThreads(
						"spt-delete-deadline-duration-run-cleanup-retry",
						"spt-delete-drain-deadline-duration-run-cleanup-retry",
						"spt-delete-context-admission-",
						"spt-delete-context-stop-");
	}

	@Test
	void durationRunValidatesTerminalAccountingAfterClosingItsLocalContext() throws Exception {
		final Config config = durationConfig();
		config.val("load-step-id", "duration-run-terminal-validation");
		config.val("load-step-limit-time", "1s");
		final LoadStepContext context = mock(LoadStepContext.class);
		when(context.schedulingExhaustedAtNanos()).thenReturn(Long.MAX_VALUE);
		doThrow(new IntegrityTerminalException(
						IntegrityTerminalException.Category.EXECUTION,
						"unresolved duration identity"))
						.when(context).validateTerminalState();
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), context);

		final var failure = assertThrows(IntegrityTerminalException.class, loadStep::run);

		assertTrue(failure.getMessage().contains("unresolved duration identity"));
		verify(context).validateTerminalState();
		verify(context).shutdown();
		verify(context).stop();
		verify(context).close();
		assertNoLiveThreads(
						"spt-delete-deadline-duration-run-terminal-validation",
						"spt-delete-drain-deadline-duration-run-terminal-validation",
						"spt-delete-context-stop-");
	}

	@Test
	void durationRunCleanupExhaustionCancelsDeadlineGuardsAndPreservesFirstFailure()
					throws Exception {
		final Config config = durationConfig();
		config.val("load-step-id", "duration-run-cleanup-exhaustion");
		final AtomicLong admissionAttempts = new AtomicLong();
		final LoadStepContext context = mock(LoadStepContext.class);
		when(context.isDone()).thenReturn(true);
		when(context.schedulingExhaustedAtNanos()).thenReturn(System.nanoTime());
		doAnswer(invocation -> {
			admissionAttempts.incrementAndGet();
			throw new IllegalStateException("permanent admission failure");
		})
						.when(context).closeOperationAdmissionForStepStop();
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), context);

		final var failure = assertThrows(IntegrityTerminalException.class, loadStep::run);

		assertTrue(failure.getMessage().contains("inventory slice exhausted"));
		assertEquals(1, failure.getSuppressed().length);
		final Throwable firstCleanupFailure = failure.getSuppressed()[0];
		assertTrue(firstCleanupFailure.getMessage().contains("close operation admission"));
		assertEquals(2, firstCleanupFailure.getSuppressed().length);
		assertEquals(3, admissionAttempts.get());
		verify(context, times(3)).closeOperationAdmissionForStepStop();
		verify(context, never()).recoverQueuedOperationsForStepStop();
		verify(context, never()).close();
		assertNoLiveThreads(
						"spt-delete-deadline-duration-run-cleanup-exhaustion",
						"spt-delete-drain-deadline-duration-run-cleanup-exhaustion",
						"spt-delete-context-admission-",
						"spt-delete-context-stop-");
		final long completedAdmissionAttempts = admissionAttempts.get();
		Thread.sleep(100);
		assertEquals(
						completedAdmissionAttempts,
						admissionAttempts.get(),
						"a canceled deadline guard made a later context call");
	}

	@Test
	void durationRepeatedRecoveryFailureRetainsContextsUntilFullCleanupRetry() throws Exception {
		final Config config = durationConfig();
		final AtomicLong recoveryAttempts = new AtomicLong();
		final LoadStepContext context = mock(LoadStepContext.class);
		doAnswer(invocation -> {
			if (recoveryAttempts.incrementAndGet() <= 2) {
				throw new IllegalStateException("transient recovery failure");
			}
			return null;
		}).when(context).recoverQueuedOperationsForStepStop();
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), context);

		assertDoesNotThrow(loadStep::stop);
		assertThrows(IntegrityTerminalException.class, loadStep::close);
		verify(context, never()).close();

		assertThrows(IntegrityTerminalException.class, loadStep::close);
		verify(context, never()).close();

		assertDoesNotThrow(loadStep::close);
		verify(context).closeOperationAdmissionForStepStop();
		verify(context, times(3)).recoverQueuedOperationsForStepStop();
		verify(context).close();
		verify(context).validateTerminalState();
		assertTrue(loadStep.isClosed());
	}

	@Test
	void durationFinalDeadlineAuditRejectsExhaustionDuringTheLastPollingWindow() {
		final Config config = durationConfig();
		final AtomicLong exhaustedBeforeDeadline = new AtomicLong();
		final AtomicLong probes = new AtomicLong();
		final LoadStepContext exhaustedAtDeadline = mock(LoadStepContext.class);
		doAnswer(invocation -> {
			if (probes.incrementAndGet() == 1) {
				exhaustedBeforeDeadline.set(System.nanoTime());
				Thread.sleep(60);
				return Long.MAX_VALUE;
			}
			return exhaustedBeforeDeadline.get();
		}).when(exhaustedAtDeadline).schedulingExhaustedAtNanos();
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), exhaustedAtDeadline);

		assertThrows(
						IntegrityTerminalException.class,
						() -> loadStep.awaitContextsForTest(50, TimeUnit.MILLISECONDS));
		verify(exhaustedAtDeadline, times(2)).schedulingExhaustedAtNanos();
	}

	@Test
	void durationAwaitFailureRemainsStickyForThePostAdmissionVerdict() {
		final Config config = durationConfig();
		final LoadStepContext failed = mock(LoadStepContext.class);
		when(failed.schedulingExhaustedAtNanos()).thenThrow(new IllegalStateException("probe failed"));
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), failed);

		assertThrows(
						IllegalStateException.class,
						() -> loadStep.awaitContextsForTest(1, TimeUnit.SECONDS));
		assertEquals(
						com.dell.spt.base.load.step.DurationAwaitStatus.FAILED,
						loadStep.durationAwaitStatus());
	}

	@Test
	void durationExhaustionTimestampedBeforeDeadlineInvalidatesTheRun() {
		final Config config = durationConfig();
		final LoadStepContext exhaustedBeforeDeadline = mock(LoadStepContext.class);
		when(exhaustedBeforeDeadline.schedulingExhaustedAtNanos()).thenReturn(System.nanoTime());
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), exhaustedBeforeDeadline);

		assertThrows(
						IntegrityTerminalException.class,
						() -> loadStep.awaitContextsForTest(1, TimeUnit.SECONDS));
	}

	@Test
	void durationAuditRecognizesLongMaxExhaustionBeforeAWrappedDeadline() throws Exception {
		final LoadStepContext exhaustedAtLongMax = mock(LoadStepContext.class);
		when(exhaustedAtLongMax.schedulingExhaustionNanos())
						.thenReturn(java.util.OptionalLong.of(Long.MAX_VALUE));
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						durationConfig(), mockMetricsManager(), exhaustedAtLongMax);
		loadStep.setDurationAuditStateForTest(Long.MIN_VALUE + 10);

		assertEquals(DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE, loadStep.durationAwaitStatus());
	}

	@ParameterizedTest
	@EnumSource(LocalCleanupFailurePhase.class)
	void durationCleanupRetriesOnlyTheFailedContextForEveryLaterPhase(
					final LocalCleanupFailurePhase failedPhase) throws Exception {
		final Config config = durationConfig();
		final AtomicLong attempts = new AtomicLong();
		final LoadStepContext failing = mock(LoadStepContext.class);
		final LoadStepContext healthy = mock(LoadStepContext.class);
		configureOneShotFailure(failing, failedPhase, attempts);
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), failing, healthy);

		assertDoesNotThrow(loadStep::stop);
		assertThrows(IntegrityTerminalException.class, loadStep::close);
		assertDoesNotThrow(loadStep::close);

		verifyLocalPhase(failing, failedPhase, times(2));
		verifyLocalPhase(healthy, failedPhase, times(1));
		assertTrue(loadStep.isClosed());
	}

	@Test
	void durationRecoveryOffersEveryContextOnceAndRetainsBlockedCallsAcrossRetry() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 0);
		final int contextCount = BLOCKING_PHASE_WIDTH + 2;
		final CountDownLatch blockersEntered = new CountDownLatch(BLOCKING_PHASE_WIDTH);
		final CountDownLatch trailingEntered = new CountDownLatch(2);
		final CountDownLatch releaseBlockers = new CountDownLatch(1);
		final List<LoadStepContext> contexts = new java.util.ArrayList<>(contextCount);
		for (int i = 0; i < contextCount; i++) {
			final int index = i;
			final LoadStepContext context = mock(LoadStepContext.class);
			doAnswer(invocation -> {
				if (index < BLOCKING_PHASE_WIDTH) {
					blockersEntered.countDown();
					while (releaseBlockers.getCount() > 0) {
						try {
							releaseBlockers.await();
						} catch (final InterruptedException ignored) {
							// Model a local lifecycle hook which ignores cancellation.
						}
					}
				} else {
					trailingEntered.countDown();
				}
				return null;
			}).when(context).recoverQueuedOperationsForStepStop();
			contexts.add(context);
		}
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), contexts.toArray(LoadStepContext[]::new));

		final AtomicReference<Throwable> retryFailure = new AtomicReference<>();
		try {
			assertDoesNotThrow(loadStep::stop);
			assertTrue(blockersEntered.await(1, TimeUnit.SECONDS));
			assertTrue(trailingEntered.await(1, TimeUnit.SECONDS));
			assertThrows(IntegrityTerminalException.class, loadStep::close);
			final Thread retry = Thread.ofPlatform().start(() -> {
				try {
					loadStep.close();
				} catch (final Throwable failure) {
					retryFailure.set(failure);
				}
			});
			retry.join(200);
			assertTrue(retry.isAlive());
			for (final LoadStepContext context : contexts) {
				verify(context).closeOperationAdmissionForStepStop();
				verify(context, times(1)).recoverQueuedOperationsForStepStop();
				verify(context, never()).drainDispatchedOperationsForStepStop(anyLong());
			}
			releaseBlockers.countDown();
			retry.join(TimeUnit.SECONDS.toMillis(3));
			assertFalse(retry.isAlive());
			assertNull(retryFailure.get());
			assertTrue(loadStep.isClosed());
		} finally {
			releaseBlockers.countDown();
		}
	}

	@Test
	void durationBlockingAdmissionIsInterruptedWithoutCrossingRecoveryBarrier() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 1);
		final CountDownLatch blockingAdmissionEntered = new CountDownLatch(1);
		final CountDownLatch releaseBlockingAdmission = new CountDownLatch(1);
		final CountDownLatch blockingAdmissionInterrupted = new CountDownLatch(1);
		final LoadStepContext blocking = mock(LoadStepContext.class);
		doAnswer(invocation -> {
			blockingAdmissionEntered.countDown();
			try {
				releaseBlockingAdmission.await();
			} catch (final InterruptedException e) {
				blockingAdmissionInterrupted.countDown();
				throw e;
			}
			return null;
		}).when(blocking).closeOperationAdmissionForStepStop();
		final LoadStepContext healthy = mock(LoadStepContext.class);
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config, mockMetricsManager(), blocking, healthy);

		final Thread stopThread = Thread.ofPlatform().start(loadStep::stop);
		try {
			assertTrue(blockingAdmissionEntered.await(1, TimeUnit.SECONDS));
			stopThread.join(TimeUnit.SECONDS.toMillis(3));
			assertFalse(stopThread.isAlive(), "a blocking context must not stall local cleanup");
			assertFalse(blockingAdmissionInterrupted.await(100, TimeUnit.MILLISECONDS));
			verify(healthy, never()).recoverQueuedOperationsForStepStop();
			verify(healthy, never()).drainDispatchedOperationsForStepStop(anyLong());
			verify(healthy, never()).shutdown();
			verify(healthy, never()).stop();
			final var failure = assertThrows(IntegrityTerminalException.class, loadStep::close);
			assertTrue(failure.getMessage().contains("close operation admission"));
			verify(blocking, never()).close();
			verify(healthy, never()).close();
			verify(healthy, never()).validateTerminalState();
			releaseBlockingAdmission.countDown();
			assertDoesNotThrow(loadStep::close);
			assertTrue(loadStep.isClosed());
		} finally {
			releaseBlockingAdmission.countDown();
			stopThread.interrupt();
			stopThread.join(TimeUnit.SECONDS.toMillis(2));
		}
	}

	@Test
	void durationAdmissionBarrierOffersClosureBeyondUninterruptibleContextWave() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 1);
		final CountDownLatch blockersEntered = new CountDownLatch(BLOCKING_PHASE_WIDTH);
		final CountDownLatch releaseBlockers = new CountDownLatch(1);
		final List<LoadStepContext> contexts = new java.util.ArrayList<>();
		for (int i = 0; i < BLOCKING_PHASE_WIDTH; i++) {
			final LoadStepContext blocker = mock(LoadStepContext.class);
			doAnswer(invocation -> {
				blockersEntered.countDown();
				while (true) {
					try {
						releaseBlockers.await();
						return null;
					} catch (final InterruptedException ignored) {
						// Model an extension which does not cooperate with cancellation.
					}
				}
			}).when(blocker).closeOperationAdmissionForStepStop();
			contexts.add(blocker);
		}
		final CountDownLatch trailingAdmissionClosed = new CountDownLatch(1);
		final LoadStepContext trailing = mock(LoadStepContext.class);
		doAnswer(invocation -> {
			trailingAdmissionClosed.countDown();
			return null;
		}).when(trailing).closeOperationAdmissionForStepStop();
		contexts.add(trailing);
		final TestLoadStepLocalBase loadStep = new TestLoadStepLocalBase(
						config,
						mockMetricsManager(),
						contexts.toArray(LoadStepContext[]::new));

		final Thread stopThread = Thread.ofPlatform().start(loadStep::stop);
		try {
			assertTrue(blockersEntered.await(1, TimeUnit.SECONDS));
			assertTrue(
							trailingAdmissionClosed.await(1, TimeUnit.SECONDS),
							"a trailing context never received the deadline admission-close signal");
			stopThread.join(TimeUnit.SECONDS.toMillis(3));
			assertFalse(stopThread.isAlive(), "an uninterruptible context stalled the local step");
			verify(trailing, never()).recoverQueuedOperationsForStepStop();
		} finally {
			releaseBlockers.countDown();
			stopThread.interrupt();
			stopThread.join(TimeUnit.SECONDS.toMillis(2));
		}
	}

	private static LoadStepContext phasedContext(
					final CountDownLatch admissionsClosed,
					final CountDownLatch drainsEntered,
					final AtomicLong deadlineCapture) throws Exception {
		final LoadStepContext context = mock(LoadStepContext.class);
		doAnswer(invocation -> {
			admissionsClosed.countDown();
			return null;
		}).when(context).closeOperationAdmissionForStepStop();
		doAnswer(invocation -> {
			assertEquals(0, admissionsClosed.getCount(), "recovery started before every admission gate closed");
			return null;
		}).when(context).recoverQueuedOperationsForStepStop();
		doAnswer(invocation -> {
			assertEquals(0, admissionsClosed.getCount(), "drain started before every admission gate closed");
			deadlineCapture.set(invocation.getArgument(0));
			drainsEntered.countDown();
			assertTrue(drainsEntered.await(1, TimeUnit.SECONDS), "context drains were serialized");
			return null;
		}).when(context).drainDispatchedOperationsForStepStop(anyLong());
		return context;
	}

	private enum LocalCleanupFailurePhase {
		RECOVERY, DRAIN, SHUTDOWN, STOP, CLOSE
	}

	private static void configureOneShotFailure(
					final LoadStepContext context,
					final LocalCleanupFailurePhase phase,
					final AtomicLong attempts) throws Exception {
		final org.mockito.stubbing.Answer<Void> answer = invocation -> {
			if (attempts.incrementAndGet() == 1) {
				throw new IllegalStateException("transient " + phase.name().toLowerCase(Locale.ROOT));
			}
			return null;
		};
		switch (phase) {
		case RECOVERY -> doAnswer(answer).when(context).recoverQueuedOperationsForStepStop();
		case DRAIN -> doAnswer(answer).when(context).drainDispatchedOperationsForStepStop(anyLong());
		case SHUTDOWN -> doAnswer(answer).when(context).shutdown();
		case STOP -> doAnswer(answer).when(context).stop();
		case CLOSE -> doAnswer(answer).when(context).close();
		default -> throw new AssertionError(phase);
		}
	}

	private static void verifyLocalPhase(
					final LoadStepContext context,
					final LocalCleanupFailurePhase phase,
					final org.mockito.verification.VerificationMode mode) throws Exception {
		switch (phase) {
		case RECOVERY -> verify(context, mode).recoverQueuedOperationsForStepStop();
		case DRAIN -> verify(context, mode).drainDispatchedOperationsForStepStop(anyLong());
		case SHUTDOWN -> verify(context, mode).shutdown();
		case STOP -> verify(context, mode).stop();
		case CLOSE -> verify(context, mode).close();
		default -> throw new AssertionError(phase);
		}
	}

	private static Config durationConfig() {
		final Config config = baseConfig();
		config.val("load-step-id", "duration-multi-input");
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", true);
		config.val("load-step-limit-time", "60s");
		return config;
	}

	private static void assertNoLiveThreads(final String... namePrefixes)
					throws InterruptedException {
		final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
		List<String> liveNames;
		do {
			liveNames = Thread.getAllStackTraces().keySet().stream()
							.filter(Thread::isAlive)
							.map(Thread::getName)
							.filter(name -> java.util.Arrays.stream(namePrefixes).anyMatch(name::startsWith))
							.toList();
			if (liveNames.isEmpty()) {
				return;
			}
			Thread.sleep(10);
		} while (System.nanoTime() < deadlineNanos);
		org.junit.jupiter.api.Assertions.fail("duration local lifecycle threads remain live: " + liveNames);
	}

	private static final class TestLoadStepLocalBase extends LoadStepLocalBase {

		TestLoadStepLocalBase(
						final Config baseConfig,
						final MetricsManager metricsManager,
						final LoadStepContext... stepContexts) {
			super(baseConfig, List.<Extension> of(), List.<Config> of(), metricsManager);
			this.stepContexts.addAll(List.of(stepContexts));
		}

		@Override
		public String getTypeName() {
			return "test-load-step";
		}

		@Override
		protected void init() {
			// not needed for these tests
		}

		void initMetricsForTest(final Config metricsConfig) {
			initMetrics(0, OpType.CREATE, 1, metricsConfig, new SizeInBytes(1L), false);
		}

		void setRawValue(final String path, final Object value) {
			final int sep = path.lastIndexOf('-');
			if (sep < 0) {
				config.mapVal(com.github.akurilov.confuse.Config.ROOT_PATH).put(path, value);
				return;
			}
			final String parentPath = path.substring(0, sep);
			final String leafKey = path.substring(sep + 1);
			config.mapVal(parentPath).put(leafKey, value);
		}

		Map<String, Object> latestMetadata() {
			return metricsContexts.get(metricsContexts.size() - 1).metadata();
		}

		boolean effectiveVerifyFlagForTest(
						final boolean verifyFlag, final boolean dedupable, final String stepId) {
			return effectiveVerifyFlag(verifyFlag, dedupable, stepId);
		}

		void startContextsForTest() {
			doStartWrapped();
		}

		void shutdownContextsForTest() {
			doShutdown();
		}

		boolean awaitContextsForTest(final long timeout, final TimeUnit unit) {
			return await(timeout, unit);
		}

		void setDurationAuditStateForTest(final long deadlineNanos) throws Exception {
			setLoadStepLocalField("durationAwaitDeadlineNanos", deadlineNanos);
			setLoadStepLocalField("durationIntervalArmed", true);
			setLoadStepLocalField("durationAwaitStatus", DurationAwaitStatus.RUNNING);
		}

		private void setLoadStepLocalField(final String name, final Object value) throws Exception {
			final Field field = LoadStepLocalBase.class.getDeclaredField(name);
			field.setAccessible(true);
			field.set(this, value);
		}

		int contextCount() {
			return stepContexts.size();
		}

		LoadStepContext contextAt(final int index) {
			return stepContexts.get(index);
		}
	}

}
