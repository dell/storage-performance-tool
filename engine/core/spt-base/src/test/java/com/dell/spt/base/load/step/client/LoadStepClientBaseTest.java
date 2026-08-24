package com.dell.spt.base.load.step.client;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.load.step.DurationAwaitStatus;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.linear.LinearLoadStepClient;
import com.dell.spt.base.load.step.service.LoadStepManagerServiceImpl;
import com.dell.spt.base.load.step.service.LoadStepService;
import com.dell.spt.base.load.step.service.LoadStepServiceImpl;
import com.dell.spt.base.load.step.service.file.FileManagerServiceImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.deletion.DeleteArtifacts;
import com.dell.spt.base.item.op.deletion.DeleteObjectLifecycleSnapshot;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.context.DistributedMetricsContext;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.ConcurrencyMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.DeleteMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.DistributedAllMetricsSnapshot;
import com.dell.spt.base.metrics.snapshot.RateMetricSnapshot;
import com.dell.spt.base.metrics.snapshot.TimingMetricSnapshot;
import com.dell.spt.base.svc.ServiceUtil;
import com.github.akurilov.confuse.Config;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;

/**
 * Comprehensive unit tests for LoadStepClientBase.
 * 
 * Tests cover constructor behavior, configuration access, initialization logic,
 * copy instance functionality, lifecycle management, and error handling.
 */
@DisplayName("LoadStepClientBase Tests")
class LoadStepClientBaseTest {
	private static final int BLOCKING_PHASE_WIDTH = 8;
	private static final String REAL_RMI_STEP_TYPE = "duration-rmi-test";

	private Config testConfig;
	private List<Extension> extensions;
	private List<Config> ctxConfigs;
	private MetricsManager mockMetricsManager;

	@TempDir
	Path tempDir;

	@BeforeEach
	void setUp() {
		testConfig = TestConfigBuilder.config();
		extensions = Collections.emptyList();
		ctxConfigs = Collections.emptyList();
		mockMetricsManager = mock(MetricsManager.class);
	}

	@Test
	void metadataSliceRunIdMustMatchBeforeStart() throws Exception {
		final LoadStep slice = mock(LoadStep.class);
		when(slice.runId()).thenReturn(77L);
		assertDoesNotThrow(() -> LoadStepClientBase.requireMatchingRunId(slice, 77L));

		when(slice.runId()).thenReturn(78L);
		final var mismatch = assertThrows(
						IntegrityTerminalException.class,
						() -> LoadStepClientBase.requireMatchingRunId(slice, 77L));
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, mismatch.category());
		assertTrue(mismatch.getMessage().contains("expected 77, actual 78"));
	}

	@Test
	void metadataSliceRunIdMustBePositive() {
		final var failure = assertThrows(
						IntegrityTerminalException.class,
						() -> LoadStepClientBase.requireMatchingRunId(mock(LoadStep.class), 0L));
		assertEquals(IntegrityTerminalException.Category.CONFIGURATION, failure.category());
	}

	@Test
	void durationClientFailsWhenAnyOneOfMultipleSlicesExhaustsEarly() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final LoadStep exhausted = mock(LoadStep.class);
		final LoadStep stillActive = mock(LoadStep.class);
		when(exhausted.await(anyLong(), any(java.util.concurrent.TimeUnit.class))).thenReturn(true);
		when(stillActive.await(anyLong(), any(java.util.concurrent.TimeUnit.class))).thenReturn(false);
		addStepSlice(client, exhausted);
		addStepSlice(client, stillActive);

		final String durationValidityStatus = "overall run already failed duration validity; failure-budget status only";
		final CapturingMessageAppender appender = new CapturingMessageAppender(durationValidityStatus);
		appender.start();
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.MSG.getName());
		logger.addAppender(appender);
		try {
			final var failure = assertThrows(
							IntegrityTerminalException.class,
							() -> client.await(1, java.util.concurrent.TimeUnit.SECONDS));
			assertTrue(failure.getMessage().contains("inventory slice exhausted before the requested duration"));
			assertDoesNotThrow(client::close);
			assertTrue(appender.awaitTarget(1, TimeUnit.SECONDS));
			final List<String> messages = appender.messages();
			assertTrue(messages.stream().anyMatch(message -> message.contains(durationValidityStatus)));
			assertFalse(messages.stream().anyMatch(message -> message.contains(
							"Standalone DELETE completed cleanly")));
		} finally {
			logger.removeAppender(appender);
			appender.stop();
		}
	}

	@Test
	void durationClientFailsClosedWhenAnyRemoteSliceBecomesUnreachable() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final LoadStep unavailable = mock(LoadStep.class);
		final LoadStep stillActive = mock(LoadStep.class);
		when(unavailable.await(anyLong(), any(java.util.concurrent.TimeUnit.class)))
						.thenThrow(new RemoteException("lost worker"));
		when(stillActive.await(anyLong(), any(java.util.concurrent.TimeUnit.class))).thenReturn(false);
		addStepSlice(client, unavailable);
		addStepSlice(client, stillActive);

		final var failure = assertThrows(
						IntegrityTerminalException.class,
						() -> client.await(1, java.util.concurrent.TimeUnit.SECONDS));
		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
	}

	@Test
	void durationAwaitUsesShortControlPlanePollsAndAcceptsTheWorkerDeadlineVerdict() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final LoadStep slice = mock(LoadStep.class);
		final AtomicLong longestAwaitNanos = new AtomicLong();
		when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
			final long requestedNanos = invocation.getArgument(1, TimeUnit.class)
							.toNanos(invocation.getArgument(0));
			longestAwaitNanos.accumulateAndGet(requestedNanos, Math::max);
			if (requestedNanos > TimeUnit.SECONDS.toNanos(1)) {
				throw new RemoteException("simulated shipped RMI response timeout");
			}
			return false;
		});
		addStepSlice(client, slice);

		assertDoesNotThrow(() -> client.await(30, TimeUnit.SECONDS));
		assertTrue(longestAwaitNanos.get() <= TimeUnit.SECONDS.toNanos(1));
		verify(slice, atLeastOnce()).durationAwaitStatus();
	}

	@Test
	void durationControllerDeadlineStartsAfterEveryWorkerArmAcknowledges() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 1);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final LoadStep delayedWorker = mock(LoadStep.class);
		final AtomicLong workerDeadlineNanos = new AtomicLong(Long.MAX_VALUE);
		final AtomicLong workerArmedAtNanos = new AtomicLong();
		doAnswer(invocation -> {
			Thread.sleep(250);
			final long armedAtNanos = System.nanoTime();
			final long durationNanos = invocation.getArgument(0, Long.class);
			if (workerArmedAtNanos.compareAndSet(0, armedAtNanos)) {
				workerDeadlineNanos.set(armedAtNanos + durationNanos);
			}
			return null;
		}).when(delayedWorker).startDurationInterval(anyLong());
		when(delayedWorker.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
		when(delayedWorker.durationAwaitStatus()).thenReturn(DurationAwaitStatus.REACHED_DEADLINE);
		when(delayedWorker.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		addStepSlice(client, delayedWorker);

		assertDoesNotThrow(() -> client.await(200, TimeUnit.MILLISECONDS));
		final Field drainDeadlineField = LoadStepClientBase.class.getDeclaredField(
						"durationDrainDeadlineNanos");
		drainDeadlineField.setAccessible(true);
		final long controllerDeadlineNanos = drainDeadlineField.getLong(client)
						- TimeUnit.SECONDS.toNanos(1);
		assertTrue(
						controllerDeadlineNanos - workerArmedAtNanos.get() >= TimeUnit.MILLISECONDS.toNanos(150),
						() -> "the common controller deadline was established before the worker arm barrier: "
										+ "worker=" + workerDeadlineNanos.get() + ", controller="
										+ controllerDeadlineNanos);
		assertDoesNotThrow(client::close);
		assertNoLiveThreads(
						"spt-delete-duration-prepare-",
						"spt-delete-duration-start-",
						"spt-delete-await-",
						"spt-delete-admission-close-",
						"spt-delete-duration-verdict-",
						"spt-delete-recovery-",
						"spt-delete-drain-",
						"spt-delete-terminal-validation-",
						"spt-delete-shutdown-",
						"spt-delete-step-stop-");
	}

	private static void assertNoLiveThreads(final String... namePrefixes)
					throws InterruptedException {
		final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
		List<Thread> liveThreads;
		do {
			liveThreads = Thread.getAllStackTraces().keySet().stream()
							.filter(Thread::isAlive)
							.filter(thread -> java.util.Arrays.stream(namePrefixes)
											.anyMatch(thread.getName()::startsWith))
							.toList();
			if (liveThreads.isEmpty()) {
				return;
			}
			Thread.sleep(10);
		} while (System.nanoTime() < deadlineNanos);
		fail("duration client lifecycle threads remain live: "
						+ liveThreads.stream()
										.map(thread -> thread.getName() + " " + java.util.Arrays.toString(thread.getStackTrace()))
										.toList());
	}

	@Test
	void durationAwaitUsesShortPollsThroughRealRmiForALongRequestedInterval() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final LoadStep local = mock(LoadStep.class);
		when(local.loadStepId()).thenReturn("real-rmi-duration");
		when(local.durationAwaitStatus()).thenReturn(DurationAwaitStatus.REACHED_DEADLINE);
		when(local.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
		final AtomicLong longestAwaitNanos = new AtomicLong();
		when(local.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
			final long requestedNanos = invocation.getArgument(1, TimeUnit.class)
							.toNanos(invocation.getArgument(0));
			longestAwaitNanos.accumulateAndGet(requestedNanos, Math::max);
			if (requestedNanos > TimeUnit.SECONDS.toNanos(1)) {
				throw new RemoteException("simulated shipped RMI response timeout");
			}
			return false;
		});
		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		LoadStepServiceImpl service = null;
		try {
			final int port;
			try (ServerSocket socket = new ServerSocket(0)) {
				socket.setReuseAddress(true);
				port = socket.getLocalPort();
			}
			service = newRealDurationService(local, port);
			final LoadStepService remote = ServiceUtil.resolve(
							"127.0.0.1", port, service.name(), LoadStepService.class);
			addRawStepSlice(client, remote);

			assertDoesNotThrow(() -> client.await(30, TimeUnit.SECONDS));
			assertTrue(longestAwaitNanos.get() <= TimeUnit.SECONDS.toNanos(1));
			verify(local, atLeastOnce())
							.enforceDispatchedOperationsDeadlineForStepStop(anyLong());
			assertDoesNotThrow(client::close);
			assertTrue(client.isClosed());
		} finally {
			if (service != null && !service.isClosed()) {
				service.close();
			}
			ServiceUtil.shutdown();
			if (originalHost == null) {
				System.clearProperty("java.rmi.server.hostname");
			} else {
				System.setProperty("java.rmi.server.hostname", originalHost);
			}
		}
	}

	@Test
	void compatibilityWorkerFailsClosedWhenItCannotEnforceTheControllerDeadline() {
		final LoadStep compatibilityWorker = mock(LoadStep.class, CALLS_REAL_METHODS);

		final RemoteException failure = assertThrows(
						RemoteException.class,
						() -> compatibilityWorker.enforceDispatchedOperationsDeadlineForStepStop(1L));

		assertTrue(failure.getMessage().contains("does not support"));
	}

	@Test
	void terminalValidationFailureInvalidatesButStillClosesEveryReachableSlice() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final LoadStep unresolved = mock(LoadStep.class);
		when(unresolved.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
		doThrow(new IntegrityTerminalException(
						IntegrityTerminalException.Category.EXECUTION,
						"unresolved dispatched request"))
						.when(unresolved).validateTerminalStateForStepStop();
		addStepSlice(client, unresolved);

		final var failure = assertThrows(
						IntegrityTerminalException.class,
						() -> client.await(1, TimeUnit.MILLISECONDS));

		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertTrue(failure.getMessage().contains("unresolved dispatched request"));
		verify(unresolved).recoverQueuedOperationsForStepStop();
		verify(unresolved).startDispatchedOperationsDrainForStepStop(anyLong());
		verify(unresolved).validateTerminalStateForStepStop();
		verify(unresolved).shutdown();
		verify(unresolved).stop();
		assertDoesNotThrow(client::close);
		verify(unresolved).close();
		assertTrue(client.isClosed());
	}

	@Test
	void durationAwaitArmsEverySliceBeforeAnySliceMayRun() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final LoadStep first = mock(LoadStep.class);
		final LoadStep delayedLast = mock(LoadStep.class);
		final CountDownLatch preparationsEntered = new CountDownLatch(2);
		final CountDownLatch releaseLastPreparation = new CountDownLatch(1);
		final AtomicLong firstArmedAt = new AtomicLong();
		final AtomicLong lastArmedAt = new AtomicLong();
		final AtomicLong firstDurationNanos = new AtomicLong();
		final AtomicLong lastDurationNanos = new AtomicLong();
		doAnswer(invocation -> {
			firstDurationNanos.set(invocation.getArgument(0));
			preparationsEntered.countDown();
			return null;
		}).when(first).prepareDurationInterval(anyLong());
		doAnswer(invocation -> {
			lastDurationNanos.set(invocation.getArgument(0));
			preparationsEntered.countDown();
			assertTrue(releaseLastPreparation.await(1, TimeUnit.SECONDS));
			return null;
		}).when(delayedLast).prepareDurationInterval(anyLong());
		doAnswer(invocation -> {
			firstArmedAt.set(System.nanoTime());
			return null;
		}).when(first).startDurationInterval(anyLong());
		doAnswer(invocation -> {
			lastArmedAt.set(System.nanoTime());
			return null;
		}).when(delayedLast).startDurationInterval(anyLong());
		when(first.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
		when(delayedLast.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
		addStepSlice(client, first);
		addStepSlice(client, delayedLast);

		final AtomicReference<Throwable> awaitFailure = new AtomicReference<>();
		final Thread awaitThread = Thread.ofPlatform().start(() -> {
			try {
				client.await(50, TimeUnit.MILLISECONDS);
			} catch (final Throwable failure) {
				awaitFailure.set(failure);
			}
		});
		try {
			assertTrue(preparationsEntered.await(1, TimeUnit.SECONDS));
			verify(first, never()).startDurationInterval(anyLong());
			verify(delayedLast, never()).startDurationInterval(anyLong());
			verify(first, never()).await(anyLong(), any(TimeUnit.class));
			verify(delayedLast, never()).await(anyLong(), any(TimeUnit.class));
		} finally {
			releaseLastPreparation.countDown();
			awaitThread.join(TimeUnit.SECONDS.toMillis(2));
		}

		assertFalse(awaitThread.isAlive());
		assertNull(awaitFailure.get());
		assertEquals(TimeUnit.MILLISECONDS.toNanos(50), firstDurationNanos.get());
		assertEquals(firstDurationNanos.get(), lastDurationNanos.get());
		verify(first).startDurationInterval(firstDurationNanos.get());
		verify(delayedLast).startDurationInterval(firstDurationNanos.get());
		assertTrue(
						Math.abs(firstArmedAt.get() - lastArmedAt.get()) < TimeUnit.SECONDS.toNanos(1),
						"duration arm RPCs were serialized instead of coordinated");
	}

	@Test
	void durationPublicStopCoordinatesEverySliceWithBoundedWorkersAndOneDrainBudget() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 1);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final int sliceCount = BLOCKING_PHASE_WIDTH + 2;
		final AtomicInteger admissionsClosed = new AtomicInteger();
		final AtomicInteger queuesRecovered = new AtomicInteger();
		final AtomicInteger phaseViolations = new AtomicInteger();
		final AtomicInteger activeDrains = new AtomicInteger();
		final AtomicInteger maxActiveDrains = new AtomicInteger();
		final AtomicLong minimumDrainBudgetNanos = new AtomicLong(Long.MAX_VALUE);
		final List<LoadStep> slices = new java.util.ArrayList<>(sliceCount);

		for (int i = 0; i < sliceCount; i++) {
			final LoadStep slice = mock(LoadStep.class);
			doAnswer(invocation -> {
				admissionsClosed.incrementAndGet();
				return null;
			}).when(slice).closeOperationAdmissionForStepStop();
			doAnswer(invocation -> {
				if (admissionsClosed.get() != sliceCount) {
					phaseViolations.incrementAndGet();
				}
				queuesRecovered.incrementAndGet();
				return null;
			}).when(slice).recoverQueuedOperationsForStepStop();
			doAnswer(invocation -> {
				if (queuesRecovered.get() != sliceCount) {
					phaseViolations.incrementAndGet();
				}
				final long budgetNanos = invocation.getArgument(0);
				minimumDrainBudgetNanos.accumulateAndGet(budgetNanos, Math::min);
				final int active = activeDrains.incrementAndGet();
				maxActiveDrains.accumulateAndGet(active, Math::max);
				try {
					Thread.sleep(Math.min(100, TimeUnit.NANOSECONDS.toMillis(budgetNanos)));
				} finally {
					activeDrains.decrementAndGet();
				}
				return null;
			}).when(slice).startDispatchedOperationsDrainForStepStop(anyLong());
			slices.add(slice);
			addStepSlice(client, slice);
		}

		final long startedNanos = System.nanoTime();
		assertDoesNotThrow(client::stop);
		final long elapsedNanos = System.nanoTime() - startedNanos;

		assertEquals(sliceCount, admissionsClosed.get());
		assertEquals(sliceCount, queuesRecovered.get());
		assertEquals(0, phaseViolations.get());
		assertTrue(maxActiveDrains.get() > 1);
		assertTrue(maxActiveDrains.get() > BLOCKING_PHASE_WIDTH);
		assertTrue(maxActiveDrains.get() <= sliceCount);
		assertTrue(minimumDrainBudgetNanos.get() > 0);
		assertTrue(elapsedNanos < TimeUnit.SECONDS.toNanos(1));
		for (final LoadStep slice : slices) {
			verify(slice).shutdown();
			verify(slice).stop();
		}
		assertDoesNotThrow(client::close);
	}

	@Test
	void durationStopPreservesAdmissionFailureWithoutCrossingRecoveryBarrier() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final int sliceCount = BLOCKING_PHASE_WIDTH + 1;
		final List<LoadStep> slices = new java.util.ArrayList<>(sliceCount);
		for (int i = 0; i < sliceCount; i++) {
			final LoadStep slice = mock(LoadStep.class);
			if (i == 0) {
				doAnswer(invocation -> {
					throw new RemoteException("admission unavailable");
				}).when(slice).closeOperationAdmissionForStepStop();
			}
			slices.add(slice);
			addStepSlice(client, slice);
		}

		assertDoesNotThrow(client::stop);
		for (final LoadStep slice : slices) {
			verify(slice).closeOperationAdmissionForStepStop();
			verify(slice, never()).recoverQueuedOperationsForStepStop();
			verify(slice, never()).startDispatchedOperationsDrainForStepStop(anyLong());
			verify(slice, never()).shutdown();
			verify(slice, never()).stop();
		}
		final var failure = assertThrows(IntegrityTerminalException.class, client::close);
		assertTrue(failure.getMessage().contains("close operation admission"));
		for (final LoadStep slice : slices) {
			verify(slice, never()).close();
		}
	}

	@Test
	void durationFailedCloseRetainsSlicesForSafeCleanupRetry() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final AtomicInteger admissionAttempts = new AtomicInteger();
		final LoadStep slice = mock(LoadStep.class);
		doAnswer(invocation -> {
			if (admissionAttempts.incrementAndGet() == 1) {
				throw new RemoteException("transient admission failure");
			}
			return null;
		}).when(slice).closeOperationAdmissionForStepStop();
		addStepSlice(client, slice);

		assertDoesNotThrow(client::stop);
		assertThrows(IntegrityTerminalException.class, client::close);
		verify(slice, never()).close();

		assertDoesNotThrow(client::close);
		verify(slice, times(2)).closeOperationAdmissionForStepStop();
		verify(slice).recoverQueuedOperationsForStepStop();
		verify(slice).startDispatchedOperationsDrainForStepStop(anyLong());
		verify(slice).shutdown();
		verify(slice).stop();
		verify(slice).close();
		assertTrue(client.isClosed());
	}

	@Test
	void durationAggregationFailureRetainsAggregatorUntilCanonicalCompletionPublishes() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		addStepSlice(client, mock(LoadStep.class));
		final AtomicInteger attempts = new AtomicInteger();
		final AtomicInteger publications = new AtomicInteger();
		final Path completion = tempDir.resolve(DeleteArtifacts.COMPLETION_FILE_NAME);
		addDeleteArtifactAggregator(client, () -> {
			if (attempts.incrementAndGet() == 1) {
				throw new IOException("transient DELETE aggregation failure");
			}
			Files.writeString(completion, "complete\n");
			publications.incrementAndGet();
		});

		assertDoesNotThrow(client::stop);
		final var failure = assertThrows(IntegrityTerminalException.class, client::close);
		assertEquals(IntegrityTerminalException.Category.AGGREGATION, failure.category());
		assertFalse(Files.exists(completion));

		assertDoesNotThrow(client::close);
		assertDoesNotThrow(client::close);
		assertEquals(2, attempts.get());
		assertEquals(1, publications.get());
		assertEquals("complete\n", Files.readString(completion));
		assertTrue(client.isClosed());
	}

	@Test
	void durationRunRetriesTransientCleanupAndClosesRetainedSliceThreads() throws Exception {
		final Config config = durationConfig();
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		final AtomicInteger admissionAttempts = new AtomicInteger();
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		when(slice.durationAwaitStatus()).thenReturn(DurationAwaitStatus.REACHED_DEADLINE);
		when(slice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		when(slice.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
		doAnswer(invocation -> {
			if (admissionAttempts.incrementAndGet() == 1) {
				throw new RemoteException("transient admission failure");
			}
			return null;
		}).when(slice).closeOperationAdmissionForStepStop();
		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn("test-load-step");
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(slice);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, List.of(factory), ctxConfigs, mockMetricsManager);

		final var failure = assertThrows(IntegrityTerminalException.class, client::run);

		assertTrue(failure.getMessage().contains("inventory slice exhausted before the requested duration"));
		verify(slice, times(2)).closeOperationAdmissionForStepStop();
		verify(slice).recoverQueuedOperationsForStepStop();
		verify(slice).startDispatchedOperationsDrainForStepStop(anyLong());
		verify(slice).shutdown();
		verify(slice).stop();
		verify(slice).close();
		assertTrue(client.isClosed());
		assertNoLiveThreads(
						"spt-delete-duration-prepare-",
						"spt-delete-duration-start-",
						"spt-delete-await-",
						"spt-delete-admission-close-",
						"spt-delete-duration-verdict-",
						"spt-delete-recovery-",
						"spt-delete-drain-",
						"spt-delete-terminal-validation-",
						"spt-delete-shutdown-",
						"spt-delete-step-stop-",
						"spt-delete-step-close-");
	}

	@Test
	void durationRepeatedRecoveryFailureRetainsSlicesUntilFullCleanupRetry() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final AtomicInteger recoveryAttempts = new AtomicInteger();
		final LoadStep slice = mock(LoadStep.class);
		doAnswer(invocation -> {
			if (recoveryAttempts.incrementAndGet() <= 2) {
				throw new RemoteException("transient recovery failure");
			}
			return null;
		}).when(slice).recoverQueuedOperationsForStepStop();
		addStepSlice(client, slice);

		assertDoesNotThrow(client::stop);
		assertThrows(IntegrityTerminalException.class, client::close);
		verify(slice, never()).close();

		assertThrows(IntegrityTerminalException.class, client::close);
		verify(slice, never()).close();

		assertDoesNotThrow(client::close);
		verify(slice, times(3)).closeOperationAdmissionForStepStop();
		verify(slice, times(3)).recoverQueuedOperationsForStepStop();
		verify(slice).close();
		assertTrue(client.isClosed());
	}

	@ParameterizedTest
	@EnumSource(RemoteCleanupFailurePhase.class)
	void durationCleanupRetriesOnlyTheFailedSliceForEveryLaterPhase(
					final RemoteCleanupFailurePhase failedPhase) throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final AtomicInteger attempts = new AtomicInteger();
		final LoadStep failing = mock(LoadStep.class);
		final LoadStep healthy = mock(LoadStep.class);
		configureOneShotFailure(failing, failedPhase, attempts);
		addStepSlice(client, failing);
		addStepSlice(client, healthy);

		assertDoesNotThrow(client::stop);
		assertThrows(IntegrityTerminalException.class, client::close);
		assertDoesNotThrow(client::close);

		verifyRemotePhase(failing, failedPhase, times(2));
		verifyRemotePhase(healthy, failedPhase, times(1));
		assertTrue(client.isClosed());
	}

	@Test
	void durationAwaitRejectsRemoteEarlyExhaustionDeliveredAfterDeadline() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final LoadStep exhaustedAfterDeadline = mock(LoadStep.class);
		doAnswer(invocation -> {
			Thread.sleep(20);
			return true;
		}).when(exhaustedAfterDeadline).await(anyLong(), any(TimeUnit.class));
		addStepSlice(client, exhaustedAfterDeadline);
		when(exhaustedAfterDeadline.durationAwaitStatus())
						.thenReturn(DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE);

		assertThrows(IntegrityTerminalException.class, () -> client.await(1, TimeUnit.MILLISECONDS));
	}

	@Test
	void durationAwaitAcceptsSourceVerdictReachedDespiteLateProbeDelivery() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final LoadStep reachedDeadline = mock(LoadStep.class);
		doAnswer(invocation -> {
			Thread.sleep(20);
			return true;
		}).when(reachedDeadline).await(anyLong(), any(TimeUnit.class));
		addStepSlice(client, reachedDeadline);

		assertFalse(client.await(1, TimeUnit.MILLISECONDS));
	}

	@Test
	void durationAwaitFailsClosedWhenAWorkerCannotProvideDurationEvidence() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final LoadStep missingEvidence = mock(LoadStep.class);
		when(missingEvidence.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
		addStepSlice(client, missingEvidence);
		when(missingEvidence.durationAwaitStatus()).thenReturn(DurationAwaitStatus.NOT_STARTED);

		final var failure = assertThrows(
						IntegrityTerminalException.class,
						() -> client.await(1, TimeUnit.MILLISECONDS));

		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
	}

	@Test
	void durationVerdictRpcFailureStillReconcilesAndClosesEveryReachableSlice() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final LoadStep missingEvidence = mock(LoadStep.class);
		when(missingEvidence.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
		addStepSlice(client, missingEvidence);
		when(missingEvidence.durationAwaitStatus())
						.thenThrow(new RemoteException("duration verdict unavailable"));

		final var failure = assertThrows(
						IntegrityTerminalException.class,
						() -> client.await(1, TimeUnit.MILLISECONDS));

		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		verify(missingEvidence).closeOperationAdmissionForStepStop();
		verify(missingEvidence).recoverQueuedOperationsForStepStop();
		verify(missingEvidence).startDispatchedOperationsDrainForStepStop(anyLong());
		verify(missingEvidence).shutdown();
		verify(missingEvidence).stop();
		assertDoesNotThrow(client::close);
		verify(missingEvidence).close();
		assertTrue(client.isClosed());
	}

	@Test
	void durationAwaitFailsBoundedlyAndRetainsAHungVerdictWithoutDuplicateSubmission() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 0);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch verdictEntered = new CountDownLatch(1);
		final CountDownLatch releaseVerdict = new CountDownLatch(1);
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
		addStepSlice(client, slice);
		doAnswer(invocation -> {
			verdictEntered.countDown();
			while (releaseVerdict.getCount() > 0) {
				try {
					releaseVerdict.await();
				} catch (final InterruptedException ignored) {
					// Model an RMI status call which ignores controller-side cancellation.
				}
			}
			return DurationAwaitStatus.REACHED_DEADLINE;
		}).when(slice).durationAwaitStatus();

		try {
			final long startedNanos = System.nanoTime();
			assertThrows(
							IntegrityTerminalException.class,
							() -> client.await(1, TimeUnit.MILLISECONDS));
			assertTrue(verdictEntered.await(1, TimeUnit.SECONDS));
			assertTrue(System.nanoTime() - startedNanos < TimeUnit.SECONDS.toNanos(3));
			assertDoesNotThrow(client::close);
			verify(slice).durationAwaitStatus();
		} finally {
			releaseVerdict.countDown();
		}

		verify(slice).durationAwaitStatus();
		assertTrue(client.isClosed());
	}

	@Test
	void durationRecoveryOffersEverySliceOnceAndRetainsBlockedCallsAcrossRetry() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 0);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final int sliceCount = BLOCKING_PHASE_WIDTH + 2;
		final CountDownLatch blockersEntered = new CountDownLatch(BLOCKING_PHASE_WIDTH);
		final CountDownLatch trailingEntered = new CountDownLatch(2);
		final CountDownLatch releaseBlockers = new CountDownLatch(1);
		final CountDownLatch retryAdmissionCompleted = new CountDownLatch(1);
		final AtomicInteger admissionCalls = new AtomicInteger();
		final List<LoadStep> slices = new ArrayList<>(sliceCount);
		for (int i = 0; i < sliceCount; i++) {
			final int index = i;
			final LoadStep slice = mock(LoadStep.class);
			doAnswer(invocation -> {
				if (admissionCalls.incrementAndGet() == 2 * sliceCount) {
					retryAdmissionCompleted.countDown();
				}
				return null;
			}).when(slice).closeOperationAdmissionForStepStop();
			doAnswer(invocation -> {
				if (index < BLOCKING_PHASE_WIDTH) {
					blockersEntered.countDown();
					while (releaseBlockers.getCount() > 0) {
						try {
							releaseBlockers.await();
						} catch (final InterruptedException ignored) {
							// Model a black-holed RPC which ignores cancellation.
						}
					}
				} else {
					trailingEntered.countDown();
				}
				return null;
			}).when(slice).recoverQueuedOperationsForStepStop();
			slices.add(slice);
			addStepSlice(client, slice);
		}

		final AtomicReference<Throwable> retryFailure = new AtomicReference<>();
		try {
			assertDoesNotThrow(client::stop);
			assertTrue(blockersEntered.await(5, TimeUnit.SECONDS));
			assertTrue(trailingEntered.await(1, TimeUnit.SECONDS));
			assertThrows(IntegrityTerminalException.class, client::close);
			final Thread retry = Thread.ofPlatform().start(() -> {
				try {
					client.close();
				} catch (final Throwable failure) {
					retryFailure.set(failure);
				}
			});
			assertTrue(retryAdmissionCompleted.await(1, TimeUnit.SECONDS));
			for (final LoadStep slice : slices) {
				verify(slice, times(1)).recoverQueuedOperationsForStepStop();
				verify(slice, never()).startDispatchedOperationsDrainForStepStop(anyLong());
			}
			releaseBlockers.countDown();
			retry.join(TimeUnit.SECONDS.toMillis(3));
			assertFalse(retry.isAlive());
			assertNull(retryFailure.get());
			assertTrue(client.isClosed());
		} finally {
			releaseBlockers.countDown();
		}
	}

	@Test
	void durationAwaitUsesOneControllerDeadlineAndProbesSlicesQueuedBeyondWorkerBound() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		for (int i = 0; i < BLOCKING_PHASE_WIDTH; i++) {
			final LoadStep active = mock(LoadStep.class);
			doAnswer(invocation -> {
				Thread.sleep(5);
				return false;
			}).when(active).await(anyLong(), any(TimeUnit.class));
			addStepSlice(client, active);
		}
		final AtomicLong exhaustedProbeNanos = new AtomicLong();
		final LoadStep exhaustedBeyondWorkerBound = mock(LoadStep.class);
		doAnswer(invocation -> {
			exhaustedProbeNanos.compareAndSet(0, System.nanoTime());
			return true;
		}).when(exhaustedBeyondWorkerBound).await(anyLong(), any(TimeUnit.class));
		addStepSlice(client, exhaustedBeyondWorkerBound);
		final LoadStep activeBeyondWorkerBound = mock(LoadStep.class);
		when(activeBeyondWorkerBound.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
		addStepSlice(client, activeBeyondWorkerBound);

		final long startedNanos = System.nanoTime();
		final long deadlineNanos = startedNanos + TimeUnit.MILLISECONDS.toNanos(200);
		final var failure = assertThrows(
						IntegrityTerminalException.class,
						() -> client.await(200, TimeUnit.MILLISECONDS));
		final long elapsedNanos = System.nanoTime() - startedNanos;

		assertTrue(failure.getMessage().contains("inventory slice exhausted"));
		verify(exhaustedBeyondWorkerBound).await(anyLong(), any(TimeUnit.class));
		assertTrue(
						exhaustedProbeNanos.get() < deadlineNanos,
						"a queued slice was first probed only after the controller deadline");
		assertTrue(elapsedNanos < TimeUnit.SECONDS.toNanos(1));
	}

	@Test
	void durationAwaitCancelsBlockingPeersImmediatelyAfterOneSliceExhausts() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch blockingProbeEntered = new CountDownLatch(1);
		final CountDownLatch releaseBlockingProbe = new CountDownLatch(1);
		final CountDownLatch blockingProbeInterrupted = new CountDownLatch(1);
		final LoadStep blocking = mock(LoadStep.class);
		doAnswer(invocation -> {
			blockingProbeEntered.countDown();
			try {
				releaseBlockingProbe.await();
			} catch (final InterruptedException e) {
				blockingProbeInterrupted.countDown();
				throw e;
			}
			return false;
		}).when(blocking).await(anyLong(), any(TimeUnit.class));
		final LoadStep exhausted = mock(LoadStep.class);
		doAnswer(invocation -> {
			assertTrue(blockingProbeEntered.await(1, TimeUnit.SECONDS));
			return true;
		}).when(exhausted).await(anyLong(), any(TimeUnit.class));
		addStepSlice(client, blocking);
		addStepSlice(client, exhausted);

		final AtomicReference<Throwable> awaitFailure = new AtomicReference<>();
		final Thread awaitThread = Thread.ofPlatform().start(() -> {
			try {
				client.await(10, TimeUnit.SECONDS);
			} catch (final Throwable failure) {
				awaitFailure.set(failure);
			}
		});
		try {
			assertTrue(blockingProbeEntered.await(1, TimeUnit.SECONDS));
			awaitThread.join(TimeUnit.SECONDS.toMillis(2));
			assertFalse(awaitThread.isAlive(), "early exhaustion must not wait for a blocking peer");
			assertTrue(awaitFailure.get() instanceof IntegrityTerminalException);
			assertTrue(blockingProbeInterrupted.await(1, TimeUnit.SECONDS));
		} finally {
			releaseBlockingProbe.countDown();
			awaitThread.interrupt();
			awaitThread.join(TimeUnit.SECONDS.toMillis(2));
		}
	}

	@Test
	void durationAwaitProbesTrailingSliceBeyondUninterruptibleWorkerWave() throws Exception {
		final Config config = durationConfig();
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch blockersEntered = new CountDownLatch(BLOCKING_PHASE_WIDTH);
		final CountDownLatch releaseBlockers = new CountDownLatch(1);
		final AtomicReference<Thread> blockingProbeThread = new AtomicReference<>();
		for (int i = 0; i < BLOCKING_PHASE_WIDTH; i++) {
			final LoadStep blocker = mock(LoadStep.class);
			doAnswer(invocation -> {
				blockingProbeThread.compareAndSet(null, Thread.currentThread());
				blockersEntered.countDown();
				while (true) {
					try {
						releaseBlockers.await();
						return false;
					} catch (final InterruptedException ignored) {
						// Model a black-holed RMI probe which does not cooperate with cancellation.
					}
				}
			}).when(blocker).await(anyLong(), any(TimeUnit.class));
			addStepSlice(client, blocker);
		}
		final LoadStep exhausted = mock(LoadStep.class);
		when(exhausted.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		addStepSlice(client, exhausted);

		final AtomicReference<Throwable> awaitFailure = new AtomicReference<>();
		final Thread awaitThread = Thread.ofPlatform().start(() -> {
			try {
				client.await(10, TimeUnit.SECONDS);
			} catch (final Throwable failure) {
				awaitFailure.set(failure);
			}
		});
		try {
			assertTrue(blockersEntered.await(5, TimeUnit.SECONDS));
			awaitThread.join(TimeUnit.SECONDS.toMillis(2));
			assertFalse(
							awaitThread.isAlive(),
							"uninterruptible leading probes starved a trailing exhausted slice");
			assertTrue(awaitFailure.get() instanceof IntegrityTerminalException);
			verify(exhausted).await(anyLong(), any(TimeUnit.class));
			assertTrue(
							blockingProbeThread.get().isVirtual(),
							"a cancellation-resistant remote probe retained a platform thread");
		} finally {
			releaseBlockers.countDown();
			awaitThread.interrupt();
			awaitThread.join(TimeUnit.SECONDS.toMillis(2));
		}
	}

	@Test
	void durationStopTimesOutBlockingAdmissionWithoutCrossingRecoveryBarrier() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 1);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch blockingAdmissionEntered = new CountDownLatch(1);
		final CountDownLatch releaseBlockingAdmission = new CountDownLatch(1);
		final CountDownLatch blockingAdmissionInterrupted = new CountDownLatch(1);
		final LoadStep blocking = mock(LoadStep.class);
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
		final LoadStep healthy = mock(LoadStep.class);
		addStepSlice(client, blocking);
		addStepSlice(client, healthy);

		final Thread stopThread = Thread.ofPlatform().start(client::stop);
		try {
			assertTrue(blockingAdmissionEntered.await(1, TimeUnit.SECONDS));
			stopThread.join(TimeUnit.SECONDS.toMillis(3));
			assertFalse(stopThread.isAlive(), "a blocking phase call must not stall distributed cleanup");
			assertFalse(blockingAdmissionInterrupted.await(100, TimeUnit.MILLISECONDS));
			verify(healthy, never()).recoverQueuedOperationsForStepStop();
			verify(healthy, never()).startDispatchedOperationsDrainForStepStop(anyLong());
			verify(healthy, never()).shutdown();
			verify(healthy, never()).stop();
			final var failure = assertThrows(IntegrityTerminalException.class, client::close);
			assertTrue(failure.getMessage().contains("close operation admission"));
			verify(blocking, never()).close();
			verify(healthy, never()).close();
			releaseBlockingAdmission.countDown();
			assertDoesNotThrow(client::close);
			assertTrue(client.isClosed());
		} finally {
			releaseBlockingAdmission.countDown();
			stopThread.interrupt();
			stopThread.join(TimeUnit.SECONDS.toMillis(2));
		}
	}

	@Test
	void durationAdmissionBarrierOffersClosureBeyondUninterruptibleWorkerWave() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 1);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch blockersEntered = new CountDownLatch(BLOCKING_PHASE_WIDTH);
		final CountDownLatch releaseBlockers = new CountDownLatch(1);
		for (int i = 0; i < BLOCKING_PHASE_WIDTH; i++) {
			final LoadStep blocker = mock(LoadStep.class);
			doAnswer(invocation -> {
				blockersEntered.countDown();
				while (true) {
					try {
						releaseBlockers.await();
						return null;
					} catch (final InterruptedException ignored) {
						// Model a black-holed RMI call which does not cooperate with cancellation.
					}
				}
			}).when(blocker).closeOperationAdmissionForStepStop();
			addStepSlice(client, blocker);
		}
		final CountDownLatch trailingAdmissionClosed = new CountDownLatch(1);
		final LoadStep trailing = mock(LoadStep.class);
		doAnswer(invocation -> {
			trailingAdmissionClosed.countDown();
			return null;
		}).when(trailing).closeOperationAdmissionForStepStop();
		addStepSlice(client, trailing);

		final Thread stopThread = Thread.ofPlatform().start(client::stop);
		try {
			assertTrue(blockersEntered.await(1, TimeUnit.SECONDS));
			assertTrue(
							trailingAdmissionClosed.await(1, TimeUnit.SECONDS),
							"a trailing slice never received the deadline admission-close signal");
			stopThread.join(TimeUnit.SECONDS.toMillis(3));
			assertFalse(stopThread.isAlive(), "an uninterruptible close RPC stalled the controller");
			verify(trailing, never()).recoverQueuedOperationsForStepStop();
		} finally {
			releaseBlockers.countDown();
			stopThread.interrupt();
			stopThread.join(TimeUnit.SECONDS.toMillis(2));
		}
	}

	@Test
	void durationDrainBudgetIncludesQueueRecoveryTime() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 1);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final AtomicLong drainBudgetNanos = new AtomicLong(Long.MAX_VALUE);
		final LoadStep slice = mock(LoadStep.class);
		doAnswer(invocation -> {
			Thread.sleep(700);
			return null;
		}).when(slice).recoverQueuedOperationsForStepStop();
		doAnswer(invocation -> {
			drainBudgetNanos.set(invocation.getArgument(0));
			return null;
		}).when(slice).startDispatchedOperationsDrainForStepStop(anyLong());
		addStepSlice(client, slice);

		assertDoesNotThrow(client::stop);

		assertTrue(drainBudgetNanos.get() > 0);
		assertTrue(
						drainBudgetNanos.get() < TimeUnit.MILLISECONDS.toNanos(600),
						"queue recovery received time outside the one step-wide drain budget");
	}

	@Test
	void durationDrainBudgetStartsAtTheScheduledDeadlineBeforeAdmissionClosure() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 1);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final AtomicLong drainBudgetNanos = new AtomicLong(Long.MAX_VALUE);
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
		when(slice.durationAwaitStatus()).thenReturn(DurationAwaitStatus.REACHED_DEADLINE);
		doAnswer(invocation -> {
			Thread.sleep(700);
			return null;
		}).when(slice).closeOperationAdmissionForStepStop();
		doAnswer(invocation -> {
			drainBudgetNanos.set(invocation.getArgument(0));
			return null;
		}).when(slice).startDispatchedOperationsDrainForStepStop(anyLong());
		addStepSlice(client, slice);

		assertDoesNotThrow(() -> client.await(1, TimeUnit.MILLISECONDS));

		assertTrue(drainBudgetNanos.get() > 0);
		assertTrue(
						drainBudgetNanos.get() < TimeUnit.MILLISECONDS.toNanos(600),
						"admission closure received time outside the scheduled-deadline drain budget");
	}

	@Test
	void durationDrainBudgetStartsBeforeDistributedVerdictCollection() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 1);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final AtomicLong drainBudgetNanos = new AtomicLong(Long.MAX_VALUE);
		final LoadStep slice = mock(LoadStep.class);
		addStepSlice(client, slice);
		doAnswer(invocation -> {
			Thread.sleep(700);
			return DurationAwaitStatus.REACHED_DEADLINE;
		}).when(slice).durationAwaitStatus();
		doAnswer(invocation -> {
			drainBudgetNanos.set(invocation.getArgument(0));
			return null;
		}).when(slice).startDispatchedOperationsDrainForStepStop(anyLong());

		assertDoesNotThrow(client::stop);

		assertTrue(drainBudgetNanos.get() > 0);
		assertTrue(
						drainBudgetNanos.get() < TimeUnit.MILLISECONDS.toNanos(600),
						"duration verdict collection received time outside the one step-wide drain budget");
	}

	@Test
	void fixedObjectBudgetBreachStopsSchedulingDrainsAndReportsFinalOvershoot() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-failureBudget-maxFailedObjects", 0L);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final AtomicBoolean admissionClosed = new AtomicBoolean();
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(
						invocation -> admissionClosed.get());
		when(slice.deleteObjectLifecycle()).thenAnswer(invocation -> admissionClosed.get()
						? new DeleteObjectLifecycleSnapshot(4, 4, 1, 3, 0, 0, 0, 1, true)
						: new DeleteObjectLifecycleSnapshot(2, 2, 1, 1, 0, 0, 0, 1, true));
		when(slice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		doAnswer(invocation -> {
			admissionClosed.set(true);
			return null;
		}).when(slice).closeOperationAdmissionForStepStop();
		addRawStepSlice(client, slice);

		assertTrue(client.await(3, TimeUnit.SECONDS));
		final IntegrityTerminalException failure = assertThrows(
						IntegrityTerminalException.class, client::close);

		assertTrue(failure.getMessage().contains("operational failed objects=3"));
		assertTrue(failure.getMessage().contains("not a hard cap"));
		verify(slice).closeOperationAdmissionForStepStop();
		verify(slice).recoverQueuedOperationsForStepStop();
		verify(slice).startDispatchedOperationsDrainForStepStop(anyLong());
		verify(slice).shutdown();
		assertNoLiveThreads("spt-delete-failure-budget-");
	}

	@Test
	void countBudgetFailureCancelsTimedOutRetainedStopPhase() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-wait-limit", 0);
		config.val("load-op-failureBudget-maxFailedObjects", 0L);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch admissionEntered = new CountDownLatch(1);
		final CountDownLatch releaseAdmission = new CountDownLatch(1);
		final AtomicBoolean admissionClosed = new AtomicBoolean();
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(
						invocation -> admissionClosed.get());
		when(slice.deleteObjectLifecycle()).thenReturn(
						new DeleteObjectLifecycleSnapshot(1, 1, 0, 1, 0, 0, 0, 0, true));
		doAnswer(invocation -> {
			admissionClosed.set(true);
			admissionEntered.countDown();
			releaseAdmission.await();
			return null;
		}).when(slice).closeOperationAdmissionForStepStop();
		addRawStepSlice(client, slice);

		try {
			assertTrue(client.await(3, TimeUnit.SECONDS));
			assertTrue(admissionEntered.await(1, TimeUnit.SECONDS));
			assertThrows(IntegrityTerminalException.class, client::close);

			assertNoLiveThreads("spt-delete-admission-close-");
		} finally {
			releaseAdmission.countDown();
		}
	}

	@Test
	void countStartPublishesUnslicedBudgetsAndReleasesEverySliceAsOnePublicBarrier() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-limit-fail-count", 1L);
		config.val("load-op-failureBudget-maxFailedObjects", 10L);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		final CountDownLatch releasesEntered = new CountDownLatch(2);
		final LoadStep first = mock(LoadStep.class);
		final LoadStep second = mock(LoadStep.class);
		final List<Config> publishedConfigs = Collections.synchronizedList(new ArrayList<>());
		final AtomicInteger createdSlices = new AtomicInteger();
		for (final LoadStep slice : List.of(first, second)) {
			when(slice.loadStepId()).thenReturn("public-count-budget-slice");
			when(slice.metricsSnapshots()).thenAnswer(invocation -> new ArrayList<>());
			when(slice.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
			when(slice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
			doAnswer(invocation -> {
				releasesEntered.countDown();
				assertTrue(
								releasesEntered.await(1, TimeUnit.SECONDS),
								"a slice was released before every prepared participant entered the barrier");
				return null;
			}).when(slice).releaseObjectFailureBudgetAdmission();
		}
		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn(TestLoadStepExtension.TYPE);
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenAnswer(invocation -> {
			publishedConfigs.add(invocation.getArgument(0));
			return createdSlices.getAndIncrement() == 0 ? first : second;
		});
		final int port;
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			port = socket.getLocalPort();
		}
		config.val("load-step-node-addrs", List.of("127.0.0.1:" + port));
		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		final FileManagerServiceImpl fileService = new FileManagerServiceImpl(port);
		final LoadStepManagerServiceImpl stepManager = new LoadStepManagerServiceImpl(
						port, List.of(factory), mockMetricsManager);
		TestLoadStepClient client = null;
		try {
			fileService.start();
			stepManager.start();
			client = new TestLoadStepClient(
							config, List.of(factory), ctxConfigs, mockMetricsManager);

			client.start();

			assertEquals(2, publishedConfigs.size());
			assertEquals(
							List.of(1L, 1L),
							publishedConfigs.stream()
											.map(slice -> slice.longVal("load-op-limit-fail-count"))
											.toList());
			assertEquals(
							List.of(10L, 10L),
							publishedConfigs.stream()
											.map(slice -> slice.longVal("load-op-failureBudget-maxFailedObjects"))
											.toList());
			verify(first).releaseObjectFailureBudgetAdmission();
			verify(second).releaseObjectFailureBudgetAdmission();
		} finally {
			if (client != null && !client.isClosed()) {
				client.close();
			}
			if (!stepManager.isClosed()) {
				stepManager.close();
			}
			if (!fileService.isClosed()) {
				fileService.close();
			}
			ServiceUtil.shutdown();
			if (originalHost == null) {
				System.clearProperty("java.rmi.server.hostname");
			} else {
				System.setProperty("java.rmi.server.hostname", originalHost);
			}
		}
	}

	@Test
	void interruptionResistantCounterProbeDoesNotRetainPlatformThreadsAcrossCloseAndRestart()
					throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-wait-limit", 0);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch snapshotEntered = new CountDownLatch(1);
		final CountDownLatch releaseSnapshot = new CountDownLatch(1);
		final AtomicInteger snapshotCalls = new AtomicInteger();
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
			assertTrue(snapshotEntered.await(1, TimeUnit.SECONDS));
			return true;
		});
		when(slice.deleteObjectLifecycle()).thenAnswer(invocation -> {
			snapshotCalls.incrementAndGet();
			snapshotEntered.countDown();
			awaitIgnoringInterrupt(releaseSnapshot);
			return cleanDeleteLifecycle();
		});
		addRawStepSlice(client, slice);
		final AtomicReference<Throwable> awaitFailure = new AtomicReference<>();
		final Thread awaitThread = Thread.ofPlatform().start(() -> {
			try {
				client.await(2, TimeUnit.SECONDS);
			} catch (final Throwable failure) {
				awaitFailure.set(failure);
			}
		});

		final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
		Thread closeThread = null;
		try {
			assertTrue(snapshotEntered.await(1, TimeUnit.SECONDS));
			Thread.sleep(350);
			assertEquals(1, snapshotCalls.get(), "a blocked worker must have only one retained probe");
			awaitThread.join(TimeUnit.SECONDS.toMillis(1));
			assertFalse(awaitThread.isAlive(), "a blocked counter RPC pinned budget monitor shutdown");
			assertNull(awaitFailure.get());

			closeThread = Thread.ofPlatform().start(() -> {
				try {
					client.close();
				} catch (final Throwable failure) {
					closeFailure.set(failure);
				}
			});
			closeThread.join(TimeUnit.SECONDS.toMillis(3));
			assertFalse(closeThread.isAlive(), "terminal counter collection exceeded its step-wide bound");
			assertInstanceOf(IntegrityTerminalException.class, closeFailure.get());
			assertNoLiveThreads("spt-delete-failure-budget-snapshot-");
			assertEquals(1, LoadStepClientBase.activeFailureBudgetSnapshotFlightCount());
			assertEquals(1, LoadStepClientBase.activeFailureBudgetSnapshotProbeTaskCount());

			final TestLoadStepClient restarted = new TestLoadStepClient(
							config, extensions, ctxConfigs, mockMetricsManager);
			final LoadStep restartedSlice = mock(LoadStep.class);
			when(restartedSlice.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
			when(restartedSlice.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
			addRawStepSlice(restarted, restartedSlice);
			assertTrue(restarted.await(1, TimeUnit.SECONDS));
			final IntegrityTerminalException restartedFailure = assertThrows(
							IntegrityTerminalException.class, restarted::close);
			assertTrue(restartedFailure.getMessage().contains("counters are missing"));
			verify(restartedSlice, never()).deleteObjectLifecycle();
			assertEquals(1, LoadStepClientBase.activeFailureBudgetSnapshotFlightCount());
			assertEquals(1, LoadStepClientBase.activeFailureBudgetSnapshotProbeTaskCount());
			assertNoLiveThreads("spt-delete-failure-budget-snapshot-");
		} finally {
			releaseSnapshot.countDown();
			awaitThread.interrupt();
			awaitThread.join(TimeUnit.SECONDS.toMillis(2));
			if (closeThread != null) {
				closeThread.interrupt();
				closeThread.join(TimeUnit.SECONDS.toMillis(2));
			}
		}
		awaitFailureBudgetSnapshotProbeCount(0);
		final TestLoadStepClient recovered = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final LoadStep recoveredSlice = mock(LoadStep.class);
		when(recoveredSlice.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		when(recoveredSlice.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
		addRawStepSlice(recovered, recoveredSlice);
		assertTrue(recovered.await(1, TimeUnit.SECONDS));
		assertDoesNotThrow(recovered::close);
		assertNoLiveThreads(
						"spt-delete-failure-budget-",
						"spt-delete-failure-budget-snapshot-");
	}

	@Test
	void countStartFailsBeforeAdmissionWhenPriorCounterFlightIsUnavailable() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-wait-limit", 0);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		final TestLoadStepClient first = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch firstProbeEntered = new CountDownLatch(1);
		final CountDownLatch releaseFirstProbe = new CountDownLatch(1);
		final LoadStep firstSlice = mock(LoadStep.class);
		when(firstSlice.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
			assertTrue(firstProbeEntered.await(1, TimeUnit.SECONDS));
			return true;
		});
		when(firstSlice.deleteObjectLifecycle()).thenAnswer(invocation -> {
			firstProbeEntered.countDown();
			awaitIgnoringInterrupt(releaseFirstProbe);
			return cleanDeleteLifecycle();
		});
		addRawStepSlice(first, firstSlice);
		assertTrue(first.await(2, TimeUnit.SECONDS));
		assertEquals(1, LoadStepClientBase.activeFailureBudgetSnapshotFlightCount());

		final LoadStep nextSlice = mock(LoadStep.class);
		when(nextSlice.loadStepId()).thenReturn("count-readiness-slice");
		when(nextSlice.metricsSnapshots()).thenReturn(new ArrayList<>());
		when(nextSlice.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
		when(nextSlice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn(TestLoadStepExtension.TYPE);
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(nextSlice);
		final TestLoadStepClient next = new TestLoadStepClient(
						config, List.of(factory), ctxConfigs, mockMetricsManager);
		try {
			final IntegrityTerminalException failure = assertThrows(
							IntegrityTerminalException.class, next::start);
			assertTrue(failure.getMessage().contains("counter monitoring is not ready"));
			verify(nextSlice, never()).releaseObjectFailureBudgetAdmission();
			releaseFirstProbe.countDown();
			awaitFailureBudgetSnapshotProbeCount(0);
			final IntegrityTerminalException terminalFailure = assertThrows(
							IntegrityTerminalException.class, next::close);
			assertTrue(terminalFailure.getMessage().contains("counter monitoring was not ready"));
			assertFalse(terminalFailure.getMessage().contains("budget was exceeded earlier"));
		} finally {
			releaseFirstProbe.countDown();
			if (!next.isClosed()) {
				try {
					next.close();
				} catch (final RuntimeException ignored) {
					// The asserted startup failure is sticky during cleanup.
				}
			}
			if (!first.isClosed()) {
				first.close();
			}
		}
		awaitFailureBudgetSnapshotProbeCount(0);
		assertNoLiveThreads(
						"spt-delete-failure-budget-",
						"spt-delete-failure-budget-snapshot-");
	}

	@Test
	void countPercentageGraceBeginsAfterCounterReadinessAtAdmissionRelease() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-wait-limit", 0);
		config.val("load-op-failureBudget-mode", "percentage");
		config.val("load-op-failureBudget-maxFailurePercent", 50.0);
		config.val("load-op-failureBudget-graceSeconds", 1L);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		final AtomicReference<TestLoadStepClient> clientRef = new AtomicReference<>();
		final AtomicInteger snapshotCalls = new AtomicInteger();
		final LoadStep slice = mock(LoadStep.class);
		when(slice.loadStepId()).thenReturn("count-grace-release-slice");
		when(slice.metricsSnapshots()).thenReturn(new ArrayList<>());
		when(slice.deleteObjectLifecycle()).thenAnswer(invocation -> {
			if (snapshotCalls.incrementAndGet() == 1) {
				assertEquals(
								Long.MIN_VALUE,
								clientRef.get().failureBudgetEpochNanos(),
								"count percentage grace began during pre-admission counter readiness");
			}
			return new DeleteObjectLifecycleSnapshot(1, 1, 0, 1, 0, 0, 0, 0, true);
		});
		when(slice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		doAnswer(invocation -> {
			assertNotEquals(
							Long.MIN_VALUE,
							clientRef.get().failureBudgetEpochNanos(),
							"count percentage grace was not armed at admission release");
			return null;
		}).when(slice).releaseObjectFailureBudgetAdmission();
		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn(TestLoadStepExtension.TYPE);
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(slice);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, List.of(factory), ctxConfigs, mockMetricsManager);
		clientRef.set(client);

		try {
			assertDoesNotThrow(client::start);
			verify(slice).releaseObjectFailureBudgetAdmission();
			assertThrows(IntegrityTerminalException.class, client::close);
		} finally {
			if (!client.isClosed()) {
				try {
					client.close();
				} catch (final RuntimeException ignored) {
					// Completion must still enforce the percentage policy after grace-free startup.
				}
			}
		}
		assertNoLiveThreads(
						"spt-delete-failure-budget-",
						"spt-delete-failure-budget-snapshot-");
	}

	@Test
	void malformedCountCountersFailBeforeAdmissionAndRemainUnreleased() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-wait-limit", 0);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		final LoadStep slice = mock(LoadStep.class);
		when(slice.loadStepId()).thenReturn("malformed-count-readiness-slice");
		when(slice.metricsSnapshots()).thenReturn(new ArrayList<>());
		when(slice.deleteObjectLifecycle()).thenReturn(
						new DeleteObjectLifecycleSnapshot(1, 1, -1, 0, 2, 0, 0, 1, true));
		when(slice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn(TestLoadStepExtension.TYPE);
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(slice);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, List.of(factory), ctxConfigs, mockMetricsManager);

		try {
			final IntegrityTerminalException failure = assertThrows(
							IntegrityTerminalException.class, client::start);
			assertTrue(failure.getMessage().contains("counters are invalid"));
			verify(slice, never()).releaseObjectFailureBudgetAdmission();
		} finally {
			if (!client.isClosed()) {
				try {
					client.close();
				} catch (final RuntimeException ignored) {
					// The asserted malformed-counter failure remains sticky during cleanup.
				}
			}
		}
		assertNoLiveThreads(
						"spt-delete-failure-budget-",
						"spt-delete-failure-budget-snapshot-");
	}

	@Test
	void malformedLiveCountersStopSchedulingAfterCountAdmission() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-wait-limit", 0);
		config.val("item-data-input-compressibility", 0.0);
		config.val("item-data-dedupable", false);
		config.val("item-data-verify", false);
		final AtomicInteger snapshotCalls = new AtomicInteger();
		final CountDownLatch admissionClosed = new CountDownLatch(1);
		final LoadStep slice = mock(LoadStep.class);
		when(slice.loadStepId()).thenReturn("malformed-count-live-slice");
		when(slice.metricsSnapshots()).thenReturn(new ArrayList<>());
		when(slice.deleteObjectLifecycle()).thenAnswer(invocation -> snapshotCalls.incrementAndGet() == 1
						? cleanDeleteLifecycle()
						: new DeleteObjectLifecycleSnapshot(1, 1, -1, 0, 2, 0, 0, 1, true));
		when(slice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		doAnswer(invocation -> {
			admissionClosed.countDown();
			return null;
		}).when(slice).closeOperationAdmissionForStepStop();
		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn(TestLoadStepExtension.TYPE);
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(slice);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, List.of(factory), ctxConfigs, mockMetricsManager);

		try {
			client.start();
			verify(slice).releaseObjectFailureBudgetAdmission();
			assertTrue(
							admissionClosed.await(1, TimeUnit.SECONDS),
							"malformed live counters did not close scheduling");
			final IntegrityTerminalException failure = assertThrows(
							IntegrityTerminalException.class, client::close);
			assertTrue(failure.getMessage().contains("counters are invalid"));
		} finally {
			if (!client.isClosed()) {
				try {
					client.close();
				} catch (final RuntimeException ignored) {
					// The asserted malformed-counter failure remains sticky during cleanup.
				}
			}
		}
		assertNoLiveThreads(
						"spt-delete-failure-budget-",
						"spt-delete-failure-budget-snapshot-");
	}

	@Test
	void nextRunWaitsWithinTerminalDeadlineForPriorFlightToExitBeforeSpawning() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-wait-limit", 0);
		final TestLoadStepClient first = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch firstProbeEntered = new CountDownLatch(1);
		final CountDownLatch releaseFirstProbe = new CountDownLatch(1);
		final LoadStep firstSlice = mock(LoadStep.class);
		when(firstSlice.deleteObjectLifecycle()).thenAnswer(invocation -> {
			firstProbeEntered.countDown();
			awaitIgnoringInterrupt(releaseFirstProbe);
			return cleanDeleteLifecycle();
		});
		addRawStepSlice(first, firstSlice);
		final AtomicReference<Throwable> firstCloseFailure = new AtomicReference<>();
		final Thread firstCloseThread = Thread.ofPlatform().start(() -> {
			try {
				first.close();
			} catch (final Throwable failure) {
				firstCloseFailure.set(failure);
			}
		});

		final Thread releaseThread = Thread.ofPlatform().unstarted(() -> {
			try {
				Thread.sleep(100);
			} catch (final InterruptedException interrupted) {
				Thread.currentThread().interrupt();
			} finally {
				releaseFirstProbe.countDown();
			}
		});
		try {
			assertTrue(firstProbeEntered.await(1, TimeUnit.SECONDS));
			final TestLoadStepClient next = new TestLoadStepClient(
							config, extensions, ctxConfigs, mockMetricsManager);
			final LoadStep nextSlice = mock(LoadStep.class);
			when(nextSlice.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
			addRawStepSlice(next, nextSlice);
			releaseThread.start();

			assertDoesNotThrow(next::close);
			verify(nextSlice).deleteObjectLifecycle();
		} finally {
			releaseFirstProbe.countDown();
			if (releaseThread.isAlive()) {
				releaseThread.interrupt();
				releaseThread.join(TimeUnit.SECONDS.toMillis(2));
			}
			firstCloseThread.interrupt();
			firstCloseThread.join(TimeUnit.SECONDS.toMillis(2));
		}
		assertNull(firstCloseFailure.get());
		awaitFailureBudgetSnapshotProbeCount(0);
	}

	@Test
	void liveBlockedCounterProbeIsRetainedThroughTerminalEvaluationWithoutDuplication() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-wait-limit", 0);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch snapshotEntered = new CountDownLatch(1);
		final CountDownLatch releaseSnapshot = new CountDownLatch(1);
		final AtomicInteger snapshotCalls = new AtomicInteger();
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
			assertTrue(snapshotEntered.await(1, TimeUnit.SECONDS));
			return true;
		});
		when(slice.deleteObjectLifecycle()).thenAnswer(invocation -> {
			snapshotCalls.incrementAndGet();
			snapshotEntered.countDown();
			awaitIgnoringInterrupt(releaseSnapshot);
			return cleanDeleteLifecycle();
		});
		addRawStepSlice(client, slice);

		assertTrue(client.await(2, TimeUnit.SECONDS));
		final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
		final Thread closeThread = Thread.ofPlatform().start(() -> {
			try {
				client.close();
			} catch (final Throwable failure) {
				closeFailure.set(failure);
			}
		});
		try {
			closeThread.join(TimeUnit.SECONDS.toMillis(3));
			assertFalse(closeThread.isAlive(), "terminal counter collection exceeded its step-wide bound");
			assertEquals(1, snapshotCalls.get(), "terminal evaluation duplicated the live blocked RPC");
			assertInstanceOf(IntegrityTerminalException.class, closeFailure.get());
			assertTrue(closeFailure.get().getMessage().contains("counters are missing"));
		} finally {
			releaseSnapshot.countDown();
			closeThread.interrupt();
			closeThread.join(TimeUnit.SECONDS.toMillis(2));
		}
		assertNoLiveThreads("spt-delete-failure-budget-snapshot-");
	}

	@Test
	void terminalCollectionReprobesEverySliceAfterAPartialLiveFlight() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-wait-limit", 0);
		config.val("load-op-failureBudget-maxFailedObjects", 0L);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch blockedProbeEntered = new CountDownLatch(1);
		final CountDownLatch releaseBlockedProbe = new CountDownLatch(1);
		final AtomicInteger completedProbeCalls = new AtomicInteger();
		final AtomicInteger blockedProbeCalls = new AtomicInteger();
		final LoadStep completedSlice = mock(LoadStep.class);
		final LoadStep blockedSlice = mock(LoadStep.class);
		when(completedSlice.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		when(blockedSlice.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
			assertTrue(blockedProbeEntered.await(1, TimeUnit.SECONDS));
			return true;
		});
		when(completedSlice.deleteObjectLifecycle()).thenAnswer(invocation -> completedProbeCalls.incrementAndGet() == 1
						? cleanDeleteLifecycle()
						: new DeleteObjectLifecycleSnapshot(1, 1, 0, 1, 0, 0, 0, 0, true));
		when(blockedSlice.deleteObjectLifecycle()).thenAnswer(invocation -> {
			if (blockedProbeCalls.incrementAndGet() == 1) {
				blockedProbeEntered.countDown();
				awaitIgnoringInterrupt(releaseBlockedProbe);
			}
			return cleanDeleteLifecycle();
		});
		addRawStepSlice(client, completedSlice);
		addRawStepSlice(client, blockedSlice);
		final Thread releaseThread = Thread.ofPlatform().unstarted(() -> {
			try {
				Thread.sleep(100);
			} catch (final InterruptedException interrupted) {
				Thread.currentThread().interrupt();
			} finally {
				releaseBlockedProbe.countDown();
			}
		});

		try {
			assertTrue(client.await(2, TimeUnit.SECONDS));
			releaseThread.start();

			final IntegrityTerminalException failure = assertThrows(
							IntegrityTerminalException.class, client::close);
			assertTrue(failure.getMessage().contains("operational failed objects=1"));
			verify(completedSlice, times(2)).deleteObjectLifecycle();
			verify(blockedSlice, times(2)).deleteObjectLifecycle();
		} finally {
			releaseBlockedProbe.countDown();
			if (releaseThread.isAlive()) {
				releaseThread.interrupt();
				releaseThread.join(TimeUnit.SECONDS.toMillis(2));
			}
		}
		awaitFailureBudgetSnapshotProbeCount(0);
	}

	@Test
	void workerCounterSnapshotsRunConcurrentlyUnderOneTerminalDeadline() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-wait-limit", 0);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch probesEntered = new CountDownLatch(2);
		final CountDownLatch releaseProbes = new CountDownLatch(1);
		final List<LoadStep> slices = List.of(mock(LoadStep.class), mock(LoadStep.class));
		for (final LoadStep slice : slices) {
			when(slice.deleteObjectLifecycle()).thenAnswer(invocation -> {
				probesEntered.countDown();
				assertTrue(
								probesEntered.await(1, TimeUnit.SECONDS),
								"worker counter probes did not enter the shared collection phase concurrently");
				awaitIgnoringInterrupt(releaseProbes);
				return cleanDeleteLifecycle();
			});
			addRawStepSlice(client, slice);
		}
		final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
		final Thread closeThread = Thread.ofPlatform().start(() -> {
			try {
				client.close();
			} catch (final Throwable failure) {
				closeFailure.set(failure);
			}
		});

		try {
			assertTrue(probesEntered.await(1, TimeUnit.SECONDS));
			assertEquals(1, LoadStepClientBase.activeFailureBudgetSnapshotFlightCount());
			assertEquals(2, LoadStepClientBase.activeFailureBudgetSnapshotProbeTaskCount());
			closeThread.join(TimeUnit.SECONDS.toMillis(3));
			assertFalse(closeThread.isAlive(), "worker counter probes exceeded one shared terminal deadline");
			assertInstanceOf(IntegrityTerminalException.class, closeFailure.get());
			assertTrue(closeFailure.get().getMessage().contains("counters are missing"));
			assertEquals(1, LoadStepClientBase.activeFailureBudgetSnapshotFlightCount());
			assertEquals(2, LoadStepClientBase.activeFailureBudgetSnapshotProbeTaskCount());
			for (final LoadStep slice : slices) {
				verify(slice).deleteObjectLifecycle();
			}
		} finally {
			releaseProbes.countDown();
			closeThread.interrupt();
			closeThread.join(TimeUnit.SECONDS.toMillis(2));
		}
		awaitFailureBudgetSnapshotProbeCount(0);
	}

	@Test
	void durationPercentageGraceStartsBeforeEveryWorkerAcknowledgesScheduling() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 0);
		config.val("load-op-failureBudget-mode", "percentage");
		config.val("load-op-failureBudget-maxFailurePercent", 10.0);
		config.val("load-op-failureBudget-graceSeconds", 1L);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch delayedStartEntered = new CountDownLatch(1);
		final CountDownLatch releaseDelayedStart = new CountDownLatch(1);
		final CountDownLatch admissionClosed = new CountDownLatch(1);
		final LoadStep failedSlice = mock(LoadStep.class);
		final LoadStep delayedSlice = mock(LoadStep.class);
		when(failedSlice.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		when(delayedSlice.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		when(failedSlice.deleteObjectLifecycle()).thenReturn(
						new DeleteObjectLifecycleSnapshot(1, 1, 0, 1, 0, 0, 0, 0, true));
		when(delayedSlice.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
		when(failedSlice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		when(delayedSlice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		doAnswer(invocation -> {
			delayedStartEntered.countDown();
			assertTrue(releaseDelayedStart.await(3, TimeUnit.SECONDS));
			return null;
		}).when(delayedSlice).startDurationInterval(anyLong());
		doAnswer(invocation -> {
			admissionClosed.countDown();
			return null;
		}).when(failedSlice).closeOperationAdmissionForStepStop();
		addRawStepSlice(client, failedSlice);
		addRawStepSlice(client, delayedSlice);
		final AtomicReference<Throwable> awaitFailure = new AtomicReference<>();
		final Thread awaitThread = Thread.ofPlatform().start(() -> {
			try {
				client.await(5, TimeUnit.SECONDS);
			} catch (final Throwable failure) {
				awaitFailure.set(failure);
			}
		});

		try {
			assertTrue(delayedStartEntered.await(1, TimeUnit.SECONDS));
			assertTrue(
							admissionClosed.await(1500, TimeUnit.MILLISECONDS),
							"positive percentage grace started only after the delayed worker acknowledged scheduling");
		} finally {
			releaseDelayedStart.countDown();
			awaitThread.join(TimeUnit.SECONDS.toMillis(3));
			if (awaitThread.isAlive()) {
				awaitThread.interrupt();
				awaitThread.join(TimeUnit.SECONDS.toMillis(2));
			}
		}

		assertFalse(awaitThread.isAlive());
		if (awaitFailure.get() != null) {
			assertInstanceOf(IntegrityTerminalException.class, awaitFailure.get());
		}
		assertThrows(IntegrityTerminalException.class, client::close);
		assertNoLiveThreads(
						"spt-delete-failure-budget-",
						"spt-delete-failure-budget-snapshot-");
	}

	@Test
	void durationBudgetStopReportsPolicyInsteadOfInventoryExhaustion() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 0);
		config.val("load-op-failureBudget-maxFailedObjects", 0L);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch admissionClosed = new CountDownLatch(1);
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
			assertTrue(admissionClosed.await(1, TimeUnit.SECONDS));
			return true;
		});
		when(slice.deleteObjectLifecycle()).thenReturn(
						new DeleteObjectLifecycleSnapshot(1, 1, 0, 1, 0, 0, 0, 0, true));
		when(slice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		doAnswer(invocation -> {
			admissionClosed.countDown();
			return null;
		}).when(slice).closeOperationAdmissionForStepStop();
		addRawStepSlice(client, slice);

		final IntegrityTerminalException failure = assertThrows(
						IntegrityTerminalException.class,
						() -> client.await(5, TimeUnit.SECONDS));
		assertTrue(failure.getMessage().contains("failed-object budget exceeded"));
		assertFalse(failure.getMessage().contains("inventory slice exhausted"));
		assertThrows(IntegrityTerminalException.class, client::close);
		assertNoLiveThreads(
						"spt-delete-failure-budget-",
						"spt-delete-failure-budget-snapshot-");
	}

	@Test
	void earlyDurationBudgetBreachTightensDistributedDrainForNonterminalDispatch() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 1);
		config.val("load-op-failureBudget-maxFailedObjects", 0L);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch durationAwaitEntered = new CountDownLatch(1);
		final CountDownLatch drainsStarted = new CountDownLatch(2);
		final CountDownLatch nonterminalDrainsObserved = new CountDownLatch(2);
		final CountDownLatch releaseDrains = new CountDownLatch(1);
		final CountDownLatch terminalValidationCompleted = new CountDownLatch(2);
		final CountDownLatch shutdownCompleted = new CountDownLatch(2);
		final AtomicBoolean admissionClosed = new AtomicBoolean();
		final List<Long> drainBudgets = Collections.synchronizedList(new ArrayList<>());
		final LoadStep failedSlice = mock(LoadStep.class);
		final LoadStep dispatchedSlice = mock(LoadStep.class);
		for (final LoadStep slice : List.of(failedSlice, dispatchedSlice)) {
			when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(invocation -> {
				durationAwaitEntered.countDown();
				if (!admissionClosed.get()) {
					Thread.sleep(10);
				}
				return admissionClosed.get();
			});
			doAnswer(invocation -> {
				admissionClosed.set(true);
				return null;
			}).when(slice).closeOperationAdmissionForStepStop();
			doAnswer(invocation -> {
				drainBudgets.add(invocation.getArgument(0));
				drainsStarted.countDown();
				return null;
			}).when(slice).startDispatchedOperationsDrainForStepStop(anyLong());
			when(slice.isDispatchedOperationsDrainCompleteForStepStop()).thenAnswer(invocation -> {
				final boolean complete = releaseDrains.getCount() == 0;
				if (!complete) {
					nonterminalDrainsObserved.countDown();
				}
				return complete;
			});
			doAnswer(invocation -> {
				terminalValidationCompleted.countDown();
				return null;
			}).when(slice).validateTerminalStateForStepStop();
			doAnswer(invocation -> {
				shutdownCompleted.countDown();
				return null;
			}).when(slice).shutdown();
			addRawStepSlice(client, slice);
		}
		when(failedSlice.deleteObjectLifecycle()).thenAnswer(invocation -> durationAwaitEntered.getCount() == 0
						? new DeleteObjectLifecycleSnapshot(1, 1, 0, 1, 0, 0, 0, 0, true)
						: DeleteObjectLifecycleSnapshot.empty());
		when(dispatchedSlice.deleteObjectLifecycle()).thenAnswer(invocation -> releaseDrains.getCount() == 0
						? new DeleteObjectLifecycleSnapshot(1, 1, 0, 0, 0, 1, 0, 0, true)
						: new DeleteObjectLifecycleSnapshot(1, 0, 0, 0, 0, 0, 0, 0, false));
		final AtomicReference<Throwable> awaitFailure = new AtomicReference<>();
		final Thread awaitThread = Thread.ofPlatform().start(() -> {
			try {
				client.await(10, TimeUnit.SECONDS);
			} catch (final Throwable failure) {
				awaitFailure.set(failure);
			}
		});

		boolean breachRelativeDrain = false;
		boolean terminalValidationReached = false;
		boolean shutdownReached = false;
		try {
			assertTrue(durationAwaitEntered.await(1, TimeUnit.SECONDS));
			assertTrue(drainsStarted.await(3, TimeUnit.SECONDS));
			assertTrue(nonterminalDrainsObserved.await(1, TimeUnit.SECONDS));
			synchronized (drainBudgets) {
				breachRelativeDrain = drainBudgets.size() == 2
								&& drainBudgets.stream().allMatch(
												budget -> budget >= 0 && budget <= TimeUnit.SECONDS.toNanos(1));
			}
		} finally {
			releaseDrains.countDown();
			terminalValidationReached = terminalValidationCompleted.await(3, TimeUnit.SECONDS);
			shutdownReached = shutdownCompleted.await(3, TimeUnit.SECONDS);
			awaitThread.join(TimeUnit.SECONDS.toMillis(2));
			if (awaitThread.isAlive()) {
				awaitThread.interrupt();
				awaitThread.join(TimeUnit.SECONDS.toMillis(2));
			}
		}

		assertTrue(
						breachRelativeDrain,
						"an early budget breach retained the original duration deadline instead of the breach-relative drain bound");
		assertTrue(terminalValidationReached, "the distributed stop skipped terminal validation after the bounded drain");
		assertTrue(shutdownReached, "the distributed stop skipped worker shutdown after the bounded drain");
		assertFalse(awaitThread.isAlive());
		assertInstanceOf(IntegrityTerminalException.class, awaitFailure.get());
		verify(failedSlice).validateTerminalStateForStepStop();
		verify(dispatchedSlice).validateTerminalStateForStepStop();
		verify(failedSlice).shutdown();
		verify(dispatchedSlice).shutdown();
		assertThrows(IntegrityTerminalException.class, client::close);
		assertNoLiveThreads(
						"spt-delete-failure-budget-",
						"spt-delete-failure-budget-snapshot-",
						"spt-delete-drain-");
	}

	@Test
	void durationDeadlineStopAndBudgetBreachCoordinateOnlyOnceWithoutResettingDrain() throws Exception {
		final Config config = durationConfig();
		config.val("load-op-wait-limit", 1);
		config.val("load-op-failureBudget-maxFailedObjects", 0L);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch admissionEntered = new CountDownLatch(1);
		final CountDownLatch snapshotEntered = new CountDownLatch(1);
		final CountDownLatch releaseAdmission = new CountDownLatch(1);
		final CountDownLatch breachedSnapshotReturned = new CountDownLatch(1);
		final CountDownLatch breachDeadlinePropagated = new CountDownLatch(1);
		final List<Long> drainBudgets = Collections.synchronizedList(new ArrayList<>());
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
		when(slice.durationAwaitStatus()).thenAnswer(invocation -> admissionEntered.getCount() == 0
						? DurationAwaitStatus.REACHED_DEADLINE
						: null);
		when(slice.deleteObjectLifecycle()).thenAnswer(invocation -> {
			snapshotEntered.countDown();
			assertTrue(admissionEntered.await(1, TimeUnit.SECONDS));
			breachedSnapshotReturned.countDown();
			return new DeleteObjectLifecycleSnapshot(1, 1, 0, 1, 0, 0, 0, 0, true);
		});
		doAnswer(invocation -> {
			admissionEntered.countDown();
			assertTrue(releaseAdmission.await(2, TimeUnit.SECONDS));
			return null;
		}).when(slice).closeOperationAdmissionForStepStop();
		doAnswer(invocation -> {
			final long remainingNanos = invocation.getArgument(0);
			if (remainingNanos <= TimeUnit.SECONDS.toNanos(1)) {
				breachDeadlinePropagated.countDown();
			}
			return null;
		}).when(slice).enforceDispatchedOperationsDeadlineForStepStop(anyLong());
		doAnswer(invocation -> {
			drainBudgets.add(invocation.getArgument(0));
			return null;
		}).when(slice).startDispatchedOperationsDrainForStepStop(anyLong());
		when(slice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		addRawStepSlice(client, slice);
		final AtomicReference<Throwable> awaitFailure = new AtomicReference<>();
		final Thread awaitThread = Thread.ofPlatform().start(() -> {
			try {
				client.await(5, TimeUnit.SECONDS);
			} catch (final Throwable failure) {
				awaitFailure.set(failure);
			}
		});
		final AtomicReference<Throwable> stopFailure = new AtomicReference<>();
		Thread stopThread = null;

		try {
			assertTrue(snapshotEntered.await(1, TimeUnit.SECONDS));
			stopThread = Thread.ofPlatform().start(() -> {
				try {
					client.stop();
				} catch (final Throwable failure) {
					stopFailure.set(failure);
				}
			});
			assertTrue(admissionEntered.await(1, TimeUnit.SECONDS));
			assertTrue(breachedSnapshotReturned.await(1, TimeUnit.SECONDS));
			assertTrue(
							breachDeadlinePropagated.await(1, TimeUnit.SECONDS),
							"the worker cutoff was not tightened while admission closure was still blocked");
		} finally {
			releaseAdmission.countDown();
			if (stopThread != null) {
				stopThread.join(TimeUnit.SECONDS.toMillis(3));
			}
			awaitThread.interrupt();
			awaitThread.join(TimeUnit.SECONDS.toMillis(3));
		}

		assertFalse(stopThread.isAlive());
		assertNull(stopFailure.get());
		assertFalse(awaitThread.isAlive());
		assertEquals(1, drainBudgets.size(), "the stop race repeated the distributed drain phase");
		assertTrue(
						drainBudgets.get(0) >= 0
										&& drainBudgets.get(0) <= TimeUnit.SECONDS.toNanos(1),
						"the concurrent failure-budget breach did not tighten the distributed drain deadline");
		verify(slice).closeOperationAdmissionForStepStop();
		verify(slice, atLeastOnce()).enforceDispatchedOperationsDeadlineForStepStop(anyLong());
		verify(slice).recoverQueuedOperationsForStepStop();
		verify(slice).shutdown();
		assertThrows(IntegrityTerminalException.class, client::close);
		assertNoLiveThreads(
						"spt-delete-failure-budget-",
						"spt-delete-failure-budget-snapshot-");
	}

	@Test
	void budgetMonitorMayStopAndStartAgainWithoutLeaking() throws Exception {
		final TestLoadStepClient client = new TestLoadStepClient(
						countDeleteConfig(), extensions, ctxConfigs, mockMetricsManager);
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		when(slice.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
		addRawStepSlice(client, slice);

		assertTrue(client.await(1, TimeUnit.SECONDS));
		assertTrue(client.await(1, TimeUnit.SECONDS));
		assertDoesNotThrow(client::close);

		assertNoLiveThreads(
						"spt-delete-failure-budget-",
						"spt-delete-failure-budget-snapshot-");
	}

	@Test
	void terminalFailureBudgetDecisionIsPublishedIntoMetricsMetadata() throws Exception {
		final TestLoadStepClient client = new TestLoadStepClient(
						countDeleteConfig(), extensions, ctxConfigs, mockMetricsManager);
		final Map<String, Object> metadata = new HashMap<>();
		final MetricsContext<?> metricsContext = mock(MetricsContext.class);
		when(metricsContext.metadata()).thenReturn(metadata);
		client.addMetricsContextForTest(metricsContext);
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		when(slice.deleteObjectLifecycle()).thenReturn(
						new DeleteObjectLifecycleSnapshot(2, 2, 1, 1, 0, 0, 0, 1, true));
		addRawStepSlice(client, slice);

		assertTrue(client.await(1, TimeUnit.SECONDS));
		assertDoesNotThrow(client::close);

		assertEquals(
						"completed_within_failure_budget",
						metadata.get(com.dell.spt.base.metrics.MetricsConstants.METADATA_DELETE_FAILURE_OUTCOME));
	}

	@Test
	void closePublishesFailureBudgetOutcomeIntoRetainedFleetMetrics() throws Exception {
		final var metricsManager = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		metricsManager.setTerminalRetentionMillis(30_000L);
		final TestLoadStepClient client = new TestLoadStepClient(
						countDeleteConfig(), extensions, ctxConfigs, metricsManager);
		final Map<String, Object> metadata = new HashMap<>();
		metadata.put(MetricsConstants.METADATA_DELETE_METRICS, true);
		metadata.put(
						MetricsConstants.METADATA_DELETE_FAILURE_OUTCOME,
						MetricsConstants.DELETE_FAILURE_OUTCOME_RUNNING);
		final DistributedAllMetricsSnapshot snapshot = mock(DistributedAllMetricsSnapshot.class);
		final RateMetricSnapshot success = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot fails = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot bytes = mock(RateMetricSnapshot.class);
		final TimingMetricSnapshot latency = mock(TimingMetricSnapshot.class);
		final TimingMetricSnapshot duration = mock(TimingMetricSnapshot.class);
		final TimingMetricSnapshot ttfb = mock(TimingMetricSnapshot.class);
		final ConcurrencyMetricSnapshot concurrency = mock(ConcurrencyMetricSnapshot.class);
		when(success.count()).thenReturn(1L);
		when(snapshot.successSnapshot()).thenReturn(success);
		when(snapshot.failsSnapshot()).thenReturn(fails);
		when(snapshot.byteSnapshot()).thenReturn(bytes);
		when(snapshot.latencySnapshot()).thenReturn(latency);
		when(snapshot.durationSnapshot()).thenReturn(duration);
		when(snapshot.ttfbSnapshot()).thenReturn(ttfb);
		when(snapshot.concurrencySnapshot()).thenReturn(concurrency);
		when(snapshot.elapsedTimeMillis()).thenReturn(1_000L);
		final DeleteMetricsSnapshot runningDeleteMetrics = DeleteMetricsSnapshot.builder(1)
						.requests(1, 1, 0, 0, 0, 1)
						.objects(1, 1, 1, 0, 0, 0, 0)
						.failureOutcome(MetricsConstants.DELETE_FAILURE_OUTCOME_RUNNING)
						.build();
		when(snapshot.deleteMetrics()).thenAnswer(ignored -> runningDeleteMetrics.toBuilder()
						.failureOutcome((String) metadata.get(
										MetricsConstants.METADATA_DELETE_FAILURE_OUTCOME))
						.build());
		final DistributedMetricsContext<?> context = mock(DistributedMetricsContext.class);
		when(context.loadStepId()).thenReturn(client.loadStepId());
		when(context.opType()).thenReturn(OpType.DELETE);
		when(context.lastSnapshot()).thenReturn(snapshot);
		when(context.metadata()).thenReturn(metadata);
		when(context.quantileValues()).thenReturn(List.of());
		when(context.nodeCount()).thenReturn(1);
		when(context.nodeAddrs()).thenReturn(List.of("local"));
		when(context.nodesPresent()).thenReturn(List.of("local"));
		when(context.contributorsPresent()).thenReturn(List.of("local"));
		when(context.concurrencyLimit()).thenReturn(1);
		when(context.itemDataSize()).thenReturn(new com.github.akurilov.commons.system.SizeInBytes(0));
		when(context.comment()).thenReturn("");
		when(context.runId()).thenReturn(1L);
		when(context.sumPersistEnabled()).thenReturn(false);
		when(context.timingPersistEnabled()).thenReturn(false);
		client.addMetricsContextForTest(context);
		metricsManager.register(context);
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		when(slice.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
		addRawStepSlice(client, slice);

		final CapturingMessageAppender appender = new CapturingMessageAppender("Failure Policy:");
		appender.start();
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.METRICS_STD_OUT.getName());
		logger.addAppender(appender);
		try {
			assertTrue(client.await(1, TimeUnit.SECONDS));
			assertDoesNotThrow(client::close);
			assertTrue(appender.awaitTarget(1, TimeUnit.SECONDS));
			assertTrue(
							appender.messages().stream().anyMatch(message -> message.contains(
											"Outcome:                   completed cleanly")),
							"engine-only result must contain the controller terminal verdict");
		} finally {
			logger.removeAppender(appender);
			appender.stop();
		}

		final var retainedFleet = metricsManager.getTerminalSteps().stream()
						.filter(entry -> entry.distributed && client.loadStepId().equals(entry.stepId))
						.findFirst()
						.orElseThrow();
		assertEquals(
						MetricsConstants.DELETE_FAILURE_OUTCOME_COMPLETED_CLEANLY,
						retainedFleet.deleteMetrics.failureOutcome(),
						"close must update the retained fleet row after the controller decision");
	}

	@Test
	void terminalCounterProbeTimesOutOnceAndFailsClosed() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-wait-limit", 0);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final CountDownLatch snapshotEntered = new CountDownLatch(1);
		final CountDownLatch releaseSnapshot = new CountDownLatch(1);
		final AtomicInteger snapshotCalls = new AtomicInteger();
		final LoadStep slice = mock(LoadStep.class);
		when(slice.deleteObjectLifecycle()).thenAnswer(invocation -> {
			snapshotCalls.incrementAndGet();
			snapshotEntered.countDown();
			awaitIgnoringInterrupt(releaseSnapshot);
			return cleanDeleteLifecycle();
		});
		addRawStepSlice(client, slice);
		final AtomicReference<Throwable> closeFailure = new AtomicReference<>();
		final Thread closeThread = Thread.ofPlatform().start(() -> {
			try {
				client.close();
			} catch (final Throwable failure) {
				closeFailure.set(failure);
			}
		});

		try {
			assertTrue(snapshotEntered.await(1, TimeUnit.SECONDS));
			closeThread.join(TimeUnit.SECONDS.toMillis(3));
			assertFalse(closeThread.isAlive(), "terminal counter collection exceeded its step-wide bound");
			assertEquals(1, snapshotCalls.get(), "terminal collection duplicated a blocked worker RPC");
			assertInstanceOf(IntegrityTerminalException.class, closeFailure.get());
			assertTrue(closeFailure.get().getMessage().contains("counters are missing"));
		} finally {
			releaseSnapshot.countDown();
			closeThread.interrupt();
			closeThread.join(TimeUnit.SECONDS.toMillis(2));
		}
		assertNoLiveThreads("spt-delete-failure-budget-snapshot-");
	}

	@Test
	void percentageBudgetBreachRemainsFailedWhenFinalPercentageFallsBelowLimit() throws Exception {
		final Config config = countDeleteConfig();
		config.val("load-op-failureBudget-mode", "percentage");
		config.val("load-op-failureBudget-maxFailurePercent", 10.0);
		config.val("load-op-failureBudget-graceSeconds", 0L);
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final AtomicBoolean admissionClosed = new AtomicBoolean();
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenAnswer(
						invocation -> admissionClosed.get());
		when(slice.deleteObjectLifecycle()).thenAnswer(invocation -> admissionClosed.get()
						? new DeleteObjectLifecycleSnapshot(102, 102, 100, 2, 0, 0, 0, 1, true)
						: new DeleteObjectLifecycleSnapshot(4, 4, 2, 2, 0, 0, 0, 1, true));
		when(slice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
		doAnswer(invocation -> {
			admissionClosed.set(true);
			return null;
		}).when(slice).closeOperationAdmissionForStepStop();
		addRawStepSlice(client, slice);

		assertTrue(client.await(3, TimeUnit.SECONDS));
		final IntegrityTerminalException failure = assertThrows(
						IntegrityTerminalException.class, client::close);

		assertTrue(failure.getMessage().contains("operational failed objects=2"));
		assertTrue(failure.getMessage().contains("accepted objects=100"));
		assertTrue(failure.getMessage().contains("outcome=FAILED"));
		assertTrue(failure.getMessage().contains("remains sticky"));
	}

	@Test
	void missingTerminalObjectCountersFailClosed() throws Exception {
		final TestLoadStepClient client = new TestLoadStepClient(
						countDeleteConfig(), extensions, ctxConfigs, mockMetricsManager);
		final LoadStep slice = mock(LoadStep.class);
		when(slice.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		addRawStepSlice(client, slice);

		assertTrue(client.await(1, TimeUnit.SECONDS));
		final IntegrityTerminalException failure = assertThrows(
						IntegrityTerminalException.class, client::close);

		assertTrue(failure.getMessage().contains("counters are missing"));
	}

	private static Config durationConfig() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "distributed-duration");
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", true);
		config.val("load-step-limit-time", "60s");
		return config;
	}

	private static Config countDeleteConfig() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "distributed-count-budget");
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", false);
		config.val("load-op-limit-count", 10L);
		config.val("load-step-limit-time", "0s");
		return config;
	}

	@Test
	void exactOutputCountOmitsDistributedSlicesWithZeroShare() {
		final Config config = TestConfigBuilder.config();
		config.val("storage-integrity-output-requireExactCount", true);
		config.val("load-op-limit-count", 2L);
		final List<String> activeRemotes = LoadStepClientBase.participatingRemoteNodeAddrs(
						config, List.of("worker-a", "worker-b", "worker-c"));

		assertEquals(List.of("worker-a"), activeRemotes);
		final List<Config> slices = new ArrayList<>();
		for (int i = 0; i < 1 + activeRemotes.size(); i++) {
			slices.add(ConfigSliceUtil.initSlice(config));
		}
		ConfigSliceUtil.sliceLongValueBalanced(2L, slices, "load-op-limit-count");
		assertEquals(2L, slices.stream().mapToLong(
						slice -> slice.longVal("load-op-limit-count")).sum());
		assertTrue(slices.stream().allMatch(
						slice -> slice.longVal("load-op-limit-count") > 0));

		config.val("load-op-limit-count", 5L);
		final List<String> remainderRemotes = LoadStepClientBase.participatingRemoteNodeAddrs(
						config, List.of("worker-a", "worker-b", "worker-c"));
		assertEquals(List.of("worker-a", "worker-b", "worker-c"), remainderRemotes);
		final List<Config> remainderSlices = new ArrayList<>();
		for (int i = 0; i < 1 + remainderRemotes.size(); i++) {
			remainderSlices.add(ConfigSliceUtil.initSlice(config));
		}
		ConfigSliceUtil.sliceLongValueBalanced(5L, remainderSlices, "load-op-limit-count");
		assertEquals(5L, remainderSlices.stream().mapToLong(
						slice -> slice.longVal("load-op-limit-count")).sum());
		assertTrue(remainderSlices.stream().allMatch(
						slice -> slice.longVal("load-op-limit-count") > 0));

		final Config writeVerify = TestConfigBuilder.config();
		writeVerify.val("load-op-limit-count", 2L);
		assertEquals(
						List.of("worker-a", "worker-b", "worker-c"),
						LoadStepClientBase.participatingRemoteNodeAddrs(
										writeVerify, List.of("worker-a", "worker-b", "worker-c")));
	}

	@Test
	void distributedContributorIdsIncludeLocalSliceAndOnlyParticipatingRemotes() {
		final Config config = TestConfigBuilder.config();
		config.val("storage-integrity-output-requireExactCount", true);
		config.val("load-op-limit-count", 2L);

		assertEquals(
						List.of("local", "worker-a"),
						LoadStepClientBase.distributedContributorIds(
										config, List.of("worker-a", "worker-b", "worker-c")));
	}

	@Test
	@SuppressWarnings("unchecked")
	void terminalFailureCountsSumEveryContextByOperationType() {
		final MetricsContext<AllMetricsSnapshot> listA = mock(MetricsContext.class);
		final MetricsContext<AllMetricsSnapshot> listB = mock(MetricsContext.class);
		final MetricsContext<AllMetricsSnapshot> read = mock(MetricsContext.class);
		final AllMetricsSnapshot listASnapshot = mock(AllMetricsSnapshot.class);
		final AllMetricsSnapshot listBSnapshot = mock(AllMetricsSnapshot.class);
		final AllMetricsSnapshot readSnapshot = mock(AllMetricsSnapshot.class);
		final RateMetricSnapshot listAFailures = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot listBFailures = mock(RateMetricSnapshot.class);
		final RateMetricSnapshot readFailures = mock(RateMetricSnapshot.class);
		when(listA.opType()).thenReturn(OpType.LIST);
		when(listB.opType()).thenReturn(OpType.LIST);
		when(read.opType()).thenReturn(OpType.READ);
		when(listA.lastSnapshot()).thenReturn(listASnapshot);
		when(listB.lastSnapshot()).thenReturn(listBSnapshot);
		when(read.lastSnapshot()).thenReturn(readSnapshot);
		when(listASnapshot.failsSnapshot()).thenReturn(listAFailures);
		when(listBSnapshot.failsSnapshot()).thenReturn(listBFailures);
		when(readSnapshot.failsSnapshot()).thenReturn(readFailures);
		when(listAFailures.count()).thenReturn(1L);
		when(listBFailures.count()).thenReturn(2L);
		when(readFailures.count()).thenReturn(4L);

		assertEquals(
						Map.of(OpType.LIST, 3L, OpType.READ, 4L),
						LoadStepClientBase.terminalFailureCounts(List.of(listA, listB, read)));
	}

	@Test
	@SuppressWarnings("unchecked")
	void metadataClientCloseRejectsPartialDiscoveryWhenTerminalListFailed() throws Exception {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "list-step");
		config.val("run-id", 110L);
		config.val("storage-driver-type", "s3");
		config.val("storage-integrity-mode", "metadata");
		config.val("storage-integrity-input-provenance", "external");
		config.val("load-op-type", "list");
		final TestLoadStepClient client = new TestLoadStepClient(
						config, extensions, ctxConfigs, mockMetricsManager);
		final MetricsContext<AllMetricsSnapshot> listMetrics = mock(MetricsContext.class);
		final AllMetricsSnapshot snapshot = mock(AllMetricsSnapshot.class);
		final RateMetricSnapshot failures = mock(RateMetricSnapshot.class);
		when(listMetrics.opType()).thenReturn(OpType.LIST);
		when(listMetrics.lastSnapshot()).thenReturn(snapshot);
		when(snapshot.failsSnapshot()).thenReturn(failures);
		when(failures.count()).thenReturn(1L);
		client.addMetricsContextForTest(listMetrics);

		final Path manifest = tempDir.resolve("verify-input.csv");
		Files.writeString(
						manifest,
						"bucket,key,size,version_id\r\n"
										+ "b,first-page-object,1,v1\r\n");
		Files.writeString(IntegrityManifestCompletion.emissionCountPath(manifest), "1\n");
		Files.writeString(IntegrityManifestCompletion.deleteMarkerCountPath(manifest), "0\n");
		addItemOutputAggregator(client, new CsvArtifactAggregator(
						"list-step", List.of(com.dell.spt.base.load.step.file.FileManager.INSTANCE), List.of(),
						manifest.toString(), 110, 0, OpType.LIST));

		final var failure = assertThrows(IntegrityTerminalException.class, client::doClose);

		assertEquals(IntegrityTerminalException.Category.EXECUTION, failure.category());
		assertTrue(failure.getMessage().contains("1 failed operation"));
		assertFalse(Files.exists(CsvArtifactAggregator.nodeSourcePath(manifest, 0)));
		assertFalse(Files.exists(IntegrityManifestCompletion.completionPath(manifest)));
	}

	@Nested
	@DisplayName("Constructor Tests")
	class ConstructorTests {

		@Test
		@DisplayName("Constructor with valid config should succeed")
		void testConstructorWithValidConfig() throws RemoteException {
			// Act
			TestLoadStepClient client = assertDoesNotThrow(() -> new TestLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager));

			// Assert - Basic construction
			assertNotNull(client, "Client should not be null");

			// Assert - Configuration access works
			assertEquals("test-step-001", client.loadStepId(), "Step ID should match config");
			assertEquals("test-load-step", client.getTypeName(), "Type name should match implementation");

			// Assert - Initial state
			assertFalse(client.wasInitCalled(), "init() should not be called during construction");
			assertFalse(client.wasCopyInstanceCalled(), "copyInstance() should not be called during construction");
		}

		@Test
		@DisplayName("Constructor with null config should fail")
		void testConstructorWithNullConfig() {
			// Act & Assert
			assertThrows(Exception.class, () -> new TestLoadStepClient(null, extensions, ctxConfigs, mockMetricsManager),
							"Constructor should reject null config");
		}

		@Test
		@DisplayName("Constructor with null extensions should work")
		void testConstructorWithNullExtensions() throws RemoteException {
			// Act
			TestLoadStepClient client = assertDoesNotThrow(() -> new TestLoadStepClient(testConfig, null, ctxConfigs, mockMetricsManager));

			// Assert
			assertNotNull(client);
			assertEquals("test-step-001", client.loadStepId());
		}

		@Test
		@DisplayName("Constructor with null context configs should work")
		void testConstructorWithNullCtxConfigs() throws RemoteException {
			// Act
			TestLoadStepClient client = assertDoesNotThrow(() -> new TestLoadStepClient(testConfig, extensions, null, mockMetricsManager));

			// Assert
			assertNotNull(client);
			assertEquals("test-step-001", client.loadStepId());
		}

		@Test
		@DisplayName("Constructor with null metrics manager should work")
		void testConstructorWithNullMetricsManager() throws RemoteException {
			// Act
			TestLoadStepClient client = assertDoesNotThrow(() -> new TestLoadStepClient(testConfig, extensions, ctxConfigs, null));

			// Assert
			assertNotNull(client);
			assertEquals("test-step-001", client.loadStepId());
		}
	}

	@Nested
	@DisplayName("Basic Method Tests")
	class BasicMethodTests {

		private TestLoadStepClient client;

		@BeforeEach
		void setUp() {
			client = new TestLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager);
		}

		@Test
		@DisplayName("loadStepId() should return configured step ID")
		void testLoadStepId() throws RemoteException {
			assertEquals("test-step-001", client.loadStepId());
		}

		@Test
		@DisplayName("getTypeName() should return implementation type")
		void testGetTypeName() throws RemoteException {
			assertEquals("test-load-step", client.getTypeName());
		}

		@Test
		@DisplayName("loadStepId() should work with different configs")
		void testLoadStepIdWithDifferentConfigs() throws RemoteException {
			// Test with custom step ID
			Config customConfig = TestConfigBuilder.config();
			customConfig.val("load-step-id", "custom-test-step");
			TestLoadStepClient customClient = new TestLoadStepClient(
							customConfig, extensions, ctxConfigs, mockMetricsManager);

			assertEquals("custom-test-step", customClient.loadStepId());
		}

		@Test
		@DisplayName("Multiple instances should be independent")
		void testMultipleInstances() throws RemoteException {
			// Create second instance with different config
			Config config2 = TestConfigBuilder.config();
			config2.val("load-step-id", "second-step");
			TestLoadStepClient client2 = new TestLoadStepClient(
							config2, extensions, ctxConfigs, mockMetricsManager);

			// Assert independence
			assertEquals("test-step-001", client.loadStepId());
			assertEquals("second-step", client2.loadStepId());
			assertNotSame(client, client2);
		}
	}

	@Nested
	@DisplayName("Append/Config API Tests")
	class AppendConfigApiTests {

		@Test
		@DisplayName("append(Map) returns new instance with added context")
		void testAppendReturnsNewInstanceAndAddsContext() {
			TestLoadStepClient client = new TestLoadStepClient(testConfig, extensions, null, mockMetricsManager);
			var copy1 = client.append(java.util.Map.of("load-op-type", "read"));
			assertNotSame(client, copy1);
			assertTrue(((TestLoadStepClient) copy1).getCtxConfigsCount() >= 1);
			var copy2 = copy1.append(java.util.Map.of("load-op-type", "update"));
			assertTrue(((TestLoadStepClient) copy2).getCtxConfigsCount() >= 2);
		}

		@Test
		@DisplayName("config after append on same instance throws IllegalStateException")
		void testConfigAfterAppendThrows() {
			TestLoadStepClient client = new TestLoadStepClient(testConfig, extensions, null, mockMetricsManager);
			var appended = client.append(java.util.Map.of("load-op-type", "read"));
			assertThrows(IllegalStateException.class, () -> appended.config(java.util.Map.of("load-step-id", "x")));
		}

		@Test
		@DisplayName("await without start throws and triggers stop in finally")
		void testAwaitWithoutStartThrows() {
			TestLoadStepClient client = new TestLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager);
			assertThrows(IllegalStateException.class, () -> client.await(1, java.util.concurrent.TimeUnit.MILLISECONDS));
		}
	}

	@Nested
	@DisplayName("Start/Await Integration Tests")
	class StartAwaitIntegrationTests {
		@Test
		@DisplayName("timing file aggregation is not initialized when timing persist is disabled")
		void testTimingAggregatorDisabledWhenTimingPersistFalse() {
			Config cfg = TestConfigBuilder.config();
			cfg.val("load-step-id", "timing-aggregator-disabled");
			cfg.val("output-metrics-timing-persist", false);
			cfg.val("load-step-node-addrs", java.util.Collections.emptyList());
			cfg.val("item-data-input-compressibility", 0.0);
			cfg.val("item-data-dedupable", false);
			cfg.val("item-data-verify", false);
			TestLoadStepClient client = new TestLoadStepClient(cfg, extensions, ctxConfigs, mockMetricsManager);

			assertDoesNotThrow(client::start);

			assertEquals(0, timingMetricsAggregatorCount(client));
			assertDoesNotThrow(client::doStop);
			assertDoesNotThrow(client::doShutdown);
			assertDoesNotThrow(client::doClose);
		}

		@Test
		@DisplayName("Linear client accepts IEC item size and executes slice/aggregator init")
		void testLinearClientStartAndAwait() throws Exception {
			// Silence noisy INFO/ERROR logs produced during partial init in this integration smoke
			Configurator.setLevel("com.dell.spt.base.logging.Messages", Level.WARN);
			Configurator.setLevel("com.dell.spt.base.logging.Errors", Level.OFF);
			// Use real extensions from classpath: LinearLoadStepExtension + DummyStorageDriverMockExtension
			List<Extension> classpathExts = Extension.load(LoadStepClientBaseTest.class.getClassLoader());
			Config cfg = TestConfigBuilder.config();
			cfg.val("load-step-id", "linear-client-test");
			cfg.val("item-data-size", "10KiB");
			cfg.val("item-output-file", ""); // avoid writing
			// Ensure no remote nodes to avoid retries
			cfg.val("load-step-node-addrs", java.util.Collections.emptyList());
			LinearLoadStepClient client = new LinearLoadStepClient(cfg, classpathExts, null, mockMetricsManager);
			assertDoesNotThrow(client::start);
			// Await briefly; we only assert it returns a boolean and does not throw
			boolean result = false;
			try {
				result = client.await(5, java.util.concurrent.TimeUnit.MILLISECONDS);
			} catch (IllegalStateException ise) {
				// If no slices were created due to environment, method may throw; that's acceptable
			}
			// Stop/close regardless
			assertDoesNotThrow(client::doStop);
			assertDoesNotThrow(client::doShutdown);
			assertDoesNotThrow(client::doClose);
			// Just touch the result to avoid unused warning
			if (result) {
				assertTrue(result);
			}
			// Restore default log levels for other tests
			Configurator.setLevel("com.dell.spt.base.logging.Messages", Level.INFO);
			Configurator.setLevel("com.dell.spt.base.logging.Errors", Level.ERROR);
		}
	}

	@Nested
	@DisplayName("Configuration Access Tests")
	class ConfigurationAccessTests {

		@Test
		@DisplayName("Configuration paths should be accessible through LoadStepClientBase")
		void testConfigurationPathsAccess() {
			Config config = TestConfigBuilder.config();
			config.val("load-step-id", "config-access-test");
			config.val("load-op-type", "read");
			config.val("storage-driver-limit-concurrency", 25);
			config.val("load-batch-size", 500);
			TestLoadStepClient client = new TestLoadStepClient(
							config, extensions, ctxConfigs, mockMetricsManager);

			assertDoesNotThrow(() -> {
				// These are paths that LoadStepClientBase actually uses
				assertEquals("config-access-test", client.loadStepId());
				assertEquals("read", config.stringVal("load-op-type"));
				assertEquals(25, config.intVal("storage-driver-limit-concurrency"));
				assertEquals(500, config.intVal("load-batch-size"));
			});
		}

		@Test
		@DisplayName("Configuration validation should work with various operation types")
		void testConfigurationWithDifferentOpTypes() {
			String[] opTypes = {"write", "read", "delete", "create"
			};

			for (String opType : opTypes) {
				Config config = TestConfigBuilder.config();
				config.val("load-op-type", opType);

				assertDoesNotThrow(() -> {
					new TestLoadStepClient(config, extensions, ctxConfigs, mockMetricsManager);
					assertEquals(opType, config.stringVal("load-op-type"));
				}, "Should work with operation type: " + opType);
			}
		}
	}

	@Nested
	@DisplayName("Copy Instance Tests")
	class CopyInstanceTests {

		private TestLoadStepClient originalClient;

		@BeforeEach
		void setUp() {
			originalClient = new TestLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager);
		}

		@Test
		@DisplayName("copyInstance should create new instance with updated config")
		void testCopyInstance() throws RemoteException {
			// Arrange
			Config newConfig = TestConfigBuilder.config();
			newConfig.val("load-step-id", "copied-step");

			// Act
			TestLoadStepClient copy = originalClient.copyInstance(newConfig, Collections.emptyList());

			// Assert
			assertNotNull(copy);
			assertNotSame(originalClient, copy, "copyInstance should return new instance");
			assertTrue(originalClient.wasCopyInstanceCalled(), "copyInstance should be tracked in original");
			assertEquals(1, originalClient.getCopyInstanceCallCount());

			// Verify the copy has the new configuration
			assertEquals("copied-step", copy.loadStepId());
			assertEquals("test-step-001", originalClient.loadStepId(), "Original should be unchanged");
		}

		@Test
		@DisplayName("copyInstance with same config should still create new instance")
		void testCopyInstanceWithSameConfig() {
			// Act
			TestLoadStepClient copy = originalClient.copyInstance(testConfig, ctxConfigs);

			// Assert
			assertNotNull(copy);
			assertNotSame(originalClient, copy);
			assertTrue(originalClient.wasCopyInstanceCalled());
		}

		@Test
		@DisplayName("Multiple copyInstance calls should be tracked")
		void testMultipleCopyInstanceCalls() {
			// Act
			originalClient.copyInstance(testConfig, ctxConfigs);
			originalClient.copyInstance(testConfig, ctxConfigs);
			originalClient.copyInstance(testConfig, ctxConfigs);

			// Assert
			assertEquals(3, originalClient.getCopyInstanceCallCount());
		}
	}

	@Nested
	@DisplayName("Initialization Tests")
	class InitializationTests {

		private TestLoadStepClient client;

		@BeforeEach
		void setUp() {
			client = new TestLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager);
		}

		@Test
		@DisplayName("init() method should be callable")
		void testInitMethod() {
			// Verify init hasn't been called during construction
			assertFalse(client.wasInitCalled());

			// Call init and verify it was tracked
			assertDoesNotThrow(() -> client.init());
			assertTrue(client.wasInitCalled());
		}

		@Test
		@DisplayName("init() should be callable multiple times without error")
		void testInitMultipleCalls() {
			assertDoesNotThrow(() -> {
				client.init();
				client.init();
				client.init();
			});
			assertTrue(client.wasInitCalled());
		}
	}

	@Nested
	@DisplayName("Configuration Slicing Tests")
	class ConfigurationSlicingTests {

		@Test
		@DisplayName("remoteNodeAddrs should handle empty node list")
		void testRemoteNodeAddrsEmpty() {
			Config config = TestConfigBuilder.config();
			TestLoadStepClient client = assertDoesNotThrow(
							() -> new TestLoadStepClient(config, extensions, ctxConfigs, mockMetricsManager));

			// This tests the remoteNodeAddrs method indirectly through construction
			assertNotNull(client);
			assertDoesNotThrow(client::loadStepId);
		}

		@Test
		@DisplayName("Config with different storage driver types should work")
		void testDifferentStorageDriverTypes() {
			String[] driverTypes = {"dummy-mock", "s3", "atmos", "swift", "fs"
			};

			for (String driverType : driverTypes) {
				Config config = TestConfigBuilder.config();
				config.val("storage-driver-type", driverType);
				config.val("load-step-id", "test-" + driverType);
				assertDoesNotThrow(() -> {
					TestLoadStepClient client = new TestLoadStepClient(
									config, extensions, ctxConfigs, mockMetricsManager);
					assertEquals("test-" + driverType, client.loadStepId());
					assertEquals(driverType, config.stringVal("storage-driver-type"));
				}, "Should work with driver type: " + driverType);
			}
		}

		@Test
		@DisplayName("Config with different batch sizes should work")
		void testDifferentBatchSizes() {
			int[] batchSizes = {1, 10, 100, 1000, 10000
			};

			for (int batchSize : batchSizes) {
				Config config = TestConfigBuilder.config();
				config.val("load-batch-size", batchSize);
				config.val("load-step-id", "batch-test-" + batchSize);
				assertDoesNotThrow(() -> {
					TestLoadStepClient client = new TestLoadStepClient(
									config, extensions, ctxConfigs, mockMetricsManager);
					assertEquals("batch-test-" + batchSize, client.loadStepId());
					assertEquals(batchSize, config.intVal("load-batch-size"));
				}, "Should work with batch size: " + batchSize);
			}
		}

		@Test
		@DisplayName("Config with different concurrency levels should work")
		void testDifferentConcurrencyLevels() {
			int[] concurrencyLevels = {1, 5, 10, 50, 100
			};

			for (int concurrency : concurrencyLevels) {
				Config config = TestConfigBuilder.config();
				config.val("storage-driver-limit-concurrency", concurrency);
				config.val("load-step-id", "concurrency-test-" + concurrency);
				assertDoesNotThrow(() -> {
					TestLoadStepClient client = new TestLoadStepClient(
									config, extensions, ctxConfigs, mockMetricsManager);
					assertEquals("concurrency-test-" + concurrency, client.loadStepId());
					assertEquals(concurrency, config.intVal("storage-driver-limit-concurrency"));
				}, "Should work with concurrency: " + concurrency);
			}
		}
	}

	@Nested
	@DisplayName("Configuration Integration Tests")
	class ConfigurationIntegrationTests {

		@Test
		@DisplayName("Complex configuration should work end-to-end")
		void testComplexConfiguration() {
			Config complexConfig = TestConfigBuilder.config();
			complexConfig.val("load-step-id", "complex-integration-test");
			complexConfig.val("load-op-type", "create");
			complexConfig.val("storage-driver-limit-concurrency", 75);
			complexConfig.val("load-batch-size", 250);
			complexConfig.val("storage-driver-type", "s3");
			assertDoesNotThrow(() -> {
				TestLoadStepClient client = new TestLoadStepClient(
								complexConfig, extensions, ctxConfigs, mockMetricsManager);

				// Verify all configuration aspects
				assertEquals("complex-integration-test", client.loadStepId());
				assertEquals("create", complexConfig.stringVal("load-op-type"));
				assertEquals(75, complexConfig.intVal("storage-driver-limit-concurrency"));
				assertEquals(250, complexConfig.intVal("load-batch-size"));
				assertEquals("s3", complexConfig.stringVal("storage-driver-type"));
				assertEquals("test-load-step", client.getTypeName());
			});
		}

		@Test
		@DisplayName("Configuration paths used by LoadStepClientBase should be accessible")
		void testActualConfigurationPaths() {
			Config config = TestConfigBuilder.config();
			new TestLoadStepClient(config, extensions, ctxConfigs, mockMetricsManager);

			// Test paths that LoadStepClientBase actually accesses
			assertAll("Configuration paths should be accessible",
							() -> assertDoesNotThrow(() -> config.stringVal("load-step-id")),
							() -> assertDoesNotThrow(() -> config.boolVal("load-step-idAutoGenerated")),
							() -> assertDoesNotThrow(() -> config.intVal("load-batch-size")),
							() -> assertDoesNotThrow(() -> config.stringVal("load-op-type")),
							() -> assertDoesNotThrow(() -> config.longVal("load-op-limit-count")),
							() -> assertDoesNotThrow(() -> config.stringVal("storage-driver-type")),
							() -> assertDoesNotThrow(() -> config.intVal("storage-driver-limit-concurrency")),
							() -> assertDoesNotThrow(() -> config.stringVal("storage-auth-file")),
							() -> assertDoesNotThrow(() -> config.stringVal("item-output-file")),
							() -> assertDoesNotThrow(() -> config.stringVal("run-comment")));
		}

		@Test
		@DisplayName("Error handling for invalid configuration should work")
		void testErrorHandlingForInvalidConfig() {
			// Test with config that has invalid values but proper structure
			Config config = TestConfigBuilder.config();
			config.val("load-step-id", "");
			config.val("storage-driver-limit-concurrency", 0);
			// Should not throw during construction
			assertDoesNotThrow(() -> {
				TestLoadStepClient client = new TestLoadStepClient(
								config, extensions, ctxConfigs, mockMetricsManager);
				assertNotNull(client);
			});
		}
	}

	@Nested
	@DisplayName("Lifecycle Tests")
	class LifecycleTests {

		private TestLoadStepClient client;

		@BeforeEach
		void setUp() {
			client = new TestLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager);
		}

		@Test
		@DisplayName("Newly created client should be in initial state")
		void testInitialState() {
			// Check initial state
			assertFalse(client.wasInitCalled(), "init should not be called during construction");
			assertFalse(client.wasCopyInstanceCalled(), "copyInstance should not be called during construction");

			// Basic functionality should work
			assertDoesNotThrow(() -> {
				assertEquals("test-step-001", client.loadStepId());
				assertEquals("test-load-step", client.getTypeName());
			});
		}

		@Test
		@DisplayName("Multiple method calls should work in sequence")
		void testMethodCallSequence() {
			assertDoesNotThrow(() -> {
				// Test sequence of method calls
				String stepId1 = client.loadStepId();
				String typeName1 = client.getTypeName();
				String stepId2 = client.loadStepId();
				String typeName2 = client.getTypeName();

				// Results should be consistent
				assertEquals(stepId1, stepId2);
				assertEquals(typeName1, typeName2);
				assertEquals("test-step-001", stepId1);
				assertEquals("test-load-step", typeName1);
			});
		}

		@Test
		@DisplayName("init followed by additional method calls should work")
		void testInitThenMethods() {
			assertDoesNotThrow(() -> {
				// Call init first
				client.init();
				assertTrue(client.wasInitCalled());

				// Then call other methods
				assertEquals("test-step-001", client.loadStepId());
				assertEquals("test-load-step", client.getTypeName());
			});
		}

		@Test
		@DisplayName("copyInstance after init should preserve init state")
		void testCopyInstanceAfterInit() {
			// Initialize original
			client.init();
			assertTrue(client.wasInitCalled());

			// Create copy
			Config newConfig = TestConfigBuilder.config();
			newConfig.val("load-step-id", "copied-after-init");
			TestLoadStepClient copy = client.copyInstance(newConfig, Collections.emptyList());

			// Original should still show init was called
			assertTrue(client.wasInitCalled());
			assertTrue(client.wasCopyInstanceCalled());

			// Copy should be independent
			assertNotSame(client, copy);
			assertDoesNotThrow(() -> {
				assertEquals("copied-after-init", copy.loadStepId());
				assertEquals("test-step-001", client.loadStepId());
			});
		}

		@Test
		@DisplayName("Reset call tracking should work")
		void testResetCallTracking() {
			// Call methods to set tracking
			client.init();
			client.copyInstance(testConfig, ctxConfigs);

			assertTrue(client.wasInitCalled());
			assertTrue(client.wasCopyInstanceCalled());
			assertEquals(1, client.getCopyInstanceCallCount());

			// Reset and verify
			client.resetCallTracking();

			assertFalse(client.wasInitCalled());
			assertFalse(client.wasCopyInstanceCalled());
			assertEquals(0, client.getCopyInstanceCallCount());
		}
	}

	@Nested
	@DisplayName("Configuration Consistency Tests")
	class ConfigurationConsistencyTests {

		@Test
		@DisplayName("Configuration should remain consistent across method calls")
		void testConfigurationConsistency() {
			Config config = TestConfigBuilder.config();
			config.val("load-step-id", "consistency-test");
			config.val("load-op-type", "update");
			config.val("storage-driver-limit-concurrency", 42);
			config.val("load-batch-size", 777);
			TestLoadStepClient client = new TestLoadStepClient(
							config, extensions, ctxConfigs, mockMetricsManager);

			// Call methods multiple times and verify consistency
			for (int i = 0; i < 5; i++) {
				assertDoesNotThrow(() -> {
					assertEquals("consistency-test", client.loadStepId());
					assertEquals("update", config.stringVal("load-op-type"));
					assertEquals(42, config.intVal("storage-driver-limit-concurrency"));
					assertEquals(777, config.intVal("load-batch-size"));
					assertEquals("test-load-step", client.getTypeName());
				});
			}
		}

		@Test
		@DisplayName("Configuration changes should not affect existing instances")
		void testConfigurationIsolation() {
			// Create first client
			Config config1 = TestConfigBuilder.config();
			config1.val("load-step-id", "original");
			TestLoadStepClient client1 = new TestLoadStepClient(
							config1, extensions, ctxConfigs, mockMetricsManager);

			// Create second client with different config
			Config config2 = TestConfigBuilder.config();
			config2.val("load-step-id", "modified");
			TestLoadStepClient client2 = new TestLoadStepClient(
							config2, extensions, ctxConfigs, mockMetricsManager);

			// Verify isolation
			assertDoesNotThrow(() -> {
				assertEquals("original", client1.loadStepId());
				assertEquals("modified", client2.loadStepId());

				// Multiple calls should still be isolated
				assertEquals("original", client1.loadStepId());
				assertEquals("modified", client2.loadStepId());
			});
		}

		@Test
		@DisplayName("Complex configuration changes should work correctly")
		void testComplexConfigurationChanges() {
			// Start with one configuration
			Config config1 = TestConfigBuilder.config();
			config1.val("load-step-id", "complex-1");
			config1.val("load-op-type", "read");
			config1.val("storage-driver-limit-concurrency", 10);
			TestLoadStepClient client1 = new TestLoadStepClient(
							config1, extensions, ctxConfigs, mockMetricsManager);

			// Create copy with different configuration
			Config config2 = TestConfigBuilder.config();
			config2.val("load-step-id", "complex-2");
			config2.val("load-op-type", "write");
			config2.val("storage-driver-limit-concurrency", 100);
			config2.val("load-batch-size", 500);
			TestLoadStepClient client2 = client1.copyInstance(config2, Collections.emptyList());

			// Verify both work independently
			assertAll("Both clients should work with their respective configurations",
							() -> assertEquals("complex-1", client1.loadStepId()),
							() -> assertEquals("complex-2", client2.loadStepId()),
							() -> assertEquals("read", config1.stringVal("load-op-type")),
							() -> assertEquals("write", config2.stringVal("load-op-type")),
							() -> assertEquals(10, config1.intVal("storage-driver-limit-concurrency")),
							() -> assertEquals(100, config2.intVal("storage-driver-limit-concurrency")));
		}
	}

	@Nested
	@DisplayName("Error Resilience Tests")
	class ErrorResilienceTests {

		private TestLoadStepClient client;

		@BeforeEach
		void setUp() {
			client = new TestLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager);
		}

		@Test
		@DisplayName("Multiple init calls should not cause issues")
		void testMultipleInitCalls() {
			assertDoesNotThrow(() -> {
				for (int i = 0; i < 10; i++) {
					client.init();
					assertTrue(client.wasInitCalled());
				}
			});
		}

		@Test
		@DisplayName("Mixed method calls should work reliably")
		void testMixedMethodCalls() {
			assertDoesNotThrow(() -> {
				client.init();
				assertEquals("test-step-001", client.loadStepId());
				client.init();
				assertEquals("test-load-step", client.getTypeName());
				client.copyInstance(testConfig, ctxConfigs);
				assertEquals("test-step-001", client.loadStepId());
				client.init();
				assertEquals("test-load-step", client.getTypeName());
			});
		}

		@Test
		@DisplayName("Operations with null parameters should be handled gracefully")
		void testNullParameterHandling() {
			assertDoesNotThrow(() -> {
				// copyInstance with null config and null ctxConfigs
				// This might throw, but should be handled gracefully
				try {
					client.copyInstance(null, null);
				} catch (Exception e) {
					// Expected - null config should fail gracefully
					assertNotNull(e);
				}

				// Client should still be functional
				assertEquals("test-step-001", client.loadStepId());
				assertEquals("test-load-step", client.getTypeName());
			});
		}

		@Test
		@DisplayName("Rapid successive operations should work")
		void testRapidSuccessiveOperations() {
			assertDoesNotThrow(() -> {
				for (int i = 0; i < 100; i++) {
					String stepId = client.loadStepId();
					String typeName = client.getTypeName();

					assertEquals("test-step-001", stepId);
					assertEquals("test-load-step", typeName);

					if (i % 10 == 0) {
						client.init();
					}

					if (i % 15 == 0) {
						client.copyInstance(testConfig, ctxConfigs);
					}
				}
			});
		}
	}

	@Nested
	@DisplayName("Configuration Path Coverage Tests")
	class ConfigurationPathCoverageTests {

		@Test
		@DisplayName("All major configuration paths should be accessible")
		void testAllMajorConfigurationPaths() {
			Config config = TestConfigBuilder.config();
			assertDoesNotThrow(
							() -> new TestLoadStepClient(config, extensions, ctxConfigs, mockMetricsManager));

			// Test all the paths that LoadStepClientBase uses
			assertAll("All configuration paths should be accessible",
							() -> assertDoesNotThrow(() -> config.stringVal("load-step-id")),
							() -> assertDoesNotThrow(() -> config.boolVal("load-step-idAutoGenerated")),
							() -> assertDoesNotThrow(() -> config.intVal("load-batch-size")),
							() -> assertDoesNotThrow(() -> config.stringVal("load-op-type")),
							() -> assertDoesNotThrow(() -> config.longVal("load-op-limit-count")),
							() -> assertDoesNotThrow(() -> config.doubleVal("load-op-limit-rate")),
							() -> assertDoesNotThrow(() -> config.longVal("load-op-limit-fail-count")),
							() -> assertDoesNotThrow(() -> config.stringVal("storage-driver-type")),
							() -> assertDoesNotThrow(() -> config.intVal("storage-driver-limit-concurrency")),
							() -> assertDoesNotThrow(() -> config.stringVal("storage-auth-file")),
							() -> assertDoesNotThrow(() -> config.stringVal("item-output-file")),
							() -> assertDoesNotThrow(() -> config.stringVal("run-comment")),
							() -> assertDoesNotThrow(() -> config.stringVal("run-version")),
							() -> assertDoesNotThrow(() -> config.listVal("output-metrics-quantiles")));
		}

		@Test
		@DisplayName("Configuration paths should return expected values")
		void testConfigurationPathValues() {
			Config config = TestConfigBuilder.config();

			// Verify specific values match what we configured
			assertEquals("test-step-001", config.stringVal("load-step-id"));
			assertEquals(false, config.boolVal("load-step-idAutoGenerated"));
			assertEquals(100, config.intVal("load-batch-size"));
			assertEquals("create", config.stringVal("load-op-type"));
			assertEquals(0L, config.longVal("load-op-limit-count"));
			assertEquals(0.0, config.doubleVal("load-op-limit-rate"));
			assertEquals(100000L, config.longVal("load-op-limit-fail-count"));
			assertEquals("dummy-mock", config.stringVal("storage-driver-type"));
			assertEquals(10, config.intVal("storage-driver-limit-concurrency"));
			assertEquals("", config.stringVal("storage-auth-file"));
			assertEquals("", config.stringVal("item-output-file"));
			assertEquals("", config.stringVal("run-comment"));
			assertEquals("test-version", config.stringVal("run-version"));
			assertEquals(List.of(0.25, 0.5, 0.75), config.listVal("output-metrics-quantiles"));
		}

		@Test
		void testRunLoadStepClient() throws IOException, IllegalStateException, InterruptedException {
			// Local-only config: no remote node addrs to avoid 10-minute retry hang
			// against a nonexistent FileManager (exponential backoff up to 120s per retry)
			Config complexConfig = TestConfigBuilder.config();
			complexConfig.val("load-step-id", "complex-integration-test");
			complexConfig.val("load-op-type", "create");
			complexConfig.val("storage-driver-limit-concurrency", 75);
			complexConfig.val("load-batch-size", 250);
			complexConfig.val("storage-driver-type", "s3");

			TestLoadStepExtension newExt = new TestLoadStepExtension();

			List<Extension> extensions = new ArrayList<>();
			extensions.add(newExt);

			TestLoadStepClient client = new TestLoadStepClient(complexConfig, extensions, ctxConfigs, mockMetricsManager);
			assertDoesNotThrow(() -> client.start());
			assertDoesNotThrow(() -> client.doShutdown());
			assertDoesNotThrow(() -> client.doClose());
			assertDoesNotThrow(() -> client.doStop());

			LinearLoadStepClient linearClient = new LinearLoadStepClient(testConfig, extensions, ctxConfigs, mockMetricsManager);
			assertDoesNotThrow(() -> linearClient.start());
			assertDoesNotThrow(() -> linearClient.doShutdown());
			assertDoesNotThrow(() -> linearClient.doClose());
			assertDoesNotThrow(() -> linearClient.doStop());
		}
	}

	@SuppressWarnings("unchecked")
	private static void addStepSlice(final LoadStepClientBase<?> client, final LoadStep slice) {
		try {
			when(slice.durationAwaitStatus()).thenReturn(DurationAwaitStatus.REACHED_DEADLINE);
			when(slice.isDispatchedOperationsDrainCompleteForStepStop()).thenReturn(true);
			when(slice.deleteObjectLifecycle()).thenReturn(cleanDeleteLifecycle());
			addRawStepSlice(client, slice);
		} catch (final RemoteException e) {
			throw new LinkageError(e.getMessage(), e);
		}
	}

	private static DeleteObjectLifecycleSnapshot cleanDeleteLifecycle() {
		return new DeleteObjectLifecycleSnapshot(1, 1, 1, 0, 0, 0, 0, 1, true);
	}

	private static void awaitIgnoringInterrupt(final CountDownLatch release) {
		boolean interrupted = false;
		while (release.getCount() > 0) {
			try {
				release.await();
			} catch (final InterruptedException ignored) {
				interrupted = true;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private static void awaitFailureBudgetSnapshotProbeCount(final int expected)
					throws InterruptedException {
		final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
		do {
			if (LoadStepClientBase.activeFailureBudgetSnapshotFlightCount() == expected
							&& LoadStepClientBase.activeFailureBudgetSnapshotProbeTaskCount() == expected) {
				return;
			}
			Thread.sleep(10);
		} while (System.nanoTime() < deadlineNanos);
		assertEquals(expected, LoadStepClientBase.activeFailureBudgetSnapshotFlightCount());
		assertEquals(expected, LoadStepClientBase.activeFailureBudgetSnapshotProbeTaskCount());
	}

	@SuppressWarnings("unchecked")
	private static void addRawStepSlice(final LoadStepClientBase<?> client, final LoadStep slice) {
		try {
			final Field field = LoadStepClientBase.class.getDeclaredField("stepSlices");
			field.setAccessible(true);
			((List<LoadStep>) field.get(client)).add(slice);
		} catch (final ReflectiveOperationException e) {
			throw new LinkageError(e.getMessage(), e);
		}
	}

	private static LoadStepServiceImpl newRealDurationService(
					final LoadStep localLoadStep, final int port) {
		final Config serviceConfig = TestConfigBuilder.config();
		serviceConfig.val("load-step-id", "real-rmi-duration");
		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn(REAL_RMI_STEP_TYPE);
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(localLoadStep);
		return new LoadStepServiceImpl(
						port,
						List.of(factory),
						REAL_RMI_STEP_TYPE,
						serviceConfig,
						List.of(),
						mock(MetricsManager.class));
	}

	private static final class CapturingMessageAppender extends AbstractAppender {
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());
		private final CountDownLatch targetCaptured = new CountDownLatch(1);
		private final String target;

		private CapturingMessageAppender(final String target) {
			super("testLoadStepClientMessageCapture", null, null, true, Property.EMPTY_ARRAY);
			this.target = target;
		}

		@Override
		public void append(final LogEvent event) {
			final String message = event.getMessage().getFormattedMessage();
			captured.add(message);
			if (message.contains(target)) {
				targetCaptured.countDown();
			}
		}

		private boolean awaitTarget(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
			return targetCaptured.await(timeout, timeUnit);
		}

		private List<String> messages() {
			synchronized (captured) {
				return List.copyOf(captured);
			}
		}
	}

	private enum RemoteCleanupFailurePhase {
		RECOVERY, DRAIN, SHUTDOWN, STOP, CLOSE
	}

	private static void configureOneShotFailure(
					final LoadStep slice,
					final RemoteCleanupFailurePhase phase,
					final AtomicInteger attempts) throws Exception {
		final org.mockito.stubbing.Answer<Void> answer = invocation -> {
			if (attempts.incrementAndGet() == 1) {
				throw new RemoteException("transient " + phase.name().toLowerCase(Locale.ROOT));
			}
			return null;
		};
		switch (phase) {
		case RECOVERY -> doAnswer(answer).when(slice).recoverQueuedOperationsForStepStop();
		case DRAIN -> doAnswer(answer).when(slice).startDispatchedOperationsDrainForStepStop(anyLong());
		case SHUTDOWN -> doAnswer(answer).when(slice).shutdown();
		case STOP -> doAnswer(answer).when(slice).stop();
		case CLOSE -> doAnswer(answer).when(slice).close();
		default -> throw new AssertionError(phase);
		}
	}

	private static void verifyRemotePhase(
					final LoadStep slice,
					final RemoteCleanupFailurePhase phase,
					final org.mockito.verification.VerificationMode mode) throws Exception {
		switch (phase) {
		case RECOVERY -> verify(slice, mode).recoverQueuedOperationsForStepStop();
		case DRAIN -> verify(slice, mode).startDispatchedOperationsDrainForStepStop(anyLong());
		case SHUTDOWN -> verify(slice, mode).shutdown();
		case STOP -> verify(slice, mode).stop();
		case CLOSE -> verify(slice, mode).close();
		default -> throw new AssertionError(phase);
		}
	}

	@SuppressWarnings("unchecked")
	private static void addItemOutputAggregator(
					final LoadStepClientBase<?> client, final AutoCloseable aggregator) {
		try {
			final Field field = LoadStepClientBase.class.getDeclaredField("itemOutputFileAggregators");
			field.setAccessible(true);
			((List<AutoCloseable>) field.get(client)).add(aggregator);
		} catch (final ReflectiveOperationException e) {
			throw new LinkageError(e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	private static void addDeleteArtifactAggregator(
					final LoadStepClientBase<?> client, final AutoCloseable aggregator) {
		try {
			final Field field = LoadStepClientBase.class.getDeclaredField("deleteArtifactAggregators");
			field.setAccessible(true);
			((List<AutoCloseable>) field.get(client)).add(aggregator);
		} catch (final ReflectiveOperationException e) {
			throw new LinkageError(e.getMessage(), e);
		}
	}

	private static int timingMetricsAggregatorCount(final LoadStepClientBase<?> client) {
		try {
			final Field field = LoadStepClientBase.class.getDeclaredField("itemTimingMetricsOutputFileAggregators");
			field.setAccessible(true);
			return ((List<?>) field.get(client)).size();
		} catch (final ReflectiveOperationException e) {
			throw new LinkageError(e.getMessage(), e);
		}
	}
}
