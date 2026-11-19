package com.dell.spt.base.load.step.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.DurationAwaitStatus;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.load.lifecycle.OperationLifecycleArtifact;
import com.dell.spt.base.load.step.local.LoadStepLocalBase;
import com.dell.spt.base.load.step.local.context.LoadStepContext;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.rmi.registry.LocateRegistry;
import java.util.UUID;
import com.dell.spt.base.svc.Service;
import com.dell.spt.base.svc.ServiceUtil;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.net.ServerSocket;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import com.dell.spt.base.logging.Loggers;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

/**
 * Tests for {@link LoadStepServiceImpl} focusing on delegation behavior and await handling.
 *
 * This test uses a mocked {@link LoadStepFactory} (which is also an {@link Extension}) to supply a
 * mocked local {@link LoadStep} instance. The service constructor internally uses
 * {@link LoadStepFactory#createLocalLoadStep} to obtain that instance based on the provided
 * extensions list and the step type string.
 */
public class LoadStepServiceImplTest {

	private static final String STEP_TYPE = "test-step-type";

	private static LoadStepServiceImpl newServiceWith(final LoadStep loadStep) {
		// Build a valid base config for tests
		final Config baseConfig = TestConfigBuilder.config();
		baseConfig.val("load-step-id", "svc-test-1");

		// Mock a LoadStepFactory that returns our provided loadStep
		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn(STEP_TYPE);
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(loadStep);

		final List<Extension> extensions = List.of(factory);
		final List<Config> ctxConfigs = Collections.emptyList();
		final MetricsManager metricsMgr = mock(MetricsManager.class);

		// Use a high, unlikely-to-conflict port; the constructor will attempt to start an RMI service
		final int port = 51234;

		return new LoadStepServiceImpl(port, extensions, STEP_TYPE, baseConfig, ctxConfigs, metricsMgr);
	}

	@Test
	@DisplayName("Delegates basic getters and await to local LoadStep")
	public void testDelegation() throws Exception {
		final LoadStep local = mock(LoadStep.class);
		when(local.loadStepId()).thenReturn("svc-test-1");
		when(local.runId()).thenReturn(42L);
		when(local.getTypeName()).thenReturn("local");
		// Mockito's generics capture requires raw cast to satisfy List<CAP#>
		when(local.metricsSnapshots()).thenReturn((List) Collections.emptyList());
		when(local.await(eq(1L), eq(TimeUnit.SECONDS))).thenReturn(true);

		try (MockedStatic<ServiceUtil> util = mockStatic(ServiceUtil.class)) {
			util.when(() -> ServiceUtil.create(any(Service.class), anyInt())).thenReturn("rmi://localhost/test");
			util.when(() -> ServiceUtil.close(any(Service.class))).thenReturn("rmi://localhost/test");

			final LoadStepServiceImpl svc = newServiceWith(local);

			// name(): prefix + hashCode
			assertTrue(
							svc.name().startsWith(LoadStepService.SVC_NAME_PREFIX),
							"Service name should start with prefix");
			assertEquals(
							LoadStepService.SVC_NAME_PREFIX + svc.hashCode(),
							svc.name(),
							"Service name should be prefix + hashCode");

			// Delegated calls
			assertEquals("svc-test-1", svc.loadStepId());
			assertEquals(42L, svc.runId());
			assertEquals("local", svc.getTypeName());
			assertNotNull(svc.metricsSnapshots());
			assertTrue(svc.await(1, TimeUnit.SECONDS));

			// Verify delegation occurred
			verify(local, atLeastOnce()).loadStepId();
			verify(local).runId();
			verify(local).getTypeName();
			verify(local).metricsSnapshots();
			verify(local).await(1L, TimeUnit.SECONDS);
		}
	}

	@Test
	@DisplayName("await returns false when local await throws RemoteException")
	public void testAwaitHandlesRemoteException() throws Exception {
		final LoadStep local = mock(LoadStep.class);
		when(local.loadStepId()).thenReturn("svc-test-2");
		when(local.await(anyLong(), any())).thenThrow(new RemoteException("boom"));

		try (MockedStatic<ServiceUtil> util = mockStatic(ServiceUtil.class)) {
			util.when(() -> ServiceUtil.create(any(Service.class), anyInt())).thenReturn("rmi://localhost/test");
			util.when(() -> ServiceUtil.close(any(Service.class))).thenReturn("rmi://localhost/test");

			final LoadStepServiceImpl svc = newServiceWith(local);

			// Should swallow RemoteException and return false
			assertFalse(svc.await(5, TimeUnit.MILLISECONDS));

			verify(local).await(5L, TimeUnit.MILLISECONDS);
		}
	}

	@Test
	@DisplayName("doStart propagates RemoteException from local load step")
	void testDoStartPropagatesRemoteException() throws Exception {
		final LoadStep local = mock(LoadStep.class);
		when(local.loadStepId()).thenReturn("svc-test-3");
		when(local.start()).thenThrow(new RemoteException("boom"));

		try (MockedStatic<ServiceUtil> util = mockStatic(ServiceUtil.class)) {
			util.when(() -> ServiceUtil.create(any(Service.class), anyInt())).thenReturn("rmi://localhost/test");
			util.when(() -> ServiceUtil.close(any(Service.class))).thenReturn("rmi://localhost/test");

			final LoadStepServiceImpl svc = newServiceWith(local);
			assertThrows(IllegalStateException.class, svc::doStart);
		}
	}

	@Test
	@DisplayName("doStop propagates RemoteException from local load step")
	void testDoStopPropagatesRemoteException() throws Exception {
		final LoadStep local = mock(LoadStep.class);
		when(local.loadStepId()).thenReturn("svc-test-4");
		doThrow(new RemoteException("stop"))
						.when(local)
						.stop();

		try (MockedStatic<ServiceUtil> util = mockStatic(ServiceUtil.class)) {
			util.when(() -> ServiceUtil.create(any(Service.class), anyInt())).thenReturn("rmi://localhost/test");
			util.when(() -> ServiceUtil.close(any(Service.class))).thenReturn("rmi://localhost/test");

			final LoadStepServiceImpl svc = newServiceWith(local);
			assertThrows(IllegalStateException.class, svc::doStop);
		}
	}

	@Test
	@DisplayName("Delegates distributed duration stop phases to the local load step")
	void delegatesDurationStopPhases() throws Exception {
		final LoadStep local = mock(LoadStep.class);
		when(local.loadStepId()).thenReturn("svc-duration");
		when(local.durationAwaitStatus()).thenReturn(DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE);

		try (MockedStatic<ServiceUtil> util = mockStatic(ServiceUtil.class)) {
			util.when(() -> ServiceUtil.create(any(Service.class), anyInt())).thenReturn("rmi://localhost/test");
			util.when(() -> ServiceUtil.close(any(Service.class))).thenReturn("rmi://localhost/test");

			final LoadStepServiceImpl svc = newServiceWith(local);
			svc.releaseObjectFailureBudgetAdmission();
			svc.prepareDurationInterval(456L);
			svc.startDurationInterval(456L);
			assertEquals(DurationAwaitStatus.EXHAUSTED_BEFORE_DEADLINE, svc.durationAwaitStatus());
			svc.closeOperationAdmissionForStepStop();
			svc.enforceDispatchedOperationsDeadlineForStepStop(123L);
			svc.recoverQueuedOperationsForStepStop();
			svc.drainDispatchedOperationsForStepStop(123L);
			svc.validateTerminalStateForStepStop();
			svc.shutdown();

			verify(local).closeOperationAdmissionForStepStop();
			verify(local).enforceDispatchedOperationsDeadlineForStepStop(123L);
			verify(local).releaseObjectFailureBudgetAdmission();
			verify(local).prepareDurationInterval(456L);
			verify(local).startDurationInterval(456L);
			verify(local).durationAwaitStatus();
			verify(local).recoverQueuedOperationsForStepStop();
			verify(local).drainDispatchedOperationsForStepStop(123L);
			verify(local).validateTerminalStateForStepStop();
			verify(local).shutdown();
		}
	}

	@Test
	@DisplayName("A slice runs from its start until its stop, also a stop that fails")
	void aSliceRunsFromItsStartUntilItsStop() throws Exception {
		final LoadStep local = mock(LoadStep.class);
		when(local.loadStepId()).thenReturn("svc-running");
		final int port = freePort();
		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		try {
			final LoadStepServiceImpl service = newServiceWith(local, port);
			assertFalse(service.isRunning(), "created, not started");
			service.start();
			assertTrue(service.isRunning());
			service.shutdown();
			assertTrue(service.isRunning(), "a shut down slice still finishes its operations");
			service.stop();
			assertFalse(service.isRunning());
			service.start();
			assertTrue(service.isRunning(), "a slice can start again");
			doThrow(new IllegalStateException("publish failed")).doReturn(local).when(local).stop();
			assertThrows(IllegalStateException.class, service::stop);
			assertFalse(service.isRunning(), "its contexts had their stop");
			service.close();
		} finally {
			ServiceUtil.shutdown();
			restoreHost(originalHost);
		}
	}

	@Test
	@DisplayName("A failed start leaves the slice not running")
	void aFailedStartLeavesTheSliceNotRunning() throws Exception {
		final LoadStep local = mock(LoadStep.class);
		when(local.loadStepId()).thenReturn("svc-failed-start");
		when(local.start()).thenThrow(new IllegalStateException("no context started"));
		final int port = freePort();
		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		try {
			final LoadStepServiceImpl service = newServiceWith(local, port);
			assertThrows(IllegalStateException.class, service::start);
			assertFalse(service.isRunning());
			service.close();
		} finally {
			ServiceUtil.shutdown();
			restoreHost(originalHost);
		}
	}

	@Test
	@DisplayName("The release is reported once, after the service left the registry, and not while a retry is due")
	void theReleaseIsReportedOnceAfterTheServiceLeftTheRegistry() throws Exception {
		final LoadStep local = mock(LoadStep.class);
		when(local.loadStepId()).thenReturn("svc-release-hook");
		doThrow(new IOException("transient local cleanup failure")).doNothing().when(local).close();
		final int port = freePort();
		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		try {
			final List<Boolean> boundAtRelease = new java.util.ArrayList<>();
			final Config baseConfig = TestConfigBuilder.config();
			baseConfig.val("load-step-id", "svc-release-hook");
			@SuppressWarnings("unchecked")
			final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
			when(factory.id()).thenReturn(STEP_TYPE);
			when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(local);
			final LoadStepServiceImpl service = new LoadStepServiceImpl(
							port, List.of(factory), STEP_TYPE, baseConfig, Collections.emptyList(), mock(MetricsManager.class),
							released -> {
								try {
									boundAtRelease.add(List.of(LocateRegistry.getRegistry("127.0.0.1", port).list()).contains(released.name()));
								} catch (final RemoteException e) {
									throw new AssertionError(e);
								}
							});

			assertThrows(IOException.class, service::close);
			assertEquals(List.of(), boundAtRelease, "the close of this kind of step is retried");
			service.close();
			service.close();
			assertEquals(List.of(false), boundAtRelease);
		} finally {
			ServiceUtil.shutdown();
			restoreHost(originalHost);
		}
	}

	@Test
	@DisplayName("A failed local close leaves the real RMI endpoint available for retry")
	void failedLocalCloseKeepsRealRmiEndpointAvailableForRetry() throws Exception {
		final LoadStep local = mock(LoadStep.class);
		when(local.loadStepId()).thenReturn("svc-close-retry");
		when(local.durationAwaitStatus()).thenReturn(DurationAwaitStatus.RUNNING);
		doThrow(new IOException("transient local cleanup failure"))
						.doNothing()
						.when(local)
						.close();

		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		try {
			final int port;
			try (ServerSocket socket = new ServerSocket(0)) {
				socket.setReuseAddress(true);
				port = socket.getLocalPort();
			}
			final LoadStepServiceImpl service = newServiceWith(local, port);
			final LoadStepService remote = ServiceUtil.resolve(
							"127.0.0.1", port, service.name(), LoadStepService.class);

			assertEquals(DurationAwaitStatus.RUNNING, remote.durationAwaitStatus());
			assertThrows(IOException.class, service::doClose);
			assertEquals(DurationAwaitStatus.RUNNING, remote.durationAwaitStatus());

			assertDoesNotThrow(service::doClose);
			assertThrows(RemoteException.class, remote::durationAwaitStatus);
			verify(local, times(2)).close();
		} finally {
			ServiceUtil.shutdown();
			if (originalHost == null) {
				System.clearProperty("java.rmi.server.hostname");
			} else {
				System.setProperty("java.rmi.server.hostname", originalHost);
			}
		}
	}

	@Test
	@DisplayName("A slice whose stop and close fail ends closed and unexported, once")
	void sliceWhoseStopAndCloseFailIsClosedAndUnexported() throws Exception {
		final LoadStepContext context = mock(LoadStepContext.class);
		final FailingSlice local = new FailingSlice(sliceConfig("create"), context);
		final IllegalStateException publishFailure = new IllegalStateException("injected lifecycle publish failure");
		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		final PrintStream originalErr = System.err;
		try (MockedStatic<OperationLifecycleArtifact> artifact = mockStatic(OperationLifecycleArtifact.class)) {
			artifact.when(() -> OperationLifecycleArtifact.publish(anyLong(), anyString(), anyString(), anyList(), anyBoolean()))
							.thenThrow(publishFailure);
			final int port = freePort();
			final LoadStepServiceImpl service = newServiceWith(local, port);
			final LoadStepService remote = ServiceUtil.resolve(
							"127.0.0.1", port, service.name(), LoadStepService.class);
			service.start();
			assertEquals(local.loadStepId(), remote.loadStepId());

			assertSame(publishFailure, assertThrows(IllegalStateException.class, service::stop));
			assertSame(publishFailure, assertThrows(IllegalStateException.class, service::close));

			assertThrows(RemoteException.class, remote::loadStepId);
			assertFalse(List.of(LocateRegistry.getRegistry("127.0.0.1", port).list()).contains(service.name()));
			verify(context).close();
			artifact.verify(
							() -> OperationLifecycleArtifact.publish(anyLong(), anyString(), anyString(), anyList(), anyBoolean()),
							times(1));

			// The worker's shutdown and the JVM exit close it again: nothing is released twice.
			final ByteArrayOutputStream err = new ByteArrayOutputStream();
			System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
			assertDoesNotThrow(service::close);
			assertDoesNotThrow(local::close);
			System.setErr(originalErr);
			assertTrue(service.isClosed());
			assertTrue(local.isClosed());
			assertFalse(err.toString(StandardCharsets.UTF_8).contains("Failed to remove the service"), err.toString(StandardCharsets.UTF_8));
			verify(context, times(1)).close();
		} finally {
			System.setErr(originalErr);
			ServiceUtil.shutdown();
			restoreHost(originalHost);
		}
	}

	@Test
	@DisplayName("A first close with no stop before it releases a slice whose stop fails")
	void firstCloseWithoutStopReleasesSliceWhoseStopFails() throws Exception {
		final LoadStepContext context = mock(LoadStepContext.class);
		final FailingSlice local = new FailingSlice(sliceConfig("create"), context);
		final IllegalStateException publishFailure = new IllegalStateException("injected lifecycle publish failure");
		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		try (MockedStatic<OperationLifecycleArtifact> artifact = mockStatic(OperationLifecycleArtifact.class)) {
			artifact.when(() -> OperationLifecycleArtifact.publish(anyLong(), anyString(), anyString(), anyList(), anyBoolean()))
							.thenThrow(publishFailure);
			final int port = freePort();
			final LoadStepServiceImpl service = newServiceWith(local, port);
			final LoadStepService remote = ServiceUtil.resolve(
							"127.0.0.1", port, service.name(), LoadStepService.class);
			service.start();
			assertEquals(local.loadStepId(), remote.loadStepId());

			// As at worker shutdown (the step manager closes its child), the close comes first.
			assertSame(publishFailure, assertThrows(IllegalStateException.class, service::close));

			assertThrows(RemoteException.class, remote::loadStepId);
			assertFalse(List.of(LocateRegistry.getRegistry("127.0.0.1", port).list()).contains(service.name()));
			verify(context).stop();
			verify(context).close();
			artifact.verify(
							() -> OperationLifecycleArtifact.publish(anyLong(), anyString(), anyString(), anyList(), anyBoolean()),
							times(1));
			// Released: closing again releases nothing and throws nothing.
			assertDoesNotThrow(service::close);
			assertDoesNotThrow(local::close);
			assertTrue(service.isClosed());
			assertTrue(local.isClosed());
			verify(context, times(1)).close();
		} finally {
			ServiceUtil.shutdown();
			restoreHost(originalHost);
		}
	}

	@Test
	@DisplayName("A DELETE duration slice stays exported after a failed close until its last retry")
	void durationDeleteSliceStaysExportedUntilItsLastFailedClose() throws Exception {
		final LoadStepContext context = mock(LoadStepContext.class);
		doThrow(new IllegalStateException("injected admission close failure"))
						.when(context).closeOperationAdmissionForStepStop();
		final Config config = sliceConfig("delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", true);
		config.val("load-step-limit-time", "60s");
		config.val("load-op-wait-limit", 1);
		final FailingSlice local = new FailingSlice(config, context);
		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		try {
			final int port = freePort();
			final LoadStepServiceImpl service = newServiceWith(local, port);
			final LoadStepService remote = ServiceUtil.resolve(
							"127.0.0.1", port, service.name(), LoadStepService.class);
			assertDoesNotThrow(service::stop);
			final int limit = FailingSlice.closeAttemptLimit();
			for (var attempt = 1; attempt < limit; attempt++) {
				assertThrows(RuntimeException.class, service::close, "close attempt " + attempt);
				assertEquals(local.loadStepId(), remote.loadStepId(), "exported for the retry after attempt " + attempt);
			}
			assertThrows(RuntimeException.class, service::close, "the last close attempt");
			assertThrows(RemoteException.class, remote::loadStepId);
			assertFalse(List.of(LocateRegistry.getRegistry("127.0.0.1", port).list()).contains(service.name()));
			assertDoesNotThrow(service::close);
			assertTrue(service.isClosed());
		} finally {
			ServiceUtil.shutdown();
			restoreHost(originalHost);
		}
	}

	private static final long ENTRY_SILENCE_NANOS = TimeUnit.SECONDS.toNanos(120);
	private static final long NO_CHECK_NANOS = TimeUnit.HOURS.toNanos(1);
	private static final String ENTRY_SILENCE_WARNING = "No call from the entry node";

	/** A step service on a real RMI port whose clock the test moves, with its checker parked unless asked. */
	private static final class WatchedSlice implements AutoCloseable {
		final AtomicLong clock = new AtomicLong(1_000);
		final List<LoadStepServiceImpl> releases = new CopyOnWriteArrayList<>();
		final LoadStepServiceImpl service;
		final String threadName;
		private final String stepId;
		private final String originalHost = System.getProperty("java.rmi.server.hostname");
		private final WarningCapture warnings = new WarningCapture();
		private final org.apache.logging.log4j.core.Logger errors = LoggerContext.getContext(false).getLogger(Loggers.ERR.getName());
		private final Level errorsLevel = errors.getLevel();

		WatchedSlice(final LoadStep local, final long checkPeriodNanos) throws Exception {
			System.setProperty("java.rmi.server.hostname", "127.0.0.1");
			warnings.start();
			errors.addAppender(warnings);
			errors.setLevel(Level.WARN);
			stepId = local.loadStepId();
			threadName = "spt-entry-watchdog-" + stepId;
			final Config config = TestConfigBuilder.config();
			config.val("load-step-id", stepId);
			@SuppressWarnings("unchecked")
			final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
			when(factory.id()).thenReturn(STEP_TYPE);
			when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(local);
			service = new LoadStepServiceImpl(
							freePort(), List.of(factory), STEP_TYPE, config, Collections.emptyList(), mock(MetricsManager.class),
							releases::add, ENTRY_SILENCE_NANOS, checkPeriodNanos, clock::get);
		}

		void pass(final long nanos) {
			clock.addAndGet(nanos);
		}

		/** This slice's only: a line of an earlier slice can still be on its way through the async logger. */
		long silenceWarnings() {
			return warnings.messages.stream()
							.filter(message -> message.contains(ENTRY_SILENCE_WARNING) && message.contains(stepId))
							.count();
		}

		/** The engine's loggers are asynchronous. */
		void awaitWarning(final String... parts) throws InterruptedException {
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (warnings.messages.stream().noneMatch(message -> List.of(parts).stream().allMatch(message::contains))) {
				assertTrue(System.nanoTime() < deadline, "no warning with " + List.of(parts) + " in " + warnings.messages);
				Thread.sleep(5);
			}
		}

		@Override
		public void close() throws Exception {
			try {
				service.close();
			} finally {
				errors.removeAppender(warnings);
				errors.setLevel(errorsLevel);
				warnings.stop();
				ServiceUtil.shutdown();
				restoreHost(originalHost);
			}
		}
	}

	private static final class WarningCapture extends AbstractAppender {
		final List<String> messages = new CopyOnWriteArrayList<>();

		WarningCapture() {
			super("entryWatchdogWarnings", null, null, true, Property.EMPTY_ARRAY);
		}

		@Override
		public void append(final LogEvent event) {
			if (Level.WARN.equals(event.getLevel())) {
				messages.add(event.getMessage().getFormattedMessage());
			}
		}
	}

	private static LoadStep localStep(final String stepId) throws RemoteException {
		final LoadStep local = mock(LoadStep.class);
		when(local.loadStepId()).thenReturn(stepId);
		return local;
	}

	@Test
	@DisplayName("A started slice called once and then silent for the limit is stopped and closed, with one warning")
	void aSliceSilentForTheLimitIsStoppedAndClosedOnce() throws Exception {
		final LoadStep local = localStep("svc-silent");
		try (WatchedSlice slice = new WatchedSlice(local, NO_CHECK_NANOS)) {
			slice.service.start();
			slice.service.metricsSnapshots();

			slice.pass(ENTRY_SILENCE_NANOS);
			assertFalse(slice.service.stopIfEntryIsSilent(), "exactly the limit is not past it");
			verify(local, never()).stop();

			slice.pass(1);
			assertTrue(slice.service.stopIfEntryIsSilent());
			assertFalse(slice.service.stopIfEntryIsSilent(), "it gives up once");

			verify(local).stop();
			verify(local).close();
			assertTrue(slice.service.isClosed());
			assertFalse(slice.service.isRunning());
			assertEquals(List.of(slice.service), slice.releases);
			slice.awaitWarning(ENTRY_SILENCE_WARNING, "120 s", "svc-silent");
			assertEquals(1, slice.silenceWarnings());
			assertNoLiveThread(slice.threadName);
		}
	}

	@Test
	@DisplayName("The checker finds a silent slice by itself and then ends")
	void theCheckerFindsASilentSliceByItself() throws Exception {
		final LoadStep local = localStep("svc-checker");
		try (WatchedSlice slice = new WatchedSlice(local, TimeUnit.MILLISECONDS.toNanos(5))) {
			slice.service.start();
			slice.service.runId();
			slice.pass(ENTRY_SILENCE_NANOS + 1);
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
			while (!slice.service.isClosed() && System.nanoTime() < deadline) {
				Thread.sleep(5);
			}
			assertTrue(slice.service.isClosed());
			assertNoLiveThread(slice.threadName);
			slice.awaitWarning(ENTRY_SILENCE_WARNING);
			assertEquals(1, slice.silenceWarnings());
		}
	}

	@Test
	@DisplayName("Every kind of call from the entry keeps a slice open")
	void callsFromTheEntryKeepASliceOpen() throws Throwable {
		final LoadStep local = localStep("svc-called");
		when(local.await(anyLong(), any())).thenReturn(false);
		try (WatchedSlice slice = new WatchedSlice(local, NO_CHECK_NANOS)) {
			slice.service.start();
			final List<org.junit.jupiter.api.function.Executable> calls = List.of(
							slice.service::metricsSnapshots,
							() -> slice.service.await(1, TimeUnit.MILLISECONDS),
							() -> slice.service.preparationStatus(1L),
							slice.service::isDeleteInventoryPreValidationComplete,
							slice.service::isDispatchedOperationsDrainCompleteForStepStop,
							slice.service::durationAwaitStatus,
							slice.service::loadStepId,
							slice.service::runId);
			for (final var call : calls) {
				call.execute();
				slice.pass(ENTRY_SILENCE_NANOS);
				assertFalse(slice.service.stopIfEntryIsSilent());
			}
			verify(local, never()).stop();
			assertTrue(slice.service.isRunning());
			assertEquals(0, slice.silenceWarnings());
		}
	}

	@Test
	@DisplayName("A long call in flight counts as contact until it ends")
	void aLongCallInFlightCountsAsContact() throws Exception {
		final LoadStep local = localStep("svc-long-call");
		final CountDownLatch entered = new CountDownLatch(1);
		final CountDownLatch release = new CountDownLatch(1);
		doAnswer(invocation -> {
			entered.countDown();
			assertTrue(release.await(30, TimeUnit.SECONDS));
			return null;
		}).when(local).drainDispatchedOperationsForStepStop(anyLong());
		final var pool = Executors.newSingleThreadExecutor();
		try (WatchedSlice slice = new WatchedSlice(local, NO_CHECK_NANOS)) {
			slice.service.start();
			final Future<?> drain = pool.submit(() -> {
				slice.service.drainDispatchedOperationsForStepStop(1L);
				return null;
			});
			assertTrue(entered.await(10, TimeUnit.SECONDS));
			slice.pass(3 * ENTRY_SILENCE_NANOS);
			assertFalse(slice.service.stopIfEntryIsSilent(), "the call is running");

			release.countDown();
			drain.get(10, TimeUnit.SECONDS);
			assertFalse(slice.service.stopIfEntryIsSilent(), "the silence starts when the call ends");
			slice.pass(ENTRY_SILENCE_NANOS + 1);
			assertTrue(slice.service.stopIfEntryIsSilent());
		} finally {
			release.countDown();
			pool.shutdownNow();
		}
	}

	@Test
	@DisplayName("A slice with no call since its start is left alone: the entry may still be starting others")
	void aSliceWithNoCallSinceItsStartIsLeftAlone() throws Exception {
		final LoadStep local = localStep("svc-start-phase");
		try (WatchedSlice slice = new WatchedSlice(local, NO_CHECK_NANOS)) {
			slice.service.loadStepId();
			slice.service.start();
			slice.pass(5 * ENTRY_SILENCE_NANOS);
			assertFalse(slice.service.stopIfEntryIsSilent());
			verify(local, never()).stop();
			assertTrue(slice.service.isRunning());
		}
	}

	@Test
	@DisplayName("A slice never started is left alone and has no checker")
	void aSliceNeverStartedIsLeftAlone() throws Exception {
		final LoadStep local = localStep("svc-unstarted");
		try (WatchedSlice slice = new WatchedSlice(local, NO_CHECK_NANOS)) {
			slice.service.metricsSnapshots();
			slice.pass(5 * ENTRY_SILENCE_NANOS);
			assertFalse(slice.service.stopIfEntryIsSilent());
			verify(local, never()).close();
			assertNoLiveThread(slice.threadName);
		}
	}

	@Test
	@DisplayName("A stopped slice is left alone and stays open")
	void aStoppedSliceIsLeftAlone() throws Exception {
		final LoadStep local = localStep("svc-stopped");
		try (WatchedSlice slice = new WatchedSlice(local, NO_CHECK_NANOS)) {
			slice.service.start();
			slice.service.metricsSnapshots();
			slice.service.stop();
			slice.pass(5 * ENTRY_SILENCE_NANOS);
			assertFalse(slice.service.stopIfEntryIsSilent());
			verify(local, never()).close();
			assertFalse(slice.service.isClosed());
			assertEquals(0, slice.silenceWarnings());
		}
	}

	@Test
	@DisplayName("A restarted slice is watched again only after a call since the new start")
	void aRestartedSliceWaitsForACallAfterTheNewStart() throws Exception {
		final LoadStep local = localStep("svc-restart");
		try (WatchedSlice slice = new WatchedSlice(local, NO_CHECK_NANOS)) {
			slice.service.start();
			slice.service.metricsSnapshots();
			slice.service.stop();
			slice.service.start();
			slice.pass(2 * ENTRY_SILENCE_NANOS);
			assertFalse(slice.service.stopIfEntryIsSilent());
			slice.service.metricsSnapshots();
			slice.pass(ENTRY_SILENCE_NANOS + 1);
			assertTrue(slice.service.stopIfEntryIsSilent());
		}
	}

	@Test
	@DisplayName("The worker's own reads neither arm the checker nor count as contact")
	void theWorkersOwnReadsAreNotContact() throws Exception {
		final LoadStep local = localStep("svc-own-reads");
		try (WatchedSlice slice = new WatchedSlice(local, NO_CHECK_NANOS)) {
			slice.service.start();
			slice.service.sliceRunId();
			slice.service.sliceStepId();
			slice.service.isRunning();
			slice.pass(2 * ENTRY_SILENCE_NANOS);
			assertFalse(slice.service.stopIfEntryIsSilent(), "not armed by the worker's reads");

			slice.service.metricsSnapshots();
			slice.pass(ENTRY_SILENCE_NANOS);
			slice.service.sliceRunId();
			slice.service.sliceStepId();
			slice.service.isRunning();
			slice.pass(1);
			assertTrue(slice.service.stopIfEntryIsSilent(), "the silence is not reset by them");
		}
	}

	@Test
	@DisplayName("A failed stop and a failed close are logged and the checker survives them")
	void aFailedStopAndCloseAreLogged() throws Exception {
		final LoadStep local = localStep("svc-failing");
		doThrow(new IllegalStateException("stop failed")).doReturn(local).when(local).stop();
		doThrow(new IOException("close failed")).doNothing().when(local).close();
		try (WatchedSlice slice = new WatchedSlice(local, NO_CHECK_NANOS)) {
			slice.service.start();
			slice.service.metricsSnapshots();
			slice.pass(ENTRY_SILENCE_NANOS + 1);
			assertDoesNotThrow(() -> assertTrue(slice.service.stopIfEntryIsSilent()));
			slice.awaitWarning(ENTRY_SILENCE_WARNING);
			slice.awaitWarning("Failed to stop step");
			slice.awaitWarning("Failed to close step");
			assertEquals(1, slice.silenceWarnings());
		}
	}

	@Test
	@DisplayName("A DELETE duration slice whose close will be retried stays exported after the checker gave up")
	void aSliceAwaitingACloseRetryStaysExportedAfterTheCheckerGaveUp() throws Exception {
		final LoadStepContext context = mock(LoadStepContext.class);
		doThrow(new IllegalStateException("injected admission close failure"))
						.when(context).closeOperationAdmissionForStepStop();
		final Config config = sliceConfig("delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", true);
		config.val("load-step-limit-time", "60s");
		config.val("load-op-wait-limit", 1);
		final FailingSlice local = new FailingSlice(config, context);
		try (WatchedSlice slice = new WatchedSlice(local, NO_CHECK_NANOS)) {
			slice.service.start();
			slice.service.metricsSnapshots();
			slice.pass(ENTRY_SILENCE_NANOS + 1);

			assertTrue(slice.service.stopIfEntryIsSilent());

			assertEquals(List.of(), slice.releases, "still open for the close the worker's shutdown retries");
			assertFalse(slice.service.isRunning());
			for (var attempt = 2; attempt < FailingSlice.closeAttemptLimit(); attempt++) {
				assertThrows(RuntimeException.class, slice.service::close, "close attempt " + attempt);
			}
			assertThrows(RuntimeException.class, slice.service::close, "the last close attempt");
			assertEquals(List.of(slice.service), slice.releases);
			assertNoLiveThread(slice.threadName);
		}
	}

	@Test
	@DisplayName("Giving up on the entry racing a late stop and close stops and closes the slice once")
	void givingUpRacingLateControllerCallsStopsAndClosesOnce() throws Exception {
		final var pool = Executors.newFixedThreadPool(4);
		try {
			for (var round = 0; round < 40; round++) {
				final LoadStep local = localStep("svc-race-" + round);
				try (WatchedSlice slice = new WatchedSlice(local, NO_CHECK_NANOS)) {
					slice.service.start();
					slice.service.metricsSnapshots();
					slice.pass(ENTRY_SILENCE_NANOS + 1);
					final var go = new CountDownLatch(1);
					final List<Future<?>> racers = new ArrayList<>();
					racers.add(pool.submit(() -> {
						go.await();
						return slice.service.stopIfEntryIsSilent();
					}));
					racers.add(pool.submit(() -> {
						go.await();
						return slice.service.stop();
					}));
					racers.add(pool.submit(() -> {
						go.await();
						slice.service.close();
						return null;
					}));
					racers.add(pool.submit(() -> {
						go.await();
						slice.service.close();
						return null;
					}));
					go.countDown();
					for (final Future<?> racer : racers) {
						racer.get(30, TimeUnit.SECONDS);
					}
					verify(local, times(1)).stop();
					verify(local, times(1)).close();
					assertEquals(List.of(slice.service), slice.releases);
					assertTrue(slice.silenceWarnings() <= 1);
					assertNoLiveThread(slice.threadName);
				}
			}
		} finally {
			pool.shutdownNow();
		}
	}

	@Test
	@DisplayName("The silence limit is 120 s unless the test property holds a positive number")
	void theSilenceLimitIs120SecondsUnlessTheTestPropertyIsValid() {
		final String property = LoadStepServiceImpl.ENTRY_SILENCE_TIMEOUT_PROPERTY;
		final String original = System.getProperty(property);
		try {
			System.clearProperty(property);
			assertEquals(120_000L, LoadStepServiceImpl.entrySilenceTimeoutMillis());
			for (final String invalid : List.of("soon", "", "0", "-5")) {
				System.setProperty(property, invalid);
				assertEquals(120_000L, LoadStepServiceImpl.entrySilenceTimeoutMillis(), invalid);
			}
			System.setProperty(property, "1500");
			assertEquals(1500L, LoadStepServiceImpl.entrySilenceTimeoutMillis());
		} finally {
			if (original == null) {
				System.clearProperty(property);
			} else {
				System.setProperty(property, original);
			}
		}
	}

	private static Config sliceConfig(final String opType) {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "svc-release-" + opType + "-" + UUID.randomUUID());
		config.val("load-op-type", opType);
		return config;
	}

	private static int freePort() throws IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		}
	}

	private static void restoreHost(final String originalHost) {
		if (originalHost == null) {
			System.clearProperty("java.rmi.server.hostname");
		} else {
			System.setProperty("java.rmi.server.hostname", originalHost);
		}
	}

	/** A real local slice over mocked contexts, as a worker's step service holds one. */
	private static final class FailingSlice extends LoadStepLocalBase {
		private final List<LoadStepContext> contexts;

		FailingSlice(final Config config, final LoadStepContext... contexts) {
			super(config, List.<Extension> of(), List.<Config> of(), mock(MetricsManager.class));
			this.contexts = List.of(contexts);
			stepContexts.addAll(this.contexts);
		}

		static int closeAttemptLimit() {
			return DURATION_RUN_CLOSE_ATTEMPT_LIMIT;
		}

		@Override
		public String getTypeName() {
			return "release-test";
		}

		@Override
		protected void init() {
			stepContexts.clear();
			stepContexts.addAll(contexts);
		}
	}

	@Test
	@DisplayName("A real RMI drain remains responsive beyond the shipped response timeout")
	void realRmiLongDrainUsesOneBackgroundAttemptAndCannotShutdownEarly() throws Exception {
		final LoadStep local = mock(LoadStep.class);
		when(local.loadStepId()).thenReturn("svc-long-drain");
		final CountDownLatch drainEntered = new CountDownLatch(1);
		final CountDownLatch releaseDrain = new CountDownLatch(1);
		doAnswer(invocation -> {
			drainEntered.countDown();
			assertTrue(releaseDrain.await(15, TimeUnit.SECONDS));
			return null;
		}).when(local).drainDispatchedOperationsForStepStop(anyLong());

		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		LoadStepServiceImpl service = null;
		try {
			final int port;
			try (ServerSocket socket = new ServerSocket(0)) {
				socket.setReuseAddress(true);
				port = socket.getLocalPort();
			}
			service = newServiceWith(local, port);
			final LoadStepService remote = ServiceUtil.resolve(
							"127.0.0.1", port, service.name(), LoadStepService.class);

			remote.startDispatchedOperationsDrainForStepStop(TimeUnit.SECONDS.toNanos(30));
			assertTrue(drainEntered.await(1, TimeUnit.SECONDS));
			remote.startDispatchedOperationsDrainForStepStop(TimeUnit.SECONDS.toNanos(30));
			assertFalse(remote.isDispatchedOperationsDrainCompleteForStepStop());

			Thread.sleep(TimeUnit.SECONDS.toMillis(10) + 100);
			assertFalse(remote.isDispatchedOperationsDrainCompleteForStepStop());
			verify(local, never()).shutdown();
			releaseDrain.countDown();
			final long completionDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
			while (!remote.isDispatchedOperationsDrainCompleteForStepStop()
							&& System.nanoTime() < completionDeadline) {
				Thread.sleep(10);
			}
			assertTrue(remote.isDispatchedOperationsDrainCompleteForStepStop());
			assertNoLiveThread("spt-delete-service-drain-svc-long-drain");
			remote.shutdown();
			verify(local).drainDispatchedOperationsForStepStop(anyLong());
			verify(local).shutdown();
		} finally {
			releaseDrain.countDown();
			if (service != null) {
				assertDoesNotThrow(service::doClose);
			}
			ServiceUtil.shutdown();
			if (originalHost == null) {
				System.clearProperty("java.rmi.server.hostname");
			} else {
				System.setProperty("java.rmi.server.hostname", originalHost);
			}
		}
	}

	private static void assertNoLiveThread(final String exactName) throws InterruptedException {
		final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
		boolean live;
		do {
			live = Thread.getAllStackTraces().keySet().stream()
							.anyMatch(thread -> thread.isAlive() && exactName.equals(thread.getName()));
			if (!live) {
				return;
			}
			Thread.sleep(10);
		} while (System.nanoTime() < deadlineNanos);
		fail("lifecycle thread remains live: " + exactName);
	}

	private static LoadStepServiceImpl newServiceWith(final LoadStep loadStep, final int port) {
		final Config baseConfig = TestConfigBuilder.config();
		baseConfig.val("load-step-id", "svc-test-real-rmi");

		@SuppressWarnings("unchecked")
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn(STEP_TYPE);
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(loadStep);

		return new LoadStepServiceImpl(
						port,
						List.of(factory),
						STEP_TYPE,
						baseConfig,
						Collections.emptyList(),
						mock(MetricsManager.class));
	}
}
