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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
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
