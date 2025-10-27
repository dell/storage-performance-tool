package com.dell.spt.base.load.step.service;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.svc.Service;
import com.dell.spt.base.svc.ServiceUtil;
import com.github.akurilov.confuse.Config;
import java.rmi.RemoteException;
import java.util.Collections;
import java.util.List;
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
}
