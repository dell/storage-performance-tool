package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.load.step.LoadStep;
import com.dell.spt.base.load.step.LoadStepBase;
import com.dell.spt.base.load.step.LoadStepFactory;
import com.dell.spt.base.load.step.local.LoadStepLocalBase;
import com.dell.spt.base.load.step.local.context.LoadStepContext;
import com.dell.spt.base.load.step.service.LoadStepService;
import com.dell.spt.base.load.step.service.LoadStepServiceImpl;
import com.dell.spt.base.metrics.MetricsManager;
import com.dell.spt.base.svc.ServiceUtil;
import com.github.akurilov.confuse.Config;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

/**
 * An Entry Node whose bounded DELETE duration cleanup gives up before it reaches the slice close
 * still leaves the worker's slice released: the worker counts the closes it is given to the same
 * limit, and the Entry Node delivers them once its own retries are used up.
 */
class DurationCleanupExhaustionReleaseTest {

	private static final String STEP_TYPE = "duration-exhaustion-release-test";

	@Test
	void aWorkerSliceIsReleasedWhenTheEntryNodeCleanupIsExhaustedBeforeAnySliceClose() throws Exception {
		final String originalHost = System.getProperty("java.rmi.server.hostname");
		System.setProperty("java.rmi.server.hostname", "127.0.0.1");
		final Config config = durationConfig();
		final LoadStepContext context = mock(LoadStepContext.class);
		// The admission close keeps failing, so the Entry Node's stop coordination fails on every
		// attempt, and its close throws before it closes any slice.
		doThrow(new IllegalStateException("injected admission close failure"))
						.when(context).closeOperationAdmissionForStepStop();
		final Slice local = new Slice(config, context);
		try {
			final int port = freePort();
			final LoadStepServiceImpl service = newService(local, port);
			service.start();
			final LoadStepService remote = ServiceUtil.resolve("127.0.0.1", port, service.name(), LoadStepService.class);
			final TestLoadStepClient client = new TestLoadStepClient(config, List.of(), List.of(), mock(MetricsManager.class));
			addSlice(client, remote);
			assertDoesNotThrow(client::stop);

			assertNotNull(closeAfterRun(client), "the Entry Node's cleanup fails");

			assertThrows(RemoteException.class, remote::loadStepId);
			assertFalse(List.of(LocateRegistry.getRegistry("127.0.0.1", port).list()).contains(service.name()));
			assertFalse(local.awaitsCleanupRetry());
			// As after a standalone run's last retry, a slice whose admission never closed keeps its contexts.
			verify(context, never()).close();
			assertDoesNotThrow(service::close);
			assertTrue(service.isClosed());
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
	void anExhaustedCleanupDeliversNoCloseToASliceThatIsAlreadyClosed() throws Exception {
		final Config config = durationConfig();
		final LoadStep slice = mock(LoadStep.class);
		when(slice.toString()).thenReturn("closed-slice");
		doThrow(new java.rmi.NoSuchObjectException("released")).when(slice).close();
		final TestLoadStepClient client = new TestLoadStepClient(config, List.of(), List.of(), mock(MetricsManager.class));
		addSlice(client, slice);

		assertDoesNotThrow(client::cancelDurationRunCleanupResourcesAfterExhaustion);

		verify(slice, org.mockito.Mockito.times(1)).close();
	}

	private static Throwable closeAfterRun(final LoadStepBase step) throws ReflectiveOperationException {
		final Method closeAfterRun = LoadStepBase.class.getDeclaredMethod("closeAfterRun");
		closeAfterRun.setAccessible(true);
		return (Throwable) closeAfterRun.invoke(step);
	}

	@SuppressWarnings("unchecked")
	private static void addSlice(final LoadStepClientBase<?> client, final LoadStep slice)
					throws ReflectiveOperationException {
		final Field field = LoadStepClientBase.class.getDeclaredField("stepSlices");
		field.setAccessible(true);
		((List<LoadStep>) field.get(client)).add(slice);
	}

	private static Config durationConfig() {
		final Config config = TestConfigBuilder.config();
		config.val("load-step-id", "duration-exhaustion-" + UUID.randomUUID());
		config.val("load-op-type", "delete");
		config.val("load-op-delete-standalone", true);
		config.val("load-op-delete-duration", true);
		config.val("load-step-limit-time", "60s");
		config.val("load-op-wait-limit", 1);
		return config;
	}

	@SuppressWarnings("unchecked")
	private static LoadStepServiceImpl newService(final LoadStep local, final int port) {
		final LoadStepFactory<LoadStep, ?> factory = mock(LoadStepFactory.class);
		when(factory.id()).thenReturn(STEP_TYPE);
		when(factory.createLocal(any(), anyList(), anyList(), any())).thenReturn(local);
		final Config serviceConfig = TestConfigBuilder.config();
		serviceConfig.val("load-step-id", "duration-exhaustion-service");
		return new LoadStepServiceImpl(
						port, List.<Extension> of(factory), STEP_TYPE, serviceConfig, List.of(), mock(MetricsManager.class));
	}

	private static int freePort() throws java.io.IOException {
		try (ServerSocket socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
	}

	/** A worker's local slice over a mocked context. */
	private static final class Slice extends LoadStepLocalBase {
		private final LoadStepContext context;

		Slice(final Config config, final LoadStepContext context) {
			super(config, List.<Extension> of(), List.<Config> of(), mock(MetricsManager.class));
			this.context = context;
			stepContexts.add(context);
		}

		@Override
		public String getTypeName() {
			return STEP_TYPE;
		}

		@Override
		protected void init() {
			stepContexts.clear();
			stepContexts.add(context);
		}
	}
}
