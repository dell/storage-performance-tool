package com.dell.spt.base.control;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.control.run.RunServlet;
import com.dell.spt.base.svc.Service;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class NodeShutdownCoordinatorTest {

	@Test
	void joinsActiveRunBeforeClosingServicesAndRunsOnlyOnce() throws Exception {
		final var events = new ArrayList<String>();
		final var runServlet = mock(RunServlet.class);
		final var firstService = mock(Service.class);
		final var secondService = mock(Service.class);
		when(runServlet.stopActiveRunAndAwait(
						NodeShutdownCoordinator.ACTIVE_RUN_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS))
						.thenAnswer(invocation -> {
							events.add("run-stopped");
							return true;
						});
		doAnswer(invocation -> {
			events.add("first-service-closed");
			return null;
		}).when(firstService).close();
		doAnswer(invocation -> {
			events.add("second-service-closed");
			return null;
		}).when(secondService).close();
		final var coordinator = new NodeShutdownCoordinator(runServlet, List.of(firstService, secondService));

		coordinator.shutdown();
		coordinator.shutdown();

		assertEquals(
						List.of("run-stopped", "first-service-closed", "second-service-closed"),
						events);
		verify(runServlet).stopActiveRunAndAwait(
						NodeShutdownCoordinator.ACTIVE_RUN_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		verify(firstService).close();
		verify(secondService).close();
	}

	@Test
	void activeRunTimeoutStillClosesServices() throws Exception {
		final var runServlet = mock(RunServlet.class);
		final var service = mock(Service.class);
		when(runServlet.stopActiveRunAndAwait(
						NodeShutdownCoordinator.ACTIVE_RUN_STOP_TIMEOUT_SECONDS, TimeUnit.SECONDS))
						.thenReturn(false);

		new NodeShutdownCoordinator(runServlet, List.of(service)).shutdown();

		verify(service).close();
	}

	@Test
	void serviceErrorDoesNotPreventRemainingCleanup() throws Exception {
		final var firstService = mock(Service.class);
		final var secondService = mock(Service.class);
		doThrow(new LinkageError("extension unload race")).when(firstService).close();

		new NodeShutdownCoordinator(List.of(firstService, secondService)).shutdown();

		verify(firstService).close();
		verify(secondService).close();
	}
}
