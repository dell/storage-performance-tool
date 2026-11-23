package com.dell.spt.base.load.step.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;

class LoadStepManagerServiceImplTest {

	@Test
	void closeClosesAndReleasesActiveStepService() throws Exception {
		try (MockedConstruction<LoadStepServiceImpl> construction = mockConstruction(LoadStepServiceImpl.class)) {
			final var manager = new LoadStepManagerServiceImpl(1099, List.of(), mock(MetricsManager.class));
			manager.newStepService("Load", mock(Config.class), List.of());
			final var activeService = construction.constructed().getFirst();

			manager.doClose();

			verify(activeService).close();
			assertNull(manager.getStepService());
		}
	}

	@Test
	void failedChildCloseRetainsReferenceAndRejectsNewSteps() throws Exception {
		try (MockedConstruction<LoadStepServiceImpl> construction = mockConstruction(LoadStepServiceImpl.class)) {
			final var manager = new LoadStepManagerServiceImpl(1099, List.of(), mock(MetricsManager.class));
			manager.newStepService("Load", mock(Config.class), List.of());
			final var activeService = construction.constructed().getFirst();
			doThrow(new IOException("cleanup failed")).when(activeService).close();

			assertThrows(IOException.class, manager::doClose);
			assertSame(activeService, manager.getStepService());
			assertThrows(java.rmi.RemoteException.class, () -> manager.newStepService("Load", mock(Config.class), List.of()));
		}
	}

	@Test
	void aNewStepServiceLeavesTheOlderOneOpenAndBecomesTheLatest() throws Exception {
		try (MockedConstruction<LoadStepServiceImpl> construction = mockConstruction(LoadStepServiceImpl.class)) {
			final var manager = newManager();
			manager.newStepService("Load", mock(Config.class), List.of());
			manager.newStepService("Load", mock(Config.class), List.of());
			final var older = construction.constructed().get(0);
			final var newer = construction.constructed().get(1);

			verify(older, never()).close();
			verify(older, never()).stop();
			assertSame(newer, manager.getStepService());
			assertTrue(manager.hasOpenSlices());

			manager.doClose();

			verify(older).close();
			verify(newer).close();
			assertFalse(manager.hasOpenSlices());
			assertNull(manager.getStepService());
		}
	}

	@Test
	void aFailedCloseDoesNotKeepTheOtherServicesOpen() throws Exception {
		try (MockedConstruction<LoadStepServiceImpl> construction = mockConstruction(LoadStepServiceImpl.class)) {
			final var manager = newManager();
			for (var i = 0; i < 3; i++) {
				manager.newStepService("Load", mock(Config.class), List.of());
			}
			final var services = construction.constructed();
			final var first = new IOException("first");
			final var second = new IllegalStateException("second");
			doThrow(first).when(services.get(0)).close();
			doThrow(second).when(services.get(1)).close();

			assertSame(first, assertThrows(IOException.class, manager::doClose));

			assertEquals(List.of(second), List.of(first.getSuppressed()));
			verify(services.get(2)).close();
			assertTrue(manager.hasOpenSlices(), "the two whose close failed are not released");
		}
	}

	@Test
	void aReleasedServiceIsDroppedAndStaysTheLatestCreated() throws Exception {
		final List<Consumer<LoadStepServiceImpl>> hooks = new ArrayList<>();
		try (MockedConstruction<LoadStepServiceImpl> construction = mockConstruction(
						LoadStepServiceImpl.class,
						(service, context) -> hooks.add(releaseHook(context.arguments())))) {
			final var manager = newManager();
			manager.newStepService("Load", mock(Config.class), List.of());
			final var service = construction.constructed().getFirst();
			when(service.isRunning()).thenReturn(true);
			when(service.sliceRunId()).thenReturn(7L);

			hooks.getFirst().accept(service);

			assertFalse(manager.hasOpenSlices());
			assertEquals(List.of(), manager.busySliceRunIds());
			assertFalse(manager.stopSlicesOfRun(7L));
			assertSame(service, manager.getStepService());
			manager.doClose();
			verify(service, never()).close();
		}
	}

	@Test
	void onlyRunningServicesAreBusyNewestFirst() throws Exception {
		try (MockedConstruction<LoadStepServiceImpl> construction = mockConstruction(LoadStepServiceImpl.class)) {
			final var manager = newManager();
			assertEquals(List.of(), manager.busySliceRunIds());
			for (var i = 0; i < 3; i++) {
				manager.newStepService("Load", mock(Config.class), List.of());
			}
			final var services = construction.constructed();
			when(services.get(0).isRunning()).thenReturn(true);
			when(services.get(0).sliceRunId()).thenReturn(1L);
			when(services.get(1).sliceRunId()).thenReturn(2L);
			when(services.get(2).isRunning()).thenReturn(true);
			when(services.get(2).sliceRunId()).thenReturn(3L);

			assertEquals(List.of(3L, 1L), manager.busySliceRunIds());
			assertTrue(manager.hasOpenSlices());
		}
	}

	@Test
	void stoppingARunStopsItsRunningSlicesOnlyAndClosesNothing() throws Exception {
		try (MockedConstruction<LoadStepServiceImpl> construction = mockConstruction(LoadStepServiceImpl.class)) {
			final var manager = newManager();
			for (var i = 0; i < 4; i++) {
				manager.newStepService("Load", mock(Config.class), List.of());
			}
			final var services = construction.constructed();
			final var running = services.get(0);
			final var failing = services.get(1);
			final var notRunning = services.get(2);
			final var otherRun = services.get(3);
			for (final var service : List.of(running, failing, notRunning)) {
				when(service.sliceRunId()).thenReturn(5L);
			}
			when(otherRun.sliceRunId()).thenReturn(6L);
			when(running.isRunning()).thenReturn(true);
			when(failing.isRunning()).thenReturn(true);
			when(otherRun.isRunning()).thenReturn(true);
			doThrow(new IllegalStateException("stop failed")).when(failing).stop();

			assertTrue(manager.stopSlicesOfRun(5L));

			verify(running).stop();
			verify(failing).stop();
			verify(notRunning, never()).stop();
			verify(otherRun, never()).stop();
			for (final var service : services) {
				verify(service, never()).close();
			}
			assertFalse(manager.stopSlicesOfRun(9L));
			assertTrue(manager.hasOpenSlices());
		}
	}

	private static LoadStepManagerServiceImpl newManager() {
		return new LoadStepManagerServiceImpl(1099, List.of(), mock(MetricsManager.class));
	}

	@SuppressWarnings("unchecked")
	private static Consumer<LoadStepServiceImpl> releaseHook(final List<?> constructorArguments) {
		return (Consumer<LoadStepServiceImpl>) constructorArguments.stream()
						.filter(Consumer.class::isInstance)
						.findFirst()
						.orElseThrow();
	}
}
