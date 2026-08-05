package com.dell.spt.base.load.step.service;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;

import com.dell.spt.base.metrics.MetricsManager;
import com.github.akurilov.confuse.Config;
import java.io.IOException;
import java.util.List;
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
}
