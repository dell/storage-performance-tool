package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.github.akurilov.commons.concurrent.AsyncRunnable;
import java.io.IOException;
import java.rmi.RemoteException;
import java.util.List;
import org.junit.jupiter.api.Test;

class TempInputTextFileSlicerTasksTest {

	@Test
	void startTasksPropagatesRemoteException() throws RemoteException {
		final AsyncRunnable failingTask = mock(AsyncRunnable.class);
		doThrow(new RemoteException("start failed")).when(failingTask).start();
		final AsyncRunnable okTask = mock(AsyncRunnable.class);
		final IOException thrown = assertThrows(
						IOException.class,
						() -> TempInputTextFileSlicer.startTasks(
										"step-test",
										List.of(failingTask, okTask),
										"source.txt"));
		assertEquals("Failed to start async task for input file source.txt", thrown.getMessage());
		assertEquals(0, thrown.getSuppressed().length);
		verify(failingTask).start();
		verify(okTask).start();
	}
}
