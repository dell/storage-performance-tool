package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.dell.spt.base.load.step.LoadStep;
import java.rmi.RemoteException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class AwaitStepSliceTaskTest {

	@Test
	void constructorSetsLoadStepId() throws RemoteException {
		var stepSlice = mock(LoadStep.class);
		when(stepSlice.loadStepId()).thenReturn("test-step-1");
		var latch = new CountDownLatch(1);

		var task = new AwaitStepSliceTask(stepSlice, latch);
		assertNotNull(task);
	}

	@Test
	void constructorThrowsOnRemoteException() throws RemoteException {
		var stepSlice = mock(LoadStep.class);
		when(stepSlice.loadStepId()).thenThrow(new RemoteException("connection failed"));
		var latch = new CountDownLatch(1);

		assertThrows(IllegalStateException.class, () -> new AwaitStepSliceTask(stepSlice, latch));
	}

	@Test
	void doWorkCountsDownWhenAwaitReturnsTrue() throws Exception {
		var stepSlice = mock(LoadStep.class);
		when(stepSlice.loadStepId()).thenReturn("test-step-2");
		when(stepSlice.await(anyLong(), any(TimeUnit.class))).thenReturn(true);
		var latch = new CountDownLatch(1);

		var task = new AwaitStepSliceTask(stepSlice, latch);
		task.start();

		// The task should count down the latch and stop itself
		assertTrue(latch.await(5, TimeUnit.SECONDS), "Latch should have been counted down");
		task.close();
	}

	@Test
	void doWorkDoesNotCountDownWhenAwaitReturnsFalse() throws Exception {
		var stepSlice = mock(LoadStep.class);
		when(stepSlice.loadStepId()).thenReturn("test-step-3");
		when(stepSlice.await(anyLong(), any(TimeUnit.class))).thenReturn(false);
		var latch = new CountDownLatch(1);

		var task = new AwaitStepSliceTask(stepSlice, latch);
		task.start();

		// Give the task a few iterations to run
		Thread.sleep(300);
		assertEquals(1, latch.getCount(), "Latch should not have been counted down");

		// Verify await was called multiple times (polling)
		verify(stepSlice, atLeast(2)).await(anyLong(), any(TimeUnit.class));
		task.close();
	}

	@Test
	void doWorkHandlesIllegalStateException() throws Exception {
		var stepSlice = mock(LoadStep.class);
		when(stepSlice.loadStepId()).thenReturn("test-step-4");
		when(stepSlice.await(anyLong(), any(TimeUnit.class)))
						.thenThrow(new IllegalStateException("step not started"));
		var latch = new CountDownLatch(1);

		var task = new AwaitStepSliceTask(stepSlice, latch);
		task.start();

		// Give the task time to encounter the exception and keep running
		Thread.sleep(300);
		assertEquals(1, latch.getCount(), "Latch should not have been counted down");
		task.close();
	}
}
