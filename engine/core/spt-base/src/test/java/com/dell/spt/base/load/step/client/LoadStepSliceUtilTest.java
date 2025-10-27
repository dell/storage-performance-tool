package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.load.step.LoadStep;
import java.rmi.RemoteException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link LoadStepSliceUtil} communication helpers.
 */
public class LoadStepSliceUtilTest {

	@Test
	public void awaitRetriesOnRemoteExceptionsAndEventuallySucceeds() throws Exception {
		final LoadStep slice = mock(LoadStep.class);
		when(slice.loadStepId()).thenReturn("slice-1");
		when(slice.await(anyLong(), any(TimeUnit.class)))
						.thenThrow(new RemoteException("transient"))
						.thenReturn(true);

		final boolean completed = LoadStepSliceUtil.await(slice, 5, TimeUnit.MILLISECONDS);

		assertTrue(completed);
		verify(slice, times(2)).await(anyLong(), any(TimeUnit.class));
	}

	@Test
	public void awaitReturnsFalseWhenIdLookupFails() throws Exception {
		final LoadStep slice = mock(LoadStep.class);
		when(slice.loadStepId()).thenThrow(new RemoteException("id"));

		final boolean completed = LoadStepSliceUtil.await(slice, 1, TimeUnit.MILLISECONDS);

		assertFalse(completed);
		verify(slice, times(0)).await(anyLong(), any(TimeUnit.class));
	}
}
