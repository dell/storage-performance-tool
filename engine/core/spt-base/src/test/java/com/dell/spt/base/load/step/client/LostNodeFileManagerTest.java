package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import java.io.EOFException;
import java.rmi.RemoteException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;

/** A worker's file manager, guarded: every call reaches the worker until its slice is lost, and none after. */
class LostNodeFileManagerTest {

	private final AtomicBoolean lost = new AtomicBoolean();

	@Test
	void callsReachTheWorkerUntilItsSliceIsLost() throws Exception {
		final FileManagerService worker = mock(FileManagerService.class);
		when(worker.readFromFile("f", 0)).thenReturn(new byte[]{1
		});
		when(worker.fileSize("f")).thenReturn(7L);
		when(worker.logFileName("logger", "step")).thenReturn("/log/step/f");
		final FileManager guarded = LostNodeFileManager.guard(worker, lost::get);

		assertArrayEquals(new byte[]{1
		}, guarded.readFromFile("f", 0));
		assertEquals(7L, guarded.fileSize("f"));
		assertEquals("/log/step/f", guarded.logFileName("logger", "step"));
		guarded.deleteFile("f");

		verify(worker).deleteFile("f");
	}

	@Test
	void aLostSlicesNodeGetsNoFurtherCallAndItsFilesReadAsEnded() throws Exception {
		final FileManagerService worker = mock(FileManagerService.class);
		final FileManager guarded = LostNodeFileManager.guard(worker, lost::get);
		lost.set(true);

		assertThrows(EOFException.class, () -> guarded.readFromFile("f", 0));
		assertEquals(0L, guarded.fileSize("f"));
		guarded.deleteFile("f");
		final RemoteException name = assertThrows(RemoteException.class, () -> guarded.logFileName("logger", "step"));
		assertTrue(name.getMessage().contains("the entry lost this node's step slice"), name.getMessage());
		assertThrows(RemoteException.class, () -> guarded.writeToFile("f", new byte[]{1
		}));
		assertThrows(RemoteException.class, guarded::newTmpFileName);

		verifyNoMoreInteractions(worker);
	}

	@Test
	void aWorkersFileManagerStaysOneAndAnotherDoesNotBecomeOne() {
		final FileManager service = LostNodeFileManager.guard(mock(FileManagerService.class), lost::get);
		final FileManager plain = LostNodeFileManager.guard(mock(FileManager.class), lost::get);

		// The collectors tell a worker's file manager from the entry's own by its type.
		assertTrue(service instanceof FileManagerService);
		assertFalse(plain instanceof FileManagerService);
		// Collectors key their maps by file manager.
		assertEquals(service, service);
		assertNotEquals(service, plain);
		assertEquals(System.identityHashCode(service), service.hashCode());
	}
}
