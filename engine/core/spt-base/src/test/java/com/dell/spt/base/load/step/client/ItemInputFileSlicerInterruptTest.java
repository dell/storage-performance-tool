package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.load.step.file.FileManager;
import com.github.akurilov.commons.io.Input;
import java.util.List;
import org.junit.jupiter.api.Test;

class ItemInputFileSlicerInterruptTest {

	@Test
	@SuppressWarnings("unchecked")
	void anInterruptStopsTheDistributionAndRemovesItsSlices() throws Exception {
		final FileManager fileManager = mock(FileManager.class);
		when(fileManager.newTmpFileName()).thenReturn("slice-0");
		final Input<Item> items = mock(Input.class);
		when(items.get(any(), anyInt())).thenReturn(1);

		Thread.currentThread().interrupt();
		try {
			assertThrows(
							InterruptedException.class,
							() -> new ItemInputFileSlicer("step", List.of(fileManager), List.of(TestConfigBuilder.config()), items, 1));
		} finally {
			Thread.interrupted();
		}

		verify(items, never()).get(any(), anyInt());
		verify(fileManager, never()).writeToFile(anyString(), any());
		verify(fileManager).deleteFile("slice-0");
	}
}
