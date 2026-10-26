package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.load.step.service.file.FileManagerService;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.impl.BasicConfig;
import java.io.EOFException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** How {@link ItemOutputFileAggregator} collects each remote slice's item output file. */
@Timeout(30)
public class ItemOutputFileAggregatorTest {

	private static final String REMOTE_FILE = "remote-items";

	@TempDir
	Path dir;

	@Test
	void aCompleteRemoteFileIsAppendedOnceAndThenDeleted() throws Exception {
		final FileManagerService remote = remote();
		when(remote.readFromFile(REMOTE_FILE, 0)).thenReturn(bytes("a\nb\n"));
		when(remote.readFromFile(REMOTE_FILE, 4)).thenReturn(bytes("c\n"));
		when(remote.readFromFile(REMOTE_FILE, 6)).thenThrow(new EOFException());

		collect(output(), remote);

		assertEquals("a\nb\nc\n", Files.readString(output()));
		verify(remote).deleteFile(REMOTE_FILE);
	}

	@Test
	void itemsOfABucketSetKeepTheirMemberPathsAcrossUnevenNodes() throws Exception {
		final String local = "/bench-0/p/a,0,400,0/0\n/bench-1/p/b,1,400,0/0\n/bench-2/p/c,2,400,0/0\n/bench-3/p/d,3,400,0/0\n";
		Files.writeString(output(), local);
		final FileManagerService remote = remote();
		when(remote.readFromFile(REMOTE_FILE, 0)).thenReturn(bytes("/bench-2/p/e,6,400,0/0\n"));
		when(remote.readFromFile(REMOTE_FILE, 23)).thenThrow(new EOFException());

		collect(output(), remote);

		assertEquals(local + "/bench-2/p/e,6,400,0/0\n", Files.readString(output()));
	}

	@Test
	void anEmptyRemoteFileIsCompleteAndDeleted() throws Exception {
		final FileManagerService ended = remote();
		when(ended.readFromFile(REMOTE_FILE, 0)).thenThrow(new EOFException());
		final FileManagerService empty = remote();
		when(empty.readFromFile(REMOTE_FILE, 0)).thenReturn(new byte[0]);

		collect(output(), ended, empty);

		assertEquals("", Files.readString(output()));
		verify(ended).deleteFile(REMOTE_FILE);
		verify(empty).deleteFile(REMOTE_FILE);
	}

	@Test
	void aReadFailingBeforeTheFirstPartIsRetried() throws Exception {
		final FileManagerService remote = remote();
		when(remote.readFromFile(REMOTE_FILE, 0))
						.thenThrow(new IOException("injected read failure"))
						.thenReturn(bytes("a\nb\n"));
		when(remote.readFromFile(REMOTE_FILE, 4)).thenThrow(new EOFException());

		collect(output(), remote);

		assertEquals("a\nb\n", Files.readString(output()));
		verify(remote).deleteFile(REMOTE_FILE);
	}

	@Test
	void aReadFailingAfterAPartResumesFromItsOffsetWithoutDuplicates() throws Exception {
		final FileManagerService remote = remote();
		when(remote.readFromFile(REMOTE_FILE, 0)).thenReturn(bytes("a\nb\n"));
		when(remote.readFromFile(REMOTE_FILE, 4))
						.thenThrow(new IOException("injected read failure"))
						.thenReturn(bytes("c\n"));
		when(remote.readFromFile(REMOTE_FILE, 6)).thenThrow(new EOFException());

		collect(output(), remote);

		assertEquals("a\nb\nc\n", Files.readString(output()));
		verify(remote, times(1)).readFromFile(REMOTE_FILE, 0);
		verify(remote, times(2)).readFromFile(REMOTE_FILE, 4);
		verify(remote).deleteFile(REMOTE_FILE);
	}

	@Test
	void aRemoteFileWhoseReadsKeepFailingIsKeptWithWhatArrived() throws Exception {
		final FileManagerService remote = remote();
		when(remote.readFromFile(REMOTE_FILE, 0)).thenReturn(bytes("a\nb\n"));
		when(remote.readFromFile(REMOTE_FILE, 4)).thenThrow(new IOException("injected read failure"));
		final FileManagerService other = remote();
		when(other.readFromFile(REMOTE_FILE, 0)).thenReturn(bytes("x\n"));
		when(other.readFromFile(REMOTE_FILE, 2)).thenThrow(new EOFException());

		collect(output(), remote, other);

		final List<String> collected = Files.readAllLines(output());
		assertEquals(List.of("a", "b", "x"), collected.stream().sorted().toList());
		verify(remote, times(ItemOutputFileAggregator.READ_ATTEMPTS)).readFromFile(REMOTE_FILE, 4);
		verify(remote, never()).deleteFile(anyString());
		verify(other).deleteFile(REMOTE_FILE);
	}

	@Test
	void aFailedLocalWriteKeepsTheRemoteFileAndIsNotRetried() throws Exception {
		final Path full = Path.of("/dev/full");
		assumeTrue(Files.isWritable(full), "needs a device whose writes fail");
		final FileManagerService remote = remote();
		when(remote.readFromFile(REMOTE_FILE, 0)).thenReturn(bytes("a\nb\n"));

		collect(full, remote);

		verify(remote, times(1)).readFromFile(anyString(), anyLong());
		verify(remote, never()).deleteFile(anyString());
	}

	private Path output() {
		return dir.resolve("items.csv");
	}

	private static FileManagerService remote() throws IOException {
		final FileManagerService remote = mock(FileManagerService.class);
		when(remote.newTmpFileName()).thenReturn(REMOTE_FILE);
		return remote;
	}

	private static void collect(final Path output, final FileManagerService... remotes) {
		final List<FileManager> fileMgrs = new ArrayList<>();
		fileMgrs.add(FileManager.INSTANCE);
		fileMgrs.addAll(List.of(remotes));
		final Config base = TestConfigBuilder.config();
		final List<Config> slices = new ArrayList<>();
		for (int i = 0; i < fileMgrs.size(); i++) {
			slices.add(new BasicConfig(base));
		}
		new ItemOutputFileAggregator("collect-step", fileMgrs, slices, output.toString(), 0).close();
	}

	private static byte[] bytes(final String text) {
		return text.getBytes(StandardCharsets.UTF_8);
	}
}
