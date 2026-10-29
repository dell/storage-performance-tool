package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.nic.NicPathsArtifact;
import java.io.EOFException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NicPathsCollectorTest {

	private static final String LOGGER = Loggers.NIC_PATHS.getName();

	@TempDir
	Path dir;

	@Test
	void eachNodesFileIsKeptApartAndAMissingOneDoesNotFailTheStep() throws Exception {
		final Path canonical = dir.resolve(NicPathsArtifact.FILE_NAME);
		final var managers = new ArrayList<FileManager>();
		for (int i = 0; i < 4; i++) {
			final var manager = mock(FileManager.class);
			when(manager.logFileName(LOGGER, "step")).thenReturn(i == 0 ? canonical.toString() : "/node-" + i + "/nic.paths.csv");
			managers.add(manager);
		}
		Files.writeString(canonical, body("entry"));
		final byte[] remote = body("node-1").getBytes(StandardCharsets.UTF_8);
		when(managers.get(1).readFromFile("/node-1/nic.paths.csv", 0)).thenReturn(remote);
		when(managers.get(1).readFromFile("/node-1/nic.paths.csv", remote.length)).thenThrow(new EOFException());
		when(managers.get(2).readFromFile("/node-2/nic.paths.csv", 0)).thenThrow(new NoSuchFileException("/node-2/nic.paths.csv"));

		new NicPathsCollector("step", managers, NicPathsCollector.nodeNames(List.of("node-b.example:1099", "node-c.example:1099", "node-d.example:1099")),
						List.of(true, true, true, false)).close();

		assertFalse(Files.exists(canonical), "the Entry Node's file moved to node-000");
		assertEquals(body("entry"), Files.readString(dir.resolve("nic.paths.node-000.csv")));
		assertEquals(body("node-1"), Files.readString(dir.resolve("nic.paths.node-001.csv")));
		assertFalse(Files.exists(dir.resolve("nic.paths.node-002.csv")));
		assertFalse(Files.exists(dir.resolve("nic.paths.node-003.csv")));
		verify(managers.get(3), never()).readFromFile(anyString(), anyLong());
		try (final var files = Files.list(dir)) {
			assertTrue(files.noneMatch(file -> file.getFileName().toString().endsWith(".tmp")), "no staging file is left");
		}
	}

	@Test
	void aStaleNodeFileIsNeitherReplacedNorFatal() throws Exception {
		final Path canonical = dir.resolve(NicPathsArtifact.FILE_NAME);
		final var manager = mock(FileManager.class);
		when(manager.logFileName(LOGGER, "step")).thenReturn(canonical.toString());
		final var remote = mock(FileManager.class);
		when(remote.logFileName(LOGGER, "step")).thenReturn("/node-1/nic.paths.csv");
		Files.writeString(canonical, body("entry"));
		Files.writeString(dir.resolve("nic.paths.node-000.csv"), "stale");

		new NicPathsCollector("step", List.of(manager, remote), NicPathsCollector.nodeNames(List.of("node-b.example:1099")),
						List.of(true, false)).close();

		assertEquals("stale", Files.readString(dir.resolve("nic.paths.node-000.csv")));
		assertTrue(Files.exists(canonical));
	}

	private static String body(final String worker) {
		return NicPathsArtifact.HEADER + "\n" + NicPathsArtifact.rows(1, "step", worker, List.of()) + "\n";
	}
}
