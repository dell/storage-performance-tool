package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class RdmaLanePlacementTest {

	@Test
	void readsTheKernelsListForms() {
		assertArrayEquals(new int[]{0, 1, 2, 3
		}, RdmaLanePlacement.parseCpuList("0-3\n"));
		assertArrayEquals(new int[]{0, 2, 4
		}, RdmaLanePlacement.parseCpuList("0,2,4"));
		assertArrayEquals(new int[]{1, 8, 9, 12
		}, RdmaLanePlacement.parseCpuList("1,8-9,12\n"));
		assertArrayEquals(new int[]{}, RdmaLanePlacement.parseCpuList("\n"));
	}

	@Test
	void refusesWhatIsNotAList() {
		assertThrows(IllegalArgumentException.class, () -> RdmaLanePlacement.parseCpuList("zero"));
		assertThrows(IllegalArgumentException.class, () -> RdmaLanePlacement.parseCpuList("3-1"));
		assertThrows(IllegalArgumentException.class, () -> RdmaLanePlacement.parseCpuList("1,,2"));
	}

	@Test
	void choosesTheNodesCpusOnlyWithSeveralNodes(@TempDir final Path nodes) throws Exception {
		Files.createDirectories(nodes.resolve("node1"));
		Files.writeString(nodes.resolve("node1/cpulist"), "4-5\n");
		Files.createFile(nodes.resolve("possible"));
		assertNull(RdmaLanePlacement.cpusFor("step", "nic", 1, nodes), "one node, nothing to choose");

		Files.createDirectories(nodes.resolve("node0"));
		Files.writeString(nodes.resolve("node0/cpulist"), "0-3\n");
		assertArrayEquals(new int[]{4, 5
		}, RdmaLanePlacement.cpusFor("step", "nic", 1, nodes));
		assertNull(RdmaLanePlacement.cpusFor("step", "nic", null, nodes), "no node known");
		assertNull(RdmaLanePlacement.cpusFor("step", "nic", 2, nodes), "no such node");
	}

	@Test
	void aNodeWithoutCpusAndAMissingDirectoryPlaceNothing(@TempDir final Path nodes) throws Exception {
		assertNull(RdmaLanePlacement.cpusFor("step", "nic", 0, nodes.resolve("absent")));
		Files.createDirectories(nodes.resolve("node0"));
		Files.createDirectories(nodes.resolve("node1"));
		Files.writeString(nodes.resolve("node0/cpulist"), "\n");
		Files.writeString(nodes.resolve("node1/cpulist"), "4\n");
		assertNull(RdmaLanePlacement.cpusFor("step", "nic", 0, nodes));
	}
}
