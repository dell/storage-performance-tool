package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

final class RdmaPoolPlacementTest {

	private static final RdmaConfig CONFIG = new RdmaConfig(true, 0, false, "auto", "", "WARN");

	@Test
	void readsTheKernelsListForms() {
		assertArrayEquals(new int[]{0, 1, 2, 3
		}, RdmaPoolPlacement.parseCpuList("0-3\n"));
		assertArrayEquals(new int[]{0, 2, 4
		}, RdmaPoolPlacement.parseCpuList("0,2,4"));
		assertArrayEquals(new int[]{1, 8, 9, 12
		}, RdmaPoolPlacement.parseCpuList("1,8-9,12\n"));
		assertArrayEquals(new int[]{}, RdmaPoolPlacement.parseCpuList("\n"));
	}

	@Test
	void refusesWhatIsNotAList() {
		assertThrows(IllegalArgumentException.class, () -> RdmaPoolPlacement.parseCpuList("zero"));
		assertThrows(IllegalArgumentException.class, () -> RdmaPoolPlacement.parseCpuList("3-1"));
		assertThrows(IllegalArgumentException.class, () -> RdmaPoolPlacement.parseCpuList("1,,2"));
		assertThrows(IllegalArgumentException.class, () -> RdmaPoolPlacement.parseCpuList("0,"));
		assertThrows(IllegalArgumentException.class, () -> RdmaPoolPlacement.parseCpuList("0-2147483647"));
		assertThrows(IllegalArgumentException.class, () -> RdmaPoolPlacement.parseCpuList("2147483647"));
	}

	@Test
	void choosesTheNodesCpusOnlyWithSeveralNodes(@TempDir final Path nodes) throws Exception {
		Files.createDirectories(nodes.resolve("node1"));
		Files.writeString(nodes.resolve("node1/cpulist"), "4-5\n");
		Files.createFile(nodes.resolve("possible"));
		assertNull(RdmaPoolPlacement.cpusOf("step", "nic", 1, nodes), "one node, nothing to choose");

		Files.createDirectories(nodes.resolve("node0"));
		Files.writeString(nodes.resolve("node0/cpulist"), "0-3\n");
		assertArrayEquals(new int[]{4, 5
		}, RdmaPoolPlacement.cpusOf("step", "nic", 1, nodes));
		assertNull(RdmaPoolPlacement.cpusOf("step", "nic", null, nodes), "no node known");
		assertNull(RdmaPoolPlacement.cpusOf("step", "nic", 2, nodes), "no such node");
	}

	@Test
	void aNodeWithoutCpusAndAMissingDirectoryPlaceNothing(@TempDir final Path nodes) throws Exception {
		assertNull(RdmaPoolPlacement.cpusOf("step", "nic", 0, nodes.resolve("absent")));
		Files.createDirectories(nodes.resolve("node0"));
		Files.createDirectories(nodes.resolve("node1"));
		Files.writeString(nodes.resolve("node0/cpulist"), "\n");
		Files.writeString(nodes.resolve("node1/cpulist"), "4\n");
		assertNull(RdmaPoolPlacement.cpusOf("step", "nic", 0, nodes));
	}

	@Test
	void anAllocatorIsLeftAloneWhenThereIsNothingToPlace(@TempDir final Path nodes) {
		final IntFunction<ByteBuffer> allocator = ByteBuffer::allocateDirect;
		assertSame(allocator, RdmaPoolPlacement.allocator("step", "nic", 0, nodes, new FakeRdmaTransport(CONFIG), allocator));
	}

	@Test
	void theAllocationRunsOnTheNodesCpusThatTheThreadMayUseAndAFailureStillRestores(@TempDir final Path nodes) throws Exception {
		Files.createDirectories(nodes.resolve("node0"));
		Files.createDirectories(nodes.resolve("node1"));
		Files.writeString(nodes.resolve("node0/cpulist"), "0-3\n");
		Files.writeString(nodes.resolve("node1/cpulist"), "4-7\n");
		final var transport = new FakeRdmaTransport(CONFIG);
		transport.setThreadCpus(2, 3, 4);
		final var during = new AtomicReference<int[]>();
		final var placed = RdmaPoolPlacement.allocator("step", "nic", 0, nodes, transport, size -> {
			during.set(transport.currentThreadCpus());
			throw new OutOfMemoryError("direct");
		});

		assertThrows(OutOfMemoryError.class, () -> placed.apply(4096));

		assertArrayEquals(new int[]{2, 3
		}, during.get());
		assertArrayEquals(new int[]{2, 3, 4
		}, transport.currentThreadCpus());
		assertEquals(2, transport.getCpusSet().size());
	}

	@Test
	void theNativeCallsChangeOnlyTheCallingThread() throws Exception {
		assumeTrue(RdmaTransport.isNativeAvailable(), "the native library is not on this host");
		final var transport = new RdmaTransport(CONFIG);
		final int[] main = transport.currentThreadCpus();
		final var failure = new AtomicReference<Throwable>();
		final var thread = new Thread(() -> {
			try {
				final int[] before = transport.currentThreadCpus();
				assertEquals(0, transport.setCurrentThreadCpus(new int[]{before[0]
				}));
				assertArrayEquals(new int[]{before[0]
				}, transport.currentThreadCpus());
				assertEquals(0, transport.setCurrentThreadCpus(before));
				assertArrayEquals(before, transport.currentThreadCpus());
				final var started = new AtomicReference<int[]>();
				final var later = new Thread(() -> started.set(transport.currentThreadCpus()));
				later.start();
				later.join();
				assertArrayEquals(before, started.get(), "a thread started after the restore has the whole mask");
			} catch (final Throwable t) {
				failure.set(t);
			}
		});
		thread.start();
		thread.join();
		assertNull(failure.get());
		assertArrayEquals(main, transport.currentThreadCpus());
	}
}
