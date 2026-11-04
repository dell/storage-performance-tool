package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.IntFunction;
import java.util.regex.Pattern;
import org.apache.logging.log4j.Level;

/**
 * Puts a NIC's pool buffers on the NIC's NUMA node (D77). A buffer's pages land where they are first
 * touched, and registration then holds them. Allocating a direct buffer zero-fills it, so the thread
 * that allocates keeps to the node's CPUs for that call alone. Without this, the lanes tend to share a
 * node with the threads that started them, every pool lands there, and the other NICs write across the
 * socket link.
 *
 * <p>It is only for that call because a thread passes its CPU mask to the threads it starts: a lane kept
 * on the node for its whole life would also confine the event loops it happens to start and JVM-wide
 * workers such as the common pool's.
 *
 * <p>A host with one node has nothing to choose. A placement that cannot be made is one warning per NIC,
 * and its buffers land where they did before: a load never fails over this. This asks for CPUs and does
 * not bind memory, so a cgroup's {@code cpuset.mems} still has the last word.
 */
final class RdmaPoolPlacement {

	static final Path SYSFS_NODES = Path.of("/sys/devices/system/node");
	private static final Pattern NODE_DIRECTORY = Pattern.compile("node[0-9]+");
	/** More than any kernel's CPU limit, so that a corrupt list is refused and not expanded. */
	private static final int MAX_CPU = 1 << 20;

	private final String stepId;
	private final String nic;
	private final int node;
	private final int[] nodeCpus;
	private final RdmaTransport transport;
	private final IntFunction<ByteBuffer> allocator;
	private volatile boolean gaveUp;
	private volatile boolean reported;

	private RdmaPoolPlacement(final String stepId, final String nic, final int node, final int[] nodeCpus, final RdmaTransport transport,
					final IntFunction<ByteBuffer> allocator) {
		this.stepId = stepId;
		this.nic = nic;
		this.node = node;
		this.nodeCpus = nodeCpus;
		this.transport = transport;
		this.allocator = allocator;
	}

	/**
	 * The allocator for a NIC's pool.
	 *
	 * @param node the NIC's NUMA node, {@code null} when it has none
	 * @param allocator what allocates; returned as it is when the buffers are not to be placed
	 */
	static IntFunction<ByteBuffer> allocator(final String stepId, final String nic, final Integer node, final Path nodes,
					final RdmaTransport transport, final IntFunction<ByteBuffer> allocator) {
		final var cpus = cpusOf(stepId, nic, node, nodes);
		return cpus == null ? allocator : new RdmaPoolPlacement(stepId, nic, node, cpus, transport, allocator)::allocate;
	}

	private ByteBuffer allocate(final int size) {
		if (gaveUp) {
			return allocator.apply(size);
		}
		final int[] before;
		final int[] wanted;
		try {
			before = transport.currentThreadCpus();
			wanted = intersection(before, nodeCpus);
			if (wanted.length == 0) {
				giveUp("this process may use none of its CPUs");
				return allocator.apply(size);
			}
			final int result = transport.setCurrentThreadCpus(wanted);
			if (result != 0) {
				giveUp("the kernel refused, errno " + result);
				return allocator.apply(size);
			}
		} catch (final RuntimeException | LinkageError e) {
			LogUtil.exception(Level.DEBUG, e, "{}: NIC {}: pool placement failed", stepId, nic);
			giveUp(e.toString());
			return allocator.apply(size);
		}
		try {
			return allocator.apply(size);
		} finally {
			restore(before);
			if (!reported) {
				reported = true;
				Loggers.MSG.info("{}: NIC {}: pool buffers are allocated on the CPUs of NUMA node {}", stepId, nic, node);
			}
		}
	}

	private void restore(final int[] before) {
		try {
			final int result = transport.setCurrentThreadCpus(before);
			if (result != 0) {
				giveUp("the thread stays on the node, as the kernel refused its restoration, errno " + result);
			}
		} catch (final RuntimeException | LinkageError e) {
			giveUp("the thread stays on the node: " + e);
		}
	}

	private void giveUp(final String why) {
		gaveUp = true;
		warn(stepId, nic, node, why);
	}

	private static void warn(final String stepId, final String nic, final Integer node, final String why) {
		Loggers.MSG.warn("{}: NIC {}: its pool could not be placed on NUMA node {}: {}. Its buffers land where the thread allocating them runs, "
						+ "which can lower throughput", stepId, nic, node, why);
	}

	/** The node's CPUs, or {@code null} when the pool is not to be placed. */
	static int[] cpusOf(final String stepId, final String nic, final Integer node, final Path nodes) {
		try {
			if (!severalNodes(nodes)) {
				return null;
			}
			if (node == null) {
				Loggers.MSG.info("{}: NIC {}: no NUMA node is known for it, so its pool is not placed", stepId, nic);
				return null;
			}
			final var cpus = parseCpuList(Files.readString(nodes.resolve("node" + node).resolve("cpulist")));
			if (cpus.length == 0) {
				warn(stepId, nic, node, "the node has no CPUs");
				return null;
			}
			return cpus;
		} catch (final IOException | IllegalArgumentException e) {
			warn(stepId, nic, node, e.toString());
			return null;
		}
	}

	private static boolean severalNodes(final Path nodes) throws IOException {
		if (!Files.isDirectory(nodes)) {
			return false;
		}
		try (var entries = Files.list(nodes)) {
			return entries.filter(entry -> NODE_DIRECTORY.matcher(entry.getFileName().toString()).matches()).limit(2).count() > 1;
		}
	}

	private static int[] intersection(final int[] first, final int[] second) {
		final var sorted = second.clone();
		Arrays.sort(sorted);
		return Arrays.stream(first).filter(cpu -> Arrays.binarySearch(sorted, cpu) >= 0).toArray();
	}

	/** Parses the kernel's list form, such as {@code 0-3,8,10-11}, or an empty line. */
	static int[] parseCpuList(final String list) {
		final var cpus = new ArrayList<Integer>();
		final var trimmed = list.trim();
		if (!trimmed.isEmpty()) {
			for (final var part : trimmed.split(",", -1)) {
				addRange(cpus, part);
			}
		}
		return cpus.stream().mapToInt(Integer::intValue).toArray();
	}

	private static void addRange(final List<Integer> cpus, final String part) {
		final int dash = part.indexOf('-');
		final int first = Integer.parseInt(dash < 0 ? part : part.substring(0, dash));
		final int last = dash < 0 ? first : Integer.parseInt(part.substring(dash + 1));
		if (first < 0 || last < first || last > MAX_CPU) {
			throw new IllegalArgumentException("bad CPU range " + part);
		}
		for (var cpu = first; cpu <= last; cpu++) {
			cpus.add(cpu);
		}
	}
}
