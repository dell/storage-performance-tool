package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.logging.log4j.Level;

/**
 * Keeps a NIC's Preparation Lane on the NIC's NUMA node, so that the NIC's pool lands there: a buffer's
 * pages are placed where they are first touched, and registration then holds them (D77). Without it the
 * lanes tend to share one node with the threads that started them, and every other NIC writes across
 * the socket link. Threads the lane starts, such as its NIC's copier threads, inherit the placement.
 *
 * <p>A host with one node has nothing to choose. A placement that cannot be made is a warning, and the
 * lane runs where the scheduler puts it, as it did before: a load never fails over this.
 */
final class RdmaLanePlacement {

	static final Path SYSFS_NODES = Path.of("/sys/devices/system/node");
	private static final Pattern NODE_DIRECTORY = Pattern.compile("node[0-9]+");

	/** What {@link RdmaTransport#confineCurrentThread} returns when no CPU of the node is open to the thread. */
	static final int NO_CPU_ALLOWED = -1;

	private RdmaLanePlacement() {}

	/**
	 * The CPUs a lane of a NIC on {@code node} should keep to, read before the lane starts.
	 *
	 * @param node the NIC's NUMA node, {@code null} when it has none
	 * @return the CPUs, or {@code null} when the lane is not to be placed
	 */
	static int[] cpusFor(final String stepId, final String nic, final Integer node, final Path nodes) {
		try {
			if (!severalNodes(nodes)) {
				return null;
			}
			if (node == null) {
				Loggers.MSG.info("{}: NIC {}: no NUMA node is known for it, so its preparation lane is not placed", stepId, nic);
				return null;
			}
			final var cpus = parseCpuList(Files.readString(nodes.resolve("node" + node).resolve("cpulist")));
			if (cpus.length == 0) {
				warnUnplaced(stepId, nic, node, "the node has no CPUs");
				return null;
			}
			return cpus;
		} catch (final IOException | IllegalArgumentException e) {
			warnUnplaced(stepId, nic, node, e.toString());
			return null;
		}
	}

	/** Runs on the lane, before its first task. */
	static void confine(final String stepId, final RdmaNic nic, final Integer node, final int[] cpus) {
		if (cpus == null) {
			return;
		}
		final int result;
		try {
			result = nic.transport.confineCurrentThread(cpus);
		} catch (final RuntimeException | LinkageError e) {
			LogUtil.exception(Level.DEBUG, e, "{}: NIC {}: lane placement failed", stepId, nic.name);
			warnUnplaced(stepId, nic.name, node, e.toString());
			return;
		}
		if (result == 0) {
			Loggers.MSG.info("{}: NIC {}: preparation lane placed on NUMA node {}", stepId, nic.name, node);
		} else if (result == NO_CPU_ALLOWED) {
			warnUnplaced(stepId, nic.name, node, "this process may use none of its CPUs");
		} else {
			warnUnplaced(stepId, nic.name, node, "the kernel refused, errno " + result);
		}
	}

	private static void warnUnplaced(final String stepId, final String nic, final Integer node, final String why) {
		Loggers.MSG.warn("{}: NIC {}: its preparation lane could not be placed on NUMA node {}: {}. Its pool lands wherever the lane runs, "
						+ "which can lower throughput", stepId, nic, node, why);
	}

	private static boolean severalNodes(final Path nodes) throws IOException {
		try (var entries = Files.list(nodes)) {
			return entries.filter(entry -> NODE_DIRECTORY.matcher(entry.getFileName().toString()).matches()).limit(2).count() > 1;
		}
	}

	/** Parses the kernel's list form, such as {@code 0-3,8,10-11}, or an empty line. */
	static int[] parseCpuList(final String list) {
		final var cpus = new ArrayList<Integer>();
		final var trimmed = list.trim();
		if (!trimmed.isEmpty()) {
			for (final var part : trimmed.split(",")) {
				addRange(cpus, part);
			}
		}
		return toArray(cpus);
	}

	private static void addRange(final List<Integer> cpus, final String part) {
		final int dash = part.indexOf('-');
		final int first = Integer.parseInt(dash < 0 ? part : part.substring(0, dash));
		final int last = dash < 0 ? first : Integer.parseInt(part.substring(dash + 1));
		if (first < 0 || last < first) {
			throw new IllegalArgumentException("bad CPU range " + part);
		}
		for (var cpu = first; cpu <= last; cpu++) {
			cpus.add(cpu);
		}
	}

	private static int[] toArray(final List<Integer> cpus) {
		final var out = new int[cpus.size()];
		for (var i = 0; i < out.length; i++) {
			out[i] = cpus.get(i);
		}
		return out;
	}
}
