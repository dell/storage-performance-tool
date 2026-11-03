package com.dell.spt.base.nic.pool;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * This engine's effective memory (N6): the smaller of the host's {@code MemTotal} and the
 * {@code memory.max} of its cgroup, ancestors included, such as an LXC container's. The CLI takes off
 * the heap and a reserve. A probe sees its whole chain only when it runs in the host's cgroup
 * namespace, which is how the CLI starts it.
 *
 * @param memTotalBytes {@code MemTotal}, or -1 when it cannot be read
 * @param cgroupLimitBytes the smallest limit on the chain, or -1 without one
 * @param cgroup the cgroup that sets that limit, or empty
 * @param note what could not be read, or empty
 */
public record MemoryCapacity(long memTotalBytes, long cgroupLimitBytes, String cgroup, String note) {

	public static final String SOURCE_MEM_TOTAL = "MemTotal";
	public static final String SOURCE_CGROUP = "cgroup";

	/** The smaller of the two, or -1 when neither was read. */
	public long bytes() {
		if (cgroupLimitBytes >= 0 && (memTotalBytes < 0 || cgroupLimitBytes < memTotalBytes)) {
			return cgroupLimitBytes;
		}
		return memTotalBytes;
	}

	/** Which of the two bounds it. */
	public String source() {
		return cgroupLimitBytes >= 0 && (memTotalBytes < 0 || cgroupLimitBytes < memTotalBytes) ? SOURCE_CGROUP : SOURCE_MEM_TOTAL;
	}

	public void write(final ObjectNode node) {
		node.put("bytes", bytes());
		node.put("source", source());
		node.put("memTotalBytes", memTotalBytes);
		node.put("cgroupLimitBytes", cgroupLimitBytes);
		if (!cgroup.isEmpty()) {
			node.put("cgroup", cgroup);
		}
		if (!note.isEmpty()) {
			node.put("note", note);
		}
	}

	public static MemoryCapacity ofThisHost() {
		return read(Path.of("/"));
	}

	/** Reads below {@code root}, which tests replace. */
	static MemoryCapacity read(final Path root) {
		final var notes = new StringBuilder();
		long memTotal = -1;
		try {
			memTotal = memTotal(Files.readAllLines(root.resolve("proc/meminfo"), StandardCharsets.UTF_8));
		} catch (final IOException | NumberFormatException e) {
			notes.append("cannot read MemTotal: ").append(e);
		}
		final var limit = new CgroupMemoryLimit();
		try {
			final var lines = Files.readAllLines(root.resolve("proc/self/cgroup"), StandardCharsets.UTF_8);
			final var unified = unifiedCgroup(lines);
			if (unified != null) {
				limit.readChain(root.resolve("sys/fs/cgroup"), unified, "memory.max");
			}
			// A cgroup v1 memory controller, alone or beside a unified hierarchy that lacks it. In a
			// container its mount is the container's own cgroup, so the mount's root counts too.
			final var v1 = memoryController(lines);
			if (v1 != null) {
				final var mount = root.resolve("sys/fs/cgroup/memory");
				limit.readChain(mount, v1, "memory.limit_in_bytes");
				limit.readFile(mount.resolve("memory.limit_in_bytes"), v1);
				limit.readHierarchical(mount.resolve("memory.stat"), v1);
			}
		} catch (final IOException | NumberFormatException e) {
			if (notes.length() > 0) {
				notes.append("; ");
			}
			notes.append("cannot read the cgroup chain: ").append(e);
		}
		return new MemoryCapacity(memTotal, limit.bytes(), limit.cgroup(), notes.toString());
	}

	private static long memTotal(final List<String> lines) {
		for (final String line : lines) {
			if (line.startsWith("MemTotal:")) {
				final var fields = line.trim().split("\\s+", -1);
				return Long.parseLong(fields[1]) * 1024;
			}
		}
		throw new NumberFormatException("no MemTotal line");
	}

	/** The cgroup v2 path, from the {@code 0::} line; {@code null} on a host with cgroup v1 alone. */
	private static String unifiedCgroup(final List<String> lines) {
		for (final String line : lines) {
			if (line.startsWith("0::")) {
				final var path = line.substring(3).trim();
				return path.isEmpty() ? "/" : path;
			}
		}
		return null;
	}

	/** The cgroup v1 memory controller's path, from a line such as {@code 4:memory:/docker/id}; {@code null} without one. */
	private static String memoryController(final List<String> lines) {
		for (final String line : lines) {
			final var fields = line.split(":", 3);
			if (fields.length == 3 && List.of(fields[1].split(",", -1)).contains("memory")) {
				return fields[2].trim().isEmpty() ? "/" : fields[2].trim();
			}
		}
		return null;
	}
}
