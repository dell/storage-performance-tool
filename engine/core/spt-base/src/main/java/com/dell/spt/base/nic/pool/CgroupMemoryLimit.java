package com.dell.spt.base.nic.pool;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

/** The smallest memory limit found on a cgroup chain, and the cgroup that sets it. */
final class CgroupMemoryLimit {

	/** What cgroup v2 reports for no limit. */
	private static final String NO_LIMIT = "max";
	/** What cgroup v1 reports for no limit is a page-aligned {@code Long.MAX_VALUE}. */
	private static final long NO_V1_LIMIT = 1L << 60;
	private static final String HIERARCHICAL_LIMIT = "hierarchical_memory_limit ";

	private long bytes = -1;
	private String cgroup = "";

	long bytes() {
		return bytes;
	}

	String cgroup() {
		return cgroup;
	}

	/** The cgroup itself, then each ancestor; the root has no limit file. */
	void readChain(final Path mount, final String cgroupPath, final String fileName) throws IOException {
		for (String path = cgroupPath; path != null; path = parent(path)) {
			readFile(mount.resolve(path.startsWith("/") ? path.substring(1) : path).resolve(fileName), path);
		}
	}

	void readFile(final Path file, final String path) throws IOException {
		if (!Files.isReadable(file)) {
			return;
		}
		final var text = Files.readString(file, StandardCharsets.UTF_8).trim();
		if (!NO_LIMIT.equals(text)) {
			consider(Long.parseLong(text), path);
		}
	}

	/**
	 * The smallest limit on the whole chain, from cgroup v1's {@code memory.stat}. A container's mount
	 * is its own cgroup, so it cannot read an ancestor's limit file; this figure includes them, without
	 * saying which one set it.
	 *
	 * @return whether the figure was there
	 */
	boolean readHierarchical(final Path statFile, final String path) throws IOException {
		if (!Files.isReadable(statFile)) {
			return false;
		}
		for (final String line : Files.readAllLines(statFile, StandardCharsets.UTF_8)) {
			if (line.startsWith(HIERARCHICAL_LIMIT)) {
				consider(Long.parseLong(line.substring(HIERARCHICAL_LIMIT.length()).trim()), path + " or an ancestor");
				return true;
			}
		}
		return false;
	}

	private void consider(final long value, final String path) {
		if (value < NO_V1_LIMIT && (bytes < 0 || value < bytes)) {
			bytes = value;
			cgroup = path;
		}
	}

	private static String parent(final String path) {
		if ("/".equals(path) || path.isEmpty()) {
			return null;
		}
		final int separator = path.lastIndexOf('/');
		return separator <= 0 ? "/" : path.substring(0, separator);
	}
}
