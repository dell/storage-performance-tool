package com.dell.spt.base.metrics.bucket;

import java.util.List;

/**
 * Candidate lookup from an object path to its bucket's slot, for ticket 01 of bucket metrics: the
 * first path segment is hashed in place and resolved through a precomputed open-addressed table,
 * with a string comparison on every hash match. It neither allocates nor builds a substring.
 */
public final class BucketSlotTable {

	static final char SEPARATOR = '/';

	private final String[] names;
	private final int[] slots;
	private final int mask;
	private final int catchAll;

	public BucketSlotTable(final List<String> members) {
		catchAll = members.size();
		final int capacity = Integer.highestOneBit(Math.max(2, members.size()) * 4 - 1) << 1;
		names = new String[capacity];
		slots = new int[capacity];
		mask = capacity - 1;
		for (int slot = 0; slot < members.size(); slot++) {
			final String name = members.get(slot);
			int index = spread(name.hashCode()) & mask;
			while (names[index] != null) {
				index = (index + 1) & mask;
			}
			names[index] = name;
			slots[index] = slot;
		}
	}

	/** The slot of the path's first segment, or the catch-all's when it names no member. */
	public int slotOfPath(final String path) {
		if (path == null) {
			return catchAll;
		}
		final int length = path.length();
		final int start = length > 0 && path.charAt(0) == SEPARATOR ? 1 : 0;
		int hash = 0;
		int end = start;
		while (end < length) {
			final char c = path.charAt(end);
			if (c == SEPARATOR) {
				break;
			}
			hash = 31 * hash + c;
			end++;
		}
		final int segmentLength = end - start;
		int index = spread(hash) & mask;
		while (true) {
			final String name = names[index];
			if (name == null) {
				return catchAll;
			}
			if (name.length() == segmentLength && path.regionMatches(start, name, 0, segmentLength)) {
				return slots[index];
			}
			index = (index + 1) & mask;
		}
	}

	public int catchAllSlot() {
		return catchAll;
	}

	private static int spread(final int hash) {
		return hash ^ (hash >>> 16);
	}
}
