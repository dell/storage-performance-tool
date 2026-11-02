package com.dell.spt.base.metrics.bucket;

import com.dell.spt.base.item.op.OpType;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntFunction;

/** Every member's meter set, plus the catch-all's, for one metrics context of a step. */
public final class BucketMeters {

	private final OpType opType;
	private final List<String> members;
	private final BucketMeterSet[] sets;

	public BucketMeters(final OpType opType, final List<String> members) {
		this.opType = opType;
		this.members = List.copyOf(members);
		sets = new BucketMeterSet[members.size() + 1];
		for (int i = 0; i < sets.length; i++) {
			sets[i] = new BucketMeterSet();
		}
	}

	public OpType opType() {
		return opType;
	}

	public int memberCount() {
		return members.size();
	}

	BucketKind kindOf(final int slot) {
		return slot < members.size() ? BucketKind.MEMBER : BucketKind.CATCH_ALL;
	}

	/** The member's name at a slot, or null for the catch-all's. */
	String nameOf(final int slot) {
		return slot < members.size() ? members.get(slot) : null;
	}

	/** The sets by slot, the members in set order and then the catch-all. */
	BucketMeterSet[] sets() {
		return sets;
	}

	/**
	 * Reads the values of every set of a quiescent step, the members in set order and then the
	 * catch-all, which is present even when empty. Each set's histograms are copied, encoded and
	 * released before the next set is read, so the step never holds every uncompressed copy.
	 */
	public List<BucketTerminalRecord> terminalRecords() {
		final List<BucketTerminalRecord> records = new ArrayList<>(sets.length);
		final ByteBuffer[] buffer = new ByteBuffer[1];
		final IntFunction<ByteBuffer> encodeBuffer = capacity -> {
			if (buffer[0] == null || buffer[0].capacity() < capacity) {
				buffer[0] = ByteBuffer.allocate(capacity);
			}
			return buffer[0];
		};
		for (int slot = 0; slot < members.size(); slot++) {
			records.add(sets[slot].terminalRecord(opType, BucketKind.MEMBER, members.get(slot), encodeBuffer));
		}
		records.add(sets[members.size()].terminalRecord(opType, BucketKind.CATCH_ALL, null, encodeBuffer));
		return records;
	}
}
