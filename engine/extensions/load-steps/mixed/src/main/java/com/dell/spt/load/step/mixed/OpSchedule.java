package com.dell.spt.load.step.mixed;

import com.dell.spt.base.item.op.OpType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A pre-shuffled, fixed-size cyclic schedule of {@link OpType} values.
 *
 * <p>Built from integer weights (e.g., {CREATE=60, DELETE=40}), the schedule contains
 * exactly {@link #SCHEDULE_SIZE} entries distributed proportionally, then shuffled once.
 * Threads call {@link #next()} to advance through the array; the index wraps around
 * automatically, guaranteeing the exact distribution over every full cycle.
 *
 * <p>Modeled after warp's 1000-element operation schedule.
 */
public final class OpSchedule {

	public static final int SCHEDULE_SIZE = 1000;

	private final OpType[] schedule;
	private final AtomicInteger index = new AtomicInteger(0);
	private final OpType[] activeOps;
	private final Map<OpType, Integer> originIndices;

	/**
	 * @param weights non-empty map of op types to positive integer weights;
	 *                at least two entries must have weight &gt; 0
	 */
	public OpSchedule(final Map<OpType, Integer> weights) {
		if (weights == null || weights.isEmpty()) {
			throw new IllegalArgumentException("Weights map must be non-empty");
		}

		int totalWeight = 0;
		int nonZeroCount = 0;
		for (final var entry : weights.entrySet()) {
			final int w = entry.getValue();
			if (w < 0) {
				throw new IllegalArgumentException("Negative weight for " + entry.getKey() + ": " + w);
			}
			if (w > 0) {
				totalWeight += w;
				nonZeroCount++;
			}
		}
		if (totalWeight == 0) {
			throw new IllegalArgumentException("Total weight must be > 0");
		}
		if (nonZeroCount < 2) {
			throw new IllegalArgumentException("At least 2 non-zero op types required, got " + nonZeroCount);
		}

		// Build schedule array proportionally
		final List<OpType> entries = new ArrayList<>(SCHEDULE_SIZE);
		int allocated = 0;
		OpType lastOp = null;
		for (final var entry : weights.entrySet()) {
			final int w = entry.getValue();
			if (w <= 0) {
				continue;
			}
			lastOp = entry.getKey();
			final int count = (int) Math.round((double) w / totalWeight * SCHEDULE_SIZE);
			for (int i = 0; i < count && allocated < SCHEDULE_SIZE; i++) {
				entries.add(lastOp);
				allocated++;
			}
		}
		// Fill any remaining slots (rounding residual) with the last active op
		while (allocated < SCHEDULE_SIZE) {
			entries.add(lastOp);
			allocated++;
		}

		Collections.shuffle(entries);
		this.schedule = entries.toArray(new OpType[0]);

		// Build active ops list and origin index mapping (in enum ordinal order)
		final List<OpType> activeList = new ArrayList<>(4);
		this.originIndices = new EnumMap<>(OpType.class);
		for (final OpType op : OpType.values()) {
			final Integer w = weights.get(op);
			if (w != null && w > 0) {
				originIndices.put(op, activeList.size());
				activeList.add(op);
			}
		}
		this.activeOps = activeList.toArray(new OpType[0]);
	}

	/** Returns the next operation type in the cyclic schedule. Thread-safe. */
	public OpType next() {
		final int i = index.getAndIncrement();
		// Use unsigned modulo to handle integer overflow gracefully
		return schedule[Integer.remainderUnsigned(i, SCHEDULE_SIZE)];
	}

	/** Total number of entries in the schedule. */
	public int size() {
		return SCHEDULE_SIZE;
	}

	/**
	 * Returns the origin index (0-based, consecutive) for the given op type,
	 * or -1 if the op type has zero weight (inactive).
	 */
	public int originIndexFor(final OpType opType) {
		final Integer idx = originIndices.get(opType);
		return idx != null ? idx : -1;
	}

	/** Returns the active (non-zero weight) op types in enum ordinal order. */
	public OpType[] activeOpTypes() {
		return activeOps.clone();
	}
}
