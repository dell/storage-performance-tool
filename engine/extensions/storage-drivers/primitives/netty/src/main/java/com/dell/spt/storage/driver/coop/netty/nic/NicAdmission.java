package com.dell.spt.storage.driver.coop.netty.nic;

import java.util.concurrent.atomic.AtomicIntegerArray;

/**
 * Per-NIC admission of a NIC-set driver's request attempts (item 8b, D30, D37). The dispatch task
 * takes a NIC for each attempt after its concurrency permit: the next NIC in rotation that has
 * room, never a full one, so no operation waits on a NIC that cannot take it (N3).
 *
 * <p>With a positive concurrency limit, a NIC counts its attempts from admission to their end, so
 * it carries at most its share of the limit when {@code P >= N}, or one attempt when
 * {@code 0 < P < N}. Without a limit it counts them until their wait for a connection ends, so at
 * most {@link NicSetSettings#UNLIMITED_WAITING_LIMIT} attempts per NIC connect or wait at once.
 *
 * <p>Every attempt that took a NIC reports {@link #waitEnded} once, when the pool reports its
 * connection or its failure, and then {@link #attemptEnded} once, when it completes or is dropped
 * before its request. An operation dispatched directly on a completed attempt's connection
 * continues that attempt. An attempt that never asked the pool reports {@link #cancelled} instead.
 */
public final class NicAdmission {

	private static final NicAdmission NONE = new NicAdmission(new int[0], true);

	private final int[] limits;
	private final AtomicIntegerArray counts;
	private final boolean untilEnd;
	// Only the dispatch task takes NICs; a lost update would only skew the rotation.
	private int cursor;

	private NicAdmission(final int[] limits, final boolean untilEnd) {
		this.limits = limits;
		this.counts = new AtomicIntegerArray(limits.length);
		this.untilEnd = untilEnd;
	}

	/** No NIC: nothing can be taken. A NIC-set pool's admission before it opens. */
	static NicAdmission none() {
		return NONE;
	}

	/** The admission of {@code nics} NICs for the driver's concurrency limit, per {@link NicSetSettings#admissionLimit}. */
	static NicAdmission of(final int concurrencyLimit, final int nics) {
		final var limits = new int[nics];
		for (var i = 0; i < nics; i++) {
			limits[i] = NicSetSettings.admissionLimit(concurrencyLimit, nics, i);
		}
		return new NicAdmission(limits, concurrencyLimit > 0);
	}

	/**
	 * Takes the next NIC in rotation that has room, starting after the NIC taken last. Called by
	 * the dispatch task only.
	 *
	 * @return the NIC's index in layout order, or -1 when every NIC is full
	 */
	public int take() {
		final int n = limits.length;
		final int first = cursor;
		for (var i = 0; i < n; i++) {
			final int nic = first + i < n ? first + i : first + i - n;
			final int limit = limits[nic];
			for (var count = counts.get(nic); count < limit; count = counts.get(nic)) {
				if (counts.compareAndSet(nic, count, count + 1)) {
					cursor = nic + 1 < n ? nic + 1 : 0;
					return nic;
				}
			}
		}
		return -1;
	}

	/** Whether some NIC has room for an attempt. */
	public boolean hasRoom() {
		for (var nic = 0; nic < limits.length; nic++) {
			if (counts.get(nic) < limits[nic]) {
				return true;
			}
		}
		return false;
	}

	/** The attempts the NICs can take now, together. */
	public int room() {
		var room = 0;
		for (var nic = 0; nic < limits.length; nic++) {
			room += Math.max(0, limits[nic] - counts.get(nic));
		}
		return room;
	}

	/**
	 * The attempt's wait for a connection on {@code nic} ended, with a connection or a failure.
	 *
	 * @return whether this made room on a NIC that was full, which the caller then signals to the
	 *     dispatch task; only possible without a concurrency limit, as the permit's release
	 *     signals otherwise
	 */
	public boolean waitEnded(final int nic) {
		return !untilEnd && counts.getAndDecrement(nic) == limits[nic];
	}

	/** The attempt on {@code nic} completed or was dropped before its request; after {@link #waitEnded}. */
	public void attemptEnded(final int nic) {
		if (untilEnd) {
			counts.decrementAndGet(nic);
		}
	}

	/** Returns {@code nic}, just taken by the dispatch task, for an attempt that did not ask the pool. */
	public void cancelled(final int nic) {
		counts.decrementAndGet(nic);
	}

	/** Whether a NIC counts its attempts until their end rather than until their wait ends. */
	public boolean countsUntilEnd() {
		return untilEnd;
	}

	/** The number of NICs. */
	public int size() {
		return limits.length;
	}

	/** The attempts {@code nic} counts now. */
	public int count(final int nic) {
		return counts.get(nic);
	}

	/** The attempts {@code nic} may count at once. */
	public int limit(final int nic) {
		return limits[nic];
	}
}
