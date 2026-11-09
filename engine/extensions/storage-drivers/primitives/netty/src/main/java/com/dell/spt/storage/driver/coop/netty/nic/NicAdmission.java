package com.dell.spt.storage.driver.coop.netty.nic;

import java.util.Arrays;
import java.util.List;
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
 *
 * <p>A NIC is failing from an attempt on it that ended with no response ({@link #failed}) until an
 * attempt on it succeeds ({@link #succeeded}) (D87). While some NIC is not failing, a failing NIC
 * takes an attempt only as a trial: when it counts none and its {@link Gate} says an attempt now
 * would not fail at once, as it would while its Path is in connect back-off. So it carries at most
 * one attempt, and none while its back-off interval runs, and a trial that succeeds ends it. When
 * every NIC is failing, the rotation takes any NIC with room, as it does without failures, so a
 * step whose every NIC fails still fails at the rate of its attempts. An admitted attempt keeps
 * its NIC (D21).
 */
public final class NicAdmission {

	/** Whether a NIC's pool would serve an attempt now rather than fail it at once. Asked only of a failing NIC. */
	@FunctionalInterface
	interface Gate {
		boolean servesNow();
	}

	private static final Gate ALWAYS = () -> true;
	private static final NicAdmission NONE = new NicAdmission(new int[0], true, new Gate[0]);

	private final int[] limits;
	private final AtomicIntegerArray counts;
	// 1 while the NIC is failing. Written by the threads that end attempts, only when the value changes.
	private final AtomicIntegerArray failing;
	private final Gate[] gates;
	private final boolean untilEnd;
	// Only the dispatch task takes NICs; a lost update would only skew the rotation.
	private int cursor;

	private NicAdmission(final int[] limits, final boolean untilEnd, final Gate[] gates) {
		this.limits = limits;
		this.counts = new AtomicIntegerArray(limits.length);
		this.failing = new AtomicIntegerArray(limits.length);
		this.gates = gates;
		this.untilEnd = untilEnd;
	}

	/** No NIC: nothing can be taken. A NIC-set pool's admission before it opens. */
	static NicAdmission none() {
		return NONE;
	}

	/**
	 * The admission of {@code nics} NICs for the driver's concurrency limit, per
	 * {@link NicSetSettings#admissionLimit}, whose pools always serve.
	 */
	static NicAdmission of(final int concurrencyLimit, final int nics) {
		final var gates = new Gate[nics];
		Arrays.fill(gates, ALWAYS);
		return of(concurrencyLimit, List.of(gates));
	}

	/** The admission of one NIC per gate, in layout order, for the driver's concurrency limit. */
	static NicAdmission of(final int concurrencyLimit, final List<? extends Gate> gates) {
		final int nics = gates.size();
		final var limits = new int[nics];
		for (var i = 0; i < nics; i++) {
			limits[i] = NicSetSettings.admissionLimit(concurrencyLimit, nics, i);
		}
		return new NicAdmission(limits, concurrencyLimit > 0, gates.toArray(Gate[]::new));
	}

	/**
	 * Takes the next NIC in rotation that has room, starting after the NIC taken last; a failing NIC
	 * only for a trial while another NIC is not failing. Called by the dispatch task only.
	 *
	 * @return the NIC's index in layout order, or -1 when no NIC can take the attempt
	 */
	public int take() {
		final int n = limits.length;
		final int first = cursor;
		var working = false;
		for (var i = 0; i < n; i++) {
			final int nic = first + i < n ? first + i : first + i - n;
			if (failing.get(nic) == 0) {
				working = true;
				if (claim(nic)) {
					return taken(nic);
				}
			} else if (trialReady(nic) && counts.compareAndSet(nic, 0, 1)) {
				return taken(nic);
			}
		}
		if (working) {
			return -1;
		}
		// Every NIC is failing: the rotation as without failures.
		for (var i = 0; i < n; i++) {
			final int nic = first + i < n ? first + i : first + i - n;
			if (claim(nic)) {
				return taken(nic);
			}
		}
		return -1;
	}

	private boolean claim(final int nic) {
		final int limit = limits[nic];
		for (var count = counts.get(nic); count < limit; count = counts.get(nic)) {
			if (counts.compareAndSet(nic, count, count + 1)) {
				return true;
			}
		}
		return false;
	}

	private int taken(final int nic) {
		cursor = nic + 1 < limits.length ? nic + 1 : 0;
		return nic;
	}

	/** A failing NIC counts no attempt and its pool would serve one now. */
	private boolean trialReady(final int nic) {
		return counts.get(nic) == 0 && gates[nic].servesNow();
	}

	/** Whether {@link #take()} could take a NIC now. */
	public boolean hasRoom() {
		var working = false;
		for (var nic = 0; nic < limits.length; nic++) {
			if (failing.get(nic) == 0) {
				working = true;
				if (counts.get(nic) < limits[nic]) {
					return true;
				}
			} else if (trialReady(nic)) {
				return true;
			}
		}
		if (working) {
			return false;
		}
		for (var nic = 0; nic < limits.length; nic++) {
			if (counts.get(nic) < limits[nic]) {
				return true;
			}
		}
		return false;
	}

	/** The attempts {@link #take()} could take now, together. */
	public int room() {
		var room = 0;
		var working = false;
		for (var nic = 0; nic < limits.length; nic++) {
			if (failing.get(nic) == 0) {
				working = true;
				room += Math.max(0, limits[nic] - counts.get(nic));
			} else if (trialReady(nic)) {
				room++;
			}
		}
		if (working) {
			return room;
		}
		room = 0;
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

	/**
	 * An attempt on {@code nic} ended with no response ({@code FAIL_IO}): a failed connect, a Path in
	 * back-off, a lost connection or a request timeout. The NIC is failing until one succeeds.
	 * Reported before the attempt gives back its NIC.
	 */
	public void failed(final int nic) {
		if (failing.get(nic) == 0) {
			failing.set(nic, 1);
		}
	}

	/** An attempt on {@code nic} succeeded, so the NIC is not failing. Reported before the attempt gives back its NIC. */
	public void succeeded(final int nic) {
		if (failing.get(nic) != 0) {
			failing.set(nic, 0);
		}
	}

	/** Whether {@code nic} is failing. */
	public boolean isFailing(final int nic) {
		return failing.get(nic) != 0;
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
