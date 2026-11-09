package com.dell.spt.storage.driver.coop.netty.nic;

import com.dell.spt.base.item.op.Operation;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.function.LongSupplier;

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
 * Each attempt carries, from {@link #tookTrial()}, whether it is its NIC's trial, and reports that
 * at its end.
 *
 * <p>A NIC is failing from an attempt on it that ended with no response ({@link #failed}) until an
 * attempt on it gets a response ({@link #answered}) (D87). While some NIC is not failing, a failing
 * NIC takes an attempt only as a trial: when it counts none, no trial of it is still running, the
 * trial interval since its last failure has run, and its {@link Gate} says an attempt now would
 * not fail at once, as it would while its Path is in connect back-off. The interval is 100 ms after
 * the first failure and doubles with each failure after it has run, to 5 s, as a Path's connect
 * back-off; a failure while it runs, such as one of the attempts admitted before the NIC failed,
 * does not lengthen it. A trial holds the NIC until its own attempt ends, with or without a
 * concurrency limit, so a failing NIC carries at most one trial, and none while an interval runs,
 * and a trial that gets a response ends it. When every NIC is failing, the rotation takes any NIC
 * with room, as it does without failures, so a step whose every NIC fails still fails at the rate
 * of its attempts. An admitted attempt keeps its NIC (D21).
 *
 * <p>While only a failing NIC's later trial could take an attempt, and no trial of it runs whose
 * end would signal the dispatch task, {@link #hasRoom()} schedules a re-check of the dispatch task
 * with the {@link Timer}, as no completion may signal it when a trial becomes due.
 */
public final class NicAdmission {

	/** Whether a NIC's pool would serve an attempt now rather than fail it at once. Asked only of a failing NIC. */
	@FunctionalInterface
	interface Gate {
		boolean servesNow();
	}

	/** Runs a task once, after a delay; the NIC set's event loops. */
	@FunctionalInterface
	public interface Timer {
		void schedule(Runnable task, long delayMillis);
	}

	// PathBackoff's interval reaches its cap after 7 failures in a row.
	private static final int MAX_FAILURES_IN_ROW = 7;

	/** How soon the dispatch task looks again while a failing NIC's trial is not yet due. */
	static final long TRIAL_RECHECK_MILLIS = 50;

	private static final Gate ALWAYS = () -> true;
	private static final NicAdmission NONE = new NicAdmission(new int[0], true, new Gate[0], System::nanoTime, null, null);

	private final int[] limits;
	private final AtomicIntegerArray counts;
	// 0 while the NIC is not failing, else its failures in a row (capped). Written by the threads that
	// end attempts: on a failure, and on a response only when the value changes.
	private final AtomicIntegerArray failing;
	// 1 from a trial's admission until its own attempt ends.
	private final AtomicIntegerArray trials;
	// The time a failing NIC's next trial may start, from its last failure.
	private final AtomicLongArray trialAfter;
	private final Gate[] gates;
	private final boolean untilEnd;
	private final LongSupplier nanoClock;
	private final Timer timer;
	private final Runnable recheck;
	private final AtomicBoolean recheckScheduled = new AtomicBoolean();
	// Only the dispatch task takes NICs; a lost update would only skew the rotation.
	private int cursor;
	// Whether the last take() took a trial; the dispatch task's own.
	private boolean tookTrial;

	private NicAdmission(final int[] limits, final boolean untilEnd, final Gate[] gates, final LongSupplier nanoClock,
					final Timer timer, final Runnable recheck) {
		this.limits = limits;
		this.counts = new AtomicIntegerArray(limits.length);
		this.failing = new AtomicIntegerArray(limits.length);
		this.trials = new AtomicIntegerArray(limits.length);
		this.trialAfter = new AtomicLongArray(limits.length);
		this.gates = gates;
		this.untilEnd = untilEnd;
		this.nanoClock = nanoClock;
		this.timer = timer;
		this.recheck = recheck;
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

	/** The admission of one NIC per gate, in layout order, for the driver's concurrency limit; no re-check. */
	static NicAdmission of(final int concurrencyLimit, final List<? extends Gate> gates) {
		return of(concurrencyLimit, gates, System::nanoTime, null, null);
	}

	/**
	 * As above, timing trial intervals with {@code nanoClock}; {@code recheck} wakes the dispatch
	 * task, run by {@code timer} while only a failing NIC's later trial could take an attempt. Without
	 * a timer, nothing is scheduled.
	 */
	static NicAdmission of(final int concurrencyLimit, final List<? extends Gate> gates, final LongSupplier nanoClock,
					final Timer timer, final Runnable recheck) {
		final int nics = gates.size();
		final var limits = new int[nics];
		for (var i = 0; i < nics; i++) {
			limits[i] = NicSetSettings.admissionLimit(concurrencyLimit, nics, i);
		}
		return new NicAdmission(limits, concurrencyLimit > 0, gates.toArray(Gate[]::new), nanoClock, timer, recheck);
	}

	/**
	 * Takes the next NIC in rotation that has room, starting after the NIC taken last; a failing NIC
	 * only for a trial while another NIC is not failing. Called by the dispatch task only, which then
	 * asks {@link #tookTrial()}.
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
					return taken(nic, false);
				}
			} else if (trialReady(nic) && trials.compareAndSet(nic, 0, 1)) {
				// Only the dispatch task adds to a count, so the count seen as 0 is still 0 here.
				counts.incrementAndGet(nic);
				return taken(nic, true);
			}
		}
		if (working) {
			return -1;
		}
		// Every NIC is failing: the rotation as without failures.
		for (var i = 0; i < n; i++) {
			final int nic = first + i < n ? first + i : first + i - n;
			if (claim(nic)) {
				return taken(nic, false);
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

	private int taken(final int nic, final boolean trial) {
		cursor = nic + 1 < limits.length ? nic + 1 : 0;
		tookTrial = trial;
		return nic;
	}

	/**
	 * Whether the NIC {@link #take()} returned last was taken for a trial, which its attempt reports
	 * at its end. Asked by the dispatch task right after the take.
	 */
	public boolean tookTrial() {
		return tookTrial;
	}

	/**
	 * Whether a trial of {@code nic} runs. An attempt that ends on a connection asks first, so an
	 * ordinary attempt looks for no trial mark while none runs.
	 */
	public boolean trialRunning(final int nic) {
		return trials.get(nic) != 0;
	}

	/**
	 * A failing NIC counts no attempt, runs no trial, its trial interval has run and its pool would
	 * serve an attempt now.
	 */
	private boolean trialReady(final int nic) {
		return trials.get(nic) == 0 && counts.get(nic) == 0 && nanoClock.getAsLong() - trialAfter.get(nic) >= 0
						&& gates[nic].servesNow();
	}

	/**
	 * Whether {@link #take()} could take a NIC now. When it could not, but a failing NIC that runs no
	 * trial could take one later while another NIC works, schedules a re-check of the dispatch task;
	 * a running trial's end signals it.
	 */
	public boolean hasRoom() {
		var working = false;
		var trialAwaited = false;
		for (var nic = 0; nic < limits.length; nic++) {
			if (failing.get(nic) == 0) {
				working = true;
				if (counts.get(nic) < limits[nic]) {
					return true;
				}
			} else if (trialReady(nic)) {
				return true;
			} else if (trials.get(nic) == 0) {
				trialAwaited = true;
			}
		}
		if (working) {
			if (trialAwaited) {
				scheduleRecheck();
			}
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

	/** Once at a time: a trial becomes due with the clock, which signals nothing. */
	private void scheduleRecheck() {
		if (timer == null || !recheckScheduled.compareAndSet(false, true)) {
			return;
		}
		try {
			timer.schedule(() -> {
				recheckScheduled.set(false);
				recheck.run();
			}, TRIAL_RECHECK_MILLIS);
		} catch (final RejectedExecutionException e) {
			// The NIC set's loops are shutting down: so is the step, and nothing waits for a trial.
			recheckScheduled.set(false);
		}
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

	/**
	 * The attempt on {@code nic} completed or was dropped before its request; after {@link #waitEnded}.
	 * The end of a {@code trial} lets the next trial start once its interval has run.
	 */
	public void attemptEnded(final int nic, final boolean trial) {
		if (untilEnd) {
			counts.decrementAndGet(nic);
		}
		if (trial) {
			trials.set(nic, 0);
		}
	}

	/**
	 * Returns {@code nic}, just taken by the dispatch task, for an attempt that did not ask the pool;
	 * {@code trial} as {@link #tookTrial()} said for it.
	 */
	public void cancelled(final int nic, final boolean trial) {
		counts.decrementAndGet(nic);
		if (trial) {
			trials.set(nic, 0);
		}
	}

	/**
	 * An attempt on {@code nic} ended with no response ({@code FAIL_IO}): a failed connect, a Path in
	 * back-off, a lost connection or a request timeout. The NIC is failing until one gets a response,
	 * and its next trial waits an interval that doubles with each failure in a row. A failure while
	 * the interval runs is no failed trial, as no trial starts before it has run, so it changes
	 * nothing. Reported before the attempt gives back its NIC.
	 */
	public void failed(final int nic) {
		final int inRow = failing.get(nic);
		final long now = nanoClock.getAsLong();
		if (inRow != 0 && now - trialAfter.get(nic) < 0) {
			return;
		}
		// Racing failures may lose a count, which only shortens one interval.
		final int next = inRow < MAX_FAILURES_IN_ROW ? inRow + 1 : MAX_FAILURES_IN_ROW;
		trialAfter.set(nic, now + TimeUnit.MILLISECONDS.toNanos(PathBackoff.intervalMillis(next)));
		failing.set(nic, next);
	}

	/**
	 * An attempt on {@code nic} got a response, a success or the target's error: the NIC reaches the
	 * target, so it is not failing. Reported before the attempt gives back its NIC.
	 */
	public void answered(final int nic) {
		if (failing.get(nic) != 0) {
			failing.set(nic, 0);
		}
	}

	/**
	 * Whether {@code status} is the target's answer, which {@link #answered} reports: a success or a
	 * response error, a 504 ({@code FAIL_TIMEOUT}) included. No status is no answer.
	 */
	public static boolean isAnswer(final Operation.Status status) {
		if (status == null) {
			return false;
		}
		return switch (status) {
		case SUCC, RESP_FAIL_UNKNOWN, RESP_FAIL_CLIENT, RESP_FAIL_SVC, RESP_FAIL_NOT_FOUND, RESP_FAIL_AUTH, RESP_FAIL_CORRUPT,
						RESP_FAIL_SPACE, FAIL_TIMEOUT ->
			true;
		default -> false;
		};
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
