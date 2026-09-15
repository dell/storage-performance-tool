package com.dell.spt.base.load.generator.range;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.data.range.RangeReadOperation;
import com.dell.spt.base.item.op.data.range.RangeReadCirculation;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.io.EOFException;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;
import java.util.function.Consumer;

/**
 * Range-only generator output: valid selections reach the driver, invalid selections consume
 * accepted logical work without transport. Refusal uses the generator's timed output backoff.
 * One instance bounds local errors to 1000/s, burst one, across its concurrent callers. The
 * runtime must share/allocate this admission budget at the step boundary and close admission
 * before recovery; this does not replace the driver's actual transport admission guard.
 * Capacity bounds admitted logical reads, including retry delays and queues. Runtime construction
 * supplies the capacity and installs releaseSettled after final delivery or recovery. Local selection
 * errors need no long-lived capacity reservation.
 */
public final class RangeReadAdmission<I extends DataItem> implements Output<RangeReadOperation<I>> {
	static final long LOCAL_ERROR_INTERVAL_NANOS = 1_000_000;
	private final Output<RangeReadOperation<I>> driver;
	private final OperationLifecycleTracker<? super RangeReadOperation<I>> tracker;
	private final LongSupplier clock;
	private final Consumer<RangeReadOperation<I>> localResultOutput;
	private final Object localAdmissionLock = new Object();
	private volatile boolean open = true;
	private boolean pacingStarted;
	private boolean localErrorInProgress;
	private long nextLocalError;
	private final int capacity;
	private final Set<RangeReadCirculation> admitted = Collections.newSetFromMap(new IdentityHashMap<>());

	public RangeReadAdmission(final Output<RangeReadOperation<I>> driver,
					final OperationLifecycleTracker<? super RangeReadOperation<I>> tracker, final int capacity) {
		this(driver, tracker, capacity, System::nanoTime);
	}

	public RangeReadAdmission(final Output<RangeReadOperation<I>> driver,
					final OperationLifecycleTracker<? super RangeReadOperation<I>> tracker, final int capacity,
					final LongSupplier clock) {
		this(driver, tracker, capacity, clock, ignored -> {});
	}

	public RangeReadAdmission(final Output<RangeReadOperation<I>> driver,
					final OperationLifecycleTracker<? super RangeReadOperation<I>> tracker, final int capacity,
					final LongSupplier clock, final Consumer<RangeReadOperation<I>> localResultOutput) {
		this.localResultOutput = Objects.requireNonNull(localResultOutput);
		if (capacity <= 0) {
			throw new IllegalArgumentException("Range admission capacity must be positive");
		}
		this.capacity = capacity;
		this.driver = Objects.requireNonNull(driver);
		this.tracker = Objects.requireNonNull(tracker);
		this.clock = Objects.requireNonNull(clock);
		if (!tracker.isEnabled()) {
			throw new IllegalArgumentException("Range admission requires tracked lifecycle custody");
		}
	}

	@Override
	public boolean put(final RangeReadOperation<I> op) {
		try {
			return admit(op);
		} catch (Exception failure) {
			if (failure instanceof EOFException) {
				throwUnchecked(failure);
			}
			throw new IntegrityTerminalException(IntegrityTerminalException.Category.EXECUTION,
							"Range admission could not retain an accepted outcome", failure);
		}
	}

	private boolean admit(final RangeReadOperation<I> op) {
		if (!open) {
			throwUnchecked(new EOFException("Range admission is closed"));
		}
		if (op.selection().error() == null) {
			return admitValid(op);
		}
		final long now;
		synchronized (localAdmissionLock) {
			if (!open) {
				throwUnchecked(new EOFException("Range admission is closed"));
			}
			now = clock.getAsLong();
			if (localErrorInProgress || (pacingStarted && now - nextLocalError < 0)) {
				return false;
			}
			localErrorInProgress = true;
		}
		// Terminal observers acquire capacity under the lifecycle monitor. Reserve the
		// pacing slot first, but never acquire that monitor under localAdmissionLock.
		// Shutdown recovery may win the lifecycle claim; a refusal consumes no slot.
		boolean accepted = false;
		try {
			accepted = tracker.localFailure(op, op.lifecycle());
			// Optional publication follows committed accounting, outside both monitors.
			// Keep the single local-error pacing reservation until publication returns.
			if (accepted)
				localResultOutput.accept(op);
			return accepted;
		} finally {
			synchronized (localAdmissionLock) {
				if (accepted) {
					// nanoTime wraparound is valid over this short pacing interval.
					nextLocalError = now + LOCAL_ERROR_INTERVAL_NANOS;
					pacingStarted = true;
				}
				localErrorInProgress = false;
			}
		}
	}

	private boolean admitValid(final RangeReadOperation<I> op) {
		final var circulation = op.circulation();
		final boolean reserved;
		synchronized (localAdmissionLock) {
			if (!open) {
				throwUnchecked(new EOFException("Range admission is closed"));
			}
			// Read the atomic state without acquiring a lifecycle monitor while holding this
			// lock: terminal observers release capacity while holding their lifecycle monitor.
			final var state = circulation.lifecycle().state();
			if (state != OperationLifecycleState.GENERATOR_BUFFERED
							&& state != OperationLifecycleState.DRIVER_QUEUED
							&& state != OperationLifecycleState.DISPATCHED) {
				return false;
			}
			if (admitted.contains(circulation)) {
				reserved = false; // A retry retains its original logical reservation.
			} else {
				if (state == OperationLifecycleState.DISPATCHED || admitted.size() >= capacity) {
					return false;
				}
				admitted.add(circulation);
				reserved = true;
			}
		}
		// Driver callbacks may complete/recycle synchronously; never hold the admission lock.
		// An exceptional handoff retains capacity until recovery because ownership is unknown.
		final boolean accepted = driver.put(op);
		if (!accepted && reserved) {
			synchronized (localAdmissionLock) {
				admitted.remove(circulation);
			}
		}
		return accepted;
	}

	/**
	 * Bounded terminal-observer hook. Release by immutable circulation, never by a recycled
	 * operation's current identity. Call for final outcomes and recovered unattempted work.
	 */
	public boolean releaseSettled(final RangeReadCirculation circulation) {
		synchronized (localAdmissionLock) {
			final var state = circulation.lifecycle().state();
			if (state != OperationLifecycleState.TERMINAL
							&& state != OperationLifecycleState.UNATTEMPTED
							&& state != OperationLifecycleState.UNRESOLVED) {
				return false;
			}
			return admitted.remove(circulation);
		}
	}

	/** Driver queue admission must be backed by the step's bounded logical reservation. */
	public boolean isAdmitted(final RangeReadCirculation circulation) {
		synchronized (localAdmissionLock) {
			return admitted.contains(circulation);
		}
	}

	public int admittedCirculations() {
		synchronized (localAdmissionLock) {
			return admitted.size();
		}
	}

	@Override
	public int put(final List<RangeReadOperation<I>> ops, final int from, final int to) {
		Objects.checkFromToIndex(from, to, ops.size());
		if (!open) {
			throwUnchecked(new EOFException("Range admission is closed"));
		}
		int accepted = 0;
		for (int i = from; i < to; i++) {
			if (!open) {
				break;
			}
			try {
				if (!put(ops.get(i))) {
					break;
				}
			} catch (Exception failure) {
				if (failure instanceof EOFException) {
					break;
				}
				throwUnchecked(failure);
			}
			accepted++;
		}
		return accepted;
	}

	@Override
	public int put(final List<RangeReadOperation<I>> ops) {
		return put(ops, 0, ops.size());
	}

	public void closeAdmission() {
		synchronized (localAdmissionLock) {
			open = false;
		}
	}

	@Override
	public Input<RangeReadOperation<I>> getInput() {
		return driver.getInput();
	}

	@Override
	public void close() throws Exception {
		closeAdmission();
		try {
			driver.close();
		} finally {
			synchronized (localAdmissionLock) {
				admitted.clear();
			}
		}
	}
}
