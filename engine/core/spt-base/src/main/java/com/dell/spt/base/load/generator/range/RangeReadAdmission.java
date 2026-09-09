package com.dell.spt.base.load.generator.range;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.data.range.RangeReadOperation;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.io.EOFException;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * Range-only generator output: valid selections reach the driver, invalid selections consume
 * accepted logical work without transport. Refusal uses the generator's timed output backoff.
 * One instance bounds local errors to 1000/s, burst one, across its concurrent callers. The
 * runtime must share/allocate this admission budget at the step boundary and close admission
 * before recovery; this does not replace the driver's actual transport admission guard.
 */
public final class RangeReadAdmission<I extends DataItem> implements Output<RangeReadOperation<I>> {
	static final long LOCAL_ERROR_INTERVAL_NANOS = 1_000_000;
	private final Output<RangeReadOperation<I>> driver;
	private final OperationLifecycleTracker<? super RangeReadOperation<I>> tracker;
	private final LongSupplier clock;
	private final Object localAdmissionLock = new Object();
	private volatile boolean open = true;
	private boolean pacingStarted;
	private long nextLocalError;

	public RangeReadAdmission(final Output<RangeReadOperation<I>> driver,
					final OperationLifecycleTracker<? super RangeReadOperation<I>> tracker) {
		this(driver, tracker, System::nanoTime);
	}

	public RangeReadAdmission(final Output<RangeReadOperation<I>> driver,
					final OperationLifecycleTracker<? super RangeReadOperation<I>> tracker, final LongSupplier clock) {
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
			return driver.put(op);
		}
		synchronized (localAdmissionLock) {
			if (!open) {
				throwUnchecked(new EOFException("Range admission is closed"));
			}
			final long now = clock.getAsLong();
			if (pacingStarted && now - nextLocalError < 0) {
				return false;
			}
			if (!tracker.localFailure(op, op.lifecycle())) {
				return false;
			}
			// Clock arithmetic deliberately permits nanoTime wraparound over this short interval.
			nextLocalError = now + LOCAL_ERROR_INTERVAL_NANOS;
			pacingStarted = true;
			return true;
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
		driver.close();
	}
}
