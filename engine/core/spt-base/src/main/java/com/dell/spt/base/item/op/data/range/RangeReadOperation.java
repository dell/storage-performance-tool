package com.dell.spt.base.item.op.data.range;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.load.lifecycle.OperationLifecycle;
import com.dell.spt.base.load.lifecycle.OperationLifecycleState;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.collection.Range;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.LongUnaryOperator;
import java.util.function.LongSupplier;

/**
 * One explicit range READ, never a legacy composite or item slice. Reset preserves selection;
 * only a successful new logical circulation resamples. Result snapshots retain the old selection.
 */
public final class RangeReadOperation<I extends DataItem> extends OperationImpl<I> implements DataOperation<I> {
	private final RangeReadPolicy policy;
	private final LongUnaryOperator boundedDraw;
	private final LongSupplier clock;
	private volatile RangeReadCirculation circulation;
	private long bytesDone;
	private long dataResponseStart;

	public RangeReadOperation(final int originIndex, final I item, final String srcPath,
					final String dstPath, final Credential credential, final RangeReadPolicy policy) {
		this(originIndex, item, srcPath, dstPath, credential, policy,
						bound -> ThreadLocalRandom.current().nextLong(bound));
	}

	RangeReadOperation(final int originIndex, final I item, final String srcPath,
					final String dstPath, final Credential credential, final RangeReadPolicy policy,
					final LongUnaryOperator boundedDraw) {
		this(originIndex, item, srcPath, dstPath, credential, policy, boundedDraw, RangeReadAttempt::clockMicros);
	}

	RangeReadOperation(final int originIndex, final I item, final String srcPath, final String dstPath,
					final Credential credential, final RangeReadPolicy policy, final LongUnaryOperator boundedDraw,
					final LongSupplier clock) {
		super(originIndex, OpType.READ, item, srcPath, dstPath, credential);
		this.clock = Objects.requireNonNull(clock);
		this.policy = Objects.requireNonNull(policy);
		this.boundedDraw = Objects.requireNonNull(boundedDraw);
		this.circulation = new RangeReadCirculation(lifecycle(), select(), clock);
		reset();
	}

	private RangeReadOperation(final RangeReadOperation<I> other) {
		super(other);
		policy = other.policy;
		clock = other.clock;
		boundedDraw = other.boundedDraw;
		circulation = other.circulation;
		bytesDone = other.bytesDone;
		dataResponseStart = other.dataResponseStart;
	}

	public RangeReadPolicy policy() {
		return policy;
	}

	public RangeReadPolicy.Selection selection() {
		return circulation.selection();
	}

	public RangeReadCirculation circulation() {
		return circulation;
	}

	@Override
	public synchronized RangeReadOperation<I> result() {
		buildItemPath(item, dstPath == null ? srcPath : dstPath);
		return new RangeReadOperation<>(this);
	}

	@Override
	public synchronized OperationLifecycle startNextLifecycle() {
		final var previous = lifecycle();
		final var state = previous.state();
		if ((state == OperationLifecycleState.COMPLETING || state == OperationLifecycleState.TERMINAL
						|| state == OperationLifecycleState.UNATTEMPTED) && status != Status.SUCC) {
			// Retired failures and shutdown recovery are not new READ circulations.
			return previous;
		}
		final var next = super.startNextLifecycle();
		if (next != previous) {
			circulation = new RangeReadCirculation(next, select(), clock);
		}
		return next;
	}

	private RangeReadPolicy.Selection select() {
		if (policy.fixedOffset() != null) {
			return policy.select(0, boundedDraw);
		}
		try {
			return policy.select(item.size(), boundedDraw);
		} catch (final IOException unavailable) {
			return policy.select(-1, boundedDraw);
		}
	}

	/** Does not reset/mutate the shared source item or resample a retry's byte range. */
	@Override
	public void reset() {
		resetTiming();
		nodeAddr = null;
		status = Status.PENDING;
		returnedVersionId = null;
		responseRequestId = null;
		integrityVerificationResult = null;
		bytesDone = 0;
		dataResponseStart = 0;
	}

	RangeReadAttempt.Timing timing() {
		return new RangeReadAttempt.Timing(reqTimeStart, reqTimeDone, respTimeStart, dataResponseStart, respTimeDone);
	}

	void timing(final RangeReadAttempt.Timing timing) {
		reqTimeStart = timing.dispatch();
		reqTimeDone = timing.requestComplete();
		respTimeStart = timing.responseHeaders();
		dataResponseStart = timing.firstBody();
		respTimeDone = timing.responseComplete();
	}

	@Override
	public long countBytesDone() {
		return bytesDone;
	}

	@Override
	public void countBytesDone(final long count) {
		if (count < 0 || count > policy.length()) {
			throw new IllegalArgumentException("Successful range bytes must be within the requested length");
		}
		bytesDone = count;
	}

	@Override
	public long respDataTimeStart() {
		return dataResponseStart;
	}

	@Override
	public void startDataResponse() {
		if (reqTimeDone == 0) {
			throw new IllegalStateException("Range response body started before request completion");
		}
		dataResponseStart = START_OFFSET_MICROS + System.nanoTime() / 1000;
	}

	@Override
	public long dataLatency() {
		return reqTimeDone == 0 || dataResponseStart == 0 ? 0 : dataResponseStart - reqTimeDone;
	}

	@Override
	public boolean hasMarkedRanges() {
		return false;
	}

	@Override
	public long markedRangesSize() {
		return 0;
	}

	@Override
	public List<Range> fixedRanges() {
		return List.of();
	}

	@Override
	public int randomRangesCount() {
		return 0;
	}

	@Override
	public List<I> srcItemsToConcat() {
		return List.of();
	}

	@Override
	public int currRangeIdx() {
		return 0;
	}

	@Override
	public void currRangeIdx(final int index) {
		throw legacyUnsupported();
	}

	@Override
	public void markRandomRanges(final int count) {
		throw legacyUnsupported();
	}

	@Override
	public BitSet[] markedRangesMaskPair() {
		throw legacyUnsupported();
	}

	@Override
	public DataItem currRange() {
		throw legacyUnsupported();
	}

	@Override
	public DataItem currRangeUpdate() {
		throw legacyUnsupported();
	}

	private static UnsupportedOperationException legacyUnsupported() {
		return new UnsupportedOperationException("Single-range READ does not use legacy marked ranges or item slices");
	}
}
