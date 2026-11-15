package com.dell.spt.load.step.mixed;

import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.Operation.Status;
import com.dell.spt.base.item.op.composite.CompositeOperation;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.util.List;

/**
 * The driver's result output of a count-limited mixed step. It passes each result on to the step
 * context, which counts most final results, and counts on the generator each final result the
 * step context does not count, so the generator knows when every operation it dispatched has
 * ended. Only those uncounted results write shared state here. A step with no count limit, or on
 * a driver that does not declare its composite results, does not use it.
 *
 * <p>{@link #kind} follows the step context's counting rules in
 * {@code LoadStepContextImpl.put}: its uncounted results are a part's result, an unfinished
 * composite's result unless it failed, an {@code INTERRUPTED} result and an {@code OMIT} one. A
 * mixed step neither retries nor recycles an operation, so each counted result is final.
 *
 * <p>A known limit: the step context does count a part's result published as {@code PENDING}
 * (through {@code recyclePendingOp}), which {@link #kind} takes for a part's result, not final.
 * No in-tree driver publishes one, and the step context counted it before as well. A driver that
 * did would make the step count more results than it ran operations, and end early.
 */
@SuppressWarnings({"rawtypes", "unchecked"
})
final class UncountedFinalResultOutput implements Output {

	/** A result followed by more results of its operation: a part's, or an initiation's. */
	static final int NOT_FINAL = 0;
	/** An operation's final result, which the step context counts. */
	static final int COUNTED = 1;
	/** An operation's final result, which the step context does not count. */
	static final int UNCOUNTED = 2;

	private final Output output;
	private final MixedLoadGenerator generator;
	// Whether the driver follows a successful, unfinished composite CREATE or READ with its parts
	// and a final result (CompositeResults.IN_PARTS from StorageDriver.compositeResults), read once
	// at setup.
	private final boolean compositesInParts;

	UncountedFinalResultOutput(final Output output, final MixedLoadGenerator generator, final boolean compositesInParts) {
		this.output = output;
		this.generator = generator;
		this.compositesInParts = compositesInParts;
	}

	@Override
	public boolean put(final Object op) {
		final int kind = kind((Operation) op, compositesInParts);
		final boolean accepted;
		try {
			accepted = output.put(op);
		} catch (final RuntimeException | Error e) {
			// The step context counts a result last, so a result it threw on is not counted.
			if (kind != NOT_FINAL) {
				generator.onUncountedFinalResult();
			}
			throw e;
		}
		if (kind == UNCOUNTED) {
			generator.onUncountedFinalResult();
		}
		return accepted;
	}

	/** One result at a time, so that each is accounted as {@link #put(Object)} accounts it. */
	@Override
	public int put(final List ops, final int from, final int to) {
		for (int i = from; i < to; i++) {
			put(ops.get(i));
		}
		return to - from;
	}

	@Override
	public int put(final List ops) {
		return put(ops, 0, ops.size());
	}

	@Override
	public Input getInput() {
		return output.getInput();
	}

	@Override
	public void close() throws Exception {
		output.close();
	}

	/**
	 * Whether {@code op} is its operation's final result, and whether the step context counts it.
	 * Not final: a part's result, and, on a driver that completes composites in parts, a successful
	 * unfinished CREATE or READ composite, since its parts and its final result are still to come.
	 * An aborted upload that a driver leaves an unfinished success, as {@code fs} does, looks the
	 * same: the cooperative base then sends its unsent parts and the upload never ends, a known
	 * defect of that base. Final but uncounted: any other unfinished composite that did not fail,
	 * such as the one result of any composite on a driver that publishes one result for it, or of a
	 * composite of another type than CREATE or READ, which only an operations builder outside the
	 * engine makes, and an {@code INTERRUPTED} or {@code OMIT} result.
	 */
	static int kind(final Operation<?> op, final boolean compositesInParts) {
		final Status status = op.status();
		if (status == Status.SUCC) {
			// Most results: tested for a composite first, and for a part only after.
			if (op instanceof CompositeOperation<?> composite) {
				if (op instanceof PartialOperation) {
					return NOT_FINAL;
				}
				if (composite.allSubOperationsDone()) {
					return COUNTED;
				}
				final OpType type = op.type();
				return compositesInParts && (type == OpType.CREATE || type == OpType.READ) ? NOT_FINAL : UNCOUNTED;
			}
			return op instanceof PartialOperation ? NOT_FINAL : COUNTED;
		}
		if (op instanceof PartialOperation) {
			return NOT_FINAL;
		}
		if (status == Status.INTERRUPTED || status == Status.OMIT) {
			return UNCOUNTED;
		}
		if (status == Status.PENDING || status == Status.ACTIVE) {
			// Not a failure: an unfinished composite's result is not counted, as for a success.
			return op instanceof CompositeOperation<?> composite && !composite.allSubOperationsDone() ? UNCOUNTED : COUNTED;
		}
		// A failure, finished or not, and a result with no status.
		return COUNTED;
	}
}
