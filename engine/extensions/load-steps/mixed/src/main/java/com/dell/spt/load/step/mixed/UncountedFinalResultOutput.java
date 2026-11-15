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
 * ended. Only those uncounted results write shared state here. A step with no count limit does
 * not use it.
 *
 * <p>{@link #kind} follows the step context's counting rules in
 * {@code LoadStepContextImpl.put}: its uncounted results are a part's result, an unfinished
 * composite's result unless it failed, an {@code INTERRUPTED} result and an {@code OMIT} one. A
 * mixed step neither retries nor recycles an operation, so each counted result is final.
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
	// and a final result (StorageDriver.completesCompositesInParts), read once at setup.
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
	 * such as the one result of a DELETE or STAT above the ranges threshold, or of any composite on
	 * a driver that publishes one result for it, and an {@code INTERRUPTED} or {@code OMIT} result.
	 */
	static int kind(final Operation<?> op, final boolean compositesInParts) {
		if (op instanceof PartialOperation) {
			return NOT_FINAL;
		}
		final Status status = op.status();
		if (op instanceof CompositeOperation<?> composite && !composite.allSubOperationsDone()) {
			if (isFailure(status)) {
				return COUNTED;
			}
			if (compositesInParts
							&& Status.SUCC.equals(status)
							&& (OpType.CREATE.equals(op.type()) || OpType.READ.equals(op.type()))) {
				return NOT_FINAL;
			}
			return UNCOUNTED;
		}
		if (Status.INTERRUPTED.equals(status) || Status.OMIT.equals(status)) {
			return UNCOUNTED;
		}
		return COUNTED;
	}

	/** As the step context's: a status that ends an operation as failed. */
	private static boolean isFailure(final Status status) {
		return !(Status.SUCC.equals(status) || Status.PENDING.equals(status) || Status.ACTIVE.equals(status)
						|| Status.INTERRUPTED.equals(status) || Status.OMIT.equals(status));
	}
}
