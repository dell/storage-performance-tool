package com.dell.spt.load.step.mixed;

import com.dell.spt.base.concurrent.TaskBase;
import com.dell.spt.base.concurrent.ThreadTaskExecutor;
import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.dell.spt.base.item.op.partial.PartialOperation;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.storage.driver.CompositeResults;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;

import java.io.EOFException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;

/**
 * A schedule-driven load generator for mixed workloads.
 *
 * <p>Unlike {@link com.dell.spt.base.load.generator.LoadGeneratorImpl} (which drives a single
 * op type from a single item source), this generator uses a pre-shuffled {@link OpSchedule}
 * to dispatch multiple operation types through a single {@link com.dell.spt.base.storage.driver.StorageDriver}.
 * All threads can perform any operation type, eliminating the concurrency waste of the
 * multi-generator model.
 *
 * <p>Key design choices:
 * <ul>
 *   <li>Single driver with full concurrency — no per-op connection pool splitting
 *   <li>OpSchedule guarantees exact distribution per 1000-op window
 *   <li>DELETE re-rolls to another op type when the delete queue is empty (non-blocking)
 *   <li>GET/STAT use the static read pool; CREATE generates new items; DELETE pops from queue
 * </ul>
 *
 * @param <I> item type
 * @param <O> operation type
 */
public final class MixedLoadGenerator<I extends Item, O extends Operation<I>>
				extends TaskBase implements LoadGenerator<I, O> {

	private final OpSchedule schedule;
	private final PoolItemInput<I> pool;
	private final Map<OpType, OperationsBuilder<I, O>> builders;
	private final Output<O> opOutput;
	private final Input<I> newItemInput;
	private final Semaphore concurrencyThrottle;
	private final Map<OpType, AtomicInteger> inFlightByOpType;

	private final Queue<O> recycleQueue = new ConcurrentLinkedQueue<>();
	private final LongAdder generatedCount = new LongAdder();
	private volatile boolean itemInputFinished = false;
	// The most operations this generator dispatches; Long.MAX_VALUE when the step has no count limit.
	private final long countLimit;
	// Operations the driver accepted. Written and read on the task thread only, so a plain long.
	private long dispatched = 0;
	// Set once dispatch has reached the count limit, read by the step's completion check.
	private volatile boolean countLimitReached = false;
	// Whether the driver declares how it publishes composite results, see compositeResults. Set once
	// at setup, before operationLifecycle, which alone reads it.
	private boolean compositeResultsDeclared;
	// Operations whose final result the step context did not count, such as an interrupted one.
	// Counted only in a count-limited step, by UncountedFinalResultOutput on the drivers'
	// completion threads; a result the step context counts is not counted here. Each platform
	// thread counts in a cell of its own, which it alone writes, so an increment needs no locked
	// instruction; virtual threads, unbounded in number, share an adder.
	private final ThreadLocal<UncountedCell> uncountedCell = ThreadLocal.withInitial(this::newUncountedCell);
	private final List<UncountedCell> uncountedCells = new CopyOnWriteArrayList<>();
	private final LongAdder uncountedOnVirtualThreads = new LongAdder();
	private volatile OperationLifecycleTracker<O> operationLifecycle = OperationLifecycleTracker.disabled();
	private final AtomicReference<IntegrityTerminalException> terminalFailure = new AtomicReference<>();
	// Set on the task thread when the driver refuses an operation with EOF, read by handleError.
	private boolean outputClosed = false;

	private static final long CAPPED_PARK_NANOS = 1_000_000L;

	// Active op types excluding DELETE (for re-roll)
	private final OpType[] rerollTargets;
	private final int[] rerollCumulativeWeights;
	private final int rerollTotalWeight;

	/**
	 * @param executor         thread executor for the generation task
	 * @param schedule         pre-shuffled operation schedule
	 * @param pool             item source (read pool + delete queue); its read pool may be empty
	 *                         only when the schedule has no GET or STAT
	 * @param builders         per-op-type operation builders
	 * @param opOutput         driver to submit operations to
	 * @param concurrencyLimit max in-flight operations (matches driver concurrency)
	 * @param newItemInput     item source for CREATE operations (generates new items)
	 * @param countLimit       the most operations to dispatch, the step's {@code load-op-limit-count};
	 *                         0 or less, or {@link Long#MAX_VALUE}, for no limit
	 * @throws IllegalArgumentException if the schedule has GET or STAT and the read pool is empty
	 */
	public MixedLoadGenerator(
					final ThreadTaskExecutor executor,
					final OpSchedule schedule,
					final PoolItemInput<I> pool,
					final Map<OpType, OperationsBuilder<I, O>> builders,
					final Output<O> opOutput,
					final int concurrencyLimit,
					final Input<I> newItemInput,
					final long countLimit) {
		super(executor);
		this.countLimit = isCountLimit(countLimit) ? countLimit : Long.MAX_VALUE;
		this.schedule = schedule;
		this.pool = pool;
		this.builders = builders;
		this.opOutput = opOutput;
		this.concurrencyThrottle = new Semaphore(
						concurrencyLimit > 0 ? concurrencyLimit : Integer.MAX_VALUE, true);
		this.newItemInput = newItemInput;
		this.inFlightByOpType = new EnumMap<>(OpType.class);

		// Build the re-roll target list: all active ops except DELETE
		final OpType[] active = schedule.activeOpTypes();
		final java.util.List<OpType> targets = new java.util.ArrayList<>(active.length);
		for (final OpType op : active) {
			// Checked here, once, so the per-operation draw needs no check of its own.
			if ((op == OpType.READ || op == OpType.STAT) && pool.readPoolSize() == 0) {
				throw new IllegalArgumentException(
								"MixedLoad: GET and STAT draw from the seed items, and the read pool has none");
			}
			inFlightByOpType.put(op, new AtomicInteger());
			if (op != OpType.DELETE) {
				targets.add(op);
			}
		}
		this.rerollTargets = targets.toArray(new OpType[0]);
		this.rerollCumulativeWeights = new int[this.rerollTargets.length];
		int cumulative = 0;
		for (int i = 0; i < this.rerollTargets.length; i++) {
			cumulative += schedule.weight(this.rerollTargets[i]);
			this.rerollCumulativeWeights[i] = cumulative;
		}
		this.rerollTotalWeight = cumulative;
	}

	/** A generator with no count limit. Preserves the constructor descriptor used by existing extensions. */
	public MixedLoadGenerator(
					final ThreadTaskExecutor executor,
					final OpSchedule schedule,
					final PoolItemInput<I> pool,
					final Map<OpType, OperationsBuilder<I, O>> builders,
					final Output<O> opOutput,
					final int concurrencyLimit,
					final Input<I> newItemInput) {
		this(executor, schedule, pool, builders, opOutput, concurrencyLimit, newItemInput, 0);
	}

	/** Preserves the constructor descriptor used by existing extensions. */
	public MixedLoadGenerator(
					final VirtualThreadExecutor executor,
					final OpSchedule schedule,
					final PoolItemInput<I> pool,
					final Map<OpType, OperationsBuilder<I, O>> builders,
					final Output<O> opOutput,
					final int concurrencyLimit,
					final Input<I> newItemInput) {
		this((ThreadTaskExecutor) executor, schedule, pool, builders, opOutput, concurrencyLimit, newItemInput, 0);
	}

	@Override
	protected void doInit() {
		outputClosed = false;
		ThreadContext.put("generator", "mixed");
	}

	@Override
	protected void doWork() throws Exception {
		// The count limit caps dispatch here, as LoadGeneratorImpl caps generation: the step's own
		// count check polls, and would let everything dispatched before it sees the limit run.
		if (dispatched >= countLimit) {
			countLimitReached = true;
			itemInputFinished = true;
			// Parked, not yielded, so the task thread does not spin until the step stops.
			LockSupport.parkNanos(CAPPED_PARK_NANOS);
			return;
		}
		// Acquire a concurrency permit before generating the next op.
		// This prevents the generator from flooding the driver's input queue
		// faster than the driver can process operations.
		concurrencyThrottle.acquire();
		boolean permitTransferred = false;

		try {
			OpType opType = schedule.next();

			// Re-roll DELETE when the queue is empty
			if (opType == OpType.DELETE) {
				final I deleteItem = pool.pollDelete();
				if (deleteItem == null) {
					opType = reroll();
					permitTransferred = dispatchNonDelete(opType);
				} else {
					permitTransferred = dispatchDelete(deleteItem);
				}
			} else {
				permitTransferred = dispatchNonDelete(opType);
			}
		} catch (final Error e) {
			// The task runner catches only exceptions: an error would end the task unreported.
			throw failure(e);
		} finally {
			if (!permitTransferred) {
				concurrencyThrottle.release();
			}
		}
	}

	private boolean dispatchNonDelete(final OpType opType) throws IOException {
		final I item;
		if (opType == OpType.CREATE) {
			item = newItemInput.get();
			if (item == null) {
				itemInputFinished = true;
				Thread.yield();
				return false;
			}
		} else {
			// READ or STAT — random item from static pool
			item = pool.randomItem();
		}

		final O op = builders.get(opType).buildOp(item);
		final AtomicInteger counter = inFlightByOpType.get(opType);
		countInFlight(counter);
		boolean accepted = false;
		try {
			accepted = opOutput.put(op);
		} catch (final Exception e) {
			// The coop drivers throw EOFException, unchecked, once their admission is closed.
			if (e instanceof EOFException) {
				outputClosed = true;
			}
			throw e;
		} finally {
			if (!accepted) {
				countOutOfFlight(counter);
			}
		}
		if (!accepted) {
			Thread.yield();
			return false;
		}
		generatedCount.increment();
		dispatched++;
		return true;
	}

	private boolean dispatchDelete(final I item) throws IOException {
		final O op = builders.get(OpType.DELETE).buildOp(item);
		final AtomicInteger counter = inFlightByOpType.get(OpType.DELETE);
		countInFlight(counter);
		boolean accepted = false;
		try {
			accepted = opOutput.put(op);
		} catch (final Exception e) {
			if (e instanceof EOFException) {
				// The driver refused the item, so it stays in the delete queue.
				pool.addDeleteItem(item);
				outputClosed = true;
			}
			throw e;
		} finally {
			if (!accepted) {
				countOutOfFlight(counter);
			}
		}
		if (!accepted) {
			// Put item back if driver rejected — avoid item loss
			pool.addDeleteItem(item);
			Thread.yield();
			return false;
		}
		generatedCount.increment();
		dispatched++;
		return true;
	}

	/**
	 * Counts an operation in flight before it is handed to the driver, which can complete it
	 * before {@code put} returns: counted afterwards, that completion would find nothing to
	 * subtract and the operation would stay counted for the rest of the step.
	 */
	private static void countInFlight(final AtomicInteger counter) {
		if (counter != null) {
			counter.incrementAndGet();
		}
	}

	/** Counts one operation out of flight, at its completion or when the driver did not accept it. */
	private static void countOutOfFlight(final AtomicInteger counter) {
		if (counter != null) {
			counter.getAndUpdate(v -> v > 0 ? v - 1 : 0);
		}
	}

	/** Re-roll DELETE to another active op type (weighted by configured non-DELETE weights). */
	private OpType reroll() {
		if (rerollTargets.length == 1) {
			return rerollTargets[0];
		}
		final int draw = ThreadLocalRandom.current().nextInt(rerollTotalWeight);
		for (int i = 0; i < rerollCumulativeWeights.length; i++) {
			if (draw < rerollCumulativeWeights[i]) {
				return rerollTargets[i];
			}
		}
		return rerollTargets[rerollTargets.length - 1];
	}

	@Override
	protected void doStop() {
		Loggers.MSG.info("MixedLoadGenerator: generated {} ops total", generatedCount.sum());
	}

	@Override
	protected void handleError(final Exception e) {
		if (outputClosed && e instanceof EOFException) {
			// The driver closes its admission when the step stops, as LoadGeneratorImpl expects.
			Loggers.MSG.debug("MixedLoadGenerator: finish due to the driver's EOF");
			return;
		}
		if (e instanceof EOFException) {
			LogUtil.exception(Level.ERROR, e, "MixedLoadGenerator error in doWork");
			return;
		}
		// The task has stopped, so the step can only fail: ending it as done would report success.
		failure(e);
	}

	/**
	 * Records the failure that stopped the task as the step's terminal failure. Its public message
	 * names only the failure's class; the failure goes to the log.
	 */
	private IntegrityTerminalException failure(final Throwable cause) {
		final var found = IntegrityTerminalException.find(cause);
		final var terminal = found != null
						? found
						: new IntegrityTerminalException(
										IntegrityTerminalException.Category.EXECUTION,
										"Mixed load generator stopped on an unexpected " + cause.getClass().getName(),
										cause);
		terminalFailure.compareAndSet(null, terminal);
		if (found == null) {
			LogUtil.exception(Level.ERROR, cause, "MixedLoadGenerator error in doWork");
		}
		return terminal;
	}

	@Override
	public IntegrityTerminalException terminalFailure() {
		return terminalFailure.get();
	}

	@Override
	protected void doClose() {
		recycleQueue.clear();
		try {
			if (newItemInput != null) {
				newItemInput.close();
			}
		} catch (final Exception e) {
			LogUtil.exception(Level.WARN, e, "Failed to close new item input");
		}
	}

	// ── LoadGenerator interface ───────────────────────────────────────────

	@Override
	public boolean isItemInputFinished() {
		return itemInputFinished;
	}

	@Override
	public long generatedOpCount() {
		return generatedCount.sum();
	}

	@Override
	public void recycle(final O op) {
		// For mixed mode, recycling means:
		// - GET/STAT: item goes back to pool (already there — static pool, no-op)
		// - CREATE: successful PUTs feed the delete queue (handled by result callback)
		// - DELETE: item already consumed, no recycle
		// We don't use the standard recycle queue for mixed mode.
	}

	@Override
	public void retry(final O op) {
		// Deliberate no-op, unreachable in practice: supportsRetry() below reports false,
		// which LoadStepContextImpl validates against at construction time (load-op-retry
		// against a generator that can't actually requeue a failed operation would silently
		// drop it - neither retried nor counted as failed).
	}

	@Override
	public void operationLifecycle(final OperationLifecycleTracker<O> lifecycle) {
		this.operationLifecycle = lifecycle == null ? OperationLifecycleTracker.disabled() : lifecycle;
		if (lifecycle != null && lifecycle.isEnabled() && compositeResultsDeclared) {
			lifecycle.unattemptedObserver(this::onOperationUnattempted);
		}
	}

	/**
	 * Counts out an operation the driver accepted but never sent, which publishes no result at
	 * stop. The generator registers it only on a driver that declares its composite results (see
	 * {@link #compositeResults}). The tracker calls it only for an operation that entered the
	 * driver's custody as one the generator dispatched, not one the driver created itself ({@code
	 * OperationLifecycleTracker.driverCreatedQueued}), so it debits only operations the generator
	 * counted in. A part was never counted in either, so it never counts out, from whatever route.
	 * A composite counts out here once: the result wrapper retires a composite only at its last
	 * result, so an unfinished one is still counted when the stop settles it, and a composite that
	 * published its last result is terminal and is not settled unattempted after it. Runs inside
	 * the tracker's unattempted transition, so it must stay bounded and non-throwing.
	 */
	void onOperationUnattempted(final O op) {
		if (!(op instanceof PartialOperation)) {
			onOperationCompleted(op);
		}
	}

	/**
	 * Tells the generator how its driver publishes composite results, read once at setup, before
	 * {@link #operationLifecycle}. Only on a driver that declares it ({@link CompositeResults#IN_PARTS}
	 * or {@link CompositeResults#ONE_RESULT}) does the generator count out an operation the driver
	 * held unsent at the stop, since only there does the result wrapper retire each operation once.
	 * On any other driver ({@link CompositeResults#UNDECLARED}), and when this is never called, the
	 * step keeps its earlier accounting: every result retires an operation and nothing counts out
	 * at the stop.
	 */
	void compositeResults(final CompositeResults compositeResults) {
		compositeResultsDeclared = compositeResults == CompositeResults.IN_PARTS
						|| compositeResults == CompositeResults.ONE_RESULT;
	}

	/**
	 * Whether {@code countLimit}, a step's {@code load-op-limit-count}, limits the step: 0 or less,
	 * and {@link Long#MAX_VALUE}, mean no limit.
	 */
	static boolean isCountLimit(final long countLimit) {
		return countLimit > 0 && countLimit < Long.MAX_VALUE;
	}

	/**
	 * Whether a step with {@code countLimit} caps its dispatch at it, on a driver that publishes
	 * composite results as {@code compositeResults} says: only on a driver that declares how, since
	 * the step can tell an operation's final result only then. Any other driver keeps the step as it
	 * was before, with no cap, so it can run past its count.
	 */
	static boolean capsDispatch(final long countLimit, final CompositeResults compositeResults) {
		return isCountLimit(countLimit)
						&& (compositeResults == CompositeResults.IN_PARTS || compositeResults == CompositeResults.ONE_RESULT);
	}

	/**
	 * Whether dispatch has reached the count limit, every operation dispatched has published its
	 * final result, and the driver holds no operation in its lifecycle custody. Each final result
	 * is either among the step context's {@code countedResults} or counted here as uncounted (see
	 * {@link UncountedFinalResultOutput}), so a step then ends even if some of those operations
	 * produced no counted result, for example an {@code INTERRUPTED} one. Read by the step's
	 * completion check, not per operation.
	 */
	@Override
	public boolean isCountLimitDrained(final long countedResults) {
		// At the cap the driver has accepted exactly countLimit operations, and the mixed step
		// neither retries nor recycles one, so each has exactly one final result.
		return countLimitReached
						&& countedResults + uncountedFinalResultCount() >= countLimit
						&& !operationLifecycle.hasOutstandingOperations();
	}

	/**
	 * Counts one operation's final result that the step context did not count. Called only in a
	 * count-limited step, after the step context has taken the result.
	 */
	void onUncountedFinalResult() {
		if (Thread.currentThread().isVirtual()) {
			uncountedOnVirtualThreads.increment();
		} else {
			uncountedCell.get().increment();
		}
	}

	/**
	 * The uncounted final results so far. A count taken while results arrive may miss the latest,
	 * never adds one that has not arrived, and is exact once they have all arrived.
	 */
	long uncountedFinalResultCount() {
		long count = uncountedOnVirtualThreads.sum();
		for (final UncountedCell cell : uncountedCells) {
			count += cell.count();
		}
		return count;
	}

	private UncountedCell newUncountedCell() {
		final UncountedCell cell = new UncountedCell();
		uncountedCells.add(cell);
		return cell;
	}

	/** One platform thread's count of uncounted final results: written by that thread alone. */
	static final class UncountedCell {
		private static final VarHandle COUNT;

		static {
			try {
				COUNT = MethodHandles.lookup().findVarHandle(UncountedCell.class, "count", long.class);
			} catch (final ReflectiveOperationException e) {
				throw new ExceptionInInitializerError(e);
			}
		}
		private long count;

		void increment() {
			// The single writer reads its own last write, so a release store is enough.
			COUNT.setRelease(this, count + 1);
		}

		long count() {
			return (long) COUNT.getAcquire(this);
		}
	}

	@Override
	public boolean isNothingToRecycle() {
		return true; // Mixed mode doesn't use standard recycling
	}

	@Override
	public boolean supportsRetry() {
		return false;
	}

	/**
	 * Release one concurrency permit. Called by the step context when an
	 * operation completes (success or failure) to allow the generator to
	 * dispatch the next operation.
	 */
	public void releasePermit() {
		concurrencyThrottle.release();
	}

	public void onOperationCompleted(final O op) {
		if (op == null) {
			return;
		}
		countOutOfFlight(inFlightByOpType.get(op.type()));
	}

	public int inFlightCount(final OpType opType) {
		final AtomicInteger counter = inFlightByOpType.get(opType);
		return counter != null ? counter.get() : 0;
	}
}
