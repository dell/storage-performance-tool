package com.dell.spt.load.step.mixed;

import com.dell.spt.base.concurrent.TaskBase;
import com.dell.spt.base.concurrent.ThreadTaskExecutor;
import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.dell.spt.base.load.generator.LoadGenerator;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;

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

	// Active op types excluding DELETE (for re-roll)
	private final OpType[] rerollTargets;
	private final int[] rerollCumulativeWeights;
	private final int rerollTotalWeight;

	/**
	 * @param executor         thread executor for the generation task
	 * @param schedule         pre-shuffled operation schedule
	 * @param pool             item source (read pool + delete queue)
	 * @param builders         per-op-type operation builders
	 * @param opOutput         driver to submit operations to
	 * @param concurrencyLimit max in-flight operations (matches driver concurrency)
	 * @param newItemInput     item source for CREATE operations (generates new items)
	 */
	public MixedLoadGenerator(
					final ThreadTaskExecutor executor,
					final OpSchedule schedule,
					final PoolItemInput<I> pool,
					final Map<OpType, OperationsBuilder<I, O>> builders,
					final Output<O> opOutput,
					final int concurrencyLimit,
					final Input<I> newItemInput) {
		super(executor);
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

	/** Preserves the constructor descriptor used by existing extensions. */
	public MixedLoadGenerator(
					final VirtualThreadExecutor executor,
					final OpSchedule schedule,
					final PoolItemInput<I> pool,
					final Map<OpType, OperationsBuilder<I, O>> builders,
					final Output<O> opOutput,
					final int concurrencyLimit,
					final Input<I> newItemInput) {
		this((ThreadTaskExecutor) executor, schedule, pool, builders, opOutput, concurrencyLimit, newItemInput);
	}

	@Override
	protected void doInit() {
		ThreadContext.put("generator", "mixed");
	}

	@Override
	protected void doWork() throws Exception {
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
		if (!opOutput.put(op)) {
			Thread.yield();
			return false;
		}
		final AtomicInteger counter = inFlightByOpType.get(opType);
		if (counter != null) {
			counter.incrementAndGet();
		}
		generatedCount.increment();
		return true;
	}

	private boolean dispatchDelete(final I item) throws IOException {
		final O op = builders.get(OpType.DELETE).buildOp(item);
		if (!opOutput.put(op)) {
			// Put item back if driver rejected — avoid item loss
			pool.addDeleteItem(item);
			Thread.yield();
			return false;
		}
		final AtomicInteger counter = inFlightByOpType.get(OpType.DELETE);
		if (counter != null) {
			counter.incrementAndGet();
		}
		generatedCount.increment();
		return true;
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
		LogUtil.exception(Level.ERROR, e, "MixedLoadGenerator error in doWork");
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
		final AtomicInteger counter = inFlightByOpType.get(op.type());
		if (counter != null) {
			counter.getAndUpdate(v -> v > 0 ? v - 1 : 0);
		}
	}

	public int inFlightCount(final OpType opType) {
		final AtomicInteger counter = inFlightByOpType.get(opType);
		return counter != null ? counter.get() : 0;
	}
}
