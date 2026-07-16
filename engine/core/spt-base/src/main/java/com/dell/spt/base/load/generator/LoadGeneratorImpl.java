package com.dell.spt.base.load.generator;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.concurrent.TaskBase;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.commons.collection.CircularArrayBuffer;
import com.github.akurilov.commons.collection.CircularBuffer;
import com.github.akurilov.commons.concurrent.throttle.IndexThrottle;
import com.github.akurilov.commons.concurrent.throttle.Throttle;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Locale;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.ThreadContext;

/** Created by kurila on 11.07.16. */
public class LoadGeneratorImpl<I extends Item, O extends Operation<I>> extends TaskBase
				implements LoadGenerator<I, O> {

	private static final String CLS_NAME = LoadGeneratorImpl.class.getSimpleName();

	private volatile boolean recycleQueueFullState = false;
	private volatile boolean itemInputFinishFlag = false;
	private volatile boolean opInputFinishFlag = false;
	private volatile boolean outputFinishFlag = false;

	private final Input<I> itemInput;
	private final OperationsBuilder<I, O> opsBuilder;
	private final int originIndex;
	private final Object[] throttles;
	private final Output<O> opOutput;
	private final Lock inputLock = new ReentrantLock();
	private final int batchSize;
	private final long countLimit;
	private final Queue<O> recycleQueue;
	private final int recycleQueueCapacity;
	private final AtomicInteger recycleQueueSize = new AtomicInteger();
	// load-op-retry redispatches: deliberately a *separate* queue from recycleQueue above,
	// drained unconditionally on every doWork() iteration regardless of countLimit/
	// itemInputFinishFlag state. See LoadGenerator#retry's javadoc for why a retry must not
	// be gated by (or count against) load-op-limit-count the way recycleQueue's true
	// recycle-mode contents deliberately are.
	private final Queue<O> retryQueue = new ConcurrentLinkedQueue<>();
	private final boolean recycleFlag;
	// See the retryFlag constructor javadoc: only controls the countLimit self-stop below.
	private final boolean retryFlag;
	private final boolean shuffleFlag;
	private final Random rnd;
	private final String name;
	private volatile boolean fastRecycleQuiesce = false;
	private volatile boolean generatorParked = false;
	private volatile Thread generatorThread;
	private final ThreadLocal<CircularBuffer<O>> threadLocalOpBuff;
	private final LongAdder builtTasksCounter = new LongAdder();
	private final LongAdder recycledOpCounter = new LongAdder();
	private final LongAdder outputOpCounter = new LongAdder();
	private final Lock tempBufferLock = new ReentrantLock();
	private List<I> items;

	@SuppressWarnings("unchecked")
	public LoadGeneratorImpl(
					final Input<I> itemInput,
					final OperationsBuilder<I, O> opsBuilder,
					final List<Object> throttles,
					final Output<O> opOutput,
					final int batchSize,
					final long countLimit,
					final int recycleQueueSize,
					final boolean recycleFlag,
					final boolean shuffleFlag) {
		this(itemInput, opsBuilder, throttles, opOutput, batchSize, countLimit, recycleQueueSize, recycleFlag, shuffleFlag, false);
	}

	/**
	 * @param retryFlag whether {@code load-op-retry} is enabled. Unrelated to {@code
	 *                  recycleFlag} (see {@link LoadGenerator#retry}'s javadoc for the
	 *                  dedicated retry path) - this only controls whether reaching {@code
	 *                  countLimit} self-stops the generator outright. A dispatch that just
	 *                  exhausted the count limit may still turn into a retry once it
	 *                  completes (that decision is made later, by whoever is watching this
	 *                  generator's output - the generator itself has no visibility into it),
	 *                  so it must not shut itself down before that can happen.
	 */
	@SuppressWarnings("unchecked")
	public LoadGeneratorImpl(
					final Input<I> itemInput,
					final OperationsBuilder<I, O> opsBuilder,
					final List<Object> throttles,
					final Output<O> opOutput,
					final int batchSize,
					final long countLimit,
					final int recycleQueueSize,
					final boolean recycleFlag,
					final boolean shuffleFlag,
					final boolean retryFlag) {
		super(ServiceTaskExecutor.TASK_EXECUTOR);
		this.itemInput = itemInput;
		this.opsBuilder = opsBuilder;
		this.originIndex = opsBuilder.originIndex();
		this.throttles = throttles.toArray(new Object[]{});
		this.opOutput = opOutput;
		this.batchSize = batchSize;
		this.countLimit = countLimit > 0 ? countLimit : Long.MAX_VALUE;
		this.recycleQueue = new ConcurrentLinkedQueue<>();
		this.recycleQueueCapacity = recycleQueueSize;
		this.recycleFlag = recycleFlag;
		this.retryFlag = retryFlag;
		this.shuffleFlag = shuffleFlag;
		this.rnd = shuffleFlag ? new Random() : null;
		final var ioStr = opsBuilder.opType().toString();
		name = Character.toUpperCase(ioStr.charAt(0))
						+ ioStr.substring(1).toLowerCase(Locale.ROOT)
						+ (countLimit > 0 && countLimit < Long.MAX_VALUE ? Long.toString(countLimit) : "")
						+ itemInput.toString();
		threadLocalOpBuff = ThreadLocal.withInitial(() -> new CircularArrayBuffer<>(batchSize));
		this.items = new ArrayList<>(batchSize); // prepare the items buffer
	}

	@Override
	protected void doInit() {
		ThreadContext.put(KEY_CLASS_NAME, CLS_NAME);
		generatorThread = Thread.currentThread();
	}

	@Override
	protected final void doWork() throws Exception {

		drainRetryQueue();

		final var opBuff = threadLocalOpBuff.get();
		var pendingOpCount = opBuff.size();
		var n = batchSize - pendingOpCount;

		try {

			if (n > 0) { // the tasks buffer has free space for the new tasks
				if (itemInputFinishFlag) { // items input was exhausted
					if (!recycleFlag) { // never recycled -> recycling is not enabled
						opInputFinishFlag = true; // allow shutdown
					} else { // recycle the tasks if any
						n = 0;
						O recycledOp;
						final var limit = batchSize - pendingOpCount;
						// Spin-poll: try the lock-free queue, then spin-wait (PAUSE
						// instruction, ~10ns) for up to ~1μs before falling back to
						// parkNanos. This avoids the full futex/context-switch cost
						// when ops return quickly from the server.
						var spins = 0;
						while (n < limit) {
							recycledOp = recycleQueue.poll();
							if (recycledOp != null) {
								opBuff.add(recycledOp);
								n++;
								spins = 0;
							} else if (spins < 128) {
								Thread.onSpinWait();
								spins++;
							} else {
								break;
							}
						}
						if (n > 0) {
							recycleQueueSize.addAndGet(-n);
							pendingOpCount += n;
							recycledOpCounter.add(n);
						} else {
							// No recycled ops available right now.
							if (fastRecycleQuiesce) {
								// Fast-recycle is handling ops inline in the driver;
								// park this task thread until recycle() unparks us or the
								// timeout expires. 10ms keeps it idle
								// while still bounding wake-up latency for fallback ops.
								generatorParked = true;
								LockSupport.parkNanos(10_000_000);
								generatorParked = false;
							} else {
								// Yield the task thread so in-flight ops can complete
								// without a timed parking syscall.
								yieldThread();
							}
						}
					}
				} else {
					// produce new items from the items input
					inputLock.lock();
					try {
						// find the remaining count of the ops to generate
						final var remainingOpCount = countLimit - generatedOpCount();
						if (remainingOpCount > 0) {
							// make the limit not more than batch size
							n = (int) Math.min(remainingOpCount, n);
							tempBufferLock.lock();
							try {
								items = getItems(itemInput, n);
							} catch (final ConcurrentModificationException cme) {
								Loggers.MSG.debug(
												"{}: item input changed while fetching operations; retrying",
												name);
								items.clear();
								return;
							} finally {
								tempBufferLock.unlock();
							}

							if (items == null) {
								itemInputFinishFlag = true;
								Loggers.MSG.debug(
												"End of items input \"{}\", generated op count: {}",
												itemInput.toString(),
												generatedOpCount());
							} else {
								tempBufferLock.lock();
								try {
									n = items.size();
									if (n > 0) {
										final long newlyBuilt = buildOps(items, opBuff, n);
										pendingOpCount = (int) (pendingOpCount + newlyBuilt);
									} else {
										itemInputFinishFlag = true;
									}
								} catch (final ConcurrentModificationException cme) {
									Loggers.MSG.debug(
													"{}: items buffer changed while building operations; retrying",
													name);
									items.clear();
									return;
								} finally {
									tempBufferLock.unlock();
								}
							}
						}
					} finally {
						inputLock.unlock();
					}
				}
			}

			if (outputOpCounter.sum() < countLimit) {

				if (pendingOpCount > 0) {

					n = pendingOpCount;

					// acquire the permit for all the throttles
					for (final Object throttle : throttles) {
						if (throttle instanceof Throttle) {
							n = ((Throttle) throttle).tryAcquire(n);
						} else if (throttle instanceof IndexThrottle) {
							n = ((IndexThrottle) throttle).tryAcquire(originIndex, n);
						} else {
							throw new AssertionError("Unexpected throttle type: " + throttle.getClass());
						}
					}

					// try to output
					var outputProgress = false;
					if (n > 0) {
						if (n == 1) { // single mode branch
							try {
								final var op = opBuff.get(0);
								if (opOutput.put(op)) {
									outputOpCounter.increment();
									if (pendingOpCount == 1) {
										opBuff.clear();
									} else {
										opBuff.remove(0);
									}
									outputProgress = true;
								}
							} catch (final Exception e) {
								throwUncheckedIfInterrupted(e);
								if (e instanceof EOFException) {
									Loggers.MSG.debug("{}: finish due to output's EOF, {}", name, e);
									outputFinishFlag = true;
								} else {
									LogUtil.exception(Level.ERROR, e, "{}: operation output failure", name);
								}
							}
						} else { // batch mode branch
							try {
								n = opOutput.put(opBuff, 0, n);
								outputOpCounter.add(n);
								if (n > 0) {
									outputProgress = true;
								}
								if (n < pendingOpCount) {
									opBuff.removeFirst(n);
								} else {
									opBuff.clear();
								}
							} catch (final Exception e) {
								throwUncheckedIfInterrupted(e);
								if (e instanceof EOFException) {
									Loggers.MSG.debug("{}: finish due to output's EOF, {}", name, e);
									outputFinishFlag = true;
								} else {
									LogUtil.trace(Loggers.ERR, Level.ERROR, e, "Unexpected failure");
								}
							}
						}
					}
					// Backpressure relief: if we had ops to send but made no output progress
					// (either throttle denied permits or output queue was full), briefly
					// yield the task thread to avoid a CPU-burning spin loop.
					if (!outputProgress) {
						yieldThread();
					}
				} else if (retryFlag) {
					// Nothing pending to dispatch (e.g. item input just exhausted). With
					// load-op-retry enabled, isFinished() deliberately won't self-stop on
					// that alone (see its own comment) - it may sit in exactly this state
					// for as long as the slowest still-in-flight operation takes to
					// resolve (potentially real network I/O plus backoff delay), so this
					// must yield rather than busy-spin. Without load-op-retry, isFinished()
					// will pick this up and stop the generator on the very next check
					// below, so a yield here would rarely even execute - only bothering
					// for the retry case keeps the common path branch-free.
					yieldThread();
				}
			} else if (!retryFlag) { // operations count limit is reached
				outputFinishFlag = true;
			} else {
				// Count limit reached, but load-op-retry is enabled: a dispatch that just
				// consumed the last slot in the count budget may still turn into a retry
				// once it completes - that decision happens later (by whoever is watching
				// this generator's output), so self-stopping here could abandon it before
				// it ever gets a configured retry attempt. Keep the task alive (yielding, not
				// busy-spinning) so drainRetryQueue() above keeps running; whoever is
				// watching completion externally (comparing terminal results against how
				// many operations were actually generated, which correctly accounts for
				// in-flight retries not yet resolved) will call stop() once truly done.
				yieldThread();
			}

		} catch (final EOFException eof) {
			Loggers.MSG.debug("{}: terminating load generator due to EOF", name);
		} catch (final Throwable t) {
			throwUncheckedIfInterrupted(t);
			LogUtil.trace(Loggers.ERR, Level.ERROR, t, "{}: unexpected failure", name);
		} finally {
			if (isFinished()) {
				try {
					stop();
				} catch (final IllegalStateException e) {
					Loggers.MSG.debug("{}: stop already in progress; ignoring redundant stop", name);
				}
			}
		}
	}

	private List<I> getItems(final Input<I> itemInput, final int n) {
		items.clear();
		try {
			itemInput.get(items, n); // get the items from the input
		} catch (final Exception e) {
			throwUncheckedIfInterrupted(e);
			if (e instanceof EOFException) {
				return null;
			}
		}
		return items;
	}

	@SuppressWarnings("ThreadPriorityCheck") // intentional cooperative scheduler hint
	private static void yieldThread() {
		Thread.yield();
	}

	// build new tasks for the corresponding items
	private long buildOps(final List<I> items, final CircularBuffer<O> opBuff, final int n)
					throws IOException {
		if (shuffleFlag) {
			Collections.shuffle(items, rnd);
		}
		try {
			opsBuilder.buildOps(items, opBuff);
			builtTasksCounter.add(n);
			return n;
		} catch (final IllegalArgumentException e) {
			LogUtil.exception(Level.ERROR, e, "Failed to generate the load operation");
		}
		return 0;
	}

	@Override
	public final boolean isItemInputFinished() {
		return itemInputFinishFlag;
	}

	@Override
	public final long generatedOpCount() {
		return builtTasksCounter.sum() + recycledOpCounter.sum();
	}

	@Override
	public final void recycle(final O op) {
		recycleQueue.add(op);
		final var size = recycleQueueSize.incrementAndGet();
		if (!recycleQueueFullState && size > recycleQueueCapacity) {
			recycleQueueFullState = true;
			Loggers.ERR.warn("{}: recycle queue exceeded configured capacity ({})", name, recycleQueueCapacity);
		}
		// Wake the generator task if it's actually parked in the quiesce state.
		// Checking generatorParked avoids spurious unpark() calls when the
		// generator is running (e.g. at higher concurrency where fast-recycle
		// doesn't handle all ops).
		if (fastRecycleQuiesce && generatorParked) {
			LockSupport.unpark(generatorThread);
		}
	}

	@Override
	public final void retry(final O op) {
		retryQueue.add(op);
		if (fastRecycleQuiesce && generatorParked) {
			LockSupport.unpark(generatorThread);
		}
	}

	/**
	 * Dispatches every operation currently sitting in {@link #retryQueue}, bypassing
	 * countLimit entirely (see {@link LoadGenerator#retry}'s javadoc for why) but still
	 * subject to the same rate/index throttles as normal dispatch - unlike {@code
	 * load-op-limit-count} (a unit-count budget a retry must not consume), throttles pace
	 * the actual request rate against the target, and letting a burst of retries bypass
	 * them too could exceed the user's configured rate by roughly the original traffic
	 * plus retry traffic during a failure storm. Called unconditionally at the very start
	 * of every {@link #doWork()} iteration, regardless of {@code itemInputFinishFlag}/
	 * count-limit state, so a retry always gets its configured attempt even if the
	 * generator's normal fresh-dispatch budget is already exhausted. Bounded to one pass
	 * over however many are queued *right now* (retries are inherently rare relative to
	 * the main workload; this avoids a pathological retry storm starving the rest of this
	 * method indefinitely).
	 */
	private void drainRetryQueue() {
		var remaining = retryQueue.size();
		O retryOp;
		while (remaining-- > 0 && (retryOp = retryQueue.poll()) != null) {
			var permitted = 1;
			for (final Object throttle : throttles) {
				if (throttle instanceof Throttle) {
					permitted = ((Throttle) throttle).tryAcquire(permitted);
				} else if (throttle instanceof IndexThrottle) {
					permitted = ((IndexThrottle) throttle).tryAcquire(originIndex, permitted);
				} else {
					throw new AssertionError("Unexpected throttle type: " + throttle.getClass());
				}
			}
			if (permitted <= 0) {
				// Throttled - preserve it for a later iteration rather than dropping it or
				// busy-spinning retrying the same op immediately.
				retryQueue.add(retryOp);
				break;
			}
			try {
				if (!opOutput.put(retryOp)) {
					// Driver backpressure - preserve it for the next iteration rather than
					// dropping it, same as the normal dispatch path does for its own buffer.
					retryQueue.add(retryOp);
					break;
				}
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				if (e instanceof EOFException) {
					// The output itself is done and will never accept anything again
					// (including a re-enqueue) - matches how the normal dispatch path
					// below also abandons its own buffered op on output EOF rather than
					// looping forever trying to redeliver it.
					Loggers.MSG.debug("{}: retry redispatch finished due to output's EOF, {}", name, e);
					outputFinishFlag = true;
					break;
				} else {
					LogUtil.exception(Level.ERROR, e, "{}: retry redispatch failure, will retry next iteration", name);
					retryQueue.add(retryOp);
					break;
				}
			}
		}
	}

	@Override
	public final boolean isNothingToRecycle() {
		return recycleQueue.isEmpty();
	}

	@Override
	public final boolean isNothingPendingRetry() {
		return retryQueue.isEmpty();
	}

	@Override
	public final List<O> drainPendingRetries() {
		if (retryQueue.isEmpty()) {
			return List.of();
		}
		final List<O> drained = new ArrayList<>();
		O op;
		while ((op = retryQueue.poll()) != null) {
			drained.add(op);
		}
		return drained;
	}

	@Override
	public void enableFastRecycleQuiesce() {
		fastRecycleQuiesce = true;
		Loggers.MSG.info("{}: fast-recycle quiesce enabled (generator task will park when idle)", name);
	}

	private boolean isFinished() {
		// Never actually finish while a load-op-retry redispatch is still waiting to be
		// drained - otherwise stop() below would tear this generator down before
		// drainRetryQueue() ever got to run it, abandoning it unresolved forever (it isn't
		// in recycleQueue/opBuff at that point, so nothing else would ever pick it up).
		if (!retryQueue.isEmpty()) {
			return false;
		}
		if (retryFlag) {
			// With load-op-retry enabled, "every generated operation has been handed to
			// the output" (the formula below) is not the same thing as "every generated
			// operation has been resolved" - a real (especially async/cooperative) driver
			// can report completion long after accepting a dispatch, and that completion
			// might still turn into a retry once it arrives. This generator has no
			// visibility into that decision (it's made later, by whoever is watching this
			// generator's output), so it must not self-stop on this signal alone - only a
			// hard stop (outputFinishFlag, e.g. output EOF) or an explicit external stop()
			// call should end it. LoadStepContextImpl's own completion check (comparing
			// terminal results against operations generated, which correctly accounts for
			// operations still in flight or retrying) decides when to make that call.
			return outputFinishFlag;
		}
		return outputFinishFlag
						|| (itemInputFinishFlag && opInputFinishFlag && generatedOpCount() == outputOpCounter.sum());
	}

	@Override
	protected final void doStop() {
		Loggers.MSG.debug(
						"{}: generated {}, recycled {}, output {} operations",
						LoadGeneratorImpl.this.toString(),
						builtTasksCounter.sum(),
						recycledOpCounter.sum(),
						outputOpCounter.sum());
	}

	@Override
	protected final void doClose() {
		recycleQueue.clear();
		retryQueue.clear();
		// the item input may be instantiated by the load generator builder which has no reference to it
		// so the load
		// generator builder should close it
		if (itemInput != null) {
			inputLock.lock();
			try {
				itemInput.close();
			} catch (final Exception e) {
				LogUtil.exception(Level.WARN, e, "{}: failed to close the item input", toString());
			} finally {
				inputLock.unlock();
			}
		}
		// ops builder is instantiated by the load generator builder which forgets it so the load
		// generator should close it
		opsBuilder.close();
	}

	@Override
	public final String toString() {
		return name;
	}
}
