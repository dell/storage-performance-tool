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
	private final boolean recycleFlag;
	private final boolean shuffleFlag;
	private final Random rnd;
	private final String name;
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
		super(ServiceTaskExecutor.VT_EXECUTOR);
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
	}

	@Override
	protected final void doWork() throws Exception {

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
							// Yield the virtual thread back to the ForkJoinPool so
							// in-flight ops can complete. Thread.yield() is purely
							// userspace on VTs — no futex/context-switch overhead
							// unlike LockSupport.parkNanos() which triggers a full
							// kernel round-trip even for parkNanos(1).
							yieldThread();
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
					// park the VT to avoid a CPU-burning spin loop. 1μs is 1000x faster
					// than the previous 1ms sleep; with the dispatch task's Condition-based
					// signaling the output queue drains within microseconds.
					if (!outputProgress) {
						LockSupport.parkNanos(1_000);
					}
				}
			} else { // operations count limit is reached
				outputFinishFlag = true;
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

	@SuppressWarnings("ThreadPriorityCheck") // intentional: VT yield is userspace-only, no kernel cost
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
	}

	@Override
	public final boolean isNothingToRecycle() {
		return recycleQueue.isEmpty();
	}

	private boolean isFinished() {
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
