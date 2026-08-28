package com.dell.spt.base.load.generator;

import static com.dell.spt.base.Constants.KEY_CLASS_NAME;
import static com.dell.spt.base.Constants.TASK_STOP_WAIT_SECONDS;
import static com.dell.spt.base.Exceptions.throwUncheckedIfInterrupted;
import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
import com.dell.spt.base.concurrent.TaskBase;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.io.RemainingItemCountInput;
import com.dell.spt.base.item.io.TerminalItemInputException;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationAssembler;
import com.dell.spt.base.item.op.OperationAssemblyResult;
import com.dell.spt.base.item.op.OperationAssemblyStopReason;
import com.dell.spt.base.item.op.OperationsBuilder;
import com.dell.spt.base.item.op.OperationsBuilderAssembler;
import com.dell.spt.base.item.op.deletion.DeleteRequestAssembler;
import com.dell.spt.base.logging.LogUtil;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.load.step.DurationTime;
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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
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

	private static final class IdentityKey<T> {
		private final T value;
		private final int hashCode;

		private IdentityKey(final T value) {
			this.value = value;
			this.hashCode = System.identityHashCode(value);
		}

		@Override
		public boolean equals(final Object obj) {
			return obj instanceof IdentityKey<?> other && value == other.value;
		}

		@Override
		public int hashCode() {
			return hashCode;
		}
	}

	private record SchedulingExhaustion(long observedAtNanos) {}

	private volatile boolean recycleQueueFullState = false;
	private volatile boolean itemInputFinishFlag = false;
	private volatile boolean opInputFinishFlag = false;
	private volatile boolean outputFinishFlag = false;

	private final Input<I> itemInput;
	private final OperationAssembler<I, O> opAssembler;
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
	private final ThreadLocal<CircularBuffer<O>> threadLocalOpBuff;
	private final ThreadLocal<List<O>> threadLocalAssemblyBuff;
	private final LongAdder consumedItemsCounter = new LongAdder();
	private final LongAdder aggregateUnattemptedItemsCounter = new LongAdder();
	private final LongAdder builtTasksCounter = new LongAdder();
	private final LongAdder recycledOpCounter = new LongAdder();
	private final LongAdder outputOpCounter = new LongAdder();
	private final AtomicReference<IntegrityTerminalException> terminalFailure = new AtomicReference<>();
	private final AtomicReference<SchedulingExhaustion> schedulingExhaustion = new AtomicReference<>();
	private final AtomicBoolean admissionOpen = new AtomicBoolean(true);
	private volatile long admissionDeadlineNanos;
	private volatile boolean admissionDeadlineSet;
	private final AtomicBoolean assemblerFinished = new AtomicBoolean(false);
	private final Lock assemblyLock = new ReentrantLock();
	private final Lock admissionLock = new ReentrantLock();
	private final long standaloneDeleteInputIdentityCount;
	private final Set<IdentityKey<O>> compatibilityBufferedOperations = ConcurrentHashMap.newKeySet();
	private volatile OperationLifecycleTracker<O> operationLifecycle = OperationLifecycleTracker.disabled();
	private boolean custodyModeSelected;
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
	 * Creates a generator using a cardinality-neutral operation assembler.
	 *
	 * @param opAssembler assembler that owns its retained operation-building resources
	 */
	public LoadGeneratorImpl(
					final Input<I> itemInput,
					final OperationAssembler<I, O> opAssembler,
					final List<Object> throttles,
					final Output<O> opOutput,
					final int batchSize,
					final long countLimit,
					final int recycleQueueSize,
					final boolean recycleFlag,
					final boolean shuffleFlag) {
		this(itemInput, opAssembler, throttles, opOutput, batchSize, countLimit, recycleQueueSize, recycleFlag, shuffleFlag, false);
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
		this(
						itemInput,
						new OperationsBuilderAssembler<>(opsBuilder),
						throttles,
						opOutput,
						batchSize,
						countLimit,
						recycleQueueSize,
						recycleFlag,
						shuffleFlag,
						retryFlag);
	}

	/**
	 * Creates a generator using a cardinality-neutral operation assembler.
	 *
	 * @param opAssembler assembler that owns its retained operation-building resources
	 * @param retryFlag whether {@code load-op-retry} is enabled; see the compatibility
	 *                  constructor for its lifecycle semantics
	 */
	@SuppressWarnings("unchecked")
	public LoadGeneratorImpl(
					final Input<I> itemInput,
					final OperationAssembler<I, O> opAssembler,
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
		this.opAssembler = opAssembler;
		if (opAssembler instanceof DeleteRequestAssembler) {
			if (!(itemInput instanceof RemainingItemCountInput<?> countInput)) {
				throw new IllegalArgumentException(
								"Standalone DELETE requires an input with an exact remaining-item count");
			}
			this.standaloneDeleteInputIdentityCount = countInput.remainingItemCount();
			if (standaloneDeleteInputIdentityCount < 0) {
				throw new IllegalArgumentException(
								"Standalone DELETE input reported a negative remaining-item count");
			}
		} else {
			this.standaloneDeleteInputIdentityCount = -1;
		}
		this.originIndex = opAssembler.originIndex();
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
		final var ioStr = opAssembler.opType().toString();
		name = Character.toUpperCase(ioStr.charAt(0))
						+ ioStr.substring(1).toLowerCase(Locale.ROOT)
						+ (countLimit > 0 && countLimit < Long.MAX_VALUE ? Long.toString(countLimit) : "")
						+ itemInput.toString();
		threadLocalOpBuff = ThreadLocal.withInitial(() -> new CircularArrayBuffer<>(batchSize));
		threadLocalAssemblyBuff = ThreadLocal.withInitial(() -> new ArrayList<>(batchSize));
		this.items = new ArrayList<>(batchSize); // prepare the items buffer
	}

	@Override
	protected void doInit() {
		ThreadContext.put(KEY_CLASS_NAME, CLS_NAME);
	}

	@Override
	protected final void doWork() throws Exception {

		final var priorTerminalFailure = terminalFailure.get();
		if (priorTerminalFailure != null) {
			throw priorTerminalFailure;
		}

		drainRetryQueue();

		final var opBuff = operationBuffer();
		var pendingOpCount = opBuff.size();
		var n = batchSize - pendingOpCount;

		try {

			if (n > 0) { // the tasks buffer has free space for the new tasks
				if (itemInputFinishFlag) { // items input was exhausted
					if (!recycleFlag) { // never recycled -> recycling is not enabled
						pendingOpCount += finishAssemblerNormally(opBuff);
						opInputFinishFlag = assemblerFinished.get();
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
							// Yield so in-flight operations can complete and return through
							// recycleQueue without imposing a timed parking syscall.
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
								pendingOpCount += finishAssemblerNormally(opBuff);
								if (!recycleFlag) {
									opInputFinishFlag = true;
								}
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
										pendingOpCount += finishKnownStandaloneInput(opBuff);
									} else {
										itemInputFinishFlag = true;
										pendingOpCount += finishAssemblerNormally(opBuff);
										if (!recycleFlag) {
											opInputFinishFlag = true;
										}
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

			if (admissionOpen.get() && outputOpCounter.sum() < countLimit) {

				if (pendingOpCount > 0) {

					var permittedCount = pendingOpCount;

					// acquire the permit for all the throttles
					for (final Object throttle : throttles) {
						if (throttle instanceof Throttle) {
							permittedCount = ((Throttle) throttle).tryAcquire(permittedCount);
						} else if (throttle instanceof IndexThrottle) {
							permittedCount = ((IndexThrottle) throttle).tryAcquire(originIndex, permittedCount);
						} else {
							throw new AssertionError("Unexpected throttle type: " + throttle.getClass());
						}
					}
					assertOutputRange("Throttle permit", permittedCount, pendingOpCount, opBuff.size());

					// try to output
					var outputProgress = false;
					if (permittedCount > 0 && isAdmissionOpen()) {
						if (permittedCount == 1) { // single mode branch
							try {
								final var op = opBuff.get(0);
								if (opOutput.put(op)) {
									outputOpCounter.increment();
									onFinalOperationHandoff();
									releaseCompatibilityCustody(op);
									if (pendingOpCount == 1) {
										opBuff.clear();
									} else {
										opBuff.remove(0);
									}
									outputProgress = true;
								}
							} catch (final Exception e) {
								throwUncheckedIfInterrupted(e);
								final var terminal = IntegrityTerminalException.find(e);
								if (terminal != null) {
									throw terminal;
								}
								if (e instanceof EOFException) {
									Loggers.MSG.debug("{}: finish due to output's EOF, {}", name, e);
									outputFinishFlag = true;
								} else {
									LogUtil.exception(Level.ERROR, e, "{}: operation output failure", name);
								}
							}
						} else { // batch mode branch
							try {
								final var writtenCount = opOutput.put(opBuff, 0, permittedCount);
								assertOutputRange("Operation output", writtenCount, permittedCount, opBuff.size());
								outputOpCounter.add(writtenCount);
								onFinalOperationHandoff();
								for (var i = 0; i < writtenCount; i++) {
									releaseCompatibilityCustody(opBuff.get(i));
								}
								if (writtenCount > 0) {
									outputProgress = true;
								}
								if (writtenCount < pendingOpCount) {
									opBuff.removeFirst(writtenCount);
								} else {
									opBuff.clear();
								}
							} catch (final Exception e) {
								throwUncheckedIfInterrupted(e);
								final var terminal = IntegrityTerminalException.find(e);
								if (terminal != null) {
									throw terminal;
								}
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
			final var terminal = IntegrityTerminalException.find(t);
			if (terminal != null) {
				terminalFailure.compareAndSet(null, terminal);
				throw terminal;
			}
			LogUtil.trace(Loggers.ERR, Level.ERROR, t, "{}: unexpected failure", name);
		} finally {
			if (isFinished()) {
				if (finiteSchedulingExhausted()) {
					recordSchedulingExhaustion();
				}
				try {
					stop();
				} catch (final IllegalStateException e) {
					Loggers.MSG.debug("{}: stop already in progress; ignoring redundant stop", name);
				}
			}
		}
	}

	@Override
	public final long schedulingExhaustedAtNanos() {
		final SchedulingExhaustion exhaustion = schedulingExhaustion.get();
		return exhaustion == null ? Long.MAX_VALUE : exhaustion.observedAtNanos();
	}

	@Override
	public final OptionalLong schedulingExhaustionNanos() {
		final SchedulingExhaustion exhaustion = schedulingExhaustion.get();
		return exhaustion == null
						? OptionalLong.empty()
						: OptionalLong.of(exhaustion.observedAtNanos());
	}

	private boolean finiteSchedulingExhausted() {
		return !recycleFlag
						&& itemInputFinishFlag
						&& opInputFinishFlag
						&& generatedOpCount() == outputOpCounter.sum();
	}

	private void onFinalOperationHandoff() {
		if (finiteSchedulingExhausted()) {
			recordSchedulingExhaustion();
			afterFinalOperationHandoff();
		}
	}

	private void recordSchedulingExhaustion() {
		if (schedulingExhaustion.get() == null) {
			schedulingExhaustion.compareAndSet(
							null, new SchedulingExhaustion(schedulingExhaustionTimeNanos()));
		}
	}

	/** Package-local deterministic clock seam, read only for a finite scheduling transition. */
	long schedulingExhaustionTimeNanos() {
		return System.nanoTime();
	}

	/** Package-local deterministic scheduling seam, invoked only for the final finite handoff. */
	void afterFinalOperationHandoff() {}

	private int finishKnownStandaloneInput(final CircularBuffer<O> opBuff) throws IOException {
		if (standaloneDeleteInputIdentityCount < 0) {
			return 0;
		}
		final long consumedIdentityCount = consumedItemsCounter.sum();
		if (consumedIdentityCount < standaloneDeleteInputIdentityCount) {
			return 0;
		}
		if (consumedIdentityCount > standaloneDeleteInputIdentityCount) {
			throw contractFailure(
							"Standalone DELETE consumed "
											+ consumedIdentityCount
											+ " identities from an input that reported "
											+ standaloneDeleteInputIdentityCount);
		}
		itemInputFinishFlag = true;
		final int finishedOperationCount = finishAssemblerNormally(opBuff);
		if (!recycleFlag) {
			opInputFinishFlag = assemblerFinished.get();
		}
		return finishedOperationCount;
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
			final var terminal = IntegrityTerminalException.find(e);
			if (terminal != null) {
				throw terminal;
			}
			if (e instanceof TerminalItemInputException) {
				throw new IntegrityTerminalException(
								IntegrityTerminalException.Category.INPUT, e.getMessage(), e);
			}
			if (opAssembler instanceof DeleteRequestAssembler) {
				throw new IntegrityTerminalException(
								IntegrityTerminalException.Category.INPUT,
								"Standalone DELETE input failed before its frozen selection was consumed",
								e);
			}
			LogUtil.exception(Level.WARN, e, "Failed to read the next load-generator items");
			return null;
		}
		return items;
	}

	private CircularBuffer<O> operationBuffer() {
		return threadLocalOpBuff.get();
	}

	private boolean isAdmissionOpen() {
		admissionLock.lock();
		try {
			return admissionAllowedLocked();
		} finally {
			admissionLock.unlock();
		}
	}

	private boolean admissionAllowedLocked() {
		if (!admissionOpen.get()) {
			return false;
		}
		if (admissionDeadlineSet
						&& DurationTime.deadlineReached(admissionDeadlineNanos, System.nanoTime())) {
			admissionOpen.set(false);
			return false;
		}
		return true;
	}

	private static void yieldThread() {
		LockSupport.parkNanos(50_000);
	}

	private void assertOutputRange(
					final String source, final int count, final int requestedCount, final int bufferSize) {
		if (count < 0 || count > requestedCount || count > bufferSize) {
			throw contractFailure(
							source
											+ " count "
											+ count
											+ " is outside [0, "
											+ requestedCount
											+ "] for an operation buffer of size "
											+ bufferSize);
		}
	}

	// build new tasks for the corresponding items
	private long buildOps(final List<I> items, final CircularBuffer<O> opBuff, final int n)
					throws IOException {
		if (shuffleFlag) {
			Collections.shuffle(items, rnd);
		}
		assemblyLock.lock();
		try {
			if (!isAdmissionOpen() || assemblerFinished.get()) {
				return 0;
			}
			final var assemblyBuffer = threadLocalAssemblyBuff.get();
			try {
				assemblyBuffer.clear();
				final var assemblyResult = opAssembler.assemble(items, assemblyBuffer);
				if (assemblyResult.consumedIdentityCount() != n) {
					throw contractFailure(
									"Operation assembler consumed "
													+ assemblyResult.consumedIdentityCount()
													+ " identities from an input batch of "
													+ n);
				}
				final var emittedOperationCount = assemblyResult.emittedOperationCount();
				if (emittedOperationCount != assemblyBuffer.size()) {
					throw contractFailure(
									"Operation assembler reported "
													+ emittedOperationCount
													+ " emitted operations but appended "
													+ assemblyBuffer.size());
				}
				final var availableOperationSlots = batchSize - opBuff.size();
				if (emittedOperationCount > availableOperationSlots) {
					throw contractFailure(
									"Operation assembler emitted "
													+ emittedOperationCount
													+ " operations with only "
													+ availableOperationSlots
													+ " buffer slots available");
				}
				final var bufferedCount = admitAssembledOperations(opBuff, assemblyBuffer);
				consumedItemsCounter.add(assemblyResult.consumedIdentityCount());
				builtTasksCounter.add(emittedOperationCount);
				return bufferedCount;
			} catch (final RuntimeException e) {
				if (!(opAssembler instanceof OperationsBuilderAssembler<?, ?>)) {
					throw abortAssembly(e, n);
				}
				if (e instanceof IllegalArgumentException) {
					LogUtil.exception(Level.ERROR, e, "Failed to generate the load operation");
				} else {
					throw e;
				}
			} catch (final IOException e) {
				if (!(opAssembler instanceof OperationsBuilderAssembler<?, ?>)) {
					throw abortAssembly(e, n);
				}
				throw e;
			} finally {
				assemblyBuffer.clear();
			}
			return 0;
		} finally {
			assemblyLock.unlock();
		}
	}

	private int admitAssembledOperations(
					final CircularBuffer<O> opBuff, final List<O> assemblyBuffer) {
		admissionLock.lock();
		try {
			if (admissionAllowedLocked()) {
				registerAssemblyCustody(assemblyBuffer);
				opBuff.addAll(assemblyBuffer);
				return assemblyBuffer.size();
			}
			for (final O op : assemblyBuffer) {
				registerGeneratorCustody(op);
				operationLifecycle.unattempted(op);
				releaseCompatibilityCustody(op);
			}
			return 0;
		} finally {
			admissionLock.unlock();
		}
	}

	private void registerAssemblyCustody(final List<O> operations) {
		var registeredCount = 0;
		try {
			for (final O op : operations) {
				registerGeneratorCustody(op);
				registeredCount++;
			}
		} catch (final IntegrityTerminalException failure) {
			for (var i = 0; i < registeredCount; i++) {
				releaseRegisteredGeneratorCustody(operations.get(i));
			}
			throw failure;
		}
	}

	private void registerGeneratorCustody(final O op) {
		custodyModeSelected = true;
		if (!operationLifecycle.isEnabled()) {
			compatibilityBufferedOperations.add(identityKey(op));
			return;
		}
		final boolean registered;
		try {
			registered = operationLifecycle.generatorBuffered(op);
		} catch (final RuntimeException failure) {
			throw registrationFailure(failure);
		}
		if (!registered) {
			throw registrationFailure(null);
		}
	}

	private IntegrityTerminalException registrationFailure(final RuntimeException cause) {
		final var failure = new IntegrityTerminalException(
						IntegrityTerminalException.Category.EXECUTION,
						"Operation lifecycle rejected generator custody registration",
						cause);
		terminalFailure.compareAndSet(null, failure);
		return terminalFailure.get();
	}

	private void releaseRegisteredGeneratorCustody(final O op) {
		if (operationLifecycle.isEnabled()) {
			operationLifecycle.unattempted(op);
		} else {
			compatibilityBufferedOperations.remove(identityKey(op));
		}
	}

	private void releaseCompatibilityCustody(final O op) {
		if (!operationLifecycle.isEnabled()) {
			compatibilityBufferedOperations.remove(identityKey(op));
		}
	}

	private IntegrityTerminalException abortAssembly(
					final Exception assemblyFailure, final int failedInputIdentityCount) {
		outputFinishFlag = true;
		final int unrecoverableIdentityCount = opAssembler instanceof DeleteRequestAssembler deleteAssembler
						? deleteAssembler.unrecoverableIdentityCount()
						: 0;
		consumedItemsCounter.add(failedInputIdentityCount - unrecoverableIdentityCount);
		if (assemblerFinished.compareAndSet(false, true)) {
			final var assemblyBuffer = new ArrayList<O>(1);
			try {
				finishAssembler(OperationAssemblyStopReason.ABORTED, assemblyBuffer);
				for (final var operation : assemblyBuffer) {
					registerGeneratorCustody(operation);
					operationLifecycle.unattempted(operation);
					releaseCompatibilityCustody(operation);
				}
			} catch (final IOException abortFailure) {
				assemblyFailure.addSuppressed(abortFailure);
			} finally {
				aggregateUnattemptedItemsCounter.add(unrecoverableIdentityCount);
			}
		}
		return new IntegrityTerminalException(
						IntegrityTerminalException.Category.EXECUTION,
						"Operation assembly failed after recovering retained work",
						assemblyFailure);
	}

	private AssertionError contractFailure(final String message) {
		outputFinishFlag = true;
		return new AssertionError(message);
	}

	private int finishAssemblerNormally(final CircularBuffer<O> opBuff) throws IOException {
		assemblyLock.lock();
		try {
			// finish() may consume one retained tail. Defer that irreversible call until
			// the dispatch buffer has the slot promised by OperationAssembler.
			if (opBuff.size() >= batchSize) {
				return 0;
			}
			if (!assemblerFinished.compareAndSet(false, true)) {
				return 0;
			}
			final var assemblyBuffer = threadLocalAssemblyBuff.get();
			assemblyBuffer.clear();
			try {
				final var result = finishAssembler(
								OperationAssemblyStopReason.NORMAL_COMPLETION, assemblyBuffer);
				final var availableOperationSlots = batchSize - opBuff.size();
				if (result.emittedOperationCount() > availableOperationSlots) {
					throw contractFailure(
									"Operation assembler finished with "
													+ result.emittedOperationCount()
													+ " operations but only "
													+ availableOperationSlots
													+ " buffer slots are available");
				}
				return admitAssembledOperations(opBuff, assemblyBuffer);
			} finally {
				assemblyBuffer.clear();
			}
		} finally {
			assemblyLock.unlock();
		}
	}

	private void finishAssemblerForRecovery() {
		if (!assemblerFinished.compareAndSet(false, true)) {
			return;
		}
		final var assemblyBuffer = new ArrayList<O>(1);
		try {
			finishAssembler(OperationAssemblyStopReason.ADMISSION_CLOSED, assemblyBuffer);
			registerAssemblyCustody(assemblyBuffer);
		} catch (final IOException e) {
			throw new IllegalStateException("Failed to recover retained operation-assembler work", e);
		}
	}

	private OperationAssemblyResult finishAssembler(
					final OperationAssemblyStopReason reason, final List<O> assemblyBuffer)
					throws IOException {
		final var result = opAssembler.finish(reason, assemblyBuffer);
		validateFinishedAssembly(result, assemblyBuffer);
		builtTasksCounter.add(result.emittedOperationCount());
		return result;
	}

	private void validateFinishedAssembly(
					final OperationAssemblyResult result, final List<O> assemblyBuffer) {
		if (result.consumedIdentityCount() != 0) {
			throw contractFailure(
							"Operation assembler consumed identities while finishing: "
											+ result.consumedIdentityCount());
		}
		if (result.emittedOperationCount() != assemblyBuffer.size()) {
			throw contractFailure(
							"Operation assembler reported "
											+ result.emittedOperationCount()
											+ " finished operations but appended "
											+ assemblyBuffer.size());
		}
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
	public final long consumedItemCount() {
		return consumedItemsCounter.sum();
	}

	@Override
	public final long aggregateUnattemptedItemCount() {
		return aggregateUnattemptedItemsCounter.sum();
	}

	@Override
	public final void recycle(final O op) {
		admissionLock.lock();
		try {
			if (!admissionAllowedLocked()) {
				operationLifecycle.unattempted(op);
				return;
			}
			registerGeneratorCustody(op);
			recycleQueue.add(op);
			final var size = recycleQueueSize.incrementAndGet();
			if (!recycleQueueFullState && size > recycleQueueCapacity) {
				recycleQueueFullState = true;
				Loggers.ERR.warn("{}: recycle queue exceeded configured capacity ({})", name, recycleQueueCapacity);
			}
		} finally {
			admissionLock.unlock();
		}
	}

	@Override
	public final void retry(final O op) {
		admissionLock.lock();
		try {
			if (!admissionAllowedLocked()) {
				operationLifecycle.unattempted(op);
				return;
			}
			registerGeneratorCustody(op);
			retryQueue.add(op);
		} finally {
			admissionLock.unlock();
		}
	}

	@Override
	public final void operationLifecycle(final OperationLifecycleTracker<O> lifecycle) {
		final var selectedLifecycle = lifecycle == null
						? OperationLifecycleTracker.<O> disabled()
						: lifecycle;
		if (operationLifecycle == selectedLifecycle) {
			return;
		}
		assemblyLock.lock();
		try {
			admissionLock.lock();
			try {
				if (isStarted()
								|| !isGeneratorCustodyEmpty()
								|| hasOutstandingCustody(selectedLifecycle)
								|| (custodyModeSelected && admissionOpen.get())) {
					throw new IllegalStateException(
									"Operation custody may only change while the generator is stopped and empty");
				}
				operationLifecycle = selectedLifecycle;
				custodyModeSelected = true;
			} finally {
				admissionLock.unlock();
			}
		} finally {
			assemblyLock.unlock();
		}
	}

	private boolean isGeneratorCustodyEmpty() {
		return compatibilityBufferedOperations.isEmpty()
						&& retryQueue.isEmpty()
						&& recycleQueue.isEmpty()
						&& !hasOutstandingCustody(operationLifecycle);
	}

	private boolean hasOutstandingCustody(final OperationLifecycleTracker<O> lifecycle) {
		final var snapshot = lifecycle.snapshot();
		return snapshot.generatorBuffered() != 0
						|| snapshot.driverQueued() != 0
						|| snapshot.inFlight() != 0;
	}

	@Override
	public final void openAdmission() {
		admissionLock.lock();
		try {
			admissionDeadlineSet = false;
			admissionOpen.set(true);
		} finally {
			admissionLock.unlock();
		}
	}

	@Override
	public final void openAdmissionUntil(final long deadlineNanos) {
		admissionLock.lock();
		try {
			admissionDeadlineNanos = deadlineNanos;
			admissionDeadlineSet = true;
			admissionOpen.set(!DurationTime.deadlineReached(deadlineNanos, System.nanoTime()));
		} finally {
			admissionLock.unlock();
		}
	}

	@Override
	public final void holdAdmission() {
		admissionLock.lock();
		try {
			admissionDeadlineSet = false;
			admissionOpen.set(false);
		} finally {
			admissionLock.unlock();
		}
	}

	@Override
	public final void closeAdmission() {
		admissionLock.lock();
		try {
			admissionOpen.set(false);
		} finally {
			admissionLock.unlock();
		}
		final boolean taskWasRunning = isStarted();
		final boolean taskWasAlreadyStopped = isStopped();
		stop();
		if (taskWasRunning || taskWasAlreadyStopped) {
			try {
				if (!await(TASK_STOP_WAIT_SECONDS, TimeUnit.SECONDS)) {
					Loggers.ERR.warn(
									"{}: generator task did not stop within {} second(s); recovering without waiting for its input read",
									name,
									TASK_STOP_WAIT_SECONDS);
				}
			} catch (final InterruptedException e) {
				throwUnchecked(e);
			}
		}
	}

	@Override
	public final List<O> recoverBufferedOperations() {
		assemblyLock.lock();
		try {
			admissionLock.lock();
			try {
				recoverUnconsumedStandaloneDeleteItems();
				IntegrityTerminalException tailFailure = null;
				try {
					finishAssemblerForRecovery();
				} catch (final IntegrityTerminalException failure) {
					tailFailure = failure;
				}
				final List<O> recovered;
				if (operationLifecycle.isEnabled()) {
					recovered = operationLifecycle.recoverGeneratorBufferedAsUnattempted();
				} else {
					final var recoveredByIdentity = new LinkedHashMap<IdentityKey<O>, O>();
					for (final var operationKey : compatibilityBufferedOperations) {
						recoveredByIdentity.put(operationKey, operationKey.value);
					}
					compatibilityBufferedOperations.clear();
					recovered = List.copyOf(recoveredByIdentity.values());
				}
				retryQueue.clear();
				recycleQueue.clear();
				recycleQueueSize.set(0);
				if (tailFailure != null) {
					throw tailFailure;
				}
				return recovered;
			} finally {
				admissionLock.unlock();
			}
		} finally {
			assemblyLock.unlock();
		}
	}

	private void recoverUnconsumedStandaloneDeleteItems() {
		if (!(opAssembler instanceof DeleteRequestAssembler) || itemInputFinishFlag) {
			return;
		}
		final long unreadIdentityCount = Math.subtractExact(
						Math.subtractExact(
										standaloneDeleteInputIdentityCount, consumedItemsCounter.sum()),
						aggregateUnattemptedItemsCounter.sum());
		if (unreadIdentityCount < 0) {
			throw new IntegrityTerminalException(
							IntegrityTerminalException.Category.EXECUTION,
							"Standalone DELETE consumed more identities than its frozen input count",
							null);
		}
		aggregateUnattemptedItemsCounter.add(unreadIdentityCount);
		itemInputFinishFlag = true;
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
		while (remaining-- > 0) {
			O retryOp = null;
			admissionLock.lock();
			try {
				if (!admissionAllowedLocked()) {
					break;
				}
				retryOp = retryQueue.poll();
				if (retryOp == null) {
					break;
				}
			} finally {
				admissionLock.unlock();
			}
			try {
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
					requeueRetry(retryOp);
					break;
				}
				if (!isAdmissionOpen() || !opOutput.put(retryOp)) {
					// Driver backpressure - preserve it for the next iteration rather than
					// dropping it, same as the normal dispatch path does for its own buffer.
					requeueRetry(retryOp);
					break;
				}
				releaseCompatibilityCustody(retryOp);
			} catch (final Exception e) {
				throwUncheckedIfInterrupted(e);
				final var terminal = IntegrityTerminalException.find(e);
				if (terminal != null) {
					throw terminal;
				}
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
					if (retryOp != null) {
						requeueRetry(retryOp);
					}
					break;
				}
			}
		}
	}

	private void requeueRetry(final O retryOp) {
		admissionLock.lock();
		try {
			if (admissionAllowedLocked()) {
				retryQueue.add(retryOp);
			} else {
				operationLifecycle.unattempted(retryOp);
				releaseCompatibilityCustody(retryOp);
			}
		} finally {
			admissionLock.unlock();
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
	public final IntegrityTerminalException terminalFailure() {
		return terminalFailure.get();
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
		closeAdmission();
		recoverBufferedOperations();
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
		// The assembler owns the operation-building resources supplied to the generator.
		opAssembler.close();
	}

	@Override
	public final String toString() {
		return name;
	}

	private static <T> IdentityKey<T> identityKey(final T value) {
		return new IdentityKey<>(value);
	}
}
