package com.dell.spt.base.load.step.local;

import static com.github.akurilov.commons.lang.Exceptions.throwUnchecked;

import com.dell.spt.base.concurrent.AsyncRunnableBase;
import com.dell.spt.base.integrity.IntegrityTerminalException;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.deletion.DeleteRequestOperation;
import com.dell.spt.base.item.op.deletion.DeleteTransportResult;
import com.dell.spt.base.load.lifecycle.OperationLifecycleTracker;
import com.dell.spt.base.storage.driver.StorageDriver;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

/**
 * Test-only storage driver: every operation completes after a fixed latency {@code L}, with at
 * most {@code C} operations in flight, so a saturated closed loop runs at {@code C / L}.
 *
 * <p>The counters follow the driver contract: {@link #scheduledOpCount()} counts operations
 * accepted into the driver (its input queue, or straight to dispatch without one),
 * {@link #activeOpCount()} those holding one of the {@link #concurrencyLimit()} permits, held ones
 * included, and {@link #completedOpCount()} those finished, whatever their outcome. A permit is
 * released only after its operation is counted finished.
 *
 * <p>Seams for lifecycle tests:
 *
 * <ul>
 *   <li>an input queue, which accepts operations before a permit is free;
 *   <li>a slow first wave: the first {@code n} operations take a longer latency and are published
 *       together, before their permits are released, so no observation sees part of the wave;
 *   <li>a completion hold: while a condition holds, completions stay in flight until released,
 *       by the test or a fixed delay after the driver's admission closes (the drain);
 *   <li>a completion gate: a completion that falls due while a condition holds waits, for a
 *       bounded time, until it no longer holds;
 *   <li>a stop probe evaluated when the first stop-path action reaches the driver;
 *   <li>a hook on the step's await loop, which polls {@link #terminalFailure()}, to hold it or
 *       throw from it;
 *   <li>a failure thrown once from {@link #shutdown()}.
 * </ul>
 *
 * <p>Ordinary data operations complete without lifecycle tracking, like the dummy mock driver.
 * Standalone DELETE requests use a real {@link OperationLifecycleTracker}.
 */
final class LatencyStorageDriver<I extends Item, O extends Operation<I>> extends AsyncRunnableBase
				implements StorageDriver<I, O> {

	private static final long GATE_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(5);
	private static final long GATE_POLL_NANOS = TimeUnit.MICROSECONDS.toNanos(200);

	private final int concurrency;
	private final long latencyNanos;
	private final boolean standaloneDelete;
	private final OperationLifecycleTracker<O> lifecycle;
	private final ScheduledExecutorService completer;
	private final AtomicInteger inFlight = new AtomicInteger();
	private final AtomicLong accepted = new AtomicLong();
	private final AtomicLong dispatched = new AtomicLong();
	private final AtomicLong finished = new AtomicLong();
	private final AtomicLong acceptedAtFirstCompletion = new AtomicLong(-1);
	private final AtomicInteger heldTotal = new AtomicInteger();
	private final Object holdLock = new Object();
	private final List<O> held = new ArrayList<>();
	private final List<O> slowWave = new ArrayList<>();
	private final ArrayDeque<O> inputQueue = new ArrayDeque<>();
	private final List<O> queuedUndispatched = new ArrayList<>();
	private final CountDownLatch admissionClosed = new CountDownLatch(1);
	private volatile int inputQueueCapacity;
	private volatile Output<O> resultOutput;
	private volatile long startedNanos;
	private volatile int slowFirstCount;
	private volatile long slowFirstLatencyNanos;
	private volatile BooleanSupplier holdWhen = () -> false;
	private volatile BooleanSupplier gateWhile = () -> false;
	private volatile boolean holdingStopped;
	private volatile long releaseAfterAdmissionCloseNanos = -1;
	private volatile BooleanSupplier stopProbe = () -> true;
	private volatile boolean stopProbeAtAdmissionClose;
	private volatile long admissionClosedNanos;
	private volatile Runnable awaitLoopHook = () -> {};
	private volatile RuntimeException shutdownFailure;

	private LatencyStorageDriver(
					final int concurrency, final long latencyNanos, final boolean standaloneDelete) {
		this.concurrency = concurrency;
		this.latencyNanos = latencyNanos;
		this.standaloneDelete = standaloneDelete;
		this.lifecycle = standaloneDelete
						? new OperationLifecycleTracker<>()
						: OperationLifecycleTracker.disabled();
		this.completer = Executors.newScheduledThreadPool(
						2, Thread.ofPlatform().daemon().name("latency-driver-", 0).factory());
	}

	/** A driver for ordinary data operations. */
	static <I extends Item, O extends Operation<I>> LatencyStorageDriver<I, O> data(
					final int concurrency, final long latencyMillis) {
		return new LatencyStorageDriver<>(concurrency, TimeUnit.MILLISECONDS.toNanos(latencyMillis), false);
	}

	/** A driver for standalone DELETE requests, with operation lifecycle accounting. */
	static <I extends Item, O extends Operation<I>> LatencyStorageDriver<I, O> standaloneDelete(
					final int concurrency, final long latencyMillis) {
		return new LatencyStorageDriver<>(concurrency, TimeUnit.MILLISECONDS.toNanos(latencyMillis), true);
	}

	/** Accepts up to {@code capacity} data operations into an input queue ahead of dispatch. */
	LatencyStorageDriver<I, O> inputQueue(final int capacity) {
		if (standaloneDelete) {
			throw new IllegalStateException("the input queue serves data operations only");
		}
		inputQueueCapacity = capacity;
		return this;
	}

	/** The first {@code count} operations take {@code latencyMillis} and complete as one wave. */
	LatencyStorageDriver<I, O> slowFirstWave(final int count, final long latencyMillis) {
		slowFirstCount = count;
		slowFirstLatencyNanos = TimeUnit.MILLISECONDS.toNanos(latencyMillis);
		return this;
	}

	/** Holds every completion that falls due while {@code condition} is true, until released. */
	LatencyStorageDriver<I, O> holdCompletionsWhen(final BooleanSupplier condition) {
		holdWhen = condition;
		return this;
	}

	/** Delays each completion that falls due while {@code condition} holds, up to the wait bound. */
	LatencyStorageDriver<I, O> gateCompletionsWhile(final BooleanSupplier condition) {
		gateWhile = condition;
		return this;
	}

	/** Releases held completions this long after the driver's admission closes (in the drain). */
	LatencyStorageDriver<I, O> releaseHeldAfterAdmissionClose(final long delayMillis) {
		releaseAfterAdmissionCloseNanos = TimeUnit.MILLISECONDS.toNanos(delayMillis);
		return this;
	}

	/** Evaluated once, when the first stop-path action (admission close) reaches the driver. */
	LatencyStorageDriver<I, O> stopProbe(final BooleanSupplier probe) {
		stopProbe = probe;
		return this;
	}

	/** Runs on the step's await thread at each poll of this driver's terminal failure. */
	LatencyStorageDriver<I, O> awaitLoopHook(final Runnable hook) {
		awaitLoopHook = hook;
		return this;
	}

	/** Thrown once from the driver's shutdown, after the window has closed. */
	LatencyStorageDriver<I, O> failShutdownOnce(final RuntimeException failure) {
		shutdownFailure = failure;
		return this;
	}

	long startedNanos() {
		return startedNanos;
	}

	/** Operations sent: each holds a permit from here until it finishes. */
	long dispatchedCount() {
		return dispatched.get();
	}

	long completedCount() {
		return finished.get();
	}

	/** Operations accepted when the first completion fell due, or -1 before it. */
	long acceptedAtFirstCompletion() {
		return acceptedAtFirstCompletion.get();
	}

	int queuedNow() {
		synchronized (inputQueue) {
			return inputQueue.size();
		}
	}

	int heldNow() {
		synchronized (holdLock) {
			return held.size();
		}
	}

	int heldTotal() {
		return heldTotal.get();
	}

	boolean awaitAdmissionClosed(final long timeout, final TimeUnit unit) throws InterruptedException {
		return admissionClosed.await(timeout, unit);
	}

	long admissionClosedNanos() {
		return admissionClosedNanos;
	}

	/** Whether the stop probe held when the first stop-path action reached this driver. */
	boolean stopProbeHeldAtAdmissionClose() {
		return stopProbeAtAdmissionClose;
	}

	/** Stops holding and completes every held operation on the calling thread. */
	void releaseHeld() {
		final List<O> released;
		synchronized (holdLock) {
			holdingStopped = true;
			released = new ArrayList<>(held);
			held.clear();
		}
		for (final O op : released) {
			publish(op);
			inFlight.decrementAndGet();
		}
		dispatchQueued();
	}

	@Override
	public boolean put(final O op) {
		if (!isStarted()) {
			throwUnchecked(new EOFException());
		}
		if (inputQueueCapacity > 0) {
			synchronized (inputQueue) {
				if (inputQueue.size() >= inputQueueCapacity) {
					return false;
				}
				inputQueue.add(op);
				accepted.incrementAndGet();
			}
			dispatchQueued();
			return true;
		}
		if (!reservePermit()) {
			return false;
		}
		if (!lifecycle.driverQueued(op)) {
			inFlight.decrementAndGet();
			return false;
		}
		if (!lifecycle.explicitlyDispatched(op)) {
			inFlight.decrementAndGet();
			synchronized (queuedUndispatched) {
				queuedUndispatched.add(op);
			}
			return true;
		}
		accepted.incrementAndGet();
		dispatch(op);
		return true;
	}

	@Override
	public int put(final List<O> ops, final int from, final int to) {
		int i = from;
		while (i < to && put(ops.get(i))) {
			i++;
		}
		return i - from;
	}

	@Override
	public int put(final List<O> ops) {
		return put(ops, 0, ops.size());
	}

	private boolean reservePermit() {
		while (true) {
			final int current = inFlight.get();
			if (current >= concurrency) {
				return false;
			}
			if (inFlight.compareAndSet(current, current + 1)) {
				return true;
			}
		}
	}

	/** Moves queued operations to dispatch while permits are free. */
	private void dispatchQueued() {
		while (true) {
			final O next;
			synchronized (inputQueue) {
				if (inputQueue.isEmpty() || !reservePermit()) {
					return;
				}
				next = inputQueue.poll();
			}
			dispatch(next);
		}
	}

	private void dispatch(final O op) {
		final long index = dispatched.getAndIncrement();
		op.reset();
		op.startRequest();
		if (op instanceof DeleteRequestOperation deleteOp) {
			deleteOp.markRequestFirstByteSent();
		}
		op.finishRequest();
		if (index < slowFirstCount) {
			final List<O> wave;
			synchronized (slowWave) {
				slowWave.add(op);
				wave = slowWave.size() == slowFirstCount ? List.copyOf(slowWave) : null;
			}
			if (wave != null) {
				schedule(wave, slowFirstLatencyNanos);
			}
		} else {
			schedule(List.of(op), latencyNanos);
		}
	}

	private void schedule(final List<O> ops, final long delayNanos) {
		final var unused = completer.schedule(() -> completeDue(ops), delayNanos, TimeUnit.NANOSECONDS);
	}

	/** Publishes every due operation that is not held, then releases their permits together. */
	private void completeDue(final List<O> ops) {
		acceptedAtFirstCompletion.compareAndSet(-1, accepted.get());
		final long gateDeadlineNanos = System.nanoTime() + GATE_TIMEOUT_NANOS;
		while (gateWhile.getAsBoolean() && System.nanoTime() - gateDeadlineNanos < 0) {
			LockSupport.parkNanos(GATE_POLL_NANOS);
		}
		int published = 0;
		for (final O op : ops) {
			if (hold(op)) {
				continue;
			}
			publish(op);
			published++;
		}
		inFlight.addAndGet(-published);
		dispatchQueued();
	}

	private boolean hold(final O op) {
		synchronized (holdLock) {
			if (holdingStopped || !holdWhen.getAsBoolean()) {
				return false;
			}
			held.add(op);
			heldTotal.incrementAndGet();
			return true;
		}
	}

	private void publish(final O op) {
		if (op instanceof DeleteRequestOperation deleteOp) {
			deleteOp.markResponseFirstByteReceived();
			op.finishResponse();
			deleteOp.completeDelete(DeleteTransportResult.success(deleteOp.deleteRequest().targets()));
			if (lifecycle.completionStarted(op)) {
				@SuppressWarnings("unchecked")
				final O result = (O) deleteOp.result();
				if (resultOutput.put(result)) {
					lifecycle.terminal(op);
				}
			}
			// Finished by the driver, whether or not the lifecycle accepted the outcome.
			finished.incrementAndGet();
			return;
		}
		op.startResponse();
		if (op instanceof DataOperation<?> dataOp) {
			if (dataOp.type() == OpType.CREATE) {
				try {
					dataOp.countBytesDone(((DataItem) dataOp.item()).size());
				} catch (final IOException e) {
					throw new IllegalStateException(e);
				}
			}
			dataOp.startDataResponse();
		}
		op.finishResponse();
		op.status(Operation.Status.SUCC);
		resultOutput.put(op);
		finished.incrementAndGet();
	}

	@Override
	public void closeAdmission() {
		if (admissionClosed.getCount() == 0) {
			return;
		}
		stopProbeAtAdmissionClose = stopProbe.getAsBoolean();
		admissionClosedNanos = System.nanoTime();
		admissionClosed.countDown();
		final long releaseDelayNanos = releaseAfterAdmissionCloseNanos;
		if (releaseDelayNanos >= 0) {
			final var unused = completer.schedule(this::releaseHeld, releaseDelayNanos, TimeUnit.NANOSECONDS);
		}
	}

	@Override
	public List<O> recoverQueuedOperations() {
		final List<O> recovered = new ArrayList<>();
		synchronized (inputQueue) {
			recovered.addAll(inputQueue);
			inputQueue.clear();
		}
		synchronized (queuedUndispatched) {
			recovered.addAll(queuedUndispatched);
			queuedUndispatched.clear();
		}
		return recovered;
	}

	@Override
	public IntegrityTerminalException terminalFailure() {
		awaitLoopHook.run();
		return null;
	}

	@Override
	public OperationLifecycleTracker<O> operationLifecycle() {
		return lifecycle;
	}

	@Override
	public boolean supportsStandaloneDeleteRequests() {
		return standaloneDelete;
	}

	@Override
	public void operationResultOutput(final Output<O> output) {
		resultOutput = output;
	}

	@Override
	public Input<O> getInput() {
		throw new AssertionError();
	}

	@Override
	public List<I> list(
					final ItemFactory<I> itemFactory,
					final String path,
					final String prefix,
					final int idRadix,
					final I lastPrevItem,
					final int count) {
		return List.of();
	}

	@Override
	public int concurrencyLimit() {
		return concurrency;
	}

	@Override
	public int activeOpCount() {
		return inFlight.get();
	}

	@Override
	public long scheduledOpCount() {
		return accepted.get();
	}

	@Override
	public long completedOpCount() {
		return finished.get();
	}

	@Override
	public boolean isIdle() {
		return inFlight.get() == 0 && queuedNow() == 0;
	}

	@Override
	public void adjustIoBuffers(final long avgTransferSize, final OpType opType) {}

	@Override
	protected void doStart() {
		startedNanos = System.nanoTime();
	}

	@Override
	protected void doShutdown() {
		final RuntimeException failure = shutdownFailure;
		if (failure != null) {
			shutdownFailure = null;
			throw failure;
		}
	}

	@Override
	protected void doClose() {
		completer.shutdownNow();
	}
}
