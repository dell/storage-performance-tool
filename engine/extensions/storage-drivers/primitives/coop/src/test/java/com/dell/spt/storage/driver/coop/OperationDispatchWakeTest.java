package com.dell.spt.storage.driver.coop;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.concurrent.ThreadTaskExecutor;
import com.dell.spt.base.concurrent.VirtualThreadExecutor;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.github.akurilov.commons.system.SizeInBytes;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * How the dispatch task waits and wakes when multipart uploads are deferred, with a real task,
 * real queues and a mocked driver. A producer's signal must wake the task whatever lock the task
 * contends between its checks and its wait: an {@code ArrayBlockingQueue} lock the task waits for
 * consumes the thread's park permit. A test holds a queue's real lock through
 * {@code removeIf}, which calls its predicate under that lock, and acts as the producer once the
 * task is blocked, read from {@link LockSupport#getBlocker(Thread)}. The task also waits, instead
 * of spinning, while queued input can't be taken because direct dispatch has no capacity, and
 * serves deferred uploads before new ones when it wakes.
 */
@SuppressWarnings("unchecked")
@Timeout(30)
final class OperationDispatchWakeTest {

	private static final int BATCH_SIZE = 4;
	private static final long WAIT_MILLIS = 3_000;

	/** A real queue that runs a one-shot action on the draining thread after a drain. */
	private static final class HookedQueue<E> extends ArrayBlockingQueue<E> {
		private static final long serialVersionUID = 1L;
		private transient volatile Runnable afterDrain;
		private final AtomicInteger drains = new AtomicInteger();

		HookedQueue(final int capacity) {
			super(capacity);
		}

		@Override
		public int drainTo(final Collection<? super E> c, final int maxElements) {
			final int drained = super.drainTo(c, maxElements);
			drains.incrementAndGet();
			final var action = afterDrain;
			if (action != null) {
				afterDrain = null;
				action.run();
			}
			return drained;
		}
	}

	/** Holds a non-empty queue's real lock on its own thread until released. */
	private static final class LockHolder {
		private final CountDownLatch held = new CountDownLatch(1);
		private final CountDownLatch release = new CountDownLatch(1);
		private final Thread thread;

		/** {@code removeHeld}: the held element leaves the queue when the lock is released. */
		LockHolder(final BlockingQueue<?> queue, final boolean removeHeld) throws InterruptedException {
			thread = Thread.ofPlatform().name("queue-lock-holder").start(() -> queue.removeIf(e -> {
				if (held.getCount() == 0) {
					return false;
				}
				held.countDown();
				try {
					release.await();
				} catch (final InterruptedException ex) {
					Thread.currentThread().interrupt();
				}
				return removeHeld;
			}));
			assertTrue(held.await(5, TimeUnit.SECONDS), "the holder took the queue's lock");
		}

		void release() throws InterruptedException {
			release.countDown();
			thread.join(5_000);
		}
	}

	private VirtualThreadExecutor executor;
	private CoopStorageDriverBase<Item, Operation<Item>> driver;
	private HookedQueue<Operation<Item>> inOpQueue;
	private HookedQueue<Operation<Item>> childOpQueue;
	private OperationDispatchTask<Item, Operation<Item>> task;
	private Thread dispatcher;
	private final List<LockHolder> holders = java.util.Collections.synchronizedList(new ArrayList<>());
	private final List<Operation<Item>> submitted = java.util.Collections.synchronizedList(new ArrayList<>());
	/** Transport permits: bounds {@code dispatchAttemptLimit} and gives {@code hasAvailableDispatchCapacity}. */
	private final AtomicInteger permits = new AtomicInteger(BATCH_SIZE);
	private final AtomicInteger mpuPermits = new AtomicInteger();
	private final AtomicInteger mpuPermitTries = new AtomicInteger();
	private final AtomicInteger capacityChecks = new AtomicInteger();
	private final AtomicInteger capacityChecksWithChildQueued = new AtomicInteger();
	private volatile Set<Operation<Item>> mpuInits = Set.of();

	@BeforeEach
	void setUp() {
		executor = new VirtualThreadExecutor();
		driver = mock(CoopStorageDriverBase.class);
		inOpQueue = new HookedQueue<>(16);
		childOpQueue = new HookedQueue<>(16);
		when(driver.dispatchAttemptLimit(anyInt()))
						.thenAnswer(inv -> Math.max(0, Math.min((int) inv.getArgument(0), permits.get())));
		when(driver.hasAvailableDispatchCapacity()).thenAnswer(inv -> {
			capacityChecks.incrementAndGet();
			if (!childOpQueue.isEmpty()) {
				capacityChecksWithChildQueued.incrementAndGet();
			}
			return permits.get() > 0;
		});
		when(driver.isMpuInit(any(Operation.class))).thenAnswer(inv -> mpuInits.contains(inv.getArgument(0)));
		when(driver.tryAcquireMpuObjectPermit(any(Operation.class))).thenAnswer(inv -> {
			mpuPermitTries.incrementAndGet();
			if (!mpuInits.contains(inv.getArgument(0))) {
				return true;
			}
			while (true) {
				final int free = mpuPermits.get();
				if (free <= 0) {
					return false;
				}
				if (mpuPermits.compareAndSet(free, free - 1)) {
					return true;
				}
			}
		});
		when(driver.submit(any(Operation.class))).thenAnswer(inv -> {
			submitted.add(inv.getArgument(0));
			return true;
		});
		when(driver.submit(anyList(), anyInt(), anyInt())).thenAnswer(inv -> {
			final List<Operation<Item>> ops = inv.getArgument(0);
			final int from = inv.getArgument(1);
			final int to = inv.getArgument(2);
			for (int i = from; i < to; i++) {
				submitted.add(ops.get(i));
			}
			return to - from;
		});
	}

	@AfterEach
	void tearDown() throws Exception {
		for (final var holder : List.copyOf(holders)) {
			holder.release();
		}
		if (dispatcher != null) {
			dispatcher.interrupt();
			dispatcher.join(5_000);
		}
		if (task != null) {
			task.close();
		}
		executor.close();
	}

	private void newTask(final ThreadTaskExecutor taskExecutor, final int deferredCapacity) {
		final var dispatchLock = new ReentrantLock();
		task = new OperationDispatchTask<>(
						taskExecutor, driver, inOpQueue, childOpQueue, "wake-test", BATCH_SIZE,
						dispatchLock, dispatchLock.newCondition(), deferredCapacity);
	}

	private Operation<Item> op(final String name) {
		final Operation<Item> op = mock(Operation.class, name);
		return op;
	}

	private Operation<Item> mpuInit(final String name) {
		final var op = op(name);
		final var inits = new java.util.HashSet<>(mpuInits);
		inits.add(op);
		mpuInits = Set.copyOf(inits);
		return op;
	}

	/** Runs one iteration on the test thread with transport capacity, so every MPU init is deferred. */
	private void defer(final Operation<Item>... inits) throws Exception {
		final int saved = permits.getAndSet(BATCH_SIZE);
		for (final var init : inits) {
			inOpQueue.add(init);
		}
		task.doWork();
		assertTrue(inOpQueue.isEmpty(), "every MPU init was deferred");
		assertTrue(submitted.isEmpty(), "no MPU init had a permit");
		permits.set(saved);
		mpuPermitTries.set(0);
		capacityChecks.set(0);
		capacityChecksWithChildQueued.set(0);
	}

	/** The task's work loop on a platform thread of the test's, as {@code TaskBase.run()} runs it. */
	private void startDispatcher() {
		dispatcher = Thread.ofPlatform().name("dispatcher").start(() -> {
			try {
				while (!Thread.currentThread().isInterrupted()) {
					task.doWork();
				}
			} catch (final InterruptedException e) {
				// stopped
			} catch (final Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

	/** Waits until the dispatcher blocks: parked in its own wait, or queued on a lock. */
	private Object awaitBlocked(final Thread thread) {
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MILLIS);
		while (System.nanoTime() < deadline) {
			final var blocker = LockSupport.getBlocker(thread);
			if (blocker != null) {
				return blocker;
			}
			LockSupport.parkNanos(100_000);
		}
		return fail("the dispatcher never blocked: it is spinning");
	}

	private void awaitParkedInWait(final Thread thread, final String why) {
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MILLIS);
		while (System.nanoTime() < deadline) {
			if (LockSupport.getBlocker(thread) == task) {
				return;
			}
			LockSupport.parkNanos(100_000);
		}
		fail(why + "; blocker " + LockSupport.getBlocker(thread) + ", state " + thread.getState());
	}

	/** Waits until the dispatcher queues on a lock a holder has, past its child-queue check. */
	private void awaitContended(final Thread thread) {
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MILLIS);
		Object blocker = null;
		while (System.nanoTime() < deadline) {
			blocker = LockSupport.getBlocker(thread);
			// A queue's ReentrantLock, not the latch a hook waits on while its holder starts.
			if (blocker instanceof AbstractQueuedSynchronizer && blocker.getClass().getEnclosingClass() == ReentrantLock.class) {
				return;
			}
			LockSupport.parkNanos(100_000);
		}
		fail("the dispatcher never contended the held queue lock; blocker " + blocker);
	}

	/** A completion on another thread: changes state, then signals the task. */
	private void produce(final Runnable change) throws InterruptedException {
		final var producer = Thread.ofPlatform().name("completion").start(() -> {
			change.run();
			task.unpark();
		});
		producer.join(5_000);
	}

	private void assertSubmittedWithin(final Operation<Item> op, final String why) {
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MILLIS);
		while (System.nanoTime() < deadline) {
			if (submitted.contains(op)) {
				return;
			}
			LockSupport.parkNanos(100_000);
		}
		final var thread = dispatcher;
		fail(why + "; dispatcher " + (thread == null ? "-" : thread.getState() + ", blocker " + LockSupport.getBlocker(thread))
						+ ", child queue " + childOpQueue.size() + ", submitted " + submitted);
	}

	private LockHolder hold(final BlockingQueue<Operation<Item>> queue, final boolean removeHeld)
					throws InterruptedException {
		final var holder = new LockHolder(queue, removeHeld);
		holders.add(holder);
		return holder;
	}

	// --- Lost wake-ups (multipart-dispatcher-silent-stall 01) ---------------------------------------

	/**
	 * Deferred queue full, no MPU permit: the last in-flight upload's next part is queued and signalled
	 * after the task read the child queue empty, while the generator, blocked on the full input queue,
	 * holds that queue's lock.
	 */
	@Test
	void childQueuedWhileInputLockHeldWakesTaskWithDeferredQueueFull() throws Exception {
		childSignalWithDeferredQueueFull(true);
	}

	@Test
	void childQueuedWakesTaskWithDeferredQueueFull() throws Exception {
		childSignalWithDeferredQueueFull(false);
	}

	private void childSignalWithDeferredQueueFull(final boolean inputLockHeld) throws Exception {
		newTask(executor, 2);
		defer(mpuInit("mpu1"), mpuInit("mpu2"));
		inOpQueue.add(mpuInit("mpu3")); // the generator's next init waits in the input queue
		final var child = op("part");
		final var holder = inputLockHeld ? hold(inOpQueue, false) : null;
		startDispatcher();
		if (inputLockHeld) {
			awaitBlocked(dispatcher);
		} else {
			awaitParkedInWait(dispatcher, "the task waits with the deferred queue full");
		}
		produce(() -> childOpQueue.offer(child));
		if (holder != null) {
			holder.release();
		}
		assertSubmittedWithin(child, "the queued part is sent");
	}

	/**
	 * Deferred queue partly full, no MPU permit, input queue empty: a generator offer (here, the
	 * holder's) holds the input queue's lock as the task checks it, after the task read the child
	 * queue empty and before a completion queues a part.
	 */
	@Test
	void childQueuedWhileInputLockHeldWakesTaskWithDeferredQueuePartlyFull() throws Exception {
		childSignalWithDeferredQueuePartlyFull(true);
	}

	@Test
	void childQueuedWakesTaskWithDeferredQueuePartlyFull() throws Exception {
		childSignalWithDeferredQueuePartlyFull(false);
	}

	private void childSignalWithDeferredQueuePartlyFull(final boolean inputLockHeld) throws Exception {
		newTask(executor, 4);
		defer(mpuInit("mpu1"));
		final var child = op("part");
		final var holder = new AtomicReference<LockHolder>();
		if (inputLockHeld) {
			inOpQueue.afterDrain = () -> {
				inOpQueue.offer(op("seed"));
				try {
					holder.set(hold(inOpQueue, true));
				} catch (final InterruptedException e) {
					throw new RuntimeException(e);
				}
			};
			startDispatcher();
			awaitContended(dispatcher);
		} else {
			startDispatcher();
			awaitParkedInWait(dispatcher, "the task waits with the deferred queue partly full");
		}
		produce(() -> childOpQueue.offer(child));
		if (holder.get() != null) {
			holder.get().release();
		}
		assertSubmittedWithin(child, "the queued part is sent");
	}

	/**
	 * Nothing deferred, input queue empty, no later generator put (a count limit or a finite input):
	 * the input queue's lock is held as the task checks it, and a completion queues the next part.
	 */
	@Test
	void childQueuedWhileInputLockHeldWakesTaskWithNoLaterInput() throws Exception {
		childSignalWithNoLaterInput(true);
	}

	@Test
	void childQueuedWakesTaskWithNoLaterInput() throws Exception {
		childSignalWithNoLaterInput(false);
	}

	private void childSignalWithNoLaterInput(final boolean inputLockHeld) throws Exception {
		newTask(executor, 16);
		final var child = op("part");
		final var holder = new AtomicReference<LockHolder>();
		if (inputLockHeld) {
			inOpQueue.afterDrain = () -> {
				inOpQueue.offer(op("seed"));
				try {
					holder.set(hold(inOpQueue, true));
				} catch (final InterruptedException e) {
					throw new RuntimeException(e);
				}
			};
			startDispatcher();
			awaitContended(dispatcher);
		} else {
			startDispatcher();
			awaitParkedInWait(dispatcher, "the task waits with nothing queued");
		}
		produce(() -> childOpQueue.offer(child));
		if (holder.get() != null) {
			holder.get().release();
		}
		assertSubmittedWithin(child, "the queued part is sent");
	}

	/**
	 * A split READ's final recycle cycle under direct dispatch: every permit is in flight and the
	 * generator puts nothing more. The completion of the next-to-last range returns its permit and
	 * queues the last range while the input queue's lock is held.
	 */
	@Test
	void lastRangeOfSplitReadWhileInputLockHeldIsSent() throws Exception {
		lastRangeOfSplitRead(true);
	}

	@Test
	void lastRangeOfSplitReadIsSent() throws Exception {
		lastRangeOfSplitRead(false);
	}

	private void lastRangeOfSplitRead(final boolean inputLockHeld) throws Exception {
		newTask(executor, 16);
		when(driver.directDispatchEnabled()).thenReturn(true);
		permits.set(0);
		final var dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
		final var item = new DataItemImpl("split-read", 0, 3 * 1024);
		item.dataInput(dataInput);
		final var read = new CompositeDataOperationImpl<DataItem>(0, OpType.READ, item, null, "/bucket", null, null, 0, 1024);
		final var ranges = read.subOperations();
		assertEquals(3, ranges.size());
		final var lastRange = (Operation<Item>) (Operation<?>) ranges.get(ranges.size() - 1);
		LockHolder holder = null;
		if (inputLockHeld) {
			inOpQueue.add(op("seed"));
			holder = hold(inOpQueue, true);
		}
		startDispatcher();
		if (inputLockHeld) {
			awaitContended(dispatcher);
		} else {
			awaitParkedInWait(dispatcher, "the task waits with every permit in flight");
		}
		produce(() -> {
			permits.incrementAndGet();
			childOpQueue.offer(lastRange);
		});
		if (holder != null) {
			holder.release();
		}
		assertSubmittedWithin(lastRange, "the last range is sent");
	}

	/** The child queue's lock is held while a plain operation is queued and signalled. */
	@Test
	void inputQueuedWhileChildLockHeldIsSent() throws Exception {
		newTask(executor, 16);
		childOpQueue.add(op("seed-part"));
		final var holder = hold(childOpQueue, true);
		final var plain = op("plain");
		startDispatcher();
		awaitContended(dispatcher);
		produce(() -> inOpQueue.offer(plain));
		holder.release();
		assertSubmittedWithin(plain, "the queued operation is sent");
	}

	// --- Spin with deferred uploads (dispatch-busy-spin-deferred-mpu 01) ----------------------------

	/** One deferred init, no MPU permit, a plain operation queued, and direct dispatch with no capacity. */
	private Operation<Item> spinState(final int deferredCapacity) throws Exception {
		newTask(executor, deferredCapacity);
		when(driver.directDispatchEnabled()).thenReturn(true);
		defer(mpuInit("mpu1"));
		permits.set(0);
		final var plain = op("plain");
		inOpQueue.add(plain);
		return plain;
	}

	@Test
	void taskWaitsWhileDirectDispatchHasNoCapacity() throws Exception {
		spinState(4);
		startDispatcher();
		awaitParkedInWait(dispatcher, "the task waits instead of spinning");
		final int iterations = mpuPermitTries.get();
		Thread.sleep(200); // a window for a spinning task to keep running
		assertEquals(iterations, mpuPermitTries.get(), "the task stays in its wait");
		assertSame(task, LockSupport.getBlocker(dispatcher));
	}

	@Test
	void plainOperationIsSentWhenCapacityReturns() throws Exception {
		final var plain = spinState(4);
		startDispatcher();
		awaitParkedInWait(dispatcher, "the task waits instead of spinning");
		produce(() -> permits.set(1));
		assertSubmittedWithin(plain, "the plain operation is sent once a permit returns");
	}

	@Test
	void deferredInitsGoBeforeNewOnesAfterTheWait() throws Exception {
		newTask(executor, 4);
		when(driver.directDispatchEnabled()).thenReturn(true);
		final var mpu1 = mpuInit("mpu1");
		final var mpu2 = mpuInit("mpu2");
		defer(mpu1, mpu2);
		permits.set(0);
		final var mpu3 = mpuInit("mpu3");
		final var mpu4 = mpuInit("mpu4");
		inOpQueue.add(mpu3);
		inOpQueue.add(mpu4);
		startDispatcher();
		awaitParkedInWait(dispatcher, "the task waits with no capacity and no MPU permit");
		produce(() -> {
			mpuPermits.set(2);
			permits.set(BATCH_SIZE);
		});
		assertSubmittedWithin(mpu2, "the deferred inits are sent");
		synchronized (submitted) {
			assertEquals(List.of(mpu1, mpu2), submitted, "the oldest deferred inits take the freed permits");
		}
	}

	/**
	 * A driver whose capacity is more than the permits (the NIC set's admission room) has none: the
	 * task checks it once, waits, and wakes when the capacity returns.
	 */
	@Test
	void taskWaitsOnceWhenDriverCapacityBeyondPermitsIsExhausted() throws Exception {
		final var plain = spinState(4);
		startDispatcher();
		awaitParkedInWait(dispatcher, "the task waits while the driver has no capacity");
		assertEquals(1, capacityChecks.get(), "the capacity check, which may schedule a recheck, runs once");
		produce(() -> permits.set(1));
		assertSubmittedWithin(plain, "the plain operation is sent once capacity returns");
	}

	/** A part queued after the task drained the child queue is taken without asking for capacity first. */
	@Test
	void childQueuedOnEntryIsTakenBeforeTheCapacityCheck() throws Exception {
		spinState(4);
		final var child = op("part");
		childOpQueue.afterDrain = () -> childOpQueue.offer(child);
		startDispatcher();
		awaitParkedInWait(dispatcher, "the task waits for capacity for the part");
		assertEquals(0, capacityChecksWithChildQueued.get(), "no capacity check while a part is queued");
		produce(() -> permits.set(1));
		assertSubmittedWithin(child, "the part is sent once a permit returns");
	}

	/** The child queue's lock is held while a completion returns a permit and signals the task. */
	@Test
	void permitReturnedWhileChildLockHeldSendsPlainOperation() throws Exception {
		final var plain = spinState(4);
		final var holder = new AtomicReference<LockHolder>();
		childOpQueue.afterDrain = () -> {
			childOpQueue.offer(op("seed-part"));
			try {
				holder.set(hold(childOpQueue, true));
			} catch (final InterruptedException e) {
				throw new RuntimeException(e);
			}
		};
		startDispatcher();
		awaitContended(dispatcher);
		produce(() -> permits.set(1));
		holder.get().release();
		assertSubmittedWithin(plain, "the plain operation is sent");
	}

	@Test
	void withoutDirectDispatchTheSameStateSendsPlainOperation() throws Exception {
		final var plain = spinState(4);
		when(driver.directDispatchEnabled()).thenReturn(false);
		permits.set(1); // the dispatcher itself needs a permit to send
		startDispatcher();
		assertSubmittedWithin(plain, "the dispatcher takes the plain operation itself");
	}

	/**
	 * Stop ends the new wait promptly. A signal sent after the stop is for the earlier cycle: the
	 * restarted task drains once for its fresh work, once more to find nothing, and waits. A signal
	 * kept from the earlier cycle would make that wait return and the task drain twice more.
	 */
	@Test
	void stopEndsTheWaitAndRestartStartsWithNoSignal() throws Exception {
		final var gate = new AtomicReference<>(new CountDownLatch(0));
		final var gated = new ThreadTaskExecutor(r -> {
			final var open = gate.get();
			return Thread.ofPlatform().unstarted(() -> {
				try {
					open.await();
				} catch (final InterruptedException e) {
					return;
				}
				r.run();
			});
		}) {};
		try {
			newTask(gated, 4);
			when(driver.directDispatchEnabled()).thenReturn(true);
			defer(mpuInit("mpu1"));
			permits.set(0);
			inOpQueue.add(op("plain"));
			task.start();
			// defer() ran an iteration on this thread, which recorded it as the dispatch thread.
			final var first = awaitDispatchThreadOtherThan(Thread.currentThread());
			awaitParkedInWait(first, "the task waits instead of spinning");

			task.stop();
			assertTrue(task.await(5, TimeUnit.SECONDS), "stop ends the wait promptly");

			task.unpark(); // a completion of the earlier cycle
			inOpQueue.clear();
			permits.set(BATCH_SIZE);
			final var restartGate = new CountDownLatch(1);
			gate.set(restartGate);
			task.restart();
			final var fresh = op("fresh");
			inOpQueue.add(fresh);
			final int drainsBefore = inOpQueue.drains.get();
			restartGate.countDown();
			final var second = awaitDispatchThreadOtherThan(first);
			assertSubmittedWithin(fresh, "the restarted task sends fresh work");
			awaitParkedInWait(second, "the restarted task waits with nothing queued");
			assertEquals(2, inOpQueue.drains.get() - drainsBefore, "the restarted task iterates only for its own work");
		} finally {
			task.stop();
			task.await(5, TimeUnit.SECONDS);
			task.close();
			task = null;
			gated.close();
		}
	}

	private Thread awaitDispatchThreadOtherThan(final Thread earlier) throws Exception {
		final var threadField = OperationDispatchTask.class.getDeclaredField("dispatchThread");
		threadField.setAccessible(true);
		final long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(WAIT_MILLIS);
		while (System.nanoTime() < deadline) {
			final var current = (Thread) threadField.get(task);
			if (current != null && current != earlier) {
				return current;
			}
			LockSupport.parkNanos(100_000);
		}
		return fail("the task's thread never ran doWork()");
	}
}
