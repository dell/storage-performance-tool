package com.dell.spt.base.load.lifecycle;

import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/**
 * Holds one terminal transition of a tracker between its custody removal and its in-flight
 * decrement, as a completion thread descheduled there would, until closed.
 */
public final class StalledTerminalTransition implements AutoCloseable {

	private final CountDownLatch removed = new CountDownLatch(1);
	private final CountDownLatch release = new CountDownLatch(1);
	private final FutureTask<Boolean> completion;

	private <O extends Operation<? extends Item>> StalledTerminalTransition(
					final OperationLifecycleTracker<O> tracker, final O op) throws ReflectiveOperationException {
		final var field = OperationLifecycleTracker.class.getDeclaredField("outstanding");
		field.setAccessible(true);
		@SuppressWarnings("unchecked")
		final Set<Object> outstanding = (Set<Object>) field.get(tracker);
		field.set(tracker, new AbstractSet<>() {
			@Override
			public Iterator<Object> iterator() {
				return outstanding.iterator();
			}

			@Override
			public int size() {
				return outstanding.size();
			}

			@Override
			public boolean contains(final Object key) {
				return outstanding.contains(key);
			}

			@Override
			public boolean add(final Object key) {
				return outstanding.add(key);
			}

			@Override
			public boolean remove(final Object key) {
				final boolean result = outstanding.remove(key);
				removed.countDown();
				try {
					if (!release.await(5, TimeUnit.SECONDS)) {
						throw new AssertionError("the stalled terminal transition was never released");
					}
				} catch (final InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new AssertionError(e);
				}
				return result;
			}
		});
		completion = new FutureTask<>(() -> tracker.terminal(op));
	}

	/** Starts {@code tracker.terminal(op)} on another thread and returns once it removed {@code op}. */
	public static <O extends Operation<? extends Item>> StalledTerminalTransition stall(
					final OperationLifecycleTracker<O> tracker, final O op) throws Exception {
		final var stalled = new StalledTerminalTransition(tracker, op);
		Thread.ofVirtual().start(stalled.completion);
		assertTrue(stalled.removed.await(5, TimeUnit.SECONDS), "the terminal transition never reached its removal");
		return stalled;
	}

	/** Lets the transition finish and checks that it was accepted. */
	@Override
	public void close() throws Exception {
		release.countDown();
		assertTrue(completion.get(5, TimeUnit.SECONDS));
	}
}
