package com.dell.spt.base.metrics.bucket;

import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.steady.SteadyResult;
import com.dell.spt.base.metrics.steady.SteadyWindow;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;

/**
 * The step's one fence between the aggregates' end cuts and admission closure, and its cut pool.
 *
 * <p>An aggregate's start and end events only queue work here, so no aggregate window ever waits for
 * a bucket cut, and the metrics manager thread never copies a bucket histogram or waits. A closer
 * closes every aggregate first and then calls {@link #await}, which waits for the end passes it
 * queued under one absolute deadline, fixed when the first end pass is queued. Closers may race or
 * repeat, and all see the same outcomes. After a miss the fence is marked: late work checks that
 * mark before and between bucket sets and publishes nothing, and the closer returns without waiting
 * for the pool. Only {@link #close()}, after drain, waits for the pool, and it waits a bounded time.
 *
 * <p>A bucket window is tied to its aggregate's by those events, and is shifted later by each cut's
 * delay. Each record states its own interval and delays.
 */
public final class BucketStepFence implements AutoCloseable {

	/** The whole per-set pass took under 2 s at N = 1000 with four operation types, on a loaded 8-CPU machine. */
	public static final long DEFAULT_DEADLINE_NANOS = TimeUnit.SECONDS.toNanos(5);
	private static final long POOL_WAIT_MILLIS = 1_000;
	private static final int MAX_CUT_THREADS = 4;
	private static final long UNSET = Long.MIN_VALUE;
	private static final ThreadLocal<ByteBuffer> ENCODE_BUFFER = new ThreadLocal<>();

	private final String stepId;
	private final long deadlineBudgetNanos;
	private final int threads;
	private final LongSupplier clock;
	private final List<ContextCuts> contexts = new ArrayList<>();
	private final Object lock = new Object();
	private ExecutorService pool;
	private boolean closed;
	private long deadlineNanos = UNSET;
	private volatile boolean fenced;
	private volatile IntConsumer beforeSetPass = slot -> {};

	public BucketStepFence(final String stepId) {
		this(
						stepId,
						DEFAULT_DEADLINE_NANOS,
						Math.min(MAX_CUT_THREADS, Runtime.getRuntime().availableProcessors()),
						System::nanoTime);
	}

	BucketStepFence(
					final String stepId, final long deadlineBudgetNanos, final int threads, final LongSupplier clock) {
		this.stepId = stepId;
		this.deadlineBudgetNanos = deadlineBudgetNanos;
		this.threads = threads;
		this.clock = clock;
	}

	boolean poolCreated() {
		synchronized (lock) {
			return pool != null;
		}
	}

	/** Tests hold a pass here, to prove what the fence orders. */
	void beforeSetPass(final IntConsumer hook) {
		beforeSetPass = hook;
	}

	/** Adds a metrics context's meters; the result listens to that context's aggregate window. */
	public ContextCuts register(final BucketMeters meters) {
		final ContextCuts context = new ContextCuts(meters);
		synchronized (lock) {
			contexts.add(context);
		}
		return context;
	}

	/** Waits for every context of this step that has closed. */
	public void await() {
		final List<ContextCuts> all;
		synchronized (lock) {
			all = List.copyOf(contexts);
		}
		await(all);
	}

	/**
	 * Waits for the given contexts' passes, until the step's one deadline at the latest. It is safe
	 * to call from any closer, any number of times.
	 */
	public void await(final Collection<ContextCuts> waitedFor) {
		final List<CompletableFuture<BucketSteadyOutcome>> pending = new ArrayList<>();
		for (final ContextCuts context : waitedFor) {
			if (context.endQueued.get() && !context.outcome.isDone()) {
				pending.add(context.outcome);
			}
		}
		if (pending.isEmpty()) {
			return;
		}
		final long deadline;
		synchronized (lock) {
			deadline = deadlineNanos;
		}
		try {
			CompletableFuture.allOf(pending.toArray(new CompletableFuture<?>[0]))
							.get(Math.max(0L, deadline - clock.getAsLong()), TimeUnit.NANOSECONDS);
		} catch (final TimeoutException e) {
			miss("the fence deadline passed");
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			miss("the step was interrupted");
		} catch (final ExecutionException e) {
			// A pass's failure reaches its own outcome as unmeasured, not as an exception.
			miss("a cut pass failed: " + e.getCause());
		}
	}

	private void miss(final String reason) {
		fenced = true;
		final List<ContextCuts> all;
		synchronized (lock) {
			all = List.copyOf(contexts);
			if (pool != null) {
				pool.shutdownNow();
			}
		}
		for (final ContextCuts context : all) {
			if (context.startQueued.get()) {
				context.finish(BucketSteadyOutcome.unmeasured(reason));
			}
		}
	}

	/** Ends the pool at step close, after drain and off the stop path, for a bounded time. */
	@Override
	public void close() {
		final ExecutorService ending;
		synchronized (lock) {
			closed = true;
			ending = pool;
		}
		if (ending == null) {
			return;
		}
		ending.shutdown();
		try {
			if (!ending.awaitTermination(POOL_WAIT_MILLIS, TimeUnit.MILLISECONDS)) {
				ending.shutdownNow();
				Loggers.MSG.warn("{}: a bucket metrics cut was still running at step close", stepId);
			}
		} catch (final InterruptedException e) {
			ending.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}

	private void execute(final Runnable task) {
		final ExecutorService executor;
		synchronized (lock) {
			if (closed || fenced) {
				throw new RejectedExecutionException("the bucket metrics cut pool has ended");
			}
			if (pool == null) {
				final AtomicInteger index = new AtomicInteger();
				pool = Executors.newFixedThreadPool(threads, runnable -> {
					final Thread thread = new Thread(runnable, "bucket-cut-" + stepId + "-" + index.getAndIncrement());
					thread.setDaemon(true);
					return thread;
				});
			}
			executor = pool;
		}
		executor.execute(task);
	}

	private void markDeadline() {
		synchronized (lock) {
			if (deadlineNanos == UNSET) {
				deadlineNanos = clock.getAsLong() + deadlineBudgetNanos;
			}
		}
	}

	/** Whether the step's one deadline has passed, whether or not any closer is waiting for it. */
	private boolean deadlinePassed() {
		synchronized (lock) {
			return deadlineNanos != UNSET && clock.getAsLong() - deadlineNanos > 0;
		}
	}

	private static ByteBuffer encodeBuffer(final int capacity) {
		ByteBuffer buffer = ENCODE_BUFFER.get();
		if (buffer == null || buffer.capacity() < capacity) {
			buffer = ByteBuffer.allocate(capacity);
			ENCODE_BUFFER.set(buffer);
		}
		return buffer;
	}

	/** One metrics context's bucket window. */
	public final class ContextCuts implements SteadyWindow.Listener {

		private final BucketMeters meters;
		private final CompletableFuture<Void> startDone = new CompletableFuture<>();
		private final CompletableFuture<BucketSteadyOutcome> outcome = new CompletableFuture<>();
		private final AtomicBoolean startQueued = new AtomicBoolean();
		private final AtomicBoolean endQueued = new AtomicBoolean();
		private final AtomicReferenceArray<BucketCut> startCuts;
		private volatile long aggregateStartNanos;
		private volatile long aggregateEndNanos;

		private ContextCuts(final BucketMeters meters) {
			this.meters = meters;
			startCuts = new AtomicReferenceArray<>(meters.sets().length);
		}

		public BucketMeters meters() {
			return meters;
		}

		boolean startPassDone() {
			return startDone.isDone();
		}

		/** The window's outcome, or null while it is still open or its passes are running. */
		public BucketSteadyOutcome outcome() {
			return outcome.getNow(null);
		}

		@Override
		public void onWindowStart(final long startNanos) {
			aggregateStartNanos = startNanos;
			startQueued.set(true);
			runPass(this::startPass, () -> startDone.complete(null), startDone::completeExceptionally);
		}

		@Override
		public void onWindowClose(final long endNanos, final boolean started, final SteadyResult result) {
			aggregateEndNanos = endNanos;
			if (!started) {
				finish(BucketSteadyOutcome.notReached());
				return;
			}
			markDeadline();
			endQueued.set(true);
			// An end never precedes its start: it is queued when the start pass ends.
			startDone.whenComplete((ignored, failure) -> {
				if (failure != null) {
					finish(BucketSteadyOutcome.unmeasured("the start cut failed: " + failure));
					return;
				}
				final BucketTerminalRecordsBySlot records = new BucketTerminalRecordsBySlot(meters.sets().length);
				runPass(
								slot -> endPass(slot, records),
								() -> {
									if (fenced || deadlinePassed()) {
										finish(BucketSteadyOutcome.unmeasured("the fence deadline passed"));
									} else {
										finish(BucketSteadyOutcome.measured(records.inOrder()));
									}
								},
								error -> finish(BucketSteadyOutcome.unmeasured("the end cut failed: " + error)));
			});
		}

		private void finish(final BucketSteadyOutcome result) {
			if (outcome.complete(result) && result.kind() != BucketSteadyOutcome.Kind.MEASURED) {
				if (result.kind() == BucketSteadyOutcome.Kind.UNMEASURED) {
					Loggers.MSG.warn(
									"{}: {} bucket steady rows are unmeasured: {}",
									stepId,
									meters.opType(),
									result.reason());
				}
			}
			for (int slot = 0; slot < startCuts.length(); slot++) {
				startCuts.set(slot, null);
			}
		}

		/** Runs one pass over every set, each cut thread taking whole sets. */
		private void runPass(
						final IntConsumer perSet,
						final Runnable done,
						final java.util.function.Consumer<Throwable> failed) {
			final int sets = meters.sets().length;
			final AtomicInteger cursor = new AtomicInteger();
			final AtomicInteger finished = new AtomicInteger();
			final AtomicBoolean failure = new AtomicBoolean();
			final Runnable worker = () -> {
				try {
					int slot;
					while ((slot = cursor.getAndIncrement()) < sets) {
						if (fenced) {
							throw new IllegalStateException("the fence deadline passed");
						}
						beforeSetPass.accept(slot);
						perSet.accept(slot);
						if (finished.incrementAndGet() == sets && !failure.get()) {
							done.run();
						}
					}
				} catch (final Throwable e) {
					if (failure.compareAndSet(false, true)) {
						failed.accept(e);
					}
				}
			};
			try {
				for (int i = 0; i < Math.min(threads, sets); i++) {
					execute(worker);
				}
			} catch (final RejectedExecutionException e) {
				if (failure.compareAndSet(false, true)) {
					failed.accept(e);
				}
			}
		}

		private void startPass(final int slot) {
			startCuts.set(slot, meters.sets()[slot].cut(clock));
		}

		private void endPass(final int slot, final BucketTerminalRecordsBySlot records) {
			final long passStart = clock.getAsLong();
			final BucketMeterSet set = meters.sets()[slot];
			final BucketCut start = startCuts.getAndSet(slot, null);
			final BucketCut end = set.cut(clock);
			final BucketTerminalRecord values = set.windowRecord(
							meters.opType(),
							meters.kindOf(slot),
							meters.nameOf(slot),
							start,
							end,
							capacity -> encodeBuffer(capacity));
			if (fenced) {
				throw new IllegalStateException("the fence deadline passed");
			}
			records.set(slot, new BucketSteadyRecord(
							values,
							Math.max(0L, end.timeNanos - start.timeNanos),
							Math.max(0L, start.timeNanos - aggregateStartNanos),
							Math.max(0L, end.timeNanos - aggregateEndNanos),
							clock.getAsLong() - passStart));
		}
	}

	private static final class BucketTerminalRecordsBySlot {
		private final AtomicReferenceArray<BucketSteadyRecord> records;

		BucketTerminalRecordsBySlot(final int sets) {
			records = new AtomicReferenceArray<>(sets);
		}

		void set(final int slot, final BucketSteadyRecord record) {
			records.set(slot, record);
		}

		List<BucketSteadyRecord> inOrder() {
			final List<BucketSteadyRecord> ordered = new ArrayList<>(records.length());
			for (int slot = 0; slot < records.length(); slot++) {
				ordered.add(records.get(slot));
			}
			return ordered;
		}
	}
}
