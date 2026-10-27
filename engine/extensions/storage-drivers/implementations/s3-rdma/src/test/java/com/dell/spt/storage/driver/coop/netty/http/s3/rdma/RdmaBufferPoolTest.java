package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verifyNoInteractions;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

class RdmaBufferPoolTest {

	private static final int MIB = 1 << 20;
	private static final long UNBOUNDED = Long.MAX_VALUE;

	private FakeRdmaTransport transport;

	@BeforeEach
	void setUp() {
		transport = new FakeRdmaTransport(new RdmaConfig(true, 0, false, "auto", "", "WARN"));
		transport.init("http://10.0.0.1:9020", "key", "secret");
	}

	@Test
	void sizeClassesArePowersOfTwoFromOneMibThenExact() {
		assertEquals(MIB, RdmaBufferPool.capacityFor(1));
		assertEquals(MIB, RdmaBufferPool.capacityFor(MIB));
		assertEquals(2 * MIB, RdmaBufferPool.capacityFor(MIB + 1));
		assertEquals(4 * MIB, RdmaBufferPool.capacityFor(3 * MIB));
		assertEquals(4 * MIB, RdmaBufferPool.capacityFor(4 * MIB));
		assertEquals(1 << 30, RdmaBufferPool.capacityFor(1 << 30));
	}

	@Test
	void transfersAboveTheLargestClassAreNotPooled() {
		// Also bounds stale-content invalidation, which runs only on pool buffers, to 1 GiB.
		final var pool = new RdmaBufferPool(transport, 2, UNBOUNDED);

		assertNull(pool.acquire(RdmaBufferPool.MAX_CLASS_BYTES + 1));

		assertEquals(1, pool.unpooled.sum());
		assertEquals(0, transport.getRegisterCount());
	}

	@Test
	void reusedBufferIsCleared() {
		final var pool = new RdmaBufferPool(transport, 1, UNBOUNDED);
		final var first = pool.acquire(MIB);
		first.buffer().limit(4096).position(7);
		pool.release(first);

		final var reused = pool.acquire(2 * 4096);

		assertSame(first, reused);
		assertEquals(0, reused.buffer().position());
		assertEquals(MIB, reused.buffer().limit(), "a smaller earlier limit must not constrain the next request");
	}

	@Test
	void newBufferEvictsIdleBuffersOfOtherClassesToStayWithinTheBudget() {
		final var pool = new RdmaBufferPool(transport, 4, 4L * MIB);
		final var small = pool.acquire(MIB);
		final var medium = pool.acquire(2 * MIB);
		pool.release(small);
		pool.release(medium);

		final var large = pool.acquire(3 * MIB);

		assertNotNull(large);
		assertEquals(4 * MIB, large.capacity());
		assertEquals(2, pool.evicted.sum());
		assertTrue(transport.wasDeregistered(small.mrHandle()));
		assertTrue(transport.wasDeregistered(medium.mrHandle()));
		assertEquals(4L * MIB, pool.liveBytes());
	}

	@Test
	void bufferThatCannotFitIsNotPooledAndEvictsNothing() {
		final var pool = new RdmaBufferPool(transport, 4, 4L * MIB);
		final var held = pool.acquire(2 * MIB);
		final var idle = pool.acquire(MIB);
		pool.release(idle);

		assertNull(pool.acquire(4 * MIB), "2 MiB leased + 4 MiB exceeds the 4 MiB budget even after eviction");

		assertNotNull(held);
		assertEquals(1, pool.exhausted.sum());
		assertEquals(0, pool.evicted.sum());
		assertEquals(1, pool.idleBuffers());
	}

	@Test
	void liveBytesNeverExceedTheBudget() {
		final long budget = 6L * MIB;
		final var pool = new RdmaBufferPool(transport, 3, budget);
		final var random = new Random(20261007);
		final List<RdmaBufferPool.PooledBuffer> leased = new ArrayList<>();
		for (int step = 0; step < 2_000; step++) {
			if (!leased.isEmpty() && random.nextBoolean()) {
				final var returned = leased.remove(random.nextInt(leased.size()));
				if (random.nextInt(4) == 0) {
					pool.discard(returned);
				} else {
					pool.release(returned);
				}
			} else {
				final var pooled = pool.acquire(1 + random.nextInt(5 * MIB));
				if (pooled != null) {
					leased.add(pooled);
				}
			}
			assertTrue(pool.liveBytes() <= budget, "step " + step + ": " + pool.liveBytes());
		}
		leased.forEach(pool::release);
		pool.close();
		assertEquals(0, pool.liveBuffers());
		assertEquals(0, pool.invalidReturns.sum());
		assertTrue(transport.areAllDeregistered());
	}

	@Test
	void secondReturnOfABufferIsIgnored() {
		final var pool = new RdmaBufferPool(transport, 1, UNBOUNDED);
		final var pooled = pool.acquire(MIB);
		pool.release(pooled);

		pool.discard(pooled);

		assertEquals(1, pool.invalidReturns.sum());
		assertFalse(transport.wasDeregistered(pooled.mrHandle()), "the idle buffer must stay registered");
		assertSame(pooled, pool.acquire(MIB));
	}

	@Test
	void reclaimIdleDropsOnlyIdleBuffers() {
		final var pool = new RdmaBufferPool(transport, 4, UNBOUNDED);
		final var idle = pool.acquire(2 * MIB);
		final var leased = pool.acquire(MIB);
		pool.release(idle);

		assertEquals(2L * MIB, pool.reclaimIdle());

		assertTrue(transport.wasDeregistered(idle.mrHandle()));
		assertFalse(transport.wasDeregistered(leased.mrHandle()));
		assertEquals(0, pool.idleBuffers());
		assertEquals(1, pool.liveBuffers());
		assertEquals((long) MIB, pool.liveBytes());
	}

	@Test
	void exhaustedUnpooledAllocationReclaimsIdleBuffersAndRetriesOnce() {
		final var pool = new RdmaBufferPool(transport, 4, UNBOUNDED);
		pool.release(pool.acquire(MIB));
		final int[] calls = {0
		};

		final ByteBuffer allocated = RdmaBufferPool.allocateUnpooled(4096, pool, size -> {
			if (calls[0]++ == 0) {
				throw new OutOfMemoryError("Cannot reserve 4096 bytes of direct buffer memory");
			}
			return ByteBuffer.allocateDirect(size);
		});

		assertNotNull(allocated);
		assertEquals(2, calls[0]);
		assertEquals(0, pool.idleBuffers());
	}

	@Test
	void exhaustedUnpooledAllocationWithNothingToReclaimFails() {
		final var pool = new RdmaBufferPool(transport, 4, UNBOUNDED);
		final int[] calls = {0
		};
		final java.util.function.IntFunction<ByteBuffer> exhausted = size -> {
			calls[0]++;
			throw new OutOfMemoryError("Cannot reserve direct buffer memory");
		};

		assertNull(RdmaBufferPool.allocateUnpooled(4096, pool, exhausted));
		assertEquals(1, calls[0], "no retry without reclaimed memory");
		assertNull(RdmaBufferPool.allocateUnpooled(4096, null, exhausted));
	}

	@Test
	void defaultBudgetIsAShareOfTheDirectMemoryLimit() {
		assertTrue(RdmaBufferPool.defaultMaxPooledBytes() > 0);
	}

	@Test
	void releasedBufferIsReusedWithoutRegistering() {
		final var pool = new RdmaBufferPool(transport, 2, UNBOUNDED);
		final var first = pool.acquire(MIB);
		pool.release(first);

		final var second = pool.acquire(MIB - 1);

		assertSame(first, second);
		assertEquals(1, transport.getRegisterCount());
		assertEquals(1, pool.hits.sum());
		assertEquals(1, pool.created.sum());
	}

	@Test
	void eachSizeClassHasItsOwnBuffers() {
		final var pool = new RdmaBufferPool(transport, 2, UNBOUNDED);
		final var small = pool.acquire(MIB);
		pool.release(small);

		final var large = pool.acquire(4 * MIB);

		assertNotEquals(small.mrHandle(), large.mrHandle());
		assertEquals(4 * MIB, large.capacity());
		assertEquals(2, transport.getRegisterCount());
	}

	@Test
	void exhaustedClassReturnsNullUntilABufferIsReleased() {
		final var pool = new RdmaBufferPool(transport, 1, UNBOUNDED);
		final var held = pool.acquire(MIB);

		assertNull(pool.acquire(MIB));
		assertEquals(1, pool.exhausted.sum());

		pool.release(held);
		assertSame(held, pool.acquire(MIB));
	}

	@Test
	void discardDeregistersAndFreesTheSlot() {
		final var pool = new RdmaBufferPool(transport, 1, UNBOUNDED);
		final var discarded = pool.acquire(MIB);

		pool.discard(discarded);

		assertTrue(transport.wasDeregistered(discarded.mrHandle()));
		assertEquals(0, pool.liveBuffers());
		final var replacement = pool.acquire(MIB);
		assertNotNull(replacement, "the discarded buffer's slot is available again");
		assertNotEquals(discarded.mrHandle(), replacement.mrHandle(), "a discarded buffer is never handed out");
	}

	@Test
	void registrationFailureDoesNotConsumeASlot() {
		final var pool = new RdmaBufferPool(transport, 1, UNBOUNDED);
		transport.setFailAfterNRegistrations(0);
		assertNull(pool.acquire(MIB));
		assertEquals(0, pool.liveBuffers());

		transport.setFailAfterNRegistrations(-1);
		assertNotNull(pool.acquire(MIB));
	}

	@Test
	void registrationFailureIsCountedAsOneAndReturnsTheReservation() {
		final var pool = new RdmaBufferPool(transport, 2, 8L * MIB);
		transport.setFailNextRegistrations(1);

		assertNull(pool.acquire(MIB));

		assertEquals(1, pool.registrationFailed.sum());
		assertEquals(0, pool.allocationFailed.sum());
		assertEquals(0, pool.created.sum());
		assertEquals(0, pool.exhausted.sum(), "the pool had room; the transport refused the buffer");
		// The capacity was reserved while the buffer was being created, and is free again.
		assertEquals(new RdmaBufferPool.Levels(0, 8L * MIB, 0, 0, MIB), pool.levels());
		assertNotNull(pool.acquire(MIB));
		assertEquals(new RdmaBufferPool.Levels(MIB, 8L * MIB, 1, 0, MIB), pool.levels());
	}

	@Test
	void registrationThatThrowsIsCountedAsARegistrationFailure() {
		final var throwing = new FakeRdmaTransport(new RdmaConfig(true, 0, false, "auto", "", "WARN")) {
			@Override
			public long registerBuffer(final ByteBuffer buffer, final int size) {
				throw new IllegalStateException("injected registration failure");
			}
		};
		final var pool = new RdmaBufferPool(throwing, 2, UNBOUNDED);

		assertNull(pool.acquire(MIB));

		assertEquals(1, pool.registrationFailed.sum());
		assertEquals(0, pool.allocationFailed.sum());
		assertEquals(0, pool.liveBuffers());
	}

	@Test
	void allocationFailureIsCountedAsOneAndReturnsTheReservation() {
		final var pool = new RdmaBufferPool(transport, 2, 8L * MIB);
		pool.allocator = capacity -> {
			throw new OutOfMemoryError("Cannot reserve " + capacity + " bytes of direct buffer memory");
		};

		assertNull(pool.acquire(2 * MIB));

		assertEquals(1, pool.allocationFailed.sum());
		assertEquals(0, pool.registrationFailed.sum());
		assertEquals(0, pool.created.sum());
		assertEquals(0, transport.getRegisterCount(), "nothing to register without a buffer");
		assertEquals(new RdmaBufferPool.Levels(0, 8L * MIB, 0, 0, 2L * MIB), pool.levels());
	}

	@Test
	void levelsDescribeLeasedAndIdleBuffersTogether() {
		final var pool = new RdmaBufferPool(transport, 2, 16L * MIB);
		assertEquals(new RdmaBufferPool.Levels(0, 16L * MIB, 0, 0, 0), pool.levels());

		final var small = pool.acquire(MIB);
		final var large = pool.acquire(4 * MIB);
		assertEquals(new RdmaBufferPool.Levels(5L * MIB, 16L * MIB, 2, 0, 5L * MIB), pool.levels());

		pool.release(small);
		assertEquals(new RdmaBufferPool.Levels(5L * MIB, 16L * MIB, 2, 1, 5L * MIB), pool.levels());

		pool.discard(large);
		assertEquals(new RdmaBufferPool.Levels(MIB, 16L * MIB, 1, 1, 5L * MIB), pool.levels());
		assertEquals(pool.liveBytes(), pool.levels().reservedBytes());
		assertEquals(pool.liveBuffers(), pool.levels().buffers());
		assertEquals(pool.idleBuffers(), pool.levels().idleBuffers());
	}

	@Test
	void peakKeepsTheMostEverReservedAcrossCreationEvictionAndClose() {
		final var pool = new RdmaBufferPool(transport, 4, 4L * MIB);
		final var small = pool.acquire(MIB);
		assertEquals((long) MIB, pool.levels().peakReservedBytes());
		final var medium = pool.acquire(2 * MIB);
		assertEquals(3L * MIB, pool.levels().peakReservedBytes());

		// A reused buffer reserves nothing new.
		pool.release(small);
		assertSame(small, pool.acquire(MIB));
		assertEquals(3L * MIB, pool.levels().peakReservedBytes());

		// A discarded buffer lowers what is reserved, not the peak.
		pool.discard(small);
		assertEquals(new RdmaBufferPool.Levels(2L * MIB, 4L * MIB, 1, 0, 3L * MIB), pool.levels());

		// The eviction makes room before the new buffer is reserved: 2 MiB out, 4 MiB in.
		pool.release(medium);
		final var large = pool.acquire(3 * MIB);
		assertEquals(1, pool.evicted.sum());
		assertEquals(new RdmaBufferPool.Levels(4L * MIB, 4L * MIB, 1, 0, 4L * MIB), pool.levels());

		pool.release(large);
		pool.close();
		assertEquals(new RdmaBufferPool.Levels(0, 4L * MIB, 0, 0, 4L * MIB), pool.levels());
	}

	@Test
	@Timeout(30)
	void buffersBeingCreatedAtTheSameTimeAreEachReservedAndCountedOnce() throws Exception {
		final int creators = 4;
		final var pool = new RdmaBufferPool(transport, creators, UNBOUNDED);
		final CountDownLatch allCreating = new CountDownLatch(creators);
		final CountDownLatch finish = new CountDownLatch(1);
		final AtomicInteger allocations = new AtomicInteger();
		// Every creation waits in its allocation, outside the pool's lock; every second one then fails.
		pool.allocator = capacity -> {
			final int number = allocations.incrementAndGet();
			allCreating.countDown();
			try {
				assertTrue(finish.await(20, TimeUnit.SECONDS), "the creations were not let go");
			} catch (final InterruptedException e) {
				throw new IllegalStateException(e);
			}
			if (number % 2 == 0) {
				throw new OutOfMemoryError("Cannot reserve " + capacity + " bytes of direct buffer memory");
			}
			return ByteBuffer.allocateDirect(capacity);
		};
		final List<RdmaBufferPool.PooledBuffer> leased = java.util.Collections.synchronizedList(new ArrayList<>());
		final List<Thread> threads = new ArrayList<>();
		for (int i = 0; i < creators; i++) {
			final Thread thread = new Thread(() -> {
				final var pooled = pool.acquire(MIB);
				if (pooled != null) {
					leased.add(pooled);
				}
			}, "pool-creator-" + i);
			thread.start();
			threads.add(thread);
		}
		assertTrue(allCreating.await(20, TimeUnit.SECONDS), "the creations did not start");

		// The levels are readable while buffers are being created, and count each as reserved.
		assertEquals(new RdmaBufferPool.Levels((long) creators * MIB, UNBOUNDED, creators, 0, (long) creators * MIB),
						pool.levels());
		assertNull(pool.acquire(MIB), "every slot of the class is reserved");
		assertEquals(1, pool.exhausted.sum());

		finish.countDown();
		for (final Thread thread : threads) {
			thread.join(TimeUnit.SECONDS.toMillis(20));
			assertFalse(thread.isAlive(), thread.getName());
		}

		assertEquals(creators / 2, leased.size());
		assertEquals(creators / 2, pool.created.sum());
		assertEquals(creators / 2, pool.allocationFailed.sum());
		assertEquals(creators / 2, transport.getRegisterCount());
		assertEquals(new RdmaBufferPool.Levels((long) leased.size() * MIB, UNBOUNDED, leased.size(), 0, (long) creators * MIB),
						pool.levels());
		leased.forEach(pool::release);
		assertEquals(leased.size(), pool.levels().idleBuffers());
	}

	@Test
	@Timeout(60)
	void levelsReadWhileBuffersComeAndGoAgreeWithOneAnother() throws Exception {
		final long budget = 6L * MIB;
		// One size class, so the bytes reserved are always the buffers times the class size.
		final var pool = new RdmaBufferPool(transport, 8, budget);
		final AtomicBoolean done = new AtomicBoolean();
		final AtomicReference<Throwable> failure = new AtomicReference<>();
		final AtomicInteger reads = new AtomicInteger();
		final Thread reader = new Thread(() -> {
			try {
				long lastPeak = 0;
				while (!done.get()) {
					final RdmaBufferPool.Levels levels = pool.levels();
					if (levels.reservedBytes() != (long) levels.buffers() * MIB
									|| levels.idleBuffers() < 0
									|| levels.idleBuffers() > levels.buffers()
									|| levels.reservedBytes() > levels.budgetBytes()
									|| levels.peakReservedBytes() < levels.reservedBytes()
									|| levels.peakReservedBytes() < lastPeak
									|| levels.peakReservedBytes() > budget) {
						throw new AssertionError("levels disagree: " + levels + " after peak " + lastPeak);
					}
					lastPeak = levels.peakReservedBytes();
					reads.incrementAndGet();
				}
			} catch (final Throwable e) {
				failure.set(e);
			}
		}, "pool-levels-reader");
		reader.start();
		final List<Thread> workers = new ArrayList<>();
		for (int i = 0; i < 4; i++) {
			final long seed = 20261013L + i;
			final Thread worker = new Thread(() -> {
				final var random = new Random(seed);
				final List<RdmaBufferPool.PooledBuffer> leased = new ArrayList<>();
				for (int step = 0; step < 2_000 && failure.get() == null; step++) {
					if (!leased.isEmpty() && random.nextBoolean()) {
						final var returned = leased.remove(random.nextInt(leased.size()));
						if (random.nextInt(3) == 0) {
							pool.discard(returned);
						} else {
							pool.release(returned);
						}
					} else {
						if (random.nextInt(8) == 0) {
							transport.setFailNextRegistrations(1);
						}
						final var pooled = pool.acquire(1 + random.nextInt(MIB));
						if (pooled != null) {
							leased.add(pooled);
						}
					}
				}
				leased.forEach(pool::release);
			}, "pool-worker-" + i);
			worker.start();
			workers.add(worker);
		}
		for (final Thread worker : workers) {
			worker.join(TimeUnit.SECONDS.toMillis(50));
			assertFalse(worker.isAlive(), worker.getName());
		}
		done.set(true);
		reader.join(TimeUnit.SECONDS.toMillis(10));

		assertFalse(reader.isAlive(), "the reader did not finish");
		assertNull(failure.get(), () -> "levels read while buffers came and went: " + failure.get());
		assertTrue(reads.get() > 0, "the reader read nothing");
		final RdmaBufferPool.Levels atRest = pool.levels();
		assertEquals(atRest.buffers(), atRest.idleBuffers(), "every lease was returned: " + atRest);
		assertEquals(0, pool.invalidReturns.sum());
	}

	@Test
	void readingTheLevelsNeverReachesTheTransport() {
		final FakeRdmaTransport observed = spy(transport);
		final var pool = new RdmaBufferPool(observed, 2, UNBOUNDED);
		final var leased = pool.acquire(MIB);
		pool.release(pool.acquire(2 * MIB));
		clearInvocations(observed);

		for (int i = 0; i < 100; i++) {
			assertEquals(3L * MIB, pool.levels().reservedBytes());
		}
		verifyNoInteractions(observed);

		pool.release(leased);
		pool.close();
		clearInvocations(observed);

		assertEquals(new RdmaBufferPool.Levels(0, UNBOUNDED, 0, 0, 3L * MIB), pool.levels());
		verifyNoInteractions(observed);
	}

	@Test
	void closeDeregistersIdleBuffersAndLaterReleasesDiscard() {
		final var pool = new RdmaBufferPool(transport, 2, UNBOUNDED);
		final var idle = pool.acquire(MIB);
		final var inUse = pool.acquire(MIB);
		pool.release(idle);

		pool.close();

		assertTrue(transport.wasDeregistered(idle.mrHandle()));
		assertEquals(1, pool.liveBuffers(), "the in-use buffer belongs to its request until it ends");
		assertNull(pool.acquire(MIB));

		pool.release(inUse);
		assertTrue(transport.wasDeregistered(inUse.mrHandle()));
		assertEquals(0, pool.liveBuffers());
		assertEquals(0, pool.idleBuffers());
		assertTrue(transport.areAllDeregistered());
	}

	@Test
	void staleContentIsAlteredOnEveryPage() {
		final int size = 3 * S3RdmaStorageDriver.STALE_CONTENT_STRIDE_BYTES + 1;
		final ByteBuffer buffer = ByteBuffer.allocateDirect(MIB);
		final byte[] before = new byte[size];
		for (int i = 0; i < size; i++) {
			buffer.put(i, (byte) i);
			before[i] = (byte) i;
		}

		S3RdmaStorageDriver.invalidateStaleContent(buffer, size);

		for (int page = 0; page < size; page += S3RdmaStorageDriver.STALE_CONTENT_STRIDE_BYTES) {
			final int start = page;
			final int end = Math.min(size, page + S3RdmaStorageDriver.STALE_CONTENT_STRIDE_BYTES);
			boolean changed = false;
			for (int i = start; i < end; i++) {
				changed |= buffer.get(i) != before[i];
			}
			assertTrue(changed, "page at " + page + " must differ");
		}
		assertNotEquals(before[size - 1], buffer.get(size - 1), "the last byte must differ");
	}
}
