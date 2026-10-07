package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

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
		final int[] calls = {0};

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
		final int[] calls = {0};
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
