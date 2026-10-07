package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RdmaBufferPoolTest {

	private static final int MIB = 1 << 20;

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
		assertEquals((1 << 30) + 1, RdmaBufferPool.capacityFor((1 << 30) + 1));
	}

	@Test
	void releasedBufferIsReusedWithoutRegistering() {
		final var pool = new RdmaBufferPool(transport, 2);
		final var first = pool.acquire(MIB);
		pool.release(first);

		final var second = pool.acquire(MIB - 1);

		assertSame(first, second);
		assertEquals(1, transport.getRegisterCount());
		assertEquals(1, pool.hits.sum());
		assertEquals(1, pool.misses.sum());
	}

	@Test
	void eachSizeClassHasItsOwnBuffers() {
		final var pool = new RdmaBufferPool(transport, 2);
		final var small = pool.acquire(MIB);
		pool.release(small);

		final var large = pool.acquire(4 * MIB);

		assertNotEquals(small.mrHandle(), large.mrHandle());
		assertEquals(4 * MIB, large.capacity());
		assertEquals(2, transport.getRegisterCount());
	}

	@Test
	void exhaustedClassReturnsNullUntilABufferIsReleased() {
		final var pool = new RdmaBufferPool(transport, 1);
		final var held = pool.acquire(MIB);

		assertNull(pool.acquire(MIB));
		assertEquals(1, pool.exhausted.sum());

		pool.release(held);
		assertSame(held, pool.acquire(MIB));
	}

	@Test
	void discardDeregistersAndFreesTheSlot() {
		final var pool = new RdmaBufferPool(transport, 1);
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
		final var pool = new RdmaBufferPool(transport, 1);
		transport.setFailAfterNRegistrations(0);
		assertNull(pool.acquire(MIB));
		assertEquals(0, pool.liveBuffers());

		transport.setFailAfterNRegistrations(-1);
		assertNotNull(pool.acquire(MIB));
	}

	@Test
	void closeDeregistersIdleBuffersAndLaterReleasesDiscard() {
		final var pool = new RdmaBufferPool(transport, 2);
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
