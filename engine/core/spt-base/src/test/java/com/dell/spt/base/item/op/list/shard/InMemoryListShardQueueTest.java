package com.dell.spt.base.item.op.list.shard;

import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class InMemoryListShardQueueTest {

	@Test
	void acquireReturnsOfferedShard() throws Exception {
		final var queue = ListShardQueue.inMemory();
		final var shard = new ListShard("a", "b", null, null);
		assertTrue(queue.offer(shard));
		try (ListShardQueue.Lease lease = queue.acquire()) {
			assertSame(shard.prefix(), lease.shard().prefix());
			lease.complete();
		}
		assertEquals(0, queue.pendingCount());
	}

	@Test
	void requeueMakesShardAvailableAgain() throws Exception {
		final var queue = ListShardQueue.inMemory();
		queue.offer(new ListShard("p", "q", null, null));
		final ListShardQueue.Lease lease = queue.acquire();
		final var updated = lease.shard().withContinuationToken("token-1");
		lease.update(updated);
		lease.requeue();
		assertEquals(1, queue.pendingCount());
		try (ListShardQueue.Lease nextLease = queue.acquire()) {
			assertEquals("token-1", nextLease.shard().continuationToken());
		}
	}

	@Test
	void metricsRecorderReceivesLifecycleCallbacks() throws Exception {
		final var leases = new ArrayList<String>();
		final var requeues = new ArrayList<String>();
		final var completes = new ArrayList<String>();
		final var metricsRecorder = new ListShardMetricsRecorder() {
			@Override
			public void onLease(final ListShard shard) {
				leases.add(shard.prefix());
			}

			@Override
			public void onRequeue(final ListShard shard) {
				requeues.add(shard.prefix());
			}

			@Override
			public void onComplete(final ListShard shard) {
				completes.add(shard.prefix());
			}
		};
		final var queue = ListShardQueue.inMemory(metricsRecorder);
		queue.offer(new ListShard("0", "1", null, null));
		try (ListShardQueue.Lease lease = queue.acquire()) {
			lease.complete();
		}
		assertEquals(List.of("0"), leases);
		assertEquals(List.of("0"), completes);
		assertEquals(List.of("0"), requeues); // offer() path triggers requeue callback
	}

	@Test
	void timedAcquireReturnsNullWhenNoShardAvailable() throws Exception {
		final var queue = ListShardQueue.inMemory();
		assertNull(queue.acquire(50, TimeUnit.MILLISECONDS));
	}

	@Test
	void leaseIsIdempotentOnCompletion() throws Exception {
		final AtomicInteger completes = new AtomicInteger();
		final var queue = ListShardQueue.inMemory(new ListShardMetricsRecorder() {
			@Override
			public void onComplete(final ListShard shard) {
				completes.incrementAndGet();
			}
		});
		queue.offer(new ListShard("x", "y", null, null));
		try (ListShardQueue.Lease lease = queue.acquire()) {
			lease.complete();
			lease.complete();
			lease.requeue();
		}
		assertEquals(1, completes.get());
	}

	@Test
	void splitReplacesParentWithChildren() throws Exception {
		final var completes = new ArrayList<String>();
		final var requeues = new ArrayList<String>();
		final var recorder = new ListShardMetricsRecorder() {
			@Override
			public void onRequeue(final ListShard shard) {
				requeues.add(shard.prefix());
			}

			@Override
			public void onComplete(final ListShard shard) {
				completes.add(shard.prefix());
			}
		};
		final var queue = ListShardQueue.inMemory(recorder);
		final var parent = new ListShard("p", null, null, null);
		assertTrue(queue.offer(parent));
		try (ListShardQueue.Lease lease = queue.acquire()) {
			final var children = List.of(
							new ListShard("pa", null, null, null, null, parent.splitDepth() + 1),
							new ListShard("pb", null, null, null, null, parent.splitDepth() + 1));
			assertTrue(lease.split(children));
			assertThrows(IllegalStateException.class, () -> lease.update(children.get(0)));
		}
		assertEquals(2, queue.pendingCount());
		try (ListShardQueue.Lease childLease = queue.acquire()) {
			assertEquals("pa", childLease.shard().prefix());
			childLease.complete();
		}
		try (ListShardQueue.Lease childLease = queue.acquire()) {
			assertEquals("pb", childLease.shard().prefix());
			childLease.complete();
		}
		assertEquals(0, queue.pendingCount());
		assertEquals(List.of("p", "pa", "pb"), completes);
		assertEquals(List.of("p", "pa", "pb"), requeues);
	}

	@Test
	void requeuePreservesShardMetadata() throws Exception {
		final var queue = ListShardQueue.inMemory();
		final var original = new ListShard("logs/", null, "tok-0", "start-0", "last-0", 3);
		assertTrue(queue.offer(original));
		try (ListShardQueue.Lease lease = queue.acquire()) {
			final var updated = original.withContinuationToken("tok-1").withStartAfter("start-1").withLastKey("start-1");
			lease.update(updated);
			lease.requeue();
		}
		try (ListShardQueue.Lease lease = queue.acquire()) {
			final var shard = lease.shard();
			assertEquals(3, shard.splitDepth());
			assertEquals("tok-1", shard.continuationToken());
			assertEquals("start-1", shard.startAfter());
			assertEquals("start-1", shard.lastKey());
			lease.complete();
		}
	}

	@Test
	void concurrentLeasesAreExclusive() throws Exception {
		final int shardCount = 8;
		final var queue = ListShardQueue.inMemory();
		for (int i = 0; i < shardCount; i++) {
			queue.offer(new ListShard("p" + i, null, null, null));
		}
		final var seenPrefixes = Collections.synchronizedSet(new HashSet<String>());
		final var latch = new CountDownLatch(shardCount);
		final var workers = Executors.newFixedThreadPool(shardCount);
		try {
			for (int i = 0; i < shardCount; i++) {
				workers.execute(
								() -> {
									try (ListShardQueue.Lease lease = queue.acquire()) {
										final String prefix = lease.shard().prefix();
										assertTrue(seenPrefixes.add(prefix), () -> "duplicate lease for " + prefix);
										lease.complete();
									} catch (InterruptedException e) {
										Thread.currentThread().interrupt();
										fail(e);
									} finally {
										latch.countDown();
									}
								});
			}
			assertTrue(latch.await(5, TimeUnit.SECONDS), "workers did not finish in time");
		} finally {
			workers.shutdownNow();
		}
		assertEquals(shardCount, seenPrefixes.size());
		assertEquals(0, queue.pendingCount());
	}

	@Test
	void watchdogRescuesStalledLease() throws Exception {
		final var rescued = new java.util.ArrayList<String>();
		final var recorder = new ListShardMetricsRecorder() {
			@Override
			public void onRescue(final ListShard shard) {
				rescued.add(shard.prefix());
			}

			@Override
			public long stallThresholdMillis() {
				return 50L;
			}
		};
		final var queue = ListShardQueue.inMemory(recorder, 50L);
		final var shard = new ListShard("rescue", null, null, null);
		assertTrue(queue.offer(shard));
		final ListShardQueue.Lease lease = queue.acquire();
		// simulate worker stall by doing nothing and allowing watchdog to run
		Thread.sleep(150L);
		final ListShardQueue.Lease rescuedLease = queue.acquire(1, TimeUnit.SECONDS);
		assertNotNull(rescuedLease, "watchdog should requeue stalled shard");
		assertEquals("rescue", rescuedLease.shard().prefix());
		rescuedLease.complete();
		lease.complete();
		assertTrue(rescued.contains("rescue"));
	}
}
