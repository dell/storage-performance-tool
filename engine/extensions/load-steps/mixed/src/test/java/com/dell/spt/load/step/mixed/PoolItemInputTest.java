package com.dell.spt.load.step.mixed;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemImpl;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link PoolItemInput}.
 *
 * <p>PoolItemInput manages two item sources for the MixedLoadGenerator:
 * <ol>
 *   <li>A static read pool (immutable after construction) for GET/STAT operations
 *   <li>A non-blocking delete queue (ConcurrentLinkedQueue) for DELETE operations, fed by PUT completions
 * </ol>
 */
class PoolItemInputTest {

	// ── Read pool construction ────────────────────────────────────────────

	@Test
	@DisplayName("Rejects empty seed list")
	void rejectsEmptySeedList() {
		assertThrows(IllegalArgumentException.class,
						() -> new PoolItemInput<>(List.of()));
	}

	@Test
	@DisplayName("Rejects null seed list")
	void rejectsNullSeedList() {
		assertThrows(IllegalArgumentException.class,
						() -> new PoolItemInput<>(null));
	}

	@Test
	@DisplayName("Read pool size matches seed list")
	void readPoolSizeMatchesSeed() {
		final List<Item> seeds = makeItems(100);
		final PoolItemInput<Item> pool = new PoolItemInput<>(seeds);
		assertEquals(100, pool.readPoolSize());
	}

	// ── randomItem (read pool) ────────────────────────────────────────────

	@Test
	@DisplayName("randomItem returns items from the seed pool")
	void randomItemReturnsFromPool() {
		final List<Item> seeds = makeItems(10);
		final PoolItemInput<Item> pool = new PoolItemInput<>(seeds);
		final Set<String> seedNames = new HashSet<>();
		for (final Item s : seeds)
			seedNames.add(s.name());

		for (int i = 0; i < 100; i++) {
			final Item item = pool.randomItem();
			assertNotNull(item);
			assertTrue(seedNames.contains(item.name()),
							"randomItem returned item not in seed pool: " + item.name());
		}
	}

	@Test
	@DisplayName("randomItem covers the full pool over many calls")
	void randomItemCoversFullPool() {
		final List<Item> seeds = makeItems(5);
		final PoolItemInput<Item> pool = new PoolItemInput<>(seeds);
		final Set<String> seen = new HashSet<>();

		// With 5 items and 1000 calls, we should see all of them
		for (int i = 0; i < 1000; i++) {
			seen.add(pool.randomItem().name());
		}
		assertEquals(5, seen.size(), "Should have seen all 5 seed items");
	}

	@Test
	@DisplayName("randomItem is thread-safe — concurrent calls don't throw")
	void randomItemThreadSafe() throws InterruptedException {
		final List<Item> seeds = makeItems(50);
		final PoolItemInput<Item> pool = new PoolItemInput<>(seeds);

		final int threadCount = 8;
		final int callsPerThread = 10_000;
		final Thread[] threads = new Thread[threadCount];
		final boolean[] errors = {false
		};

		for (int t = 0; t < threadCount; t++) {
			threads[t] = new Thread(() -> {
				for (int i = 0; i < callsPerThread; i++) {
					final Item item = pool.randomItem();
					if (item == null) {
						errors[0] = true;
						return;
					}
				}
			});
		}
		for (final Thread t : threads)
			t.start();
		for (final Thread t : threads)
			t.join();
		assertFalse(errors[0], "randomItem returned null during concurrent access");
	}

	// ── Delete queue ──────────────────────────────────────────────────────

	@Test
	@DisplayName("pollDelete returns null when queue is empty")
	void pollDeleteEmptyReturnsNull() {
		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(5));
		assertNull(pool.pollDelete(), "Empty delete queue should return null");
	}

	@Test
	@DisplayName("addDeleteItem and pollDelete form a FIFO queue")
	void addAndPollDeleteFIFO() {
		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(5));
		final Item a = new ItemImpl("item-a");
		final Item b = new ItemImpl("item-b");
		final Item c = new ItemImpl("item-c");

		pool.addDeleteItem(a);
		pool.addDeleteItem(b);
		pool.addDeleteItem(c);

		assertSame(a, pool.pollDelete());
		assertSame(b, pool.pollDelete());
		assertSame(c, pool.pollDelete());
		assertNull(pool.pollDelete());
	}

	@Test
	@DisplayName("deleteQueueSize tracks queue depth")
	void deleteQueueSizeTracking() {
		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(5));
		assertEquals(0, pool.deleteQueueSize());

		pool.addDeleteItem(new ItemImpl("x"));
		pool.addDeleteItem(new ItemImpl("y"));
		assertEquals(2, pool.deleteQueueSize());

		pool.pollDelete();
		assertEquals(1, pool.deleteQueueSize());
	}

	@Test
	@DisplayName("Pre-populate delete queue from a collection")
	void prePopulateDeleteQueue() {
		final List<Item> seeds = makeItems(5);
		final List<Item> deleteSeeds = makeItems(3, "del-");
		final PoolItemInput<Item> pool = new PoolItemInput<>(seeds, deleteSeeds);

		assertEquals(3, pool.deleteQueueSize());
		assertEquals("del-0", pool.pollDelete().name());
		assertEquals("del-1", pool.pollDelete().name());
		assertEquals("del-2", pool.pollDelete().name());
		assertNull(pool.pollDelete());
	}

	@Test
	@DisplayName("Concurrent add and poll on delete queue don't lose items")
	void concurrentDeleteQueueAccess() throws InterruptedException {
		final PoolItemInput<Item> pool = new PoolItemInput<>(makeItems(5));
		final int producerCount = 4;
		final int itemsPerProducer = 1000;
		final ConcurrentLinkedQueue<Item> consumed = new ConcurrentLinkedQueue<>();
		final Thread[] producers = new Thread[producerCount];
		final Thread[] consumers = new Thread[producerCount];

		for (int p = 0; p < producerCount; p++) {
			final int pid = p;
			producers[p] = new Thread(() -> {
				for (int i = 0; i < itemsPerProducer; i++) {
					pool.addDeleteItem(new ItemImpl("p" + pid + "-" + i));
				}
			});
		}

		for (int c = 0; c < producerCount; c++) {
			consumers[c] = new Thread(() -> {
				int spins = 0;
				while (consumed.size() < producerCount * itemsPerProducer && spins < 100_000) {
					final Item item = pool.pollDelete();
					if (item != null) {
						consumed.add(item);
						spins = 0;
					} else {
						spins++;
					}
				}
			});
		}

		for (final Thread t : producers)
			t.start();
		for (final Thread t : consumers)
			t.start();
		for (final Thread t : producers)
			t.join();
		for (final Thread t : consumers)
			t.join();

		// All produced items should have been consumed (queue should be drained)
		// Allow for some remaining in queue due to timing
		final int totalConsumed = consumed.size() + pool.deleteQueueSize();
		assertEquals(producerCount * itemsPerProducer, totalConsumed,
						"Total items (consumed + remaining in queue) should equal total produced");
	}

	// ── Helpers ───────────────────────────────────────────────────────────

	private static List<Item> makeItems(final int count) {
		return makeItems(count, "item-");
	}

	private static List<Item> makeItems(final int count, final String prefix) {
		final List<Item> items = new ArrayList<>(count);
		for (int i = 0; i < count; i++) {
			items.add(new ItemImpl(prefix + i));
		}
		return items;
	}
}
