package com.dell.spt.load.step.mixed;

import com.dell.spt.base.item.op.OpType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for {@link OpSchedule}.
 *
 * <p>OpSchedule generates a pre-shuffled, fixed-size array of {@link OpType} values
 * that enforces the desired operation distribution. Modelled after warp's 1000-op
 * round-robin schedule.
 */
class OpScheduleTest {

	// ── Construction & validation ──────────────────────────────────────────

	@Test
	@DisplayName("Rejects empty weights map")
	void rejectsEmptyWeights() {
		assertThrows(IllegalArgumentException.class,
						() -> new OpSchedule(Map.of()));
	}

	@Test
	@DisplayName("Rejects all-zero weights")
	void rejectsAllZeroWeights() {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 0);
		weights.put(OpType.DELETE, 0);
		assertThrows(IllegalArgumentException.class,
						() -> new OpSchedule(weights));
	}

	@Test
	@DisplayName("Rejects negative weights")
	void rejectsNegativeWeights() {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 60);
		weights.put(OpType.DELETE, -10);
		assertThrows(IllegalArgumentException.class,
						() -> new OpSchedule(weights));
	}

	@Test
	@DisplayName("Rejects fewer than 2 non-zero op types")
	void rejectsSingleOpType() {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 100);
		weights.put(OpType.DELETE, 0);
		assertThrows(IllegalArgumentException.class,
						() -> new OpSchedule(weights));
	}

	// ── Ratio correctness ─────────────────────────────────────────────────

	@Test
	@DisplayName("60/40 PUT/DELETE produces exactly 600 and 400 entries in schedule")
	void exactRatio60_40() {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 60);
		weights.put(OpType.DELETE, 40);
		final OpSchedule schedule = new OpSchedule(weights);

		assertEquals(OpSchedule.SCHEDULE_SIZE, schedule.size());

		final Map<OpType, Integer> counts = countOps(schedule);
		assertEquals(600, counts.getOrDefault(OpType.CREATE, 0));
		assertEquals(400, counts.getOrDefault(OpType.DELETE, 0));
	}

	@Test
	@DisplayName("70/20/10 GET/PUT/DELETE produces correct proportions")
	void exactRatio70_20_10() {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 70);
		weights.put(OpType.CREATE, 20);
		weights.put(OpType.DELETE, 10);
		final OpSchedule schedule = new OpSchedule(weights);

		final Map<OpType, Integer> counts = countOps(schedule);
		assertEquals(700, counts.getOrDefault(OpType.READ, 0));
		assertEquals(200, counts.getOrDefault(OpType.CREATE, 0));
		assertEquals(100, counts.getOrDefault(OpType.DELETE, 0));
	}

	@Test
	@DisplayName("Equal weights 25/25/25/25 produce 250 each")
	void equalWeightsFourOps() {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 25);
		weights.put(OpType.CREATE, 25);
		weights.put(OpType.DELETE, 25);
		weights.put(OpType.STAT, 25);
		final OpSchedule schedule = new OpSchedule(weights);

		final Map<OpType, Integer> counts = countOps(schedule);
		assertEquals(250, counts.getOrDefault(OpType.READ, 0));
		assertEquals(250, counts.getOrDefault(OpType.CREATE, 0));
		assertEquals(250, counts.getOrDefault(OpType.DELETE, 0));
		assertEquals(250, counts.getOrDefault(OpType.STAT, 0));
	}

	@Test
	@DisplayName("Weights that don't sum to 100 are normalized correctly")
	void nonStandardWeightsNormalized() {
		// 3:1 ratio → 750 and 250
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 3);
		weights.put(OpType.DELETE, 1);
		final OpSchedule schedule = new OpSchedule(weights);

		final Map<OpType, Integer> counts = countOps(schedule);
		assertEquals(750, counts.getOrDefault(OpType.CREATE, 0));
		assertEquals(250, counts.getOrDefault(OpType.DELETE, 0));
	}

	@Test
	@DisplayName("Zero-weight op types are excluded from schedule")
	void zeroWeightExcluded() {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 0);
		weights.put(OpType.CREATE, 60);
		weights.put(OpType.DELETE, 40);
		weights.put(OpType.STAT, 0);
		final OpSchedule schedule = new OpSchedule(weights);

		final Map<OpType, Integer> counts = countOps(schedule);
		assertFalse(counts.containsKey(OpType.READ));
		assertFalse(counts.containsKey(OpType.STAT));
		assertEquals(600, counts.getOrDefault(OpType.CREATE, 0));
		assertEquals(400, counts.getOrDefault(OpType.DELETE, 0));
	}

	// ── Shuffle ───────────────────────────────────────────────────────────

	@Test
	@DisplayName("Schedule is shuffled — not a solid block of one type then another")
	void scheduleIsShuffled() {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 60);
		weights.put(OpType.DELETE, 40);
		final OpSchedule schedule = new OpSchedule(weights);

		// Check first 20 entries — with a good shuffle, both types should appear
		final Set<OpType> firstTwenty = new HashSet<>();
		for (int i = 0; i < 20; i++) {
			firstTwenty.add(schedule.next());
		}
		assertTrue(firstTwenty.size() > 1,
						"First 20 entries should contain more than one op type after shuffle");
	}

	// ── Cycling ───────────────────────────────────────────────────────────

	@Test
	@DisplayName("next() cycles back to start after exhausting schedule")
	void cyclesCorrectly() {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 60);
		weights.put(OpType.DELETE, 40);
		final OpSchedule schedule = new OpSchedule(weights);

		// Record first full cycle
		final OpType[] firstCycle = new OpType[OpSchedule.SCHEDULE_SIZE];
		for (int i = 0; i < OpSchedule.SCHEDULE_SIZE; i++) {
			firstCycle[i] = schedule.next();
		}

		// Second cycle should be identical (same array, same order)
		for (int i = 0; i < OpSchedule.SCHEDULE_SIZE; i++) {
			assertEquals(firstCycle[i], schedule.next(),
							"Mismatch at position " + i + " on second cycle");
		}
	}

	@Test
	@DisplayName("Ratio is preserved across multiple full cycles")
	void ratioPreservedAcrossCycles() {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 60);
		weights.put(OpType.DELETE, 40);
		final OpSchedule schedule = new OpSchedule(weights);

		final int cycles = 5;
		final int totalOps = OpSchedule.SCHEDULE_SIZE * cycles;
		int createCount = 0;
		int deleteCount = 0;
		for (int i = 0; i < totalOps; i++) {
			final OpType op = schedule.next();
			if (op == OpType.CREATE) createCount++;
			else if (op == OpType.DELETE) deleteCount++;
		}

		assertEquals(600 * cycles, createCount);
		assertEquals(400 * cycles, deleteCount);
	}

	// ── Thread safety (basic smoke test) ──────────────────────────────────

	@Test
	@DisplayName("Concurrent next() calls don't throw and maintain total count")
	void concurrentAccessDoesNotThrow() throws InterruptedException {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.CREATE, 50);
		weights.put(OpType.DELETE, 50);
		final OpSchedule schedule = new OpSchedule(weights);

		final int threadCount = 8;
		final int opsPerThread = 10_000;
		final int[] totalCounts = new int[OpType.values().length];
		final Thread[] threads = new Thread[threadCount];

		for (int t = 0; t < threadCount; t++) {
			threads[t] = new Thread(() -> {
				final int[] local = new int[OpType.values().length];
				for (int i = 0; i < opsPerThread; i++) {
					local[schedule.next().ordinal()]++;
				}
				synchronized (totalCounts) {
					for (int i = 0; i < local.length; i++) {
						totalCounts[i] += local[i];
					}
				}
			});
		}
		for (final Thread t : threads) t.start();
		for (final Thread t : threads) t.join();

		final int total = threadCount * opsPerThread;
		int sum = 0;
		for (final int c : totalCounts) sum += c;
		assertEquals(total, sum, "Total ops dispatched should equal threadCount * opsPerThread");

		// With 50/50 weights, both types should be well-represented (within 5% of 50%)
		final double createRatio = (double) totalCounts[OpType.CREATE.ordinal()] / total;
		final double deleteRatio = (double) totalCounts[OpType.DELETE.ordinal()] / total;
		assertEquals(0.5, createRatio, 0.05, "CREATE ratio should be ~50%");
		assertEquals(0.5, deleteRatio, 0.05, "DELETE ratio should be ~50%");
	}

	// ── originIndex mapping ───────────────────────────────────────────────

	@Test
	@DisplayName("originIndexFor returns consecutive indices for non-zero ops")
	void originIndexMapping() {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 0);
		weights.put(OpType.CREATE, 60);
		weights.put(OpType.DELETE, 40);
		weights.put(OpType.STAT, 0);
		final OpSchedule schedule = new OpSchedule(weights);

		// Only CREATE and DELETE are active, so indices should be 0 and 1
		assertEquals(0, schedule.originIndexFor(OpType.CREATE));
		assertEquals(1, schedule.originIndexFor(OpType.DELETE));
		assertEquals(-1, schedule.originIndexFor(OpType.READ),
						"Inactive op type should return -1");
		assertEquals(-1, schedule.originIndexFor(OpType.STAT),
						"Inactive op type should return -1");
	}

	@Test
	@DisplayName("activeOpTypes returns only non-zero op types in order")
	void activeOpTypes() {
		final Map<OpType, Integer> weights = new EnumMap<>(OpType.class);
		weights.put(OpType.READ, 10);
		weights.put(OpType.CREATE, 60);
		weights.put(OpType.DELETE, 20);
		weights.put(OpType.STAT, 10);
		final OpSchedule schedule = new OpSchedule(weights);

		final OpType[] active = schedule.activeOpTypes();
		assertEquals(4, active.length);
		// Should be in enum ordinal order: CREATE(1), READ(2), DELETE(4), STAT(6)
		assertEquals(OpType.CREATE, active[0]);
		assertEquals(OpType.READ, active[1]);
		assertEquals(OpType.DELETE, active[2]);
		assertEquals(OpType.STAT, active[3]);
	}

	// ── Helpers ───────────────────────────────────────────────────────────

	private static Map<OpType, Integer> countOps(final OpSchedule schedule) {
		final Map<OpType, Integer> counts = new EnumMap<>(OpType.class);
		for (int i = 0; i < schedule.size(); i++) {
			counts.merge(schedule.next(), 1, Integer::sum);
		}
		return counts;
	}
}
