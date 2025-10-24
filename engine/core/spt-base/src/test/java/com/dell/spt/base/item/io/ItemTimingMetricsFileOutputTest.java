package com.dell.spt.base.item.io;

import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.Operation;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ItemTimingMetricsFileOutputTest {

	private Path tempDir;

	@BeforeEach
	void setUp() throws Exception {
		tempDir = Files.createTempDirectory("itm-timing-");
	}

	@AfterEach
	void tearDown() throws Exception {
		if (tempDir != null) {
			try (var paths = Files.walk(tempDir)) {
				paths
								.sorted((a, b) -> b.getNameCount() - a.getNameCount())
								.forEach(p -> {
									try {
										Files.deleteIfExists(p);
									} catch (final Exception e) {
										throw new RuntimeException("Failed to delete path " + p, e);
									}
								});
			} catch (final Exception e) {
				fail("Failed to clean up temporary directory " + tempDir, e);
			}
		}
	}

	@Test
	void putSingleWritesLine() throws Exception {
		Path out = tempDir.resolve("a/b/metrics.txt");
		ItemTimingMetricsFileOutput<Item, Operation<Item>> output = new ItemTimingMetricsFileOutput<>(out);

		Operation<Item> op = mockOperation(100L, 200L);
		assertTrue(output.put(op));
		output.close();

		List<String> lines = Files.readAllLines(out);
		assertEquals(List.of("100 200"), lines);
	}

	@Test
	void putListWritesAllAndStopsOnPoison() throws Exception {
		Path out = tempDir.resolve("metrics2.txt");
		ItemTimingMetricsFileOutput<Item, Operation<Item>> output = new ItemTimingMetricsFileOutput<>(out);

		Operation<Item> op1 = mockOperation(1L, 2L);
		Operation<Item> op2 = mockOperation(3L, 4L);
		Operation<Item> poison = null; // indicates finish
		Operation<Item> op3 = mockOperation(5L, 6L);

		int written = output.put(Arrays.asList(op1, op2, poison, op3));
		// After poison, only the first two should be written
		assertEquals(2, written);

		List<String> lines = Files.readAllLines(out);
		assertEquals(List.of("1 2", "3 4"), lines);
	}

	@Test
	void putRangeWritesSubset() throws Exception {
		Path out = tempDir.resolve("metrics3.txt");
		ItemTimingMetricsFileOutput<Item, Operation<Item>> output = new ItemTimingMetricsFileOutput<>(out);

		List<Operation<Item>> ops = List.of(
						mockOperation(10L, 20L),
						mockOperation(30L, 40L),
						mockOperation(50L, 60L));

		int from = 1, to = 3; // should write second and third
		int n = output.put(ops, from, to);
		assertEquals(to - from, n);
		output.close();

		List<String> lines = Files.readAllLines(out);
		assertEquals(List.of("30 40", "50 60"), lines);
	}

	@Test
	void putSinglePoisonCloses() throws Exception {
		Path out = tempDir.resolve("metrics4.txt");
		ItemTimingMetricsFileOutput<Item, Operation<Item>> output = new ItemTimingMetricsFileOutput<>(out);

		// null is the poison pill: closes and returns true
		Operation<Item> poison = null;
		assertTrue(output.put(poison));

		// File should exist but be empty
		assertTrue(Files.exists(out));
		List<String> lines = Files.readAllLines(out);
		assertTrue(lines.isEmpty());
	}

	// Helper to create a minimal Operation mock that only stubs latency and duration
	@SuppressWarnings("unchecked")
	private Operation<Item> mockOperation(long latency, long duration) {
		Operation<Item> op = mock(Operation.class, Mockito.withSettings().lenient());
		when(op.latency()).thenReturn(latency);
		when(op.duration()).thenReturn(duration);
		return op;
	}
}
