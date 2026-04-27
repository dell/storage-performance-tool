package com.dell.spt.base.item.op.composite.data;

import static org.junit.jupiter.api.Assertions.*;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.github.akurilov.commons.system.SizeInBytes;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * Tests for CompositeDataOperationImpl, focusing on the result() copy constructor
 * behavior critical for operation recycling in duration-based workloads (MPUGAP-1).
 *
 * <p>When the engine recycles operations via result(), the copy must have empty subTasks
 * so that subOperations() regenerates them on the next cycle. Without this, recycled ops
 * would see allSubOperationsDone()==true immediately and complete without doing any I/O.
 */
class CompositeDataOperationImplTest {

	private static CompositeDataOperationImpl<DataItem> newCompositeOp(
					OpType opType, long itemSize, long threshold) throws IOException {
		final var item = new DataItemImpl("obj", 0, itemSize);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		return new CompositeDataOperationImpl<>(0, opType, item, "/bucket", null, null, null, 0, threshold);
	}

	// ---------- subOperations() generation ----------

	@Test
	void subOperations_generatesCorrectPartCount() throws Exception {
		// 4096 bytes / 1024 threshold = 4 equal parts
		var op = newCompositeOp(OpType.READ, 4096, 1024);
		var parts = op.subOperations();
		assertEquals(4, parts.size());
		assertFalse(op.allSubOperationsDone());
	}

	@Test
	void subOperations_handlesUnevenSizeWithTailPart() throws Exception {
		// 100 bytes / 30 threshold = 3 equal parts (30 each) + 1 tail part (10 bytes)
		var op = newCompositeOp(OpType.READ, 100, 30);
		var parts = op.subOperations();
		assertEquals(4, parts.size(), "3 equal + 1 tail");
	}

	@Test
	void subOperations_cachedOnSecondCall() throws Exception {
		var op = newCompositeOp(OpType.READ, 4096, 1024);
		var first = op.subOperations();
		var second = op.subOperations();
		assertSame(first, second, "Second call should return cached list");
	}

	@Test
	void allSubOperationsDone_initiallyFalseAfterGeneration() throws Exception {
		var op = newCompositeOp(OpType.READ, 4096, 1024);
		op.subOperations();
		assertFalse(op.allSubOperationsDone(), "pendingSubTasksCount should be N after generation");
	}

	@Test
	void allSubOperationsDone_trueAfterMarkingAllComplete() throws Exception {
		var op = newCompositeOp(OpType.READ, 4096, 1024);
		op.subOperations(); // pendingSubTasksCount = 4
		for (int i = 0; i < 4; i++) {
			op.markSubTaskCompleted();
		}
		assertTrue(op.allSubOperationsDone());
	}

	@Test
	void nextSubOperations_returnsRequestedLimit() throws Exception {
		var op = newCompositeOp(OpType.READ, 4096, 1024);

		var parts1 = op.nextSubOperations(2);
		assertEquals(2, parts1.size());

		var parts2 = op.nextSubOperations(1);
		assertEquals(1, parts2.size());

		var parts3 = op.nextSubOperations(5);
		assertEquals(1, parts3.size(), "Should only return remaining parts (1)");

		var parts4 = op.nextSubOperations(1);
		assertTrue(parts4.isEmpty(), "Should return empty list when exhausted");
	}

	@Test
	void nextSubOperations_withZeroLimit_returnsAll() throws Exception {
		var op = newCompositeOp(OpType.READ, 4096, 1024);
		var parts = op.nextSubOperations(0);
		assertEquals(4, parts.size());

		var partsAfter = op.nextSubOperations(1);
		assertTrue(partsAfter.isEmpty());
	}

	// ---------- result() copy behavior (Fix #4: recycled ops) ----------

	@Test
	void resultCopy_preservesSizeThreshold() throws Exception {
		var op = newCompositeOp(OpType.READ, 4096, 1024);
		op.subOperations();
		for (int i = 0; i < 4; i++)
			op.markSubTaskCompleted();
		var copy = op.result();
		assertEquals(op.sizeThreshold(), copy.sizeThreshold());
	}

	@Test
	void resultCopy_copiesPendingSubTasksCount() throws Exception {
		var op = newCompositeOp(OpType.READ, 4096, 1024);
		op.subOperations(); // pendingSubTasksCount = 4
		for (int i = 0; i < 4; i++)
			op.markSubTaskCompleted(); // pendingSubTasksCount = 0
		var copy = op.result();
		// Copy has pendingSubTasksCount = 0 (copied from original)
		assertTrue(copy.allSubOperationsDone(), "Copy should reflect original's completed state");
	}

	@Test
	void resultCopy_hasEmptySubTasks_regeneratedOnAccess() throws Exception {
		var op = newCompositeOp(OpType.READ, 4096, 1024);
		op.subOperations(); // generates 4 sub-tasks
		for (int i = 0; i < 4; i++)
			op.markSubTaskCompleted();
		var copy = op.result();
		// The copy constructor does NOT copy subTasks — the field initializes to empty ArrayList.
		// Calling subOperations() on the copy regenerates them.
		var copyParts = copy.subOperations();
		assertEquals(4, copyParts.size(), "Sub-tasks should be regenerated on the copy");
	}

	@Test
	void subOperations_afterResultCopy_resetsPendingCount() throws Exception {
		// This is the critical behavior for Fix #4: after result(), pendingSubTasksCount
		// is 0 (copied from completed original). Calling subOperations() regenerates
		// sub-tasks and resets pendingSubTasksCount to N.
		var op = newCompositeOp(OpType.READ, 4096, 1024);
		op.subOperations();
		for (int i = 0; i < 4; i++)
			op.markSubTaskCompleted();
		assertTrue(op.allSubOperationsDone());

		var copy = op.result();
		assertTrue(copy.allSubOperationsDone(), "Before regeneration: pending count = 0");

		copy.subOperations();
		assertFalse(copy.allSubOperationsDone(),
						"After regeneration: pending count should be N (not 0)");
	}

	@Test
	void resultCopy_doesNotShareContextData() throws Exception {
		var op = newCompositeOp(OpType.CREATE, 4096, 1024);
		op.put("uploadId", "u-123");
		var copy = op.result();
		assertNull(copy.get("uploadId"), "Context data should not be shared with the copy");
	}

	@Test
	void subOperations_onCopy_generatesIndependentParts() throws Exception {
		var op = newCompositeOp(OpType.READ, 4096, 1024);
		var originalParts = op.subOperations();
		for (int i = 0; i < 4; i++)
			op.markSubTaskCompleted();

		var copy = op.result();
		var copyParts = copy.subOperations();

		assertEquals(originalParts.size(), copyParts.size());
		assertNotSame(originalParts.get(0), copyParts.get(0),
						"Copy's parts should be independent objects");
	}

	@Test
	void subOperations_partReferencesParent() throws Exception {
		var op = newCompositeOp(OpType.READ, 2048, 1024);
		var parts = op.subOperations();
		for (var part : parts) {
			assertSame(op, part.parent(), "Each part should reference the parent composite op");
		}
	}

	// ---------- reset() interaction with composite state ----------

	@Test
	void reset_doesNotClearSubTasks() throws Exception {
		var op = newCompositeOp(OpType.READ, 4096, 1024);
		op.subOperations(); // generates 4 sub-tasks
		op.reset();
		// reset() clears timing/status but subTasks and pendingSubTasksCount are unchanged
		var parts = op.subOperations(); // should return cached (non-empty) list
		assertEquals(4, parts.size(), "reset should not clear sub-tasks");
	}

	@Test
	void markAndUndoSubTaskCompleted_isSymmetric() throws Exception {
		var op = newCompositeOp(OpType.READ, 4096, 1024);
		op.subOperations(); // pendingSubTasksCount = 4
		op.markSubTaskCompleted(); // 3
		op.markSubTaskCompleted(); // 2
		op.undoMarkSubTaskCompleted(); // 3 (retry case)
		op.markSubTaskCompleted(); // 2
		op.markSubTaskCompleted(); // 1
		op.markSubTaskCompleted(); // 0
		assertTrue(op.allSubOperationsDone());
	}

	// ---------- Concurrency tests (Concurrent Map Corruption) ----------

	@Test
	void concurrentPutOperations_preservesAllEntries() throws Exception {
		var op = newCompositeOp(OpType.CREATE, 4096, 1024);

		// Use reflection to access the contextData field
		Field contextDataField = CompositeDataOperationImpl.class.getDeclaredField("contextData");
		contextDataField.setAccessible(true);

		int threadCount = 20;
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);

		try {
			List<CompletableFuture<Void>> futures = new ArrayList<>();

			// Spawn 20 threads to concurrently put entries
			for (int i = 0; i < threadCount; i++) {
				final int threadId = i;
				String eTag = "eTag-" + threadId;
				CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
					op.put(String.valueOf(threadId), eTag);
				}, executor);
				futures.add(future);
			}

			// Wait for all threads to complete
			CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
					.get(10, TimeUnit.SECONDS);

			// Verify that all entries were preserved (no data loss)
			@SuppressWarnings("unchecked")
			Map<String, String> contextData = (Map<String, String>) contextDataField.get(op);
			assertEquals(threadCount, contextData.size(), "All concurrent puts should be preserved");
			for (int i = 0; i < threadCount; i++) {
				String expectedEtag = "eTag-" + i;
				assertEquals(expectedEtag, op.get(String.valueOf(i)),
						"Entry " + i + " should have correct eTag");
			}
		} finally {
			executor.shutdown();
			executor.awaitTermination(5, TimeUnit.SECONDS);
		}
	}
}
