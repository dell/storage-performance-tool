package com.dell.spt.base.nic.pool;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The engine's own read of an item file (D54, D54-R1), against the fixtures the CLI's reader shares. */
final class ItemSizeScanTest {

	private static final Path FIXTURES = Path.of("../../../test-fixtures/pool-demand");

	@SuppressWarnings("unchecked")
	private static final ItemFactory<DataItem> ITEMS = (ItemFactory<DataItem>) ItemType.getItemFactory(ItemType.DATA);

	private static ItemSizeSummary scan(final Path file) {
		return ItemSizeScan.scan(file.toString(), ITEMS, System.nanoTime() + 60_000_000_000L, System::nanoTime);
	}

	@Test
	void everySharedFixtureGivesTheExpectedSummary() throws Exception {
		final var expected = new ObjectMapper().readTree(FIXTURES.resolve("expected-summaries.json").toFile()).get("files");
		final var names = new ArrayList<String>();
		expected.fieldNames().forEachRemaining(names::add);
		assertFalse(names.isEmpty());
		for (final var name : names) {
			final var want = expected.get(name);
			final var summary = scan(FIXTURES.resolve("item-files").resolve(name));
			assertEquals(want.get("complete").asBoolean(), summary.complete(), name + ": " + summary);
			if (want.has("reason")) {
				assertEquals(want.get("reason").asText(), summary.reason().name(), name);
			}
			if (summary.complete()) {
				assertEquals(want.get("items").asLong(), summary.items(), name);
				final var bounds = new ArrayList<ItemSizeSummary.Bounds>(summary.bounds().values());
				assertEquals(want.get("bounds").size(), bounds.size(), name + ": " + bounds);
				for (var i = 0; i < bounds.size(); i++) {
					assertEquals(want.get("bounds").get(i).get(0).asLong(), bounds.get(i).smallest(), name);
					assertEquals(want.get("bounds").get(i).get(1).asLong(), bounds.get(i).largest(), name);
				}
			} else {
				assertTrue(summary.bounds().isEmpty(), "an incomplete summary keeps no bounds to misuse");
				assertFalse(summary.why().isEmpty(), name);
			}
		}
	}

	@Test
	void aValidPrefixBeforeAMalformedLineNeverPassesForTheWholeFile(@TempDir final Path dir) throws Exception {
		final var file = dir.resolve("items.csv");
		final var lines = new StringBuilder();
		// A batch boundary: the bad line is the first of the scan's second batch.
		for (var i = 0; i < 4096; i++) {
			lines.append("obj-").append(i).append(",0,4194304,0/0\n");
		}
		lines.append("obj-x,0,bad,0/0\n");
		for (var i = 0; i < 10; i++) {
			lines.append("after-").append(i).append(",0,4194304,0/0\n");
		}
		Files.writeString(file, lines.toString());
		final var summary = scan(file);
		assertFalse(summary.complete());
		assertEquals(ItemSizeSummary.Reason.MALFORMED_ITEM, summary.reason());
		assertTrue(summary.why().contains("obj-x"), summary.why());
	}

	@Test
	void aBinaryItemFileIsReadAsTheWorkloadReadsIt(@TempDir final Path dir) throws Exception {
		final var file = dir.resolve("slice-bin");
		try (final var out = new ObjectOutputStream(Files.newOutputStream(file))) {
			final var item = new DataItemImpl("obj,0,4194304,0/0");
			out.writeUnshared(item);
			out.writeUnshared(new DataItemImpl("obj2,0,3145728,0/0"));
		}
		final var summary = scan(file);
		assertTrue(summary.complete(), summary::why);
		assertEquals(2, summary.items());
		assertEquals(3145728, summary.smallest());
		assertEquals(4194304, summary.largest());
	}

	@Test
	void aMissingFileIsAReadFailure(@TempDir final Path dir) {
		final var summary = scan(dir.resolve("gone.csv"));
		assertFalse(summary.complete());
		assertEquals(ItemSizeSummary.Reason.READ_FAILED, summary.reason());
	}

	@Test
	void anExpiredBudgetLeavesItIncomplete() {
		final var file = FIXTURES.resolve("item-files/one-class.csv");
		final long now = System.nanoTime();
		final var summary = ItemSizeScan.scan(file.toString(), ITEMS, now - 1, () -> now);
		assertFalse(summary.complete());
		assertEquals(ItemSizeSummary.Reason.BUDGET_EXPIRED, summary.reason());
	}

	@Test
	void aSummarySurvivesItsJsonForm() {
		final var recorder = new ItemSizeSummary.Recorder("items.csv");
		recorder.record(3 << 20);
		recorder.record(4 << 20);
		recorder.record(1L << 33);
		recorder.finish();
		final var summary = recorder.summary();
		final var decoded = ItemSizeSummary.decode(summary.encode());
		assertEquals(summary.bounds(), decoded.bounds());
		assertTrue(decoded.complete());
		assertEquals(3, decoded.items());

		final var failed = new ItemSizeSummary.Recorder("items.csv");
		failed.record(1 << 20);
		failed.fail(ItemSizeSummary.Reason.WRITE_FAILED, "gone");
		final var incomplete = ItemSizeSummary.decode(failed.summary().encode());
		assertFalse(incomplete.complete());
		assertEquals(ItemSizeSummary.Reason.WRITE_FAILED, incomplete.reason());
		assertEquals("gone", incomplete.detail());
		assertTrue(incomplete.bounds().isEmpty());
	}
}
