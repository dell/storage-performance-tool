package com.dell.spt.base.load.step.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.dell.spt.base.control.run.RunTerminalException;
import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.ItemType;
import com.dell.spt.base.item.io.ItemInputFactory;
import com.dell.spt.base.load.step.file.FileManager;
import com.dell.spt.base.nic.pool.ItemSizeRecording;
import com.dell.spt.base.nic.pool.ItemSizeSummary;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.confuse.Config;
import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The Entry Node's record of each slice's item sizes (D54, D54-R1, D54-R2). */
final class ItemInputFileSlicerSizesTest {

	@SuppressWarnings("unchecked")
	private static final ItemFactory<DataItem> ITEMS = (ItemFactory<DataItem>) ItemType.getItemFactory(ItemType.DATA);

	private static final ItemSizeRecording NO_LIMIT = new ItemSizeRecording(Long.MAX_VALUE, () -> 0L);

	/** A file manager that keeps what it is given, as a worker's would. */
	private static final class Slices {
		final Map<String, java.io.ByteArrayOutputStream> written = new ConcurrentHashMap<>();
		int nextName;

		FileManager manager() throws IOException {
			final FileManager manager = mock(FileManager.class);
			final String name = "slice-" + nextName++;
			when(manager.newTmpFileName()).thenReturn(name);
			org.mockito.Mockito.doAnswer(call -> {
				written.computeIfAbsent(call.getArgument(0), ignored -> new java.io.ByteArrayOutputStream())
								.writeBytes(call.getArgument(1));
				return null;
			}).when(manager).writeToFile(anyString(), any());
			return manager;
		}
	}

	private static Config slice(final String... interfaces) {
		final Config config = TestConfigBuilder.config();
		config.val("storage-driver-type", "s3-rdma");
		config.val("load-op-type", "read");
		config.val("item-type", "data");
		config.val("storage-net-interfaces", List.of(interfaces));
		return config;
	}

	private static ItemSizeSummary summaryOf(final Config slice) {
		return ItemSizeSummary.decode(slice.stringVal(ItemSizeSummary.CONFIG_PATH));
	}

	private static boolean hasSummary(final Config slice) {
		try {
			return slice.stringVal(ItemSizeSummary.CONFIG_PATH) != null;
		} catch (final RuntimeException absent) {
			return false;
		}
	}

	private static Input<DataItem> csv(final Path dir, final String content) throws IOException {
		final var file = dir.resolve("items.csv");
		Files.writeString(file, content);
		return ItemInputFactory.createFileItemInput(ITEMS, file.toString());
	}

	private static String lines(final long... sizes) {
		final var out = new StringBuilder();
		for (var i = 0; i < sizes.length; i++) {
			out.append("obj-").append(i).append(",0,").append(sizes[i]).append(",0/0\n");
		}
		return out.toString();
	}

	@Test
	void eachSliceHasTheSizesOfTheItemsWrittenToIt(@TempDir final Path dir) throws Exception {
		final var slices = new Slices();
		final var configs = List.of(slice("a"), slice("b"));
		try (final var input = csv(dir, lines(4 << 20, 1L << 30, 4 << 20, 1L << 30));
						final var slicer = new ItemInputFileSlicer("step", List.of(slices.manager(), slices.manager()), configs, input, 2, false, NO_LIMIT)) {
			final var first = summaryOf(configs.get(0));
			final var second = summaryOf(configs.get(1));
			assertTrue(first.complete(), first::why);
			assertEquals(2, first.items());
			assertEquals(4 << 20, first.largest());
			assertTrue(second.complete(), second::why);
			assertEquals(1L << 30, second.smallest());
			assertEquals(1L << 30, second.largest());
		}
	}

	@Test
	void aMalformedRecordAtTheStartWithinABatchOrAtABatchBoundaryEndsTheRunNamingItsLine(@TempDir final Path dir) throws Exception {
		final String good = "good,0,4194304,0/0\n";
		for (final var content : List.of(
						"bad,0,oops,0/0\n" + good + good,
						good + "bad,0,oops,0/0\n" + good,
						good + good + "bad,0,oops,0/0\n" + good)) {
			final var slices = new Slices();
			final var configs = List.of(slice("a"));
			try (final var input = csv(dir, content)) {
				final var failure = assertThrows(RunTerminalException.class,
								() -> new ItemInputFileSlicer("step", List.of(slices.manager()), configs, input, 2, false, NO_LIMIT));
				assertTrue(failure.getMessage().contains("is not a valid item"), failure.getMessage());
			}
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void aReadFailureAfterAValidPrefixEndsTheRun() throws Exception {
		final var slices = new Slices();
		final var configs = List.of(slice("a"));
		final Input<DataItem> input = mock(Input.class, org.mockito.Mockito.withSettings().extraInterfaces(com.github.akurilov.commons.io.file.FileInput.class));
		final var calls = new AtomicLong();
		when(input.get(any(), org.mockito.ArgumentMatchers.anyInt())).thenAnswer(call -> {
			if (calls.getAndIncrement() == 0) {
				((List<DataItem>) call.getArgument(0)).add(new DataItemImpl("good,0,4194304,0/0"));
				return 1;
			}
			throw new IOException("disk gone");
		});
		assertThrows(RunTerminalException.class, () -> new ItemInputFileSlicer("step", List.of(slices.manager()), configs, input, 1, false, NO_LIMIT));
	}

	@Test
	void aFailedSliceWriteEndsTheRun(@TempDir final Path dir) throws Exception {
		final var slices = new Slices();
		final var failing = slices.manager();
		doThrow(new IOException("worker gone")).when(failing).writeToFile(anyString(), any());
		final var configs = List.of(slice("a"), slice("b"));
		try (final var input = csv(dir, lines(4 << 20, 4 << 20))) {
			assertThrows(RunTerminalException.class,
							() -> new ItemInputFileSlicer("step", List.of(failing, slices.manager()), configs, input, 2, false, NO_LIMIT));
		}
	}

	@Test
	void anExpiredBudgetGivesUnknownWhileTheDistributionFinishesAndDropsNoRecord(@TempDir final Path dir) throws Exception {
		final var slices = new Slices();
		final var configs = List.of(slice("a"));
		final var time = new AtomicLong();
		// The budget ends once the first batch has been scattered.
		final var recording = new ItemSizeRecording(1, time::get);
		final var manager = slices.manager();
		org.mockito.Mockito.doAnswer(call -> {
			time.set(10);
			slices.written.computeIfAbsent(call.getArgument(0), ignored -> new java.io.ByteArrayOutputStream()).writeBytes(call.getArgument(1));
			return null;
		}).when(manager).writeToFile(anyString(), any());
		try (final var input = csv(dir, lines(4 << 20, 4 << 20, 4 << 20, 4 << 20, 4 << 20));
						final var slicer = new ItemInputFileSlicer("step", List.of(manager), configs, input, 1, false, recording)) {
			final var summary = summaryOf(configs.get(0));
			assertFalse(summary.complete());
			assertEquals(ItemSizeSummary.Reason.BUDGET_EXPIRED, summary.reason());
			assertEquals(5, readItems(slices.written.get("slice-0").toByteArray()), "every workload record was written");
		}
	}

	/** Only the check after the last write sees this: the file ends without another batch. */
	@Test
	void theLastWriteCrossingTheBudgetLeavesTheSummaryUnknown(@TempDir final Path dir) throws Exception {
		final var slices = new Slices();
		final var configs = List.of(slice("a"));
		final var time = new AtomicLong();
		final var recording = new ItemSizeRecording(1, time::get);
		final var manager = slices.manager();
		org.mockito.Mockito.doAnswer(call -> {
			time.set(10);
			slices.written.computeIfAbsent(call.getArgument(0), ignored -> new java.io.ByteArrayOutputStream()).writeBytes(call.getArgument(1));
			return null;
		}).when(manager).writeToFile(anyString(), any());
		try (final var input = csv(dir, lines(4 << 20));
						final var slicer = new ItemInputFileSlicer("step", List.of(manager), configs, input, 1, false, recording)) {
			final var summary = summaryOf(configs.get(0));
			assertFalse(summary.complete());
			assertEquals(ItemSizeSummary.Reason.BUDGET_EXPIRED, summary.reason());
			assertEquals(1, readItems(slices.written.get("slice-0").toByteArray()), "the workload record was written");
		}
	}

	/** A mixed step's slices record by its GET weight, which this schema does not have, whatever its op type. */
	@Test
	void aMixedStepsSliceDoesNotRecordByItsOpType(@TempDir final Path dir) throws Exception {
		final var slices = new Slices();
		final var configs = List.of(slice("a"));
		try (final var input = csv(dir, lines(4 << 20));
						final var slicer = new ItemInputFileSlicer("step", List.of(slices.manager()), configs, input, 1, false, NO_LIMIT.asMixed())) {
			assertFalse(hasSummary(configs.get(0)));
		}
	}

	private static int readItems(final byte[] bytes) throws Exception {
		final var items = new ArrayList<Object>();
		try (final var in = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
			while (true) {
				items.add(in.readUnshared());
			}
		} catch (final EOFException end) {
			return items.size();
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void aDistributionFencedByTheDeadlinePublishesNoSummary() throws Exception {
		final var slices = new Slices();
		final var configs = List.of(slice("a"));
		final Input<DataItem> input = mock(Input.class);
		when(input.get(any(), org.mockito.ArgumentMatchers.anyInt())).thenAnswer(call -> {
			Thread.currentThread().interrupt();
			return 0;
		});
		try {
			assertThrows(InterruptedException.class,
							() -> new ItemInputFileSlicer("step", List.of(slices.manager()), configs, input, 1, false, NO_LIMIT));
		} finally {
			Thread.interrupted();
		}
		assertFalse(hasSummary(configs.get(0)), "a summary that arrives after the deadline is never used");
	}

	@Test
	void aWorkerWithANicSetGetsASummaryWhileTheEntryNodesValueIsNone(@TempDir final Path dir) throws Exception {
		final var slices = new Slices();
		final var entry = slice();
		final var worker = slice("mlx5_0");
		try (final var input = csv(dir, lines(4 << 20, 4 << 20));
						final var slicer = new ItemInputFileSlicer("step", List.of(slices.manager(), slices.manager()), List.of(entry, worker), input, 2, false, NO_LIMIT)) {
			assertFalse(hasSummary(entry));
			assertTrue(summaryOf(worker).complete());
		}
	}

	@Test
	void withoutARecordingNoSummaryIsAdded(@TempDir final Path dir) throws Exception {
		final var slices = new Slices();
		final var withNicSet = slice("mlx5_0");
		try (final var input = csv(dir, lines(4 << 20));
						final var slicer = new ItemInputFileSlicer("step", List.of(slices.manager()), List.of(withNicSet), input, 1)) {
			assertFalse(hasSummary(withNicSet), "no NIC set on any participant: no recording is made");
		}
	}

	@Test
	void anS3DriverOrADeleteGetsNoSummary(@TempDir final Path dir) throws Exception {
		final var s3 = slice("eth0");
		s3.val("storage-driver-type", "s3");
		final var delete = slice("mlx5_0");
		delete.val("load-op-type", "delete");
		final var slices = new Slices();
		try (final var input = csv(dir, lines(4 << 20, 4 << 20));
						final var slicer = new ItemInputFileSlicer("step", List.of(slices.manager(), slices.manager()), List.of(s3, delete), input, 2, false, NO_LIMIT)) {
			assertFalse(hasSummary(s3));
			assertFalse(hasSummary(delete));
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	void aBucketListingGivesAnUnknownSummary() throws Exception {
		final var slices = new Slices();
		final var configs = List.of(slice("a"));
		final Input<DataItem> listing = mock(Input.class);
		when(listing.get(any(), org.mockito.ArgumentMatchers.anyInt())).thenAnswer(call -> {
			throw new java.io.EOFException();
		});
		try (final var slicer = new ItemInputFileSlicer("step", List.of(slices.manager()), configs, listing, 1, false, NO_LIMIT)) {
			final var summary = summaryOf(configs.get(0));
			assertFalse(summary.complete());
			assertEquals(ItemSizeSummary.Reason.NOT_A_FILE, summary.reason());
		}
	}

	@Test
	void aCanonicalManifestIsRecordedInStrictMode(@TempDir final Path dir) throws Exception {
		final var file = dir.resolve("manifest.csv");
		Files.writeString(file, "bucket,key,size,version_id\nb,k1,4194304,\nb,k2,3145728,\n");
		final var slices = new Slices();
		final var configs = List.of(slice("a"));
		try (final var input = ItemInputFactory.createFileItemInput(ITEMS, file.toString());
						final var slicer = new ItemInputFileSlicer("step", List.of(slices.manager()), configs, input, 2, true, NO_LIMIT)) {
			final var summary = summaryOf(configs.get(0));
			assertTrue(summary.complete(), summary::why);
			assertEquals(2, summary.items());
		}
	}
}
