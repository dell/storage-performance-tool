package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.dell.spt.base.item.IntegrityManifestDataItem;
import java.io.EOFException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BucketRotatingManifestInputTest {
	private static final String HEADER = "bucket,key,size,version_id\n";

	@TempDir
	Path temp;

	@Test
	void takesOneRowFromEachBucketInTurnUntilEachBucketEnds() throws Exception {
		final var input = rotating(HEADER
						+ "a,a0,0,\n" + "a,a1,1,\n" + "a,a2,2,\n" + "a,a3,3,\n"
						+ "b,b0,4,\n"
						+ "c,c0,5,\n" + "c,c1,6,\n");

		assertEquals(
						List.of("a0@0", "b0@4", "c0@5", "a1@1", "c1@6", "a2@2", "a3@3"),
						readAll(input));
		assertNull(input.get());
		input.close();
	}

	@Test
	void eachRowKeepsItsFieldsAndItsFilePositionWhateverTheRowBeforeItHolds() throws Exception {
		final var input = rotating(HEADER
						+ "a,\"comma,key\",1,\n"
						+ "a,\"quote\"\"key\",2,v-1\n"
						+ "a,\"line\nbreak\",3,\n"
						+ "a,ключ-日本語-🙂,4,\n"
						+ "é-bucket,second,5,\n"
						+ "é-bucket,\"third,日本\",6,v-2\n"
						+ "z,last,7,\n");

		final var items = new ArrayList<IntegrityManifestDataItem>();
		for (var item = input.get(); item != null; item = input.get()) {
			items.add(item);
		}
		input.close();

		assertEquals(
						List.of(
										"a|comma,key|1|null|0",
										"é-bucket|second|5|null|4",
										"z|last|7|null|6",
										"a|quote\"key|2|v-1|1",
										"é-bucket|third,日本|6|v-2|5",
										"a|line\nbreak|3|null|2",
										"a|ключ-日本語-🙂|4|null|3"),
						items.stream()
										.map(item -> item.bucket() + '|' + item.name() + '|' + item.size() + '|'
														+ item.versionId() + '|' + item.selectionIndex())
										.toList());
	}

	@Test
	void readsLinesEndedByCarriageReturnAndLineFeed() throws Exception {
		final var input = rotating(
						"bucket,key,size,version_id\r\n" + "a,a0,0,\r\n" + "a,a1,1,\r\n" + "b,b0,2,\r\n");

		assertEquals(List.of("a0@0", "b0@2", "a1@1"), readAll(input));
		input.close();
	}

	@Test
	void remainingCountFollowsEveryWayOfReading() throws Exception {
		final var input = rotating(HEADER + "a,a0,0,\n" + "a,a1,1,\n" + "a,a2,2,\n" + "b,b0,3,\n" + "b,b1,4,\n");
		assertEquals(5, input.remainingItemCount());

		assertEquals(0, input.get().selectionIndex());
		assertEquals(4, input.remainingItemCount());

		final var buffer = new ArrayList<IntegrityManifestDataItem>();
		assertEquals(2, input.get(buffer, 2));
		assertEquals(List.of(3L, 1L), buffer.stream().map(IntegrityManifestDataItem::selectionIndex).toList());
		assertEquals(2, input.remainingItemCount());

		assertEquals(1, input.skip(1));
		assertEquals(1, input.remainingItemCount());

		assertEquals(1, input.skip(9));
		assertEquals(0, input.remainingItemCount());
		input.close();
	}

	@Test
	void aBufferedReadAtTheEndReportsEndOfFileAsTheManifestInputDoes() throws Exception {
		final var input = rotating(HEADER + "a,a0,0,\n" + "b,b0,1,\n");
		final var buffer = new ArrayList<IntegrityManifestDataItem>();

		assertEquals(2, input.get(buffer, 8));
		assertThrows(EOFException.class, () -> input.get(buffer, 8));
		input.close();
	}

	@Test
	void resetStartsAgainFromTheFirstRowOfEachBucket() throws Exception {
		final var input = rotating(HEADER + "a,a0,0,\n" + "a,a1,1,\n" + "b,b0,2,\n");
		assertEquals(0, input.get().selectionIndex());
		assertEquals(2, input.get().selectionIndex());

		input.reset();

		assertEquals(3, input.remainingItemCount());
		assertEquals(List.of("a0@0", "b0@2", "a1@1"), readAll(input));
		input.close();
	}

	@Test
	void closeLeavesNoRowsToReadAndCanBeRepeated() throws Exception {
		final var input = rotating(HEADER + "a,a0,0,\n" + "b,b0,1,\n");

		input.close();
		input.close();

		assertNull(input.get());
	}

	@Test
	void reportsThePathItReadsFrom() throws Exception {
		final var input = rotating(HEADER + "a,a0,0,\n" + "b,b0,1,\n");

		assertEquals(temp.resolve("manifest.csv"), input.filePath());
		input.close();
	}

	@Test
	void oneBucketKeepsTheManifestInput() throws Exception {
		final var source = source(HEADER + "a,a0,0,\n" + "a,a1,1,\n");

		assertSame(source, BucketRotatingManifestInput.across(source));

		assertEquals(0, source.remainingItemCount() - 2);
		assertEquals(-1, source.get().selectionIndex());
		source.close();
	}

	@Test
	void anEmptyManifestKeepsTheManifestInput() throws Exception {
		final var source = source(HEADER);

		assertSame(source, BucketRotatingManifestInput.across(source));
		source.close();
	}

	@Test
	void theLargestBucketSetIsRotated() throws Exception {
		final var rows = new StringBuilder(HEADER);
		for (int i = 0; i < BucketRotatingManifestInput.MAX_BUCKETS; i++) {
			rows.append("bucket-").append(i).append(",key,1,\n");
		}

		final var input = rotating(rows.toString());

		assertEquals(BucketRotatingManifestInput.MAX_BUCKETS, input.remainingItemCount());
		assertEquals(BucketRotatingManifestInput.MAX_BUCKETS, readAll(input).size());
		input.close();
	}

	@Test
	void rowsThatChangeBucketTooOftenKeepTheManifestInput() throws Exception {
		final var rows = new StringBuilder(HEADER);
		for (int i = 0; i <= BucketRotatingManifestInput.MAX_BUCKETS; i++) {
			rows.append(i % 2 == 0 ? "a" : "b").append(",key-").append(i).append(",1,\n");
		}
		final var source = source(rows.toString());

		assertSame(source, BucketRotatingManifestInput.across(source));
		assertEquals("key-0", source.get().name());
		source.close();
	}

	@Test
	void aBucketThatComesBackIsReadFromBothPlaces() throws Exception {
		final var input = rotating(HEADER + "a,a0,0,\n" + "b,b0,1,\n" + "a,a1,2,\n");

		assertEquals(List.of("a0@0", "b0@1", "a1@2"), readAll(input));
		input.close();
	}

	@Test
	void aRowDamagedAfterStartupFailsWithTheManifestPathAndLeavesCloseWorking() throws Exception {
		final var input = rotating(HEADER + "a,a0,0,\n" + "b,b0,1,\n" + "b,b1,2,\n");
		final Path manifest = temp.resolve("manifest.csv");
		// Same length, so each reader still starts at its row: the first row of bucket b is now bad.
		Files.writeString(manifest, HEADER + "a,a0,0,\n" + "b,b0,x,\n" + "b,b1,2,\n");

		input.reset();
		assertEquals(0, input.get().selectionIndex());
		final var failure = assertThrows(IllegalArgumentException.class, input::get);
		assertEquals(true, failure.getMessage().contains(manifest.toString()));

		input.close();
	}

	private BucketRotatingManifestInput rotating(final String manifest) throws Exception {
		return assertInstanceOf(
						BucketRotatingManifestInput.class, BucketRotatingManifestInput.across(source(manifest)));
	}

	private IntegrityManifestItemInput source(final String manifest) throws Exception {
		final Path path = temp.resolve("manifest.csv");
		Files.writeString(path, manifest, StandardCharsets.UTF_8);
		return new IntegrityManifestItemInput(path);
	}

	private static List<String> readAll(final BucketRotatingManifestInput input) {
		final var rows = new ArrayList<String>();
		for (var item = input.get(); item != null; item = input.get()) {
			rows.add(item.name() + '@' + item.selectionIndex());
		}
		return rows;
	}
}
