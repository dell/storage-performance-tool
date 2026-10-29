package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.integrity.IntegrityManifestCompletion;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.item.op.list.ListedObject;
import java.util.List;
import com.dell.spt.base.storage.Credential;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class IntegrityOperationManifestOutputTest {

	@TempDir
	Path tempDir;

	@Test
	void createUsesReturnedVersionAndPreservesQuotedKey() throws Exception {
		final Path manifest = tempDir.resolve("written.csv");
		final var item = new DataItemImpl("/bucket/a,b\nline", 0, 13);
		final var op = new OperationImpl<>(0, OpType.CREATE, item, null, "/bucket", Credential.NONE);
		op.returnedVersionId("returned-v");
		try (final var output = new IntegrityOperationManifestOutput<>(manifest, "/bucket", OpType.CREATE)) {
			output.put(op);
		}

		final var records = CSVFormat.RFC4180.parse(java.nio.file.Files.newBufferedReader(manifest)).getRecords();
		assertEquals(IntegrityManifestItemInput.HEADER, records.get(0).toList());
		assertEquals("bucket", records.get(1).get(0));
		assertEquals("a,b\nline", records.get(1).get(1));
		assertEquals("returned-v", records.get(1).get(3));
	}

	@Test
	void createRecordsTheMemberBucketEachOperationAddressed() throws Exception {
		final Path manifest = tempDir.resolve("written.csv");
		try (final var output = new IntegrityOperationManifestOutput<>(manifest, "/bench", OpType.CREATE)) {
			for (final var member : List.of("/bench-2/prefix", "/bench-0/prefix", "/bench-2/prefix")) {
				final var item = new DataItemImpl(member + "/key" + member.charAt(7), 0, 3);
				final var op = new OperationImpl<>(0, OpType.CREATE, item, null, member, Credential.NONE);
				op.returnedVersionId("v" + member.charAt(7));
				output.put(op);
			}
		}

		final var records = CSVFormat.RFC4180.parse(java.nio.file.Files.newBufferedReader(manifest)).getRecords();
		assertEquals(List.of("bench-2", "prefix/key2", "3", "v2"), records.get(1).toList());
		assertEquals(List.of("bench-0", "prefix/key0", "3", "v0"), records.get(2).toList());
		assertEquals(List.of("bench-2", "prefix/key2", "3", "v2"), records.get(3).toList());
	}

	@Test
	void createRecordsTheListedBucketNamesAsWrittenWhateverThePlaceholder() throws Exception {
		final Path manifest = tempDir.resolve("written.csv");
		try (final var output = new IntegrityOperationManifestOutput<>(manifest, "/placeholder", OpType.CREATE)) {
			for (final var member : List.of("/ps-eu/prefix", "/PS_US/prefix", "/placeholder/prefix", "/ps-eu/prefix")) {
				final var item = new DataItemImpl(member + "/key", 0, 3);
				final var op = new OperationImpl<>(0, OpType.CREATE, item, null, member, Credential.NONE);
				output.put(op);
			}
		}

		final var records = CSVFormat.RFC4180.parse(java.nio.file.Files.newBufferedReader(manifest)).getRecords();
		assertEquals(List.of("ps-eu", "prefix/key", "3", ""), records.get(1).toList());
		assertEquals(List.of("PS_US", "prefix/key", "3", ""), records.get(2).toList());
		assertEquals(List.of("placeholder", "prefix/key", "3", ""), records.get(3).toList(),
						"a listed bucket that happens to equal the placeholder is recorded under its own name");
		assertEquals(List.of("ps-eu", "prefix/key", "3", ""), records.get(4).toList());
	}

	@Test
	void oneBucketKeepsTheConfiguredBucketAndKey() throws Exception {
		final Path manifest = tempDir.resolve("written.csv");
		try (final var output = new IntegrityOperationManifestOutput<>(manifest, "/bench", OpType.CREATE)) {
			output.put(new OperationImpl<>(0, OpType.CREATE, new DataItemImpl("/bench/p/k", 0, 1), null, "/bench/p",
							Credential.NONE));
			output.put(new OperationImpl<>(0, OpType.CREATE, new DataItemImpl("/bench/k2", 0, 1), null, null,
							Credential.NONE));
		}

		final var records = CSVFormat.RFC4180.parse(java.nio.file.Files.newBufferedReader(manifest)).getRecords();
		assertEquals(List.of("bench", "p/k", "1", ""), records.get(1).toList());
		assertEquals(List.of("bench", "k2", "1", ""), records.get(2).toList());
	}

	@Test
	void aBucketIsWithinTheConfiguredOneOnlyAsAWholeSegment() {
		assertTrue(IntegrityOperationManifestOutput.withinBucket("/bench", "bench"));
		assertTrue(IntegrityOperationManifestOutput.withinBucket("//bench/a", "bench"));
		assertTrue(IntegrityOperationManifestOutput.withinBucket("bench", "bench"));
		assertFalse(IntegrityOperationManifestOutput.withinBucket("/bench-2/a", "bench"));
		assertFalse(IntegrityOperationManifestOutput.withinBucket("/benc", "bench"));
		assertFalse(IntegrityOperationManifestOutput.withinBucket("/bench", ""));
	}

	@Test
	void createEmitsExactSharedLFManifestBytes() throws Exception {
		final Path manifest = tempDir.resolve("written.csv");
		final var item = new DataItemImpl("/b/k", 0, 1);
		final var op = new OperationImpl<>(0, OpType.CREATE, item, null, "/b", Credential.NONE);
		try (final var output = new IntegrityOperationManifestOutput<>(manifest, "/b", OpType.CREATE)) {
			output.put(op);
		}

		assertArrayEquals(
						Files.readAllBytes(sharedCompletionFixture("nonempty").resolve("verify-input.csv")),
						Files.readAllBytes(manifest));
	}

	@Test
	void listWritesEveryDiscoveredObjectRatherThanTheSeed() throws Exception {
		final Path manifest = tempDir.resolve("verify-input.csv");
		final var op = new ListOperationImpl<PathItemImpl>(
						0, OpType.LIST, new PathItemImpl("prefix"), Credential.NONE);
		op.listedObjects(List.of(new ListedObject("prefix/a", 7, "null"), new ListedObject("prefix/b", 8, "v2")));
		op.deleteMarkersListed(3);
		try (final var output = new IntegrityOperationManifestOutput<>(manifest, "/bucket", OpType.LIST)) {
			output.put(op);
		}
		assertEquals("2", java.nio.file.Files.readString(
						IntegrityManifestCompletion.emissionCountPath(manifest)).trim());
		assertEquals("3", java.nio.file.Files.readString(
						IntegrityManifestCompletion.deleteMarkerCountPath(manifest)).trim());

		final var records = CSVFormat.RFC4180.parse(java.nio.file.Files.newBufferedReader(manifest)).getRecords();
		assertEquals(3, records.size());
		assertEquals("prefix/a", records.get(1).get(1));
		assertEquals("7", records.get(1).get(2));
		assertEquals("null", records.get(1).get(3));
		assertEquals("prefix/b", records.get(2).get(1));
		assertEquals("v2", records.get(2).get(3));
	}

	@Test
	void listRejectsOutOfPrefixResponseBeforePublishingManifest() throws Exception {
		final Path manifest = tempDir.resolve("verify-input.csv");
		final var op = new ListOperationImpl<PathItemImpl>(
						0, OpType.LIST, new PathItemImpl("seed"), Credential.NONE);
		op.item().name("guarded/");
		op.listedObjects(List.of(
						new ListedObject("guarded/inside", 7, null),
						new ListedObject("outside/untrusted", 8, null)));
		try (final var output = new IntegrityOperationManifestOutput<>(
						manifest, "/bucket", OpType.LIST)) {
			final IOException failure = assertThrows(IOException.class, () -> output.put(op));
			assertTrue(failure.getMessage().contains("outside requested prefix"));
		}
		assertFalse(Files.exists(manifest));
		assertFalse(Files.exists(IntegrityManifestCompletion.emissionCountPath(manifest)));
		assertFalse(Files.exists(IntegrityManifestCompletion.deleteMarkerCountPath(manifest)));
	}

	@Test
	void listKeepsImmutableRootWhenShardPrefixChanges() throws Exception {
		final Path manifest = tempDir.resolve("verify-input.csv");
		final var op = new ListOperationImpl<PathItemImpl>(
						0, OpType.LIST, new PathItemImpl("outside/shard/"), Credential.NONE);
		op.listedObjects(List.of(new ListedObject("outside/shard/untrusted", 8, null)));
		try (final var output = new IntegrityOperationManifestOutput<>(
						manifest, "/bucket", OpType.LIST, "guarded/")) {
			final IOException failure = assertThrows(IOException.class, () -> output.put(op));
			assertTrue(failure.getMessage().contains("outside requested prefix"));
		}
		assertFalse(Files.exists(manifest));
		assertFalse(Files.exists(IntegrityManifestCompletion.emissionCountPath(manifest)));
	}

	@Test
	void readCopiesRequestedManifestIdentity() throws Exception {
		final Path manifest = tempDir.resolve("verified.csv");
		final var item = new IntegrityManifestDataItem("bucket", "key~literal", 5, "requested-v");
		final var op = new OperationImpl<>(0, OpType.READ, item, null, null, Credential.NONE);
		op.returnedVersionId("different-returned-v");
		try (final var output = new IntegrityOperationManifestOutput<>(manifest, "", OpType.READ)) {
			output.put(op);
		}

		final var records = CSVFormat.RFC4180.parse(java.nio.file.Files.newBufferedReader(manifest)).getRecords();
		assertEquals("key~literal", records.get(1).get(1));
		assertEquals("requested-v", records.get(1).get(3));
	}

	private static Path sharedCompletionFixture(final String variant) {
		Path cursor = Path.of("").toAbsolutePath();
		while (cursor != null) {
			final Path candidate = cursor.resolve(
							Path.of("testdata", "integrity", "completion-v1", variant));
			if (Files.isDirectory(candidate)) {
				return candidate;
			}
			cursor = cursor.getParent();
		}
		throw new AssertionError("shared completion fixture not found: " + variant);
	}
}
