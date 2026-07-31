package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.item.PathItemImpl;
import com.dell.spt.base.item.op.list.ListOperationImpl;
import com.dell.spt.base.item.op.list.ListedObject;
import java.util.List;
import com.dell.spt.base.storage.Credential;
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
	void listWritesEveryDiscoveredObjectRatherThanTheSeed() throws Exception {
		final Path manifest = tempDir.resolve("verify-input.csv");
		final var op = new ListOperationImpl<PathItemImpl>(
						0, OpType.LIST, new PathItemImpl("prefix"), Credential.NONE);
		op.listedObjects(List.of(new ListedObject("prefix/a", 7), new ListedObject("prefix/b", 8)));
		try (final var output = new IntegrityOperationManifestOutput<>(manifest, "/bucket", OpType.LIST)) {
			output.put(op);
		}

		final var records = CSVFormat.RFC4180.parse(java.nio.file.Files.newBufferedReader(manifest)).getRecords();
		assertEquals(3, records.size());
		assertEquals("prefix/a", records.get(1).get(1));
		assertEquals("7", records.get(1).get(2));
		assertEquals("prefix/b", records.get(2).get(1));
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
}
