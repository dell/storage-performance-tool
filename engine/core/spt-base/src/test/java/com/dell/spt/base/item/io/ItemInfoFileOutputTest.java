package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.storage.Credential;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ItemInfoFileOutputTest {

	@TempDir
	Path tempDir;

	private static Operation<Item> op(final OpType type, final Item item, final String dstPath) {
		return new OperationImpl<>(0, type, item, null, dstPath, Credential.NONE);
	}

	private static IntegrityManifestDataItem manifestItem(final String bucket, final String key) {
		return new IntegrityManifestDataItem(bucket, key, 1024, "");
	}

	/** Writes each result with the single-result put, then the poison, and returns the file's rows. */
	private List<String> putEach(final List<Operation<Item>> results) throws Exception {
		final Path file = tempDir.resolve("single.csv");
		final var output = new ItemInfoFileOutput<Operation<Item>>(file);
		for (final var result : results) {
			assertTrue(output.put(result));
		}
		assertTrue(output.put((Operation<Item>) null));
		return Files.readAllLines(file);
	}

	@Test
	void manifestReadAndDeleteRowsNameTheManifestBucket() throws Exception {
		final var rows = putEach(List.of(
						op(OpType.READ, manifestItem("src", "k1"), null),
						op(OpType.READ, manifestItem("src", "dir/k2"), null),
						op(OpType.DELETE, manifestItem("src", "k1"), null),
						op(OpType.DELETE, manifestItem("src", "dir/k2"), null),
						op(OpType.STAT, manifestItem("/src", "k1"), null)));
		assertEquals(
						List.of(
										"/src/k1,0,1024,0/0",
										"/src/dir/k2,0,1024,0/0",
										"/src/k1,0,1024,0/0",
										"/src/dir/k2,0,1024,0/0",
										"/src/k1,0,1024,0/0"),
						rows);
	}

	@Test
	void manifestReadIgnoresTheConfiguredOutputPath() throws Exception {
		final var rows = putEach(List.of(op(OpType.READ, manifestItem("src", "k1"), "/other")));
		assertEquals(List.of("/src/k1,0,1024,0/0"), rows);
	}

	@Test
	void manifestCreateRowsNameTheOutputPath() throws Exception {
		final var rows = putEach(List.of(
						op(OpType.CREATE, manifestItem("src", "k1"), "/dst"),
						op(OpType.CREATE, manifestItem("src", "dir/k2"), "/dst/")));
		assertEquals(List.of("/dst/k1,0,1024,0/0", "/dst/dir/k2,0,1024,0/0"), rows);
	}

	@Test
	void manifestCreateWithoutAnOutputPathKeepsTheBareKey() throws Exception {
		final var rows = putEach(List.of(op(OpType.CREATE, manifestItem("src", "k1"), null)));
		assertEquals(List.of("k1,0,1024,0/0"), rows);
	}

	@Test
	void rowBuildingLeavesTheManifestItemNameAlone() throws Exception {
		final var item = manifestItem("src", "dir/k2");
		putEach(List.of(op(OpType.READ, item, null)));
		assertEquals("dir/k2", item.name());
		assertEquals("dir/k2,0,1024,0/0", item.toString());
	}

	@Test
	void plainItemRowsAreUnchanged() throws Exception {
		final var created = new DataItemImpl("/dst/k9", 0x10, 2048);
		final var read = new DataItemImpl("/src/k1", 0, 1024);
		final var readOp = op(OpType.READ, read, "/other");
		final String readRow = read.toString();
		final var rows = putEach(List.of(op(OpType.CREATE, created, "/dst"), readOp));
		assertEquals(List.of("/dst/k9,10,2048,0/0", readRow), rows);
		assertEquals("k1,0,1024,0/0", readRow);
	}

	@Test
	void listPutWritesRowsUpToThePoisonAndCloses() throws Exception {
		final Path file = tempDir.resolve("range.csv");
		final var output = new ItemInfoFileOutput<Operation<Item>>(file);
		final List<Operation<Item>> batch = List.of(
						op(OpType.READ, new DataItemImpl("/ignored/x", 0, 1), null),
						op(OpType.READ, manifestItem("src", "k1"), null),
						op(OpType.CREATE, manifestItem("src", "dir/k2"), "/dst"),
						op(OpType.CREATE, new DataItemImpl("/dst/k3", 0, 8), "/dst"),
						op(OpType.READ, new DataItemImpl("/ignored/y", 0, 1), null));
		assertEquals(3, output.put(batch, 1, 4));
		final List<Operation<Item>> poisoned = new ArrayList<>(Arrays.asList(
						op(OpType.DELETE, manifestItem("src", "dir/k2"), "/other"),
						null,
						op(OpType.READ, manifestItem("src", "after"), null)));
		assertEquals(1, output.put(poisoned, 0, 3));
		assertEquals(
						List.of(
										"/src/k1,0,1024,0/0",
										"/dst/dir/k2,0,1024,0/0",
										"/dst/k3,0,8,0/0",
										"/src/dir/k2,0,1024,0/0"),
						Files.readAllLines(file));
	}

	@Test
	void wholeListPutWritesRowsUpToThePoisonAndCloses() throws Exception {
		final Path file = tempDir.resolve("list.csv");
		final var output = new ItemInfoFileOutput<Operation<Item>>(file);
		assertEquals(2, output.put(List.of(
						op(OpType.DELETE, manifestItem("src", "dir/k2"), null),
						op(OpType.CREATE, new DataItemImpl("/dst/k3", 0, 8), "/dst"))));
		final List<Operation<Item>> poisoned = new ArrayList<>(Arrays.asList(
						op(OpType.READ, manifestItem("src", "k1"), "/other"), null));
		assertEquals(1, output.put(poisoned));
		assertEquals(
						List.of("/src/dir/k2,0,1024,0/0", "/dst/k3,0,8,0/0", "/src/k1,0,1024,0/0"),
						Files.readAllLines(file));
	}
}
