package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.storage.Credential;
import java.io.IOException;
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

	/** As a plain item's row does, so the filesystem driver reads back the file it made under it. */
	@Test
	void manifestCreateRowsKeepARelativeOutputPathAsWritten() throws Exception {
		final var rows = putEach(List.of(
						op(OpType.CREATE, manifestItem("src", "k1"), "dst"),
						op(OpType.CREATE, manifestItem("src", "dst/k2"), "dst"),
						op(OpType.CREATE, manifestItem("src", "dir/k3"), "dst/"),
						op(OpType.CREATE, manifestItem("src", "/dst/k4"), "dst")));
		assertEquals(
						List.of("dst/k1,0,1024,0/0", "dst/k2,0,1024,0/0", "dst/dir/k3,0,1024,0/0", "/dst/k4,0,1024,0/0"),
						rows);
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

	private static Operation<Item> plainOp(final String name) {
		return op(OpType.READ, new DataItemImpl(name, 0, 1), null);
	}

	/** The poison's index is absolute; the rows before it in the range are written, then the file closes. */
	@Test
	void rangePutFromAnOffsetWritesRowsUpToThePoisonAndCloses() throws Exception {
		final Path file = tempDir.resolve("offset.csv");
		final var output = new ItemInfoFileOutput<Operation<Item>>(file);
		final List<Operation<Item>> batch = Arrays.asList(
						plainOp("/b/r0"), plainOp("/b/r1"), plainOp("/b/r2"), null, plainOp("/b/r4"));
		assertEquals(2, output.put(batch, 1, 4));
		assertEquals(List.of("r1,0,1,0/0", "r2,0,1,0/0"), Files.readAllLines(file));
		assertThrows(IOException.class, () -> output.put(plainOp("/b/late")));
	}

	@Test
	void rangePutFromAnOffsetHoldingOnlyThePoisonWritesNothingAndCloses() throws Exception {
		final Path file = tempDir.resolve("poison-only.csv");
		final var output = new ItemInfoFileOutput<Operation<Item>>(file);
		final List<Operation<Item>> batch = Arrays.asList(plainOp("/b/r0"), plainOp("/b/r1"), null);
		assertEquals(0, output.put(batch, 2, 3));
		assertEquals(List.of(), Files.readAllLines(file));
		assertThrows(IOException.class, () -> output.put(plainOp("/b/late")));
	}

	@Test
	void rangePutFromZeroWritesRowsUpToThePoisonAndCloses() throws Exception {
		final Path file = tempDir.resolve("zero.csv");
		final var output = new ItemInfoFileOutput<Operation<Item>>(file);
		final List<Operation<Item>> batch = Arrays.asList(plainOp("/b/r0"), plainOp("/b/r1"), null);
		assertEquals(2, output.put(batch, 0, 3));
		assertEquals(List.of("r0,0,1,0/0", "r1,0,1,0/0"), Files.readAllLines(file));
		assertThrows(IOException.class, () -> output.put(plainOp("/b/late")));
	}
}
