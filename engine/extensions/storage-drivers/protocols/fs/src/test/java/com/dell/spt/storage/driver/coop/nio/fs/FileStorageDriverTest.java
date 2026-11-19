package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.commons.system.SizeInBytes;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.ToLongFunction;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

public class FileStorageDriverTest {

	private Path tempDir;
	private Config baseCfg;
	private DataInput dataInput;

	// Use CollectingOpOutput test utility instead of inline class

	@BeforeEach
	public void setUp() throws Exception {
		tempDir = Files.createTempDirectory("fstest-");
		baseCfg = TestConfigBuilder.config();
		// Keep threads minimal and queues small for tests
		baseCfg.val("load-batch-size", 16);
		baseCfg.val("storage-driver-limit-concurrency", 4);
		baseCfg.val("storage-driver-threads", 0);
		baseCfg.val("storage-driver-limit-queue-input", 128);
		dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false);
	}

	@AfterEach
	public void tearDown() throws Exception {
		if (dataInput != null)
			dataInput.close();
		// Clean up temp dir contents
		if (tempDir != null) {
			try (var paths = Files.walk(tempDir)) {
				paths.sorted((a, b) -> b.getNameCount() - a.getNameCount())
								.forEach(p -> {
									try {
										Files.deleteIfExists(p);
									} catch (IOException ignore) {}
								});
			}
		}
	}

	private FileStorageDriver<DataItemImpl, Operation<DataItemImpl>> newDriver(boolean verifyFlag) throws Exception {
		return new FileStorageDriver<>(
						"test-fs",
						dataInput,
						baseCfg.configVal("storage"),
						verifyFlag,
						baseCfg.intVal("load-batch-size"));
	}

	private static DataOperationImpl<DataItemImpl> newOp(
					OpType type, String name, long size, String srcPath, String dstPath) {
		DataItemImpl item = new DataItemImpl(name, 0, size);
		return new DataOperationImpl<>(0, type, item, srcPath, dstPath, null, null, 0);
	}

	@Test
	public void toStringContainsFsAndConcurrency() throws Exception {
		try (var drv = newDriver(false)) {
			String s = drv.toString();
			assertTrue(s.contains("/fs/"), "toString should contain /fs/");
			assertTrue(s.startsWith("storage/driver/"), "toString should start with storage/driver/");
		}
	}

	@Test
	public void noopOperationFinishes() throws Exception {
		try (var drv = newDriver(false)) {
			CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
			drv.operationResultOutput(out);
			drv.start();

			var op = newOp(OpType.NOOP, "noop.bin", 0, null, tempDir.toString());
			assertTrue(drv.put(op));
			Operation<DataItemImpl> res = out.await(2000);
			assertNotNull(res);
			assertEquals(Operation.Status.SUCC, res.status());
			drv.stop();
		}
	}

	@Test
	public void createWritesContentAndCreatesPath() throws Exception {
		try (var drv = newDriver(false)) {
			final String name = "create1.bin";
			final long size = 8192;
			final Path nestedDir = tempDir.resolve("nested");

			CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
			drv.operationResultOutput(out);
			drv.start();

			assertFalse(Files.exists(nestedDir));
			var op = newOp(OpType.CREATE, name, size, null, nestedDir.toString());
			assertTrue(drv.put(op));

			Operation<DataItemImpl> res = out.await(5000);
			assertNotNull(res);
			assertEquals(Operation.Status.SUCC, res.status());

			Path created = nestedDir.resolve(name);
			assertTrue(Files.exists(created));
			assertEquals(size, Files.size(created));

			drv.stop();
		}
	}

	@Test
	public void createFromSrcCopiesContent() throws Exception {
		final byte[] data = new byte[4096];
		for (int i = 0; i < data.length; i++)
			data[i] = (byte) (i & 0xFF);
		final String name = "copy.bin";
		final Path srcDir = tempDir.resolve("src");
		final Path dstDir = tempDir.resolve("dst");
		Files.createDirectories(srcDir);
		Files.createDirectories(dstDir);
		Files.write(srcDir.resolve(name), data);

		try (var drv = newDriver(false)) {
			CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
			drv.operationResultOutput(out);
			drv.start();

			var op = newOp(OpType.CREATE, name, data.length, srcDir.toString(), dstDir.toString());
			assertTrue(drv.put(op));
			Operation<DataItemImpl> res = out.await(5000);
			assertNotNull(res);
			assertEquals(Operation.Status.SUCC, res.status());
			assertArrayEquals(data, Files.readAllBytes(dstDir.resolve(name)));
			drv.stop();
		}
	}

	// Source bytes that differ from generated data, so a test passes only if the bytes are copied.
	private static byte[] sourceBytes(final int size, final int seed) {
		final byte[] data = new byte[size];
		for (int i = 0; i < data.length; i++)
			data[i] = (byte) ((i * 31 + seed) & 0xFF);
		return data;
	}

	// The destination of an item named by an absolute path: the output path plus the full name.
	private static Path underOutputPath(final Path dstDir, final String absoluteName) {
		return Path.of(dstDir.toString(), absoluteName);
	}

	private static void copyAndExpectSucc(
					final FileStorageDriver<DataItemImpl, Operation<DataItemImpl>> drv,
					final CollectingOpOutput<DataItemImpl> out, final DataItemImpl item, final Path srcDir,
					final Path dstDir) throws Exception {
		var op = new DataOperationImpl<>(0, OpType.CREATE, item, srcDir.toString(), dstDir.toString(), null, null, 0);
		assertTrue(drv.put(op));
		Operation<DataItemImpl> res = out.await(5000);
		assertNotNull(res);
		assertEquals(Operation.Status.SUCC, res.status(), item.name());
	}

	@Test
	public void createFromSrcCopiesItemsNamedByAbsolutePath() throws Exception {
		final Path srcDir = tempDir.resolve("src").resolve("nested");
		final Path dstDir = tempDir.resolve("dst");
		Files.createDirectories(srcDir);
		final byte[] data0 = sourceBytes(4096, 1);
		final byte[] data1 = sourceBytes(1000, 2);
		final Path src0 = srcDir.resolve("f0");
		final Path src1 = srcDir.resolve("f1");
		Files.write(src0, data0);
		Files.write(src1, data1);

		try (var drv = newDriver(false)) {
			CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
			drv.operationResultOutput(out);
			drv.start();
			copyAndExpectSucc(drv, out, new DataItemImpl(src0.toString(), 0, data0.length), srcDir, dstDir);
			copyAndExpectSucc(drv, out, new DataItemImpl(src1.toString(), 0, data1.length), srcDir, dstDir);
			drv.stop();
		}
		assertArrayEquals(data0, Files.readAllBytes(underOutputPath(dstDir, src0.toString())));
		assertArrayEquals(data1, Files.readAllBytes(underOutputPath(dstDir, src1.toString())));
	}

	@Test
	public void createFromSrcCopiesListedItems() throws Exception {
		final Path srcDir = tempDir.resolve("listed-src");
		final Path dstDir = tempDir.resolve("listed-dst");
		Files.createDirectories(srcDir);
		final byte[] dataA = sourceBytes(2048, 3);
		final byte[] dataB = sourceBytes(777, 4);
		Files.write(srcDir.resolve("a"), dataA);
		Files.write(srcDir.resolve("b"), dataB);

		try (var drv = newDriver(false)) {
			final List<DataItemImpl> items = drv.list(new DataItemFactoryImpl<>(), srcDir.toString(), "", 10, null, 10);
			assertEquals(2, items.size());
			CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
			drv.operationResultOutput(out);
			drv.start();
			for (final DataItemImpl item : items) {
				assertTrue(Path.of(item.name()).isAbsolute(), item.name());
				copyAndExpectSucc(drv, out, item, srcDir, dstDir);
			}
			drv.stop();
		}
		final Path srcAbs = srcDir.toAbsolutePath();
		assertArrayEquals(dataA, Files.readAllBytes(underOutputPath(dstDir, srcAbs.resolve("a").toString())));
		assertArrayEquals(dataB, Files.readAllBytes(underOutputPath(dstDir, srcAbs.resolve("b").toString())));
	}

	@Test
	public void createWithoutSrcWritesItemNamedByAbsolutePath() throws Exception {
		final String name = tempDir.resolve("was-written-here").resolve("g0").toString();
		final Path dstDir = tempDir.resolve("gen-dst");
		final long size = 3000;

		try (var drv = newDriver(false)) {
			CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
			drv.operationResultOutput(out);
			drv.start();
			var op = newOp(OpType.CREATE, name, size, null, dstDir.toString());
			assertTrue(drv.put(op));
			Operation<DataItemImpl> res = out.await(5000);
			assertNotNull(res);
			assertEquals(Operation.Status.SUCC, res.status());
			drv.stop();
		}
		assertEquals(size, Files.size(underOutputPath(dstDir, name)));
	}

	@Test
	public void updateOfMissingFileFailsIoAndCreatesNoDirectories() throws Exception {
		final Path dstDir = Files.createDirectories(tempDir.resolve("upd-dst"));
		final String name = "f";
		Files.write(dstDir.resolve(name), new byte[1024]);

		try (var drv = newDriver(false)) {
			CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
			drv.operationResultOutput(out);
			drv.start();
			// the first UPDATE succeeds, so the driver has opened the file in this directory
			assertTrue(drv.put(newOp(OpType.UPDATE, name, 1024, null, dstDir.toString())));
			Operation<DataItemImpl> res = out.await(5000);
			assertNotNull(res);
			assertEquals(Operation.Status.SUCC, res.status());

			Files.delete(dstDir.resolve(name));
			Files.delete(dstDir);

			// with the file and its directory gone, an UPDATE fails and does not recreate the directory
			assertTrue(drv.put(newOp(OpType.UPDATE, name, 1024, null, dstDir.toString())));
			res = out.await(5000);
			assertNotNull(res);
			assertEquals(Operation.Status.FAIL_IO, res.status());
			drv.stop();
		}
		assertFalse(Files.exists(dstDir));
	}

	@Test
	public void createRecreatesOutputPathRemovedMidRun() throws Exception {
		final Path dstDir = tempDir.resolve("flat-dst");

		try (var drv = newDriver(false)) {
			CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
			drv.operationResultOutput(out);
			drv.start();
			assertTrue(drv.put(newOp(OpType.CREATE, "first", 512, null, dstDir.toString())));
			Operation<DataItemImpl> res = out.await(5000);
			assertNotNull(res);
			assertEquals(Operation.Status.SUCC, res.status());

			Files.delete(dstDir.resolve("first"));
			Files.delete(dstDir);

			assertTrue(drv.put(newOp(OpType.CREATE, "second", 512, null, dstDir.toString())));
			res = out.await(5000);
			assertNotNull(res);
			assertEquals(Operation.Status.SUCC, res.status());
			drv.stop();
		}
		assertEquals(512, Files.size(dstDir.resolve("second")));
	}

	@Test
	public void readWholeFileWithoutVerify() throws Exception {
		final byte[] data = new byte[2048];
		for (int i = 0; i < data.length; i++)
			data[i] = (byte) (255 - (i & 0xFF));
		final String name = "read.bin";
		Files.write(tempDir.resolve(name), data);

		try (var drv = newDriver(false)) {
			CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
			drv.operationResultOutput(out);
			drv.start();

			var op = newOp(OpType.READ, name, data.length, tempDir.toString(), null);
			assertTrue(drv.put(op));
			Operation<DataItemImpl> res = out.await(5000);
			assertNotNull(res);
			assertEquals(Operation.Status.SUCC, res.status());
			drv.stop();
		}
	}

	@Test
	public void deleteRemovesFile() throws Exception {
		final String name = "delete.bin";
		final long size = 1024;

		// Create first
		try (var drv = newDriver(false)) {
			CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
			drv.operationResultOutput(out);
			drv.start();
			assertTrue(drv.put(newOp(OpType.CREATE, name, size, null, tempDir.toString())));
			assertNotNull(out.await(5000));
			drv.stop();
		}

		assertTrue(Files.exists(tempDir.resolve(name)));

		// Delete
		try (var drv = newDriver(false)) {
			CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
			drv.operationResultOutput(out);
			drv.start();
			var op = newOp(OpType.DELETE, name, 0, null, tempDir.toString());
			assertTrue(drv.put(op));
			Operation<DataItemImpl> res = out.await(5000);
			assertNotNull(res);
			assertEquals(Operation.Status.SUCC, res.status());
			assertFalse(Files.exists(tempDir.resolve(name)));
			drv.stop();
		}
	}

	private Operation<DataItemImpl> deleteOnce(final Operation<DataItemImpl> op) throws Exception {
		try (var drv = newDriver(false)) {
			CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
			drv.operationResultOutput(out);
			drv.start();
			assertTrue(drv.put(op));
			Operation<DataItemImpl> res = out.await(5000);
			drv.stop();
			return res;
		}
	}

	/**
	 * A DELETE of a name that already holds its path deletes that file; the name is not joined to the
	 * path again ({@code <d>/<d>/f}). Such names are an items-file name with an empty segment, such
	 * as {@code <d>//f}, which the operation keeps whole with the path {@code <d>}, and a retried
	 * DELETE, whose name {@code result()} joined to the path.
	 */
	@Test
	public void deleteOfANameThatHoldsTheDstPathDeletesThatFile() throws Exception {
		final Path dir = Files.createDirectories(tempDir.resolve("d"));
		final Path file = dir.resolve("f");
		// the path that joining the name to the path again would address
		final Path joinedAgain = Path.of(dir.toString(), dir + "/f");
		Files.createDirectories(joinedAgain.getParent());

		// an items-file name kept whole, with an explicit output path and with the path it derives
		for (final String dstPath : new String[]{dir.toString(), null
		}) {
			Files.write(file, new byte[]{1
			});
			Files.write(joinedAgain, new byte[]{2
			});
			final var op = newOp(OpType.DELETE, dir + "//f", 0, null, dstPath);
			assertEquals(dir.toString(), op.dstPath());
			assertEquals(dir + "//f", op.item().name());
			final Operation<DataItemImpl> res = deleteOnce(op);
			assertNotNull(res, "dstPath " + dstPath + ": no completed DELETE");
			assertEquals(Operation.Status.SUCC, res.status(), "dstPath " + dstPath);
			assertFalse(Files.exists(file), "dstPath " + dstPath + " must delete " + file);
			assertTrue(Files.exists(joinedAgain), "dstPath " + dstPath + " must not delete " + joinedAgain);
		}

		// a retry: the operation's result, reset and dispatched again
		Files.write(file, new byte[]{1
		});
		Files.write(joinedAgain, new byte[]{2
		});
		final var first = newOp(OpType.DELETE, "f", 0, null, dir.toString());
		final Operation<DataItemImpl> retry = first.result();
		retry.reset();
		assertEquals(dir + "/f", retry.item().name());
		final Operation<DataItemImpl> res = deleteOnce(retry);
		assertNotNull(res, "retry: no completed DELETE");
		assertEquals(Operation.Status.SUCC, res.status(), "retry");
		assertFalse(Files.exists(file), "the retry must delete " + file);
		assertTrue(Files.exists(joinedAgain), "the retry must not delete " + joinedAgain);
	}

	@Test
	public void listReturnsItemsFromDirectory() throws Exception {
		// Prepare files
		Files.write(tempDir.resolve("a.txt"), new byte[]{1
		});
		Files.write(tempDir.resolve("b.txt"), new byte[]{2, 3
		});

		try (var drv = newDriver(false)) {
			ItemFactory<DataItemImpl> fac = new DataItemFactoryImpl<>();
			List<DataItemImpl> items = drv.list(fac, tempDir.toString(), "", 10, null, 10);
			assertNotNull(items);
			assertEquals(2, items.size());
			// Expect sizes match written files
			long totalSize = items.stream().mapToLong(DataItemImpl::size).sum();
			assertEquals(3, totalSize);
		}
	}

	// Failure status of an fs operation: RESP_FAIL_SPACE (not retried) only for a real space failure.

	private static FileSystemException fsFailure(final String file, final String reason) {
		return new FileSystemException(file, null, reason);
	}

	@Test
	public void failStatusOfNonSpaceReasonsWithFreeAncestorIsFailIo() {
		final String missing = tempDir.resolve("missing").resolve("item").toString();
		for (final String reason : List.of(
						"Not a directory", "Too many open files", "Read-only file system", "File name too long")) {
			assertEquals(
							Operation.Status.FAIL_IO, FileStorageDriver.failStatusOf(fsFailure(missing, reason)), reason);
			assertEquals(
							Operation.Status.FAIL_IO, FileStorageDriver.failStatusOf(reason, missing, f -> 1L), reason);
		}
	}

	@Test
	public void failStatusOfSpaceReasonsIsRespFailSpace() {
		final String missing = tempDir.resolve("missing").toString();
		for (final String reason : List.of("No space left on device", "Disk quota exceeded")) {
			assertEquals(
							Operation.Status.RESP_FAIL_SPACE, FileStorageDriver.failStatusOf(fsFailure(missing, reason)),
							reason);
			assertEquals(
							Operation.Status.RESP_FAIL_SPACE, FileStorageDriver.failStatusOf(reason, null, f -> 1L),
							reason);
		}
	}

	@Test
	public void failStatusOfNonEnglishReasonTestsNearestExistingAncestor() {
		final String missing = tempDir.resolve("a").resolve("b").resolve("item").toString();
		final List<File> tested = new ArrayList<>();
		final ToLongFunction<File> noSpace = f -> {
			tested.add(f);
			return 0L;
		};
		assertEquals(
						Operation.Status.RESP_FAIL_SPACE,
						FileStorageDriver.failStatusOf("Auf dem Gerät ist kein Speicherplatz mehr verfügbar", missing, noSpace));
		assertEquals(List.of(tempDir.toFile().getAbsoluteFile()), tested);
		assertEquals(
						Operation.Status.FAIL_IO,
						FileStorageDriver.failStatusOf("Ist kein Verzeichnis", missing, f -> 4096L));
	}

	@Test
	public void failStatusOfNullFileIsFailIo() {
		assertEquals(
						Operation.Status.FAIL_IO,
						assertDoesNotThrow(() -> FileStorageDriver.failStatusOf(fsFailure(null, "Not a directory"))));
		assertEquals(
						Operation.Status.FAIL_IO,
						assertDoesNotThrow(() -> FileStorageDriver.failStatusOf(fsFailure(null, null))));
	}

	@Test
	public void failStatusOfRelativePathDoesNotThrow() {
		final List<File> tested = new ArrayList<>();
		assertEquals(
						Operation.Status.FAIL_IO,
						assertDoesNotThrow(() -> FileStorageDriver.failStatusOf(
										"Not a directory", "out-missing-" + System.nanoTime() + "/item", f -> {
											tested.add(f);
											return 1L;
										})));
		assertEquals(1, tested.size());
		assertTrue(tested.get(0).isAbsolute(), tested.toString());
		assertEquals(
						Operation.Status.FAIL_IO,
						assertDoesNotThrow(() -> FileStorageDriver.failStatusOf(fsFailure("out/item", "Not a directory"))));
	}

	@Test
	public void writeFailStatusOfNonWriteOperationIsFailIo() {
		final var read = newOp(OpType.READ, "item", 1, null, tempDir.toString());
		assertEquals(
						Operation.Status.FAIL_IO,
						FileStorageDriver.writeFailStatusOf(read, new IOException("No space left on device")));
		final var create = newOp(OpType.CREATE, "item", 1, null, tempDir.toString());
		assertEquals(
						Operation.Status.RESP_FAIL_SPACE,
						FileStorageDriver.writeFailStatusOf(create, new IOException("No space left on device")));
		assertEquals(
						Operation.Status.FAIL_IO,
						FileStorageDriver.writeFailStatusOf(create, new IOException("Input/output error")));
	}

	private static Operation.Status runOne(
					final FileStorageDriver<DataItemImpl, Operation<DataItemImpl>> drv,
					final DataOperationImpl<DataItemImpl> op) throws Exception {
		final CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
		drv.operationResultOutput(out);
		drv.start();
		assertTrue(drv.put(op));
		final Operation<DataItemImpl> res = out.await(5000);
		assertNotNull(res);
		drv.stop();
		return res.status();
	}

	@Test
	public void createThroughARegularFileIsFailIo() throws Exception {
		final Path blocker = tempDir.resolve("blocker");
		Files.write(blocker, new byte[0]);
		try (var drv = newDriver(false)) {
			final var op = newOp(OpType.CREATE, "item", 4096, null, blocker.resolve("out").toString());
			assertEquals(Operation.Status.FAIL_IO, runOne(drv, op));
		}
	}

	// /dev/full fails every write with ENOSPC: a destination linked to it stands in for a full file system.
	private static Path linkToDevFull(final Path link) throws IOException {
		final Path devFull = Path.of("/dev/full");
		assumeTrue(Files.isWritable(devFull), "/dev/full is not available");
		return Files.createSymbolicLink(link, devFull);
	}

	@Test
	public void copyOntoAFullFileSystemIsRespFailSpace() throws Exception {
		final String name = "copy.bin";
		final Path srcDir = tempDir.resolve("src");
		final Path dstDir = tempDir.resolve("dst");
		Files.createDirectories(srcDir);
		Files.createDirectories(dstDir);
		Files.write(srcDir.resolve(name), sourceBytes(65536, 3));
		linkToDevFull(dstDir.resolve(name));
		try (var drv = newDriver(false)) {
			final var op = newOp(OpType.CREATE, name, 65536, srcDir.toString(), dstDir.toString());
			assertEquals(Operation.Status.RESP_FAIL_SPACE, runOne(drv, op));
		}
	}

	@Test
	public void createOntoAFullFileSystemIsRespFailSpace() throws Exception {
		final String name = "create.bin";
		final Path dstDir = tempDir.resolve("dst");
		Files.createDirectories(dstDir);
		linkToDevFull(dstDir.resolve(name));
		try (var drv = newDriver(false)) {
			final var op = newOp(OpType.CREATE, name, 65536, null, dstDir.toString());
			assertEquals(Operation.Status.RESP_FAIL_SPACE, runOne(drv, op));
		}
	}

	@Test
	public void adjustIoBuffersNoop() throws Exception {
		try (var drv = newDriver(false)) {
			assertDoesNotThrow(() -> drv.adjustIoBuffers(4096, OpType.READ));
		}
	}
}
