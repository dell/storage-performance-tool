package com.dell.spt.storage.driver.coop.nio.fs;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationRetryPolicy;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.logging.Loggers;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.commons.system.SizeInBytes;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

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

	/** Runs one copy to its end and returns the completed operation. */
	private Operation<DataItemImpl> copyOnce(final String name, final long size, final String srcPath,
					final String dstPath) throws Exception {
		try (var drv = newDriver(false)) {
			CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
			drv.operationResultOutput(out);
			drv.start();
			assertTrue(drv.put(newOp(OpType.CREATE, name, size, srcPath, dstPath)));
			Operation<DataItemImpl> res = out.await(5000);
			assertNotNull(res, "no completed copy of " + name);
			drv.stop();
			return res;
		}
	}

	private void assertCopyFailsAsOwnSource(final Path source, final String name, final String srcPath,
					final String dstPath) throws Exception {
		final byte[] data = sourceBytes(3000, 5);
		Files.createDirectories(source.getParent());
		Files.write(source, data);
		final Operation<DataItemImpl> res = copyOnce(name, data.length, srcPath, dstPath);
		assertEquals(Operation.Status.RESP_FAIL_CLIENT, res.status(), srcPath + " to " + dstPath);
		assertFalse(OperationRetryPolicy.isRetryableStatus(res.status()), "load.op.retry must attempt it once");
		assertArrayEquals(data, Files.readAllBytes(source), "the source must stay as it was");
	}

	@Test
	public void aCopyOfAListedItemOntoItselfFailsAndKeepsTheSource() throws Exception {
		final Path srcDir = tempDir.resolve("src");
		final Path source = srcDir.resolve("f0");
		// equal paths, then an output path above the input path
		assertCopyFailsAsOwnSource(source, source.toString(), srcDir.toString(), srcDir.toString());
		assertCopyFailsAsOwnSource(source, source.toString(), srcDir.toString(), tempDir.toString());
	}

	@Test
	public void aCopyOfAnItemNamedUnderBothPathsFailsAndKeepsTheSource() throws Exception {
		final Path srcDir = tempDir.resolve("src");
		final Path dstDir = srcDir.resolve("out");
		final Path source = dstDir.resolve("f0");
		assertCopyFailsAsOwnSource(source, source.toString(), srcDir.toString(), dstDir.toString());
	}

	@Test
	public void aCopyOfARelativeNameOntoItselfFailsAndKeepsTheSource() throws Exception {
		final Path srcDir = tempDir.resolve("src");
		final Path source = srcDir.resolve("f0");
		assertCopyFailsAsOwnSource(source, "f0", srcDir.toString(), srcDir.toString());
		assertCopyFailsAsOwnSource(source, "f0", srcDir.toString(), tempDir + "/./src");
	}

	@Test
	public void anOutputPathThatOnlyLooksLikeTheInputPathStillCopies() throws Exception {
		final Path srcDir = tempDir.resolve("pp");
		final Path source = srcDir.resolve("f0");
		final byte[] data = sourceBytes(3000, 6);
		Files.createDirectories(srcDir);
		Files.write(source, data);
		// a sibling sharing a string prefix, then the input's parent written with ".."
		for (final String dstPath : new String[]{tempDir + "/ppx", srcDir + "/.."
		}) {
			final Operation<DataItemImpl> res = copyOnce(source.toString(), data.length, srcDir.toString(), dstPath);
			assertEquals(Operation.Status.SUCC, res.status(), dstPath);
			assertArrayEquals(data, Files.readAllBytes(Path.of(dstPath, source.toString())), dstPath);
			assertArrayEquals(data, Files.readAllBytes(source), dstPath);
		}
	}

	@Test
	public void ownSourceCopiesWarnOncePerDriver() throws Exception {
		final Path srcDir = Files.createDirectories(tempDir.resolve("src"));
		Files.write(srcDir.resolve("f0"), sourceBytes(100, 7));
		Files.write(srcDir.resolve("f1"), sourceBytes(100, 8));
		final List<String> warnings = captureOwnSourceWarnings(() -> {
			try (var drv = newDriver(false)) {
				CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
				drv.operationResultOutput(out);
				drv.start();
				for (final String name : new String[]{"f0", "f1"
				}) {
					assertTrue(drv.put(newOp(OpType.CREATE, name, 100, srcDir.toString(), srcDir.toString())));
					Operation<DataItemImpl> res = out.await(5000);
					assertNotNull(res, name);
					assertEquals(Operation.Status.RESP_FAIL_CLIENT, res.status(), name);
				}
				drv.stop();
			}
		});
		assertEquals(1, warnings.size(), warnings.toString());
		assertTrue(warnings.get(0).contains(srcDir.resolve("f0").toString()), warnings.get(0));
	}

	@Test
	public void aCopyOfAMissingSourceFailsAndCreatesNoDestination() throws Exception {
		final Path srcDir = Files.createDirectories(tempDir.resolve("src"));
		final Path dstDir = tempDir.resolve("dst");
		final Operation<DataItemImpl> res = copyOnce("gone", 1000, srcDir.toString(), dstDir.toString());
		assertEquals(Operation.Status.FAIL_IO, res.status());
		// the driver base prepares the output path itself before the operation runs
		try (var entries = Files.list(dstDir)) {
			assertEquals(0, entries.count(), "a failed source must add nothing under the output path");
		}
	}

	@Test
	public void aCopyOfAnUnreadableSourceFailsAndCreatesNoDestination() throws Exception {
		assumeFalse("root".equals(System.getProperty("user.name")), "root reads a mode 000 file");
		final Path srcDir = Files.createDirectories(tempDir.resolve("src"));
		final Path dstDir = Files.createDirectories(tempDir.resolve("dst"));
		final Path source = srcDir.resolve("f0");
		Files.write(source, sourceBytes(1000, 9));
		Files.setPosixFilePermissions(source, PosixFilePermissions.fromString("---------"));
		final Operation<DataItemImpl> res = copyOnce("f0", 1000, srcDir.toString(), dstDir.toString());
		assertEquals(Operation.Status.FAIL_IO, res.status());
		assertFalse(Files.exists(dstDir.resolve("f0")));
	}

	@Test
	public void aCopyOfAMissingSourceKeepsAnEarlierCopy() throws Exception {
		final Path srcDir = Files.createDirectories(tempDir.resolve("src"));
		final Path dstDir = Files.createDirectories(tempDir.resolve("dst"));
		final byte[] earlier = sourceBytes(3000, 10);
		Files.write(dstDir.resolve("f0"), earlier);
		final Operation<DataItemImpl> res = copyOnce("f0", 3000, srcDir.toString(), dstDir.toString());
		assertEquals(Operation.Status.FAIL_IO, res.status());
		assertArrayEquals(earlier, Files.readAllBytes(dstDir.resolve("f0")));
	}

	@Test
	public void aRerunFromTheCopysOwnItemsOutputKeepsTheEarlierCopies() throws Exception {
		final Path srcDir = Files.createDirectories(tempDir.resolve("src"));
		final Path dstDir = Files.createDirectories(tempDir.resolve("dst"));
		final byte[] data = sourceBytes(3000, 11);
		Files.write(srcDir.resolve("f0"), data);
		final Path copy = dstDir.resolve("f0");
		Files.write(copy, data);
		// the items output records the destination name, which is not a file under the input path
		final Operation<DataItemImpl> res = copyOnce(copy.toString(), 3000, srcDir.toString(), dstDir.toString());
		assertEquals(Operation.Status.FAIL_IO, res.status());
		assertArrayEquals(data, Files.readAllBytes(copy));
	}

	@Test
	public void everyRetryOfAMissingSourceFailsAndCreatesNoDestination() throws Exception {
		final Path srcDir = Files.createDirectories(tempDir.resolve("src"));
		final Path dstDir = Files.createDirectories(tempDir.resolve("dst"));
		try (var drv = newDriver(false)) {
			CollectingOpOutput<DataItemImpl> out = new CollectingOpOutput<>();
			drv.operationResultOutput(out);
			drv.start();
			Operation<DataItemImpl> op = newOp(OpType.CREATE, "gone", 1000, srcDir.toString(), dstDir.toString());
			for (int attempt = 0; attempt < 3; attempt++) {
				assertTrue(drv.put(op));
				final Operation<DataItemImpl> res = out.await(5000);
				assertNotNull(res, "attempt " + attempt);
				assertEquals(Operation.Status.FAIL_IO, res.status(), "attempt " + attempt);
				assertTrue(OperationRetryPolicy.isRetryableStatus(res.status()));
				assertFalse(Files.exists(dstDir.resolve("gone")), "attempt " + attempt);
				res.reset();
				res.startNextLifecycle();
				op = res;
			}
			drv.stop();
		}
	}

	@Test
	public void aMissingSourceNamedByAbsolutePathAddsNoDirectoriesUnderTheOutputPath() throws Exception {
		final Path srcDir = Files.createDirectories(tempDir.resolve("src"));
		final Path dstDir = Files.createDirectories(tempDir.resolve("dst"));
		final String name = srcDir.resolve("deep").resolve("gone").toString();
		final Operation<DataItemImpl> res = copyOnce(name, 1000, srcDir.toString(), dstDir.toString());
		assertEquals(Operation.Status.FAIL_IO, res.status());
		try (var entries = Files.list(dstDir)) {
			assertEquals(0, entries.count(), "nothing may be added under the output path");
		}
	}

	@FunctionalInterface
	private interface ThrowingRunnable {
		void run() throws Exception;
	}

	/**
	 * Runs {@code body} and returns the own-source warnings logged meanwhile. Engine loggers may be
	 * asynchronous, so a marker logged before and after bounds what the body logged.
	 */
	private static List<String> captureOwnSourceWarnings(final ThrowingRunnable body) throws Exception {
		final var appender = new OwnSourceWarningAppender();
		appender.start();
		final var logger = LoggerContext.getContext(false).getLogger(Loggers.ERR.getName());
		logger.addAppender(appender);
		try {
			Loggers.ERR.warn(appender.startMarker);
			awaitTrue(() -> appender.armed, "the start marker was not logged");
			body.run();
			Loggers.ERR.warn(appender.endMarker);
			awaitTrue(() -> appender.drained, "the end marker was not logged");
			return List.copyOf(appender.messages);
		} finally {
			logger.removeAppender(appender);
			appender.stop();
		}
	}

	private static void awaitTrue(final BooleanSupplier condition, final String message) throws Exception {
		final long deadline = System.nanoTime() + 5_000_000_000L;
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertTrue(condition.getAsBoolean(), message);
	}

	private static final class OwnSourceWarningAppender extends AbstractAppender {
		final List<String> messages = Collections.synchronizedList(new ArrayList<>());
		final String startMarker = "FileStorageDriverTest start " + System.nanoTime();
		final String endMarker = "FileStorageDriverTest end " + System.nanoTime();
		volatile boolean armed;
		volatile boolean drained;

		OwnSourceWarningAppender() {
			super("testOwnSourceWarning", null, null, true, Property.EMPTY_ARRAY);
		}

		@Override
		public void append(final LogEvent event) {
			final String message = event.getMessage().getFormattedMessage();
			if (message.equals(startMarker)) {
				armed = true;
			} else if (message.equals(endMarker)) {
				drained = true;
			} else if (armed && !drained && Level.WARN.equals(event.getLevel()) && message.contains("its own source")) {
				messages.add(message);
			}
		}
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

	@Test
	public void adjustIoBuffersNoop() throws Exception {
		try (var drv = newDriver(false)) {
			assertDoesNotThrow(() -> drv.adjustIoBuffers(4096, OpType.READ));
		}
	}
}
