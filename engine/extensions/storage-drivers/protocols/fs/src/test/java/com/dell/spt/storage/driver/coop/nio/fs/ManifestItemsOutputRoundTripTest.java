package com.dell.spt.storage.driver.coop.nio.fs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.TestConfigBuilder;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.io.CsvItemInput;
import com.dell.spt.base.item.io.ItemInfoFileOutput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * A manifest CREATE's items output row, read back by a later READ step, opens the file the CREATE made,
 * through the filesystem driver itself, for a relative output path and an absolute one.
 */
class ManifestItemsOutputRoundTripTest {

	private static final long SIZE = 1024;

	/** Under the working directory and given as a relative path, so the driver resolves it there. */
	private Path relativeDir;

	private Path absoluteDir;
	private Config config;
	private DataInput dataInput;

	@BeforeEach
	void setUp() throws Exception {
		final Path base = Path.of("build", "tmp");
		Files.createDirectories(base);
		relativeDir = Files.createTempDirectory(base, "manifest-items-output-");
		assertFalse(relativeDir.isAbsolute());
		absoluteDir = Files.createTempDirectory("manifest-items-output-").toAbsolutePath();
		config = TestConfigBuilder.config();
		config.val("load-batch-size", 16);
		config.val("storage-driver-limit-concurrency", 4);
		config.val("storage-driver-threads", 0);
		config.val("storage-driver-limit-queue-input", 128);
		dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (dataInput != null) {
			dataInput.close();
		}
		deleteTree(relativeDir);
		deleteTree(absoluteDir);
	}

	@Test
	void relativeOutputPathAndPlainKey() throws Exception {
		final String dst = relativeDir.resolve("dst").toString();
		assertRoundTrip(dst, "k1", dst + "/k1");
	}

	@Test
	void relativeOutputPathAndAKeyThatHoldsIt() throws Exception {
		final String dst = relativeDir.resolve("dst").toString();
		assertRoundTrip(dst, dst + "/k2", dst + "/k2");
	}

	@Test
	void absoluteOutputPathAndPlainKey() throws Exception {
		final String dst = absoluteDir.resolve("dst").toString();
		assertRoundTrip(dst, "k1", dst + "/k1");
	}

	@Test
	void absoluteOutputPathAndAKeyThatHoldsIt() throws Exception {
		final String dst = absoluteDir.resolve("dst").toString();
		assertRoundTrip(dst, dst + "/k3", dst + "/k3");
	}

	private void assertRoundTrip(final String dstPath, final String key, final String createdFile)
					throws Exception {
		final Path rows = absoluteDir.resolve("items-" + System.nanoTime() + ".csv");
		try (var driver = newDriver()) {
			final var out = new CollectingOpOutput<DataItem>();
			driver.operationResultOutput(out);
			driver.start();

			// as the operations builder makes a manifest CREATE: no input path, the output path
			final var item = new IntegrityManifestDataItem("src", key, SIZE, "");
			item.dataInput(dataInput);
			assertTrue(driver.put(new DataOperationImpl<DataItem>(0, OpType.CREATE, item, null, dstPath, null, null, 0)));
			final Operation<DataItem> created = out.await(5000);
			assertNotNull(created);
			assertEquals(Operation.Status.SUCC, created.status());
			assertTrue(Files.isRegularFile(Path.of(createdFile)), "created " + createdFile);
			assertEquals(SIZE, Files.size(Path.of(createdFile)));

			final var output = new ItemInfoFileOutput<Operation<DataItem>>(rows);
			output.put(created);
			output.close();
			final List<String> lines = Files.readAllLines(rows);
			assertEquals(List.of(createdFile + ",0,1024,0/0"), lines);

			final DataItem readBack;
			try (var input = new CsvItemInput<DataItem>(
							new ByteArrayInputStream((lines.get(0) + "\n").getBytes(StandardCharsets.UTF_8)),
							new DataItemFactoryImpl<>())) {
				readBack = input.get();
			}
			readBack.dataInput(dataInput);
			// as the operations builder makes a READ of a legacy row: no input or output path
			assertTrue(driver.put(new DataOperationImpl<>(0, OpType.READ, readBack, null, null, null, null, 0)));
			final Operation<DataItem> read = out.await(5000);
			assertNotNull(read);
			assertEquals(Operation.Status.SUCC, read.status(), "read back " + lines.get(0));
			assertEquals(SIZE, ((DataOperationImpl<DataItem>) read).countBytesDone());
			driver.stop();
		}
	}

	private FileStorageDriver<DataItem, Operation<DataItem>> newDriver() throws Exception {
		return new FileStorageDriver<>(
						"test-fs-items-output", dataInput, config.configVal("storage"), false,
						config.intVal("load-batch-size"));
	}

	private static void deleteTree(final Path root) throws IOException {
		if (root == null || !Files.exists(root)) {
			return;
		}
		try (var paths = Files.walk(root)) {
			for (final Path p : paths.sorted(Comparator.reverseOrder()).toList()) {
				Files.deleteIfExists(p);
			}
		}
	}
}
