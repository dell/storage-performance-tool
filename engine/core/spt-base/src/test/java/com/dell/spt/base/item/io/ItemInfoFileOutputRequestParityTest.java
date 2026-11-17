package com.dell.spt.base.item.io;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemNamePaths;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.OperationImpl;
import com.dell.spt.base.storage.Credential;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * A manifest item's row must name the object its request addressed, and read back as a request for
 * that object again.
 */
class ItemInfoFileOutputRequestParityTest {

	@TempDir
	Path tempDir;

	/**
	 * The request path rule of {@code HttpStorageDriverBase.dataUriPath} (extensions/storage-drivers/
	 * protocols/http, lines 442-449 at 3a4198b4), copied because spt-base's tests can't depend on the
	 * driver module.
	 */
	private static String requestPath(final String name, final String srcPath, final String dstPath) {
		final String path = dstPath != null ? dstPath : srcPath;
		if (path == null || ItemNamePaths.holdsPath(name, path)) {
			return name.startsWith("/") ? name : "/" + name;
		}
		return ItemNamePaths.join(path.startsWith("/") ? path : "/" + path, name);
	}

	private static String requestPath(final Operation<?> op) {
		return requestPath(op.item().name(), op.srcPath(), op.dstPath());
	}

	@ParameterizedTest(name = "{0} bucket={1} key={2} dst={3}")
	@CsvSource(nullValues = "null", value = {
			"READ,   src,  k1,      null, /src/k1",
			"READ,   src,  dir/k2,  null, /src/dir/k2",
			"READ,   src,  /dir/k2, null, /src/dir/k2",
			"READ,   src,  /src/k1, null, /src/k1",
			"READ,   /src, k1,      null, /src/k1",
			"READ,   /src, dir/k2,  null, /src/dir/k2",
			"READ,   src/, k1,      null, /src/k1",
			"READ,   src/, dir/k2,  null, /src/dir/k2",
			"DELETE, src,  /src/k1, null, /src/k1",
			"DELETE, src,  /dir/k2, null, /src/dir/k2",
			"STAT,   src/, dir/k2,  null, /src/dir/k2",
			"CREATE, src,  k1,      /dst, /dst/k1",
			"CREATE, src,  dir/k2,  /dst, /dst/dir/k2",
			"CREATE, src,  dir/k2,  /dst/, /dst/dir/k2",
			"CREATE, src,  k1,      dst,  /dst/k1",
			"CREATE, src,  dst/k1,  dst,  /dst/k1",
			"CREATE, src,  /dst/k1, /dst, /dst/k1",
			"CREATE, src,  /dir/k2, /dst, /dst/dir/k2",
	})
	void rowNamesTheRequestedObjectAndReadsBackAsIt(
					final OpType type,
					final String bucket,
					final String key,
					final String dstPath,
					final String expectedRequest)
					throws Exception {
		final Operation<Item> op = new OperationImpl<>(
						0, type, new IntegrityManifestDataItem(bucket, key, 1024, ""), null, dstPath, Credential.NONE);
		final String request = requestPath(op);
		assertEquals(expectedRequest, request, "first request");

		final Path file = tempDir.resolve("items.csv");
		final var output = new ItemInfoFileOutput<Operation<Item>>(file);
		output.put(op);
		output.close();
		final List<String> rows = Files.readAllLines(file);
		assertEquals(List.of(request + ",0,1024,0/0"), rows, "row");

		try (final var input = new CsvItemInput<DataItem>(
						new ByteArrayInputStream((rows.get(0) + "\n").getBytes(StandardCharsets.UTF_8)),
						new DataItemFactoryImpl<>())) {
			final DataItem readBack = input.get();
			final Operation<DataItem> readOp = new OperationImpl<>(0, OpType.READ, readBack, null, null, Credential.NONE);
			assertEquals(request, requestPath(readOp), "read-back request");
			assertEquals(1024, readBack.size());
		}
	}
}
