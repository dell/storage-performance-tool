package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.io.CsvItemInput;
import com.dell.spt.base.item.io.ItemInfoFileOutput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import io.netty.handler.codec.http.HttpRequest;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;

/**
 * An {@code item.output.file} row for an object from a {@code bucket,key,size,version_id} manifest,
 * read back by a later READ step, makes the {@code s3} driver request the object the first request
 * addressed. The row is that request path, except under a relative CREATE output path, which the row
 * keeps as written, as a plain item's row does, and the driver roots on the read back.
 */
class S3ItemsOutputRequestPathParityTest {

	private static final Credential CREDENTIAL = Credential.getInstance("user1", "secret1");
	private static final long SIZE = 1024;

	private static ParityDriver driver;
	private static DataInput dataInput;

	@TempDir
	static Path tempDir;

	@BeforeAll
	static void startDriver() throws Exception {
		driver = new ParityDriver(S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1:9024"));
		dataInput = DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true);
	}

	@AfterAll
	static void closeDriver() throws Exception {
		driver.close();
		dataInput.close();
	}

	/** A manifest row, the operation on it, the step's output path, the request path and the row's name. */
	private record Shape(OpType type, String bucket, String key, String outputPath, String request, String row) {

		Shape(final OpType type, final String bucket, final String key, final String outputPath, final String request) {
			this(type, bucket, key, outputPath, request, request);
		}
	}

	private static final List<Shape> SHAPES = List.of(
					new Shape(OpType.READ, "src", "k1", null, "/src/k1"),
					new Shape(OpType.READ, "src", "dir/k2", null, "/src/dir/k2"),
					new Shape(OpType.READ, "src", "/dir/k2", null, "/src/dir/k2"),
					// a key that already holds its bucket is not placed under it again
					new Shape(OpType.READ, "src", "/src/k1", null, "/src/k1"),
					new Shape(OpType.READ, "src", "/srcx/k1", null, "/src/srcx/k1"),
					new Shape(OpType.READ, "/src", "k1", null, "/src/k1"),
					new Shape(OpType.READ, "/src", "dir/k2", null, "/src/dir/k2"),
					new Shape(OpType.READ, "src/", "k1", null, "/src/k1"),
					new Shape(OpType.READ, "src/", "dir/k2", null, "/src/dir/k2"),
					new Shape(OpType.DELETE, "src", "/src/k1", null, "/src/k1"),
					new Shape(OpType.DELETE, "src", "/dir/k2", null, "/src/dir/k2"),
					new Shape(OpType.STAT, "src/", "dir/k2", null, "/src/dir/k2"),
					new Shape(OpType.UPDATE, "src", "dir/k2", null, "/src/dir/k2"),
					// a CREATE writes to the output path, not to the manifest's bucket
					new Shape(OpType.CREATE, "src", "k1", "/dst", "/dst/k1"),
					new Shape(OpType.CREATE, "src", "dir/k2", "/dst", "/dst/dir/k2"),
					new Shape(OpType.CREATE, "src", "dir/k2", "/dst/", "/dst/dir/k2"),
					new Shape(OpType.CREATE, "src", "/dst/k1", "/dst", "/dst/k1"),
					new Shape(OpType.CREATE, "src", "/dir/k2", "/dst", "/dst/dir/k2"),
					new Shape(OpType.CREATE, "src", "/dir/k2", "/dst/pre", "/dst/pre/dir/k2"),
					// a relative output path stays relative in the row
					new Shape(OpType.CREATE, "src", "k1", "dst", "/dst/k1", "dst/k1"),
					new Shape(OpType.CREATE, "src", "dst/k1", "dst", "/dst/k1", "dst/k1"),
					new Shape(OpType.CREATE, "src", "dir/k2", "dst/", "/dst/dir/k2", "dst/dir/k2"),
					new Shape(OpType.CREATE, "src", "/dir/k2", "dst", "/dst/dir/k2", "dst/dir/k2"),
					// a rooted key holds a relative path too: written as the key, which HTTP requested
					new Shape(OpType.CREATE, "src", "/dst/k1", "dst", "/dst/k1", "/dst/k1"));

	@TestFactory
	Stream<DynamicTest> rowReadsBackAsTheFirstRequest() {
		return SHAPES.stream().map(shape -> dynamicTest(
						shape.type() + " " + shape.bucket() + " " + shape.key() + " to " + shape.outputPath(),
						() -> assertRoundTrip(shape)));
	}

	private static void assertRoundTrip(final Shape shape) throws Exception {
		final var item = new IntegrityManifestDataItem(shape.bucket(), shape.key(), SIZE, "");
		item.dataInput(dataInput);
		// as the engine's operations builder makes it: no input path, and the output path for a CREATE
		final var op = new DataOperationImpl<>(0, shape.type(), item, null, shape.outputPath(), CREDENTIAL, null, 0);
		assertEquals(shape.request(), requestPath(op), "first request");

		final Path file = Files.createTempFile(tempDir, "items-", ".csv");
		final var output = new ItemInfoFileOutput<Operation<?>>(file);
		output.put(op.result());
		output.close();
		final List<String> rows = Files.readAllLines(file);
		assertEquals(List.of(shape.row() + ",0,1024,0/0"), rows, "row");

		final DataItem readBack;
		try (var input = new CsvItemInput<DataItem>(
						new ByteArrayInputStream((rows.get(0) + "\n").getBytes(StandardCharsets.UTF_8)),
						new DataItemFactoryImpl<>())) {
			readBack = input.get();
		}
		readBack.dataInput(dataInput);
		// as the operations builder makes a READ of a legacy row: no input or output path
		final var readOp = new DataOperationImpl<>(0, OpType.READ, readBack, null, null, CREDENTIAL, null, 0);
		assertEquals(shape.request(), requestPath(readOp), "read-back request");
		assertEquals(SIZE, readBack.size());
	}

	/** The path of the request the driver builds, through its real data URI path and request routes. */
	@SuppressWarnings("unchecked")
	private static String requestPath(final DataOperationImpl<?> op) throws Exception {
		final String uriPath = driver.dataUriPathOf(op);
		final var request = (HttpRequest) driver.httpRequest((Operation<Item>) (Operation<?>) op, "127.0.0.1");
		assertEquals(uriPath, request.uri(), "request URI");
		return uriPath;
	}

	/** The test driver, with the data URI path its requests use. */
	private static final class ParityDriver extends S3StorageDriverTest.TestS3Driver {

		ParityDriver(final Config config) throws Exception {
			super(config);
		}

		String dataUriPathOf(final Operation<?> op) {
			return dataUriPath((Item) op.item(), op.srcPath(), op.dstPath(), op.type());
		}
	}
}
