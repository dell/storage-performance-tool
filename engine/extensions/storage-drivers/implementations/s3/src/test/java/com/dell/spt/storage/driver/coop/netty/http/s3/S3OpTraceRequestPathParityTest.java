package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.IntegrityManifestDataItem;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.logging.OperationTraceRecord;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import io.netty.handler.codec.http.HttpRequest;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

/**
 * The {@code ItemPath} column of an {@code op.trace.csv} row for an object from a {@code
 * bucket,key,size,version_id} manifest is the path of the request the {@code s3} driver sends for it,
 * whatever slashes the manifest's bucket and key carry and wherever a CREATE writes.
 */
class S3OpTraceRequestPathParityTest {

	/** 63 characters, the longest valid S3 bucket name. */
	private static final String LONG_BUCKET = "spt-trace-parity-" + "b".repeat(46);

	private static final Credential CREDENTIAL = Credential.getInstance("user1", "secret1");

	private static ParityDriver driver;

	@BeforeAll
	static void startDriver() throws Exception {
		driver = new ParityDriver(S3StorageDriverTest.baseConfig(false, 4, false, null, "127.0.0.1:9024"));
	}

	@AfterAll
	static void closeDriver() throws Exception {
		driver.close();
	}

	/** A manifest row, the type of the operation on it, the step's output path and the request path. */
	private record Shape(OpType type, String bucket, String key, String outputPath, String requestPath) {}

	private static final List<Shape> SHAPES = List.of(
					// type, manifest bucket, manifest key, the step's output path, the request path
					new Shape(OpType.READ, "bucket-a", "key", null, "/bucket-a/key"),
					new Shape(OpType.READ, "bucket-a", "/key", null, "/bucket-a/key"),
					new Shape(OpType.READ, "bucket-a/", "key", null, "/bucket-a/key"),
					new Shape(OpType.READ, "/bucket-a/", "/key", null, "/bucket-a/key"),
					new Shape(OpType.READ, "/bucket-a", "prefix/key", null, "/bucket-a/prefix/key"),
					// a key that already holds its bucket is not placed under it again
					new Shape(OpType.READ, "a", "/a/k", null, "/a/k"),
					new Shape(OpType.READ, LONG_BUCKET, "prefix/key", null, "/" + LONG_BUCKET + "/prefix/key"),
					// a CREATE writes to the output path, not to the manifest's bucket
					new Shape(OpType.CREATE, "source-bucket", "key", "/destination-bucket", "/destination-bucket/key"),
					new Shape(OpType.CREATE, "source-bucket", "key", "destination-bucket", "/destination-bucket/key"),
					new Shape(OpType.CREATE, "source-bucket", "/prefix/key", "/destination-bucket/out",
									"/destination-bucket/out/prefix/key"),
					new Shape(OpType.DELETE, "bucket-a", "prefix/key", null, "/bucket-a/prefix/key"),
					new Shape(OpType.DELETE, "bucket-a/", "/key", null, "/bucket-a/key"),
					new Shape(OpType.STAT, "bucket-a", "prefix/key", null, "/bucket-a/prefix/key"),
					new Shape(OpType.STAT, LONG_BUCKET, "/key", null, "/" + LONG_BUCKET + "/key"));

	@TestFactory
	Stream<DynamicTest> traceRowNamesTheRequestPath() {
		return SHAPES.stream().map(shape -> dynamicTest(
						shape.type() + " " + shape.bucket() + " " + shape.key() + " to " + shape.outputPath(),
						() -> assertTraceRowNamesTheRequestPath(shape)));
	}

	private static void assertTraceRowNamesTheRequestPath(final Shape shape) throws Exception {
		final var item = new IntegrityManifestDataItem(shape.bucket(), shape.key(), 16, "");
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		// As the engine's operations builder makes it: no input path, and the output path for a CREATE.
		final var op = new DataOperationImpl<>(0, shape.type(), item, null, shape.outputPath(), CREDENTIAL, null, 0);
		@SuppressWarnings("unchecked")
		final var request = (HttpRequest) driver.httpRequest((Operation<Item>) (Operation<?>) op, "127.0.0.1");

		assertEquals(shape.requestPath(), driver.dataUriPathOf(op));
		assertEquals(shape.requestPath(), request.uri());
		assertEquals(shape.requestPath(), tracePath(op.result()));
	}

	private static String tracePath(final Operation<?> result) {
		final var row = new StringBuilder();
		new OperationTraceRecord<>(result).format(row);
		return row.toString().split(",", -1)[1];
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
