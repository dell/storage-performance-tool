package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemFactoryImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.io.CsvFileItemInput;
import com.dell.spt.base.item.io.CsvFileItemOutput;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationsBuilderImpl;
import com.dell.spt.base.storage.driver.ListOptions;
import com.github.akurilov.commons.system.SizeInBytes;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

/**
 * A listed key with an empty segment right before its last segment, such as {@code /k2} or
 * {@code a//b} in bucket {@code pp}, is requested, named in an items file and read back whole. Before
 * the fix the request and the name lost that segment ({@code /pp/k2}), so a path-input DELETE
 * removed the look-alike key {@code k2}.
 */
@SuppressWarnings({"unchecked", "rawtypes"
})
class S3EmptyKeySegmentTest {

	private static final DataItemFactoryImpl<DataItem> FACTORY = new DataItemFactoryImpl<>();

	@TempDir
	Path tempDir;

	private S3StorageDriverTest.TestS3Driver drv;

	@BeforeEach
	void setUp() throws Exception {
		drv = new S3StorageDriverTest.TestS3Driver(S3StorageDriverTest.baseConfig(false, 2, false, null, "127.0.0.1"));
	}

	@AfterEach
	void tearDown() throws Exception {
		drv.close();
	}

	private static FullHttpResponse listPage(final String key) {
		final String xml = "<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>" + key
						+ "</Key><Size>1</Size></Contents></ListBucketResult>";
		return new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
						Unpooled.copiedBuffer(xml, StandardCharsets.UTF_8));
	}

	/** Lists the one key in bucket {@code pp}, the way a path item input does. */
	private DataItem listed(final String key) throws Exception {
		drv.enqueueResponse(listPage(key));
		final List<Item> items = drv.list((ItemFactory) FACTORY, "/pp", null, 36, null, 100, ListOptions.DEFAULT);
		final DataItem item = (DataItem) items.get(0);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 4, false, 0.0, true));
		return item;
	}

	/** Builds an operation the way the load generator does for a step with the given paths. */
	private static DataOperation<DataItem> build(
					final OpType type, final DataItem item, final String inputPath, final String outputPath) throws Exception {
		final var builder = new DataOperationsBuilderImpl<DataItem, DataOperation<DataItem>>(0);
		builder.opType(type);
		builder.inputPath(inputPath);
		builder.outputPathInput(outputPath == null ? null : new ConstantValueInputImpl<>(outputPath));
		builder.credentialInput(null);
		builder.sizeThreshold(Long.MAX_VALUE);
		return builder.buildOp(item);
	}

	private String request(final Operation<?> op) throws Exception {
		final var request = (HttpRequest) drv.httpRequest((Operation<Item>) (Operation<?>) op, "127.0.0.1");
		return request.method() + " " + request.uri();
	}

	/** Writes the item to an items file, as an item output file does, and reads it back. */
	private DataItem throughItemsFile(final DataItem item) throws Exception {
		final Path file = tempDir.resolve("items-" + System.nanoTime() + ".csv");
		try (var out = new CsvFileItemOutput<>(file, FACTORY)) {
			out.put(item);
		}
		try (var in = new CsvFileItemInput<>(file, FACTORY)) {
			return in.get();
		}
	}

	/** Each case: operation type, listed key, method and path. */
	private static final List<List<String>> REQUESTS = List.of(
					List.of("READ", "/k2", "GET", "/pp//k2"),
					List.of("READ", "a//b", "GET", "/pp/a//b"),
					List.of("READ", "/dir//k3", "GET", "/pp//dir//k3"),
					List.of("STAT", "/k2", "HEAD", "/pp//k2"),
					List.of("STAT", "a//b", "HEAD", "/pp/a//b"),
					List.of("DELETE", "/k2", "DELETE", "/pp//k2"),
					List.of("DELETE", "a//b", "DELETE", "/pp/a//b"),
					List.of("UPDATE", "/k2", "PUT", "/pp//k2"),
					List.of("UPDATE", "a//b", "PUT", "/pp/a//b"),
					// keys without an empty segment before the last one: as before
					List.of("READ", "k0", "GET", "/pp/k0"),
					List.of("READ", "dir/k1", "GET", "/pp/dir/k1"),
					List.of("STAT", "dir/k1", "HEAD", "/pp/dir/k1"),
					List.of("DELETE", "k0", "DELETE", "/pp/k0"),
					List.of("DELETE", "dir/k1", "DELETE", "/pp/dir/k1"),
					List.of("UPDATE", "dir/k1", "PUT", "/pp/dir/k1"));

	@Test
	void listedKeyIsRequestedAndNamedWhole() throws Exception {
		for (final List<String> c : REQUESTS) {
			final String label = c.get(0) + " " + c.get(1);
			final var op = build(OpType.valueOf(c.get(0)), listed(c.get(1)), "/pp", null);
			assertEquals(c.get(2) + " " + c.get(3), request(op), label);
			assertEquals(c.get(3), op.result().item().name(), label + ": the items-file name");
		}
	}

	@Test
	void retryAddressesTheSameKey() throws Exception {
		for (final List<String> c : REQUESTS) {
			if (!List.of("READ", "DELETE").contains(c.get(0))) {
				continue;
			}
			final String label = c.get(0) + " " + c.get(1);
			final var op = build(OpType.valueOf(c.get(0)), listed(c.get(1)), "/pp", null);
			assertEquals(c.get(2) + " " + c.get(3), request(op), label);
			final var retry = op.result();
			retry.reset();
			assertEquals(c.get(2) + " " + c.get(3), request(retry), label + ": the retry");
			assertEquals(c.get(3), retry.result().item().name(), label + ": the retry's items-file name");
		}
	}

	@Test
	void itemsFileReadBackAddressesTheListedKey() throws Exception {
		for (final List<String> c : REQUESTS) {
			if (!List.of("READ", "STAT").contains(c.get(0))) {
				continue;
			}
			final String label = c.get(0) + " " + c.get(1);
			final var op = build(OpType.valueOf(c.get(0)), listed(c.get(1)), "/pp", null);
			final DataItem fromFile = throughItemsFile(op.result().item());
			assertEquals(c.get(3), fromFile.name(), label);
			// a later step reads the items file with no item input path
			final var delete = build(OpType.DELETE, fromFile, null, null);
			assertEquals("DELETE " + c.get(3), request(delete), label + ": a DELETE from the items file");
			assertEquals(c.get(3), delete.result().item().name(), label + ": that DELETE's items-file name");
		}
	}

	@Test
	void copyItemsFileReadBackAddressesTheCopy() throws Exception {
		final List<List<String>> copies = List.of(
						List.of("/k2", "/dst/pp//k2"),
						List.of("a//b", "/dst/pp/a//b"),
						List.of("dir/k1", "/dst/pp/dir/k1"));
		for (final List<String> c : copies) {
			final var copy = build(OpType.CREATE, listed(c.get(0)), "/pp", "/dst");
			assertEquals("PUT " + c.get(1), request(copy), c.get(0));
			final DataItem fromFile = throughItemsFile(copy.result().item());
			assertEquals(c.get(1), fromFile.name(), c.get(0));
			final var read = build(OpType.READ, fromFile, null, null);
			assertEquals("GET " + c.get(1), request(read), c.get(0) + ": a READ of the copy's items file");
			assertEquals(c.get(1), read.result().item().name(), c.get(0) + ": that READ's items-file name");
		}
	}
}
