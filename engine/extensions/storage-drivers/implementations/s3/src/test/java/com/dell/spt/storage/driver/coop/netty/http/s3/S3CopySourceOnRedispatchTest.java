package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationsBuilderImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.system.SizeInBytes;
import io.netty.handler.codec.http.HttpRequest;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * A copy dispatched again, by {@code load.op.retry}, recycle mode or a PENDING redispatch, reads the
 * source its first attempt read, although {@code result()} has rebuilt its item's name under the
 * destination path. Operations are built as the load generator builds them; each further attempt is
 * the previous attempt's {@code result()}, reset, as a retry or recycle dispatches it.
 */
class S3CopySourceOnRedispatchTest {

	private static final String HOST = "127.0.0.1:9020";
	private static final Credential CREDENTIAL = Credential.getInstance("user", "secret");

	private final List<AutoCloseable> resources = new ArrayList<>();

	@AfterEach
	void close() throws Exception {
		for (final AutoCloseable resource : resources) {
			resource.close();
		}
	}

	private S3StorageDriverTest.TestS3Driver driver(final boolean versioning) throws Exception {
		final var driver = new S3StorageDriverTest.TestS3Driver(
						S3StorageDriverTest.baseConfig(versioning, 4, false, null, HOST));
		resources.add(driver);
		return driver;
	}

	@SuppressWarnings({"unchecked", "rawtypes"
	})
	private static Operation<Item> copy(final String name) throws Exception {
		final var item = new DataItemImpl(name, 0, 16);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true));
		final var builder = new DataOperationsBuilderImpl(0);
		builder.opType(OpType.CREATE);
		builder.inputPath("/src");
		builder.outputPathInput(new ConstantValueInputImpl<>("/dst"));
		builder.credentialInput(new ConstantValueInputImpl<>(CREDENTIAL));
		builder.sizeThreshold(Long.MAX_VALUE);
		return (Operation<Item>) builder.buildOp(item);
	}

	@SuppressWarnings("unchecked")
	private static Operation<Item> again(final Operation<Item> op) {
		final var next = (Operation<Item>) op.result();
		next.reset();
		return next;
	}

	/** The destination and source of the request each of three attempts of the copy sends. */
	private static List<String> threeAttempts(final S3StorageDriverTest.TestS3Driver driver, final String name)
					throws Exception {
		final List<String> sent = new ArrayList<>();
		Operation<Item> op = copy(name);
		for (int attempt = 0; attempt < 3; attempt++) {
			if (attempt > 0) {
				op = again(op);
			}
			final HttpRequest request = driver.httpRequest(op, HOST);
			sent.add(request.uri() + " <- " + request.headers().get(S3Api.KEY_X_AMZ_COPY_SOURCE));
		}
		return sent;
	}

	@Test
	void everyAttemptReadsTheListedSource() throws Exception {
		assertEquals(List.of("/dst/src/k0 <- /src/k0", "/dst/src/k0 <- /src/k0", "/dst/src/k0 <- /src/k0"),
						threeAttempts(driver(false), "/src/k0"));
	}

	@Test
	void everyAttemptReadsTheWholeNestedKey() throws Exception {
		assertEquals(
						List.of(
										"/dst/src/dir/sub/k0002 <- /src/dir/sub/k0002",
										"/dst/src/dir/sub/k0002 <- /src/dir/sub/k0002",
										"/dst/src/dir/sub/k0002 <- /src/dir/sub/k0002"),
						threeAttempts(driver(false), "/src/dir/sub/k0002"));
	}

	@Test
	void everyAttemptReadsAnItemsFileNameFromTheSourcePath() throws Exception {
		assertEquals(List.of("/dst/k0001 <- /src/k0001", "/dst/k0001 <- /src/k0001", "/dst/k0001 <- /src/k0001"),
						threeAttempts(driver(false), "k0001"));
	}

	@Test
	void aSourceKeyNamedLikeTheDestinationBucketIsReadAsListed() throws Exception {
		assertEquals(
						List.of("/dst/src/dst/k0000 <- /src/dst/k0000", "/dst/src/dst/k0000 <- /src/dst/k0000",
										"/dst/src/dst/k0000 <- /src/dst/k0000"),
						threeAttempts(driver(false), "/src/dst/k0000"));
	}

	@Test
	void aVersionedCopySentAgainReadsTheSourceWithoutTheVersionsItsResponsesNamed() throws Exception {
		// Under the legacy key~versionId convention, each response names the version of the copy it made.
		final var driver = driver(true);
		final List<String> sent = new ArrayList<>();
		Operation<Item> op = copy("/src/k0");
		for (int attempt = 1; attempt <= 3; attempt++) {
			final HttpRequest request = driver.httpRequest(op, HOST);
			sent.add(request.uri() + " <- " + request.headers().get(S3Api.KEY_X_AMZ_COPY_SOURCE));
			op.item().name(S3StorageDriver.withLegacyVersion(op.item().name(), "v" + attempt));
			op = again(op);
		}
		assertEquals(List.of("/dst/src/k0 <- /src/k0", "/dst/src/k0 <- /src/k0", "/dst/src/k0 <- /src/k0"), sent);
		assertEquals("/dst/src/k0~v3", op.item().name());
	}
}
