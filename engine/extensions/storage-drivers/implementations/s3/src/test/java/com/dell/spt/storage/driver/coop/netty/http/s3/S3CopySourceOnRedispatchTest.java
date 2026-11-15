package com.dell.spt.storage.driver.coop.netty.http.s3;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.dell.spt.base.config.ConstantValueInputImpl;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.data.DataOperationsBuilderImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.system.SizeInBytes;
import com.dell.spt.base.storage.driver.StorageDriverBase;
import com.github.akurilov.commons.io.Output;
import io.netty.handler.codec.http.HttpRequest;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

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

	/**
	 * A driver whose {@code dataUriPath} override routes through a gateway prefix and addresses an
	 * object by the last part of its name, so it gives the same paths for an item before and after
	 * {@code result()} rebuilds the item's name under the destination path.
	 */
	private S3StorageDriverTest.TestS3Driver gatewayDriver(final boolean versioning) throws Exception {
		final var driver = new S3StorageDriverTest.TestS3Driver(
						S3StorageDriverTest.baseConfig(versioning, 4, false, null, HOST)) {
			@Override
			protected String dataUriPath(
							final Item item, final String srcPath, final String dstPath, final OpType opType) {
				final String path = dstPath != null ? dstPath : srcPath;
				final String name = item.name();
				return "/gateway" + path + "/" + name.substring(name.lastIndexOf('/') + 1);
			}
		};
		resources.add(driver);
		return driver;
	}

	/** A driver whose copy source hook prefixes the source S3 resolves, so that a source it decides is told apart. */
	private S3StorageDriverTest.TestS3Driver hookDriver(final boolean versioning) throws Exception {
		final var driver = new S3StorageDriverTest.TestS3Driver(
						S3StorageDriverTest.baseConfig(versioning, 4, false, null, HOST)) {
			@Override
			protected String copySource(final Operation<Item> op) {
				return "/hook" + super.copySource(op);
			}
		};
		resources.add(driver);
		return driver;
	}

	/** The operation a completion hands to the driver's result output. */
	@SuppressWarnings("unchecked")
	private static AtomicReference<Operation<Item>> captureResults(final S3StorageDriverTest.TestS3Driver driver)
					throws Exception {
		final AtomicReference<Operation<Item>> last = new AtomicReference<>();
		final Output<Operation<Item>> output = Mockito.mock(Output.class);
		Mockito.when(output.put(Mockito.<Operation<Item>> any())).thenAnswer(call -> {
			last.set(call.getArgument(0));
			return true;
		});
		final Field field = StorageDriverBase.class.getDeclaredField("opResultOut");
		field.setAccessible(true);
		field.set(driver, output);
		return last;
	}

	@SuppressWarnings({"unchecked", "rawtypes"
	})
	private static Operation<Item> copy(final String name) throws Exception {
		return op(OpType.CREATE, name, "/src");
	}

	@SuppressWarnings({"unchecked", "rawtypes"
	})
	private static Operation<Item> op(final OpType opType, final String name, final String inputPath)
					throws Exception {
		final var item = new DataItemImpl(name, 0, 16);
		item.dataInput(DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true));
		final var builder = new DataOperationsBuilderImpl(0);
		builder.opType(opType);
		builder.inputPath(inputPath);
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

	@Test
	void aCopySentAgainReadsTheSourceThroughTheDriversPathOverride() throws Exception {
		assertGatewayOnEveryAttempt(false);
	}

	@Test
	void aVersionedCopySentAgainReadsTheSourceThroughTheDriversPathOverride() throws Exception {
		assertGatewayOnEveryAttempt(true);
	}

	private void assertGatewayOnEveryAttempt(final boolean versioning) throws Exception {
		assertEquals(
						List.of("/gateway/dst/k0 <- /gateway/src/k0", "/gateway/dst/k0 <- /gateway/src/k0",
										"/gateway/dst/k0 <- /gateway/src/k0"),
						threeAttempts(gatewayDriver(versioning), "/src/k0"));
	}

	@Test
	void theCopySourceHookDecidesTheSourceOnEveryRoute() throws Exception {
		for (final boolean versioning : new boolean[]{false, true
		}) {
			final String route = versioning ? "versioned" : "unversioned";
			assertEquals(
							List.of("/dst/src/k0 <- /hook/src/k0", "/dst/src/k0 <- /hook/src/k0",
											"/dst/src/k0 <- /hook/src/k0"),
							threeAttempts(hookDriver(versioning), "/src/k0"), route);
			assertSourceAfterAFailureBeforeTheRequest(hookDriver(versioning), "/dst/src/k0 <- /hook/src/k0");
		}
	}

	@Test
	void aCopyCompletedAgainAndAgainKeepsReadingItsFirstSource() throws Exception {
		// As recycle mode sends it: each attempt is sent and completes, and its result is sent again.
		final var driver = driver(false);
		final var results = captureResults(driver);
		final List<String> sent = new ArrayList<>();
		Operation<Item> op = copy("/src/k0");
		for (int attempt = 0; attempt < 4; attempt++) {
			final HttpRequest request = driver.httpRequest(op, HOST);
			sent.add(request.uri() + " <- " + request.headers().get(S3Api.KEY_X_AMZ_COPY_SOURCE));
			op.status(Operation.Status.SUCC);
			driver.complete(null, op);
			op = results.get();
			op.reset();
			op.startNextLifecycle();
		}
		assertEquals(List.of("/dst/src/k0 <- /src/k0", "/dst/src/k0 <- /src/k0", "/dst/src/k0 <- /src/k0",
						"/dst/src/k0 <- /src/k0"), sent);
	}

	@Test
	void aCopyThatFailedBeforeItsRequestWasBuiltReadsItsSourceWhenSentAgain() throws Exception {
		assertSourceAfterAFailureBeforeTheRequest(driver(false), "/dst/src/k0 <- /src/k0");
		assertSourceAfterAFailureBeforeTheRequest(driver(true), "/dst/src/k0 <- /src/k0");
	}

	@Test
	void aCopyThatFailedBeforeItsRequestWasBuiltReadsItsSourceThroughTheOverrideWhenSentAgain() throws Exception {
		assertSourceAfterAFailureBeforeTheRequest(gatewayDriver(false), "/gateway/dst/k0 <- /gateway/src/k0");
		assertSourceAfterAFailureBeforeTheRequest(gatewayDriver(true), "/gateway/dst/k0 <- /gateway/src/k0");
	}

	@Test
	void aVersionedCopyOfANamedVersionThatFailedBeforeItsRequestWasBuiltReadsTheSourceItsFirstRequestWouldHave()
					throws Exception {
		// Under the legacy convention a source named k0~v9 is read as the key k0.
		final var driver = driver(true);
		final HttpRequest wouldHave = driver.httpRequest(copy("/src/k0~v9"), HOST);
		assertEquals("/src/k0", wouldHave.headers().get(S3Api.KEY_X_AMZ_COPY_SOURCE));
		final var results = captureResults(driver);
		final Operation<Item> first = copy("/src/k0~v9");
		first.status(Operation.Status.FAIL_IO);
		driver.complete(null, first);
		final Operation<Item> second = results.get();
		second.reset();
		assertEquals("/src/k0", driver.httpRequest(second, HOST).headers().get(S3Api.KEY_X_AMZ_COPY_SOURCE));
	}

	private static void assertSourceAfterAFailureBeforeTheRequest(
					final S3StorageDriverTest.TestS3Driver driver, final String expected) throws Exception {
		final var results = captureResults(driver);
		final Operation<Item> first = copy("/src/k0");
		// As when no connection could be leased: the attempt completes and no request is built.
		first.status(Operation.Status.FAIL_IO);
		driver.complete(null, first);
		final Operation<Item> second = results.get();
		assertEquals("/dst/src/k0", second.item().name());
		second.reset();
		final HttpRequest request = driver.httpRequest(second, HOST);
		assertEquals(expected, request.uri() + " <- " + request.headers().get(S3Api.KEY_X_AMZ_COPY_SOURCE));
	}

	@Test
	void noOtherOperationKeepsASourceWhenItCompletes() throws Exception {
		final var driver = driver(false);
		final var results = captureResults(driver);
		for (final OpType opType : new OpType[]{OpType.READ, OpType.UPDATE, OpType.DELETE, OpType.STAT
		}) {
			final var op = op(opType, "/src/k0", "/src");
			op.status(Operation.Status.FAIL_IO);
			driver.complete(null, op);
			assertNull(results.get().copySource(), opType.name());
		}
		for (final String srcPath : new String[]{null, ""
		}) {
			final var create = op(OpType.CREATE, "k0", srcPath);
			assertEquals(srcPath, create.srcPath());
			create.status(Operation.Status.FAIL_IO);
			driver.complete(null, create);
			assertNull(results.get().copySource(), "CREATE from " + srcPath);
		}
	}
}
