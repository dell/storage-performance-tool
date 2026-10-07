package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.dell.spt.base.config.InitialConfigSchemaProvider;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.env.Extension;
import com.dell.spt.base.integrity.IntegrityMetadataCodec;
import com.dell.spt.base.item.DataItem;
import com.dell.spt.base.item.DataItemImpl;
import com.dell.spt.base.item.op.OpType;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperation;
import com.dell.spt.base.item.op.composite.data.CompositeDataOperationImpl;
import com.github.akurilov.commons.collection.Range;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.storage.Credential;
import com.github.akurilov.commons.collection.TreeUtil;
import com.github.akurilov.commons.io.Input;
import com.github.akurilov.commons.io.Output;
import com.github.akurilov.commons.system.SizeInBytes;
import com.github.akurilov.confuse.Config;
import com.github.akurilov.confuse.SchemaProvider;
import com.github.akurilov.confuse.impl.BasicConfig;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Drives the real S3-RDMA driver against a loopback HTTP server that answers like an RDMA-capable
 * S3 endpoint. The server emulates the one-sided transfer through {@link FakeRdmaTransport}: for a
 * PUT it reads the client's registered buffer, for a GET it writes into it. Each test then checks
 * the operation status, the bytes accounted, and the data-path counters.
 */
final class S3RdmaReplyLoopbackTest {

	private static final long RESULT_TIMEOUT_SECONDS = 5;
	private static final Credential CREDENTIAL = Credential.getInstance("access", "secret");
	private static final int SIZE = 4096;
	private static final long THRESHOLD = 1024;
	/** Part retries allowed by {@code CoopStorageDriverBase.MAX_PART_RETRIES}. */
	private static final int MAX_PART_RETRIES = 3;
	private static final String OBJECT_BODY = "x".repeat(SIZE);
	private static final int NO_RESPONSE = 0;
	private static final String DECLINE_BODY = "<Error><Code>RDMANotSupported</Code><Message>RDMA not available</Message></Error>";

	private final List<CapturedRequest> requests = new CopyOnWriteArrayList<>();
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	private HttpServer server;
	private FakeRdmaTransport transport;
	private volatile Reply reply;
	private volatile byte[] serverReadPayload;
	private volatile boolean chunkedReplies;
	/** Byte the emulated server writes into client memory for an RDMA GET. */
	private static final byte SERVER_GET_FILL = 0x5a;
	/** Extra headers on object responses, such as integrity metadata. */
	private volatile Map<String, String> objectResponseHeaders = Map.of();
	private final List<Integer> serverReadSizes = new CopyOnWriteArrayList<>();

	/**
	 * How the emulated server answers an object request carrying an RDMA token. An
	 * {@code httpStatus} of {@link #NO_RESPONSE} closes the connection without answering.
	 */
	private record Reply(
					int httpStatus,
					String rdmaReply,
					String bytesTransferred,
					String body,
					boolean performTransfer) {}

	private record CapturedRequest(
					String method,
					String rawPath,
					String rawQuery,
					String rdmaToken,
					String contentLength,
					int bodyLength) {}

	@BeforeEach
	void startServer() throws IOException {
		server = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
		server.createContext("/", this::handle);
		server.setExecutor(executor);
		server.start();
	}

	@AfterEach
	void stopServer() {
		if (server != null) {
			server.stop(0);
		}
		executor.shutdownNow();
	}

	// ---------- PUT ----------

	@Test
	void putTransferredWhenServerReportsRdmaSuccess() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, op(OpType.CREATE, SIZE));

			assertEquals(Operation.Status.SUCC, result.status());
			assertEquals(SIZE, ((DataOperation<?>) result).countBytesDone());
			final CapturedRequest put = onlyObjectRequest("PUT");
			assertNotNull(put.rdmaToken());
			assertEquals("0", put.contentLength());
			assertEquals(0, put.bodyLength());
			assertNotNull(serverReadPayload, "server should read the payload from registered memory");
			assertEquals(SIZE, serverReadPayload.length);
			assertEquals(1, driver.pathStats().rdmaTransferred.sum());
			assertNoBufferInUse(driver);
		}
	}

	@Test
	void putDeclinedWithHttp200AndReply501FailsWithoutBytes() throws Exception {
		// The response observed from an ECS 4.5 server while its RDMA server could not start.
		reply = new Reply(200, "501", null, DECLINE_BODY, false);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, op(OpType.CREATE, SIZE));

			assertEquals(Operation.Status.RESP_FAIL_SVC, result.status());
			assertEquals(0, ((DataOperation<?>) result).countBytesDone());
			assertEquals(1, driver.pathStats().rdmaDeclined.sum());
			assertEquals(0, driver.pathStats().rdmaTransferred.sum());
			assertNoBufferInUse(driver);
		}
	}

	@Test
	void putWithoutReplyHeaderIsDeclined() throws Exception {
		reply = new Reply(200, null, null, null, false);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, op(OpType.CREATE, SIZE));

			assertEquals(Operation.Status.RESP_FAIL_SVC, result.status());
			assertEquals(1, driver.pathStats().rdmaDeclined.sum());
		}
	}

	@Test
	void putTransferFailureStatusIsHttpError() throws Exception {
		reply = new Reply(500, null, null, "<Error><Code>RDMATransferFailed</Code></Error>", false);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, op(OpType.CREATE, SIZE));

			assertEquals(Operation.Status.RESP_FAIL_SVC, result.status());
			assertEquals(1, driver.pathStats().rdmaHttpError.sum());
			assertEquals(0, driver.pathStats().rdmaDeclined.sum());
			assertNoBufferInUse(driver);
		}
	}

	// ---------- GET ----------

	@Test
	void getTransferredCountsReportedBytesAndKeepsWrittenData() throws Exception {
		reply = new Reply(200, "200", Integer.toString(SIZE), null, true);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, op(OpType.READ, SIZE));

			assertEquals(Operation.Status.SUCC, result.status());
			assertEquals(SIZE, ((DataOperation<?>) result).countBytesDone());
			final CapturedRequest get = onlyObjectRequest("GET");
			assertNotNull(get.rdmaToken());
			assertEquals(1, driver.pathStats().rdmaTransferred.sum());
			assertEquals(0, driver.pathStats().rdmaBytesAssumed.sum());
		}
	}

	@Test
	void getDeclinedBodyIsCountedOnceWhenFallbackAllowed() throws Exception {
		reply = new Reply(200, "501", null, OBJECT_BODY, false);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, op(OpType.READ, SIZE));

			assertEquals(Operation.Status.SUCC, result.status());
			assertEquals(SIZE, ((DataOperation<?>) result).countBytesDone(),
							"HTTP body bytes must not be added to the RDMA buffer size");
			assertEquals(1, driver.pathStats().rdmaDeclined.sum());
		}
	}

	@Test
	void getDeclinedFailsWhenFallbackDisabled() throws Exception {
		reply = new Reply(200, "501", null, OBJECT_BODY, false);
		try (final var driver = newDriver(false)) {
			final var result = execute(driver, op(OpType.READ, SIZE));

			assertEquals(Operation.Status.RESP_FAIL_SVC, result.status());
			assertEquals(0, ((DataOperation<?>) result).countBytesDone());
		}
	}

	@Test
	void getShortTransferIsProtocolError() throws Exception {
		reply = new Reply(200, "200", Integer.toString(SIZE - 1), null, true);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, op(OpType.READ, SIZE));

			assertEquals(Operation.Status.RESP_FAIL_CORRUPT, result.status());
			assertEquals(0, ((DataOperation<?>) result).countBytesDone());
			assertEquals(1, driver.pathStats().rdmaProtocolError.sum());
		}
	}

	@Test
	void getRdmaSuccessWithHttpBodyIsProtocolError() throws Exception {
		reply = new Reply(200, "200", Integer.toString(SIZE), OBJECT_BODY, true);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, op(OpType.READ, SIZE));

			assertEquals(Operation.Status.RESP_FAIL_CORRUPT, result.status());
			assertEquals(1, driver.pathStats().rdmaProtocolError.sum());
		}
	}

	@Test
	void getWithoutBytesHeaderIsProtocolErrorByDefault() throws Exception {
		// Server reports RDMA success but omits the byte count and writes nothing into client memory.
		reply = new Reply(200, "200", null, null, false);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, op(OpType.READ, SIZE));

			assertEquals(Operation.Status.RESP_FAIL_CORRUPT, result.status());
			assertEquals(0, ((DataOperation<?>) result).countBytesDone());
			assertEquals(1, driver.pathStats().rdmaProtocolError.sum());
			assertEquals(0, driver.pathStats().rdmaBytesAssumed.sum());
		}
	}

	@Test
	void legacyOptInCountsRequestedSizeWithoutBytesHeader() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		try (final var driver = newDriver(true, true)) {
			final var result = execute(driver, op(OpType.READ, SIZE));

			assertEquals(Operation.Status.SUCC, result.status());
			assertEquals(SIZE, ((DataOperation<?>) result).countBytesDone());
			assertEquals(1, driver.pathStats().rdmaBytesAssumed.sum());
		}
	}

	@Test
	void chunkedBodyOnAcceptedRdmaGetIsProtocolError() throws Exception {
		chunkedReplies = true;
		reply = new Reply(200, "200", Integer.toString(SIZE), "unexpected body", true);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, op(OpType.READ, SIZE));

			assertEquals(Operation.Status.RESP_FAIL_CORRUPT, result.status());
			assertEquals(1, driver.pathStats().rdmaProtocolError.sum());
		}
	}

	@Test
	void chunkedBodyOnAcceptedRdmaPutIsProtocolError() throws Exception {
		chunkedReplies = true;
		reply = new Reply(200, "200", null, "unexpected body", true);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, op(OpType.CREATE, SIZE));

			assertEquals(Operation.Status.RESP_FAIL_CORRUPT, result.status());
			assertEquals(1, driver.pathStats().rdmaProtocolError.sum());
		}
	}

	// ---------- Preparation fallback and eligibility ----------

	@Test
	void registrationFailureWithoutFallbackFailsBeforeSending() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		try (final var driver = newDriver(false)) {
			transport.setFailAfterNRegistrations(0);
			final var result = execute(driver, op(OpType.CREATE, SIZE));

			assertEquals(Operation.Status.FAIL_IO, result.status());
			assertTrue(objectRequests().isEmpty(), "no request may be sent: " + requests);
			assertEquals(1, driver.pathStats().prepareFailed.sum());
		}
	}

	@Test
	void exhaustedDirectMemoryFailsTheOperationInsteadOfThrowing() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		try (final var driver = newDriver(false, config -> config.val("storage-rdma-bufferPool", false))) {
			driver.directAllocator = size -> {
				throw new OutOfMemoryError("Cannot reserve " + size + " bytes of direct buffer memory");
			};
			final var result = execute(driver, op(OpType.CREATE, SIZE));

			assertEquals(Operation.Status.FAIL_IO, result.status());
			assertTrue(objectRequests().isEmpty(), "no request may be sent: " + requests);
			assertEquals(1, driver.pathStats().prepareFailed.sum());
		}
	}

	@Test
	void exhaustedDirectMemoryWithFallbackSendsHttpBody() throws Exception {
		reply = new Reply(200, null, null, null, false);
		try (final var driver = newDriver(true, config -> config.val("storage-rdma-bufferPool", false))) {
			driver.directAllocator = size -> {
				throw new OutOfMemoryError("Cannot reserve " + size + " bytes of direct buffer memory");
			};
			final var result = execute(driver, op(OpType.CREATE, SIZE));

			assertEquals(Operation.Status.SUCC, result.status());
			assertNull(onlyObjectRequest("PUT").rdmaToken());
			assertEquals(1, driver.pathStats().httpFallback.sum());
		}
	}

	@Test
	void registrationFailureWithFallbackSendsHttpBody() throws Exception {
		reply = new Reply(200, null, null, null, false);
		try (final var driver = newDriver(true)) {
			transport.setFailAfterNRegistrations(0);
			final var result = execute(driver, op(OpType.CREATE, SIZE));

			assertEquals(Operation.Status.SUCC, result.status());
			final CapturedRequest put = onlyObjectRequest("PUT");
			assertNull(put.rdmaToken());
			assertEquals(SIZE, put.bodyLength());
			assertEquals(1, driver.pathStats().httpFallback.sum());
		}
	}

	@Test
	void belowThresholdUsesHttpAndIsCounted() throws Exception {
		reply = new Reply(200, null, null, null, false);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, op(OpType.CREATE, THRESHOLD - 1));

			assertEquals(Operation.Status.SUCC, result.status());
			final CapturedRequest put = onlyObjectRequest("PUT");
			assertNull(put.rdmaToken());
			assertEquals(THRESHOLD - 1, put.bodyLength());
			assertEquals(1, driver.pathStats().httpBelowThreshold.sum());
			assertEquals(0, transport.getRegisterCount());
		}
	}

	@Test
	void fixedRangeReadUsesHttpAndCountsTheRange() throws Exception {
		reply = new Reply(200, "200", Integer.toString(SIZE), null, true);
		try (final var driver = newDriver(false)) {
			@SuppressWarnings("unchecked")
			final Operation<DataItem> ranged = (Operation<DataItem>) (Operation<?>) new DataOperationImpl<>(
							0, OpType.READ, new DataItemImpl("obj-range", 0, SIZE), null, "/bucket", CREDENTIAL,
							List.of(new Range(0L, SIZE / 2 - 1, -1L)), 0);
			final var result = execute(driver, ranged);

			assertEquals(Operation.Status.SUCC, result.status());
			assertEquals(SIZE / 2, ((DataOperation<?>) result).countBytesDone());
			assertNull(onlyObjectRequest("GET").rdmaToken());
			assertEquals(1, driver.pathStats().httpIneligible.sum());
			assertEquals(0, transport.getRegisterCount());
		}
	}

	@Test
	void randomRangeReadIsNotProposedForRdma() throws Exception {
		try (final var driver = newDriver(false)) {
			@SuppressWarnings("unchecked")
			final Operation<DataItem> ranged = (Operation<DataItem>) (Operation<?>) new DataOperationImpl<>(
							0, OpType.READ, new DataItemImpl("obj-random", 0, SIZE), null, "/bucket", CREDENTIAL,
							null, 1);
			final Method eligible = S3RdmaStorageDriver.class.getDeclaredMethod("shouldUseRdma", Operation.class);
			eligible.setAccessible(true);

			assertEquals(false, eligible.invoke(driver, ranged));
			assertEquals(1, driver.pathStats().httpIneligible.sum());
		}
	}

	// ---------- Buffer pool ----------

	@Test
	void pooledBufferIsReusedAfterTheResponse() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		try (final var driver = newDriver(false)) {
			final var results = executeInSequence(driver, List.of(op(OpType.CREATE, SIZE), op(OpType.CREATE, SIZE)));

			results.forEach(result -> assertEquals(Operation.Status.SUCC, result.status()));
			assertEquals(List.of(SIZE, SIZE), serverReadSizes);
			assertEquals(1, transport.getRegisterCount(), "the second PUT reuses the first PUT's registration");
			assertEquals(1, driver.bufferPool().hits.sum());
			assertNoBufferInUse(driver);
		}
	}

	@Test
	void disabledPoolRegistersAndReleasesEachOperation() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		try (final var driver = newDriver(false, config -> config.val("storage-rdma-bufferPool", false))) {
			final var results = executeInSequence(driver, List.of(op(OpType.CREATE, SIZE), op(OpType.CREATE, SIZE)));

			results.forEach(result -> assertEquals(Operation.Status.SUCC, result.status()));
			assertNull(driver.bufferPool());
			assertEquals(2, transport.getRegisterCount());
			assertTrue(transport.areAllDeregistered());
		}
	}

	@Test
	void closeDeregistersIdlePooledBuffers() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		final RdmaBufferPool pool;
		try (final var driver = newDriver(false)) {
			assertEquals(Operation.Status.SUCC, execute(driver, op(OpType.CREATE, SIZE)).status());
			pool = driver.bufferPool();
			assertEquals(1, pool.idleBuffers());
		}
		assertEquals(0, pool.liveBuffers());
		assertEquals(transport.getRegisterCount(), transport.getDeregisterCount());
	}

	@Test
	void bufferOfARequestWithoutResponseIsNeverReused() throws Exception {
		// The server might still access memory it never answered for.
		reply = new Reply(NO_RESPONSE, null, null, null, false);
		try (final var driver = newDriver(false)) {
			final var dropped = execute(driver, op(OpType.CREATE, SIZE));
			assertTrue(dropped.status() != Operation.Status.SUCC, "dropped request status: " + dropped.status());
			assertEquals(1, driver.bufferPool().discarded.sum());
			assertEquals(0, driver.bufferPool().liveBuffers());
			assertEquals(1, transport.getDeregisterCount());
		}
	}

	@Test
	void verifiedGetOnAReusedBufferDoesNotPassOnStaleContent() throws Exception {
		// Reading the same object repeatedly: GETs the server claims without writing must fail
		// however often they repeat, although the first read left the correct content behind.
		integrityHeaders(SIZE);
		try (final var driver = newDriver(false, S3RdmaReplyLoopbackTest::integrityMode)) {
			final ResultOutput output = startWith(driver);

			final Operation<DataItem> filled = read(driver, output, SIZE, new Reply(200, "200", Integer.toString(SIZE), null, true));
			assertEquals(Operation.Status.SUCC, filled.status());
			assertTrue(filled.integrityVerificationResult().verified());

			for (int attempt = 1; attempt <= 2; attempt++) {
				final Operation<DataItem> unwritten =
								read(driver, output, SIZE, new Reply(200, "200", Integer.toString(SIZE), null, false));
				assertEquals(Operation.Status.RESP_FAIL_CORRUPT, unwritten.status(), "unwritten GET " + attempt);
			}
			assertEquals(1, driver.bufferPool().hits.sum(), "only the verified fill's buffer is reused");
			assertEquals(2, driver.bufferPool().discarded.sum(), "each failed read's buffer is discarded");
		}
	}

	@Test
	void declinedGetDoesNotReturnAnInvalidatedBufferToThePool() throws Exception {
		// A declined GET delivers its body over HTTP and leaves its invalidated buffer unwritten.
		integrityHeaders(SIZE);
		try (final var driver = newDriver(true, S3RdmaReplyLoopbackTest::integrityMode)) {
			final ResultOutput output = startWith(driver);

			assertEquals(Operation.Status.SUCC,
							read(driver, output, SIZE, new Reply(200, "200", Integer.toString(SIZE), null, true)).status());
			final String body = new String(new byte[SIZE], StandardCharsets.ISO_8859_1).replace('\0', (char) SERVER_GET_FILL);
			assertEquals(Operation.Status.SUCC,
							read(driver, output, SIZE, new Reply(200, "501", null, body, false)).status());
			assertEquals(Operation.Status.RESP_FAIL_CORRUPT,
							read(driver, output, SIZE, new Reply(200, "200", Integer.toString(SIZE), null, false)).status());
			assertEquals(1, driver.bufferPool().hits.sum());
			assertEquals(2, driver.bufferPool().discarded.sum(), "the declined and the failed reads' buffers");
		}
	}

	@Test
	void informationalResponseDoesNotReturnTheBufferToThePool() throws Exception {
		// The server may access the buffer until its final response; a 1xx does not end the request.
		try (final ServerSocket raw = new ServerSocket(0, 0, InetAddress.getLoopbackAddress());
				final var driver = newDriver(false, config -> config.val("storage-net-node-port", raw.getLocalPort()))) {
			final Thread responder = Thread.ofVirtual().start(() -> serveContinueThenOk(raw));
			try {
				execute(driver, op(OpType.CREATE, SIZE));

				assertEquals(0, driver.bufferPool().idleBuffers(), driver.bufferPool().summary());
				assertEquals(1, driver.bufferPool().discarded.sum(), driver.bufferPool().summary());
			} finally {
				raw.close();
				responder.join(TimeUnit.SECONDS.toMillis(RESULT_TIMEOUT_SECONDS));
			}
		}
	}

	private static void integrityMode(final Config config) {
		config.val("storage-driver-type", "s3-rdma");
		config.val("storage-integrity-mode", "metadata");
		config.val("storage-integrity-input-provenance", "external");
	}

	/** Integrity metadata for an object of {@code size} bytes of {@link #SERVER_GET_FILL}. */
	private void integrityHeaders(final int size) throws Exception {
		final byte[] content = new byte[size];
		Arrays.fill(content, SERVER_GET_FILL);
		final String digest = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(content));
		objectResponseHeaders = Map.of(
						IntegrityMetadataCodec.HTTP_PREFIX + IntegrityMetadataCodec.KEY_VERSION, "1",
						IntegrityMetadataCodec.HTTP_PREFIX + IntegrityMetadataCodec.KEY_ALGORITHM, "sha256",
						IntegrityMetadataCodec.HTTP_PREFIX + IntegrityMetadataCodec.KEY_DIGEST, digest,
						IntegrityMetadataCodec.HTTP_PREFIX + IntegrityMetadataCodec.KEY_SIZE, Integer.toString(size));
	}

	private static ResultOutput startWith(final S3RdmaStorageDriver<DataItem, Operation<DataItem>> driver)
					throws Exception {
		final ResultOutput output = new ResultOutput();
		driver.operationResultOutput(output);
		driver.start();
		return output;
	}

	private Operation<DataItem> read(
					final S3RdmaStorageDriver<DataItem, Operation<DataItem>> driver, final ResultOutput output,
					final int size, final Reply serverReply) throws Exception {
		reply = serverReply;
		assertTrue(driver.put(op(OpType.READ, size)));
		final Operation<DataItem> result = output.await();
		assertNotNull(result, "read did not complete");
		return result;
	}

	/**
	 * Minimal HTTP/1.1 server: object requests get {@code 100 Continue} and later the final RDMA
	 * success; other requests get an empty 200.
	 */
	private static void serveContinueThenOk(final ServerSocket server) {
		while (!server.isClosed()) {
			try (Socket socket = server.accept()) {
				final var in = new java.io.BufferedInputStream(socket.getInputStream());
				final var out = socket.getOutputStream();
				String head;
				while ((head = readHead(in)) != null) {
					final String path = head.split(" ", 3)[1];
					final boolean object = path.split("\\?", 2)[0].chars().filter(c -> c == '/').count() > 1;
					if (object) {
						out.write("HTTP/1.1 100 Continue\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
						out.flush();
						Thread.sleep(200);
						out.write(("HTTP/1.1 200 OK\r\n" + RdmaReplyContract.REPLY_HEADER
										+ ": 200\r\nContent-Length: 0\r\n\r\n").getBytes(StandardCharsets.US_ASCII));
					} else {
						out.write("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
					}
					out.flush();
				}
			} catch (final IOException | InterruptedException e) {
				// the server was closed or the client went away
			}
		}
	}

	/** Reads one request head and discards its body; {@code null} at end of stream. */
	private static String readHead(final java.io.InputStream in) throws IOException {
		final StringBuilder head = new StringBuilder();
		int c;
		while ((c = in.read()) >= 0) {
			head.append((char) c);
			if (head.length() >= 4 && head.substring(head.length() - 4).equals("\r\n\r\n")) {
				final java.util.regex.Matcher length = java.util.regex.Pattern
								.compile("(?im)^content-length:\\s*(\\d+)").matcher(head);
				if (length.find()) {
					in.readNBytes(Integer.parseInt(length.group(1)));
				}
				return head.toString();
			}
		}
		return null;
	}

	// ---------- Multipart upload ----------

	@Test
	void uploadPartsEachCarryTheirOwnPartSizedToken() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		final int partSize = 2048;
		final int parts = 2;
		try (final var driver = newDriver(true)) {
			final var item = new DataItemImpl("obj", 0, (long) partSize * parts);
			@SuppressWarnings("unchecked")
			final Operation<DataItem> mpu = (Operation<DataItem>) (Operation<?>) new CompositeDataOperationImpl<>(
							0, OpType.CREATE, item, null, "/bucket", CREDENTIAL, null, 0, partSize);
			final var result = executeUntil(driver, mpu, done -> done instanceof CompositeDataOperation<?> composite
							&& composite.allSubOperationsDone());

			assertEquals(Operation.Status.SUCC, result.status(), "requests: " + requests);
			final List<CapturedRequest> partRequests = objectRequests().stream()
							.filter(request -> "PUT".equals(request.method())
											&& request.rawQuery() != null
											&& request.rawQuery().contains("partNumber="))
							.toList();
			assertEquals(parts, partRequests.size(), "requests: " + requests);
			for (final CapturedRequest part : partRequests) {
				assertTrue(part.rawQuery().contains("uploadId=upload-1"), part.rawQuery());
				assertNotNull(part.rdmaToken(), "part must be proposed for RDMA");
				assertEquals(partSize, Integer.parseInt(part.rdmaToken().split(":", -1)[1], 16),
								"token must describe the part, not the whole object");
				assertEquals("0", part.contentLength());
				assertEquals(0, part.bodyLength());
			}
			final List<CapturedRequest> controlRequests = objectRequests().stream()
							.filter(request -> "POST".equals(request.method()))
							.toList();
			assertEquals(2, controlRequests.size(), "initiate and complete: " + requests);
			controlRequests.forEach(request -> assertNull(request.rdmaToken(),
							"multipart control requests carry no payload: " + request));
			assertEquals(List.of(partSize, partSize), serverReadSizes);
			assertEquals(parts, driver.pathStats().rdmaTransferred.sum());
			assertNoBufferInUse(driver);
		}
	}

	@Test
	void partPreparationFailuresExhaustRetriesAndAbortTheUpload() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		try (final var driver = newDriver(false)) {
			transport.setFailAfterNRegistrations(0);
			final var upload = multipartUpload(2048, 2);
			final var result = executeUntil(driver, upload, S3RdmaReplyLoopbackTest::uploadTerminal);

			assertTrue(result.status() != Operation.Status.SUCC, "upload must fail: " + result.status());
			assertTrue(objectRequests().stream().noneMatch(r -> r.rawQuery() != null && r.rawQuery().contains("partNumber=")),
							"no part may be sent: " + requests);
			assertTrue(abortRequested(), "upload must be aborted: " + requests);
			// One part exhausts its retries; the upload is then aborted before the second part runs.
			assertEquals(1L + MAX_PART_RETRIES, driver.pathStats().prepareFailed.sum());
			assertEquals(1, pendingParts(upload),
							"every failed attempt is settled; only the never-dispatched second part remains pending");
		}
	}

	@Test
	void partRetryThatFailsPreparationAfterADeclinedAttemptStillSettles() throws Exception {
		// First attempt of each part reaches the server (declined, so its response timing is set);
		// every retry then fails preparation. The retried part keeps the stale response timing.
		reply = new Reply(200, "501", null, DECLINE_BODY, false);
		// Per-operation buffers: a pool would reuse the declined attempt's buffer and register nothing.
		try (final var driver = newDriver(false, config -> config.val("storage-rdma-bufferPool", false))) {
			transport.setFailAfterNRegistrations(2);
			final var upload = multipartUpload(2048, 2);
			final var result = executeUntil(driver, upload, S3RdmaReplyLoopbackTest::uploadTerminal);

			assertTrue(result.status() != Operation.Status.SUCC, "upload must fail: " + result.status());
			assertTrue(abortRequested(), "upload must be aborted: " + requests);
			assertEquals(2, driver.pathStats().rdmaDeclined.sum());
			assertEquals(1L + MAX_PART_RETRIES - 2, driver.pathStats().prepareFailed.sum(),
							"the retries after the two declined attempts fail preparation");
			assertEquals(1, pendingParts(upload),
							"every failed attempt is settled; only the never-dispatched second part remains pending");
		}
	}

	/** An upload is terminal when every part is done or it failed (an abort need not wait for parts). */
	private static boolean uploadTerminal(final Operation<DataItem> result) {
		return result instanceof CompositeDataOperation<?> composite
						&& (composite.allSubOperationsDone() || composite.status() != Operation.Status.SUCC);
	}

	/** Live parent's pending sub-task count (parts reference this instance, not result copies). */
	private static int pendingParts(final Operation<DataItem> upload) throws Exception {
		final Field field = CompositeDataOperationImpl.class.getDeclaredField("pendingSubTasksCount");
		field.setAccessible(true);
		return ((java.util.concurrent.atomic.AtomicInteger) field.get(upload)).get();
	}

	private boolean abortRequested() {
		return objectRequests().stream().anyMatch(r -> "DELETE".equals(r.method())
						&& r.rawQuery() != null && r.rawQuery().contains("uploadId=upload-1"));
	}

	@SuppressWarnings("unchecked")
	private static Operation<DataItem> multipartUpload(final int partSize, final int parts) {
		return (Operation<DataItem>) (Operation<?>) new CompositeDataOperationImpl<>(
						0, OpType.CREATE, new DataItemImpl("obj", 0, (long) partSize * parts), null, "/bucket",
						CREDENTIAL, null, 0, partSize);
	}

	// ---------- Emulated server ----------

	private void handle(final HttpExchange exchange) throws IOException {
		final byte[] requestBody = exchange.getRequestBody().readAllBytes();
		final String token = exchange.getRequestHeaders().getFirst(S3RdmaStorageDriver.RDMA_TOKEN_HEADER);
		final String method = exchange.getRequestMethod();
		final String path = exchange.getRequestURI().getRawPath();
		final String query = exchange.getRequestURI().getRawQuery();
		requests.add(new CapturedRequest(
						method, path, query, token,
						exchange.getRequestHeaders().getFirst("Content-Length"), requestBody.length));
		final boolean objectRequest = path.chars().filter(c -> c == '/').count() > 1;
		if (!objectRequest || token == null) {
			plainResponse(exchange, method, query, exchange.getRequestHeaders().getFirst("Range"));
			return;
		}
		final Reply current = reply;
		if (current.performTransfer()) {
			// The server accesses exactly the region the token describes.
			final ByteBuffer clientMemory = transport.getRegisteredBuffer(rkeyHandle(token)).duplicate();
			clientMemory.clear().limit(tokenSize(token));
			if ("PUT".equals(method)) {
				final byte[] payload = new byte[clientMemory.remaining()];
				clientMemory.get(payload);
				serverReadPayload = payload;
				serverReadSizes.add(payload.length);
			} else {
				while (clientMemory.hasRemaining()) {
					clientMemory.put(SERVER_GET_FILL);
				}
			}
		}
		if (current.httpStatus() == NO_RESPONSE) {
			// Closing without sending headers makes the JDK server drop the connection.
			exchange.close();
			return;
		}
		objectResponseHeaders.forEach((name, value) -> exchange.getResponseHeaders().set(name, value));
		if (current.rdmaReply() != null) {
			exchange.getResponseHeaders().set(RdmaReplyContract.REPLY_HEADER, current.rdmaReply());
		}
		if (query != null && query.contains("partNumber=")) {
			exchange.getResponseHeaders().set("ETag", "\"etag-" + query.hashCode() + "\"");
		}
		if (current.bytesTransferred() != null) {
			exchange.getResponseHeaders().set(
							RdmaReplyContract.BYTES_TRANSFERRED_HEADER, current.bytesTransferred());
		}
		final byte[] body = current.body() == null ? null : current.body().getBytes(StandardCharsets.UTF_8);
		// A zero length makes the JDK server use chunked transfer encoding (no Content-Length).
		exchange.sendResponseHeaders(current.httpStatus(), body == null ? -1 : chunkedReplies ? 0 : body.length);
		if (body != null) {
			exchange.getResponseBody().write(body);
		}
		exchange.close();
	}

	private static void plainResponse(
					final HttpExchange exchange, final String method, final String query, final String range)
					throws IOException {
		if ("POST".equals(method) && "uploads".equals(query)) {
			xmlResponse(exchange, "<InitiateMultipartUploadResult><Bucket>bucket</Bucket>"
							+ "<Key>obj</Key><UploadId>upload-1</UploadId></InitiateMultipartUploadResult>");
		} else if ("POST".equals(method) && query != null && query.startsWith("uploadId=")) {
			xmlResponse(exchange, "<CompleteMultipartUploadResult><Bucket>bucket</Bucket>"
							+ "<Key>obj</Key><ETag>\"mpu-etag\"</ETag></CompleteMultipartUploadResult>");
		} else if ("GET".equals(method) && range != null) {
			// Single range "bytes=a-b", as produced by the tests' fixed ranges.
			final String[] bounds = range.substring("bytes=".length()).split("-", -1);
			final long first = Long.parseLong(bounds[0]);
			final long last = Long.parseLong(bounds[1]);
			final byte[] body = new byte[(int) (last - first + 1)];
			exchange.getResponseHeaders().set("Content-Range", "bytes " + first + "-" + last + "/" + SIZE);
			exchange.sendResponseHeaders(206, body.length);
			exchange.getResponseBody().write(body);
		} else if ("GET".equals(method)) {
			final byte[] body = new byte[SIZE];
			exchange.sendResponseHeaders(200, body.length);
			exchange.getResponseBody().write(body);
		} else {
			exchange.sendResponseHeaders(200, -1);
		}
		exchange.close();
	}

	private static void xmlResponse(final HttpExchange exchange, final String xml) throws IOException {
		final byte[] body = xml.getBytes(StandardCharsets.UTF_8);
		exchange.getResponseHeaders().set("Content-Type", "application/xml");
		exchange.sendResponseHeaders(200, body.length);
		exchange.getResponseBody().write(body);
		exchange.close();
	}

	/** {@link FakeRdmaTransport} tokens carry the registration handle in the rkey field. */
	private static long rkeyHandle(final String token) {
		return Long.parseLong(token.split(":", -1)[2], 16);
	}

	private static int tokenSize(final String token) {
		return Integer.parseInt(token.split(":", -1)[1], 16);
	}

	// ---------- Helpers ----------

	private List<CapturedRequest> objectRequests() {
		return requests.stream()
						.filter(request -> request.rawPath().chars().filter(c -> c == '/').count() > 1)
						.toList();
	}

	private CapturedRequest onlyObjectRequest(final String method) {
		final List<CapturedRequest> matching = objectRequests().stream()
						.filter(request -> method.equals(request.method()))
						.toList();
		assertEquals(1, matching.size(), "object requests: " + requests);
		return matching.get(0);
	}

	@SuppressWarnings("unchecked")
	private static Operation<DataItem> op(final OpType opType, final long size) {
		return (Operation<DataItem>) (Operation<?>) new DataOperationImpl<>(
						0, opType, new DataItemImpl("obj-" + opType, 0, size), null, "/bucket",
						CREDENTIAL, null, 0);
	}

	private S3RdmaStorageDriver<DataItem, Operation<DataItem>> newDriver(final boolean fallback)
					throws Exception {
		return newDriver(fallback, false);
	}

	private S3RdmaStorageDriver<DataItem, Operation<DataItem>> newDriver(
					final boolean fallback, final boolean allowMissingBytesHeader) throws Exception {
		return newDriver(fallback, config -> config.val("storage-rdma-allowMissingBytesHeader", allowMissingBytesHeader));
	}

	private S3RdmaStorageDriver<DataItem, Operation<DataItem>> newDriver(
					final boolean fallback, final Consumer<Config> customize) throws Exception {
		final Config config = config(fallback);
		customize.accept(config);
		final RdmaConfig rdmaConfig = new RdmaConfig(config.configVal("storage").configVal("rdma"));
		transport = new FakeRdmaTransport(rdmaConfig);
		return new S3RdmaStorageDriver<>(
						"s3-rdma-reply-loopback",
						DataInput.instance(null, "7a42d9c483244167", new SizeInBytes("64KB"), 16, false, 0.0, true),
						config.configVal("storage"),
						false,
						config.intVal("load-batch-size"),
						ignored -> transport);
	}

	private static Operation<DataItem> execute(
					final S3RdmaStorageDriver<DataItem, Operation<DataItem>> driver,
					final Operation<DataItem> operation) throws Exception {
		final ResultOutput output = new ResultOutput();
		driver.operationResultOutput(output);
		driver.start();
		assertTrue(driver.put(operation));
		final Operation<DataItem> result = output.await();
		assertNotNull(result, "operation did not complete");
		return result;
	}

	/** Runs operations one after another, so each starts after the previous one completed. */
	private static List<Operation<DataItem>> executeInSequence(
					final S3RdmaStorageDriver<DataItem, Operation<DataItem>> driver,
					final List<Operation<DataItem>> operations) throws Exception {
		final ResultOutput output = new ResultOutput();
		driver.operationResultOutput(output);
		driver.start();
		final List<Operation<DataItem>> results = new java.util.ArrayList<>();
		for (final Operation<DataItem> operation : operations) {
			assertTrue(driver.put(operation));
			final Operation<DataItem> result = output.await();
			assertNotNull(result, "operation did not complete");
			results.add(result);
		}
		return results;
	}

	/** Every registered buffer is either deregistered or idle in the pool; none is held by a request. */
	private void assertNoBufferInUse(final S3RdmaStorageDriver<?, ?> driver) {
		final RdmaBufferPool pool = driver.bufferPool();
		if (pool == null) {
			assertTrue(transport.areAllDeregistered());
			return;
		}
		assertEquals(pool.idleBuffers(), pool.liveBuffers(), pool.summary());
		assertEquals(pool.idleBuffers(), transport.getActiveRegistrationCount(), pool.summary());
	}

	private Operation<DataItem> executeUntil(
					final S3RdmaStorageDriver<DataItem, Operation<DataItem>> driver,
					final Operation<DataItem> operation,
					final Predicate<Operation<DataItem>> finalResult) throws Exception {
		final ResultOutput output = new ResultOutput();
		driver.operationResultOutput(output);
		driver.start();
		assertTrue(driver.put(operation));
		final List<String> seen = new java.util.ArrayList<>();
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		while (System.nanoTime() < deadline) {
			final Operation<DataItem> result = output.await();
			if (result != null) {
				seen.add(result.getClass().getSimpleName() + ":" + result.status()
								+ (result instanceof CompositeDataOperation<?> c ? ":done=" + c.allSubOperationsDone() : ""));
				if (finalResult.test(result)) {
					return result;
				}
			}
		}
		throw new AssertionError("no final result; results=" + seen + "; requests=" + requests
						+ "; paths=" + driver.pathStats().summary());
	}

	private Config config(final boolean fallback) {
		try {
			final List<Map<String, Object>> configSchemas = Extension
							.load(Thread.currentThread().getContextClassLoader())
							.stream()
							.map(Extension::schemaProvider)
							.filter(Objects::nonNull)
							.map(provider -> {
								try {
									return provider.schema();
								} catch (final Exception e) {
									throw new IllegalStateException(e);
								}
							})
							.filter(Objects::nonNull)
							.collect(Collectors.toList());
			configSchemas.add(0, InitialConfigSchemaProvider.provider().schema());
			SchemaProvider
							.resolve(APP_NAME, Thread.currentThread().getContextClassLoader())
							.stream()
							.findFirst()
							.ifPresent(configSchemas::add);
			final Config config = new BasicConfig("-", TreeUtil.reduceForest(configSchemas));
			config.val("load-batch-size", 1024);
			config.val("storage-driver-limit-concurrency", 1);
			config.val("storage-driver-threads", 0);
			config.val("storage-driver-limit-queue-input", 8);
			config.val("storage-namespace", "/bucket");
			config.val("storage-net-transport", "nio");
			config.val("storage-net-reuseAddr", true);
			config.val("storage-net-bindBacklogSize", 0);
			config.val("storage-net-keepAlive", true);
			config.val("storage-net-rcvBuf", 0);
			config.val("storage-net-sndBuf", 0);
			config.val("storage-net-ssl-enabled", false);
			config.val("storage-net-ssl-protocols", List.of());
			config.val("storage-net-ssl-provider", "OPENSSL");
			config.val("storage-net-tcpNoDelay", false);
			config.val("storage-net-interestOpQueued", false);
			config.val("storage-net-writeSpinCount", 1);
			config.val("storage-net-linger", 0);
			config.val("storage-net-timeoutMilliSec", 2_000);
			config.val("storage-net-ioRatio", 50);
			config.val("storage-net-node-addrs", List.of("127.0.0.1"));
			config.val("storage-net-node-port", server.getAddress().getPort());
			config.val("storage-net-node-connAttemptsLimit", 0);
			config.val("storage-net-http-headers", new HashMap<String, String>(
							Map.of("Date", "#{date:formatNowRfc1123()}%{date:formatNowRfc1123()}")));
			config.val("storage-net-http-read-metadata-only", false);
			config.val("storage-net-http-max-chunk-size", 65536);
			config.val("storage-net-http-uri-args", Map.of());
			config.val("storage-object-fsAccess", true);
			config.val("storage-object-tagging-enabled", false);
			config.val("storage-object-tagging-tags", Map.of());
			config.val("storage-object-versioning", false);
			config.val("storage-auth-uid", CREDENTIAL.getUid());
			config.val("storage-auth-token", null);
			config.val("storage-auth-secret", CREDENTIAL.getSecret());
			config.val("storage-auth-version", 4);
			config.val("storage-checksum-enabled", false);
			config.val("storage-integrity-mode", "none");
			config.val("storage-integrity-algorithm", "sha256");
			config.val("storage-integrity-input-provenance", "none");
			config.val("storage-integrity-input-expectedProducerId", "");
			config.val("storage-integrity-selection-maxCount", 0L);
			config.val("storage-rdma-enabled", true);
			config.val("storage-rdma-thresholdBytes", THRESHOLD);
			config.val("storage-rdma-fallback", fallback);
			config.val("storage-rdma-device", "auto");
			config.val("storage-rdma-localIp", "");
			config.val("storage-rdma-logLevel", "WARN");
			config.val("storage-rdma-timeoutMs", 30_000L);
			config.val("storage-rdma-allowMissingBytesHeader", false);
			config.val("storage-rdma-bufferPool", true);
			return config;
		} catch (final Exception e) {
			throw new IllegalStateException(e);
		}
	}

	private static final class ResultOutput implements Output<Operation<DataItem>> {

		private final LinkedBlockingQueue<Operation<DataItem>> results = new LinkedBlockingQueue<>();

		@Override
		public boolean put(final Operation<DataItem> value) {
			return results.offer(value);
		}

		@Override
		public int put(final List<Operation<DataItem>> values, final int from, final int to) {
			int count = 0;
			for (int i = from; i < to; i++) {
				if (!results.offer(values.get(i))) {
					break;
				}
				count++;
			}
			return count;
		}

		@Override
		public int put(final List<Operation<DataItem>> values) {
			return put(values, 0, values.size());
		}

		@Override
		public Input<Operation<DataItem>> getInput() {
			return null;
		}

		@Override
		public void close() {
			results.clear();
		}

		private Operation<DataItem> await() throws InterruptedException {
			return results.poll(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS);
		}
	}
}
