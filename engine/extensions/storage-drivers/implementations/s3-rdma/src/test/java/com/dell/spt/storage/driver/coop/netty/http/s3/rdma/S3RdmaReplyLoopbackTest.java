package com.dell.spt.storage.driver.coop.netty.http.s3.rdma;

import static com.dell.spt.base.Constants.APP_NAME;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalAnswers.delegatesTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.dell.spt.base.concurrent.ServiceTaskExecutor;
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
import com.dell.spt.base.item.op.data.DataOperation;
import com.dell.spt.base.item.op.data.DataOperationImpl;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.base.metrics.MetricsConstants;
import com.dell.spt.base.metrics.MetricsManagerImpl;
import com.dell.spt.base.metrics.TerminalStepEntry;
import com.dell.spt.base.metrics.context.MetricsContext;
import com.dell.spt.base.metrics.context.MetricsContextImpl;
import com.dell.spt.base.metrics.snapshot.AllMetricsSnapshotImpl;
import com.dell.spt.base.metrics.snapshot.DriverStatsSnapshot;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HexFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
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
	private static final String STEP_ID = "s3-rdma-reply-loopback";
	private static final Credential CREDENTIAL = Credential.getInstance("access", "secret");
	private static final int SIZE = 4096;
	/** Longer than the 64 KB data ring of {@link #newDriver}, and not a multiple of it. */
	private static final int PAYLOAD_SIZE = 3 * 65536 + 4097;
	private static final long THRESHOLD = 1024;
	/** Part retries allowed by {@code CoopStorageDriverBase.MAX_PART_RETRIES}. */
	private static final int MAX_PART_RETRIES = 3;
	private static final String OBJECT_BODY = "x".repeat(SIZE);
	private static final int NO_RESPONSE = 0;
	private static final String DECLINE_BODY = "<Error><Code>RDMANotSupported</Code><Message>RDMA not available</Message></Error>";

	private final List<CapturedRequest> requests = new CopyOnWriteArrayList<>();
	private final ExecutorService executor = Executors.newSingleThreadExecutor();
	/** Makes the transport of the next driver fail its initialization. */
	private boolean rdmaInitFails;
	/** While set and closed, the emulated server answers no object request. */
	private volatile CountDownLatch replyGate;
	/** Body of the last PUT that carried one. */
	private volatile byte[] httpPutBody;
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
	@SuppressWarnings({"rawtypes", "unchecked"
	})
	void aTargetBucketSetMemberIsTheBucketOfRdmaPutAndGet() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		final var builder = new com.dell.spt.base.item.op.data.DataOperationsBuilderImpl(0);
		builder.opType(OpType.CREATE);
		builder.outputPathInput(new com.dell.spt.base.config.ConstantValueInputImpl<>("/bench/prefix"));
		builder.credentialInput(new com.dell.spt.base.config.ConstantValueInputImpl<>(CREDENTIAL));
		builder.sizeThreshold(Long.MAX_VALUE);
		builder.targetBucketMembers(com.dell.spt.base.storage.driver.bucket.TargetBucketSet.memberPaths("/bench/prefix", 4), 0);
		final var create = (Operation<DataItem>) builder.buildOp(new DataItemImpl("k6", 6, SIZE));
		try (final var driver = newDriver(true)) {
			assertEquals(Operation.Status.SUCC, execute(driver, create).status());
		}
		final var recorded = create.item().name();
		reply = new Reply(200, "200", Integer.toString(SIZE), null, true);
		try (final var driver = newDriver(true)) {
			final Operation<DataItem> read = (Operation<DataItem>) (Operation<?>) new DataOperationImpl<>(
							0, OpType.READ, new DataItemImpl(recorded, 6, SIZE), null, null, CREDENTIAL, null, 0);
			assertEquals(Operation.Status.SUCC, execute(driver, read).status());
		}

		assertEquals("/bench-2/prefix/k6", onlyObjectRequest("PUT").rawPath());
		assertEquals("/bench-2/prefix/k6", onlyObjectRequest("GET").rawPath());
		assertTrue(requests.stream().noneMatch(r -> r.rawPath().startsWith("/bench/")), requests::toString);
	}

	@Test
	void putDeclinedWithHttp200AndReply501FailsWithoutBytes() throws Exception {
		// The response observed from an ECS 4.5 server while its RDMA server could not start.
		reply = new Reply(200, "501", null, DECLINE_BODY, false);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, op(OpType.CREATE, SIZE));

			assertEquals(Operation.Status.RESP_FAIL_SVC, result.status());
			assertEquals(0, ((DataOperation<?>) result).countBytesDone());
			assertEquals(1, driver.pathStats().rdmaDeclinedFailed.sum());
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
			assertEquals(1, driver.pathStats().rdmaDeclinedFailed.sum());
		}
	}

	@Test
	void putTransferFailureStatusIsHttpError() throws Exception {
		reply = new Reply(500, null, null, "<Error><Code>RDMATransferFailed</Code></Error>", false);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, op(OpType.CREATE, SIZE));

			assertEquals(Operation.Status.RESP_FAIL_SVC, result.status());
			assertEquals(1, driver.pathStats().rdmaHttpError.sum());
			assertEquals(0, driver.pathStats().rdmaDeclinedFailed.sum());
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
			assertEquals(1, driver.pathStats().rdmaDeclinedServedOverHttp.sum());
			assertEquals(0, driver.pathStats().rdmaDeclinedFailed.sum());
		}
	}

	@Test
	void getDeclinedFailsWhenFallbackDisabled() throws Exception {
		reply = new Reply(200, "501", null, OBJECT_BODY, false);
		try (final var driver = newDriver(false)) {
			final var result = execute(driver, op(OpType.READ, SIZE));

			assertEquals(Operation.Status.RESP_FAIL_SVC, result.status());
			assertEquals(0, ((DataOperation<?>) result).countBytesDone());
			assertEquals(1, driver.pathStats().rdmaDeclinedFailed.sum());
			assertEquals(0, driver.pathStats().rdmaDeclinedServedOverHttp.sum());
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
	void tokenFailureWithFallbackSendsTheWholePayloadOverHttp() throws Exception {
		final byte[] payload = payloadSentOverRdma(PAYLOAD_SIZE);
		reply = new Reply(200, null, null, null, false);
		try (final var driver = newDriver(true)) {
			transport.setFailTokenGeneration(true);
			final var result = execute(driver, op(OpType.CREATE, PAYLOAD_SIZE));

			assertEquals(Operation.Status.SUCC, result.status());
			assertEquals(PAYLOAD_SIZE, ((DataOperation<?>) result).countBytesDone());
			assertNull(onlyObjectRequest("PUT").rdmaToken());
			assertArrayEquals(payload, httpPutBody, "the payload was already copied once for RDMA");
			assertEquals(1, driver.pathStats().httpFallback.sum());
			assertNoBufferInUse(driver);
		}
	}

	@Test
	void copyFailureWithFallbackSendsTheWholePayloadOverHttp() throws Exception {
		final byte[] payload = payloadSentOverRdma(PAYLOAD_SIZE);
		reply = new Reply(200, null, null, null, false);
		try (final var driver = newDriver(true)) {
			final var result = execute(driver, putFailingAfterPartialCopy(PAYLOAD_SIZE));

			assertEquals(Operation.Status.SUCC, result.status());
			assertEquals(PAYLOAD_SIZE, ((DataOperation<?>) result).countBytesDone());
			assertNull(onlyObjectRequest("PUT").rdmaToken());
			assertArrayEquals(payload, httpPutBody, "the payload was partly copied for RDMA");
			assertEquals(1, driver.pathStats().httpFallback.sum());
			assertNoBufferInUse(driver);
		}
	}

	@Test
	void tokenFailureWithoutFallbackFailsWithoutBytes() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		try (final var driver = newDriver(false)) {
			transport.setFailTokenGeneration(true);
			final var result = execute(driver, op(OpType.CREATE, SIZE));

			assertEquals(Operation.Status.FAIL_IO, result.status());
			assertEquals(0, ((DataOperation<?>) result).countBytesDone());
			assertTrue(objectRequests().isEmpty(), "no request may be sent: " + requests);
			assertEquals(1, driver.pathStats().prepareFailed.sum());
			assertNoBufferInUse(driver);
		}
	}

	/** The bytes of {@code op(OpType.CREATE, size)}, as an RDMA PUT places them in registered memory. */
	private byte[] payloadSentOverRdma(final int size) throws Exception {
		reply = new Reply(200, "200", null, null, true);
		try (final var driver = newDriver(false)) {
			assertEquals(Operation.Status.SUCC, execute(driver, op(OpType.CREATE, size)).status());
		}
		requests.clear();
		return serverReadPayload;
	}

	/** The PUT of {@link #op}, except that its first read delivers half the payload and its second fails. */
	@SuppressWarnings("unchecked")
	private static Operation<DataItem> putFailingAfterPartialCopy(final int size) throws IOException {
		final DataItem whole = new DataItemImpl("obj-" + OpType.CREATE, 0, size);
		final DataItem item = mock(DataItem.class, delegatesTo(whole));
		final AtomicInteger reads = new AtomicInteger();
		doAnswer(invocation -> {
			if (reads.getAndIncrement() > 0) {
				throw new IOException("injected read failure");
			}
			final ByteBuffer dst = invocation.getArgument(0);
			final ByteBuffer half = dst.duplicate().limit(dst.position() + size / 2);
			final int count = whole.read(half);
			dst.position(half.position());
			return count;
		}).when(item).read(any(ByteBuffer.class));
		return (Operation<DataItem>) (Operation<?>) new DataOperationImpl<>(
						0, OpType.CREATE, item, null, "/bucket", CREDENTIAL, null, 0);
	}

	// ---------- RDMA unavailable ----------

	@Test
	void initializationFallbackSendsOperationsOverHttpAndCountsThem() throws Exception {
		rdmaInitFails = true;
		for (final OpType opType : List.of(OpType.CREATE, OpType.READ)) {
			try (final var driver = newDriver(true)) {
				final var result = execute(driver, op(opType, SIZE));

				assertEquals(Operation.Status.SUCC, result.status(), opType.toString());
				assertEquals(SIZE, ((DataOperation<?>) result).countBytesDone(), opType.toString());
				assertEquals(1, driver.pathStats().httpRdmaUnavailable.sum(), opType.toString());
				assertTrue(driver.pathStats().summary().contains("httpRdmaUnavailable=1"),
								driver.pathStats().summary());
				assertEquals(0, transport.getRegisterCount());
			}
		}
		assertNull(onlyObjectRequest("PUT").rdmaToken());
		assertEquals(SIZE, onlyObjectRequest("PUT").bodyLength());
		assertNull(onlyObjectRequest("GET").rdmaToken());
	}

	@Test
	void initializationFallbackCountsTheDataOperationsOfABatch() throws Exception {
		rdmaInitFails = true;
		try (final var driver = newDriver(true)) {
			final ResultOutput output = new ResultOutput();
			driver.operationResultOutput(output);
			driver.start();
			// The first operation holds the only permit until the gate opens. The others are then
			// queued together, so the dispatcher submits them as a batch.
			replyGate = new CountDownLatch(1);
			assertTrue(driver.put(op(OpType.CREATE, SIZE)));
			final List<Operation<DataItem>> queued = List.of(
							op(OpType.READ, SIZE), op(OpType.DELETE, SIZE), op(OpType.CREATE, SIZE));
			assertEquals(queued.size(), driver.put(queued));
			replyGate.countDown();

			for (int i = 0; i <= queued.size(); i++) {
				final Operation<DataItem> result = output.await();
				assertNotNull(result, "operation " + i + " did not complete: " + requests);
				assertEquals(Operation.Status.SUCC, result.status(), result.type() + ": " + requests);
			}
			assertEquals(3, driver.pathStats().httpRdmaUnavailable.sum(),
							"two PUTs and one GET carry a payload; the DELETE does not");
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
			assertEquals(S3RdmaStorageDriver.PathSelection.HTTP_INELIGIBLE, driver.selectPath(ranged));
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
				final Operation<DataItem> unwritten = read(driver, output, SIZE, new Reply(200, "200", Integer.toString(SIZE), null, false));
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

	// ---------- Driver statistics ----------

	/** The server performed the transfer and reports no byte count: complete for a PUT. */
	private static final Reply TRANSFERRED = new Reply(200, "200", null, null, true);
	private static final Reply GET_TRANSFERRED = new Reply(200, "200", Integer.toString(SIZE), null, true);
	private static final Reply GET_DECLINED = new Reply(200, "501", null, OBJECT_BODY, false);
	private static final Reply PUT_DECLINED = new Reply(200, "501", null, DECLINE_BODY, false);
	private static final Reply FAILED = new Reply(500, null, null, "<Error><Code>RDMATransferFailed</Code></Error>", false);
	private static final Reply UNANSWERED = new Reply(NO_RESPONSE, null, null, null, false);
	private static final long MIB = 1L << 20;

	@Test
	void metricsSnapshotCarriesTheCountsOfTheRequestsTheDriverSent() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(true))) {
			// Two transfers on one pooled buffer, a request the server fails, a payload below the threshold.
			assertEquals(Operation.Status.SUCC, node.run(TRANSFERRED, op(OpType.CREATE, SIZE)).status());
			assertEquals(Operation.Status.SUCC, node.run(TRANSFERRED, op(OpType.CREATE, SIZE)).status());
			assertEquals(Operation.Status.RESP_FAIL_SVC, node.run(FAILED, op(OpType.CREATE, SIZE)).status());
			assertEquals(Operation.Status.SUCC, node.run(op(OpType.CREATE, THRESHOLD - 1)).status());

			node.assertCounted(Map.of(
							"rdma_requests_sent", 3L,
							"rdma_transferred", 2L,
							"rdma_request_failed", 1L,
							"http_below_threshold", 1L,
							"registrations_pooled", 1L,
							"pool_hits", 2L));
			final DriverStatsSnapshot stats = node.stats();
			assertEquals("s3-rdma", stats.driver());
			assertEquals(1L, stats.gauges().get("drivers"));
			assertEquals(1L, stats.gauges().get("transport_available"));
			// Name by name, the snapshot holds the driver's own counts.
			assertEquals(node.driver.driverStats(), stats);
		}
	}

	@Test
	void transfersAreCountedAsRequestsSentAndAsTransferred() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(true))) {
			assertEquals(Operation.Status.SUCC, node.run(TRANSFERRED, op(OpType.CREATE, SIZE)).status());
			node.assertCounted(Map.of("registrations_pooled", 1L, "rdma_requests_sent", 1L, "rdma_transferred", 1L));

			assertEquals(Operation.Status.SUCC, node.run(GET_TRANSFERRED, op(OpType.READ, SIZE)).status());
			node.assertCounted(Map.of(
							"registrations_pooled", 1L, "pool_hits", 1L, "rdma_requests_sent", 2L, "rdma_transferred", 2L));
		}
	}

	@Test
	void transferWithoutAByteCountIsCountedAsAssumedUnderTheLegacyOptIn() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(true, true))) {
			assertEquals(Operation.Status.SUCC, node.run(TRANSFERRED, op(OpType.READ, SIZE)).status());
			node.assertCounted(Map.of(
							"registrations_pooled", 1L,
							"rdma_requests_sent", 1L,
							"rdma_transferred", 1L,
							"rdma_bytes_assumed", 1L));
		}
	}

	@Test
	void declinedRequestsAreCountedByWhatBecameOfThem() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(true))) {
			// With fallback a declined GET delivers its body over HTTP; a declined PUT stored nothing.
			assertEquals(Operation.Status.SUCC, node.run(GET_DECLINED, op(OpType.READ, SIZE)).status());
			assertEquals(Operation.Status.RESP_FAIL_SVC, node.run(PUT_DECLINED, op(OpType.CREATE, SIZE)).status());
			node.assertCounted(Map.of(
							"registrations_pooled", 1L,
							"pool_hits", 1L,
							"rdma_requests_sent", 2L,
							"rdma_declined_served_over_http", 1L,
							"rdma_declined_failed", 1L));
		}
		try (final NodeStats node = new NodeStats(newDriver(false))) {
			assertEquals(Operation.Status.RESP_FAIL_SVC, node.run(GET_DECLINED, op(OpType.READ, SIZE)).status());
			node.assertCounted(Map.of("registrations_pooled", 1L, "rdma_requests_sent", 1L, "rdma_declined_failed", 1L));
		}
	}

	@Test
	void requestsTheServerFailsOrNeverAnswersAreCountedAsFailed() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(true))) {
			assertEquals(Operation.Status.RESP_FAIL_SVC, node.run(FAILED, op(OpType.CREATE, SIZE)).status());
			assertTrue(node.run(UNANSWERED, op(OpType.CREATE, SIZE)).status() != Operation.Status.SUCC);
			// The server might still access the buffer of the request it never answered.
			node.assertCounted(Map.of(
							"registrations_pooled", 1L,
							"pool_hits", 1L,
							"pool_discarded", 1L,
							"rdma_requests_sent", 2L,
							"rdma_request_failed", 2L));
		}
	}

	@Test
	void replyThatBreaksTheContractIsCountedAsAProtocolError() throws Exception {
		final Reply shortTransfer = new Reply(200, "200", Integer.toString(SIZE - 1), null, true);
		try (final NodeStats node = new NodeStats(newDriver(true))) {
			assertEquals(Operation.Status.RESP_FAIL_CORRUPT, node.run(shortTransfer, op(OpType.READ, SIZE)).status());
			node.assertCounted(Map.of("registrations_pooled", 1L, "rdma_requests_sent", 1L, "rdma_protocol_error", 1L));
		}
	}

	@Test
	void requestTheReaperGivesUpOnIsCountedAsTimedOut() throws Exception {
		// The reaper looks once a second; a request it then finds has waited longer than this timeout.
		try (final NodeStats node = new NodeStats(newDriver(true, config -> config.val("storage-rdma-timeoutMs", 1L)))) {
			replyGate = new CountDownLatch(1);
			try {
				assertEquals(Operation.Status.FAIL_IO, node.run(FAILED, op(OpType.READ, SIZE)).status());
			} finally {
				// The answer comes after the request was given up; it is counted under nothing.
				replyGate.countDown();
			}
			node.assertCounted(Map.of(
							"registrations_pooled", 1L,
							"pool_discarded", 1L,
							"rdma_requests_sent", 1L,
							"rdma_timed_out", 1L));
		}
	}

	@Test
	void requestsNotProposedForRdmaAreCountedByTheReason() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(true))) {
			assertEquals(Operation.Status.SUCC, node.run(op(OpType.CREATE, THRESHOLD - 1)).status());
			node.assertCounted(Map.of("http_below_threshold", 1L));
			assertEquals(Operation.Status.SUCC, node.run(rangedRead()).status());
			node.assertCounted(Map.of("http_below_threshold", 1L, "http_ineligible", 1L));
			node.run(largerThanAnyRdmaBuffer());
			node.assertCounted(Map.of("http_below_threshold", 1L, "http_ineligible", 1L, "http_oversize", 1L));
			// A request without a payload is counted under no reason.
			assertEquals(Operation.Status.SUCC, node.run(op(OpType.DELETE, SIZE)).status());
			node.assertCounted(Map.of("http_below_threshold", 1L, "http_ineligible", 1L, "http_oversize", 1L));

			assertEquals(4, objectRequests().size(), requests.toString());
			assertTrue(objectRequests().stream().allMatch(request -> request.rdmaToken() == null), requests.toString());
			assertEquals(0, transport.getRegisterCount());
		}
	}

	@Test
	void requestsOfADriverWithoutRdmaAreCountedAsRdmaUnavailable() throws Exception {
		rdmaInitFails = true;
		try (final NodeStats node = new NodeStats(newDriver(true))) {
			assertEquals(Operation.Status.SUCC, node.run(op(OpType.CREATE, SIZE)).status());
			assertEquals(Operation.Status.SUCC, node.run(op(OpType.READ, SIZE)).status());
			// A request without a payload is counted under no reason.
			assertEquals(Operation.Status.SUCC, node.run(op(OpType.DELETE, SIZE)).status());
			node.assertCounted(Map.of("http_rdma_unavailable", 2L));
			assertEquals(0L, node.gauge("transport_available"));
		}
	}

	// A preparation failure is counted under its cause and, apart from that, under what became of
	// the operation: sent over HTTP, or failed.

	@Test
	void exhaustedDirectMemoryForAnOnDemandBufferIsCountedByItsCause() throws Exception {
		assertPutCounted(false, true, S3RdmaReplyLoopbackTest::exhaustDirectMemory, Operation.Status.SUCC,
						Map.of("allocation_failed_on_demand", 1L, "http_fallback", 1L));
		assertPutCounted(false, false, S3RdmaReplyLoopbackTest::exhaustDirectMemory, Operation.Status.FAIL_IO,
						Map.of("allocation_failed_on_demand", 1L, "prepare_failed", 1L));
	}

	@Test
	void refusedRegistrationOfAnOnDemandBufferIsCountedByItsCause() throws Exception {
		assertPutCounted(false, true, driver -> transport.setFailNextRegistrations(1), Operation.Status.SUCC,
						Map.of("registration_failed_on_demand", 1L, "http_fallback", 1L));
		assertPutCounted(false, false, driver -> transport.setFailNextRegistrations(1), Operation.Status.FAIL_IO,
						Map.of("registration_failed_on_demand", 1L, "prepare_failed", 1L));
	}

	@Test
	void tokenGenerationFailureIsCountedByItsCause() throws Exception {
		assertPutCounted(true, true, driver -> transport.setFailTokenGeneration(true), Operation.Status.SUCC,
						Map.of("registrations_pooled", 1L, "token_generation_failed", 1L, "http_fallback", 1L));
		assertPutCounted(true, false, driver -> transport.setFailTokenGeneration(true), Operation.Status.FAIL_IO,
						Map.of("registrations_pooled", 1L, "token_generation_failed", 1L, "prepare_failed", 1L));
		assertPutCounted(false, true, driver -> transport.setFailTokenGeneration(true), Operation.Status.SUCC,
						Map.of("registrations_on_demand", 1L, "token_generation_failed", 1L, "http_fallback", 1L));
	}

	@Test
	void poolBufferThatCannotBeCreatedIsCountedByItsCauseAndTheRequestRegistersOnDemand() throws Exception {
		assertPutCounted(true, false, driver -> transport.setFailNextRegistrations(1), Operation.Status.SUCC,
						Map.of(
										"registration_failed_pooled", 1L,
										"registrations_on_demand", 1L,
										"rdma_requests_sent", 1L,
										"rdma_transferred", 1L));
		assertEquals(2, transport.getRegisterCount(), "the pool's refused registration and the request's own");

		assertPutCounted(true, false, driver -> driver.bufferPool().allocator = size -> {
			throw new OutOfMemoryError("Cannot reserve " + size + " bytes of direct buffer memory");
		}, Operation.Status.SUCC,
						Map.of(
										"allocation_failed_pooled", 1L,
										"registrations_on_demand", 1L,
										"rdma_requests_sent", 1L,
										"rdma_transferred", 1L));
		assertEquals(1, transport.getRegisterCount(), "the pool had no buffer to register");
	}

	@Test
	void registrationRefusedForThePoolAndThenOnDemandIsCountedOnceForEach() throws Exception {
		assertPutCounted(true, true, driver -> transport.setFailAfterNRegistrations(0), Operation.Status.SUCC,
						Map.of("registration_failed_pooled", 1L, "registration_failed_on_demand", 1L, "http_fallback", 1L));
	}

	@Test
	void payloadCopyFailureHasNoCauseOfItsOwnAndIsCountedByWhatBecameOfTheOperation() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(true))) {
			assertEquals(Operation.Status.SUCC, node.run(putFailingAfterPartialCopy(PAYLOAD_SIZE)).status());
			// An abandoned preparation's buffer is never reused.
			node.assertCounted(Map.of("registrations_pooled", 1L, "pool_discarded", 1L, "http_fallback", 1L));
		}
	}

	/** Sends one PUT on a new driver that {@code fault} prepared and checks what the node then counts. */
	private void assertPutCounted(
					final boolean pool,
					final boolean fallback,
					final Consumer<S3RdmaStorageDriver<DataItem, Operation<DataItem>>> fault,
					final Operation.Status status,
					final Map<String, Long> counted) throws Exception {
		try (final NodeStats node = new NodeStats(
						newDriver(fallback, config -> config.val("storage-rdma-bufferPool", pool)))) {
			fault.accept(node.driver);
			assertEquals(status, node.run(TRANSFERRED, op(OpType.CREATE, SIZE)).status());
			node.assertCounted(counted);
		}
	}

	private static void exhaustDirectMemory(final S3RdmaStorageDriver<?, ?> driver) {
		driver.directAllocator = size -> {
			throw new OutOfMemoryError("Cannot reserve " + size + " bytes of direct buffer memory");
		};
	}

	// Whatever keeps a request out of the pool, it registers a buffer of its own, counted once.

	@Test
	void disabledPoolRegistersEveryRequestOnDemand() throws Exception {
		try (final NodeStats node = new NodeStats(
						newDriver(false, config -> config.val("storage-rdma-bufferPool", false)))) {
			assertEquals(Operation.Status.SUCC, node.run(TRANSFERRED, op(OpType.CREATE, SIZE)).status());
			assertEquals(Operation.Status.SUCC, node.run(TRANSFERRED, op(OpType.CREATE, SIZE)).status());
			node.assertCounted(Map.of("registrations_on_demand", 2L, "rdma_requests_sent", 2L, "rdma_transferred", 2L));
			assertEquals(2, transport.getRegisterCount());
			assertEquals(0L, node.gauge("pool_budget_bytes"));
		}
	}

	@Test
	void exhaustedPoolRegistersTheRequestOnDemand() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(false))) {
			// The buffers of the request's size class are held, as requests in flight hold them: with
			// one permit the class has two.
			final RdmaBufferPool pool = node.driver.bufferPool();
			final List<RdmaBufferPool.PooledBuffer> held = List.of(pool.acquire(SIZE), pool.acquire(SIZE));
			try {
				assertEquals(Operation.Status.SUCC, node.run(TRANSFERRED, op(OpType.CREATE, SIZE)).status());
				node.assertCounted(Map.of(
								"registrations_pooled", 2L,
								"pool_exhausted", 1L,
								"registrations_on_demand", 1L,
								"rdma_requests_sent", 1L,
								"rdma_transferred", 1L));
				assertEquals(3, transport.getRegisterCount());
			} finally {
				held.forEach(pool::release);
			}
		}
	}

	@Test
	void requestLargerThanThePoolsLargestBufferRegistersOnDemand() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(false))) {
			// A small buffer stands in for one of that size: the server fails the request without accessing it.
			node.driver.directAllocator = size -> ByteBuffer.allocateDirect(SIZE);
			assertEquals(Operation.Status.RESP_FAIL_SVC, node.run(FAILED, largerThanAnyPoolBuffer()).status());
			node.assertCounted(Map.of(
							"pool_oversize", 1L,
							"registrations_on_demand", 1L,
							"rdma_requests_sent", 1L,
							"rdma_request_failed", 1L));
			assertEquals(1, transport.getRegisterCount());
		}
	}

	@Test
	void idleBuffersGivenUpForAnOnDemandBufferAreCountedAsEvicted() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(false))) {
			assertEquals(Operation.Status.SUCC, node.run(TRANSFERRED, op(OpType.CREATE, SIZE)).status());
			assertEquals(1L, node.gauge("pool_idle_buffers"));
			// Direct memory is exhausted until the pool gives up its idle buffer.
			final AtomicInteger allocations = new AtomicInteger();
			node.driver.directAllocator = size -> {
				if (allocations.getAndIncrement() == 0) {
					throw new OutOfMemoryError("Cannot reserve " + size + " bytes of direct buffer memory");
				}
				return ByteBuffer.allocateDirect(SIZE);
			};
			assertEquals(Operation.Status.RESP_FAIL_SVC, node.run(FAILED, largerThanAnyPoolBuffer()).status());
			// The allocation succeeded when it was repeated, so the preparation did not fail.
			node.assertCounted(Map.of(
							"registrations_pooled", 1L,
							"pool_evicted", 1L,
							"pool_oversize", 1L,
							"registrations_on_demand", 1L,
							"rdma_requests_sent", 2L,
							"rdma_transferred", 1L,
							"rdma_request_failed", 1L));
			assertEquals(0L, node.gauge("pool_buffers"));
			assertEquals(0L, node.gauge("pool_reserved_bytes"));
		}
	}

	@Test
	void poolBufferReturnedTwiceIsCountedAsAnInvalidReturn() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(false))) {
			final RdmaBufferPool pool = node.driver.bufferPool();
			final RdmaBufferPool.PooledBuffer buffer = pool.acquire(SIZE);
			pool.release(buffer);
			pool.release(buffer);
			node.assertCounted(Map.of("registrations_pooled", 1L, "pool_invalid_returns", 1L));
			assertEquals(1L, node.gauge("pool_idle_buffers"));
		}
	}

	// A submission is refused while no permit is free, and is then repeated. The dispatcher asks for
	// the free permits before it submits, so these tests submit as it does while a request holds the
	// only permit.

	@Test
	void refusedRdmaSubmissionIsCountedAsSentOnlyWhenItsRepeatIsAccepted() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(true))) {
			final Operation<DataItem> read = op(OpType.READ, SIZE);
			holdTheOnlyPermit(node);
			assertFalse(node.driver.submit(read));
			// The refused attempt registered a pool buffer and returned it; no request left.
			node.assertCounted(Map.of("registrations_pooled", 1L));
			assertEquals(1L, node.gauge("pool_idle_buffers"));
			freeThePermit(node);

			assertEquals(Operation.Status.SUCC, node.run(GET_TRANSFERRED, read).status());
			node.assertCounted(Map.of(
							"registrations_pooled", 1L, "pool_hits", 1L, "rdma_requests_sent", 1L, "rdma_transferred", 1L));
			assertNotNull(onlyObjectRequest("GET").rdmaToken());
		}
	}

	@Test
	void refusedHttpSubmissionIsCountedOnlyWhenItsRepeatIsAccepted() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(true))) {
			final List<Operation<DataItem>> overHttp = List.of(
							op(OpType.CREATE, THRESHOLD - 1), rangedRead(), largerThanAnyRdmaBuffer());
			holdTheOnlyPermit(node);
			for (final Operation<DataItem> operation : overHttp) {
				assertFalse(node.driver.submit(operation));
			}
			node.assertCounted(Map.of());
			freeThePermit(node);

			for (final Operation<DataItem> operation : overHttp) {
				node.run(operation);
			}
			node.assertCounted(Map.of("http_below_threshold", 1L, "http_ineligible", 1L, "http_oversize", 1L));
		}
		rdmaInitFails = true;
		try (final NodeStats node = new NodeStats(newDriver(true))) {
			final Operation<DataItem> put = op(OpType.CREATE, SIZE);
			holdTheOnlyPermit(node);
			assertFalse(node.driver.submit(put));
			node.assertCounted(Map.of());
			freeThePermit(node);

			assertEquals(Operation.Status.SUCC, node.run(put).status());
			node.assertCounted(Map.of("http_rdma_unavailable", 1L));
		}
	}

	@Test
	void preparationFailureIsCountedForEachAttemptAndItsFallbackOnceItIsAccepted() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(true))) {
			transport.setFailTokenGeneration(true);
			final Operation<DataItem> read = op(OpType.READ, SIZE);
			holdTheOnlyPermit(node);
			assertFalse(node.driver.submit(read));
			node.assertCounted(Map.of("registrations_pooled", 1L, "token_generation_failed", 1L));
			freeThePermit(node);

			assertEquals(Operation.Status.SUCC, node.run(read).status());
			node.assertCounted(Map.of(
							"registrations_pooled", 1L,
							"pool_hits", 1L,
							"token_generation_failed", 2L,
							"http_fallback", 1L));
			assertNull(onlyObjectRequest("GET").rdmaToken());
		}
	}

	@Test
	void requestInFlightIsCountedAsSentAndHasNoOutcomeYet() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(true))) {
			reply = GET_TRANSFERRED;
			replyGate = new CountDownLatch(1);
			assertTrue(node.driver.put(op(OpType.READ, SIZE)));
			node.assertCounted(Map.of("registrations_pooled", 1L, "rdma_requests_sent", 1L));
			assertEquals(MIB, node.gauge("pool_reserved_bytes"));
			assertEquals(1L, node.gauge("pool_buffers"));
			assertEquals(0L, node.gauge("pool_idle_buffers"));

			replyGate.countDown();
			assertEquals(Operation.Status.SUCC, node.awaitResult().status());
			// Every request sent now has exactly one outcome.
			node.assertCounted(Map.of("registrations_pooled", 1L, "rdma_requests_sent", 1L, "rdma_transferred", 1L));
			assertEquals(MIB, node.gauge("pool_reserved_bytes"));
			assertEquals(1L, node.gauge("pool_buffers"));
			assertEquals(1L, node.gauge("pool_idle_buffers"));
			assertEquals(MIB, node.gauge("pool_peak_reserved_bytes"));
			assertEquals(RdmaBufferPool.defaultMaxPooledBytes(), node.gauge("pool_budget_bytes"));
		}
	}

	@Test
	void batchSubmissionCountsEachRequestAsItsSingleSubmissionWould() throws Exception {
		try (final NodeStats node = new NodeStats(newDriver(true))) {
			reply = GET_TRANSFERRED;
			// The operations are queued together while a request holds the only permit, so the
			// dispatcher submits them as a batch.
			holdTheOnlyPermit(node);
			final List<Operation<DataItem>> queued = List.of(
							op(OpType.READ, SIZE), op(OpType.CREATE, THRESHOLD - 1), rangedRead(), op(OpType.DELETE, SIZE));
			assertEquals(queued.size(), node.driver.put(queued));
			freeThePermit(node);

			for (int i = 0; i < queued.size(); i++) {
				assertEquals(Operation.Status.SUCC, node.awaitResult().status(), "operation " + i + ": " + requests);
			}
			node.assertCounted(Map.of(
							"registrations_pooled", 1L,
							"rdma_requests_sent", 1L,
							"rdma_transferred", 1L,
							"http_below_threshold", 1L,
							"http_ineligible", 1L));
		}
	}

	/** Has a DELETE, which is counted under no name, hold the driver's only permit until the gate opens. */
	private void holdTheOnlyPermit(final NodeStats node) throws Exception {
		final int sentBefore = objectRequests().size();
		replyGate = new CountDownLatch(1);
		assertTrue(node.driver.put(op(OpType.DELETE, SIZE)));
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		while (objectRequests().size() == sentBefore) {
			assertTrue(System.nanoTime() < deadline, "the DELETE was not sent: " + requests);
			Thread.sleep(5);
		}
	}

	/** Lets the DELETE of {@link #holdTheOnlyPermit} complete. */
	private void freeThePermit(final NodeStats node) throws Exception {
		replyGate.countDown();
		final Operation<DataItem> deleted = node.awaitResult();
		assertEquals(OpType.DELETE, deleted.type());
		assertEquals(Operation.Status.SUCC, deleted.status());
	}

	/** A READ of half an object: ranged reads are not proposed for RDMA. */
	@SuppressWarnings("unchecked")
	private static Operation<DataItem> rangedRead() {
		return (Operation<DataItem>) (Operation<?>) new DataOperationImpl<>(
						0, OpType.READ, new DataItemImpl("obj-range", 0, SIZE), null, "/bucket", CREDENTIAL,
						List.of(new Range(0L, SIZE / 2 - 1, -1L)), 0);
	}

	/** A READ that no single RDMA buffer can hold; it goes over HTTP. */
	private static Operation<DataItem> largerThanAnyRdmaBuffer() {
		return op(OpType.READ, Integer.MAX_VALUE + 1L);
	}

	/** A READ proposed for RDMA whose buffer the pool has no size class for. */
	private static Operation<DataItem> largerThanAnyPoolBuffer() {
		return op(OpType.READ, RdmaBufferPool.MAX_CLASS_BYTES + 1L);
	}

	/** The fields of the driver's close summary, each with the counter that holds the same count. */
	private static final Map<String, String> CLOSE_SUMMARY_COUNTERS = Map.ofEntries(
					Map.entry("rdmaTransferred", "rdma_transferred"),
					Map.entry("bytesAssumed", "rdma_bytes_assumed"),
					Map.entry("rdmaHttpError", "rdma_request_failed"),
					Map.entry("rdmaProtocolError", "rdma_protocol_error"),
					Map.entry("rdmaTimedOut", "rdma_timed_out"),
					Map.entry("httpRdmaUnavailable", "http_rdma_unavailable"),
					Map.entry("httpBelowThreshold", "http_below_threshold"),
					Map.entry("httpIneligible", "http_ineligible"),
					Map.entry("httpOversize", "http_oversize"),
					Map.entry("httpFallback", "http_fallback"),
					Map.entry("prepareFailed", "prepare_failed"),
					Map.entry("poolHits", "pool_hits"),
					Map.entry("poolCreated", "registrations_pooled"),
					Map.entry("poolExhausted", "pool_exhausted"),
					Map.entry("poolUnpooled", "pool_oversize"),
					Map.entry("poolEvicted", "pool_evicted"),
					Map.entry("poolDiscarded", "pool_discarded"),
					Map.entry("poolInvalidReturns", "pool_invalid_returns"));
	/** The close summary field whose count two counters hold between them. */
	private static final String CLOSE_SUMMARY_DECLINED = "rdmaDeclined";
	/** The counters the close summary has no field for. */
	private static final Set<String> NOT_IN_CLOSE_SUMMARY = Set.of(
					"rdma_requests_sent",
					"rdma_declined_served_over_http",
					"rdma_declined_failed",
					"registrations_on_demand",
					"registration_failed_pooled",
					"registration_failed_on_demand",
					"allocation_failed_pooled",
					"allocation_failed_on_demand",
					"token_generation_failed");
	private static final String CLOSE_SUMMARY = "RDMA data path summary";

	@Test
	void finishedStepKeepsTheCountsOfItsStoppedDriverAndTheyMatchTheCloseSummary() throws Exception {
		final MetricsManagerImpl mgr = new MetricsManagerImpl(ServiceTaskExecutor.VT_EXECUTOR);
		mgr.setTerminalRetentionMillis(TimeUnit.MINUTES.toMillis(1));
		final DriverStatsSnapshot kept;
		final DriverStatsSnapshot afterClose;
		final String closeSummary;
		try (final MessageLog log = new MessageLog()) {
			final var driver = newDriver(true);
			final MetricsContext<AllMetricsSnapshotImpl> metrics = metricsReporting(driver);
			try (driver) {
				mgr.register(metrics);
				final ResultOutput output = new ResultOutput();
				driver.operationResultOutput(output);
				driver.start();
				// A registration failure sent over HTTP, a request the server never answers, two transfers
				// on one pooled buffer, a request the server fails, a declined GET and a declined PUT, a
				// payload below the threshold.
				reply = new Reply(200, null, null, null, false);
				transport.setFailAfterNRegistrations(0);
				assertEquals(Operation.Status.SUCC, complete(driver, output, op(OpType.CREATE, SIZE)).status());
				transport.setFailAfterNRegistrations(-1);
				reply = new Reply(NO_RESPONSE, null, null, null, false);
				assertTrue(complete(driver, output, op(OpType.CREATE, SIZE)).status() != Operation.Status.SUCC);
				reply = new Reply(200, "200", null, null, true);
				assertEquals(Operation.Status.SUCC, complete(driver, output, op(OpType.CREATE, SIZE)).status());
				assertEquals(Operation.Status.SUCC, complete(driver, output, op(OpType.CREATE, SIZE)).status());
				reply = new Reply(500, null, null, "<Error><Code>RDMATransferFailed</Code></Error>", false);
				assertEquals(
								Operation.Status.RESP_FAIL_SVC, complete(driver, output, op(OpType.CREATE, SIZE)).status());
				reply = GET_DECLINED;
				assertEquals(Operation.Status.SUCC, complete(driver, output, op(OpType.READ, SIZE)).status());
				reply = PUT_DECLINED;
				assertEquals(
								Operation.Status.RESP_FAIL_SVC, complete(driver, output, op(OpType.CREATE, SIZE)).status());
				reply = new Reply(200, null, null, null, false);
				assertEquals(
								Operation.Status.SUCC, complete(driver, output, op(OpType.CREATE, THRESHOLD - 1)).status());

				// The step stops its driver, then its metrics context leaves the metrics manager.
				driver.stop();
				mgr.unregister(metrics);

				final List<TerminalStepEntry> entries = mgr.getTerminalSteps();
				assertEquals(1, entries.size(), entries.toString());
				kept = entries.get(0).driverStats;
				assertEquals(driver.driverStats(), kept, "the values of the stopped driver");
			} finally {
				metrics.close();
			}
			afterClose = driver.driverStats();
			closeSummary = log.only(CLOSE_SUMMARY);
		}

		assertEquals(2L, kept.counters().get("rdma_transferred"), closeSummary);
		assertEquals(2L, kept.counters().get("rdma_request_failed"), "the unanswered and the failed: " + closeSummary);
		assertEquals(1L, kept.counters().get("http_below_threshold"), closeSummary);
		assertEquals(1L, kept.counters().get("http_fallback"), closeSummary);
		assertEquals(1L, kept.counters().get("pool_discarded"), "the buffer of the unanswered: " + closeSummary);
		assertEquals(1L, kept.counters().get("rdma_declined_served_over_http"), closeSummary);
		assertEquals(1L, kept.counters().get("rdma_declined_failed"), closeSummary);
		assertEquals(6L, kept.counters().get("rdma_requests_sent"), "all but the two sent over HTTP: " + closeSummary);
		// Every request had its outcome when the step stopped, so closing the driver changed no count:
		// field by field, the close summary shows what the step kept.
		final Map<String, Long> summary = summaryFields(closeSummary);
		final Map<String, Long> summaryAsCounters = new TreeMap<>();
		CLOSE_SUMMARY_COUNTERS.forEach((field, counter) -> summaryAsCounters.put(counter, summary.get(field)));
		final Map<String, Long> keptInSummary = new TreeMap<>(kept.counters());
		keptInSummary.keySet().removeAll(NOT_IN_CLOSE_SUMMARY);
		assertEquals(summaryAsCounters, keptInSummary, closeSummary);
		assertTrue(kept.counters().keySet().containsAll(NOT_IN_CLOSE_SUMMARY), kept.counters().toString());
		// The summary's declined requests are the two kinds the counters tell apart.
		assertEquals(2L, summary.get(CLOSE_SUMMARY_DECLINED), closeSummary);
		final List<String> otherFields = new ArrayList<>(summary.keySet());
		otherFields.removeAll(CLOSE_SUMMARY_COUNTERS.keySet());
		assertEquals(List.of(CLOSE_SUMMARY_DECLINED), otherFields, closeSummary);
		assertEquals(kept.counters(), afterClose.counters());
		// The gauges are those of the driver before it closed. Closing drained an idle buffer; that is
		// no discard, and the peak stays.
		assertTrue(closeSummary.contains("(transport available)"), closeSummary);
		assertEquals(1L, kept.gauges().get("transport_available"));
		assertEquals(0L, afterClose.gauges().get("transport_available"));
		assertEquals(1L, kept.gauges().get("pool_idle_buffers"));
		assertEquals(MIB, kept.gauges().get("pool_reserved_bytes"));
		assertEquals(0L, afterClose.gauges().get("pool_idle_buffers"));
		assertEquals(0L, afterClose.gauges().get("pool_reserved_bytes"));
		assertEquals(MIB, kept.gauges().get("pool_peak_reserved_bytes"));
		assertEquals(MIB, afterClose.gauges().get("pool_peak_reserved_bytes"));
	}

	/** The {@code name=count} fields of a close summary, by name. */
	private static Map<String, Long> summaryFields(final String closeSummary) {
		final Map<String, Long> fields = new TreeMap<>();
		final Matcher field = Pattern.compile("(\\w+)=(\\d+)").matcher(closeSummary);
		while (field.find()) {
			assertNull(fields.put(field.group(1), Long.parseLong(field.group(2))), closeSummary);
		}
		return fields;
	}

	/**
	 * Collects the records of the engine's message logger. The logger is asynchronous, so a read
	 * first sends a marker through it and waits for the marker.
	 */
	private static final class MessageLog extends AbstractAppender implements AutoCloseable {

		private static final String MARKER = "s3-rdma-loopback-test-marker";
		private final List<String> captured = Collections.synchronizedList(new ArrayList<>());
		private final org.apache.logging.log4j.core.Logger logger = LoggerContext.getContext(false).getLogger(Loggers.MSG.getName());
		private final Level previousLevel = logger.getLevel();

		MessageLog() {
			super("s3RdmaLoopbackMessageLog", null, null, true, Property.EMPTY_ARRAY);
			start();
			logger.addAppender(this);
			logger.setLevel(Level.INFO);
		}

		@Override
		public void append(final LogEvent event) {
			captured.add(event.getMessage().getFormattedMessage());
		}

		/** The one record that contains the text. */
		String only(final String text) throws InterruptedException {
			Loggers.MSG.warn(MARKER);
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
			while (!captured.contains(MARKER)) {
				assertFalse(System.nanoTime() > deadline, "log marker did not arrive: " + captured);
				Thread.sleep(5);
			}
			final List<String> matching;
			synchronized (captured) {
				matching = captured.stream().filter(message -> message.contains(text)).toList();
			}
			assertEquals(1, matching.size(), captured.toString());
			return matching.get(0);
		}

		@Override
		public void close() {
			logger.removeAppender(this);
			logger.setLevel(previousLevel);
			stop();
		}
	}

	/** A metrics context that reports the driver as a step's context does. */
	@SuppressWarnings("unchecked")
	private static MetricsContext<AllMetricsSnapshotImpl> metricsReporting(final S3RdmaStorageDriver<?, ?> driver) {
		final MetricsContext<AllMetricsSnapshotImpl> metrics = MetricsContextImpl.builder()
						.loadStepId(STEP_ID)
						.runId(1L)
						.opType(OpType.CREATE)
						.concurrencyLimit(1)
						.concurrencyThreshold(0)
						.itemDataSize(new SizeInBytes(SIZE))
						.stdOutColorFlag(false)
						.outputPeriodSec(1)
						.actualConcurrencyGauge(() -> 0)
						.build();
		metrics.metadata().put(
						MetricsConstants.METADATA_DRIVER_STATS, (Supplier<DriverStatsSnapshot>) driver::driverStats);
		metrics.start();
		return metrics;
	}

	/**
	 * A started driver and the statistics its node reports for it: those in the snapshot of a
	 * metrics context that asks the driver, as a step's context does.
	 *
	 * <p>A request is counted as sent once the dispatcher's submission of it has returned, which may
	 * be after its result was output. {@link #assertCounted} therefore waits for the counts it
	 * expects; counts only grow, so the wait cannot hide one that is already too high. Closing
	 * stops the driver, whose dispatcher has then returned from every submission, and fails if
	 * anything was counted after the last assertion.
	 */
	private final class NodeStats implements AutoCloseable {

		private final S3RdmaStorageDriver<DataItem, Operation<DataItem>> driver;
		private final MetricsContext<AllMetricsSnapshotImpl> metrics;
		private final ResultOutput output = new ResultOutput();
		private Map<String, Long> lastAsserted;

		NodeStats(final S3RdmaStorageDriver<DataItem, Operation<DataItem>> driver) {
			this.driver = driver;
			metrics = metricsReporting(driver);
			driver.operationResultOutput(output);
			driver.start();
		}

		/** Sends an operation whose request carries no RDMA token and returns its result. */
		Operation<DataItem> run(final Operation<DataItem> operation) throws Exception {
			return complete(driver, output, operation);
		}

		/** Sends an operation whose RDMA request the server answers with {@code answer}; returns its result. */
		Operation<DataItem> run(final Reply answer, final Operation<DataItem> operation) throws Exception {
			reply = answer;
			return complete(driver, output, operation);
		}

		Operation<DataItem> awaitResult() throws InterruptedException {
			final Operation<DataItem> result = output.await();
			assertNotNull(result, "operation did not complete: " + requests);
			return result;
		}

		DriverStatsSnapshot stats() {
			metrics.refreshLastSnapshot(true);
			return metrics.lastSnapshot().driverStats();
		}

		long gauge(final String name) {
			return stats().gauges().get(name);
		}

		/** Waits for the counters that are not zero to be exactly these. */
		void assertCounted(final Map<String, Long> expected) throws InterruptedException {
			final Map<String, Long> ordered = new TreeMap<>(expected);
			final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
			while (!ordered.equals(counted()) && System.nanoTime() < deadline) {
				Thread.sleep(5);
			}
			assertEquals(ordered, counted());
			lastAsserted = ordered;
		}

		private Map<String, Long> counted() {
			final Map<String, Long> counted = new TreeMap<>(stats().counters());
			counted.values().removeIf(count -> count == 0);
			return counted;
		}

		@Override
		public void close() throws Exception {
			try {
				if (lastAsserted != null) {
					driver.stop();
					assertEquals(lastAsserted, counted(), "counted after the last assertion");
				}
			} finally {
				try {
					metrics.close();
				} finally {
					driver.close();
				}
			}
		}
	}

	private static Operation<DataItem> complete(
					final S3RdmaStorageDriver<DataItem, Operation<DataItem>> driver,
					final ResultOutput output,
					final Operation<DataItem> operation) throws Exception {
		assertTrue(driver.put(operation));
		final Operation<DataItem> result = output.await();
		assertNotNull(result, "operation did not complete");
		return result;
	}

	// ---------- PUT payload copy ----------

	@Test
	void putCopiedOnSeveralThreadsSendsTheSameBytesAndEndsItsThreadsOnClose() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		final int copyThreads = 3;
		final int size = copyThreads * RdmaPayloadCopier.PART_BYTES + 4097;
		final byte[] copiedByTheDispatcher;
		try (final var driver = newDriver(false)) {
			assertEquals(Operation.Status.SUCC, execute(driver, op(OpType.CREATE, size)).status());
			copiedByTheDispatcher = serverReadPayload;
			assertEquals(0, copyThreadsAlive(), "the shipped default starts no copy thread");
		}
		final RdmaPayloadCopier copier;
		try (final var driver = newDriver(false, config -> config.val("storage-rdma-copyThreads", copyThreads))) {
			copier = driver.payloadCopier();
			final var results = executeInSequence(driver, List.of(op(OpType.CREATE, size), op(OpType.CREATE, size)));

			results.forEach(result -> assertEquals(Operation.Status.SUCC, result.status()));
			assertEquals(size, ((DataOperation<?>) results.get(1)).countBytesDone());
			assertArrayEquals(copiedByTheDispatcher, serverReadPayload);
			assertEquals(copyThreads - 1, copyThreadsAlive(), "the dispatcher copies one part itself");
			assertNoBufferInUse(driver);
		}
		assertTrue(copier.isTerminated());
		final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(RESULT_TIMEOUT_SECONDS);
		while (copyThreadsAlive() > 0 && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertEquals(0, copyThreadsAlive(), "closing the driver ends its copy threads");
	}

	@Test
	void copyOutOfMemoryOnACopyThreadFailsTheOperationAndReleasesItsBuffer() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		final int size = 2 * RdmaPayloadCopier.PART_BYTES;
		try (final var driver = newDriver(false, config -> config.val("storage-rdma-copyThreads", 2))) {
			final var results = executeInSequence(driver, List.of(
							putWithReadHook(size, S3RdmaReplyLoopbackTest::outOfMemoryOnACopyThread),
							op(OpType.CREATE, size)));

			assertEquals(Operation.Status.FAIL_IO, results.get(0).status());
			assertEquals(0, ((DataOperation<?>) results.get(0)).countBytesDone());
			assertEquals(1, driver.pathStats().prepareFailed.sum());
			assertEquals(Operation.Status.SUCC, results.get(1).status(), "the driver must go on dispatching");
			assertNotNull(onlyObjectRequest("PUT").rdmaToken());
			assertNoBufferInUse(driver);
		}
	}

	@Test
	void copyOutOfMemoryWithFallbackSendsTheWholePayloadOverHttp() throws Exception {
		final int size = 2 * RdmaPayloadCopier.PART_BYTES;
		final byte[] payload = payloadSentOverRdma(size);
		reply = new Reply(200, null, null, null, false);
		try (final var driver = newDriver(true, config -> config.val("storage-rdma-copyThreads", 2))) {
			final var result = execute(
							driver, putWithReadHook(size, S3RdmaReplyLoopbackTest::outOfMemoryOnACopyThread));

			assertEquals(Operation.Status.SUCC, result.status());
			assertEquals(size, ((DataOperation<?>) result).countBytesDone());
			assertNull(onlyObjectRequest("PUT").rdmaToken());
			assertArrayEquals(payload, httpPutBody, "the first part was already copied for RDMA");
			assertEquals(1, driver.pathStats().httpFallback.sum());
			assertNoBufferInUse(driver);
		}
	}

	@Test
	void copyOutOfMemoryOnTheDispatcherFailsTheOperationAndTheNextOneIsSent() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		try (final var driver = newDriver(false)) {
			final var results = executeInSequence(driver, List.of(
							putWithReadHook(SIZE, partOffset -> {
								throw new OutOfMemoryError("Cannot reserve " + SIZE + " bytes of direct buffer memory");
							}),
							op(OpType.CREATE, SIZE)));

			assertEquals(Operation.Status.FAIL_IO, results.get(0).status());
			assertEquals(0, ((DataOperation<?>) results.get(0)).countBytesDone());
			assertEquals(1, driver.pathStats().prepareFailed.sum());
			assertEquals(Operation.Status.SUCC, results.get(1).status(), "the driver must go on dispatching");
			assertNotNull(onlyObjectRequest("PUT").rdmaToken());
			assertNoBufferInUse(driver);
		}
	}

	@Test
	void closeStopsARunningSplitCopyAndReleasesItsBufferBeforeTheTransportCloses() throws Exception {
		reply = new Reply(200, "200", null, null, true);
		final int size = 2 * RdmaPayloadCopier.PART_BYTES;
		final var copyThreadCopying = new CountDownLatch(1);
		final var driver = newDriver(false, config -> config.val("storage-rdma-copyThreads", 2));
		try {
			startWith(driver);
			assertTrue(driver.put(putWithReadHook(size, partOffset -> {
				if (partOffset > 0) {
					copyThreadCopying.countDown();
					// Ends only when the close interrupts the copy thread.
					new CountDownLatch(1).await();
				}
			})));
			assertTrue(copyThreadCopying.await(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS));
		} finally {
			driver.close();
		}

		assertTrue(driver.payloadCopier().isTerminated());
		assertEquals(0, transport.getActiveRegistrationsAtClose(),
						"the stopped copy's buffer must be deregistered while the transport is still open");
		assertEquals(0, driver.bufferPool().liveBuffers(), driver.bufferPool().summary());
		assertEquals(0, copyThreadsAlive(), "closing the driver ends its copy threads");
		assertTrue(objectRequests().isEmpty(), "no request may be sent: " + requests);
		assertEquals(0, driver.pathStats().prepareFailed.sum(), "a copy stopped by the close is not a failure");
	}

	/** Runs before a read of a PUT's payload; {@code partOffset} is 0 for the item and its first part. */
	private interface ReadHook {
		void beforeRead(long partOffset) throws Exception;
	}

	/** Fails a part copied by a copy thread as an exhausted data-layer allocation does. */
	private static void outOfMemoryOnACopyThread(final long partOffset) {
		if (partOffset > 0) {
			throw new OutOfMemoryError("Cannot reserve 1048576 bytes of direct buffer memory");
		}
	}

	/** The PUT of {@link #op}, except that {@code hook} runs before each read of the item or of a part of it. */
	@SuppressWarnings("unchecked")
	private static Operation<DataItem> putWithReadHook(final int size, final ReadHook hook) throws IOException {
		final DataItem whole = new DataItemImpl("obj-" + OpType.CREATE, 0, size);
		final DataItem item = hooked(whole, 0, hook);
		doAnswer(slice -> {
			final long from = slice.getArgument(0);
			final long partSize = slice.getArgument(1);
			return hooked(whole.slice(from, partSize), from, hook);
		}).when(item).slice(anyLong(), anyLong());
		return (Operation<DataItem>) (Operation<?>) new DataOperationImpl<>(
						0, OpType.CREATE, item, null, "/bucket", CREDENTIAL, null, 0);
	}

	private static DataItem hooked(final DataItem source, final long partOffset, final ReadHook hook)
					throws IOException {
		final DataItem item = mock(DataItem.class, delegatesTo(source));
		doAnswer(read -> {
			hook.beforeRead(partOffset);
			final ByteBuffer dst = read.getArgument(0);
			return source.read(dst);
		}).when(item).read(any(ByteBuffer.class));
		return item;
	}

	private static long copyThreadsAlive() {
		return Thread.getAllStackTraces().keySet().stream()
						.filter(thread -> thread.isAlive() && thread.getName().startsWith(STEP_ID + "-rdma-copy-"))
						.count();
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
			assertEquals(2, driver.pathStats().rdmaDeclinedFailed.sum());
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
		if ("PUT".equals(method) && requestBody.length > 0) {
			httpPutBody = requestBody;
		}
		final boolean objectRequest = path.chars().filter(c -> c == '/').count() > 1;
		if (objectRequest) {
			awaitReplyGate();
		}
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

	private void awaitReplyGate() throws IOException {
		final CountDownLatch gate = replyGate;
		try {
			if (gate != null && !gate.await(RESULT_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
				throw new IOException("reply gate was not opened");
			}
		} catch (final InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException(e);
		}
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
		transport.setFailInit(rdmaInitFails);
		return new S3RdmaStorageDriver<>(
						STEP_ID,
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
