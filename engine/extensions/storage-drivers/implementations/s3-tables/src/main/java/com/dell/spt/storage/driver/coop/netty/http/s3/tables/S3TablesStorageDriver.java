package com.dell.spt.storage.driver.coop.netty.http.s3.tables;

import com.dell.spt.base.config.IllegalConfigurationException;
import com.dell.spt.base.data.DataInput;
import com.dell.spt.base.item.Item;
import com.dell.spt.base.item.ItemFactory;
import com.dell.spt.base.item.op.Operation;
import com.dell.spt.base.logging.Loggers;
import com.dell.spt.storage.driver.coop.netty.http.s3.S3StorageDriver;

import com.github.akurilov.confuse.Config;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.EmptyHttpHeaders;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpVersion;

import java.io.IOException;
import java.net.ConnectException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.dell.spt.base.env.DateUtil;
import com.dell.spt.storage.driver.coop.netty.http.s3.S3Api;

public class S3TablesStorageDriver<I extends Item, O extends Operation<I>>
				extends S3StorageDriver<I, O> {

	static final String OP_MODE_PROVISION = "provision";
	static final String OP_MODE_TABLE_WRITE = "tableWrite";
	static final String OP_MODE_TABLE_READ = "tableRead";
	static final String OP_MODE_TABLE_CATALOG = "tableCatalog";
	static final String OP_MODE_CATALOG_SEED = "catalogSeed";
	static final String OP_MODE_TABLE_COMPACTION_POLL = "tableCompactionPoll";

	/** Data-plane SigV4 service name for S3 object PUT requests. */
	private static final String S3_SERVICE = "s3";

	protected final String opMode;
	protected final S3TablesControlPlane controlPlane;
	final long targetFileSizeBytes;

	/** Initialized lazily after provision completes (needs warehouseLocation). */
	private final AtomicReference<IcebergCommitter> committer = new AtomicReference<>(null);

	/** Counts HTTP 409 version-conflict retries across all threads. */
	private final AtomicLong commitRetryCount = new AtomicLong(0);

	/** Compaction stats — populated by CompactionPoller when tableCompactionPoll completes. */
	private volatile long compactionStartMs = 0;
	private volatile long compactionCompleteMs = 0;
	private volatile long compactionFilesBefore = 0;
	private volatile long compactionFilesAfter = 0;

	/** Shared writer for generating Parquet bytes; thread-safe (stateless per call). */
	private final ParquetRowGroupWriter parquetWriter;

	/** Monotonically increasing base row ID shared across all writes this step. */
	private final AtomicLong baseRowId = new AtomicLong(0);

	/** Compaction poller config — only meaningful when opMode=tableCompactionPoll. */
	private final long totalIngestBytes;
	private final double compactionToleranceFactor;
	private final int compactionStabilizationPolls;
	private final long compactionPollIntervalMs;
	private final long compactionPollTimeoutMs;

	/** Catalog test vector config — only meaningful when opMode=catalogSeed or tableCatalog. */
	private final int namespaceCount;
	private final int tablesPerNs;
	/** Global counter for assigning unique (nsIdx, tblIdx) pairs to catalogSeed ops. */
	private final AtomicLong seedIndex = new AtomicLong(0);
	/** Per-driver random source for tableCatalog op selection (not crypto, jitter only). */
	private final Random catalogRand = new Random(); // NOSONAR — non-crypto use

	public S3TablesStorageDriver(
					final String stepId,
					final DataInput dataInput,
					final Config storageConfig,
					final boolean verifyFlag,
					final int batchSize)
					throws IllegalConfigurationException, InterruptedException {
		super(stepId, dataInput, storageConfig, verifyFlag, batchSize, "s3tables");
		final Config s3tablesConfig = storageConfig.configVal("s3tables");
		this.opMode = s3tablesConfig.stringVal("opMode");
		this.targetFileSizeBytes = s3tablesConfig.intVal("targetFileSizeBytes");
		this.totalIngestBytes = s3tablesConfig.intVal("totalIngestBytes");
		this.compactionToleranceFactor = s3tablesConfig.doubleVal("compactionToleranceFactor");
		this.compactionStabilizationPolls = s3tablesConfig.intVal("compactionStabilizationPolls");
		this.compactionPollIntervalMs = s3tablesConfig.intVal("compactionPollIntervalMs");
		this.compactionPollTimeoutMs = s3tablesConfig.intVal("compactionPollTimeoutMs");
		this.namespaceCount = s3tablesConfig.intVal("namespaceCount");
		this.tablesPerNs = s3tablesConfig.intVal("tablesPerNamespace");
		this.parquetWriter = new ParquetRowGroupWriter(targetFileSizeBytes);
		final String controlPlaneEndpoint = s3tablesConfig.stringVal("controlPlaneEndpoint");
		final String effectiveEndpoint = controlPlaneEndpoint.isEmpty()
						? storageNodeAddrs[0]
						: controlPlaneEndpoint;
		this.controlPlane = new S3TablesControlPlane(
						effectiveEndpoint,
						s3tablesConfig.stringVal("bucket"),
						s3tablesConfig.stringVal("namespace"),
						s3tablesConfig.stringVal("tableName"),
						this);
		// Disable per-op path resolution for all opModes: the s3-tables driver
		// manages its own paths (Iceberg warehouse). requestNewPath is never called
		// for NOOP ops (no dstPath), so provision() is invoked directly from submit().
		this.requestNewPathFunc = null;
	}

	@Override
	protected String requestNewPath(final String path) {
		return path;
	}

	/**
	 * Initializes the IcebergCommitter lazily on first tableWrite submit.
	 * Thread-safe via compareAndSet on the AtomicReference.
	 */
	private IcebergCommitter ensureCommitter() {
		IcebergCommitter c = committer.get();
		if (c != null) {
			return c;
		}
		try {
			final IcebergCommitter.MetadataLocationResult loc = controlPlane.getTableMetadataLocation();
			c = new IcebergCommitter(
							this, deriveWarehouseLocation(loc.metadataLocation), loc.versionToken);
			committer.compareAndSet(null, c);
			return committer.get();
		} catch (final Exception e) {
			Loggers.ERR.error("{}: failed to init IcebergCommitter: {}", stepId, e.getMessage());
			return null;
		}
	}

	@Override
	protected boolean submit(final O op) throws IllegalStateException {
		// This implementation bypasses NettyStorageDriverBase.submit() for synchronous
		// control-plane modes, so it must cross the same atomic dispatch gate before the first
		// control-plane request. Completion-time fallback is too late to distinguish this work
		// from queue recovery during concurrent admission closure.
		if (!beginDispatch(op)) {
			return false;
		}
		switch (opMode) {
		case OP_MODE_PROVISION:
			try {
				controlPlane.provision();
				op.status(Operation.Status.SUCC);
			} catch (final Exception e) {
				Loggers.ERR.error("{}: provision failed: {}", stepId, e.getMessage());
				op.status(Operation.Status.FAIL_IO);
			}
			handleCompleted(op);
			return true;
		case OP_MODE_TABLE_WRITE:
			return submitTableWrite(op);
		case OP_MODE_TABLE_COMPACTION_POLL:
			return submitCompactionPoll(op);
		case OP_MODE_CATALOG_SEED:
			return submitCatalogSeed(op);
		case OP_MODE_TABLE_CATALOG:
			return submitTableCatalog(op);
		default:
			throw new IllegalStateException("Unsupported opMode: " + opMode);
		}
	}

	private boolean submitCompactionPoll(final O op) {
		final CompactionPoller poller = new CompactionPoller(
						this,
						totalIngestBytes,
						targetFileSizeBytes,
						compactionToleranceFactor,
						compactionStabilizationPolls,
						compactionPollIntervalMs,
						compactionPollTimeoutMs);
		op.startRequest();
		op.finishRequest();
		op.startResponse();
		try {
			poller.poll();
			op.status(Operation.Status.SUCC);
		} catch (final Exception e) {
			Loggers.ERR.error("{}: tableCompactionPoll failed: {}", stepId, e.getMessage());
			op.status(Operation.Status.FAIL_IO);
		} finally {
			op.finishResponse();
		}
		handleCompleted(op);
		return true;
	}

	/**
	 * catalogSeed: each op creates one (namespace, table) pair from the matrix
	 * ns-0..ns-(namespaceCount-1) × tbl-0..tbl-(tablesPerNs-1).
	 * The global seedIndex counter assigns a unique flat index per op; workers start
	 * at a random offset (modulo total) to avoid lockstep ns-0 creation.
	 */
	private boolean submitCatalogSeed(final O op) {
		final int total = namespaceCount * tablesPerNs;
		final long idx = seedIndex.getAndIncrement() % total;
		final int nsIdx = (int) (idx / tablesPerNs);
		final int tblIdx = (int) (idx % tablesPerNs);
		op.startRequest();
		op.finishRequest();
		op.startResponse();
		try {
			controlPlane.catalogSeedOne(nsIdx, tblIdx);
			op.status(Operation.Status.SUCC);
		} catch (final Exception e) {
			Loggers.ERR.warn("{}: catalogSeed ns-{}/tbl-{} failed: {}", stepId, nsIdx, tblIdx, e.getMessage());
			op.status(Operation.Status.FAIL_IO);
		} finally {
			op.finishResponse();
		}
		handleCompleted(op);
		return true;
	}

	/**
	 * tableCatalog: each op performs either a GetTable or ListTables call,
	 * chosen randomly with 50/50 probability (Rand() % 2 == 0).
	 * The target namespace and table are chosen uniformly at random from the seeded matrix.
	 */
	private boolean submitTableCatalog(final O op) {
		final int nsIdx;
		final int tblIdx;
		final boolean doGetTable;
		synchronized (catalogRand) {
			nsIdx = catalogRand.nextInt(namespaceCount);
			tblIdx = catalogRand.nextInt(tablesPerNs);
			doGetTable = (catalogRand.nextInt(2) == 0);
		}
		final String ns = "ns-" + nsIdx;
		final String tbl = "tbl-" + tblIdx;
		op.startRequest();
		op.finishRequest();
		op.startResponse();
		try {
			if (doGetTable) {
				controlPlane.getTable(ns, tbl);
			} else {
				controlPlane.listTables(ns);
			}
			op.status(Operation.Status.SUCC);
		} catch (final Exception e) {
			Loggers.ERR.warn("{}: tableCatalog {} {}/{} failed: {}",
							stepId, doGetTable ? "GetTable" : "ListTables", ns, tbl, e.getMessage());
			op.status(Operation.Status.FAIL_IO);
		} finally {
			op.finishResponse();
		}
		handleCompleted(op);
		return true;
	}

	private boolean submitTableWrite(final O op) throws IllegalStateException {
		final IcebergCommitter c = ensureCommitter();
		if (c == null) {
			op.startRequest();
			op.finishRequest();
			op.startResponse();
			op.finishResponse();
			op.status(Operation.Status.FAIL_IO);
			handleCompleted(op);
			return true;
		}
		concurrencyThrottle.acquireUninterruptibly();
		try {
			op.startRequest();
			final long rowId = baseRowId.getAndAdd(targetFileSizeBytes / 44 + 1);
			final byte[] parquetBytes = parquetWriter.write(rowId);
			// data written to S3 — request phase done, response (metadata commit) begins
			final IcebergCommitter.DataPlaneResult dp = c.commitDataPlane(parquetBytes, parquetBytes.length);
			op.finishRequest();
			op.startResponse();
			c.commitMetadata(dp);
			op.finishResponse();
			op.status(Operation.Status.SUCC);
		} catch (final Exception e) {
			Loggers.ERR.warn("{}: tableWrite failed: {}", stepId, e.getMessage());
			if (op.reqTimeStart() > 0 && op.reqTimeDone() == 0) {
				op.finishRequest();
			}
			if (op.reqTimeDone() > 0 && op.respTimeStart() == 0) {
				op.startResponse();
			}
			if (op.respTimeStart() > 0 && op.respTimeDone() == 0) {
				op.finishResponse();
			}
			op.status(Operation.Status.FAIL_IO);
		} finally {
			concurrencyThrottle.release();
		}
		handleCompleted(op);
		return true;
	}

	@Override
	protected int submit(final List<O> ops, final int from, final int to) throws IllegalStateException {
		int submitted = 0;
		for (int i = from; i < to; i++) {
			if (submit(ops.get(i))) {
				submitted++;
			} else {
				break;
			}
		}
		return submitted;
	}

	String getStepId() {
		return stepId;
	}

	S3TablesControlPlane getControlPlane() {
		return controlPlane;
	}

	void recordCommitRetry() {
		commitRetryCount.incrementAndGet();
	}

	long getCommitRetryCount() {
		return commitRetryCount.get();
	}

	/**
	 * Called by CompactionPoller on convergence or timeout to record compaction outcome.
	 */
	void recordCompactionStats(
					final long startMs,
					final long completeMs,
					final long filesBefore,
					final long filesAfter) {
		this.compactionStartMs = startMs;
		this.compactionCompleteMs = completeMs;
		this.compactionFilesBefore = filesBefore;
		this.compactionFilesAfter = filesAfter;
	}

	/**
	 * Executes a control-plane request signed with service="s3tables".
	 * Uses the inherited applyAuthHeaders (which uses this.sigV4ServiceName = "s3tables").
	 */
	FullHttpResponse executeControlPlaneRequest(
					final HttpMethod method,
					final String uri,
					final byte[] bodyBytes)
					throws Exception {
		final HttpHeaders headers = new DefaultHttpHeaders();
		headers.set(HttpHeaderNames.HOST, storageNodeAddrs[0]);
		headers.set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
		headers.set(HttpHeaderNames.CONTENT_LENGTH, bodyBytes.length);
		applyDynamicHeaders(headers);
		applySharedHeaders(headers);
		applyAuthHeaders(headers, method, uri, credential);
		final FullHttpRequest req = new DefaultFullHttpRequest(
						HttpVersion.HTTP_1_1,
						method,
						uri,
						Unpooled.wrappedBuffer(bodyBytes),
						headers,
						EmptyHttpHeaders.INSTANCE);
		try {
			return executeHttpRequest(req);
		} catch (final ConnectException e) {
			throw new Exception("Connection failure calling " + method + " " + uri + ": " + e.getMessage(), e);
		}
	}

	/**
	 * Executes a data-plane S3 PUT signed with service="s3".
	 * Cannot use applyAuthHeaders (which is locked to "s3tables"), so signs inline.
	 */
	void executeDataPlaneRequest(
					final String s3Path,
					final byte[] bodyBytes,
					final String contentType) throws Exception {
		final HttpHeaders headers = new DefaultHttpHeaders();
		final String host = storageNodeAddrs[0];
		headers.set(HttpHeaderNames.HOST, host);
		headers.set(HttpHeaderNames.CONTENT_TYPE, contentType);
		headers.set(HttpHeaderNames.CONTENT_LENGTH, bodyBytes.length);
		applyDynamicHeaders(headers);
		applySharedHeaders(headers);
		applyDataPlaneAuthHeaders(headers, HttpMethod.PUT, s3Path);

		final FullHttpRequest req = new DefaultFullHttpRequest(
						HttpVersion.HTTP_1_1,
						HttpMethod.PUT,
						s3Path,
						Unpooled.wrappedBuffer(bodyBytes),
						headers,
						EmptyHttpHeaders.INSTANCE);
		final FullHttpResponse resp;
		try {
			resp = executeHttpRequest(req);
		} catch (final ConnectException e) {
			throw new Exception("Data-plane PUT failed for " + s3Path + ": " + e.getMessage(), e);
		}
		if (resp == null) {
			throw new Exception("Data-plane PUT timeout for " + s3Path);
		}
		try {
			final int status = resp.status().code();
			if (status < 200 || status >= 300) {
				final String body = resp.content().toString(StandardCharsets.UTF_8);
				throw new Exception("Data-plane PUT " + s3Path + " returned HTTP " + status + ": " + body);
			}
		} finally {
			resp.release();
		}
	}

	/**
	 * Executes a data-plane S3 GET signed with service="s3".
	 * Returns the response body bytes, or throws on timeout/non-2xx.
	 */
	byte[] executeDataPlaneGet(final String s3Path) throws Exception {
		final HttpHeaders headers = new DefaultHttpHeaders();
		final String host = storageNodeAddrs[0];
		headers.set(HttpHeaderNames.HOST, host);
		headers.set(HttpHeaderNames.CONTENT_LENGTH, 0);
		applyDynamicHeaders(headers);
		applySharedHeaders(headers);
		applyDataPlaneAuthHeaders(headers, HttpMethod.GET, s3Path);

		final FullHttpRequest req = new DefaultFullHttpRequest(
						HttpVersion.HTTP_1_1,
						HttpMethod.GET,
						s3Path,
						Unpooled.EMPTY_BUFFER,
						headers,
						EmptyHttpHeaders.INSTANCE);
		final FullHttpResponse resp;
		try {
			resp = executeHttpRequest(req);
		} catch (final ConnectException e) {
			throw new Exception("Data-plane GET failed for " + s3Path + ": " + e.getMessage(), e);
		}
		if (resp == null) {
			throw new Exception("Data-plane GET timeout for " + s3Path);
		}
		try {
			final int status = resp.status().code();
			if (status < 200 || status >= 300) {
				final String body = resp.content().toString(StandardCharsets.UTF_8);
				throw new Exception("Data-plane GET " + s3Path + " returned HTTP " + status + ": " + body);
			}
			final byte[] bytes = new byte[resp.content().readableBytes()];
			resp.content().readBytes(bytes);
			return bytes;
		} finally {
			resp.release();
		}
	}

	/**
	 * Applies SigV4 auth headers with service="s3" for data-plane requests.
	 * Mirrors the v4 branch of S3StorageDriver.applyAuthHeaders, substituting S3_SERVICE.
	 */
	private void applyDataPlaneAuthHeaders(
					final HttpHeaders headers,
					final HttpMethod method,
					final String uri) {
		if (credential == null) {
			return;
		}
		final String uid = credential.getUid();
		final String secret = credential.getSecret();
		if (uid == null || secret == null) {
			return;
		}
		try {
			final Instant now = Instant.now();
			headers.remove(HttpHeaderNames.DATE);
			headers.set(S3Api.AMZ_DATE_HEADER, DateUtil.formatAmazon(now));

			final String contentLengthStr = headers.get(HttpHeaderNames.CONTENT_LENGTH);
			final int contentLength = contentLengthStr == null ? 0 : Integer.parseInt(contentLengthStr);
			if (contentLength > 0) {
				headers.set(S3Api.AMZ_PAYLOAD_HEADER, S3Api.AMZ_UNSIGNED_PAYLOAD);
			} else {
				headers.set(S3Api.AMZ_PAYLOAD_HEADER, S3Api.AMZ_EMPTY_BODY_SHA256);
			}

			final String datetime = headers.get(S3Api.AMZ_DATE_HEADER);
			final String date = datetime.substring(0, 8);

			// Normalize host for signing (strip default ports)
			final String hostHeader = headers.get(HttpHeaderNames.HOST);
			if (hostHeader != null) {
				final int colonIdx = hostHeader.indexOf(':');
				if (colonIdx > 0) {
					final String portPart = hostHeader.substring(colonIdx + 1);
					if ("80".equals(portPart) || "443".equals(portPart)) {
						headers.set(HttpHeaderNames.HOST, hostHeader.substring(0, colonIdx));
					}
				}
			}

			final byte[] signingKey = buildDataPlaneSigningKey(secret, date);
			final Map<String, String> sortedHeaders = getDataPlaneSortedHeaders(headers);
			final String canonicalForm = buildCanonicalRequest(headers, sortedHeaders, method, uri);
			final byte[] encodedHash = MessageDigest.getInstance("SHA-256")
							.digest(canonicalForm.getBytes(StandardCharsets.UTF_8));
			final String stringToSign = "AWS4-HMAC-SHA256\n"
							+ datetime + "\n"
							+ date + "/" + awsRegion + "/" + S3_SERVICE + "/aws4_request\n"
							+ bytesToHexStr(encodedHash);

			final byte[] sigData = hmacSHA256(stringToSign.getBytes(StandardCharsets.UTF_8), signingKey);
			final StringBuilder sb = new StringBuilder(240);
			sb.append(S3Api.AUTH_V4_PREFIX)
							.append("Credential=").append(uid).append('/').append(date)
							.append('/').append(awsRegion).append('/').append(S3_SERVICE).append("/aws4_request, ")
							.append("SignedHeaders=").append(String.join(";", sortedHeaders.keySet()))
							.append(", Signature=").append(bytesToHexStr(sigData));
			headers.set(HttpHeaderNames.AUTHORIZATION, sb.toString());
		} catch (final NoSuchAlgorithmException | InvalidKeyException e) {
			Loggers.ERR.warn("{}: data-plane SigV4 signing failed: {}", stepId, e.getMessage());
		}
	}

	private byte[] buildDataPlaneSigningKey(final String secret, final String date)
					throws NoSuchAlgorithmException, InvalidKeyException {
		final byte[] kSecret = ("AWS4" + secret).getBytes(StandardCharsets.UTF_8);
		final byte[] kDate = hmacSHA256(date.getBytes(StandardCharsets.UTF_8), kSecret);
		final byte[] kRegion = hmacSHA256(awsRegion.getBytes(StandardCharsets.UTF_8), kDate);
		final byte[] kService = hmacSHA256(S3_SERVICE.getBytes(StandardCharsets.UTF_8), kRegion);
		return hmacSHA256("aws4_request".getBytes(StandardCharsets.UTF_8), kService);
	}

	private static byte[] hmacSHA256(final byte[] data, final byte[] key)
					throws NoSuchAlgorithmException, InvalidKeyException {
		final Mac mac = Mac.getInstance("HmacSHA256");
		mac.init(new SecretKeySpec(key, "RawBytes"));
		return mac.doFinal(data);
	}

	private static Map<String, String> getDataPlaneSortedHeaders(final HttpHeaders headers) {
		final Map<String, String> sorted = new TreeMap<>();
		for (final Map.Entry<String, String> h : headers) {
			final String name = h.getKey().toLowerCase(Locale.ROOT);
			if (name.equals("host") || name.startsWith("x-amz-")) {
				sorted.put(name, h.getValue());
			}
		}
		return sorted;
	}

	private static String buildCanonicalRequest(
					final HttpHeaders headers,
					final Map<String, String> sortedHeaders,
					final HttpMethod method,
					final String uri) {
		final StringBuilder sb = new StringBuilder();
		sb.append(method.name()).append('\n');
		// canonical URI (path only, no query)
		final int qIdx = uri.indexOf('?');
		sb.append(qIdx < 0 ? uri : uri.substring(0, qIdx)).append('\n');
		// canonical query string
		sb.append(qIdx < 0 ? "" : uri.substring(qIdx + 1)).append('\n');
		// canonical headers
		for (final Map.Entry<String, String> h : sortedHeaders.entrySet()) {
			sb.append(h.getKey()).append(':').append(h.getValue().trim()).append('\n');
		}
		sb.append('\n');
		// signed headers list
		sb.append(String.join(";", sortedHeaders.keySet())).append('\n');
		// payload hash
		final String payloadHeader = headers.get(S3Api.AMZ_PAYLOAD_HEADER);
		sb.append(payloadHeader != null ? payloadHeader : S3Api.AMZ_EMPTY_BODY_SHA256);
		return sb.toString();
	}

	private static String bytesToHexStr(final byte[] bytes) {
		final StringBuilder sb = new StringBuilder(bytes.length * 2);
		for (final byte b : bytes) {
			sb.append(String.format("%02x", b & 0xFF));
		}
		return sb.toString();
	}

	/**
	 * Derives the warehouse location (s3://...) from a metadata file S3 URI.
	 * e.g. s3://bucket/prefix/metadata/v1.metadata.json → s3://bucket/prefix
	 */
	private static String deriveWarehouseLocation(final String metadataLocation) {
		if (metadataLocation == null || metadataLocation.isEmpty()) {
			return "";
		}
		final int metaIdx = metadataLocation.lastIndexOf("/metadata/");
		if (metaIdx > 0) {
			return metadataLocation.substring(0, metaIdx);
		}
		return metadataLocation;
	}

	@Override
	protected void doClose() throws IOException {
		final long durationMs = compactionCompleteMs > 0
						? compactionCompleteMs - compactionStartMs
						: 0;
		Loggers.TABLES_METRICS.info(
						"snapshot_commit_retries={},compaction_start_ms={},compaction_complete_ms={}"
										+ ",compaction_duration_ms={},compaction_files_before={},compaction_files_after={}",
						commitRetryCount.get(),
						compactionStartMs,
						compactionCompleteMs,
						durationMs,
						compactionFilesBefore,
						compactionFilesAfter);
		super.doClose();
	}

	@Override
	public List<I> list(
					final ItemFactory<I> itemFactory,
					final String path,
					final String prefix,
					final int idRadix,
					final I lastPrevItem,
					final int count)
					throws IOException {
		return java.util.Collections.emptyList();
	}
}
